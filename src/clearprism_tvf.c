/*
 * clearprism_tvf.c — clearprism_query() table-valued function
 *
 * An eponymous-only virtual table that pushes a SQL template to each
 * source database in parallel, collects per-shard results, and returns
 * them as a unified result set with _source_db appended.
 *
 * Usage:
 *   SELECT c0, sum(c1) FROM clearprism_query('vtab_name',
 *     'SELECT category, count(*) FROM items GROUP BY category')
 *   GROUP BY c0;
 *
 * The TVF declares a wide schema with generic columns c0..c15 plus
 * _source_db TEXT.  The inner query's columns map positionally to
 * c0, c1, c2, ... and unused columns return NULL.
 *
 * Two HIDDEN columns (vtab_name, sql_template) are used as arguments.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

/* Number of generic data columns (c0 .. c15) */
#define TVF_MAX_DATA_COLS 16

/* ========== Per-source result buffer ========== */

struct tvf_source_result {
    sqlite3_value **values;   /* flat: n_rows * n_cols */
    int n_rows;
    int n_cols;               /* columns from the inner query */
    char *alias;
    int errored;
};

/* ========== Worker context for parallel query execution ========== */

struct tvf_worker_ctx {
    clearprism_connpool *pool;
    clearprism_source *sources;
    int n_sources;
    int next_source;              /* atomic counter */
    const char *sql_template;     /* SQL to execute on each source */
    const char *target_table;     /* target table name for {t} substitution */
    struct tvf_source_result *results;
};

/* Worker function: checkout connection, execute SQL, collect all rows */
static void *tvf_worker(void *arg)
{
    struct tvf_worker_ctx *ctx = (struct tvf_worker_ctx *)arg;
    while (1) {
        int idx = __sync_fetch_and_add(&ctx->next_source, 1);
        if (idx >= ctx->n_sources) break;

        struct tvf_source_result *r = &ctx->results[idx];
        r->values = NULL;
        r->n_rows = 0;
        r->n_cols = 0;
        r->alias = ctx->sources[idx].alias;
        r->errored = 0;

        char *pool_err = NULL;
        sqlite3 *conn = clearprism_connpool_checkout(ctx->pool,
                            ctx->sources[idx].path,
                            ctx->sources[idx].alias, &pool_err);
        if (!conn) {
            if (pool_err) {
                sqlite3_log(SQLITE_WARNING,
                    "clearprism_query: checkout failed for '%s': %s",
                    ctx->sources[idx].path, pool_err);
            }
            sqlite3_free(pool_err);
            r->errored = 1;
            continue;
        }
        sqlite3_free(pool_err);

        /* Replace {t} with target table name if present */
        const char *sql = ctx->sql_template;
        char *expanded = NULL;
        if (ctx->target_table) {
            const char *pos = strstr(sql, "{t}");
            if (pos) {
                expanded = sqlite3_mprintf("%.*s%s%s",
                    (int)(pos - sql), sql, ctx->target_table, pos + 3);
                sql = expanded;
            }
        }

        sqlite3_stmt *stmt = NULL;
        int rc = sqlite3_prepare_v2(conn, sql, -1, &stmt, NULL);
        sqlite3_free(expanded);
        if (rc != SQLITE_OK) {
            clearprism_connpool_checkin(ctx->pool, ctx->sources[idx].path);
            r->errored = 1;
            continue;
        }

        int n_cols = sqlite3_column_count(stmt);
        r->n_cols = n_cols;

        /* Collect all rows into a flat buffer */
        int capacity = 64;
        int count = 0;
        sqlite3_value **values = sqlite3_malloc64((sqlite3_uint64)capacity * n_cols * sizeof(sqlite3_value *));
        if (!values) {
            sqlite3_finalize(stmt);
            clearprism_connpool_checkin(ctx->pool, ctx->sources[idx].path);
            r->errored = 1;
            continue;
        }

        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            if (count >= capacity) {
                capacity *= 2;
                sqlite3_value **tmp = sqlite3_realloc64(values,
                    (sqlite3_uint64)capacity * n_cols * sizeof(sqlite3_value *));
                if (!tmp) {
                    /* Free already collected values */
                    for (int i = 0; i < count * n_cols; i++) {
                        if (values[i]) sqlite3_value_free(values[i]);
                    }
                    sqlite3_free(values);
                    values = NULL;
                    r->errored = 1;
                    break;
                }
                values = tmp;
            }
            /* Deep-copy each column value */
            for (int c = 0; c < n_cols; c++) {
                sqlite3_value *src_val = sqlite3_column_value(stmt, c);
                sqlite3_value *duped = sqlite3_value_dup(src_val);
                if (!duped && sqlite3_value_type(src_val) != SQLITE_NULL) {
                    /* OOM: clean up this partial row and bail */
                    for (int j = 0; j < c; j++)
                        sqlite3_value_free(values[count * n_cols + j]);
                    r->errored = 1;
                    goto worker_cleanup;
                }
                values[count * n_cols + c] = duped;
            }
            count++;
        }

    worker_cleanup:
        sqlite3_finalize(stmt);
        clearprism_connpool_checkin(ctx->pool, ctx->sources[idx].path);

        if (r->errored) {
            /* Free all previously collected values on error */
            for (int i = 0; i < count * n_cols; i++) {
                if (values[i]) sqlite3_value_free(values[i]);
            }
            sqlite3_free(values);
            values = NULL;
        }

        if (!r->errored) {
            r->values = values;
            r->n_rows = count;
        }
    }
    return NULL;
}

/* ========== TVF virtual table structures ========== */

/* The vtab struct for the eponymous module */
struct clearprism_tvf_vtab {
    sqlite3_vtab base;
};

/* TVF cursor: holds materialized result buffer */
struct clearprism_tvf_cursor {
    sqlite3_vtab_cursor base;
    /* Flat result buffer: total_rows * (n_result_cols + 1 for _source_db) */
    sqlite3_value **all_values;    /* data columns per row, deep copies */
    char **source_aliases;         /* alias string per row (not owned, points into results) */
    char **owned_aliases;          /* alias strings we own (one per source) */
    int n_owned_aliases;
    int total_rows;
    int n_result_cols;             /* columns from the inner query (NOT including _source_db) */
    int current_row;
};

/* ========== xConnect ========== */

/*
 * Declare a wide schema with generic column names.
 * The first two columns are HIDDEN arguments (vtab_name, sql_template).
 * Then c0..c15 as data columns, and _source_db as the last column.
 *
 * Column indices:
 *   0 = vtab_name (HIDDEN)
 *   1 = sql_template (HIDDEN)
 *   2 = c0
 *   3 = c1
 *   ...
 *   17 = c15
 *   18 = _source_db
 */
static int tvf_connect(sqlite3 *db, void *pAux, int argc,
                        const char *const *argv, sqlite3_vtab **ppVtab,
                        char **pzErr)
{
    (void)pAux; (void)argc; (void)argv; (void)pzErr;

    /* Build the CREATE TABLE statement */
    char schema[2048];
    int off = snprintf(schema, sizeof(schema),
        "CREATE TABLE x(vtab_name HIDDEN, sql_template HIDDEN");
    for (int i = 0; i < TVF_MAX_DATA_COLS; i++) {
        off += snprintf(schema + off, sizeof(schema) - off, ", c%d", i);
    }
    off += snprintf(schema + off, sizeof(schema) - off, ", _source_db TEXT)");

    int rc = sqlite3_declare_vtab(db, schema);
    if (rc != SQLITE_OK) return rc;

    struct clearprism_tvf_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
    if (!vtab) return SQLITE_NOMEM;
    memset(vtab, 0, sizeof(*vtab));

    *ppVtab = &vtab->base;
    return SQLITE_OK;
}

/* ========== xDisconnect ========== */

static int tvf_disconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

/* ========== xOpen ========== */

static int tvf_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
    (void)pVtab;
    struct clearprism_tvf_cursor *cur = sqlite3_malloc(sizeof(*cur));
    if (!cur) return SQLITE_NOMEM;
    memset(cur, 0, sizeof(*cur));
    *ppCursor = &cur->base;
    return SQLITE_OK;
}

/* ========== xClose ========== */

static void tvf_cursor_free_data(struct clearprism_tvf_cursor *cur)
{
    if (cur->all_values) {
        int64_t total = (int64_t)cur->total_rows * cur->n_result_cols;
        for (int64_t i = 0; i < total; i++) {
            if (cur->all_values[i])
                sqlite3_value_free(cur->all_values[i]);
        }
        sqlite3_free(cur->all_values);
        cur->all_values = NULL;
    }
    if (cur->source_aliases) {
        sqlite3_free(cur->source_aliases);
        cur->source_aliases = NULL;
    }
    if (cur->owned_aliases) {
        for (int i = 0; i < cur->n_owned_aliases; i++)
            sqlite3_free(cur->owned_aliases[i]);
        sqlite3_free(cur->owned_aliases);
        cur->owned_aliases = NULL;
    }
    cur->total_rows = 0;
    cur->n_result_cols = 0;
    cur->current_row = 0;
}

static int tvf_close(sqlite3_vtab_cursor *pCursor)
{
    struct clearprism_tvf_cursor *cur = (struct clearprism_tvf_cursor *)pCursor;
    tvf_cursor_free_data(cur);
    sqlite3_free(cur);
    return SQLITE_OK;
}

/* ========== xBestIndex ========== */

/*
 * We expect exactly 2 EQ constraints on columns 0 (vtab_name) and 1
 * (sql_template).  Pass them through as argv[1] and argv[2].
 */
static int tvf_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *info)
{
    (void)pVtab;

    int vtab_name_idx = -1;
    int sql_template_idx = -1;

    for (int i = 0; i < info->nConstraint; i++) {
        if (!info->aConstraint[i].usable) continue;
        if (info->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
        if (info->aConstraint[i].iColumn == 0) vtab_name_idx = i;
        if (info->aConstraint[i].iColumn == 1) sql_template_idx = i;
    }

    if (vtab_name_idx < 0 || sql_template_idx < 0) {
        /* Both arguments are required */
        return SQLITE_CONSTRAINT;
    }

    info->aConstraintUsage[vtab_name_idx].argvIndex = 1;
    info->aConstraintUsage[vtab_name_idx].omit = 1;
    info->aConstraintUsage[sql_template_idx].argvIndex = 2;
    info->aConstraintUsage[sql_template_idx].omit = 1;

    info->estimatedCost = 1000000.0;
    info->estimatedRows = 1000;

    return SQLITE_OK;
}

/* ========== xFilter ========== */

/*
 * This is where the real work happens:
 * 1. Look up the clearprism_vtab by name from the global registry
 * 2. Get source list from its registry
 * 3. Execute the SQL template on each source in parallel
 * 4. Collect all results into the cursor's flat buffer
 */
static int tvf_filter(sqlite3_vtab_cursor *pCursor, int idxNum,
                       const char *idxStr, int argc, sqlite3_value **argv)
{
    (void)idxNum; (void)idxStr;

    struct clearprism_tvf_cursor *cur = (struct clearprism_tvf_cursor *)pCursor;

    /* Free any previous result data */
    tvf_cursor_free_data(cur);

    if (argc < 2) {
        pCursor->pVtab->zErrMsg = sqlite3_mprintf(
            "clearprism_query requires 2 arguments: vtab_name and sql_template");
        return SQLITE_ERROR;
    }

    const char *vtab_name = (const char *)sqlite3_value_text(argv[0]);
    const char *sql_template = (const char *)sqlite3_value_text(argv[1]);

    if (!vtab_name || !sql_template) {
        pCursor->pVtab->zErrMsg = sqlite3_mprintf(
            "clearprism_query: vtab_name and sql_template must not be NULL");
        return SQLITE_ERROR;
    }

    /* Look up the clearprism vtab from the global registry */
    clearprism_vtab *vtab = clearprism_lookup_vtab(vtab_name);
    /* NOTE: Same pattern as clearprism_agg.c -- vtab pointer is used after
       g_vtab_map_lock is released. This is safe in practice because vtab
       lifetime is tied to the parent connection, and TVF queries run on
       the same connection. A future refactor could add reference counting. */
    if (!vtab) {
        pCursor->pVtab->zErrMsg = sqlite3_mprintf(
            "clearprism_query: virtual table '%s' not found in registry", vtab_name);
        return SQLITE_ERROR;
    }

    /* Get source list */
    clearprism_source *sources = NULL;
    int n_sources = 0;
    char *snap_err = NULL;
    int rc = clearprism_registry_snapshot(vtab->registry, vtab->target_table,
                                           &sources, &n_sources, &snap_err);
    sqlite3_free(snap_err);
    if (rc != SQLITE_OK) {
        pCursor->pVtab->zErrMsg = sqlite3_mprintf(
            "clearprism_query: failed to get source list");
        return SQLITE_ERROR;
    }

    if (n_sources == 0) {
        /* No sources — return empty result set */
        clearprism_sources_free(sources, n_sources);
        cur->total_rows = 0;
        cur->current_row = 0;
        cur->n_result_cols = 0;
        return SQLITE_OK;
    }

    /* Ensure pool can hold all connections */
    if (vtab->pool && n_sources > vtab->pool->max_open) {
        pthread_mutex_lock(&vtab->pool->lock);
        vtab->pool->max_open = n_sources;
        pthread_mutex_unlock(&vtab->pool->lock);
    }

    /* Prepare worker context */
    struct tvf_worker_ctx wctx;
    memset(&wctx, 0, sizeof(wctx));
    wctx.pool = vtab->pool;
    wctx.sources = sources;
    wctx.n_sources = n_sources;
    wctx.next_source = 0;
    wctx.sql_template = sql_template;
    wctx.target_table = vtab->target_table;
    wctx.results = sqlite3_malloc(n_sources * (int)sizeof(struct tvf_source_result));
    if (!wctx.results) {
        clearprism_sources_free(sources, n_sources);
        return SQLITE_NOMEM;
    }
    memset(wctx.results, 0, n_sources * (int)sizeof(struct tvf_source_result));

    /* Launch workers */
    int n_threads = n_sources;
    if (n_threads > CLEARPRISM_MAX_PREPARE_THREADS)
        n_threads = CLEARPRISM_MAX_PREPARE_THREADS;
    if (n_threads < 1) n_threads = 1;

    if (n_threads == 1) {
        tvf_worker(&wctx);
    } else {
        pthread_t *threads = sqlite3_malloc(n_threads * (int)sizeof(pthread_t));
        if (threads) {
            for (int i = 0; i < n_threads; i++)
                pthread_create(&threads[i], NULL, tvf_worker, &wctx);
            for (int i = 0; i < n_threads; i++)
                pthread_join(threads[i], NULL);
            sqlite3_free(threads);
        } else {
            tvf_worker(&wctx);
        }
    }

    /* Count total rows and determine n_result_cols */
    int total_rows = 0;
    int n_result_cols = 0;
    for (int i = 0; i < n_sources; i++) {
        if (!wctx.results[i].errored) {
            total_rows += wctx.results[i].n_rows;
            if (wctx.results[i].n_cols > n_result_cols)
                n_result_cols = wctx.results[i].n_cols;
        }
    }

    /* Cap at TVF_MAX_DATA_COLS */
    if (n_result_cols > TVF_MAX_DATA_COLS)
        n_result_cols = TVF_MAX_DATA_COLS;

    cur->n_result_cols = n_result_cols;
    cur->total_rows = total_rows;
    cur->current_row = 0;

    if (total_rows == 0) {
        /* Free worker results */
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].values) {
                int n = wctx.results[i].n_rows * wctx.results[i].n_cols;
                for (int j = 0; j < n; j++) {
                    if (wctx.results[i].values[j])
                        sqlite3_value_free(wctx.results[i].values[j]);
                }
                sqlite3_free(wctx.results[i].values);
            }
        }
        sqlite3_free(wctx.results);
        clearprism_sources_free(sources, n_sources);
        return SQLITE_OK;
    }

    /* Allocate flat result buffer */
    cur->all_values = sqlite3_malloc64(
        (sqlite3_uint64)total_rows * n_result_cols * sizeof(sqlite3_value *));
    cur->source_aliases = sqlite3_malloc64(
        (sqlite3_uint64)total_rows * sizeof(char *));

    if (!cur->all_values || !cur->source_aliases) {
        /* Cleanup on OOM */
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].values) {
                int n = wctx.results[i].n_rows * wctx.results[i].n_cols;
                for (int j = 0; j < n; j++) {
                    if (wctx.results[i].values[j])
                        sqlite3_value_free(wctx.results[i].values[j]);
                }
                sqlite3_free(wctx.results[i].values);
            }
        }
        sqlite3_free(wctx.results);
        clearprism_sources_free(sources, n_sources);
        tvf_cursor_free_data(cur);
        return SQLITE_NOMEM;
    }

    /* We need to own copies of the alias strings since sources will be freed */
    cur->owned_aliases = sqlite3_malloc(n_sources * (int)sizeof(char *));
    cur->n_owned_aliases = 0;
    if (!cur->owned_aliases && n_sources > 0) {
        /* OOM: clean up and return error */
        tvf_cursor_free_data(cur);
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].values) {
                int n = wctx.results[i].n_rows * wctx.results[i].n_cols;
                for (int j = 0; j < n; j++) {
                    if (wctx.results[i].values[j])
                        sqlite3_value_free(wctx.results[i].values[j]);
                }
                sqlite3_free(wctx.results[i].values);
            }
        }
        sqlite3_free(wctx.results);
        clearprism_sources_free(sources, n_sources);
        return SQLITE_NOMEM;
    }
    if (cur->owned_aliases) {
        for (int i = 0; i < n_sources; i++) {
            cur->owned_aliases[i] = clearprism_strdup(sources[i].alias);
            cur->n_owned_aliases++;
        }
    }

    /* Copy results into flat buffer, transferring ownership of values */
    int row_out = 0;
    for (int s = 0; s < n_sources; s++) {
        struct tvf_source_result *r = &wctx.results[s];
        if (r->errored) continue;

        char *alias = (cur->owned_aliases && s < cur->n_owned_aliases)
                      ? cur->owned_aliases[s] : NULL;

        for (int row = 0; row < r->n_rows; row++) {
            /* Copy data columns (transfer ownership) */
            for (int c = 0; c < n_result_cols; c++) {
                if (c < r->n_cols) {
                    cur->all_values[row_out * n_result_cols + c] =
                        r->values[row * r->n_cols + c];
                    r->values[row * r->n_cols + c] = NULL;  /* transferred */
                } else {
                    cur->all_values[row_out * n_result_cols + c] = NULL;
                }
            }
            /* Free any extra columns beyond n_result_cols */
            for (int c = n_result_cols; c < r->n_cols; c++) {
                if (r->values[row * r->n_cols + c]) {
                    sqlite3_value_free(r->values[row * r->n_cols + c]);
                    r->values[row * r->n_cols + c] = NULL;
                }
            }
            cur->source_aliases[row_out] = alias;
            row_out++;
        }

        /* Free the per-source values array (values already transferred/freed) */
        sqlite3_free(r->values);
        r->values = NULL;
    }

    cur->total_rows = row_out;  /* actual count after skipping errored sources */

    sqlite3_free(wctx.results);
    clearprism_sources_free(sources, n_sources);

    return SQLITE_OK;
}

/* ========== xNext ========== */

static int tvf_next(sqlite3_vtab_cursor *pCursor)
{
    struct clearprism_tvf_cursor *cur = (struct clearprism_tvf_cursor *)pCursor;
    cur->current_row++;
    return SQLITE_OK;
}

/* ========== xEof ========== */

static int tvf_eof(sqlite3_vtab_cursor *pCursor)
{
    struct clearprism_tvf_cursor *cur = (struct clearprism_tvf_cursor *)pCursor;
    return cur->current_row >= cur->total_rows;
}

/* ========== xColumn ========== */

/*
 * Column layout:
 *   0 = vtab_name (HIDDEN) - return NULL
 *   1 = sql_template (HIDDEN) - return NULL
 *   2..17 = c0..c15 (data columns)
 *   18 = _source_db
 */
static int tvf_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx,
                       int iCol)
{
    struct clearprism_tvf_cursor *cur = (struct clearprism_tvf_cursor *)pCursor;

    if (iCol == 0 || iCol == 1) {
        /* HIDDEN argument columns */
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    if (iCol == 2 + TVF_MAX_DATA_COLS) {
        /* _source_db column */
        if (cur->current_row < cur->total_rows &&
            cur->source_aliases &&
            cur->source_aliases[cur->current_row]) {
            sqlite3_result_text(ctx, cur->source_aliases[cur->current_row],
                                -1, SQLITE_TRANSIENT);
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    /* Data column: c0..c15 mapped to iCol - 2 */
    int data_idx = iCol - 2;
    if (data_idx < 0 || data_idx >= cur->n_result_cols) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }

    sqlite3_value *val = cur->all_values[cur->current_row * cur->n_result_cols + data_idx];
    if (val) {
        sqlite3_result_value(ctx, val);
    } else {
        sqlite3_result_null(ctx);
    }

    return SQLITE_OK;
}

/* ========== xRowid ========== */

static int tvf_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
{
    struct clearprism_tvf_cursor *cur = (struct clearprism_tvf_cursor *)pCursor;
    *pRowid = cur->current_row;
    return SQLITE_OK;
}

/* ========== Module definition ========== */

static sqlite3_module clearprism_tvf_module = {
    0,                /* iVersion */
    0,                /* xCreate - NULL for eponymous-only */
    tvf_connect,      /* xConnect */
    tvf_best_index,   /* xBestIndex */
    tvf_disconnect,   /* xDisconnect */
    0,                /* xDestroy */
    tvf_open,         /* xOpen */
    tvf_close,        /* xClose */
    tvf_filter,       /* xFilter */
    tvf_next,         /* xNext */
    tvf_eof,          /* xEof */
    tvf_column,       /* xColumn */
    tvf_rowid,        /* xRowid */
    0,                /* xUpdate */
    0,                /* xBegin */
    0,                /* xSync */
    0,                /* xCommit */
    0,                /* xRollback */
    0,                /* xFindFunction */
    0,                /* xRename */
    0,                /* xSavepoint */
    0,                /* xRelease */
    0,                /* xRollbackTo */
    0,                /* xShadowName */
#if SQLITE_VERSION_NUMBER >= 3044000
    0,                /* xIntegrity */
#endif
};

/* ========== Registration ========== */

int clearprism_register_tvf(sqlite3 *db)
{
    return sqlite3_create_module(db, "clearprism_query",
                                  &clearprism_tvf_module, NULL);
}

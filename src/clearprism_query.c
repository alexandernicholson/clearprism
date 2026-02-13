/*
 * clearprism_query.c — xBestIndex, xFilter, xNext, xEof, xColumn, xRowid
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

/* Internal helpers */
static int cursor_advance_source(clearprism_cursor *cur);
static int cursor_prepare_source(clearprism_cursor *cur, int source_idx,
                                  int argc, sqlite3_value **argv);
static void cursor_build_cache_key(clearprism_cursor *cur, int argc,
                                    sqlite3_value **argv, char *buf, int buf_size);

int clearprism_vtab_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *info)
{
    clearprism_vtab *vtab = (clearprism_vtab *)pVtab;
    int flags = 0;

    char *idx_str = clearprism_where_encode(info, vtab->nCol, &flags);
    if (!idx_str) {
        idx_str = sqlite3_mprintf("");
    }

    info->idxNum = flags;
    info->idxStr = idx_str;
    info->needToFreeIdxStr = 1;

    return SQLITE_OK;
}

int clearprism_vtab_filter(sqlite3_vtab_cursor *pCur, int idxNum,
                            const char *idxStr, int argc, sqlite3_value **argv)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;
    clearprism_vtab *vtab = cur->vtab;

    /* Clean up previous iteration state */
    if (cur->current_stmt) {
        sqlite3_finalize(cur->current_stmt);
        cur->current_stmt = NULL;
    }
    if (cur->current_conn && cur->sources &&
        cur->current_source_idx < cur->n_sources) {
        clearprism_connpool_checkin(vtab->pool,
                                    cur->sources[cur->current_source_idx].path);
        cur->current_conn = NULL;
    }
    clearprism_query_plan_clear(&cur->plan);
    clearprism_sources_free(cur->sources, cur->n_sources);
    cur->sources = NULL;
    cur->n_sources = 0;
    cur->current_source_idx = 0;
    cur->row_counter = 0;
    cur->eof = 0;
    cur->serving_from_cache = 0;

    /* Free saved argv from previous iteration */
    if (cur->saved_argv) {
        for (int i = 0; i < cur->saved_argc; i++) {
            sqlite3_value_free(cur->saved_argv[i]);
        }
        sqlite3_free(cur->saved_argv);
        cur->saved_argv = NULL;
        cur->saved_argc = 0;
    }

    if (cur->cache_cursor) {
        clearprism_cache_cursor_free(cur->cache_cursor);
        cur->cache_cursor = NULL;
    }

    /* Decode plan */
    cur->plan.flags = idxNum;
    int rc = clearprism_where_decode(idxStr, &cur->plan);
    if (rc != SQLITE_OK) {
        cur->eof = 1;
        return rc;
    }

    /* If source-constrained, extract the source alias from argv */
    if (idxNum & CLEARPRISM_PLAN_SOURCE_CONSTRAINED) {
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            if (cur->plan.constraints[i].col_idx == vtab->nCol &&
                cur->plan.constraints[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
                int ai = cur->plan.constraints[i].argv_idx - 1;
                if (ai >= 0 && ai < argc) {
                    const char *alias = (const char *)sqlite3_value_text(argv[ai]);
                    if (alias) {
                        cur->plan.source_alias = clearprism_strdup(alias);
                    }
                }
                break;
            }
        }
    }

    /* Save argv for re-binding on subsequent sources */
    if (argc > 0 && argv) {
        cur->saved_argc = argc;
        cur->saved_argv = sqlite3_malloc(argc * (int)sizeof(sqlite3_value *));
        if (cur->saved_argv) {
            for (int i = 0; i < argc; i++) {
                cur->saved_argv[i] = sqlite3_value_dup(argv[i]);
            }
        }
    }

    /* Build cache key and try cache lookup */
    char cache_key[512];
    cursor_build_cache_key(cur, argc, argv, cache_key, sizeof(cache_key));

    if (vtab->cache) {
        clearprism_cache_cursor *cc = NULL;
        int cache_hit = clearprism_cache_lookup(vtab->cache, cache_key, &cc);
        if (cache_hit && cc) {
            cur->cache_cursor = cc;
            cur->serving_from_cache = 1;
            if (clearprism_cache_cursor_eof(cc)) {
                cur->eof = 1;
            }
            return SQLITE_OK;
        }
    }

    /* Get source snapshot */
    char *snap_err = NULL;
    rc = clearprism_registry_snapshot(vtab->registry, vtab->target_table,
                                       &cur->sources, &cur->n_sources, &snap_err);
    if (rc != SQLITE_OK) {
        clearprism_set_errmsg(&vtab->base, "%s", snap_err ? snap_err : "snapshot failed");
        sqlite3_free(snap_err);
        cur->eof = 1;
        return SQLITE_ERROR;
    }

    /* If source-constrained, filter to matching source */
    if (cur->plan.source_alias) {
        int found = 0;
        for (int i = 0; i < cur->n_sources; i++) {
            if (strcmp(cur->sources[i].alias, cur->plan.source_alias) == 0) {
                /* Swap to position 0 */
                if (i != 0) {
                    clearprism_source tmp = cur->sources[0];
                    cur->sources[0] = cur->sources[i];
                    cur->sources[i] = tmp;
                }
                /* Only iterate this one source */
                cur->n_sources = 1;
                found = 1;
                break;
            }
        }
        if (!found) {
            cur->n_sources = 0;
        }
    }

    if (cur->n_sources == 0) {
        cur->eof = 1;
        return SQLITE_OK;
    }

    /* Prepare first source */
    rc = cursor_prepare_source(cur, 0, argc, argv);
    if (rc != SQLITE_OK) {
        /* Try next source */
        cur->current_source_idx = 0;
        rc = cursor_advance_source(cur);
        if (rc != SQLITE_OK) {
            cur->eof = 1;
        }
    } else {
        /* Step to first row */
        rc = sqlite3_step(cur->current_stmt);
        if (rc == SQLITE_ROW) {
            /* Have a row */
        } else if (rc == SQLITE_DONE) {
            /* Empty source, try next */
            rc = cursor_advance_source(cur);
            if (rc != SQLITE_OK && rc != SQLITE_DONE) {
                cur->eof = 1;
            }
        } else {
            /* Error on first source, try next */
            rc = cursor_advance_source(cur);
            if (rc != SQLITE_OK && rc != SQLITE_DONE) {
                cur->eof = 1;
            }
        }
    }

    return SQLITE_OK;
}

int clearprism_vtab_next(sqlite3_vtab_cursor *pCur)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;

    cur->row_counter++;

    /* Cache serving path */
    if (cur->serving_from_cache && cur->cache_cursor) {
        clearprism_cache_cursor_next(cur->cache_cursor);
        if (clearprism_cache_cursor_eof(cur->cache_cursor)) {
            cur->eof = 1;
        }
        return SQLITE_OK;
    }

    /* Live query path */
    if (!cur->current_stmt) {
        cur->eof = 1;
        return SQLITE_OK;
    }

    int rc = sqlite3_step(cur->current_stmt);
    if (rc == SQLITE_ROW) {
        return SQLITE_OK;
    }

    /* Current source exhausted or errored — advance to next */
    rc = cursor_advance_source(cur);
    if (rc != SQLITE_OK) {
        cur->eof = 1;
    }
    return SQLITE_OK;
}

int clearprism_vtab_eof(sqlite3_vtab_cursor *pCur)
{
    return ((clearprism_cursor *)pCur)->eof;
}

int clearprism_vtab_column(sqlite3_vtab_cursor *pCur, sqlite3_context *ctx,
                            int iCol)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;
    clearprism_vtab *vtab = cur->vtab;

    /* Cache serving path */
    if (cur->serving_from_cache && cur->cache_cursor) {
        sqlite3_value *val = clearprism_cache_cursor_value(cur->cache_cursor, iCol);
        if (val) {
            sqlite3_result_value(ctx, val);
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    /* Hidden _source_db column */
    if (iCol == vtab->nCol) {
        const char *alias = NULL;
        if (cur->current_source_idx < cur->n_sources) {
            alias = cur->sources[cur->current_source_idx].alias;
        }
        if (alias) {
            sqlite3_result_text(ctx, alias, -1, SQLITE_TRANSIENT);
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    /* Real columns */
    if (cur->current_stmt && iCol >= 0 && iCol < vtab->nCol) {
        sqlite3_result_value(ctx, sqlite3_column_value(cur->current_stmt, iCol));
    } else {
        sqlite3_result_null(ctx);
    }

    return SQLITE_OK;
}

int clearprism_vtab_rowid(sqlite3_vtab_cursor *pCur, sqlite3_int64 *pRowid)
{
    *pRowid = ((clearprism_cursor *)pCur)->row_counter;
    return SQLITE_OK;
}

/* ---------- Internal helpers ---------- */

/*
 * Advance to the next source that has rows.
 * Returns SQLITE_OK if a row is available, or SQLITE_DONE if all sources exhausted.
 */
static int cursor_advance_source(clearprism_cursor *cur)
{
    clearprism_vtab *vtab = cur->vtab;

    while (1) {
        /* Finalize current statement and check in connection */
        if (cur->current_stmt) {
            sqlite3_finalize(cur->current_stmt);
            cur->current_stmt = NULL;
        }
        if (cur->current_conn && cur->current_source_idx < cur->n_sources) {
            clearprism_connpool_checkin(vtab->pool,
                                        cur->sources[cur->current_source_idx].path);
            cur->current_conn = NULL;
        }

        cur->current_source_idx++;
        if (cur->current_source_idx >= cur->n_sources) {
            cur->eof = 1;
            return SQLITE_DONE;
        }

        /* Prepare next source with saved argv for parameter re-binding */
        int rc = cursor_prepare_source(cur, cur->current_source_idx,
                                        cur->saved_argc, cur->saved_argv);
        if (rc != SQLITE_OK) {
            /* Skip errored source, try next */
            continue;
        }

        rc = sqlite3_step(cur->current_stmt);
        if (rc == SQLITE_ROW) {
            return SQLITE_OK;
        } else if (rc == SQLITE_DONE) {
            /* Empty source, continue to next */
            continue;
        } else {
            /* Error, skip this source */
            continue;
        }
    }
}

/*
 * Prepare a SELECT statement for the given source index.
 * Binds constraint values from the plan.
 */
static int cursor_prepare_source(clearprism_cursor *cur, int source_idx,
                                  int argc, sqlite3_value **argv)
{
    clearprism_vtab *vtab = cur->vtab;

    if (source_idx >= cur->n_sources) return SQLITE_ERROR;

    /* Checkout connection from pool */
    char *pool_err = NULL;
    sqlite3 *conn = clearprism_connpool_checkout(vtab->pool,
                                                   cur->sources[source_idx].path,
                                                   cur->sources[source_idx].alias,
                                                   &pool_err);
    if (!conn) {
        sqlite3_free(pool_err);
        return SQLITE_ERROR;
    }
    cur->current_conn = conn;
    cur->current_source_idx = source_idx;

    /* Generate SQL */
    char *sql = clearprism_where_generate_sql(vtab->target_table,
                                               vtab->cols, vtab->nCol,
                                               &cur->plan);
    if (!sql) {
        clearprism_connpool_checkin(vtab->pool, cur->sources[source_idx].path);
        cur->current_conn = NULL;
        return SQLITE_NOMEM;
    }

    /* Prepare statement */
    int rc = sqlite3_prepare_v2(conn, sql, -1, &cur->current_stmt, NULL);
    sqlite3_free(sql);

    if (rc != SQLITE_OK) {
        clearprism_connpool_checkin(vtab->pool, cur->sources[source_idx].path);
        cur->current_conn = NULL;
        return rc;
    }

    /* Bind parameters from argv. We need to map plan constraints to bind positions.
       The SQL has ? placeholders for non-_source_db constraints, in order. */
    if (argc > 0 && argv) {
        int bind_idx = 1;
        for (int i = 0; i < cur->plan.n_constraints; i++) {
            int col = cur->plan.constraints[i].col_idx;
            /* Skip _source_db constraint — not in the SQL */
            if (col == vtab->nCol) continue;
            if (col < 0 || col >= vtab->nCol) continue;

            int ai = cur->plan.constraints[i].argv_idx - 1;
            if (ai >= 0 && ai < argc) {
                sqlite3_bind_value(cur->current_stmt, bind_idx, argv[ai]);
            }
            bind_idx++;
        }
        /* Store argv values for re-binding on subsequent sources */
        /* We save the original argv in the cursor for reuse */
    }

    return SQLITE_OK;
}

static void cursor_build_cache_key(clearprism_cursor *cur, int argc,
                                    sqlite3_value **argv,
                                    char *buf, int buf_size)
{
    clearprism_vtab *vtab = cur->vtab;
    int pos = 0;

    pos += snprintf(buf + pos, buf_size - pos, "%s:", vtab->target_table);

    if (cur->plan.source_alias) {
        pos += snprintf(buf + pos, buf_size - pos, "src=%s:", cur->plan.source_alias);
    }

    for (int i = 0; i < argc && i < cur->plan.n_constraints; i++) {
        int ai = cur->plan.constraints[i].argv_idx - 1;
        if (ai >= 0 && ai < argc && argv[ai]) {
            const char *txt = (const char *)sqlite3_value_text(argv[ai]);
            if (txt) {
                pos += snprintf(buf + pos, buf_size - pos, "p%d=%s:", i, txt);
            }
        }
    }
}

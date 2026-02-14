/*
 * clearprism_agg.c — Aggregate pushdown via custom SQL functions
 *
 * Registers clearprism_count, clearprism_sum, clearprism_avg,
 * clearprism_min, clearprism_max as SQL scalar functions that
 * push aggregate computation to each source database in parallel
 * and combine the results.
 *
 * Usage:
 *   SELECT clearprism_count('table_name', 'where_clause_or_null');
 *   SELECT clearprism_sum('table_name', 'column', 'where_clause_or_null');
 *   SELECT clearprism_avg('table_name', 'column', 'where_clause_or_null');
 *   SELECT clearprism_min('table_name', 'column', 'where_clause_or_null');
 *   SELECT clearprism_max('table_name', 'column', 'where_clause_or_null');
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

/* ========== Global vtab registry ========== */

#define VTAB_MAP_MAX 64

static pthread_mutex_t g_vtab_map_lock = PTHREAD_MUTEX_INITIALIZER;
static struct {
    char *table;
    clearprism_vtab *vtab;
} g_vtab_map[VTAB_MAP_MAX];
static int g_vtab_map_size = 0;

void clearprism_register_vtab(const char *table, clearprism_vtab *vtab)
{
    pthread_mutex_lock(&g_vtab_map_lock);
    /* Check for existing entry */
    for (int i = 0; i < g_vtab_map_size; i++) {
        if (strcmp(g_vtab_map[i].table, table) == 0) {
            g_vtab_map[i].vtab = vtab;
            pthread_mutex_unlock(&g_vtab_map_lock);
            return;
        }
    }
    /* Add new entry */
    if (g_vtab_map_size < VTAB_MAP_MAX) {
        g_vtab_map[g_vtab_map_size].table = clearprism_strdup(table);
        g_vtab_map[g_vtab_map_size].vtab = vtab;
        g_vtab_map_size++;
    }
    pthread_mutex_unlock(&g_vtab_map_lock);
}

void clearprism_unregister_vtab(const char *table)
{
    pthread_mutex_lock(&g_vtab_map_lock);
    for (int i = 0; i < g_vtab_map_size; i++) {
        if (strcmp(g_vtab_map[i].table, table) == 0) {
            sqlite3_free(g_vtab_map[i].table);
            g_vtab_map[i] = g_vtab_map[--g_vtab_map_size];
            break;
        }
    }
    pthread_mutex_unlock(&g_vtab_map_lock);
}

clearprism_vtab *clearprism_lookup_vtab(const char *table)
{
    clearprism_vtab *result = NULL;
    pthread_mutex_lock(&g_vtab_map_lock);
    for (int i = 0; i < g_vtab_map_size; i++) {
        if (strcmp(g_vtab_map[i].table, table) == 0) {
            result = g_vtab_map[i].vtab;
            break;
        }
    }
    pthread_mutex_unlock(&g_vtab_map_lock);
    return result;
}

/* ========== Parallel aggregate infrastructure ========== */

enum agg_type {
    AGG_COUNT,
    AGG_SUM,
    AGG_AVG,
    AGG_MIN,
    AGG_MAX
};

struct agg_source_result {
    double value;       /* sum, min, max, or count as double */
    int64_t count;      /* for AVG: count of non-null rows */
    int has_result;     /* 1 if this source returned a result */
    int is_null;        /* 1 if aggregate result was NULL */
};

struct agg_worker_ctx {
    clearprism_connpool *pool;
    clearprism_source *sources;
    int n_sources;
    int next_source;            /* atomic */
    char *sql;                  /* query to run on each source */
    struct agg_source_result *results;
    enum agg_type type;
};

static void *agg_worker(void *arg)
{
    struct agg_worker_ctx *ctx = (struct agg_worker_ctx *)arg;
    while (1) {
        int idx = __sync_fetch_and_add(&ctx->next_source, 1);
        if (idx >= ctx->n_sources) break;

        struct agg_source_result *r = &ctx->results[idx];
        r->has_result = 0;
        r->is_null = 1;
        r->value = 0;
        r->count = 0;

        char *pool_err = NULL;
        sqlite3 *conn = clearprism_connpool_checkout(ctx->pool,
                            ctx->sources[idx].path,
                            ctx->sources[idx].alias, &pool_err);
        sqlite3_free(pool_err);
        if (!conn) continue;

        sqlite3_stmt *stmt = NULL;
        int rc = sqlite3_prepare_v2(conn, ctx->sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            clearprism_connpool_checkin(ctx->pool, ctx->sources[idx].path);
            continue;
        }

        rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            r->has_result = 1;
            if (ctx->type == AGG_AVG) {
                /* AVG uses: SELECT SUM(col), COUNT(col) */
                if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
                    r->value = sqlite3_column_double(stmt, 0);
                    r->count = sqlite3_column_int64(stmt, 1);
                    r->is_null = 0;
                }
            } else if (ctx->type == AGG_COUNT) {
                r->count = sqlite3_column_int64(stmt, 0);
                r->value = (double)r->count;
                r->is_null = 0;
            } else {
                /* SUM, MIN, MAX */
                if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
                    r->value = sqlite3_column_double(stmt, 0);
                    r->is_null = 0;
                }
            }
        }

        sqlite3_finalize(stmt);
        clearprism_connpool_checkin(ctx->pool, ctx->sources[idx].path);
    }
    return NULL;
}

/*
 * Run an aggregate query across all sources in parallel.
 * Returns the combined result.
 */
static void run_parallel_agg(clearprism_vtab *vtab, const char *sql,
                              enum agg_type type, sqlite3_context *ctx)
{
    clearprism_source *sources = NULL;
    int n_sources = 0;
    char *snap_err = NULL;
    int rc = clearprism_registry_snapshot(vtab->registry, vtab->target_table,
                                           &sources, &n_sources, &snap_err);
    sqlite3_free(snap_err);
    if (rc != SQLITE_OK || n_sources == 0) {
        if (type == AGG_COUNT)
            sqlite3_result_int64(ctx, 0);
        else
            sqlite3_result_null(ctx);
        clearprism_sources_free(sources, n_sources);
        return;
    }

    /* Ensure pool can hold all connections */
    if (vtab->pool && n_sources > vtab->pool->max_open) {
        pthread_mutex_lock(&vtab->pool->lock);
        vtab->pool->max_open = n_sources;
        pthread_mutex_unlock(&vtab->pool->lock);
    }

    struct agg_worker_ctx wctx;
    memset(&wctx, 0, sizeof(wctx));
    wctx.pool = vtab->pool;
    wctx.sources = sources;
    wctx.n_sources = n_sources;
    wctx.next_source = 0;
    wctx.sql = (char *)sql;
    wctx.type = type;
    wctx.results = sqlite3_malloc(n_sources * (int)sizeof(struct agg_source_result));
    if (!wctx.results) {
        clearprism_sources_free(sources, n_sources);
        sqlite3_result_error_nomem(ctx);
        return;
    }
    memset(wctx.results, 0, n_sources * (int)sizeof(struct agg_source_result));

    /* Launch workers */
    int n_threads = n_sources;
    if (n_threads > CLEARPRISM_MAX_PREPARE_THREADS)
        n_threads = CLEARPRISM_MAX_PREPARE_THREADS;
    if (n_threads < 1) n_threads = 1;

    if (n_threads == 1) {
        agg_worker(&wctx);
    } else {
        pthread_t *threads = sqlite3_malloc(n_threads * (int)sizeof(pthread_t));
        if (threads) {
            for (int i = 0; i < n_threads; i++)
                pthread_create(&threads[i], NULL, agg_worker, &wctx);
            for (int i = 0; i < n_threads; i++)
                pthread_join(threads[i], NULL);
            sqlite3_free(threads);
        } else {
            agg_worker(&wctx);
        }
    }

    /* Combine results */
    int any_result = 0;
    switch (type) {
    case AGG_COUNT: {
        int64_t total = 0;
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].has_result)
                total += wctx.results[i].count;
        }
        sqlite3_result_int64(ctx, total);
        break;
    }
    case AGG_SUM: {
        double total = 0;
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].has_result && !wctx.results[i].is_null) {
                total += wctx.results[i].value;
                any_result = 1;
            }
        }
        if (any_result)
            sqlite3_result_double(ctx, total);
        else
            sqlite3_result_null(ctx);
        break;
    }
    case AGG_AVG: {
        double total_sum = 0;
        int64_t total_count = 0;
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].has_result && !wctx.results[i].is_null) {
                total_sum += wctx.results[i].value;
                total_count += wctx.results[i].count;
            }
        }
        if (total_count > 0)
            sqlite3_result_double(ctx, total_sum / (double)total_count);
        else
            sqlite3_result_null(ctx);
        break;
    }
    case AGG_MIN: {
        double min_val = 0;
        int found = 0;
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].has_result && !wctx.results[i].is_null) {
                if (!found || wctx.results[i].value < min_val)
                    min_val = wctx.results[i].value;
                found = 1;
            }
        }
        if (found)
            sqlite3_result_double(ctx, min_val);
        else
            sqlite3_result_null(ctx);
        break;
    }
    case AGG_MAX: {
        double max_val = 0;
        int found = 0;
        for (int i = 0; i < n_sources; i++) {
            if (wctx.results[i].has_result && !wctx.results[i].is_null) {
                if (!found || wctx.results[i].value > max_val)
                    max_val = wctx.results[i].value;
                found = 1;
            }
        }
        if (found)
            sqlite3_result_double(ctx, max_val);
        else
            sqlite3_result_null(ctx);
        break;
    }
    }

    sqlite3_free(wctx.results);
    clearprism_sources_free(sources, n_sources);
}

/* ========== SQL function implementations ========== */

/*
 * clearprism_count(table_name, where_clause_or_null)
 */
static void clearprism_count_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 1) {
        sqlite3_result_error(ctx, "clearprism_count requires at least 1 argument", -1);
        return;
    }

    const char *table = (const char *)sqlite3_value_text(argv[0]);
    if (!table) {
        sqlite3_result_error(ctx, "clearprism_count: table name must not be NULL", -1);
        return;
    }

    clearprism_vtab *vtab = clearprism_lookup_vtab(table);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_count: table not found in vtab registry", -1);
        return;
    }

    const char *where_clause = (argc >= 2 && sqlite3_value_type(argv[1]) != SQLITE_NULL)
        ? (const char *)sqlite3_value_text(argv[1]) : NULL;

    char *sql;
    if (where_clause)
        sql = clearprism_mprintf("SELECT COUNT(*) FROM \"%s\" WHERE %s",
                                  vtab->target_table, where_clause);
    else
        sql = clearprism_mprintf("SELECT COUNT(*) FROM \"%s\"", vtab->target_table);

    run_parallel_agg(vtab, sql, AGG_COUNT, ctx);
    sqlite3_free(sql);
}

/*
 * clearprism_sum(table_name, column, where_clause_or_null)
 */
static void clearprism_sum_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 2) {
        sqlite3_result_error(ctx, "clearprism_sum requires at least 2 arguments", -1);
        return;
    }

    const char *table = (const char *)sqlite3_value_text(argv[0]);
    const char *column = (const char *)sqlite3_value_text(argv[1]);
    if (!table || !column) {
        sqlite3_result_error(ctx, "clearprism_sum: table and column must not be NULL", -1);
        return;
    }

    clearprism_vtab *vtab = clearprism_lookup_vtab(table);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_sum: table not found in vtab registry", -1);
        return;
    }

    const char *where_clause = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL)
        ? (const char *)sqlite3_value_text(argv[2]) : NULL;

    char *sql;
    if (where_clause)
        sql = clearprism_mprintf("SELECT SUM(\"%s\") FROM \"%s\" WHERE %s",
                                  column, vtab->target_table, where_clause);
    else
        sql = clearprism_mprintf("SELECT SUM(\"%s\") FROM \"%s\"",
                                  column, vtab->target_table);

    run_parallel_agg(vtab, sql, AGG_SUM, ctx);
    sqlite3_free(sql);
}

/*
 * clearprism_avg(table_name, column, where_clause_or_null)
 */
static void clearprism_avg_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 2) {
        sqlite3_result_error(ctx, "clearprism_avg requires at least 2 arguments", -1);
        return;
    }

    const char *table = (const char *)sqlite3_value_text(argv[0]);
    const char *column = (const char *)sqlite3_value_text(argv[1]);
    if (!table || !column) {
        sqlite3_result_error(ctx, "clearprism_avg: table and column must not be NULL", -1);
        return;
    }

    clearprism_vtab *vtab = clearprism_lookup_vtab(table);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_avg: table not found in vtab registry", -1);
        return;
    }

    const char *where_clause = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL)
        ? (const char *)sqlite3_value_text(argv[2]) : NULL;

    char *sql;
    if (where_clause)
        sql = clearprism_mprintf(
            "SELECT SUM(\"%s\"), COUNT(\"%s\") FROM \"%s\" WHERE %s",
            column, column, vtab->target_table, where_clause);
    else
        sql = clearprism_mprintf(
            "SELECT SUM(\"%s\"), COUNT(\"%s\") FROM \"%s\"",
            column, column, vtab->target_table);

    run_parallel_agg(vtab, sql, AGG_AVG, ctx);
    sqlite3_free(sql);
}

/*
 * clearprism_min(table_name, column, where_clause_or_null)
 */
static void clearprism_min_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 2) {
        sqlite3_result_error(ctx, "clearprism_min requires at least 2 arguments", -1);
        return;
    }

    const char *table = (const char *)sqlite3_value_text(argv[0]);
    const char *column = (const char *)sqlite3_value_text(argv[1]);
    if (!table || !column) {
        sqlite3_result_error(ctx, "clearprism_min: table and column must not be NULL", -1);
        return;
    }

    clearprism_vtab *vtab = clearprism_lookup_vtab(table);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_min: table not found in vtab registry", -1);
        return;
    }

    const char *where_clause = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL)
        ? (const char *)sqlite3_value_text(argv[2]) : NULL;

    char *sql;
    if (where_clause)
        sql = clearprism_mprintf("SELECT MIN(\"%s\") FROM \"%s\" WHERE %s",
                                  column, vtab->target_table, where_clause);
    else
        sql = clearprism_mprintf("SELECT MIN(\"%s\") FROM \"%s\"",
                                  column, vtab->target_table);

    run_parallel_agg(vtab, sql, AGG_MIN, ctx);
    sqlite3_free(sql);
}

/*
 * clearprism_max(table_name, column, where_clause_or_null)
 */
static void clearprism_max_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 2) {
        sqlite3_result_error(ctx, "clearprism_max requires at least 2 arguments", -1);
        return;
    }

    const char *table = (const char *)sqlite3_value_text(argv[0]);
    const char *column = (const char *)sqlite3_value_text(argv[1]);
    if (!table || !column) {
        sqlite3_result_error(ctx, "clearprism_max: table and column must not be NULL", -1);
        return;
    }

    clearprism_vtab *vtab = clearprism_lookup_vtab(table);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_max: table not found in vtab registry", -1);
        return;
    }

    const char *where_clause = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL)
        ? (const char *)sqlite3_value_text(argv[2]) : NULL;

    char *sql;
    if (where_clause)
        sql = clearprism_mprintf("SELECT MAX(\"%s\") FROM \"%s\" WHERE %s",
                                  column, vtab->target_table, where_clause);
    else
        sql = clearprism_mprintf("SELECT MAX(\"%s\") FROM \"%s\"",
                                  column, vtab->target_table);

    run_parallel_agg(vtab, sql, AGG_MAX, ctx);
    sqlite3_free(sql);
}

/* ========== Registration ========== */

int clearprism_register_agg_functions(sqlite3 *db)
{
    int rc;

    rc = sqlite3_create_function(db, "clearprism_count", -1,
                                  SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                  NULL, clearprism_count_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_sum", -1,
                                  SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                  NULL, clearprism_sum_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_avg", -1,
                                  SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                  NULL, clearprism_avg_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_min", -1,
                                  SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                  NULL, clearprism_min_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_max", -1,
                                  SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                  NULL, clearprism_max_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    return SQLITE_OK;
}

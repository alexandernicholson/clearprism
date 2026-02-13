/*
 * clearprism_cache_l2.c — Shadow table materialization (background thread, WAL mode)
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

/* Internal helpers */
static int  l2_create_shadow_table(clearprism_l2_cache *l2, char **errmsg);
static int  l2_do_refresh(clearprism_l2_cache *l2);
static void *l2_refresh_thread_func(void *arg);

clearprism_l2_cache *clearprism_l2_create(const char *cache_db_path,
                                           const char *target_table,
                                           clearprism_col_def *cols, int nCol,
                                           int refresh_interval_sec,
                                           clearprism_registry *registry,
                                           clearprism_connpool *pool,
                                           char **errmsg)
{
    if (!cache_db_path || !target_table) {
        if (errmsg) *errmsg = clearprism_strdup("L2: missing cache_db or table");
        return NULL;
    }

    clearprism_l2_cache *l2 = sqlite3_malloc(sizeof(*l2));
    if (!l2) {
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return NULL;
    }
    memset(l2, 0, sizeof(*l2));
    pthread_mutex_init(&l2->lock, NULL);

    l2->cache_db_path = clearprism_strdup(cache_db_path);
    l2->target_table = clearprism_strdup(target_table);
    l2->shadow_table_name = clearprism_mprintf("_clearprism_cache_%s", target_table);
    l2->refresh_interval_sec = refresh_interval_sec;
    l2->registry = registry;
    l2->pool = pool;

    /* Copy column definitions */
    l2->n_cols = nCol;
    l2->cols = sqlite3_malloc(nCol * (int)sizeof(*l2->cols));
    if (!l2->cols) {
        clearprism_l2_destroy(l2);
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return NULL;
    }
    for (int i = 0; i < nCol; i++) {
        l2->cols[i].name = clearprism_strdup(cols[i].name);
        l2->cols[i].type = clearprism_strdup(cols[i].type);
        l2->cols[i].notnull = cols[i].notnull;
        l2->cols[i].pk = cols[i].pk;
    }

    /* Open reader connection (for query threads) */
    int rc = sqlite3_open_v2(cache_db_path, &l2->reader_db,
                              SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
                              NULL);
    if (rc != SQLITE_OK) {
        if (errmsg) *errmsg = clearprism_mprintf("L2: cannot open reader: %s",
                                                   sqlite3_errmsg(l2->reader_db));
        clearprism_l2_destroy(l2);
        return NULL;
    }

    /* Enable WAL mode on reader */
    sqlite3_exec(l2->reader_db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    sqlite3_busy_timeout(l2->reader_db, 5000);

    /* Open writer connection (for background refresh thread) */
    rc = sqlite3_open_v2(cache_db_path, &l2->writer_db,
                          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
                          NULL);
    if (rc != SQLITE_OK) {
        if (errmsg) *errmsg = clearprism_mprintf("L2: cannot open writer: %s",
                                                   sqlite3_errmsg(l2->writer_db));
        clearprism_l2_destroy(l2);
        return NULL;
    }
    sqlite3_exec(l2->writer_db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    sqlite3_busy_timeout(l2->writer_db, 10000);

    /* Create shadow table and meta table */
    rc = l2_create_shadow_table(l2, errmsg);
    if (rc != SQLITE_OK) {
        clearprism_l2_destroy(l2);
        return NULL;
    }

    return l2;
}

void clearprism_l2_destroy(clearprism_l2_cache *l2)
{
    if (!l2) return;

    /* Stop refresh thread */
    pthread_mutex_lock(&l2->lock);
    if (l2->running) {
        l2->running = 0;
        pthread_mutex_unlock(&l2->lock);
        pthread_join(l2->refresh_thread, NULL);
    } else {
        pthread_mutex_unlock(&l2->lock);
    }

    if (l2->reader_db) sqlite3_close(l2->reader_db);
    if (l2->writer_db) sqlite3_close(l2->writer_db);

    sqlite3_free(l2->cache_db_path);
    sqlite3_free(l2->target_table);
    sqlite3_free(l2->shadow_table_name);

    if (l2->cols) {
        for (int i = 0; i < l2->n_cols; i++) {
            sqlite3_free(l2->cols[i].name);
            sqlite3_free(l2->cols[i].type);
        }
        sqlite3_free(l2->cols);
    }

    pthread_mutex_destroy(&l2->lock);
    sqlite3_free(l2);
}

int clearprism_l2_start_refresh(clearprism_l2_cache *l2, char **errmsg)
{
    if (!l2) return SQLITE_ERROR;

    pthread_mutex_lock(&l2->lock);
    if (l2->running) {
        pthread_mutex_unlock(&l2->lock);
        return SQLITE_OK;  /* Already running */
    }
    l2->running = 1;
    pthread_mutex_unlock(&l2->lock);

    int rc = pthread_create(&l2->refresh_thread, NULL, l2_refresh_thread_func, l2);
    if (rc != 0) {
        l2->running = 0;
        if (errmsg) *errmsg = clearprism_strdup("L2: failed to create refresh thread");
        return SQLITE_ERROR;
    }
    pthread_detach(l2->refresh_thread);
    return SQLITE_OK;
}

sqlite3_stmt *clearprism_l2_query(clearprism_l2_cache *l2,
                                   const char *where_clause,
                                   const char *source_alias,
                                   char **errmsg)
{
    if (!l2 || !l2->reader_db) return NULL;

    /* Build SELECT from shadow table */
    char *sql;
    if (source_alias && where_clause && where_clause[0]) {
        sql = clearprism_mprintf(
            "SELECT * FROM \"%s\" WHERE _cp_source_alias = '%s' AND %s",
            l2->shadow_table_name, source_alias, where_clause);
    } else if (source_alias) {
        sql = clearprism_mprintf(
            "SELECT * FROM \"%s\" WHERE _cp_source_alias = '%s'",
            l2->shadow_table_name, source_alias);
    } else if (where_clause && where_clause[0]) {
        sql = clearprism_mprintf(
            "SELECT * FROM \"%s\" WHERE %s",
            l2->shadow_table_name, where_clause);
    } else {
        sql = clearprism_mprintf("SELECT * FROM \"%s\"", l2->shadow_table_name);
    }

    if (!sql) {
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return NULL;
    }

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(l2->reader_db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);

    if (rc != SQLITE_OK) {
        if (errmsg) *errmsg = clearprism_mprintf("L2 query prepare failed: %s",
                                                   sqlite3_errmsg(l2->reader_db));
        return NULL;
    }
    return stmt;
}

int clearprism_l2_is_fresh(clearprism_l2_cache *l2)
{
    if (!l2) return 0;
    pthread_mutex_lock(&l2->lock);
    time_t now = time(NULL);
    int fresh = (l2->last_refresh > 0 &&
                 (now - l2->last_refresh) < l2->refresh_interval_sec);
    pthread_mutex_unlock(&l2->lock);
    return fresh;
}

/* ---------- Internal helpers ---------- */

static int l2_create_shadow_table(clearprism_l2_cache *l2, char **errmsg)
{
    /* Build CREATE TABLE for shadow table */
    size_t sql_size = 512;
    for (int i = 0; i < l2->n_cols; i++) {
        sql_size += strlen(l2->cols[i].name) + strlen(l2->cols[i].type) + 32;
    }

    char *sql = sqlite3_malloc((int)sql_size);
    if (!sql) {
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return SQLITE_NOMEM;
    }

    int pos = snprintf(sql, sql_size,
                        "CREATE TABLE IF NOT EXISTS \"%s\" (",
                        l2->shadow_table_name);

    for (int i = 0; i < l2->n_cols; i++) {
        if (i > 0) pos += snprintf(sql + pos, sql_size - pos, ", ");
        pos += snprintf(sql + pos, sql_size - pos, "\"%s\"", l2->cols[i].name);
        if (l2->cols[i].type[0]) {
            pos += snprintf(sql + pos, sql_size - pos, " %s", l2->cols[i].type);
        }
    }
    pos += snprintf(sql + pos, sql_size - pos,
                     ", _cp_source_alias TEXT NOT NULL"
                     ", _cp_refreshed_at TEXT NOT NULL DEFAULT (datetime('now'))"
                     ")");

    char *exec_err = NULL;
    int rc = sqlite3_exec(l2->writer_db, sql, NULL, NULL, &exec_err);
    sqlite3_free(sql);

    if (rc != SQLITE_OK) {
        if (errmsg) {
            *errmsg = clearprism_mprintf("L2 create shadow table failed: %s",
                                          exec_err ? exec_err : "unknown");
        }
        sqlite3_free(exec_err);
        return rc;
    }

    /* Create meta table */
    rc = sqlite3_exec(l2->writer_db,
        "CREATE TABLE IF NOT EXISTS _clearprism_meta ("
        "  table_name TEXT PRIMARY KEY,"
        "  last_refresh TEXT"
        ")", NULL, NULL, &exec_err);
    if (rc != SQLITE_OK) {
        sqlite3_free(exec_err);
        /* Non-fatal */
    }

    /* Create index on _cp_source_alias */
    char *idx_sql = clearprism_mprintf(
        "CREATE INDEX IF NOT EXISTS idx_%s_source ON \"%s\"(_cp_source_alias)",
        l2->shadow_table_name, l2->shadow_table_name);
    sqlite3_exec(l2->writer_db, idx_sql, NULL, NULL, NULL);
    sqlite3_free(idx_sql);

    return SQLITE_OK;
}

static int l2_do_refresh(clearprism_l2_cache *l2)
{
    if (!l2 || !l2->writer_db || !l2->registry || !l2->pool) return SQLITE_ERROR;

    /* Get current sources */
    clearprism_source *sources = NULL;
    int n_sources = 0;
    char *snap_err = NULL;
    int rc = clearprism_registry_snapshot(l2->registry, l2->target_table,
                                           &sources, &n_sources, &snap_err);
    sqlite3_free(snap_err);
    if (rc != SQLITE_OK || n_sources == 0) {
        clearprism_sources_free(sources, n_sources);
        return rc == SQLITE_OK ? SQLITE_DONE : rc;
    }

    /* Begin transaction */
    sqlite3_exec(l2->writer_db, "BEGIN IMMEDIATE", NULL, NULL, NULL);

    /* Delete all existing cached rows */
    char *del_sql = clearprism_mprintf("DELETE FROM \"%s\"", l2->shadow_table_name);
    sqlite3_exec(l2->writer_db, del_sql, NULL, NULL, NULL);
    sqlite3_free(del_sql);

    /* Build INSERT statement */
    size_t ins_size = 256;
    for (int i = 0; i < l2->n_cols; i++) {
        ins_size += strlen(l2->cols[i].name) + 8;
    }
    char *ins_sql = sqlite3_malloc((int)ins_size);
    int pos = snprintf(ins_sql, ins_size, "INSERT INTO \"%s\" (", l2->shadow_table_name);

    for (int i = 0; i < l2->n_cols; i++) {
        if (i > 0) pos += snprintf(ins_sql + pos, ins_size - pos, ", ");
        pos += snprintf(ins_sql + pos, ins_size - pos, "\"%s\"", l2->cols[i].name);
    }
    pos += snprintf(ins_sql + pos, ins_size - pos, ", _cp_source_alias) VALUES (");
    for (int i = 0; i < l2->n_cols; i++) {
        if (i > 0) pos += snprintf(ins_sql + pos, ins_size - pos, ", ");
        pos += snprintf(ins_sql + pos, ins_size - pos, "?");
    }
    pos += snprintf(ins_sql + pos, ins_size - pos, ", ?)");

    sqlite3_stmt *ins_stmt = NULL;
    rc = sqlite3_prepare_v2(l2->writer_db, ins_sql, -1, &ins_stmt, NULL);
    sqlite3_free(ins_sql);
    if (rc != SQLITE_OK) {
        sqlite3_exec(l2->writer_db, "ROLLBACK", NULL, NULL, NULL);
        clearprism_sources_free(sources, n_sources);
        return rc;
    }

    /* Build SELECT for each source */
    size_t sel_size = 128;
    for (int i = 0; i < l2->n_cols; i++) {
        sel_size += strlen(l2->cols[i].name) + 8;
    }
    char *sel_sql = sqlite3_malloc((int)sel_size);
    pos = snprintf(sel_sql, sel_size, "SELECT ");
    for (int i = 0; i < l2->n_cols; i++) {
        if (i > 0) pos += snprintf(sel_sql + pos, sel_size - pos, ", ");
        pos += snprintf(sel_sql + pos, sel_size - pos, "\"%s\"", l2->cols[i].name);
    }
    pos += snprintf(sel_sql + pos, sel_size - pos, " FROM \"%s\"", l2->target_table);

    /* Iterate each source — use dedicated connections (not the shared pool)
       to avoid thread-safety issues with SQLITE_OPEN_NOMUTEX connections */
    for (int s = 0; s < n_sources; s++) {
        /* Check if still running */
        pthread_mutex_lock(&l2->lock);
        int still_running = l2->running;
        pthread_mutex_unlock(&l2->lock);
        if (!still_running) break;

        sqlite3 *src_conn = NULL;
        rc = sqlite3_open_v2(sources[s].path, &src_conn,
                              SQLITE_OPEN_READONLY, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(src_conn);
            continue;  /* Skip unavailable source */
        }
        sqlite3_busy_timeout(src_conn, 1000);

        sqlite3_stmt *src_stmt = NULL;
        rc = sqlite3_prepare_v2(src_conn, sel_sql, -1, &src_stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(src_conn);
            continue;  /* Skip errored source */
        }

        while (sqlite3_step(src_stmt) == SQLITE_ROW) {
            sqlite3_reset(ins_stmt);
            for (int c = 0; c < l2->n_cols; c++) {
                sqlite3_bind_value(ins_stmt, c + 1,
                                    sqlite3_column_value(src_stmt, c));
            }
            sqlite3_bind_text(ins_stmt, l2->n_cols + 1,
                               sources[s].alias, -1, SQLITE_TRANSIENT);
            sqlite3_step(ins_stmt);
        }

        sqlite3_finalize(src_stmt);
        sqlite3_close(src_conn);
    }

    sqlite3_finalize(ins_stmt);
    sqlite3_free(sel_sql);

    /* Update meta table */
    char *meta_sql = clearprism_mprintf(
        "INSERT OR REPLACE INTO _clearprism_meta (table_name, last_refresh) "
        "VALUES ('%s', datetime('now'))", l2->target_table);
    sqlite3_exec(l2->writer_db, meta_sql, NULL, NULL, NULL);
    sqlite3_free(meta_sql);

    sqlite3_exec(l2->writer_db, "COMMIT", NULL, NULL, NULL);

    pthread_mutex_lock(&l2->lock);
    l2->last_refresh = time(NULL);
    pthread_mutex_unlock(&l2->lock);

    clearprism_sources_free(sources, n_sources);
    return SQLITE_OK;
}

static void *l2_refresh_thread_func(void *arg)
{
    clearprism_l2_cache *l2 = (clearprism_l2_cache *)arg;

    /* Initial refresh */
    l2_do_refresh(l2);

    while (1) {
        /* Sleep for refresh interval, checking running flag periodically */
        for (int i = 0; i < l2->refresh_interval_sec; i++) {
            pthread_mutex_lock(&l2->lock);
            int running = l2->running;
            pthread_mutex_unlock(&l2->lock);
            if (!running) return NULL;
            sleep(1);
        }

        pthread_mutex_lock(&l2->lock);
        int running = l2->running;
        pthread_mutex_unlock(&l2->lock);
        if (!running) return NULL;

        l2_do_refresh(l2);
    }
    return NULL;
}

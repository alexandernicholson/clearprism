/*
 * clearprism_cache_l2.c — Shadow table materialization (background thread, WAL mode)
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

/* Maximum threads for parallel source reads during L2 refresh */
#define L2_MAX_READ_THREADS 16

/* Internal helpers */
static int  l2_create_shadow_table(clearprism_l2_cache *l2, char **errmsg);
static int  l2_do_refresh(clearprism_l2_cache *l2);
static void *l2_refresh_thread_func(void *arg);

/* Per-source read buffer for parallel refresh */
typedef struct {
    const char  *path;
    const char  *alias;
    time_t       file_mtime;
    int          n_cols;
    const char  *target_table;
    /* Output — filled by reader thread */
    sqlite3_value **values;   /* flat: n_rows * n_cols */
    int          n_rows;
    int          cap;
    int          ok;          /* 1 if read succeeded */
} l2_read_buf;

/* Context for parallel reader threads */
typedef struct {
    l2_read_buf *bufs;
    int          n_bufs;
    int          next;        /* atomic counter */
} l2_read_ctx;

static void *l2_read_worker(void *arg)
{
    l2_read_ctx *ctx = (l2_read_ctx *)arg;
    while (1) {
        int idx = __sync_fetch_and_add(&ctx->next, 1);
        if (idx >= ctx->n_bufs) break;

        l2_read_buf *buf = &ctx->bufs[idx];
        buf->ok = 0;
        buf->n_rows = 0;

        sqlite3 *conn = NULL;
        int rc = sqlite3_open_v2(buf->path, &conn, SQLITE_OPEN_READONLY, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(conn);
            continue;
        }
        sqlite3_busy_timeout(conn, 1000);

        /* Build SELECT */
        /* We need column names but don't have l2 pointer here — use a generic SELECT * approach
           that matches the shadow table column order. We'll build the SQL from target_table. */
        char *sel = clearprism_mprintf("SELECT * FROM \"%s\"", buf->target_table);
        sqlite3_stmt *stmt = NULL;
        rc = sqlite3_prepare_v2(conn, sel, -1, &stmt, NULL);
        sqlite3_free(sel);
        if (rc != SQLITE_OK) {
            sqlite3_close(conn);
            continue;
        }

        int cap = 256;
        sqlite3_value **vals = sqlite3_malloc(cap * buf->n_cols * (int)sizeof(sqlite3_value *));
        if (!vals) {
            sqlite3_finalize(stmt);
            sqlite3_close(conn);
            continue;
        }

        int count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (count >= cap) {
                cap *= 2;
                vals = sqlite3_realloc(vals, cap * buf->n_cols * (int)sizeof(sqlite3_value *));
                if (!vals) break;
            }
            sqlite3_value **row = &vals[count * buf->n_cols];
            for (int c = 0; c < buf->n_cols; c++) {
                row[c] = sqlite3_value_dup(sqlite3_column_value(stmt, c));
            }
            count++;
        }

        sqlite3_finalize(stmt);
        sqlite3_close(conn);

        buf->values = vals;
        buf->n_rows = count;
        buf->cap = cap;
        buf->ok = 1;
    }
    return NULL;
}

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

int clearprism_l2_populate(clearprism_l2_cache *l2, char **errmsg)
{
    if (!l2) return SQLITE_ERROR;
    int rc = l2_do_refresh(l2);
    if (rc != SQLITE_OK && rc != SQLITE_DONE) {
        if (errmsg) *errmsg = clearprism_strdup("L2: initial populate failed");
        return rc;
    }
    return SQLITE_OK;
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

    /* Create per-source mtime tracking table for incremental refresh */
    rc = sqlite3_exec(l2->writer_db,
        "CREATE TABLE IF NOT EXISTS _clearprism_source_meta ("
        "  source_alias TEXT PRIMARY KEY,"
        "  source_path TEXT NOT NULL,"
        "  last_mtime INTEGER NOT NULL"
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

/* Get stored mtime for a source alias. Returns 0 if not found. */
static time_t l2_get_stored_mtime(clearprism_l2_cache *l2, const char *alias)
{
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(l2->writer_db,
        "SELECT last_mtime FROM _clearprism_source_meta WHERE source_alias = ?",
        -1, &stmt, NULL);
    if (rc != SQLITE_OK) return 0;
    sqlite3_bind_text(stmt, 1, alias, -1, SQLITE_STATIC);
    time_t mtime = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        mtime = (time_t)sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return mtime;
}

static void l2_free_read_buf(l2_read_buf *buf)
{
    if (buf->values) {
        for (int r = 0; r < buf->n_rows; r++) {
            sqlite3_value **row = &buf->values[r * buf->n_cols];
            for (int c = 0; c < buf->n_cols; c++) {
                sqlite3_value_free(row[c]);
            }
        }
        sqlite3_free(buf->values);
        buf->values = NULL;
    }
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

    /* ---- Phase 1: Identify changed sources via mtime ---- */
    int n_changed = 0;
    l2_read_buf *changed = sqlite3_malloc(n_sources * (int)sizeof(l2_read_buf));
    if (!changed) {
        clearprism_sources_free(sources, n_sources);
        return SQLITE_NOMEM;
    }
    memset(changed, 0, n_sources * (int)sizeof(l2_read_buf));

    for (int s = 0; s < n_sources; s++) {
        struct stat st;
        if (stat(sources[s].path, &st) != 0) {
            continue;  /* File gone — keep stale cached rows */
        }
        time_t file_mtime = st.st_mtime;
        time_t stored_mtime = l2_get_stored_mtime(l2, sources[s].alias);
        if (file_mtime == stored_mtime) {
            continue;  /* Unchanged */
        }

        l2_read_buf *buf = &changed[n_changed++];
        buf->path = sources[s].path;
        buf->alias = sources[s].alias;
        buf->file_mtime = file_mtime;
        buf->n_cols = l2->n_cols;
        buf->target_table = l2->target_table;
        buf->values = NULL;
        buf->n_rows = 0;
        buf->cap = 0;
        buf->ok = 0;
    }

    /* ---- Phase 2: Parallel read of changed sources ---- */
    if (n_changed > 0) {
        l2_read_ctx ctx;
        ctx.bufs = changed;
        ctx.n_bufs = n_changed;
        ctx.next = 0;

        int n_threads = n_changed;
        if (n_threads > L2_MAX_READ_THREADS) n_threads = L2_MAX_READ_THREADS;

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, 256 * 1024);

        pthread_t *threads = sqlite3_malloc(n_threads * (int)sizeof(pthread_t));
        for (int t = 0; t < n_threads; t++) {
            pthread_create(&threads[t], &attr, l2_read_worker, &ctx);
        }
        pthread_attr_destroy(&attr);

        for (int t = 0; t < n_threads; t++) {
            pthread_join(threads[t], NULL);
        }
        sqlite3_free(threads);
    }

    /* ---- Phase 3: Per-source write with per-source commits ---- */

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

    /* Prepare DELETE and mtime upsert */
    char *del_src_sql = clearprism_mprintf(
        "DELETE FROM \"%s\" WHERE _cp_source_alias = ?", l2->shadow_table_name);
    sqlite3_stmt *del_src_stmt = NULL;
    sqlite3_prepare_v2(l2->writer_db, del_src_sql, -1, &del_src_stmt, NULL);
    sqlite3_free(del_src_sql);

    sqlite3_stmt *mtime_stmt = NULL;
    sqlite3_prepare_v2(l2->writer_db,
        "INSERT OR REPLACE INTO _clearprism_source_meta "
        "(source_alias, source_path, last_mtime) VALUES (?, ?, ?)",
        -1, &mtime_stmt, NULL);

    for (int i = 0; i < n_changed; i++) {
        l2_read_buf *buf = &changed[i];
        if (!buf->ok) {
            l2_free_read_buf(buf);
            continue;
        }

        /* Check if still running */
        pthread_mutex_lock(&l2->lock);
        int still_running = l2->running;
        pthread_mutex_unlock(&l2->lock);
        if (!still_running) {
            l2_free_read_buf(buf);
            continue;
        }

        /* Per-source transaction: DELETE old rows, INSERT new, update mtime */
        sqlite3_exec(l2->writer_db, "BEGIN IMMEDIATE", NULL, NULL, NULL);

        sqlite3_reset(del_src_stmt);
        sqlite3_bind_text(del_src_stmt, 1, buf->alias, -1, SQLITE_STATIC);
        sqlite3_step(del_src_stmt);

        for (int r = 0; r < buf->n_rows; r++) {
            sqlite3_reset(ins_stmt);
            sqlite3_value **row = &buf->values[r * buf->n_cols];
            for (int c = 0; c < buf->n_cols; c++) {
                sqlite3_bind_value(ins_stmt, c + 1, row[c]);
            }
            sqlite3_bind_text(ins_stmt, buf->n_cols + 1,
                               buf->alias, -1, SQLITE_STATIC);
            sqlite3_step(ins_stmt);
        }

        sqlite3_reset(mtime_stmt);
        sqlite3_bind_text(mtime_stmt, 1, buf->alias, -1, SQLITE_STATIC);
        sqlite3_bind_text(mtime_stmt, 2, buf->path, -1, SQLITE_STATIC);
        sqlite3_bind_int64(mtime_stmt, 3, (sqlite3_int64)buf->file_mtime);
        sqlite3_step(mtime_stmt);

        sqlite3_exec(l2->writer_db, "COMMIT", NULL, NULL, NULL);

        l2_free_read_buf(buf);
    }

    sqlite3_finalize(ins_stmt);
    sqlite3_finalize(del_src_stmt);
    sqlite3_finalize(mtime_stmt);
    sqlite3_free(changed);

    /* ---- Phase 4: Prune stale sources ---- */
    if (n_sources > 0) {
        size_t alias_list_size = 64;
        for (int s = 0; s < n_sources; s++)
            alias_list_size += strlen(sources[s].alias) + 4;
        char *alias_list = sqlite3_malloc((int)alias_list_size);
        pos = 0;
        for (int s = 0; s < n_sources; s++) {
            if (s > 0) pos += snprintf(alias_list + pos, alias_list_size - pos, ", ");
            pos += snprintf(alias_list + pos, alias_list_size - pos, "'%s'", sources[s].alias);
        }

        sqlite3_exec(l2->writer_db, "BEGIN IMMEDIATE", NULL, NULL, NULL);

        char *prune_sql = clearprism_mprintf(
            "DELETE FROM \"%s\" WHERE _cp_source_alias NOT IN (%s)",
            l2->shadow_table_name, alias_list);
        sqlite3_exec(l2->writer_db, prune_sql, NULL, NULL, NULL);
        sqlite3_free(prune_sql);

        char *prune_meta_sql = clearprism_mprintf(
            "DELETE FROM _clearprism_source_meta WHERE source_alias NOT IN (%s)",
            alias_list);
        sqlite3_exec(l2->writer_db, prune_meta_sql, NULL, NULL, NULL);
        sqlite3_free(prune_meta_sql);
        sqlite3_free(alias_list);

        sqlite3_exec(l2->writer_db, "COMMIT", NULL, NULL, NULL);
    }

    /* Update meta table */
    char *meta_sql = clearprism_mprintf(
        "INSERT OR REPLACE INTO _clearprism_meta (table_name, last_refresh) "
        "VALUES ('%s', datetime('now'))", l2->target_table);
    sqlite3_exec(l2->writer_db, meta_sql, NULL, NULL, NULL);
    sqlite3_free(meta_sql);

    pthread_mutex_lock(&l2->lock);
    l2->last_refresh = time(NULL);
    pthread_mutex_unlock(&l2->lock);

    clearprism_sources_free(sources, n_sources);
    return SQLITE_OK;
}

static void *l2_refresh_thread_func(void *arg)
{
    clearprism_l2_cache *l2 = (clearprism_l2_cache *)arg;

    /* Skip initial refresh — already done synchronously by clearprism_l2_populate() */

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

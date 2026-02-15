/*
 * clearprism_vtab.c — xCreate, xConnect, xDisconnect, xDestroy, xOpen, xClose
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
static int vtab_parse_args(int argc, const char *const *argv,
                            clearprism_vtab *vtab, char **pzErr);
static int vtab_discover_schema(clearprism_vtab *vtab, char **pzErr);
static int vtab_init_subsystems(clearprism_vtab *vtab, char **pzErr);
static void vtab_free(clearprism_vtab *vtab);

/*
 * Parse a single key=value argument. Supports optional quoting with ' or ".
 */
static int parse_kv(const char *arg, char **key, char **value)
{
    const char *eq = strchr(arg, '=');
    if (!eq) return 0;

    size_t klen = (size_t)(eq - arg);
    /* Skip leading whitespace in key */
    while (klen > 0 && (arg[0] == ' ' || arg[0] == '\t')) {
        arg++;
        klen--;
    }
    /* Trim trailing whitespace in key */
    while (klen > 0 && (arg[klen - 1] == ' ' || arg[klen - 1] == '\t')) {
        klen--;
    }

    *key = sqlite3_malloc((int)(klen + 1));
    if (!*key) return 0;
    memcpy(*key, arg, klen);
    (*key)[klen] = '\0';

    const char *vstart = eq + 1;
    /* Skip leading whitespace */
    while (*vstart == ' ' || *vstart == '\t') vstart++;

    /* Strip quotes */
    size_t vlen = strlen(vstart);
    if (vlen >= 2 && ((vstart[0] == '\'' && vstart[vlen - 1] == '\'') ||
                       (vstart[0] == '"' && vstart[vlen - 1] == '"'))) {
        vstart++;
        vlen -= 2;
    }
    /* Trim trailing whitespace */
    while (vlen > 0 && (vstart[vlen - 1] == ' ' || vstart[vlen - 1] == '\t')) {
        vlen--;
    }

    *value = sqlite3_malloc((int)(vlen + 1));
    if (!*value) {
        sqlite3_free(*key);
        *key = NULL;
        return 0;
    }
    memcpy(*value, vstart, vlen);
    (*value)[vlen] = '\0';

    return 1;
}

static int vtab_parse_args(int argc, const char *const *argv,
                            clearprism_vtab *vtab, char **pzErr)
{
    /* argv[0] = module name, argv[1] = database name, argv[2] = table name
       argv[3..] = user arguments */

    /* Set defaults */
    vtab->l1_max_rows = CLEARPRISM_DEFAULT_L1_MAX_ROWS;
    vtab->l1_max_bytes = CLEARPRISM_DEFAULT_L1_MAX_BYTES;
    vtab->pool_max_open = CLEARPRISM_DEFAULT_POOL_MAX_OPEN;
    vtab->l2_refresh_interval_sec = CLEARPRISM_DEFAULT_L2_REFRESH_SEC;

    int user_set_max_bytes = 0;

    for (int i = 3; i < argc; i++) {
        char *key = NULL, *value = NULL;
        if (!parse_kv(argv[i], &key, &value)) {
            continue;  /* skip unparseable args */
        }

        if (strcmp(key, "registry_db") == 0) {
            vtab->registry_path = value;
            value = NULL;  /* transfer ownership */
        } else if (strcmp(key, "table") == 0) {
            vtab->target_table = value;
            value = NULL;
        } else if (strcmp(key, "cache_db") == 0) {
            vtab->cache_db_path = value;
            value = NULL;
        } else if (strcmp(key, "l1_max_rows") == 0) {
            vtab->l1_max_rows = atoll(value);
        } else if (strcmp(key, "l1_max_bytes") == 0) {
            vtab->l1_max_bytes = atoll(value);
            user_set_max_bytes = 1;
        } else if (strcmp(key, "pool_max_open") == 0) {
            vtab->pool_max_open = atoi(value);
        } else if (strcmp(key, "l2_refresh_sec") == 0) {
            vtab->l2_refresh_interval_sec = atoi(value);
        } else if (strcmp(key, "mode") == 0) {
            if (strcmp(value, "snapshot") == 0)
                vtab->snapshot_mode = 1;
        }
        /* else: unknown key, silently ignore */

        sqlite3_free(key);
        sqlite3_free(value);
    }

    if (!vtab->registry_path) {
        *pzErr = clearprism_strdup("clearprism: 'registry_db' argument is required");
        return SQLITE_ERROR;
    }
    if (!vtab->target_table) {
        *pzErr = clearprism_strdup("clearprism: 'table' argument is required");
        return SQLITE_ERROR;
    }

    /* Auto-scale L1 byte budget when l1_max_rows is set large but
     * l1_max_bytes was not explicitly configured.  Each cached row costs
     * ~512 bytes (struct overhead + sqlite3_value objects + payload). */
    if (!user_set_max_bytes && vtab->l1_max_rows > CLEARPRISM_DEFAULT_L1_MAX_ROWS) {
        int64_t auto_bytes = vtab->l1_max_rows * 512;
        if (auto_bytes > vtab->l1_max_bytes)
            vtab->l1_max_bytes = auto_bytes;
    }

    return SQLITE_OK;
}

static int vtab_discover_schema(clearprism_vtab *vtab, char **pzErr)
{
    /* Get first active source to introspect its schema */
    clearprism_source *sources = NULL;
    int n_sources = 0;
    char *errmsg = NULL;

    int rc = clearprism_registry_snapshot(vtab->registry, vtab->target_table,
                                           &sources, &n_sources, &errmsg);
    if (rc != SQLITE_OK || n_sources == 0) {
        if (n_sources == 0 && rc == SQLITE_OK) {
            *pzErr = clearprism_mprintf(
                "clearprism: no active sources in registry '%s'",
                vtab->registry_path);
        } else {
            *pzErr = errmsg ? errmsg : clearprism_strdup("registry snapshot failed");
        }
        clearprism_sources_free(sources, n_sources);
        return SQLITE_ERROR;
    }

    /* Open first source to run PRAGMA table_info */
    sqlite3 *src_db = NULL;
    rc = sqlite3_open_v2(sources[0].path, &src_db,
                          SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
    if (rc != SQLITE_OK) {
        *pzErr = clearprism_mprintf(
            "clearprism: cannot open source '%s' for schema discovery: %s",
            sources[0].path, sqlite3_errmsg(src_db));
        sqlite3_close(src_db);
        clearprism_sources_free(sources, n_sources);
        return SQLITE_ERROR;
    }

    char *pragma_sql = clearprism_mprintf("PRAGMA table_info(\"%s\")", vtab->target_table);
    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(src_db, pragma_sql, -1, &stmt, NULL);
    sqlite3_free(pragma_sql);

    if (rc != SQLITE_OK) {
        *pzErr = clearprism_mprintf(
            "clearprism: PRAGMA table_info failed on '%s': %s",
            sources[0].path, sqlite3_errmsg(src_db));
        sqlite3_close(src_db);
        clearprism_sources_free(sources, n_sources);
        return SQLITE_ERROR;
    }

    /* Read columns */
    int col_capacity = 16;
    vtab->cols = sqlite3_malloc(col_capacity * (int)sizeof(*vtab->cols));
    vtab->nCol = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (vtab->nCol >= col_capacity) {
            col_capacity *= 2;
            vtab->cols = sqlite3_realloc(vtab->cols,
                                          col_capacity * (int)sizeof(*vtab->cols));
        }
        clearprism_col_def *col = &vtab->cols[vtab->nCol];
        col->name = clearprism_strdup((const char *)sqlite3_column_text(stmt, 1));
        const char *type_text = (const char *)sqlite3_column_text(stmt, 2);
        col->type = clearprism_strdup(type_text ? type_text : "");
        col->notnull = sqlite3_column_int(stmt, 3);
        col->pk = sqlite3_column_int(stmt, 5);
        vtab->nCol++;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(src_db);

    if (vtab->nCol == 0) {
        *pzErr = clearprism_mprintf(
            "clearprism: table '%s' has no columns or doesn't exist in source",
            vtab->target_table);
        clearprism_sources_free(sources, n_sources);
        return SQLITE_ERROR;
    }

    /* Validate schema across all other sources */
    for (int s = 1; s < n_sources; s++) {
        sqlite3 *check_db = NULL;
        int rc2 = sqlite3_open_v2(sources[s].path, &check_db,
                                   SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
        if (rc2 != SQLITE_OK) {
            sqlite3_close(check_db);
            continue;  /* source unavailable — skip validation */
        }

        char *check_sql = clearprism_mprintf("PRAGMA table_info(\"%s\")",
                                              vtab->target_table);
        sqlite3_stmt *check_stmt = NULL;
        rc2 = sqlite3_prepare_v2(check_db, check_sql, -1, &check_stmt, NULL);
        sqlite3_free(check_sql);

        if (rc2 != SQLITE_OK) {
            sqlite3_close(check_db);
            continue;  /* table may not exist — skip */
        }

        int col_idx = 0;
        int mismatch = 0;
        while (sqlite3_step(check_stmt) == SQLITE_ROW) {
            const char *col_name = (const char *)sqlite3_column_text(check_stmt, 1);
            if (col_idx >= vtab->nCol) { mismatch = 1; break; }
            if (!col_name || strcmp(col_name, vtab->cols[col_idx].name) != 0) {
                mismatch = 1;
                break;
            }
            col_idx++;
        }
        if (col_idx != vtab->nCol) mismatch = 1;

        sqlite3_finalize(check_stmt);
        sqlite3_close(check_db);

        if (mismatch) {
            *pzErr = clearprism_mprintf(
                "clearprism: schema mismatch in source '%s' (alias '%s') — "
                "expected %d columns matching '%s'",
                sources[s].path, sources[s].alias,
                vtab->nCol, sources[0].alias);
            clearprism_sources_free(sources, n_sources);
            return SQLITE_ERROR;
        }
    }

    clearprism_sources_free(sources, n_sources);

    /* Build CREATE TABLE SQL for sqlite3_declare_vtab */
    size_t sql_size = 256;
    for (int i = 0; i < vtab->nCol; i++) {
        sql_size += strlen(vtab->cols[i].name) + strlen(vtab->cols[i].type) + 32;
    }
    char *sql = sqlite3_malloc((int)sql_size);
    int pos = snprintf(sql, sql_size, "CREATE TABLE x(");

    for (int i = 0; i < vtab->nCol; i++) {
        if (i > 0) pos += snprintf(sql + pos, sql_size - pos, ", ");
        pos += snprintf(sql + pos, sql_size - pos, "\"%s\"", vtab->cols[i].name);
        if (vtab->cols[i].type[0]) {
            pos += snprintf(sql + pos, sql_size - pos, " %s", vtab->cols[i].type);
        }
    }
    /* Add hidden _source_db column */
    pos += snprintf(sql + pos, sql_size - pos, ", _source_db TEXT HIDDEN)");
    vtab->create_table_sql = sql;

    return SQLITE_OK;
}

static int vtab_init_subsystems(clearprism_vtab *vtab, char **pzErr)
{
    /* Connection pool */
    vtab->pool = clearprism_connpool_create(vtab->pool_max_open,
                                             CLEARPRISM_DEFAULT_POOL_TIMEOUT);
    if (!vtab->pool) {
        *pzErr = clearprism_strdup("clearprism: failed to create connection pool");
        return SQLITE_ERROR;
    }

    /* L1 cache */
    clearprism_l1_cache *l1 = clearprism_l1_create(vtab->l1_max_rows,
                                                     vtab->l1_max_bytes,
                                                     CLEARPRISM_DEFAULT_L1_TTL_SEC);
    if (!l1) {
        *pzErr = clearprism_strdup("clearprism: failed to create L1 cache");
        return SQLITE_ERROR;
    }

    /* L2 cache (optional — only if cache_db is specified) */
    clearprism_l2_cache *l2 = NULL;
    if (vtab->cache_db_path) {
        char *l2_err = NULL;
        l2 = clearprism_l2_create(vtab->cache_db_path, vtab->target_table,
                                    vtab->cols, vtab->nCol,
                                    vtab->l2_refresh_interval_sec,
                                    vtab->registry, vtab->pool, &l2_err);
        if (!l2) {
            /* L2 failure is non-fatal — log and continue without it */
            sqlite3_free(l2_err);
        } else {
            /* Start background refresh thread */
            char *start_err = NULL;
            clearprism_l2_start_refresh(l2, &start_err);
            sqlite3_free(start_err);
        }
    }

    vtab->cache = clearprism_cache_create(l1, l2);

    /* Create persistent worker thread pool */
    vtab->work_pool = clearprism_work_pool_create(CLEARPRISM_MAX_PREPARE_THREADS);

    return SQLITE_OK;
}

static int vtab_init(sqlite3 *db, int argc, const char *const *argv,
                      sqlite3_vtab **ppVtab, char **pzErr, int is_create)
{
    clearprism_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
    if (!vtab) {
        *pzErr = clearprism_strdup("clearprism: out of memory");
        return SQLITE_NOMEM;
    }
    memset(vtab, 0, sizeof(*vtab));
    vtab->host_db = db;
    pthread_mutex_init(&vtab->lock, NULL);

    int rc = vtab_parse_args(argc, argv, vtab, pzErr);
    if (rc != SQLITE_OK) {
        vtab_free(vtab);
        return rc;
    }

    /* Open registry */
    char *reg_err = NULL;
    vtab->registry = clearprism_registry_open(vtab->registry_path, &reg_err);
    if (!vtab->registry) {
        *pzErr = reg_err ? reg_err : clearprism_strdup("failed to open registry");
        vtab_free(vtab);
        return SQLITE_ERROR;
    }

    /* Discover schema from first source */
    rc = vtab_discover_schema(vtab, pzErr);
    if (rc != SQLITE_OK) {
        vtab_free(vtab);
        return rc;
    }

    /* Declare the virtual table schema */
    rc = sqlite3_declare_vtab(db, vtab->create_table_sql);
    if (rc != SQLITE_OK) {
        *pzErr = clearprism_mprintf("clearprism: declare_vtab failed: %s",
                                     sqlite3_errmsg(db));
        vtab_free(vtab);
        return rc;
    }

    /* Initialize subsystems */
    rc = vtab_init_subsystems(vtab, pzErr);
    if (rc != SQLITE_OK) {
        vtab_free(vtab);
        return rc;
    }

    /* Snapshot mode setup */
    if (vtab->snapshot_mode) {
        vtab->snapshot_table = clearprism_mprintf("_clearprism_snap_%s", argv[2]);
        if (!vtab->snapshot_table) {
            *pzErr = clearprism_strdup("clearprism: out of memory for snapshot table name");
            vtab_free(vtab);
            return SQLITE_NOMEM;
        }
        if (is_create) {
            rc = clearprism_snapshot_populate(vtab);
            if (rc != SQLITE_OK) {
                *pzErr = clearprism_mprintf("clearprism: snapshot population failed");
                vtab_free(vtab);
                return rc;
            }
        }
    }

    /* Register vtab in global map for aggregate function lookup */
    clearprism_register_vtab(vtab->target_table, vtab);

    *ppVtab = &vtab->base;
    return SQLITE_OK;
}

int clearprism_vtab_create(sqlite3 *db, void *pAux, int argc,
                            const char *const *argv, sqlite3_vtab **ppVtab,
                            char **pzErr)
{
    (void)pAux;
    return vtab_init(db, argc, argv, ppVtab, pzErr, 1);
}

int clearprism_vtab_connect(sqlite3 *db, void *pAux, int argc,
                             const char *const *argv, sqlite3_vtab **ppVtab,
                             char **pzErr)
{
    (void)pAux;
    return vtab_init(db, argc, argv, ppVtab, pzErr, 0);
}

static void vtab_free(clearprism_vtab *vtab)
{
    if (!vtab) return;

    /* Unregister from global vtab map */
    if (vtab->target_table)
        clearprism_unregister_vtab(vtab->target_table);

    if (vtab->work_pool) clearprism_work_pool_destroy(vtab->work_pool);
    if (vtab->cache) clearprism_cache_destroy(vtab->cache);
    if (vtab->pool) clearprism_connpool_destroy(vtab->pool);
    if (vtab->registry) clearprism_registry_close(vtab->registry);

    sqlite3_free(vtab->registry_path);
    sqlite3_free(vtab->target_table);
    sqlite3_free(vtab->cache_db_path);
    sqlite3_free(vtab->create_table_sql);
    sqlite3_free(vtab->snapshot_table);

    if (vtab->cols) {
        for (int i = 0; i < vtab->nCol; i++) {
            sqlite3_free(vtab->cols[i].name);
            sqlite3_free(vtab->cols[i].type);
        }
        sqlite3_free(vtab->cols);
    }

    pthread_mutex_destroy(&vtab->lock);
    sqlite3_free(vtab);
}

int clearprism_vtab_disconnect(sqlite3_vtab *pVtab)
{
    vtab_free((clearprism_vtab *)pVtab);
    return SQLITE_OK;
}

int clearprism_vtab_destroy(sqlite3_vtab *pVtab)
{
    clearprism_vtab *vtab = (clearprism_vtab *)pVtab;
    if (vtab->snapshot_table) {
        char *sql = clearprism_mprintf("DROP TABLE IF EXISTS \"%s\"", vtab->snapshot_table);
        if (sql) {
            sqlite3_exec(vtab->host_db, sql, NULL, NULL, NULL);
            sqlite3_free(sql);
        }
        /* Also drop the index */
        char *idx_sql = clearprism_mprintf("DROP INDEX IF EXISTS \"%s_src\"", vtab->snapshot_table);
        if (idx_sql) {
            sqlite3_exec(vtab->host_db, idx_sql, NULL, NULL, NULL);
            sqlite3_free(idx_sql);
        }
    }
    vtab_free(vtab);
    return SQLITE_OK;
}

int clearprism_vtab_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
    clearprism_cursor *cur = sqlite3_malloc(sizeof(*cur));
    if (!cur) return SQLITE_NOMEM;
    memset(cur, 0, sizeof(*cur));
    cur->vtab = (clearprism_vtab *)pVtab;
    cur->eof = 1;
    *ppCursor = &cur->base;
    return SQLITE_OK;
}

int clearprism_vtab_close(sqlite3_vtab_cursor *pCur)
{
    clearprism_cursor *cur = (clearprism_cursor *)pCur;

    /* Join any active prefetch thread before freeing handles */
    if (cur->prefetch_active) {
        pthread_join(cur->prefetch_thread, NULL);
        cur->prefetch_active = 0;
    }

    /* Clean up all source handles (finalize stmts, checkin connections) */
    if (cur->handles) {
        for (int i = 0; i < cur->n_handles; i++) {
            clearprism_source_handle *h = &cur->handles[i];
            if (h->stmt) sqlite3_finalize(h->stmt);
            if (h->conn && cur->sources && h->source_idx < cur->n_sources)
                clearprism_connpool_checkin(cur->vtab->pool,
                                            cur->sources[h->source_idx].path);
        }
        sqlite3_free(cur->handles);
    }

    /* Free merge-sort heap and cached column types */
    sqlite3_free(cur->heap);
    sqlite3_free(cur->order_col_types);

    /* Free query plan */
    clearprism_query_plan_clear(&cur->plan);

    /* Free saved argv */
    if (cur->saved_argv) {
        for (int i = 0; i < cur->saved_argc; i++) {
            sqlite3_value_free(cur->saved_argv[i]);
        }
        sqlite3_free(cur->saved_argv);
    }

    /* Free source snapshot */
    clearprism_sources_free(cur->sources, cur->n_sources);

    /* Free cache cursor */
    if (cur->cache_cursor) {
        clearprism_cache_cursor_free(cur->cache_cursor);
    }

    /* Free IN expansion arrays */
    if (cur->in_values) {
        for (int i = 0; i < cur->total_in_values; i++) {
            sqlite3_value_free(cur->in_values[i]);
        }
        sqlite3_free(cur->in_values);
    }
    sqlite3_free(cur->in_offsets);
    sqlite3_free(cur->in_counts);

    /* Free pre-created alias values */
    if (cur->alias_values) {
        for (int i = 0; i < cur->n_sources; i++)
            sqlite3_value_free(cur->alias_values[i]);
        sqlite3_free(cur->alias_values);
    }

    /* Free snapshot statement */
    if (cur->snapshot_stmt) {
        sqlite3_finalize(cur->snapshot_stmt);
        cur->snapshot_stmt = NULL;
    }

    /* Free pre-generated SQL */
    sqlite3_free(cur->cached_sql);
    sqlite3_free(cur->cached_fallback_sql);

    /* Flush drain to L1 cache then free drain buffers (uses cache_key) */
    clearprism_cursor_flush_drain(cur);

    /* Free L1 population buffer (pre-allocated flat arrays) */
    if (cur->buf_values) {
        int total = cur->buffer_n_rows * cur->buf_n_cols;
        for (int i = 0; i < total; i++)
            sqlite3_value_free(cur->buf_values[i]);
        sqlite3_free(cur->buf_values);
    }
    sqlite3_free(cur->buf_rows);
    sqlite3_free(cur->cache_key);

    sqlite3_free(cur);
    return SQLITE_OK;
}

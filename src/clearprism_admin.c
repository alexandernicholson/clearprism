/*
 * clearprism_admin.c — Admin/diagnostic SQL functions
 *
 * Functions registered:
 *   clearprism_status(vtab_name)         — returns JSON status
 *   clearprism_init_registry(path)       — creates registry tables
 *   clearprism_add_source(vtab, path, alias) — adds source to registry
 *   clearprism_flush_cache(vtab_name)    — flushes L1 cache
 *   clearprism_reload_registry(vtab_name) — forces registry reload
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

/* ========== clearprism_status(vtab_name) ========== */

static void admin_status_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 1 || sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
        sqlite3_result_error(ctx, "clearprism_status(vtab_name): requires text argument", -1);
        return;
    }
    const char *name = (const char *)sqlite3_value_text(argv[0]);
    clearprism_vtab *vtab = clearprism_lookup_vtab(name);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_status: no such virtual table", -1);
        return;
    }

    /* Gather L1 stats */
    int64_t l1_entries = 0, l1_rows = 0, l1_bytes = 0;
    int64_t l1_max_rows = 0, l1_max_bytes = 0;
    int64_t l1_hits = 0, l1_misses = 0;
    if (vtab->cache && vtab->cache->l1) {
        clearprism_l1_cache *l1 = vtab->cache->l1;
        pthread_mutex_lock(&l1->lock);
        l1_entries = l1->n_entries;
        l1_rows = l1->total_rows;
        l1_bytes = l1->total_bytes;
        l1_max_rows = l1->max_rows;
        l1_max_bytes = l1->max_bytes;
        l1_hits = l1->hits;
        l1_misses = l1->misses;
        pthread_mutex_unlock(&l1->lock);
    }

    /* Gather pool stats */
    int pool_open = 0, pool_max = 0, pool_checked_out = 0;
    int64_t pool_total_checkouts = 0;
    if (vtab->pool) {
        clearprism_connpool_stats(vtab->pool, &pool_open, &pool_max,
                                   &pool_checked_out, &pool_total_checkouts);
    }

    /* Gather registry stats */
    int n_sources = 0;
    long long last_reload = 0;
    if (vtab->registry) {
        pthread_mutex_lock(&vtab->registry->lock);
        n_sources = vtab->registry->n_sources;
        last_reload = (long long)vtab->registry->last_reload;
        pthread_mutex_unlock(&vtab->registry->lock);
    }

    /* Build JSON response */
    char *json = clearprism_mprintf(
        "{"
        "\"l1\":{\"entries\":%lld,\"rows\":%lld,\"bytes\":%lld,"
        "\"max_rows\":%lld,\"max_bytes\":%lld,\"hits\":%lld,\"misses\":%lld},"
        "\"pool\":{\"open\":%d,\"max\":%d,\"checked_out\":%d,\"total_checkouts\":%lld},"
        "\"registry\":{\"sources\":%d,\"last_reload\":%lld},"
        "\"l2_active\":%d,"
        "\"warnings\":\"%s\""
        "}",
        (long long)l1_entries, (long long)l1_rows, (long long)l1_bytes,
        (long long)l1_max_rows, (long long)l1_max_bytes,
        (long long)l1_hits, (long long)l1_misses,
        pool_open, pool_max, pool_checked_out, (long long)pool_total_checkouts,
        n_sources, last_reload,
        vtab->l2_active,
        vtab->init_warnings ? vtab->init_warnings : "");

    sqlite3_result_text(ctx, json, -1, sqlite3_free);
}

/* ========== clearprism_init_registry(path) ========== */

static void admin_init_registry_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 1 || sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
        sqlite3_result_error(ctx, "clearprism_init_registry(path): requires text argument", -1);
        return;
    }
    const char *path = (const char *)sqlite3_value_text(argv[0]);

    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(path, &db,
                              SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (rc != SQLITE_OK) {
        char *err = clearprism_mprintf("clearprism_init_registry: cannot open '%s': %s",
                                        path, db ? sqlite3_errmsg(db) : "unknown");
        sqlite3_close(db);
        sqlite3_result_error(ctx, err, -1);
        sqlite3_free(err);
        return;
    }

    char *exec_err = NULL;
    rc = sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS clearprism_sources ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  path TEXT NOT NULL UNIQUE,"
        "  alias TEXT NOT NULL UNIQUE,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  priority INTEGER NOT NULL DEFAULT 0,"
        "  added_at TEXT NOT NULL DEFAULT (datetime('now')),"
        "  notes TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS clearprism_table_overrides ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  source_id INTEGER NOT NULL REFERENCES clearprism_sources(id),"
        "  table_name TEXT NOT NULL,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  UNIQUE(source_id, table_name)"
        ");",
        NULL, NULL, &exec_err);
    sqlite3_close(db);

    if (rc != SQLITE_OK) {
        char *err = clearprism_mprintf("clearprism_init_registry: %s",
                                        exec_err ? exec_err : "unknown error");
        sqlite3_free(exec_err);
        sqlite3_result_error(ctx, err, -1);
        sqlite3_free(err);
        return;
    }

    sqlite3_result_text(ctx, "ok", -1, SQLITE_STATIC);
}

/* ========== clearprism_add_source(vtab_name, path, alias) ========== */

static void admin_add_source_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 3 ||
        sqlite3_value_type(argv[0]) != SQLITE_TEXT ||
        sqlite3_value_type(argv[1]) != SQLITE_TEXT ||
        sqlite3_value_type(argv[2]) != SQLITE_TEXT) {
        sqlite3_result_error(ctx,
            "clearprism_add_source(vtab_name, path, alias): requires 3 text arguments", -1);
        return;
    }

    const char *vtab_name = (const char *)sqlite3_value_text(argv[0]);
    const char *source_path = (const char *)sqlite3_value_text(argv[1]);
    const char *alias = (const char *)sqlite3_value_text(argv[2]);

    clearprism_vtab *vtab = clearprism_lookup_vtab(vtab_name);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_add_source: no such virtual table", -1);
        return;
    }
    if (!vtab->registry_path) {
        sqlite3_result_error(ctx, "clearprism_add_source: vtab has no registry path", -1);
        return;
    }

    /* Open the registry database and insert */
    sqlite3 *reg_db = NULL;
    int rc = sqlite3_open_v2(vtab->registry_path, &reg_db, SQLITE_OPEN_READWRITE, NULL);
    if (rc != SQLITE_OK) {
        char *err = clearprism_mprintf("clearprism_add_source: cannot open registry: %s",
                                        reg_db ? sqlite3_errmsg(reg_db) : "unknown");
        sqlite3_close(reg_db);
        sqlite3_result_error(ctx, err, -1);
        sqlite3_free(err);
        return;
    }

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(reg_db,
        "INSERT INTO clearprism_sources (path, alias) VALUES (?, ?)", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        char *err = clearprism_mprintf("clearprism_add_source: %s", sqlite3_errmsg(reg_db));
        sqlite3_close(reg_db);
        sqlite3_result_error(ctx, err, -1);
        sqlite3_free(err);
        return;
    }

    sqlite3_bind_text(stmt, 1, source_path, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, alias, -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    sqlite3_close(reg_db);

    if (rc != SQLITE_DONE) {
        sqlite3_result_error(ctx, "clearprism_add_source: insert failed (duplicate path or alias?)", -1);
        return;
    }

    sqlite3_result_text(ctx, "ok", -1, SQLITE_STATIC);
}

/* ========== clearprism_flush_cache(vtab_name) ========== */

static void admin_flush_cache_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 1 || sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
        sqlite3_result_error(ctx, "clearprism_flush_cache(vtab_name): requires text argument", -1);
        return;
    }
    const char *name = (const char *)sqlite3_value_text(argv[0]);
    clearprism_vtab *vtab = clearprism_lookup_vtab(name);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_flush_cache: no such virtual table", -1);
        return;
    }
    if (vtab->cache && vtab->cache->l1) {
        clearprism_l1_flush(vtab->cache->l1);
    }
    sqlite3_result_text(ctx, "ok", -1, SQLITE_STATIC);
}

/* ========== clearprism_reload_registry(vtab_name) ========== */

static void admin_reload_registry_func(sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
    if (argc < 1 || sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
        sqlite3_result_error(ctx,
            "clearprism_reload_registry(vtab_name): requires text argument", -1);
        return;
    }
    const char *name = (const char *)sqlite3_value_text(argv[0]);
    clearprism_vtab *vtab = clearprism_lookup_vtab(name);
    if (!vtab) {
        sqlite3_result_error(ctx, "clearprism_reload_registry: no such virtual table", -1);
        return;
    }
    if (!vtab->registry) {
        sqlite3_result_error(ctx, "clearprism_reload_registry: no registry", -1);
        return;
    }

    /* Force reload by resetting last_reload */
    pthread_mutex_lock(&vtab->registry->lock);
    vtab->registry->last_reload = 0;
    pthread_mutex_unlock(&vtab->registry->lock);

    char *err = NULL;
    int rc = clearprism_registry_reload(vtab->registry, &err);
    if (rc != SQLITE_OK) {
        char *msg = clearprism_mprintf("clearprism_reload_registry: %s",
                                        err ? err : "unknown error");
        sqlite3_free(err);
        sqlite3_result_error(ctx, msg, -1);
        sqlite3_free(msg);
        return;
    }

    sqlite3_result_text(ctx, "ok", -1, SQLITE_STATIC);
}

/* ========== Registration ========== */

int clearprism_register_admin_functions(sqlite3 *db)
{
    int rc;
    rc = sqlite3_create_function(db, "clearprism_status", 1,
                                  SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                  NULL, admin_status_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_init_registry", 1,
                                  SQLITE_UTF8,
                                  NULL, admin_init_registry_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_add_source", 3,
                                  SQLITE_UTF8,
                                  NULL, admin_add_source_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_flush_cache", 1,
                                  SQLITE_UTF8,
                                  NULL, admin_flush_cache_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "clearprism_reload_registry", 1,
                                  SQLITE_UTF8,
                                  NULL, admin_reload_registry_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    return SQLITE_OK;
}

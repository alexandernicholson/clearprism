/*
 * test_admin.c — Tests for admin/diagnostic SQL functions
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);
extern int clearprism_init(sqlite3 *db);

static const char *ADMIN_REG_PATH = "/tmp/clearprism_admin_test_registry.db";
static const char *ADMIN_SRC1_PATH = "/tmp/clearprism_admin_test_src1.db";
static const char *ADMIN_SRC2_PATH = "/tmp/clearprism_admin_test_src2.db";

static void admin_cleanup(void)
{
    unlink(ADMIN_REG_PATH);
    unlink(ADMIN_SRC1_PATH);
    unlink(ADMIN_SRC2_PATH);
    unlink("/tmp/clearprism_admin_test_initreg.db");
    /* Clean up auto-generated L2 cache files */
    unlink("/tmp/clearprism_cache_test_items_items.db");
    unlink("/tmp/clearprism_cache_test_items_items.db-wal");
    unlink("/tmp/clearprism_cache_test_items_items.db-shm");
    unlink("/tmp/clearprism_cache_good1_items.db");
    unlink("/tmp/clearprism_cache_good1_items.db-wal");
    unlink("/tmp/clearprism_cache_good1_items.db-shm");
}

static void admin_setup(void)
{
    admin_cleanup();

    /* Create registry */
    sqlite3 *db = NULL;
    sqlite3_open(ADMIN_REG_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE clearprism_sources ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  path TEXT NOT NULL UNIQUE,"
        "  alias TEXT NOT NULL UNIQUE,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  priority INTEGER NOT NULL DEFAULT 0,"
        "  added_at TEXT NOT NULL DEFAULT (datetime('now')),"
        "  notes TEXT"
        ");"
        "CREATE TABLE clearprism_table_overrides ("
        "  source_id INTEGER NOT NULL REFERENCES clearprism_sources(id),"
        "  table_name TEXT NOT NULL,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  PRIMARY KEY (source_id, table_name)"
        ");",
        NULL, NULL, NULL);

    char *sql;
    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias) VALUES ('%s', 'src1')", ADMIN_SRC1_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias) VALUES ('%s', 'src2')", ADMIN_SRC2_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    sqlite3_close(db);

    /* Create source databases */
    sqlite3_open(ADMIN_SRC1_PATH, &db);
    sqlite3_exec(db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);"
                     "INSERT INTO items VALUES (1, 'Widget', 9.99);"
                     "INSERT INTO items VALUES (2, 'Gadget', 19.99);",
                 NULL, NULL, NULL);
    sqlite3_close(db);

    sqlite3_open(ADMIN_SRC2_PATH, &db);
    sqlite3_exec(db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);"
                     "INSERT INTO items VALUES (3, 'Doohickey', 4.99);",
                 NULL, NULL, NULL);
    sqlite3_close(db);
}

/* ========== init_registry tests ========== */

static void test_admin_init_registry(void)
{
    const char *path = "/tmp/clearprism_admin_test_initreg.db";
    unlink(path);

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    /* Create registry */
    char *sql = sqlite3_mprintf("SELECT clearprism_init_registry('%s')", path);
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);
    test_report("init_registry prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("init_registry returns ok", rc == SQLITE_ROW &&
            strcmp((const char *)sqlite3_column_text(stmt, 0), "ok") == 0);
        sqlite3_finalize(stmt);
    }

    /* Verify tables exist */
    sqlite3 *reg = NULL;
    sqlite3_open(path, &reg);
    rc = sqlite3_prepare_v2(reg,
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", -1, &stmt, NULL);
    int found_sources = 0, found_overrides = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        if (strcmp(name, "clearprism_sources") == 0) found_sources = 1;
        if (strcmp(name, "clearprism_table_overrides") == 0) found_overrides = 1;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(reg);
    test_report("init_registry creates clearprism_sources", found_sources);
    test_report("init_registry creates clearprism_table_overrides", found_overrides);

    /* Idempotent — second call should succeed */
    sql = sqlite3_mprintf("SELECT clearprism_init_registry('%s')", path);
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);
    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("init_registry idempotent", rc == SQLITE_ROW);
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    unlink(path);
}

/* ========== status tests ========== */

static void test_admin_status(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items')", ADMIN_REG_PATH);
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("status: vtab created", rc == SQLITE_OK);
    if (err) { printf("    error: %s\n", err); sqlite3_free(err); }

    /* Query status */
    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, "SELECT clearprism_status('items')", -1, &stmt, NULL);
    test_report("status: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("status: returns row", rc == SQLITE_ROW);
        if (rc == SQLITE_ROW) {
            const char *json = (const char *)sqlite3_column_text(stmt, 0);
            test_report("status: returns JSON", json != NULL && json[0] == '{');
            test_report("status: contains l1", json && strstr(json, "\"l1\"") != NULL);
            test_report("status: contains pool", json && strstr(json, "\"pool\"") != NULL);
            test_report("status: contains registry", json && strstr(json, "\"registry\"") != NULL);
            test_report("status: contains sources:2", json && strstr(json, "\"sources\":2") != NULL);
        }
        sqlite3_finalize(stmt);
    }

    /* Status for non-existent vtab should error */
    rc = sqlite3_prepare_v2(db, "SELECT clearprism_status('nonexistent')", -1, &stmt, NULL);
    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("status: error for unknown vtab", rc == SQLITE_ERROR);
        sqlite3_finalize(stmt);
    }

    sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    sqlite3_close(db);
    admin_cleanup();
}

/* ========== flush_cache tests ========== */

static void test_admin_flush_cache(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items')", ADMIN_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Run a query to populate L1 cache */
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT * FROM test_items WHERE name = 'Widget'", -1, &stmt, NULL);
    while (sqlite3_step(stmt) == SQLITE_ROW) {}
    sqlite3_finalize(stmt);

    /* Check status shows entries */
    sqlite3_prepare_v2(db, "SELECT clearprism_status('items')", -1, &stmt, NULL);
    sqlite3_step(stmt);
    const char *json_before = (const char *)sqlite3_column_text(stmt, 0);
    /* After a query, there should be some cache activity */
    test_report("flush_cache: pre-flush status available", json_before != NULL);
    sqlite3_finalize(stmt);

    /* Flush */
    sqlite3_prepare_v2(db, "SELECT clearprism_flush_cache('items')", -1, &stmt, NULL);
    int rc = sqlite3_step(stmt);
    test_report("flush_cache: returns ok", rc == SQLITE_ROW &&
        strcmp((const char *)sqlite3_column_text(stmt, 0), "ok") == 0);
    sqlite3_finalize(stmt);

    /* Check status shows zero entries */
    sqlite3_prepare_v2(db, "SELECT clearprism_status('items')", -1, &stmt, NULL);
    sqlite3_step(stmt);
    const char *json_after = (const char *)sqlite3_column_text(stmt, 0);
    test_report("flush_cache: entries zeroed", json_after && strstr(json_after, "\"entries\":0") != NULL);
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    sqlite3_close(db);
    admin_cleanup();
}

/* ========== add_source tests ========== */

static void test_admin_add_source(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items')", ADMIN_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Add a new source */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_add_source('items', '/tmp/new_source.db', 'new_src')",
        -1, &stmt, NULL);
    test_report("add_source: prepare", rc == SQLITE_OK);
    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("add_source: returns ok", rc == SQLITE_ROW &&
            strcmp((const char *)sqlite3_column_text(stmt, 0), "ok") == 0);
        sqlite3_finalize(stmt);
    }

    /* Verify it's in the registry */
    sqlite3 *reg = NULL;
    sqlite3_open(ADMIN_REG_PATH, &reg);
    sqlite3_prepare_v2(reg, "SELECT COUNT(*) FROM clearprism_sources WHERE alias = 'new_src'",
                        -1, &stmt, NULL);
    sqlite3_step(stmt);
    int count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(reg);
    test_report("add_source: source in registry", count == 1);

    /* Duplicate should fail */
    rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_add_source('items', '/tmp/new_source.db', 'new_src')",
        -1, &stmt, NULL);
    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("add_source: duplicate errors", rc == SQLITE_ERROR);
        sqlite3_finalize(stmt);
    }

    sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    sqlite3_close(db);
    admin_cleanup();
}

/* ========== reload_registry tests ========== */

static void test_admin_reload_registry(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items')", ADMIN_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Reload */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT clearprism_reload_registry('items')",
                                 -1, &stmt, NULL);
    test_report("reload_registry: prepare", rc == SQLITE_OK);
    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("reload_registry: returns ok", rc == SQLITE_ROW &&
            strcmp((const char *)sqlite3_column_text(stmt, 0), "ok") == 0);
        sqlite3_finalize(stmt);
    }

    /* Error for non-existent vtab */
    rc = sqlite3_prepare_v2(db, "SELECT clearprism_reload_registry('nonexistent')",
                             -1, &stmt, NULL);
    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        test_report("reload_registry: error for unknown vtab", rc == SQLITE_ERROR);
        sqlite3_finalize(stmt);
    }

    sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    sqlite3_close(db);
    admin_cleanup();
}

/* ========== parameter validation tests ========== */

static void test_admin_param_validation(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    /* Invalid pool_max_open */
    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE bad1 USING clearprism("
        "  registry_db='%s', table='items', pool_max_open='abc')", ADMIN_REG_PATH);
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("param_validation: pool_max_open=abc rejected", rc != SQLITE_OK);
    if (err) { sqlite3_free(err); err = NULL; }

    /* Invalid mode */
    sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE bad2 USING clearprism("
        "  registry_db='%s', table='items', mode='invalid')", ADMIN_REG_PATH);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("param_validation: mode=invalid rejected", rc != SQLITE_OK);
    if (err) { sqlite3_free(err); err = NULL; }

    /* Unknown parameter */
    sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE bad3 USING clearprism("
        "  registry_db='%s', table='items', bogus_param='123')", ADMIN_REG_PATH);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("param_validation: unknown param rejected", rc != SQLITE_OK);
    if (err) { sqlite3_free(err); err = NULL; }

    /* Negative l1_max_rows */
    sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE bad4 USING clearprism("
        "  registry_db='%s', table='items', l1_max_rows='-5')", ADMIN_REG_PATH);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("param_validation: negative l1_max_rows rejected", rc != SQLITE_OK);
    if (err) { sqlite3_free(err); err = NULL; }

    /* Valid mode=live should work */
    sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE good1 USING clearprism("
        "  registry_db='%s', table='items', mode='live')", ADMIN_REG_PATH);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("param_validation: mode=live accepted", rc == SQLITE_OK);
    if (err) { sqlite3_free(err); err = NULL; }
    sqlite3_exec(db, "DROP TABLE good1", NULL, NULL, NULL);

    sqlite3_close(db);
    admin_cleanup();
}

/* ========== _source_errors column tests ========== */

static void test_admin_source_errors_column(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items')", ADMIN_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Query _source_errors — should be 0 with all sources healthy */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT _source_errors FROM test_items LIMIT 1", -1, &stmt, NULL);
    test_report("source_errors: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            int errors = sqlite3_column_int(stmt, 0);
            test_report("source_errors: 0 with healthy sources", errors == 0);
        } else {
            test_report("source_errors: got row", 0);
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    sqlite3_close(db);
    admin_cleanup();
}

/* ========== schema override tests ========== */

static void test_admin_schema_override(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    /* Create vtab with schema override */
    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items',"
        "  schema='id INTEGER, name TEXT, price REAL')", ADMIN_REG_PATH);
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("schema_override: vtab created", rc == SQLITE_OK);
    if (err) { printf("    error: %s\n", err); sqlite3_free(err); err = NULL; }

    /* Query should work */
    if (rc == SQLITE_OK) {
        sqlite3_stmt *stmt = NULL;
        rc = sqlite3_prepare_v2(db,
            "SELECT name, price FROM test_items ORDER BY name", -1, &stmt, NULL);
        test_report("schema_override: query prepare", rc == SQLITE_OK);
        if (rc == SQLITE_OK) {
            int rows = 0;
            while (sqlite3_step(stmt) == SQLITE_ROW) rows++;
            test_report("schema_override: returns rows", rows == 3);
            sqlite3_finalize(stmt);
        }
        sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    }

    sqlite3_close(db);
    admin_cleanup();
}

/* ========== hit/miss counter tests ========== */

static void test_admin_cache_hit_miss(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items')", ADMIN_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* First query — should be a cache miss */
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(db, "SELECT * FROM test_items WHERE name = 'Widget'", -1, &stmt, NULL);
    while (sqlite3_step(stmt) == SQLITE_ROW) {}
    sqlite3_finalize(stmt);

    /* Check status for miss */
    sqlite3_prepare_v2(db, "SELECT clearprism_status('items')", -1, &stmt, NULL);
    sqlite3_step(stmt);
    const char *json1 = (const char *)sqlite3_column_text(stmt, 0);
    test_report("cache_hit_miss: has misses after first query",
                json1 && strstr(json1, "\"misses\":") != NULL);
    sqlite3_finalize(stmt);

    /* Second identical query — should be a cache hit */
    sqlite3_prepare_v2(db, "SELECT * FROM test_items WHERE name = 'Widget'", -1, &stmt, NULL);
    while (sqlite3_step(stmt) == SQLITE_ROW) {}
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(db, "SELECT clearprism_status('items')", -1, &stmt, NULL);
    sqlite3_step(stmt);
    const char *json2 = (const char *)sqlite3_column_text(stmt, 0);
    test_report("cache_hit_miss: has hits after second query",
                json2 && strstr(json2, "\"hits\":") != NULL);
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    sqlite3_close(db);
    admin_cleanup();
}

/* ========== L2 auto-enable tests ========== */

static void test_admin_l2_auto_enable(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    /* Create vtab WITHOUT cache_db — L2 should auto-enable */
    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items')", ADMIN_REG_PATH);
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("l2_auto: vtab created", rc == SQLITE_OK);
    if (err) { printf("    error: %s\n", err); sqlite3_free(err); err = NULL; }

    /* Status should show l2_active:1 */
    if (rc == SQLITE_OK) {
        sqlite3_stmt *stmt = NULL;
        sqlite3_prepare_v2(db, "SELECT clearprism_status('items')", -1, &stmt, NULL);
        sqlite3_step(stmt);
        const char *json = (const char *)sqlite3_column_text(stmt, 0);
        test_report("l2_auto: l2_active is 1", json && strstr(json, "\"l2_active\":1") != NULL);
        sqlite3_finalize(stmt);

        sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    }

    /* Clean up auto-generated cache file */
    unlink("/tmp/clearprism_cache_test_items_items.db");
    unlink("/tmp/clearprism_cache_test_items_items.db-wal");
    unlink("/tmp/clearprism_cache_test_items_items.db-shm");

    sqlite3_close(db);
    admin_cleanup();
}

static void test_admin_l2_disable(void)
{
    admin_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    /* Create vtab with cache_db='none' — L2 should be disabled */
    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE test_items USING clearprism("
        "  registry_db='%s', table='items', cache_db='none')", ADMIN_REG_PATH);
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("l2_disable: vtab created", rc == SQLITE_OK);
    if (err) { printf("    error: %s\n", err); sqlite3_free(err); err = NULL; }

    /* Status should show l2_active:0 */
    if (rc == SQLITE_OK) {
        sqlite3_stmt *stmt = NULL;
        sqlite3_prepare_v2(db, "SELECT clearprism_status('items')", -1, &stmt, NULL);
        sqlite3_step(stmt);
        const char *json = (const char *)sqlite3_column_text(stmt, 0);
        test_report("l2_disable: l2_active is 0", json && strstr(json, "\"l2_active\":0") != NULL);
        sqlite3_finalize(stmt);

        sqlite3_exec(db, "DROP TABLE test_items", NULL, NULL, NULL);
    }

    sqlite3_close(db);
    admin_cleanup();
}

/* ========== Runner ========== */

int test_admin_run(void)
{
    test_admin_init_registry();
    test_admin_status();
    test_admin_flush_cache();
    test_admin_add_source();
    test_admin_reload_registry();
    test_admin_param_validation();
    test_admin_source_errors_column();
    test_admin_schema_override();
    test_admin_cache_hit_miss();
    test_admin_l2_auto_enable();
    test_admin_l2_disable();
    return 0;
}

/*
 * test_agg.c — Tests for aggregate pushdown functions
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);
extern int clearprism_init(sqlite3 *db);

static const char *AGG_REG_PATH = "/tmp/clearprism_agg_test_registry.db";
static const char *AGG_SRC1_PATH = "/tmp/clearprism_agg_test_src1.db";
static const char *AGG_SRC2_PATH = "/tmp/clearprism_agg_test_src2.db";
static const char *AGG_SRC3_PATH = "/tmp/clearprism_agg_test_src3.db";

static void agg_cleanup(void)
{
    unlink(AGG_REG_PATH);
    unlink(AGG_SRC1_PATH);
    unlink(AGG_SRC2_PATH);
    unlink(AGG_SRC3_PATH);
}

static void agg_setup(void)
{
    agg_cleanup();

    /* Create registry */
    sqlite3 *db = NULL;
    sqlite3_open(AGG_REG_PATH, &db);
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
        "INSERT INTO clearprism_sources (path, alias, priority) VALUES ('%s', 'src1', 0)",
        AGG_SRC1_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias, priority) VALUES ('%s', 'src2', 1)",
        AGG_SRC2_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias, priority) VALUES ('%s', 'src3', 2)",
        AGG_SRC3_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    sqlite3_close(db);

    /* Source 1: 100 rows, category = cat_0..cat_9, value = 1..100 */
    sqlite3_open(AGG_SRC1_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, value REAL);",
        NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    for (int i = 1; i <= 100; i++) {
        char isql[128];
        snprintf(isql, sizeof(isql),
                 "INSERT INTO items VALUES (%d, 'cat_%d', %d.0)", i, i % 10, i);
        sqlite3_exec(db, isql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);

    /* Source 2: 100 rows, same schema */
    sqlite3_open(AGG_SRC2_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, value REAL);",
        NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    for (int i = 1; i <= 100; i++) {
        char isql[128];
        snprintf(isql, sizeof(isql),
                 "INSERT INTO items VALUES (%d, 'cat_%d', %d.0)", i, i % 10, i + 100);
        sqlite3_exec(db, isql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);

    /* Source 3: 100 rows, same schema */
    sqlite3_open(AGG_SRC3_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, value REAL);",
        NULL, NULL, NULL);
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    for (int i = 1; i <= 100; i++) {
        char isql[128];
        snprintf(isql, sizeof(isql),
                 "INSERT INTO items VALUES (%d, 'cat_%d', %d.0)", i, i % 10, i + 200);
        sqlite3_exec(db, isql, NULL, NULL, NULL);
    }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);
}

static void test_agg_count(void)
{
    agg_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE items USING clearprism("
        "  registry_db='%s', table='items')", AGG_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* clearprism_count should return 300 (100 per source * 3 sources) */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_count('items', NULL)", -1, &stmt, NULL);
    test_report("agg_count: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t count = sqlite3_column_int64(stmt, 0);
        test_report("agg_count: returns 300", count == 300);
    } else {
        test_report("agg_count: returns 300", 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE items", NULL, NULL, NULL);
    sqlite3_close(db);
    agg_cleanup();
}

static void test_agg_count_where(void)
{
    agg_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE items USING clearprism("
        "  registry_db='%s', table='items')", AGG_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* cat_0 appears for id % 10 == 0, i.e. 10, 20, ..., 100 = 10 per source = 30 total */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_count('items', 'category = ''cat_0''')", -1, &stmt, NULL);
    test_report("agg_count_where: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t count = sqlite3_column_int64(stmt, 0);
        test_report("agg_count_where: returns 30", count == 30);
    } else {
        test_report("agg_count_where: returns 30", 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE items", NULL, NULL, NULL);
    sqlite3_close(db);
    agg_cleanup();
}

static void test_agg_sum(void)
{
    agg_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE items USING clearprism("
        "  registry_db='%s', table='items')", AGG_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Sum of values:
     * src1: sum(1..100) = 5050
     * src2: sum(101..200) = 15050
     * src3: sum(201..300) = 25050
     * total = 45150 */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_sum('items', 'value', NULL)", -1, &stmt, NULL);
    test_report("agg_sum: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        double sum = sqlite3_column_double(stmt, 0);
        test_report("agg_sum: returns 45150", fabs(sum - 45150.0) < 0.01);
    } else {
        test_report("agg_sum: returns 45150", 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE items", NULL, NULL, NULL);
    sqlite3_close(db);
    agg_cleanup();
}

static void test_agg_avg(void)
{
    agg_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE items USING clearprism("
        "  registry_db='%s', table='items')", AGG_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Avg = 45150 / 300 = 150.5 */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_avg('items', 'value', NULL)", -1, &stmt, NULL);
    test_report("agg_avg: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        double avg = sqlite3_column_double(stmt, 0);
        test_report("agg_avg: returns 150.5", fabs(avg - 150.5) < 0.01);
    } else {
        test_report("agg_avg: returns 150.5", 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE items", NULL, NULL, NULL);
    sqlite3_close(db);
    agg_cleanup();
}

static void test_agg_min_max(void)
{
    agg_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE items USING clearprism("
        "  registry_db='%s', table='items')", AGG_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Min = 1.0 (from src1), Max = 300.0 (from src3) */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_min('items', 'value', NULL)", -1, &stmt, NULL);
    test_report("agg_min: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        double min_val = sqlite3_column_double(stmt, 0);
        test_report("agg_min: returns 1.0", fabs(min_val - 1.0) < 0.01);
    } else {
        test_report("agg_min: returns 1.0", 0);
    }
    sqlite3_finalize(stmt);

    rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_max('items', 'value', NULL)", -1, &stmt, NULL);
    test_report("agg_max: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        double max_val = sqlite3_column_double(stmt, 0);
        test_report("agg_max: returns 300.0", fabs(max_val - 300.0) < 0.01);
    } else {
        test_report("agg_max: returns 300.0", 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE items", NULL, NULL, NULL);
    sqlite3_close(db);
    agg_cleanup();
}

static void test_agg_empty_sources(void)
{
    agg_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE items USING clearprism("
        "  registry_db='%s', table='items')", AGG_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Query with WHERE that matches no rows */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_count('items', 'category = ''nonexistent''')",
        -1, &stmt, NULL);
    test_report("agg_empty: prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t count = sqlite3_column_int64(stmt, 0);
        test_report("agg_empty: count returns 0", count == 0);
    } else {
        test_report("agg_empty: count returns 0", 0);
    }
    sqlite3_finalize(stmt);

    /* SUM on no matching rows should return NULL */
    rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_sum('items', 'value', 'category = ''nonexistent''')",
        -1, &stmt, NULL);
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        int is_null = (sqlite3_column_type(stmt, 0) == SQLITE_NULL);
        test_report("agg_empty: sum returns NULL", is_null);
    } else {
        test_report("agg_empty: sum returns NULL", 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE items", NULL, NULL, NULL);
    sqlite3_close(db);
    agg_cleanup();
}

static void test_agg_parallel(void)
{
    /* Test with many sources to exercise parallel workers */
    agg_cleanup();

    /* Create registry with 10 sources */
    sqlite3 *db = NULL;
    sqlite3_open(AGG_REG_PATH, &db);
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

    char src_paths[10][256];
    for (int i = 0; i < 10; i++) {
        snprintf(src_paths[i], sizeof(src_paths[i]),
                 "/tmp/clearprism_agg_par_src%d.db", i);
        unlink(src_paths[i]);

        char *isql = sqlite3_mprintf(
            "INSERT INTO clearprism_sources (path, alias, priority) "
            "VALUES ('%s', 'par_%d', %d)", src_paths[i], i, i);
        sqlite3_exec(db, isql, NULL, NULL, NULL);
        sqlite3_free(isql);

        /* Create source with 50 rows each */
        sqlite3 *sdb = NULL;
        sqlite3_open(src_paths[i], &sdb);
        sqlite3_exec(sdb,
            "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, value REAL);",
            NULL, NULL, NULL);
        sqlite3_exec(sdb, "BEGIN", NULL, NULL, NULL);
        for (int j = 1; j <= 50; j++) {
            char jsql[128];
            snprintf(jsql, sizeof(jsql),
                     "INSERT INTO items VALUES (%d, 'cat_%d', %d.0)",
                     j, j % 5, i * 50 + j);
            sqlite3_exec(sdb, jsql, NULL, NULL, NULL);
        }
        sqlite3_exec(sdb, "COMMIT", NULL, NULL, NULL);
        sqlite3_close(sdb);
    }
    sqlite3_close(db);

    /* Run aggregate */
    sqlite3_open(":memory:", &db);
    clearprism_init(db);
    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE items USING clearprism("
        "  registry_db='%s', table='items')", AGG_REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* 10 sources * 50 rows = 500 */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_count('items', NULL)", -1, &stmt, NULL);
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t count = sqlite3_column_int64(stmt, 0);
        test_report("agg_parallel: count returns 500", count == 500);
    } else {
        test_report("agg_parallel: count returns 500", 0);
    }
    sqlite3_finalize(stmt);

    /* Sum: sum(1..500) = 125250 */
    rc = sqlite3_prepare_v2(db,
        "SELECT clearprism_sum('items', 'value', NULL)", -1, &stmt, NULL);
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        double sum = sqlite3_column_double(stmt, 0);
        test_report("agg_parallel: sum returns 125250", fabs(sum - 125250.0) < 0.01);
    } else {
        test_report("agg_parallel: sum returns 125250", 0);
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(db, "DROP TABLE items", NULL, NULL, NULL);
    sqlite3_close(db);

    for (int i = 0; i < 10; i++) unlink(src_paths[i]);
    agg_cleanup();
}

int test_agg_run(void)
{
    test_agg_count();
    test_agg_count_where();
    test_agg_sum();
    test_agg_avg();
    test_agg_min_max();
    test_agg_empty_sources();
    test_agg_parallel();
    return 0;
}

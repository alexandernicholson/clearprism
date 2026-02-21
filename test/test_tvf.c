/*
 * test_tvf.c — Tests for clearprism_query table-valued function
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);
extern int clearprism_init(sqlite3 *db);

static const char *TVF_REG_PATH = "/tmp/clearprism_tvf_test_registry.db";
static const char *TVF_SRC1_PATH = "/tmp/clearprism_tvf_test_src1.db";
static const char *TVF_SRC2_PATH = "/tmp/clearprism_tvf_test_src2.db";

static void tvf_cleanup(void)
{
    unlink(TVF_REG_PATH);
    unlink(TVF_SRC1_PATH);
    unlink(TVF_SRC2_PATH);
}

static void tvf_setup(void)
{
    tvf_cleanup();

    /* Create registry */
    sqlite3 *db = NULL;
    sqlite3_open(TVF_REG_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE clearprism_sources ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  path TEXT NOT NULL UNIQUE,"
        "  alias TEXT NOT NULL UNIQUE,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  priority INTEGER NOT NULL DEFAULT 0,"
        "  added_at TEXT NOT NULL DEFAULT (datetime('now')),"
        "  notes TEXT);"
        "CREATE TABLE clearprism_table_overrides ("
        "  source_id INTEGER NOT NULL,"
        "  table_name TEXT NOT NULL,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  PRIMARY KEY (source_id, table_name));",
        NULL, NULL, NULL);

    char *sql;
    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias) VALUES ('%s', 'merchant_a')",
        TVF_SRC1_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias) VALUES ('%s', 'merchant_b')",
        TVF_SRC2_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    sqlite3_close(db);

    /* Source 1: items with categories */
    sqlite3_open(TVF_SRC1_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, value REAL);"
        "INSERT INTO items VALUES (1, 'food', 10.0);"
        "INSERT INTO items VALUES (2, 'food', 20.0);"
        "INSERT INTO items VALUES (3, 'drink', 5.0);",
        NULL, NULL, NULL);
    sqlite3_close(db);

    /* Source 2: items with categories */
    sqlite3_open(TVF_SRC2_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, value REAL);"
        "INSERT INTO items VALUES (1, 'food', 30.0);"
        "INSERT INTO items VALUES (2, 'drink', 15.0);",
        NULL, NULL, NULL);
    sqlite3_close(db);
}

/* Test: basic clearprism_query returns rows from all sources */
static void test_tvf_basic_query(void)
{
    tvf_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *create_sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE fed USING clearprism("
        "  registry_db='%s', table='items', cache_db='none')", TVF_REG_PATH);
    sqlite3_exec(db, create_sql, NULL, NULL, NULL);
    sqlite3_free(create_sql);

    /* Query: GROUP BY category, pushed to each source.
     * Uses generic column names c0, c1, c2 from the TVF. */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT * FROM clearprism_query('fed',"
        "  'SELECT category, count(*) as cnt, sum(value) as total"
        "   FROM items GROUP BY category')",
        -1, &stmt, NULL);

    int passed = (rc == SQLITE_OK);
    int n_rows = 0;
    if (passed) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            n_rows++;
        }
    }
    sqlite3_finalize(stmt);

    /* 2 sources x 2 categories = 4 rows (food+drink from each) */
    passed = passed && (n_rows == 4);
    test_report("tvf basic query returns rows from all sources", passed);

    sqlite3_exec(db, "DROP TABLE fed", NULL, NULL, NULL);
    sqlite3_close(db);
    tvf_cleanup();
}

/* Test: _source_db column is appended */
static void test_tvf_source_db_column(void)
{
    tvf_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *create_sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE fed USING clearprism("
        "  registry_db='%s', table='items', cache_db='none')", TVF_REG_PATH);
    sqlite3_exec(db, create_sql, NULL, NULL, NULL);
    sqlite3_free(create_sql);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT * FROM clearprism_query('fed',"
        "  'SELECT count(*) as cnt FROM items')",
        -1, &stmt, NULL);

    int passed = (rc == SQLITE_OK);
    int got_merchant_a = 0, got_merchant_b = 0;
    int n_cols = 0;
    if (passed) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            n_cols = sqlite3_column_count(stmt);
            /* The _source_db column is the last named column in the schema.
             * With 1 result column mapped to c0, _source_db is at a fixed
             * position. Check all columns for the alias text. */
            for (int i = 0; i < n_cols; i++) {
                const char *val = (const char *)sqlite3_column_text(stmt, i);
                if (val && strcmp(val, "merchant_a") == 0) got_merchant_a = 1;
                if (val && strcmp(val, "merchant_b") == 0) got_merchant_b = 1;
            }
        }
    }
    sqlite3_finalize(stmt);

    /* Both sources should be present */
    passed = passed && got_merchant_a && got_merchant_b;
    test_report("tvf _source_db column appended", passed);

    sqlite3_exec(db, "DROP TABLE fed", NULL, NULL, NULL);
    sqlite3_close(db);
    tvf_cleanup();
}

/* Test: outer aggregation across shards works.
 * Uses generic column names: c0 = category, c1 = cnt, c2 = total */
static void test_tvf_cross_shard_aggregation(void)
{
    tvf_setup();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *create_sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE fed USING clearprism("
        "  registry_db='%s', table='items', cache_db='none')", TVF_REG_PATH);
    sqlite3_exec(db, create_sql, NULL, NULL, NULL);
    sqlite3_free(create_sql);

    /* Two-level aggregation: per-shard GROUP BY, then outer merge.
     * c0 = category, c1 = cnt, c2 = total */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT c0, sum(c1) as total_count, sum(c2) as total_value"
        " FROM clearprism_query('fed',"
        "  'SELECT category, count(*) as cnt, sum(value) as total"
        "   FROM items GROUP BY category')"
        " GROUP BY c0 ORDER BY c0",
        -1, &stmt, NULL);

    int passed = (rc == SQLITE_OK);
    double drink_total = 0, food_total = 0;
    int drink_count = 0, food_count = 0;
    if (passed) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *cat = (const char *)sqlite3_column_text(stmt, 0);
            if (cat && strcmp(cat, "drink") == 0) {
                drink_count = sqlite3_column_int(stmt, 1);
                drink_total = sqlite3_column_double(stmt, 2);
            } else if (cat && strcmp(cat, "food") == 0) {
                food_count = sqlite3_column_int(stmt, 1);
                food_total = sqlite3_column_double(stmt, 2);
            }
        }
    }
    sqlite3_finalize(stmt);

    /* drink: src1 has 1 row (5.0), src2 has 1 row (15.0) -> 2 rows, 20.0 */
    /* food: src1 has 2 rows (30.0), src2 has 1 row (30.0) -> 3 rows, 60.0 */
    passed = passed && (drink_count == 2) && (food_count == 3) &&
             (drink_total > 19.9 && drink_total < 20.1) &&
             (food_total > 59.9 && food_total < 60.1);
    test_report("tvf cross-shard aggregation", passed);

    sqlite3_exec(db, "DROP TABLE fed", NULL, NULL, NULL);
    sqlite3_close(db);
    tvf_cleanup();
}

/* Test: clearprism_query with no active sources returns 0 rows */
static void test_tvf_empty_sources(void)
{
    tvf_cleanup();

    /* Create registry with no sources */
    sqlite3 *db = NULL;
    sqlite3_open(TVF_REG_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE clearprism_sources ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  path TEXT NOT NULL UNIQUE,"
        "  alias TEXT NOT NULL UNIQUE,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  priority INTEGER NOT NULL DEFAULT 0,"
        "  added_at TEXT NOT NULL DEFAULT (datetime('now')),"
        "  notes TEXT);"
        "CREATE TABLE clearprism_table_overrides ("
        "  source_id INTEGER NOT NULL,"
        "  table_name TEXT NOT NULL,"
        "  active INTEGER NOT NULL DEFAULT 1,"
        "  PRIMARY KEY (source_id, table_name));",
        NULL, NULL, NULL);
    sqlite3_close(db);

    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *create_sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE fed USING clearprism("
        "  registry_db='%s', table='items', cache_db='none')", TVF_REG_PATH);
    sqlite3_exec(db, create_sql, NULL, NULL, NULL);
    sqlite3_free(create_sql);

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT * FROM clearprism_query('fed', 'SELECT 1')",
        -1, &stmt, NULL);

    int passed = (rc == SQLITE_OK);
    int n_rows = 0;
    if (passed) {
        while (sqlite3_step(stmt) == SQLITE_ROW)
            n_rows++;
    }
    sqlite3_finalize(stmt);

    passed = passed && (n_rows == 0);
    test_report("tvf empty sources returns 0 rows", passed);

    sqlite3_exec(db, "DROP TABLE fed", NULL, NULL, NULL);
    sqlite3_close(db);
    tvf_cleanup();
}

int test_tvf_run(void)
{
    test_tvf_basic_query();
    test_tvf_source_db_column();
    test_tvf_cross_shard_aggregation();
    test_tvf_empty_sources();
    return 0;
}

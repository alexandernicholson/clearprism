/*
 * test_vtab.c — Virtual table end-to-end tests
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);

/* Declared in clearprism_main.c for SQLITE_CORE builds */
extern int clearprism_init(sqlite3 *db);

/* Helper: create a registry and source databases for testing */
static const char *REG_PATH = "/tmp/clearprism_vtab_test_registry.db";
static const char *SRC1_PATH = "/tmp/clearprism_vtab_test_src1.db";
static const char *SRC2_PATH = "/tmp/clearprism_vtab_test_src2.db";
static const char *SRC3_PATH = "/tmp/clearprism_vtab_test_src3.db";
static const char *CACHE_PATH = "/tmp/clearprism_vtab_test_cache.db";

static void cleanup_test_files(void)
{
    unlink(REG_PATH);
    unlink(SRC1_PATH);
    unlink(SRC2_PATH);
    unlink(SRC3_PATH);
    unlink(CACHE_PATH);
    /* WAL and SHM files */
    unlink("/tmp/clearprism_vtab_test_cache.db-wal");
    unlink("/tmp/clearprism_vtab_test_cache.db-shm");
}

static int setup_test_environment(void)
{
    cleanup_test_files();

    /* Create registry */
    sqlite3 *db = NULL;
    sqlite3_open(REG_PATH, &db);
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
        "INSERT INTO clearprism_sources (path, alias, priority) VALUES ('%s', 'east', 0)",
        SRC1_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias, priority) VALUES ('%s', 'west', 1)",
        SRC2_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias, priority) VALUES ('%s', 'north', 2)",
        SRC3_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    sqlite3_close(db);

    /* Create source 1: east */
    sqlite3_open(SRC1_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);"
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');"
        "INSERT INTO users VALUES (2, 'Bob', 'bob@east.com');",
        NULL, NULL, NULL);
    sqlite3_close(db);

    /* Create source 2: west */
    sqlite3_open(SRC2_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);"
        "INSERT INTO users VALUES (10, 'Charlie', 'charlie@example.com');"
        "INSERT INTO users VALUES (11, 'Diana', 'diana@west.com');"
        "INSERT INTO users VALUES (12, 'Eve', 'eve@example.com');",
        NULL, NULL, NULL);
    sqlite3_close(db);

    /* Create source 3: north */
    sqlite3_open(SRC3_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);"
        "INSERT INTO users VALUES (20, 'Frank', 'frank@north.com');",
        NULL, NULL, NULL);
    sqlite3_close(db);

    return 0;
}

static void test_vtab_basic_select(void)
{
    setup_test_environment();

    sqlite3 *db = NULL;
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        test_report("vtab_basic_select (open)", 0);
        cleanup_test_files();
        return;
    }

    /* Register the module */
    rc = clearprism_init(db);
    test_report("module registration", rc == SQLITE_OK);

    /* Create virtual table */
    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE unified_users USING clearprism("
        "  registry_db='%s',"
        "  table='users'"
        ")", REG_PATH);

    char *err = NULL;
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("CREATE VIRTUAL TABLE succeeds", rc == SQLITE_OK);
    if (err) {
        printf("    error: %s\n", err);
        sqlite3_free(err);
    }

    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        cleanup_test_files();
        return;
    }

    /* SELECT all rows */
    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db,
        "SELECT name, email, _source_db FROM unified_users ORDER BY name",
        -1, &stmt, NULL);
    test_report("SELECT prepare succeeds", rc == SQLITE_OK);

    if (rc == SQLITE_OK) {
        int row_count = 0;
        int has_alice = 0, has_charlie = 0, has_frank = 0;
        int source_correct = 1;

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *name = (const char *)sqlite3_column_text(stmt, 0);
            const char *source = (const char *)sqlite3_column_text(stmt, 2);

            if (name && strcmp(name, "Alice") == 0) {
                has_alice = 1;
                if (!source || strcmp(source, "east") != 0) source_correct = 0;
            }
            if (name && strcmp(name, "Charlie") == 0) {
                has_charlie = 1;
                if (!source || strcmp(source, "west") != 0) source_correct = 0;
            }
            if (name && strcmp(name, "Frank") == 0) {
                has_frank = 1;
                if (!source || strcmp(source, "north") != 0) source_correct = 0;
            }
            row_count++;
        }
        sqlite3_finalize(stmt);

        test_report("SELECT returns 6 rows total", row_count == 6);
        test_report("has Alice from east", has_alice);
        test_report("has Charlie from west", has_charlie);
        test_report("has Frank from north", has_frank);
        test_report("_source_db values correct", source_correct);
    }

    /* Drop virtual table */
    sqlite3_exec(db, "DROP TABLE unified_users", NULL, NULL, NULL);
    sqlite3_close(db);
    cleanup_test_files();
}

static void test_vtab_source_filter(void)
{
    setup_test_environment();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE unified_users USING clearprism("
        "  registry_db='%s',"
        "  table='users'"
        ")", REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Filter by _source_db */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT name FROM unified_users WHERE _source_db = 'west'",
        -1, &stmt, NULL);
    test_report("source filter prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK) {
        int row_count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            row_count++;
        }
        sqlite3_finalize(stmt);

        test_report("source filter returns 3 rows (west)", row_count == 3);
    }

    sqlite3_exec(db, "DROP TABLE unified_users", NULL, NULL, NULL);
    sqlite3_close(db);
    cleanup_test_files();
}

static void test_vtab_where_pushdown(void)
{
    setup_test_environment();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE unified_users USING clearprism("
        "  registry_db='%s',"
        "  table='users'"
        ")", REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* WHERE pushdown with LIKE */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT name, email, _source_db FROM unified_users "
        "WHERE email LIKE '%@example.com'",
        -1, &stmt, NULL);
    test_report("WHERE LIKE prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK) {
        int row_count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            row_count++;
        }
        sqlite3_finalize(stmt);

        /* Alice, Charlie, Eve all have @example.com */
        test_report("WHERE LIKE returns 3 rows", row_count == 3);
    }

    /* WHERE pushdown with EQ */
    rc = sqlite3_prepare_v2(db,
        "SELECT name FROM unified_users WHERE name = 'Bob'",
        -1, &stmt, NULL);
    test_report("WHERE EQ prepare", rc == SQLITE_OK);

    if (rc == SQLITE_OK) {
        int row_count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            row_count++;
        }
        sqlite3_finalize(stmt);

        test_report("WHERE EQ returns 1 row", row_count == 1);
    }

    sqlite3_exec(db, "DROP TABLE unified_users", NULL, NULL, NULL);
    sqlite3_close(db);
    cleanup_test_files();
}

static void test_vtab_empty_result(void)
{
    setup_test_environment();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE unified_users USING clearprism("
        "  registry_db='%s',"
        "  table='users'"
        ")", REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Query for non-existent source */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT * FROM unified_users WHERE _source_db = 'nonexistent'",
        -1, &stmt, NULL);

    if (rc == SQLITE_OK) {
        int row_count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            row_count++;
        }
        sqlite3_finalize(stmt);
        test_report("nonexistent source returns 0 rows", row_count == 0);
    } else {
        test_report("nonexistent source prepare", 0);
    }

    sqlite3_exec(db, "DROP TABLE unified_users", NULL, NULL, NULL);
    sqlite3_close(db);
    cleanup_test_files();
}

static void test_vtab_with_cache(void)
{
    setup_test_environment();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE unified_users USING clearprism("
        "  registry_db='%s',"
        "  table='users',"
        "  cache_db='%s',"
        "  l1_max_rows=100,"
        "  l2_refresh_sec=300"
        ")", REG_PATH, CACHE_PATH);

    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    sqlite3_free(sql);
    test_report("CREATE with cache_db succeeds", rc == SQLITE_OK);
    if (err) {
        printf("    error: %s\n", err);
        sqlite3_free(err);
    }

    if (rc == SQLITE_OK) {
        sqlite3_stmt *stmt = NULL;
        rc = sqlite3_prepare_v2(db,
            "SELECT count(*) FROM unified_users", -1, &stmt, NULL);
        if (rc == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int count = sqlite3_column_int(stmt, 0);
                if (count != 6) printf("    got count=%d\n", count);
                test_report("SELECT count(*) with cache = 6", count == 6);
            }
            sqlite3_finalize(stmt);
        }

        sqlite3_exec(db, "DROP TABLE unified_users", NULL, NULL, NULL);
    }

    sqlite3_close(db);
    cleanup_test_files();
}

static void test_vtab_rowid(void)
{
    setup_test_environment();

    sqlite3 *db = NULL;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE unified_users USING clearprism("
        "  registry_db='%s',"
        "  table='users'"
        ")", REG_PATH);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);

    /* Check that rowids are monotonically increasing */
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT rowid FROM unified_users",
        -1, &stmt, NULL);

    if (rc == SQLITE_OK) {
        int64_t prev_rowid = -1;
        int monotonic = 1;
        int row_count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int64_t rid = sqlite3_column_int64(stmt, 0);
            if (rid <= prev_rowid && row_count > 0) {
                monotonic = 0;
            }
            prev_rowid = rid;
            row_count++;
        }
        sqlite3_finalize(stmt);
        test_report("rowids are monotonically increasing", monotonic);
    }

    sqlite3_exec(db, "DROP TABLE unified_users", NULL, NULL, NULL);
    sqlite3_close(db);
    cleanup_test_files();
}

int test_vtab_run(void)
{
    test_vtab_basic_select();
    test_vtab_source_filter();
    test_vtab_where_pushdown();
    test_vtab_empty_result();
    test_vtab_with_cache();
    test_vtab_rowid();
    return 0;
}

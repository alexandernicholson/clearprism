/*
 * test_jolie_integration.c — End-to-end Clearprism + Jolie integration tests
 *
 * Requires libjolie.so to be built. Set JOLIE_LIB_PATH env var to override.
 * Skipped automatically if libjolie.so is not found.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);
extern int clearprism_init(sqlite3 *db);

static const char *get_jolie_path(void)
{
    const char *env = getenv("JOLIE_LIB_PATH");
    if (env) return env;
    /* Default path relative to clearprism project root */
    return "../jolie/target/release/libjolie";
}

static int jolie_available(void)
{
#ifdef SQLITE_OMIT_LOAD_EXTENSION
    return 0;
#else
    const char *path = get_jolie_path();
    /* Try loading into a throwaway connection */
    sqlite3 *db;
    sqlite3_open(":memory:", &db);
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL);
    char *err = NULL;
    int rc = sqlite3_load_extension(db, path, NULL, &err);
    sqlite3_free(err);
    sqlite3_close(db);
    return (rc == SQLITE_OK);
#endif
}

/* Helper: create a jolie-backed source database */
static void create_jolie_source(const char *db_path, const char *data_sql)
{
    unlink(db_path);
    sqlite3 *db;
    sqlite3_open(db_path, &db);
#ifndef SQLITE_OMIT_LOAD_EXTENSION
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1, NULL);
    sqlite3_load_extension(db, get_jolie_path(), NULL, NULL);
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0, NULL);
#endif

    /* Create vanilla table and insert data */
    sqlite3_exec(db,
        "CREATE TABLE reservations ("
        "  id INTEGER PRIMARY KEY, date INTEGER, shop_id INTEGER,"
        "  status TEXT, domain TEXT, amount REAL);",
        NULL, NULL, NULL);
    sqlite3_exec(db, data_sql, NULL, NULL, NULL);

    /* Create jolie columnar copy */
    sqlite3_exec(db,
        "CREATE VIRTUAL TABLE reservations_col USING jolie("
        "  id INTEGER, date INTEGER, shop_id INTEGER,"
        "  status TEXT, domain TEXT, amount REAL);",
        NULL, NULL, NULL);
    sqlite3_exec(db,
        "INSERT INTO reservations_col SELECT * FROM reservations;",
        NULL, NULL, NULL);
    sqlite3_exec(db, "SELECT jolie_flush('reservations_col');", NULL, NULL, NULL);
    sqlite3_close(db);
}

void test_jolie_clearprism_query(void)
{
    if (!jolie_available()) {
        test_report("jolie+clearprism query (SKIPPED - no libjolie)", 1);
        return;
    }

    const char *reg = "/tmp/cp_jolie_int_registry.db";
    const char *src1 = "/tmp/cp_jolie_int_src1.db";
    const char *src2 = "/tmp/cp_jolie_int_src2.db";
    unlink(reg);

    /* Merchant A: 3 reservations */
    create_jolie_source(src1,
        "INSERT INTO reservations VALUES (1, 20240101, 1, 'confirmed', 'active', 100.0);"
        "INSERT INTO reservations VALUES (2, 20240102, 1, 'confirmed', 'active', 200.0);"
        "INSERT INTO reservations VALUES (3, 20240103, 2, 'cancelled', 'cancelled', 50.0);");

    /* Merchant B: 2 reservations */
    create_jolie_source(src2,
        "INSERT INTO reservations VALUES (1, 20240101, 1, 'confirmed', 'active', 150.0);"
        "INSERT INTO reservations VALUES (2, 20240102, 1, 'cancelled', 'cancelled', 75.0);");

    /* Create registry */
    sqlite3 *reg_db;
    sqlite3_open(reg, &reg_db);
    sqlite3_exec(reg_db,
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
        "INSERT INTO clearprism_sources (path, alias) VALUES ('%s', 'merchant_a')", src1);
    sqlite3_exec(reg_db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias) VALUES ('%s', 'merchant_b')", src2);
    sqlite3_exec(reg_db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    sqlite3_close(reg_db);

    /* Open host connection with clearprism */
    sqlite3 *db;
    sqlite3_open(":memory:", &db);
    clearprism_init(db);

    char *create_sql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE analytics USING clearprism("
        "  registry_db='%s', table='reservations_col',"
        "  load_extension='%s', cache_db='none')",
        reg, get_jolie_path());
    int rc = sqlite3_exec(db, create_sql, NULL, NULL, NULL);
    sqlite3_free(create_sql);

    if (rc != SQLITE_OK) {
        test_report("jolie+clearprism query (vtab create failed)", 0);
        sqlite3_close(db);
        unlink(reg); unlink(src1); unlink(src2);
        return;
    }

    /* Cross-shard analytical query via clearprism_query
       Note: uses {t} placeholder which gets replaced with target_table (reservations_col)
       Uses c0, c1, c2 generic column names from TVF */
    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db,
        "SELECT c0, sum(c1), sum(c2)"
        " FROM clearprism_query('analytics',"
        "  'SELECT domain, count(*) as cnt, sum(amount) as total"
        "   FROM {t} GROUP BY domain')"
        " GROUP BY c0 ORDER BY c0",
        -1, &stmt, NULL);

    double active_total = 0, cancelled_total = 0;
    int active_count = 0, cancelled_count = 0;
    if (rc == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *dom = (const char *)sqlite3_column_text(stmt, 0);
            if (dom && strcmp(dom, "active") == 0) {
                active_count = sqlite3_column_int(stmt, 1);
                active_total = sqlite3_column_double(stmt, 2);
            } else if (dom && strcmp(dom, "cancelled") == 0) {
                cancelled_count = sqlite3_column_int(stmt, 1);
                cancelled_total = sqlite3_column_double(stmt, 2);
            }
        }
    }
    sqlite3_finalize(stmt);

    /* active: merchant_a has 2 (300.0), merchant_b has 1 (150.0) -> 3, 450.0 */
    /* cancelled: merchant_a has 1 (50.0), merchant_b has 1 (75.0) -> 2, 125.0 */
    int passed = (active_count == 3) && (cancelled_count == 2) &&
                 (active_total > 449.9 && active_total < 450.1) &&
                 (cancelled_total > 124.9 && cancelled_total < 125.1);
    test_report("jolie+clearprism cross-shard analytical query", passed);

    sqlite3_exec(db, "DROP TABLE analytics", NULL, NULL, NULL);
    sqlite3_close(db);
    unlink(reg); unlink(src1); unlink(src2);
}

int test_jolie_integration_run(void)
{
    int failed = 0;
    test_jolie_clearprism_query();
    return failed;
}

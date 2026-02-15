/*
 * test_scanner.c — Tests for the streaming scanner API
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);

#define SCAN_REG_PATH "/tmp/clearprism_scan_test_registry.db"
#define SCAN_MAX_SOURCES 10

static void scan_cleanup(void)
{
    unlink(SCAN_REG_PATH);
    for (int i = 0; i < SCAN_MAX_SOURCES; i++) {
        char path[256];
        snprintf(path, sizeof(path), "/tmp/clearprism_scan_test_src%d.db", i);
        unlink(path);
    }
}

/*
 * Create a registry with n_sources source databases, each containing
 * rows_per_source rows in an "items" table with schema:
 *   id INTEGER PRIMARY KEY, category TEXT, name TEXT, value REAL
 */
static void scan_setup(int n_sources, int rows_per_source)
{
    scan_cleanup();

    sqlite3 *db = NULL;
    sqlite3_open(SCAN_REG_PATH, &db);
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

    for (int s = 0; s < n_sources; s++) {
        char path[256];
        snprintf(path, sizeof(path),
                 "/tmp/clearprism_scan_test_src%d.db", s);

        char *sql = sqlite3_mprintf(
            "INSERT INTO clearprism_sources (path, alias, priority) "
            "VALUES ('%s', 'src_%d', %d)", path, s, s);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
        sqlite3_free(sql);

        sqlite3 *sdb = NULL;
        sqlite3_open(path, &sdb);
        sqlite3_exec(sdb,
            "CREATE TABLE items ("
            "  id INTEGER PRIMARY KEY,"
            "  category TEXT,"
            "  name TEXT,"
            "  value REAL"
            ");"
            "CREATE INDEX idx_items_category ON items(category);",
            NULL, NULL, NULL);

        sqlite3_exec(sdb, "BEGIN", NULL, NULL, NULL);
        for (int r = 1; r <= rows_per_source; r++) {
            int global_id = s * rows_per_source + r;
            char isql[512];
            snprintf(isql, sizeof(isql),
                "INSERT INTO items VALUES (%d, 'cat_%d', 'item_%d', %d.0)",
                r, r % 5, global_id, global_id);
            sqlite3_exec(sdb, isql, NULL, NULL, NULL);
        }
        sqlite3_exec(sdb, "COMMIT", NULL, NULL, NULL);
        sqlite3_close(sdb);
    }
    sqlite3_close(db);
}

/* ========== Tests ========== */

static void test_scanner_basic_full_scan(void)
{
    scan_setup(3, 100);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_basic: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int row_count = 0;
    while (clearprism_scan_next(s)) row_count++;
    test_report("scanner_basic: returns 300 rows", row_count == 300);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_column_accessors(void)
{
    scan_setup(2, 50);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_columns: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int ok_int = 1, ok_text = 1, ok_double = 1, ok_types = 1;
    int row_count = 0;
    while (clearprism_scan_next(s)) {
        int64_t id = clearprism_scan_int64(s, 0);
        const char *category = clearprism_scan_text(s, 1);
        const char *name = clearprism_scan_text(s, 2);
        double value = clearprism_scan_double(s, 3);

        if (id < 1 || id > 50) ok_int = 0;
        if (!category) ok_text = 0;
        if (!name) ok_text = 0;
        if (value < 1.0) ok_double = 0;

        /* Verify type accessors */
        if (clearprism_scan_type(s, 0) != SQLITE_INTEGER) ok_types = 0;
        if (clearprism_scan_type(s, 1) != SQLITE_TEXT) ok_types = 0;
        if (clearprism_scan_type(s, 3) != SQLITE_FLOAT) ok_types = 0;

        row_count++;
    }

    test_report("scanner_columns: row count", row_count == 100);
    test_report("scanner_columns: int64 values valid", ok_int);
    test_report("scanner_columns: text values non-null", ok_text);
    test_report("scanner_columns: double values valid", ok_double);
    test_report("scanner_columns: type accessors correct", ok_types);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_source_alias(void)
{
    scan_setup(3, 10);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_alias: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int has_src0 = 0, has_src1 = 0, has_src2 = 0;
    int all_non_null = 1;
    while (clearprism_scan_next(s)) {
        const char *alias = clearprism_scan_source_alias(s);
        if (!alias) { all_non_null = 0; continue; }
        if (strcmp(alias, "src_0") == 0) has_src0 = 1;
        if (strcmp(alias, "src_1") == 0) has_src1 = 1;
        if (strcmp(alias, "src_2") == 0) has_src2 = 1;
    }

    test_report("scanner_alias: all non-null", all_non_null);
    test_report("scanner_alias: has all sources",
                has_src0 && has_src1 && has_src2);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_column_metadata(void)
{
    scan_setup(1, 10);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_meta: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int ncol = clearprism_scan_column_count(s);
    test_report("scanner_meta: 4 columns", ncol == 4);

    const char *c0 = clearprism_scan_column_name(s, 0);
    const char *c1 = clearprism_scan_column_name(s, 1);
    const char *c2 = clearprism_scan_column_name(s, 2);
    const char *c3 = clearprism_scan_column_name(s, 3);
    test_report("scanner_meta: col 0 is 'id'",
                c0 && strcmp(c0, "id") == 0);
    test_report("scanner_meta: col 1 is 'category'",
                c1 && strcmp(c1, "category") == 0);
    test_report("scanner_meta: col 2 is 'name'",
                c2 && strcmp(c2, "name") == 0);
    test_report("scanner_meta: col 3 is 'value'",
                c3 && strcmp(c3, "value") == 0);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_where_filter(void)
{
    scan_setup(3, 100);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_filter: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int rc = clearprism_scan_filter(s, "category = ?");
    test_report("scanner_filter: set filter", rc == SQLITE_OK);

    rc = clearprism_scan_bind_text(s, 1, "cat_0");
    test_report("scanner_filter: bind text", rc == SQLITE_OK);

    int row_count = 0;
    int all_match = 1;
    while (clearprism_scan_next(s)) {
        const char *cat = clearprism_scan_text(s, 1);
        if (!cat || strcmp(cat, "cat_0") != 0) all_match = 0;
        row_count++;
    }

    /* Each source has 100 rows, id 1-100, cat = id%5.
     * cat_0 matches ids 5,10,15,...100 = 20 rows per source.
     * 3 sources = 60 rows. */
    test_report("scanner_filter: returns 60 rows", row_count == 60);
    test_report("scanner_filter: all rows match filter", all_match);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_where_range(void)
{
    scan_setup(2, 50);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_range: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    clearprism_scan_filter(s, "value > ? AND value <= ?");
    clearprism_scan_bind_double(s, 1, 40.0);
    clearprism_scan_bind_double(s, 2, 60.0);

    int row_count = 0;
    int all_in_range = 1;
    while (clearprism_scan_next(s)) {
        double val = clearprism_scan_double(s, 3);
        if (val <= 40.0 || val > 60.0) all_in_range = 0;
        row_count++;
    }

    /* src_0: value = 1..50, so 41..50 match = 10 rows
     * src_1: value = 51..100, so 51..60 match = 10 rows
     * Total = 20 */
    test_report("scanner_range: returns 20 rows", row_count == 20);
    test_report("scanner_range: all in range", all_in_range);

    clearprism_scan_close(s);
    scan_cleanup();
}

typedef struct { int count; double sum; } scan_each_agg_ctx;

static int scan_stop_callback(clearprism_scanner *sc, void *arg) {
    (void)sc;
    scan_each_agg_ctx *c = (scan_each_agg_ctx *)arg;
    c->count++;
    return c->count >= 50 ? 1 : 0;
}

static int scan_each_callback(clearprism_scanner *sc, void *arg) {
    scan_each_agg_ctx *a = (scan_each_agg_ctx *)arg;
    a->count++;
    a->sum += clearprism_scan_double(sc, 3);
    return 0;
}

static void test_scanner_scan_each(void)
{
    scan_setup(3, 100);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_each: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    scan_each_agg_ctx ctx = {0, 0.0};

    int rc = clearprism_scan_each(s, scan_each_callback, &ctx);
    test_report("scanner_each: returns OK", rc == SQLITE_OK);
    test_report("scanner_each: counted 300 rows", ctx.count == 300);

    /* Sum of values: src0 = 1+2+...+100 = 5050,
     * src1 = 101+102+...+200 = 15050,
     * src2 = 201+202+...+300 = 25050
     * Total = 45150 */
    test_report("scanner_each: sum correct",
                ctx.sum > 45149.0 && ctx.sum < 45151.0);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_early_stop(void)
{
    scan_setup(3, 100);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_stop: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    /* Stop after 50 rows */
    scan_each_agg_ctx ctx = {0, 0.0};

    int rc = clearprism_scan_each(s, scan_stop_callback, &ctx);
    test_report("scanner_stop: callback returned non-zero", rc == 1);
    test_report("scanner_stop: stopped at 50 rows", ctx.count == 50);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_empty_sources(void)
{
    scan_setup(3, 0);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_empty: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int row_count = 0;
    while (clearprism_scan_next(s)) row_count++;
    test_report("scanner_empty: returns 0 rows", row_count == 0);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_single_source(void)
{
    scan_setup(1, 200);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_single: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int row_count = 0;
    const char *last_alias = NULL;
    while (clearprism_scan_next(s)) {
        last_alias = clearprism_scan_source_alias(s);
        row_count++;
    }

    test_report("scanner_single: returns 200 rows", row_count == 200);
    test_report("scanner_single: alias is src_0",
                last_alias && strcmp(last_alias, "src_0") == 0);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_null_handling(void)
{
    /* Create a source with NULL values */
    scan_cleanup();

    sqlite3 *db = NULL;
    sqlite3_open(SCAN_REG_PATH, &db);
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

    const char *src_path = "/tmp/clearprism_scan_test_src0.db";
    char *sql = sqlite3_mprintf(
        "INSERT INTO clearprism_sources (path, alias) VALUES ('%s', 'src_0')",
        src_path);
    sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    sqlite3_close(db);

    sqlite3 *sdb = NULL;
    sqlite3_open(src_path, &sdb);
    sqlite3_exec(sdb,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, "
        "name TEXT, value REAL);"
        "INSERT INTO items VALUES (1, NULL, 'test', 42.0);"
        "INSERT INTO items VALUES (2, 'cat_1', NULL, NULL);",
        NULL, NULL, NULL);
    sqlite3_close(sdb);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_null: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int found_null_cat = 0, found_null_name = 0, found_null_val = 0;
    while (clearprism_scan_next(s)) {
        if (clearprism_scan_is_null(s, 1)) found_null_cat = 1;
        if (clearprism_scan_is_null(s, 2)) found_null_name = 1;
        if (clearprism_scan_is_null(s, 3)) found_null_val = 1;
    }

    test_report("scanner_null: detected null category", found_null_cat);
    test_report("scanner_null: detected null name", found_null_name);
    test_report("scanner_null: detected null value", found_null_val);

    clearprism_scan_close(s);
    scan_cleanup();
}

static void test_scanner_source_id(void)
{
    scan_setup(3, 10);

    clearprism_scanner *s = clearprism_scan_open(SCAN_REG_PATH, "items");
    test_report("scanner_srcid: open", s != NULL);
    if (!s) { scan_cleanup(); return; }

    int64_t seen_ids[3] = {0, 0, 0};
    int n_distinct = 0;
    while (clearprism_scan_next(s)) {
        int64_t id = clearprism_scan_source_id(s);
        int found = 0;
        for (int i = 0; i < n_distinct; i++) {
            if (seen_ids[i] == id) { found = 1; break; }
        }
        if (!found && n_distinct < 3) {
            seen_ids[n_distinct++] = id;
        }
    }

    test_report("scanner_srcid: 3 distinct source ids", n_distinct == 3);

    clearprism_scan_close(s);
    scan_cleanup();
}

/* ========== Runner ========== */

int test_scanner_run(void)
{
    test_scanner_basic_full_scan();
    test_scanner_column_accessors();
    test_scanner_source_alias();
    test_scanner_column_metadata();
    test_scanner_where_filter();
    test_scanner_where_range();
    test_scanner_scan_each();
    test_scanner_early_stop();
    test_scanner_empty_sources();
    test_scanner_single_source();
    test_scanner_null_handling();
    test_scanner_source_id();

    return 0;
}

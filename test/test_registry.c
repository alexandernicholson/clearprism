/*
 * test_registry.c — Registry tests
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);

/* Helper: create a test registry database */
static int create_test_registry(const char *path, int n_sources)
{
    sqlite3 *db = NULL;
    int rc = sqlite3_open(path, &db);
    if (rc != SQLITE_OK) return rc;

    const char *schema =
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
        ");";

    char *err = NULL;
    rc = sqlite3_exec(db, schema, NULL, NULL, &err);
    sqlite3_free(err);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return rc;
    }

    /* Insert sources */
    for (int i = 0; i < n_sources; i++) {
        char *sql = sqlite3_mprintf(
            "INSERT INTO clearprism_sources (path, alias, active, priority) "
            "VALUES ('/tmp/clearprism_test_src_%d.db', 'source_%d', 1, %d)",
            i, i, i);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
        sqlite3_free(sql);
    }

    sqlite3_close(db);
    return SQLITE_OK;
}

/* Helper: create a test source database with a users table
   (used by tests that need source DBs) */
static int create_test_source(const char *path) __attribute__((unused));
static int create_test_source(const char *path)
{
    sqlite3 *db = NULL;
    int rc = sqlite3_open(path, &db);
    if (rc != SQLITE_OK) return rc;

    const char *schema =
        "CREATE TABLE users ("
        "  id INTEGER PRIMARY KEY,"
        "  name TEXT NOT NULL,"
        "  email TEXT"
        ");";

    char *err = NULL;
    rc = sqlite3_exec(db, schema, NULL, NULL, &err);
    sqlite3_free(err);
    sqlite3_close(db);
    return rc;
}

static void test_registry_open_close(void)
{
    const char *path = "/tmp/clearprism_test_registry.db";
    unlink(path);
    create_test_registry(path, 3);

    char *errmsg = NULL;
    clearprism_registry *reg = clearprism_registry_open(path, &errmsg);
    test_report("registry_open returns non-NULL", reg != NULL);
    if (errmsg) {
        printf("    error: %s\n", errmsg);
        sqlite3_free(errmsg);
    }

    if (reg) {
        test_report("registry has 3 sources", reg->n_sources == 3);
        clearprism_registry_close(reg);
    }

    unlink(path);
}

static void test_registry_snapshot(void)
{
    const char *path = "/tmp/clearprism_test_registry2.db";
    unlink(path);
    create_test_registry(path, 5);

    char *errmsg = NULL;
    clearprism_registry *reg = clearprism_registry_open(path, &errmsg);
    sqlite3_free(errmsg);

    if (!reg) {
        test_report("registry_snapshot (setup)", 0);
        unlink(path);
        return;
    }

    clearprism_source *sources = NULL;
    int n = 0;
    int rc = clearprism_registry_snapshot(reg, "users", &sources, &n, &errmsg);
    sqlite3_free(errmsg);

    test_report("snapshot returns OK", rc == SQLITE_OK);
    test_report("snapshot has 5 sources", n == 5);

    if (sources && n > 0) {
        test_report("first source alias is 'source_0'",
                     strcmp(sources[0].alias, "source_0") == 0);
        test_report("sources sorted by priority",
                     n >= 2 && sources[0].priority <= sources[1].priority);
    }

    clearprism_sources_free(sources, n);
    clearprism_registry_close(reg);
    unlink(path);
}

static void test_registry_invalid_path(void)
{
    char *errmsg = NULL;
    clearprism_registry *reg = clearprism_registry_open(
        "/tmp/nonexistent_clearprism.db", &errmsg);
    test_report("invalid path returns NULL", reg == NULL);
    if (errmsg) sqlite3_free(errmsg);
}

static void test_registry_null_path(void)
{
    char *errmsg = NULL;
    clearprism_registry *reg = clearprism_registry_open(NULL, &errmsg);
    test_report("NULL path returns NULL", reg == NULL);
    if (errmsg) sqlite3_free(errmsg);
}

int test_registry_run(void)
{
    test_registry_open_close();
    test_registry_snapshot();
    test_registry_invalid_path();
    test_registry_null_path();
    return 0;
}

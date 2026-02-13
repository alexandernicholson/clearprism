/*
 * test_connpool.c — Connection pool tests
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);

/* Helper: create a minimal SQLite database */
static int create_minimal_db(const char *path)
{
    sqlite3 *db = NULL;
    int rc = sqlite3_open(path, &db);
    if (rc != SQLITE_OK) return rc;
    sqlite3_exec(db, "CREATE TABLE t(x INTEGER)", NULL, NULL, NULL);
    sqlite3_close(db);
    return SQLITE_OK;
}

static void test_pool_create_destroy(void)
{
    clearprism_connpool *pool = clearprism_connpool_create(16, 5000);
    test_report("pool_create returns non-NULL", pool != NULL);
    if (pool) {
        clearprism_connpool_destroy(pool);
    }
}

static void test_pool_checkout_checkin(void)
{
    const char *db_path = "/tmp/clearprism_test_pool1.db";
    unlink(db_path);
    create_minimal_db(db_path);

    clearprism_connpool *pool = clearprism_connpool_create(8, 5000);
    if (!pool) {
        test_report("pool_checkout (setup)", 0);
        unlink(db_path);
        return;
    }

    char *errmsg = NULL;
    sqlite3 *conn = clearprism_connpool_checkout(pool, db_path, "test", &errmsg);
    test_report("checkout returns non-NULL", conn != NULL);
    if (errmsg) {
        printf("    error: %s\n", errmsg);
        sqlite3_free(errmsg);
    }

    if (conn) {
        /* Verify connection works */
        sqlite3_stmt *stmt = NULL;
        int rc = sqlite3_prepare_v2(conn, "SELECT 1", -1, &stmt, NULL);
        test_report("checked-out conn is usable", rc == SQLITE_OK);
        if (stmt) {
            rc = sqlite3_step(stmt);
            test_report("SELECT 1 returns a row", rc == SQLITE_ROW);
            sqlite3_finalize(stmt);
        }

        clearprism_connpool_checkin(pool, db_path);
    }

    /* Checkout same path again — should reuse */
    conn = clearprism_connpool_checkout(pool, db_path, "test", &errmsg);
    test_report("re-checkout returns non-NULL", conn != NULL);
    if (errmsg) sqlite3_free(errmsg);
    if (conn) {
        clearprism_connpool_checkin(pool, db_path);
    }

    clearprism_connpool_destroy(pool);
    unlink(db_path);
}

static void test_pool_multiple_dbs(void)
{
    const char *paths[] = {
        "/tmp/clearprism_test_pool_a.db",
        "/tmp/clearprism_test_pool_b.db",
        "/tmp/clearprism_test_pool_c.db",
    };

    for (int i = 0; i < 3; i++) {
        unlink(paths[i]);
        create_minimal_db(paths[i]);
    }

    clearprism_connpool *pool = clearprism_connpool_create(8, 5000);
    if (!pool) {
        test_report("pool_multiple_dbs (setup)", 0);
        return;
    }

    sqlite3 *conns[3] = {NULL};
    char *errmsg = NULL;
    int all_ok = 1;

    for (int i = 0; i < 3; i++) {
        char alias[16];
        snprintf(alias, sizeof(alias), "db_%d", i);
        conns[i] = clearprism_connpool_checkout(pool, paths[i], alias, &errmsg);
        if (!conns[i]) {
            all_ok = 0;
            sqlite3_free(errmsg);
            errmsg = NULL;
        }
    }
    test_report("checkout 3 different dbs", all_ok);

    /* Check all connections are distinct */
    test_report("connections are distinct",
                 conns[0] != conns[1] && conns[1] != conns[2] && conns[0] != conns[2]);

    for (int i = 0; i < 3; i++) {
        if (conns[i]) clearprism_connpool_checkin(pool, paths[i]);
    }

    clearprism_connpool_destroy(pool);
    for (int i = 0; i < 3; i++) unlink(paths[i]);
}

static void test_pool_eviction(void)
{
    /* Pool with max_open=2, try to open 3 dbs */
    const char *paths[] = {
        "/tmp/clearprism_test_evict_a.db",
        "/tmp/clearprism_test_evict_b.db",
        "/tmp/clearprism_test_evict_c.db",
    };
    for (int i = 0; i < 3; i++) {
        unlink(paths[i]);
        create_minimal_db(paths[i]);
    }

    clearprism_connpool *pool = clearprism_connpool_create(2, 1000);
    if (!pool) {
        test_report("pool_eviction (setup)", 0);
        return;
    }

    char *errmsg = NULL;

    /* Open first two and check them back in */
    sqlite3 *c1 = clearprism_connpool_checkout(pool, paths[0], "a", &errmsg);
    sqlite3_free(errmsg); errmsg = NULL;
    if (c1) clearprism_connpool_checkin(pool, paths[0]);

    sqlite3 *c2 = clearprism_connpool_checkout(pool, paths[1], "b", &errmsg);
    sqlite3_free(errmsg); errmsg = NULL;
    if (c2) clearprism_connpool_checkin(pool, paths[1]);

    /* Now open a third — should trigger eviction of LRU */
    sqlite3 *c3 = clearprism_connpool_checkout(pool, paths[2], "c", &errmsg);
    test_report("eviction allows new checkout", c3 != NULL);
    if (errmsg) {
        printf("    error: %s\n", errmsg);
        sqlite3_free(errmsg);
    }
    if (c3) clearprism_connpool_checkin(pool, paths[2]);

    clearprism_connpool_destroy(pool);
    for (int i = 0; i < 3; i++) unlink(paths[i]);
}

static void test_pool_invalid_path(void)
{
    clearprism_connpool *pool = clearprism_connpool_create(8, 5000);
    if (!pool) {
        test_report("pool_invalid_path (setup)", 0);
        return;
    }

    char *errmsg = NULL;
    /* Trying to open a readonly conn to a path in a nonexistent directory
       should fail since SQLITE_OPEN_READONLY requires an existing file.
       However, sqlite3_open_v2 with READONLY will fail if the file doesn't exist. */
    sqlite3 *conn = clearprism_connpool_checkout(pool,
        "/tmp/nonexistent_dir_clearprism/test.db", "bad", &errmsg);
    test_report("invalid path returns NULL", conn == NULL);
    if (errmsg) sqlite3_free(errmsg);

    clearprism_connpool_destroy(pool);
}

int test_connpool_run(void)
{
    test_pool_create_destroy();
    test_pool_checkout_checkin();
    test_pool_multiple_dbs();
    test_pool_eviction();
    test_pool_invalid_path();
    return 0;
}

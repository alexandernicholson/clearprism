/*
 * test_cache.c — Cache tests (L1 and unified cache)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "clearprism.h"

extern void test_report(const char *name, int passed);

/* Helper: create flat L1 row + values arrays (1 row) */
struct test_l1_data {
    clearprism_l1_row *rows;
    sqlite3_value **all_values;
    int n_values;
};

static struct test_l1_data make_test_row(int val1, const char *val2, int n_values)
{
    struct test_l1_data d = {NULL, NULL, n_values};

    d.rows = sqlite3_malloc(sizeof(clearprism_l1_row));
    d.all_values = sqlite3_malloc(n_values * (int)sizeof(sqlite3_value *));
    if (!d.rows || !d.all_values) {
        sqlite3_free(d.rows); sqlite3_free(d.all_values);
        d.rows = NULL; d.all_values = NULL;
        return d;
    }
    memset(d.rows, 0, sizeof(clearprism_l1_row));

    d.rows->n_values = n_values;
    d.rows->values = d.all_values;
    d.rows->next = NULL;

    /* Create sqlite3_values using a dummy statement */
    sqlite3 *mem_db = NULL;
    sqlite3_open(":memory:", &mem_db);
    sqlite3_stmt *stmt = NULL;

    char *sql = sqlite3_mprintf("SELECT %d, '%s'", val1, val2 ? val2 : "");
    sqlite3_prepare_v2(mem_db, sql, -1, &stmt, NULL);
    sqlite3_free(sql);

    sqlite3_step(stmt);

    for (int i = 0; i < n_values && i < 2; i++) {
        d.all_values[i] = sqlite3_value_dup(sqlite3_column_value(stmt, i));
    }
    /* Fill remaining with NULLs */
    for (int i = 2; i < n_values; i++) {
        d.all_values[i] = sqlite3_value_dup(sqlite3_column_value(stmt, 0));
    }

    sqlite3_finalize(stmt);
    sqlite3_close(mem_db);
    return d;
}

static void test_l1_create_destroy(void)
{
    clearprism_l1_cache *l1 = clearprism_l1_create(1000, 1024 * 1024, 60);
    test_report("l1_create returns non-NULL", l1 != NULL);
    if (l1) {
        clearprism_l1_destroy(l1);
    }
}

static void test_l1_insert_lookup(void)
{
    clearprism_l1_cache *l1 = clearprism_l1_create(1000, 1024 * 1024, 60);
    if (!l1) {
        test_report("l1_insert_lookup (setup)", 0);
        return;
    }

    struct test_l1_data d = make_test_row(42, "hello", 2);
    if (!d.rows) {
        test_report("l1_insert_lookup (make_row)", 0);
        clearprism_l1_destroy(l1);
        return;
    }

    int rc = clearprism_l1_insert(l1, "test_key", d.rows, d.all_values, 1, 2, 64);
    test_report("l1_insert returns OK", rc == SQLITE_OK);

    clearprism_l1_entry *entry = clearprism_l1_lookup(l1, "test_key");
    test_report("l1_lookup finds entry", entry != NULL);
    if (entry) {
        test_report("entry has 1 row", entry->n_rows == 1);
        test_report("entry rows non-NULL", entry->rows != NULL);
    }

    /* Miss */
    entry = clearprism_l1_lookup(l1, "nonexistent");
    test_report("l1_lookup miss returns NULL", entry == NULL);

    clearprism_l1_destroy(l1);
}

static void test_l1_eviction(void)
{
    /* Create cache with max 2 rows */
    clearprism_l1_cache *l1 = clearprism_l1_create(2, 1024 * 1024, 60);
    if (!l1) {
        test_report("l1_eviction (setup)", 0);
        return;
    }

    /* Insert 3 single-row entries — third should evict first */
    struct test_l1_data d1 = make_test_row(1, "one", 2);
    struct test_l1_data d2 = make_test_row(2, "two", 2);
    struct test_l1_data d3 = make_test_row(3, "three", 2);

    clearprism_l1_insert(l1, "key1", d1.rows, d1.all_values, 1, 2, 32);
    clearprism_l1_insert(l1, "key2", d2.rows, d2.all_values, 1, 2, 32);
    clearprism_l1_insert(l1, "key3", d3.rows, d3.all_values, 1, 2, 32);

    /* key1 should have been evicted */
    clearprism_l1_entry *e1 = clearprism_l1_lookup(l1, "key1");
    test_report("evicted entry returns NULL", e1 == NULL);

    clearprism_l1_entry *e3 = clearprism_l1_lookup(l1, "key3");
    test_report("newest entry still present", e3 != NULL);

    clearprism_l1_destroy(l1);
}

static void test_l1_ttl_expiry(void)
{
    /* Create cache with 1-second TTL */
    clearprism_l1_cache *l1 = clearprism_l1_create(1000, 1024 * 1024, 1);
    if (!l1) {
        test_report("l1_ttl_expiry (setup)", 0);
        return;
    }

    struct test_l1_data d = make_test_row(99, "ttl", 2);
    clearprism_l1_insert(l1, "ttl_key", d.rows, d.all_values, 1, 2, 32);

    /* Should be present immediately */
    clearprism_l1_entry *e = clearprism_l1_lookup(l1, "ttl_key");
    test_report("entry present before TTL", e != NULL);

    /* Wait for TTL expiry */
    sleep(2);

    e = clearprism_l1_lookup(l1, "ttl_key");
    test_report("entry expired after TTL", e == NULL);

    clearprism_l1_destroy(l1);
}

static void test_unified_cache(void)
{
    clearprism_l1_cache *l1 = clearprism_l1_create(1000, 1024 * 1024, 60);
    if (!l1) {
        test_report("unified_cache (setup)", 0);
        return;
    }

    clearprism_cache *cache = clearprism_cache_create(l1, NULL);
    test_report("cache_create returns non-NULL", cache != NULL);

    if (cache) {
        /* Lookup should miss */
        clearprism_cache_cursor *cc = NULL;
        int hit = clearprism_cache_lookup(cache, "miss_key", &cc);
        test_report("cache miss returns 0", hit == 0);
        test_report("cache miss cursor is NULL", cc == NULL);

        /* Insert into L1 via cache facade */
        struct test_l1_data td = make_test_row(7, "cached", 2);
        clearprism_cache_store_l1(cache, "hit_key", td.rows, td.all_values, 1, 2, 32);

        /* Lookup should hit */
        hit = clearprism_cache_lookup(cache, "hit_key", &cc);
        test_report("cache hit returns 1", hit == 1);
        test_report("cache hit cursor non-NULL", cc != NULL);

        if (cc) {
            test_report("cache cursor not EOF", !clearprism_cache_cursor_eof(cc));
            clearprism_cache_cursor_free(cc);
        }

        clearprism_cache_destroy(cache);
    }
}

int test_cache_run(void)
{
    test_l1_create_destroy();
    test_l1_insert_lookup();
    test_l1_eviction();
    test_l1_ttl_expiry();
    test_unified_cache();
    return 0;
}

/*
 * clearprism_cache.c — Unified cache facade (L1 -> L2 -> live query)
 */

#include <stdlib.h>
#include <string.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

clearprism_cache *clearprism_cache_create(clearprism_l1_cache *l1,
                                           clearprism_l2_cache *l2)
{
    clearprism_cache *cache = sqlite3_malloc(sizeof(*cache));
    if (!cache) return NULL;
    cache->l1 = l1;
    cache->l2 = l2;
    return cache;
}

void clearprism_cache_destroy(clearprism_cache *cache)
{
    if (!cache) return;
    if (cache->l1) clearprism_l1_destroy(cache->l1);
    if (cache->l2) clearprism_l2_destroy(cache->l2);
    sqlite3_free(cache);
}

/*
 * Look up a cache key. Returns 1 on hit (and sets *out_cursor), 0 on miss.
 *
 * Flow: L1 hit? -> serve from L1
 *       L1 miss -> L2 fresh? -> serve from L2 (and populate L1)
 *       L2 miss -> return 0 (caller does live query)
 */
int clearprism_cache_lookup(clearprism_cache *cache, const char *key,
                             clearprism_cache_cursor **out_cursor)
{
    if (!cache || !key || !out_cursor) return 0;
    *out_cursor = NULL;

    /* Try L1 */
    if (cache->l1) {
        clearprism_l1_entry *entry = clearprism_l1_lookup(cache->l1, key);
        if (entry) {
            /* Create a cache cursor serving from flat L1 arrays */
            clearprism_cache_cursor *cc = sqlite3_malloc(sizeof(*cc));
            if (!cc) return 0;
            memset(cc, 0, sizeof(*cc));
            cc->rows = entry->rows;
            cc->all_values = entry->all_values;
            cc->n_rows = entry->n_rows;
            cc->n_values_per_row = entry->n_values_per_row;
            cc->current_idx = 0;
            cc->current_rowid = (entry->n_rows > 0) ?
                entry->rows[0].composite_rowid : 0;
            *out_cursor = cc;
            return 1;
        }
    }

    /* Try L2 (if fresh) */
    if (cache->l2 && clearprism_l2_is_fresh(cache->l2)) {
        /* Parse cache key to extract source alias for L2 filtering.
           Key format: "table:src=alias:p0=val:..." */
        const char *source_alias = NULL;
        char alias_buf[256];
        const char *src_pos = strstr(key, "src=");
        if (src_pos) {
            src_pos += 4;  /* skip "src=" */
            const char *end = strchr(src_pos, ':');
            if (end && (size_t)(end - src_pos) < sizeof(alias_buf)) {
                memcpy(alias_buf, src_pos, end - src_pos);
                alias_buf[end - src_pos] = '\0';
                source_alias = alias_buf;
            }
        }

        char *l2_err = NULL;
        sqlite3_stmt *l2_stmt = clearprism_l2_query(cache->l2, NULL,
                                                       source_alias, &l2_err);
        sqlite3_free(l2_err);

        if (l2_stmt) {
            int rc = sqlite3_step(l2_stmt);
            if (rc == SQLITE_ROW) {
                /* L2 has data — create a cache cursor backed by the L2 statement */
                clearprism_cache_cursor *cc = sqlite3_malloc(sizeof(*cc));
                if (cc) {
                    memset(cc, 0, sizeof(*cc));
                    cc->l2_stmt = l2_stmt;
                    *out_cursor = cc;
                    return 1;
                }
            }
            /* No rows or alloc failed */
            sqlite3_finalize(l2_stmt);
        }
    }

    return 0;
}

void clearprism_cache_store_l1(clearprism_cache *cache, const char *key,
                                clearprism_l1_row *rows,
                                sqlite3_value **all_values,
                                int n_rows, int n_values_per_row,
                                size_t byte_size)
{
    if (!cache || !cache->l1 || !key) return;
    clearprism_l1_insert(cache->l1, key, rows, all_values,
                          n_rows, n_values_per_row, byte_size);
}

void clearprism_cache_cursor_free(clearprism_cache_cursor *cc)
{
    if (!cc) return;
    if (cc->l2_stmt) {
        sqlite3_finalize(cc->l2_stmt);
    }
    /* Note: L1 rows are owned by the L1 entry, not the cursor.
       We don't free them here. */
    sqlite3_free(cc);
}

int clearprism_cache_cursor_next(clearprism_cache_cursor *cc)
{
    if (!cc) return SQLITE_ERROR;

    /* L2 path */
    if (cc->l2_stmt) {
        int rc = sqlite3_step(cc->l2_stmt);
        if (rc != SQLITE_ROW) {
            return SQLITE_DONE;
        }
        return SQLITE_OK;
    }

    /* L1 path — flat array indexed access */
    cc->current_idx++;
    if (cc->current_idx < cc->n_rows) {
        cc->current_rowid = cc->rows[cc->current_idx].composite_rowid;
    }
    return SQLITE_OK;
}

int clearprism_cache_cursor_eof(clearprism_cache_cursor *cc)
{
    if (!cc) return 1;

    /* L2 path */
    if (cc->l2_stmt) {
        return 0;  /* We rely on next() returning DONE */
    }

    /* L1 path — flat array bounds check */
    return cc->current_idx >= cc->n_rows;
}

sqlite3_value *clearprism_cache_cursor_value(clearprism_cache_cursor *cc, int iCol)
{
    if (!cc) return NULL;

    /* L2 path */
    if (cc->l2_stmt) {
        return sqlite3_column_value(cc->l2_stmt, iCol);
    }

    /* L1 path — flat array indexed access */
    if (cc->all_values && cc->current_idx < cc->n_rows &&
        iCol >= 0 && iCol < cc->n_values_per_row) {
        return cc->all_values[cc->current_idx * cc->n_values_per_row + iCol];
    }

    return NULL;
}

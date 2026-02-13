/*
 * clearprism_connpool.c — Connection pool: lazy open, checkout/checkin, LRU eviction
 */

#define _POSIX_C_SOURCE 199309L

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

/* Internal helpers */
static clearprism_pool_entry *pool_find(clearprism_connpool *pool, const char *db_path,
                                         uint64_t hash);
static void pool_lru_remove(clearprism_connpool *pool, clearprism_pool_entry *e);
static void pool_lru_push_front(clearprism_connpool *pool, clearprism_pool_entry *e);
static int  pool_evict_one(clearprism_connpool *pool);

clearprism_connpool *clearprism_connpool_create(int max_open, int timeout_ms)
{
    clearprism_connpool *pool = sqlite3_malloc(sizeof(*pool));
    if (!pool) return NULL;
    memset(pool, 0, sizeof(*pool));

    pool->n_buckets = CLEARPRISM_CONNPOOL_INIT_BUCKETS;
    pool->buckets = sqlite3_malloc(pool->n_buckets * (int)sizeof(*pool->buckets));
    if (!pool->buckets) {
        sqlite3_free(pool);
        return NULL;
    }
    memset(pool->buckets, 0, pool->n_buckets * (int)sizeof(*pool->buckets));

    pool->max_open = max_open;
    pool->timeout_ms = timeout_ms;
    pool->n_open = 0;
    pool->lru_head = NULL;
    pool->lru_tail = NULL;
    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->cond, NULL);
    return pool;
}

void clearprism_connpool_destroy(clearprism_connpool *pool)
{
    if (!pool) return;
    pthread_mutex_lock(&pool->lock);
    for (int i = 0; i < pool->n_buckets; i++) {
        clearprism_pool_entry *e = pool->buckets[i];
        while (e) {
            clearprism_pool_entry *next = e->next;
            if (e->conn) {
                sqlite3_close(e->conn);
            }
            sqlite3_free(e->db_path);
            sqlite3_free(e->alias);
            sqlite3_free(e);
            e = next;
        }
    }
    sqlite3_free(pool->buckets);
    pthread_mutex_unlock(&pool->lock);
    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->cond);
    sqlite3_free(pool);
}

sqlite3 *clearprism_connpool_checkout(clearprism_connpool *pool,
                                       const char *db_path,
                                       const char *alias,
                                       char **errmsg)
{
    if (!pool || !db_path) return NULL;

    uint64_t hash = clearprism_fnv1a_str(db_path);

    pthread_mutex_lock(&pool->lock);

    /* Look for existing entry */
    clearprism_pool_entry *entry = pool_find(pool, db_path, hash);

    if (entry) {
        /* Found existing connection */
        entry->checkout_count++;
        entry->last_used = time(NULL);
        pool_lru_remove(pool, entry);
        pool_lru_push_front(pool, entry);
        sqlite3 *conn = entry->conn;
        pthread_mutex_unlock(&pool->lock);
        return conn;
    }

    /* Need to open a new connection — may need to evict first */
    while (pool->n_open >= pool->max_open) {
        if (!pool_evict_one(pool)) {
            /* All connections are checked out; wait with timeout */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += pool->timeout_ms / 1000;
            ts.tv_nsec += (pool->timeout_ms % 1000) * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            int wait_rc = pthread_cond_timedwait(&pool->cond, &pool->lock, &ts);
            if (wait_rc == ETIMEDOUT) {
                pthread_mutex_unlock(&pool->lock);
                if (errmsg) {
                    *errmsg = clearprism_mprintf(
                        "connection pool exhausted (max_open=%d)", pool->max_open);
                }
                return NULL;
            }
            /* Retry eviction after wakeup */
            continue;
        }
    }

    /* Create new entry — open connection outside the lock would be ideal,
       but for simplicity we do it under the lock (sqlite3_open_v2 is fast). */
    entry = sqlite3_malloc(sizeof(*entry));
    if (!entry) {
        pthread_mutex_unlock(&pool->lock);
        if (errmsg) *errmsg = clearprism_strdup("out of memory");
        return NULL;
    }
    memset(entry, 0, sizeof(*entry));
    entry->db_path = clearprism_strdup(db_path);
    entry->alias = alias ? clearprism_strdup(alias) : NULL;
    entry->path_hash = hash;

    int rc = sqlite3_open_v2(db_path, &entry->conn,
                              SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                              NULL);
    if (rc != SQLITE_OK) {
        if (errmsg) {
            *errmsg = clearprism_mprintf("cannot open source '%s': %s",
                                          db_path,
                                          entry->conn ? sqlite3_errmsg(entry->conn) : "unknown error");
        }
        sqlite3_close(entry->conn);
        sqlite3_free(entry->db_path);
        sqlite3_free(entry->alias);
        sqlite3_free(entry);
        pthread_mutex_unlock(&pool->lock);
        return NULL;
    }

    /* Enable busy timeout for concurrent access */
    sqlite3_busy_timeout(entry->conn, 1000);

    entry->checkout_count = 1;
    entry->last_used = time(NULL);

    /* Insert into hash bucket */
    int bucket = (int)(hash % (uint64_t)pool->n_buckets);
    entry->next = pool->buckets[bucket];
    pool->buckets[bucket] = entry;
    pool->n_open++;

    pool_lru_push_front(pool, entry);

    sqlite3 *conn = entry->conn;
    pthread_mutex_unlock(&pool->lock);
    return conn;
}

void clearprism_connpool_checkin(clearprism_connpool *pool, const char *db_path)
{
    if (!pool || !db_path) return;

    uint64_t hash = clearprism_fnv1a_str(db_path);

    pthread_mutex_lock(&pool->lock);
    clearprism_pool_entry *entry = pool_find(pool, db_path, hash);
    if (entry && entry->checkout_count > 0) {
        entry->checkout_count--;
        entry->last_used = time(NULL);
        if (entry->checkout_count == 0) {
            /* Signal waiters that a connection is available for eviction */
            pthread_cond_signal(&pool->cond);
        }
    }
    pthread_mutex_unlock(&pool->lock);
}

/* ---------- Internal helpers ---------- */

static clearprism_pool_entry *pool_find(clearprism_connpool *pool,
                                         const char *db_path, uint64_t hash)
{
    int bucket = (int)(hash % (uint64_t)pool->n_buckets);
    clearprism_pool_entry *e = pool->buckets[bucket];
    while (e) {
        if (e->path_hash == hash && strcmp(e->db_path, db_path) == 0) {
            return e;
        }
        e = e->next;
    }
    return NULL;
}

static void pool_lru_remove(clearprism_connpool *pool, clearprism_pool_entry *e)
{
    if (e->lru_prev) e->lru_prev->lru_next = e->lru_next;
    else pool->lru_head = e->lru_next;
    if (e->lru_next) e->lru_next->lru_prev = e->lru_prev;
    else pool->lru_tail = e->lru_prev;
    e->lru_prev = NULL;
    e->lru_next = NULL;
}

static void pool_lru_push_front(clearprism_connpool *pool, clearprism_pool_entry *e)
{
    e->lru_prev = NULL;
    e->lru_next = pool->lru_head;
    if (pool->lru_head) pool->lru_head->lru_prev = e;
    pool->lru_head = e;
    if (!pool->lru_tail) pool->lru_tail = e;
}

static int pool_evict_one(clearprism_connpool *pool)
{
    /* Walk from tail (LRU) to find an idle connection to evict */
    clearprism_pool_entry *e = pool->lru_tail;
    while (e) {
        if (e->checkout_count == 0) {
            /* Remove from LRU list */
            pool_lru_remove(pool, e);

            /* Remove from hash bucket */
            int bucket = (int)(e->path_hash % (uint64_t)pool->n_buckets);
            clearprism_pool_entry **pp = &pool->buckets[bucket];
            while (*pp) {
                if (*pp == e) {
                    *pp = e->next;
                    break;
                }
                pp = &(*pp)->next;
            }

            sqlite3_close(e->conn);
            sqlite3_free(e->db_path);
            sqlite3_free(e->alias);
            sqlite3_free(e);
            pool->n_open--;
            return 1;
        }
        e = e->lru_prev;
    }
    return 0;  /* No idle connections to evict */
}

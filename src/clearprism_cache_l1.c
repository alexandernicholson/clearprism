/*
 * clearprism_cache_l1.c — In-memory LRU cache (hash table + doubly-linked list)
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>

#if SQLITE_CORE
#include <sqlite3.h>
#else
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#endif

#include "clearprism.h"

#define L1_INIT_BUCKETS 256

/* Internal helpers */
static void l1_entry_free(clearprism_l1_entry *entry);
static void l1_lru_remove(clearprism_l1_cache *l1, clearprism_l1_entry *e);
static void l1_lru_push_front(clearprism_l1_cache *l1, clearprism_l1_entry *e);
static void l1_evict_lru(clearprism_l1_cache *l1);
static void l1_remove_entry(clearprism_l1_cache *l1, clearprism_l1_entry *e);

clearprism_l1_cache *clearprism_l1_create(int64_t max_rows, int64_t max_bytes,
                                           int default_ttl_sec)
{
    clearprism_l1_cache *l1 = sqlite3_malloc(sizeof(*l1));
    if (!l1) return NULL;
    memset(l1, 0, sizeof(*l1));

    l1->n_buckets = L1_INIT_BUCKETS;
    l1->buckets = sqlite3_malloc(l1->n_buckets * (int)sizeof(*l1->buckets));
    if (!l1->buckets) {
        sqlite3_free(l1);
        return NULL;
    }
    memset(l1->buckets, 0, l1->n_buckets * (int)sizeof(*l1->buckets));

    l1->max_rows = max_rows;
    l1->max_bytes = max_bytes;
    l1->default_ttl_sec = default_ttl_sec;
    pthread_mutex_init(&l1->lock, NULL);
    return l1;
}

void clearprism_l1_destroy(clearprism_l1_cache *l1)
{
    if (!l1) return;
    pthread_mutex_lock(&l1->lock);
    for (int i = 0; i < l1->n_buckets; i++) {
        clearprism_l1_entry *e = l1->buckets[i];
        while (e) {
            clearprism_l1_entry *next = e->hash_next;
            l1_entry_free(e);
            e = next;
        }
    }
    sqlite3_free(l1->buckets);
    pthread_mutex_unlock(&l1->lock);
    pthread_mutex_destroy(&l1->lock);
    sqlite3_free(l1);
}

clearprism_l1_entry *clearprism_l1_lookup(clearprism_l1_cache *l1, const char *key)
{
    if (!l1 || !key) return NULL;

    uint64_t hash = clearprism_fnv1a_str(key);

    pthread_mutex_lock(&l1->lock);

    int bucket = (int)(hash % (uint64_t)l1->n_buckets);
    clearprism_l1_entry *e = l1->buckets[bucket];
    while (e) {
        if (e->key_hash == hash && strcmp(e->key_str, key) == 0) {
            /* Check TTL */
            time_t now = time(NULL);
            if (now - e->created_at > e->ttl_sec) {
                /* Expired — remove and return miss */
                l1_remove_entry(l1, e);
                l1_entry_free(e);
                pthread_mutex_unlock(&l1->lock);
                return NULL;
            }
            /* Move to front of LRU */
            l1_lru_remove(l1, e);
            l1_lru_push_front(l1, e);
            pthread_mutex_unlock(&l1->lock);
            return e;
        }
        e = e->hash_next;
    }

    pthread_mutex_unlock(&l1->lock);
    return NULL;
}

int clearprism_l1_insert(clearprism_l1_cache *l1, const char *key,
                          clearprism_l1_row *rows,
                          sqlite3_value **all_values,
                          int n_rows, int n_values_per_row,
                          size_t byte_size)
{
    if (!l1 || !key) return SQLITE_ERROR;

    pthread_mutex_lock(&l1->lock);

    /* Evict until we have room */
    while ((l1->total_rows + n_rows > l1->max_rows ||
            l1->total_bytes + (int64_t)byte_size > l1->max_bytes) &&
           l1->lru_tail) {
        l1_evict_lru(l1);
    }

    /* If still can't fit and cache is empty, skip caching */
    if (l1->total_rows + n_rows > l1->max_rows ||
        l1->total_bytes + (int64_t)byte_size > l1->max_bytes) {
        pthread_mutex_unlock(&l1->lock);
        return SQLITE_FULL;
    }

    uint64_t hash = clearprism_fnv1a_str(key);

    /* Check for duplicate key — remove old entry */
    int bucket = (int)(hash % (uint64_t)l1->n_buckets);
    clearprism_l1_entry *existing = l1->buckets[bucket];
    while (existing) {
        if (existing->key_hash == hash && strcmp(existing->key_str, key) == 0) {
            l1_remove_entry(l1, existing);
            l1_entry_free(existing);
            break;
        }
        existing = existing->hash_next;
    }

    /* Create new entry */
    clearprism_l1_entry *entry = sqlite3_malloc(sizeof(*entry));
    if (!entry) {
        pthread_mutex_unlock(&l1->lock);
        return SQLITE_NOMEM;
    }
    memset(entry, 0, sizeof(*entry));

    entry->key_hash = hash;
    entry->key_str = clearprism_strdup(key);
    entry->rows = rows;
    entry->all_values = all_values;
    entry->n_rows = n_rows;
    entry->n_values_per_row = n_values_per_row;
    entry->byte_size = byte_size;
    entry->created_at = time(NULL);
    entry->ttl_sec = l1->default_ttl_sec;

    /* Insert into hash bucket */
    entry->hash_next = l1->buckets[bucket];
    l1->buckets[bucket] = entry;

    /* Add to LRU front */
    l1_lru_push_front(l1, entry);

    l1->n_entries++;
    l1->total_rows += n_rows;
    l1->total_bytes += (int64_t)byte_size;

    pthread_mutex_unlock(&l1->lock);
    return SQLITE_OK;
}

void clearprism_l1_evict_expired(clearprism_l1_cache *l1)
{
    if (!l1) return;

    pthread_mutex_lock(&l1->lock);

    time_t now = time(NULL);
    clearprism_l1_entry *e = l1->lru_tail;
    while (e) {
        clearprism_l1_entry *prev = e->lru_prev;
        if (now - e->created_at > e->ttl_sec) {
            l1_remove_entry(l1, e);
            l1_entry_free(e);
        }
        e = prev;
    }

    pthread_mutex_unlock(&l1->lock);
}

/* ---------- Internal helpers ---------- */

static void l1_entry_free(clearprism_l1_entry *entry)
{
    if (!entry) return;
    sqlite3_free(entry->key_str);

    /* Free all sqlite3_value objects from the flat values array */
    if (entry->all_values) {
        int total = entry->n_rows * entry->n_values_per_row;
        for (int i = 0; i < total; i++)
            sqlite3_value_free(entry->all_values[i]);
        sqlite3_free(entry->all_values);
    }
    /* Free the flat row array (row structs only, values already freed above) */
    sqlite3_free(entry->rows);

    sqlite3_free(entry);
}

static void l1_lru_remove(clearprism_l1_cache *l1, clearprism_l1_entry *e)
{
    if (e->lru_prev) e->lru_prev->lru_next = e->lru_next;
    else l1->lru_head = e->lru_next;
    if (e->lru_next) e->lru_next->lru_prev = e->lru_prev;
    else l1->lru_tail = e->lru_prev;
    e->lru_prev = NULL;
    e->lru_next = NULL;
}

static void l1_lru_push_front(clearprism_l1_cache *l1, clearprism_l1_entry *e)
{
    e->lru_prev = NULL;
    e->lru_next = l1->lru_head;
    if (l1->lru_head) l1->lru_head->lru_prev = e;
    l1->lru_head = e;
    if (!l1->lru_tail) l1->lru_tail = e;
}

static void l1_remove_entry(clearprism_l1_cache *l1, clearprism_l1_entry *e)
{
    /* Remove from LRU */
    l1_lru_remove(l1, e);

    /* Remove from hash bucket */
    int bucket = (int)(e->key_hash % (uint64_t)l1->n_buckets);
    clearprism_l1_entry **pp = &l1->buckets[bucket];
    while (*pp) {
        if (*pp == e) {
            *pp = e->hash_next;
            break;
        }
        pp = &(*pp)->hash_next;
    }

    l1->n_entries--;
    l1->total_rows -= e->n_rows;
    l1->total_bytes -= (int64_t)e->byte_size;
}

static void l1_evict_lru(clearprism_l1_cache *l1)
{
    clearprism_l1_entry *victim = l1->lru_tail;
    if (!victim) return;
    l1_remove_entry(l1, victim);
    l1_entry_free(victim);
}

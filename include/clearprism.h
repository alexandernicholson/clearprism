/*
 * clearprism.h — Public API, version macros, shared typedefs
 *
 * Clearprism: Federated SQLite Virtual Table Extension
 * Read-only federation across 100+ SQLite databases sharing the same schema.
 */

#ifndef CLEARPRISM_H
#define CLEARPRISM_H

#include <sqlite3ext.h>
#include <pthread.h>
#include <stdint.h>
#include <stddef.h>
#include <time.h>

/* Version macros */
#define CLEARPRISM_VERSION_MAJOR 1
#define CLEARPRISM_VERSION_MINOR 0
#define CLEARPRISM_VERSION_PATCH 0
#define CLEARPRISM_VERSION_STRING "1.0.0"

/* Default configuration */
#define CLEARPRISM_DEFAULT_L1_MAX_ROWS    10000
#define CLEARPRISM_DEFAULT_L1_MAX_BYTES   (64 * 1024 * 1024)  /* 64 MiB */
#define CLEARPRISM_DEFAULT_L1_TTL_SEC     60
#define CLEARPRISM_DEFAULT_POOL_MAX_OPEN  32
#define CLEARPRISM_DEFAULT_POOL_TIMEOUT   5000  /* 5 seconds in ms */
#define CLEARPRISM_DEFAULT_L2_REFRESH_SEC 300
#define CLEARPRISM_CONNPOOL_INIT_BUCKETS  256

/* xBestIndex plan bitmask flags (stored in idxNum) */
#define CLEARPRISM_PLAN_SOURCE_CONSTRAINED  0x01
#define CLEARPRISM_PLAN_HAS_WHERE           0x02
#define CLEARPRISM_PLAN_HAS_LIMIT           0x04
#define CLEARPRISM_PLAN_USE_CACHE           0x08
#define CLEARPRISM_PLAN_HAS_OFFSET          0x10
#define CLEARPRISM_PLAN_ROWID_LOOKUP        0x20
#define CLEARPRISM_PLAN_HAS_ORDER           0x40
#define CLEARPRISM_PLAN_HAS_MERGE_ORDER    0x80

/* Composite rowid encoding: (source_id << 40) | source_rowid */
#define CLEARPRISM_ROWID_SHIFT 40
#define CLEARPRISM_ROWID_MASK  ((int64_t)((1LL << 40) - 1))

/* Column offset: rowid is prepended to SELECT, shifting real columns by 1 */
#define CLEARPRISM_COL_OFFSET 1

/* Max threads for parallel source preparation */
#define CLEARPRISM_MAX_PREPARE_THREADS 16
#define CLEARPRISM_MIN_PARALLEL_SOURCES 4

/* Persistent worker thread pool */
typedef struct clearprism_work_pool clearprism_work_pool;
struct clearprism_work_pool {
    pthread_t  *threads;
    int         n_threads;
    void      *(*work_fn)(void *);
    void       *work_arg;
    pthread_mutex_t mtx;
    pthread_cond_t  start_cond;
    pthread_cond_t  done_cond;
    int         pending;
    int         generation;
    int         shutdown;
};

/* Forward declarations */
typedef struct clearprism_vtab       clearprism_vtab;
typedef struct clearprism_cursor     clearprism_cursor;
typedef struct clearprism_source     clearprism_source;
typedef struct clearprism_registry   clearprism_registry;
typedef struct clearprism_connpool   clearprism_connpool;
typedef struct clearprism_pool_entry clearprism_pool_entry;
typedef struct clearprism_l1_cache   clearprism_l1_cache;
typedef struct clearprism_l1_entry   clearprism_l1_entry;
typedef struct clearprism_l1_row     clearprism_l1_row;
typedef struct clearprism_l2_cache   clearprism_l2_cache;
typedef struct clearprism_cache      clearprism_cache;
typedef struct clearprism_col_def    clearprism_col_def;
typedef struct clearprism_query_plan clearprism_query_plan;
typedef struct clearprism_where_constraint clearprism_where_constraint;
typedef struct clearprism_cache_cursor clearprism_cache_cursor;
typedef struct clearprism_source_handle clearprism_source_handle;

/* ---------- Column definition ---------- */
struct clearprism_col_def {
    char *name;
    char *type;       /* SQLite type affinity string */
    int   notnull;
    int   pk;
};

/* ---------- Source database entry ---------- */
struct clearprism_source {
    int64_t id;
    char   *path;
    char   *alias;
    int     active;
    int     priority;
};

/* Default registry reload interval */
#define CLEARPRISM_DEFAULT_REGISTRY_RELOAD_SEC 60

/* ---------- Registry ---------- */
struct clearprism_registry {
    char           *db_path;
    sqlite3        *db;
    clearprism_source *sources;
    int             n_sources;
    time_t          last_reload;
    int             reload_interval_sec;
    pthread_mutex_t lock;
};

/* ---------- Connection pool entry ---------- */
struct clearprism_pool_entry {
    char    *db_path;
    char    *alias;
    sqlite3 *conn;
    int      checkout_count;
    time_t   last_used;
    uint64_t path_hash;
    clearprism_pool_entry *next;  /* hash chain */
    /* LRU doubly-linked list */
    clearprism_pool_entry *lru_prev;
    clearprism_pool_entry *lru_next;
};

/* ---------- Connection pool ---------- */
struct clearprism_connpool {
    clearprism_pool_entry **buckets;
    int      n_buckets;
    int      n_open;
    int      max_open;
    int      timeout_ms;
    /* LRU list (head = most recently used, tail = least recently used) */
    clearprism_pool_entry *lru_head;
    clearprism_pool_entry *lru_tail;
    /* Lifetime stats */
    int64_t  total_checkouts;
    int      current_checked_out;
    pthread_mutex_t lock;
    pthread_cond_t  cond;   /* signaled when a connection is checked in */
};

/* ---------- L1 in-memory LRU cache ---------- */
struct clearprism_l1_row {
    sqlite3_value **values;  /* deep copies, nCol+1 entries (_source_db included) */
    int             n_values;
    int64_t         composite_rowid;  /* composite rowid for this row */
    clearprism_l1_row *next;
};

struct clearprism_l1_entry {
    uint64_t     key_hash;
    char        *key_str;          /* full cache key string */
    clearprism_l1_row *rows;       /* contiguous flat array of n_rows rows */
    sqlite3_value **all_values;    /* flat array: n_rows * n_values_per_row */
    int          n_rows;
    int          n_values_per_row; /* columns per row (nCol+1) */
    size_t       byte_size;        /* estimated memory usage */
    time_t       created_at;
    int          ttl_sec;
    /* Hash table chain */
    clearprism_l1_entry *hash_next;
    /* LRU doubly-linked list */
    clearprism_l1_entry *lru_prev;
    clearprism_l1_entry *lru_next;
};

struct clearprism_l1_cache {
    clearprism_l1_entry **buckets;
    int      n_buckets;
    int      n_entries;
    int64_t  total_rows;
    int64_t  total_bytes;
    int64_t  max_rows;
    int64_t  max_bytes;
    int      default_ttl_sec;
    /* LRU list */
    clearprism_l1_entry *lru_head;
    clearprism_l1_entry *lru_tail;
    /* Lifetime stats */
    int64_t  hits;
    int64_t  misses;
    pthread_mutex_t lock;
};

/* ---------- L2 shadow table cache ---------- */
struct clearprism_l2_cache {
    char    *cache_db_path;
    sqlite3 *reader_db;          /* for query threads (WAL mode) */
    sqlite3 *writer_db;          /* for background refresh thread */
    char    *target_table;
    char    *shadow_table_name;  /* _clearprism_cache_{table} */
    int      n_cols;
    clearprism_col_def *cols;
    int      refresh_interval_sec;
    time_t   last_refresh;
    int      running;            /* flag for background thread */
    pthread_t refresh_thread;
    pthread_mutex_t lock;
    pthread_cond_t  populate_done_cond;    /* signaled after first refresh */
    int             initial_populate_done; /* 0 until first l2_do_refresh done */
    /* Back-reference for source enumeration during refresh */
    clearprism_registry *registry;
    clearprism_connpool *pool;
};

/* ---------- Unified cache ---------- */
struct clearprism_cache {
    clearprism_l1_cache *l1;
    clearprism_l2_cache *l2;
};

/* ---------- Source handle (for parallel prepare) ---------- */
struct clearprism_source_handle {
    int           source_idx;    /* index into cursor->sources */
    sqlite3      *conn;          /* checked-out connection */
    sqlite3_stmt *stmt;          /* prepared + first-stepped statement */
    int           has_row;       /* 1 if first step returned SQLITE_ROW */
    int           errored;       /* 1 if checkout/prepare/step failed */
};

/* ---------- ORDER BY column ---------- */
typedef struct clearprism_order_col clearprism_order_col;
struct clearprism_order_col {
    int col_idx;
    int desc;  /* 1 for DESC, 0 for ASC */
};

/* ---------- WHERE constraint ---------- */
struct clearprism_where_constraint {
    int  col_idx;   /* column index */
    int  op;        /* SQLITE_INDEX_CONSTRAINT_EQ, etc. */
    int  argv_idx;  /* index into argv[] (1-based as per SQLite convention) */
    int  is_in;     /* 1 if this is an IN constraint (sqlite3_vtab_in) */
    int  in_count;  /* number of values for IN constraint */
    int  in_offset; /* start offset into cursor's in_values array */
};

/* ---------- Query plan (decoded from xBestIndex) ---------- */
struct clearprism_query_plan {
    int  flags;              /* CLEARPRISM_PLAN_* bitmask */
    clearprism_where_constraint *constraints;
    int  n_constraints;
    char *source_alias;      /* if source-constrained, the alias */
    int64_t limit_value;     /* -1 = no limit */
    int64_t offset_value;    /* 0 = no offset */
    clearprism_order_col *order_cols;
    int  n_order_cols;
};

/* ---------- Cache cursor for serving cached rows ---------- */
struct clearprism_cache_cursor {
    /* L1 cached data — flat array access */
    clearprism_l1_row *rows;        /* flat row array (for rowid access) */
    sqlite3_value **all_values;     /* flat values: [row][col] = all_values[row*npv+col] */
    int n_rows;
    int n_values_per_row;
    int current_idx;
    int64_t current_rowid;          /* composite rowid of current L1 row */
    /* L2 cursor */
    sqlite3_stmt *l2_stmt;
    sqlite3      *l2_db;            /* per-query connection (closed on free) */
    int           l2_eof;           /* 1 when L2 statement exhausted */
};

/* ---------- Virtual table ---------- */
struct clearprism_vtab {
    sqlite3_vtab    base;           /* Must be first member */
    sqlite3        *host_db;        /* The database connection that loaded us */
    char           *registry_path;
    char           *target_table;
    char           *cache_db_path;

    /* Schema */
    int             nCol;           /* number of real columns (excluding _source_db) */
    clearprism_col_def *cols;
    char           *create_table_sql;  /* for xConnect schema declaration */

    /* Subsystems */
    clearprism_registry *registry;
    clearprism_connpool *pool;
    clearprism_cache    *cache;

    /* Configuration */
    int64_t  l1_max_rows;
    int64_t  l1_max_bytes;
    int      pool_max_open;
    int      l2_refresh_interval_sec;

    pthread_mutex_t lock;

    /* Persistent worker thread pool (created at vtab init) */
    clearprism_work_pool *work_pool;

    /* Snapshot mode */
    int   snapshot_mode;      /* 1 if mode='snapshot' */
    char *snapshot_table;     /* "_clearprism_snap_{vtab_name}" */

    /* UX: creation warnings, L2 status, schema override */
    char *init_warnings;      /* warnings collected during vtab creation */
    int   l2_active;          /* 1 if L2 cache initialized successfully */
    int   l2_disabled;        /* 1 if user explicitly set cache_db='none' */
    char *schema_override;    /* user-supplied schema string (if any) */
};

/* ---------- Cursor ---------- */
struct clearprism_cursor {
    sqlite3_vtab_cursor base;       /* Must be first member */
    clearprism_vtab    *vtab;

    /* Source iteration */
    clearprism_source  *sources;    /* snapshot taken at xFilter */
    int                 n_sources;

    /* Parallel source handles */
    clearprism_source_handle *handles;
    int                 n_handles;
    int                 current_handle_idx;
    int                 lazy_prepare;      /* 1 = prepare sources on demand */

    /* Background source prefetch (overlaps prepare with iteration) */
    pthread_t           prefetch_thread;
    int                 prefetch_next_idx;  /* handle index being prefetched */
    int                 prefetch_active;    /* 1 if prefetch thread is running */

    /* Query plan */
    clearprism_query_plan plan;

    /* Saved argv for re-binding across sources */
    sqlite3_value **saved_argv;
    int             saved_argc;

    /* Row tracking */
    int64_t  row_counter;           /* monotonic synthetic rowid */
    int      eof;

    /* Cache serving */
    clearprism_cache_cursor *cache_cursor;
    int      serving_from_cache;

    /* LIMIT / OFFSET tracking */
    int64_t  limit_remaining;       /* -1 = no limit, else rows left to return */
    int64_t  offset_remaining;      /* 0 = no offset, else rows to skip */

    /* L1 cache population buffer — pre-allocated flat arrays */
    char    *cache_key;             /* key for storing into L1 */
    clearprism_l1_row *buf_rows;    /* flat row array, capacity = buf_capacity */
    sqlite3_value **buf_values;     /* flat value array, capacity = buf_capacity * buf_n_cols */
    int      buf_capacity;          /* max rows we can buffer */
    int      buf_n_cols;            /* columns per row (nCol+1) */
    int      buffer_n_rows;         /* rows currently buffered */
    size_t   buffer_bytes;
    int      buffer_overflow;       /* 1 if result set too large to cache */

    /* IN constraint expansion */
    sqlite3_value **in_values;    /* flat array of all expanded IN values */
    int *in_offsets;              /* start offset in in_values for each IN constraint */
    int *in_counts;               /* count for each IN constraint */
    int  n_in_constraints;
    int  total_in_values;

    /* Pre-generated SQL shared across all source handles */
    char           *cached_sql;
    char           *cached_fallback_sql;

    /* Pre-created sqlite3_value for each source's alias (for L1 buffering) */
    sqlite3_value **alias_values;

    /* Merge-sort heap (array of handle indices, min-heap by ORDER BY columns) */
    int     *heap;
    int      heap_size;

    /* Cached ORDER BY column types for fast merge-sort comparison */
    int     *order_col_types;

    /* Snapshot mode serving */
    sqlite3_stmt *snapshot_stmt;         /* prepared SELECT against shadow table */
    int           serving_from_snapshot;

    /* Parallel drain — materialized flat buffer (Optimization 1) */
    clearprism_l1_row *drain_rows;
    sqlite3_value    **drain_values;
    int                drain_n_rows;
    int                drain_n_cols;
    int                drain_idx;        /* current serving position */
    int                serving_from_drain;

    /* Source error tracking */
    int                n_source_errors;  /* count of errored sources this query */
};

/* ========== Public API Functions ========== */

/* clearprism_main.c */
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_clearprism_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi);

/* clearprism_util.c */
uint64_t clearprism_fnv1a(const void *data, size_t len);
uint64_t clearprism_fnv1a_str(const char *str);
char    *clearprism_mprintf(const char *fmt, ...);
void     clearprism_set_errmsg(sqlite3_vtab *vtab, const char *fmt, ...);
char    *clearprism_strdup(const char *s);
size_t   clearprism_value_memsize(sqlite3_value *val);
int      clearprism_value_compare(sqlite3_value *a, sqlite3_value *b);

/* clearprism_registry.c */
clearprism_registry *clearprism_registry_open(const char *db_path, char **errmsg);
void     clearprism_registry_close(clearprism_registry *reg);
int      clearprism_registry_reload(clearprism_registry *reg, char **errmsg);
int      clearprism_registry_snapshot(clearprism_registry *reg,
                                      const char *table_name,
                                      clearprism_source **out_sources,
                                      int *out_n, char **errmsg);
void     clearprism_sources_free(clearprism_source *sources, int n);

/* clearprism_connpool.c */
clearprism_connpool *clearprism_connpool_create(int max_open, int timeout_ms);
void     clearprism_connpool_destroy(clearprism_connpool *pool);
sqlite3 *clearprism_connpool_checkout(clearprism_connpool *pool,
                                       const char *db_path,
                                       const char *alias,
                                       char **errmsg);
void     clearprism_connpool_checkin(clearprism_connpool *pool,
                                      const char *db_path);
void     clearprism_connpool_stats(clearprism_connpool *pool,
                                    int *out_open, int *out_max,
                                    int *out_checked_out,
                                    int64_t *out_total_checkouts);

/* clearprism_where.c */
char    *clearprism_where_encode(sqlite3_index_info *info, int nCol, int *out_flags);
int      clearprism_where_decode(const char *idx_str,
                                  clearprism_query_plan *plan);
char    *clearprism_where_generate_sql(const char *table,
                                        clearprism_col_def *cols, int nCol,
                                        clearprism_query_plan *plan,
                                        int64_t pushdown_limit);
void     clearprism_query_plan_clear(clearprism_query_plan *plan);

/* clearprism_vtab.c */
int clearprism_vtab_create(sqlite3 *db, void *pAux, int argc,
                            const char *const *argv, sqlite3_vtab **ppVtab,
                            char **pzErr);
int clearprism_vtab_connect(sqlite3 *db, void *pAux, int argc,
                             const char *const *argv, sqlite3_vtab **ppVtab,
                             char **pzErr);
int clearprism_vtab_disconnect(sqlite3_vtab *pVtab);
int clearprism_vtab_destroy(sqlite3_vtab *pVtab);
int clearprism_vtab_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor);
int clearprism_vtab_close(sqlite3_vtab_cursor *cur);

/* clearprism_query.c */
void clearprism_cursor_flush_drain(clearprism_cursor *cur);
int clearprism_vtab_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *info);
int clearprism_vtab_filter(sqlite3_vtab_cursor *cur, int idxNum,
                            const char *idxStr, int argc,
                            sqlite3_value **argv);
int clearprism_vtab_next(sqlite3_vtab_cursor *cur);
int clearprism_vtab_eof(sqlite3_vtab_cursor *cur);
int clearprism_vtab_column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                            int iCol);
int clearprism_vtab_rowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid);

/* clearprism_cache_l1.c */
clearprism_l1_cache *clearprism_l1_create(int64_t max_rows, int64_t max_bytes,
                                           int default_ttl_sec);
void clearprism_l1_destroy(clearprism_l1_cache *l1);
clearprism_l1_entry *clearprism_l1_lookup(clearprism_l1_cache *l1,
                                            const char *key);
int  clearprism_l1_insert(clearprism_l1_cache *l1, const char *key,
                           clearprism_l1_row *rows,
                           sqlite3_value **all_values,
                           int n_rows, int n_values_per_row,
                           size_t byte_size);
void clearprism_l1_evict_expired(clearprism_l1_cache *l1);
void clearprism_l1_flush(clearprism_l1_cache *l1);

/* clearprism_cache_l2.c */
clearprism_l2_cache *clearprism_l2_create(const char *cache_db_path,
                                           const char *target_table,
                                           clearprism_col_def *cols, int nCol,
                                           int refresh_interval_sec,
                                           clearprism_registry *registry,
                                           clearprism_connpool *pool,
                                           char **errmsg);
void clearprism_l2_destroy(clearprism_l2_cache *l2);
int  clearprism_l2_populate(clearprism_l2_cache *l2, char **errmsg);
int  clearprism_l2_start_refresh(clearprism_l2_cache *l2, char **errmsg);
sqlite3_stmt *clearprism_l2_query(clearprism_l2_cache *l2,
                                   const char *where_clause,
                                   const char *source_alias,
                                   char **errmsg);
sqlite3_stmt *clearprism_l2_query_ex(clearprism_l2_cache *l2,
                                      const char *where_clause,
                                      const char *source_alias,
                                      char **errmsg,
                                      sqlite3 **out_db);
int  clearprism_l2_is_fresh(clearprism_l2_cache *l2);
void clearprism_l2_wait_ready(clearprism_l2_cache *l2);
int  clearprism_l2_is_ready(clearprism_l2_cache *l2);

/* clearprism_cache.c */
clearprism_cache *clearprism_cache_create(clearprism_l1_cache *l1,
                                           clearprism_l2_cache *l2);
void clearprism_cache_destroy(clearprism_cache *cache);
int  clearprism_cache_lookup(clearprism_cache *cache, const char *key,
                              clearprism_cache_cursor **out_cursor);
void clearprism_cache_store_l1(clearprism_cache *cache, const char *key,
                                clearprism_l1_row *rows,
                                sqlite3_value **all_values,
                                int n_rows, int n_values_per_row,
                                size_t byte_size);
void clearprism_cache_cursor_free(clearprism_cache_cursor *cc);

/* Cache cursor operations */
int  clearprism_cache_cursor_next(clearprism_cache_cursor *cc);
int  clearprism_cache_cursor_eof(clearprism_cache_cursor *cc);
sqlite3_value *clearprism_cache_cursor_value(clearprism_cache_cursor *cc,
                                              int iCol);

/* clearprism_query.c — Snapshot mode */
int clearprism_snapshot_populate(clearprism_vtab *vtab);

/* clearprism_query.c — Worker thread pool */
clearprism_work_pool *clearprism_work_pool_create(int n_threads);
void clearprism_work_pool_destroy(clearprism_work_pool *pool);
void clearprism_work_pool_run(clearprism_work_pool *pool,
                               void *(*fn)(void *), void *arg);

/* clearprism_agg.c — Aggregate pushdown functions */
int clearprism_register_agg_functions(sqlite3 *db);

/* clearprism_admin.c — Admin/diagnostic SQL functions */
int clearprism_register_admin_functions(sqlite3 *db);

/* Per-connection vtab registry for aggregate function lookup */
void clearprism_register_vtab(const char *table, clearprism_vtab *vtab);
void clearprism_unregister_vtab(const char *table);
clearprism_vtab *clearprism_lookup_vtab(const char *table);

/* ---------- Streaming Scanner API ---------- */

/* Maximum bind parameters for scanner WHERE filter */
#define CLEARPRISM_SCAN_MAX_BINDS 64

typedef struct clearprism_scanner clearprism_scanner;

/* Stored bind value for per-source statement preparation */
typedef struct clearprism_scan_bind clearprism_scan_bind;
struct clearprism_scan_bind {
    int type;   /* SQLITE_INTEGER / SQLITE_FLOAT / SQLITE_TEXT / SQLITE_NULL */
    union {
        int64_t i;
        double  d;
    } u;
    char *text;  /* owned copy for SQLITE_TEXT / SQLITE_BLOB */
    int   text_len;
};

struct clearprism_scanner {
    /* Registry + sources */
    clearprism_registry *registry;
    clearprism_source   *sources;
    int                  n_sources;
    int                  owns_registry;  /* 1 if we opened it, 0 if borrowed */

    /* Connection pool */
    clearprism_connpool *pool;
    int                  owns_pool;

    /* Schema */
    char                *target_table;
    clearprism_col_def  *cols;
    int                  n_cols;

    /* Current iteration state */
    int                  current_source;
    sqlite3             *current_conn;
    sqlite3_stmt        *current_stmt;
    int                  started;  /* 1 after first scan_next call */
    int                  eof;

    /* Generated SQL */
    char                *base_sql;   /* SELECT rowid, col1, ... FROM table */
    char                *filter_sql; /* full SQL with WHERE appended */

    /* WHERE filter + bind values */
    char                *where_expr;
    clearprism_scan_bind binds[CLEARPRISM_SCAN_MAX_BINDS];
    int                  n_binds;
};

/* Lifecycle */
clearprism_scanner *clearprism_scan_open(const char *registry_db,
                                          const char *table);
int   clearprism_scan_next(clearprism_scanner *s);
void  clearprism_scan_close(clearprism_scanner *s);

/* Column accessors — zero-copy, valid until next scan_next call */
int64_t     clearprism_scan_int64(clearprism_scanner *s, int col);
double      clearprism_scan_double(clearprism_scanner *s, int col);
const char *clearprism_scan_text(clearprism_scanner *s, int col);
const void *clearprism_scan_blob(clearprism_scanner *s, int col, int *len);
int         clearprism_scan_type(clearprism_scanner *s, int col);
int         clearprism_scan_is_null(clearprism_scanner *s, int col);

/* Rowid and value accessors */
int64_t      clearprism_scan_rowid(clearprism_scanner *s);
sqlite3_value *clearprism_scan_value(clearprism_scanner *s, int col);

/* Source identification (valid during current row) */
const char *clearprism_scan_source_alias(clearprism_scanner *s);
int64_t     clearprism_scan_source_id(clearprism_scanner *s);

/* Column metadata */
int         clearprism_scan_column_count(clearprism_scanner *s);
const char *clearprism_scan_column_name(clearprism_scanner *s, int col);

/* WHERE filtering — call before first scan_next */
int  clearprism_scan_filter(clearprism_scanner *s, const char *where_expr);
int  clearprism_scan_bind_int64(clearprism_scanner *s, int idx, int64_t val);
int  clearprism_scan_bind_double(clearprism_scanner *s, int idx, double val);
int  clearprism_scan_bind_text(clearprism_scanner *s, int idx, const char *val);
int  clearprism_scan_bind_null(clearprism_scanner *s, int idx);

/* Callback iteration */
int  clearprism_scan_each(clearprism_scanner *s,
                           int (*callback)(clearprism_scanner *s, void *ctx),
                           void *ctx);

/* Parallel scan callback. Called once per row, from a worker thread.
 * Column i (0-indexed) is at sqlite3_column_*(stmt, i + CLEARPRISM_COL_OFFSET).
 * Column 0 in stmt = rowid.
 * Return 0 to continue, non-zero to stop this thread's iteration. */
typedef int (*clearprism_scan_row_fn)(
    sqlite3_stmt *stmt,
    int n_cols,
    const char *source_alias,
    int thread_id,
    void *user_ctx
);

/* Parallel scan: distributes sources across n_threads worker threads.
 * Each thread opens its own connection, prepares the query, iterates rows,
 * and calls row_cb for each row.  Zero-copy column access — no
 * sqlite3_value_dup() overhead.
 * Must be called before first scan_next() (scanner must not have started).
 * Returns SQLITE_OK on success. */
int  clearprism_scan_parallel(clearprism_scanner *s, int n_threads,
                               clearprism_scan_row_fn row_cb,
                               void *user_ctx);

#endif /* CLEARPRISM_H */

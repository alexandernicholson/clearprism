# API Reference

Clearprism's public C API is declared in `include/clearprism.h`. All functions use SQLite's memory allocator (`sqlite3_malloc` / `sqlite3_free`).

## Extension Entry Point

### sqlite3_clearprism_init

```c
int sqlite3_clearprism_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi);
```

Standard SQLite loadable extension entry point. Called automatically by `sqlite3_load_extension()` or `.load`. Registers the `"clearprism"` virtual table module.

**Returns**: `SQLITE_OK` on success, `SQLITE_ERROR` on failure.

### clearprism_init (core builds only)

```c
int clearprism_init(sqlite3 *db);
```

Available when compiled with `SQLITE_CORE=1`. Registers the module without the loadable extension scaffolding.

## Utility Functions

### clearprism_fnv1a / clearprism_fnv1a_str

```c
uint64_t clearprism_fnv1a(const void *data, size_t len);
uint64_t clearprism_fnv1a_str(const char *str);
```

FNV-1a 64-bit hash function. Used internally for hash table key hashing (connection pool and L1 cache). `clearprism_fnv1a_str` is a convenience wrapper that computes `strlen` automatically.

### clearprism_mprintf

```c
char *clearprism_mprintf(const char *fmt, ...);
```

Wrapper around `sqlite3_vmprintf`. Returns a string allocated with `sqlite3_malloc` that must be freed with `sqlite3_free`.

### clearprism_set_errmsg

```c
void clearprism_set_errmsg(sqlite3_vtab *vtab, const char *fmt, ...);
```

Sets the `zErrMsg` field on a virtual table, freeing any previous message. Used to report errors from `xFilter` and other vtab callbacks.

### clearprism_strdup

```c
char *clearprism_strdup(const char *s);
```

Duplicates a string using `sqlite3_malloc`. Returns `NULL` if `s` is `NULL` or allocation fails.

### clearprism_value_memsize

```c
size_t clearprism_value_memsize(sqlite3_value *val);
```

Estimates the memory footprint of a `sqlite3_value`. Used by the L1 cache to track byte usage.

| Type | Size |
|------|------|
| INTEGER | 8 bytes |
| FLOAT | 8 bytes |
| TEXT | byte length + 1 |
| BLOB | byte length |
| NULL | 0 bytes |

## Registry

### clearprism_registry_open

```c
clearprism_registry *clearprism_registry_open(const char *db_path, char **errmsg);
```

Opens a registry database and loads all active sources. The database is opened read-only with `SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX`.

**Returns**: Allocated registry, or `NULL` on failure (with error message in `*errmsg`).

### clearprism_registry_close

```c
void clearprism_registry_close(clearprism_registry *reg);
```

Closes the registry database and frees all associated memory. Safe to call with `NULL`.

### clearprism_registry_reload

```c
int clearprism_registry_reload(clearprism_registry *reg, char **errmsg);
```

Re-reads the `clearprism_sources` table. Thread-safe — acquires `reg->lock` during the swap.

**Returns**: `SQLITE_OK` on success.

### clearprism_registry_snapshot

```c
int clearprism_registry_snapshot(clearprism_registry *reg,
                                  const char *table_name,
                                  clearprism_source **out_sources,
                                  int *out_n, char **errmsg);
```

Returns a deep copy of the active source list, filtered by table-specific overrides. The caller owns the returned array and must free it with `clearprism_sources_free`.

Thread-safe — acquires `reg->lock` during the copy.

### clearprism_sources_free

```c
void clearprism_sources_free(clearprism_source *sources, int n);
```

Frees an array of `clearprism_source` structures (and their string members). Safe to call with `NULL`.

## Connection Pool

### clearprism_connpool_create

```c
clearprism_connpool *clearprism_connpool_create(int max_open, int timeout_ms);
```

Creates a connection pool with the given capacity and wait timeout. Initializes a hash table with 256 buckets.

### clearprism_connpool_destroy

```c
void clearprism_connpool_destroy(clearprism_connpool *pool);
```

Closes all connections and frees the pool. Safe to call with `NULL`.

### clearprism_connpool_checkout

```c
sqlite3 *clearprism_connpool_checkout(clearprism_connpool *pool,
                                       const char *db_path,
                                       const char *alias,
                                       char **errmsg);
```

Gets a connection to the specified database path. If already in the pool, reuses it. Otherwise opens a new connection with `SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX` and a 1-second busy timeout.

If the pool is at capacity, evicts the least recently used idle connection. If all connections are checked out, waits up to `timeout_ms` for one to be returned.

**Returns**: `sqlite3*` handle, or `NULL` on failure. Caller must call `clearprism_connpool_checkin` when done.

### clearprism_connpool_checkin

```c
void clearprism_connpool_checkin(clearprism_connpool *pool,
                                  const char *db_path);
```

Returns a connection to the pool. Decrements the checkout count and signals any threads waiting for connections.

## L1 Cache

### clearprism_l1_create

```c
clearprism_l1_cache *clearprism_l1_create(int64_t max_rows, int64_t max_bytes,
                                           int default_ttl_sec);
```

Creates an L1 in-memory cache with the given limits and default TTL.

### clearprism_l1_destroy

```c
void clearprism_l1_destroy(clearprism_l1_cache *l1);
```

Frees all cached entries and the cache itself.

### clearprism_l1_lookup

```c
clearprism_l1_entry *clearprism_l1_lookup(clearprism_l1_cache *l1,
                                            const char *key);
```

Looks up a cache entry by key string. Returns `NULL` on miss or if the entry has expired (expired entries are lazily removed). On hit, moves the entry to the front of the LRU list.

Thread-safe.

### clearprism_l1_insert

```c
int clearprism_l1_insert(clearprism_l1_cache *l1, const char *key,
                           clearprism_l1_row *rows, int n_rows,
                           size_t byte_size);
```

Inserts a cache entry. The cache takes ownership of the `rows` linked list. If the key already exists, the old entry is replaced. LRU eviction is performed as needed to satisfy the size limits.

**Returns**: `SQLITE_OK`, `SQLITE_FULL` (entry too large), or `SQLITE_NOMEM`.

### clearprism_l1_evict_expired

```c
void clearprism_l1_evict_expired(clearprism_l1_cache *l1);
```

Walks the LRU list from tail to head, removing all entries whose TTL has expired. Thread-safe.

## L2 Cache

### clearprism_l2_create

```c
clearprism_l2_cache *clearprism_l2_create(const char *cache_db_path,
                                           const char *target_table,
                                           clearprism_col_def *cols, int nCol,
                                           int refresh_interval_sec,
                                           clearprism_registry *registry,
                                           clearprism_connpool *pool,
                                           char **errmsg);
```

Creates an L2 shadow table cache. Opens two connections to the cache database (reader + writer, both in WAL mode). Creates the shadow table and metadata table if they don't exist.

### clearprism_l2_destroy

```c
void clearprism_l2_destroy(clearprism_l2_cache *l2);
```

Stops the refresh thread (if running), closes both database connections, and frees all memory.

### clearprism_l2_start_refresh

```c
int clearprism_l2_start_refresh(clearprism_l2_cache *l2, char **errmsg);
```

Starts the background refresh thread. The thread performs an initial refresh immediately, then refreshes at the configured interval. The thread is joined on destruction via `clearprism_l2_destroy()`.

### clearprism_l2_query

```c
sqlite3_stmt *clearprism_l2_query(clearprism_l2_cache *l2,
                                   const char *where_clause,
                                   const char *source_alias,
                                   char **errmsg);
```

Queries the shadow table. Returns a prepared statement that the caller must finalize. Supports optional filtering by source alias and/or a WHERE clause fragment.

### clearprism_l2_is_fresh

```c
int clearprism_l2_is_fresh(clearprism_l2_cache *l2);
```

Returns 1 if the L2 cache has been refreshed within the last `refresh_interval_sec` seconds, 0 otherwise. Thread-safe.

## Unified Cache

### clearprism_cache_create

```c
clearprism_cache *clearprism_cache_create(clearprism_l1_cache *l1,
                                           clearprism_l2_cache *l2);
```

Creates a unified cache facade wrapping L1 and L2. Either `l1` or `l2` may be `NULL`.

### clearprism_cache_lookup

```c
int clearprism_cache_lookup(clearprism_cache *cache, const char *key,
                              clearprism_cache_cursor **out_cursor);
```

Tries L1 first, then L2. Returns 1 on hit (with `*out_cursor` set), 0 on miss.

### clearprism_cache_store_l1

```c
void clearprism_cache_store_l1(clearprism_cache *cache, const char *key,
                                clearprism_l1_row *rows,
                                sqlite3_value **all_values,
                                int n_rows, int n_values_per_row,
                                size_t byte_size);
```

Stores rows in L1 via the cache facade.

### clearprism_cache_cursor_*

```c
int  clearprism_cache_cursor_next(clearprism_cache_cursor *cc);
int  clearprism_cache_cursor_eof(clearprism_cache_cursor *cc);
sqlite3_value *clearprism_cache_cursor_value(clearprism_cache_cursor *cc, int iCol);
void clearprism_cache_cursor_free(clearprism_cache_cursor *cc);
```

Cache cursor iteration. Works for both L1 (linked list traversal) and L2 (statement stepping) cached results.

## Scanner API

The Scanner API provides a streaming interface for high-throughput bulk iteration across federated sources. It bypasses the SQLite virtual table protocol entirely, reading directly from source databases via the connection pool. Column accessors return zero-copy pointers into the source statement's memory, valid until the next `clearprism_scan_next()` call.

Use the Scanner API when you need maximum read throughput (bulk ETL, analytics, exports). Use the virtual table when you need SQL convenience, caching, ORDER BY across sources, or integration with other SQLite features.

### clearprism_scan_open

```c
clearprism_scanner *clearprism_scan_open(const char *registry_db,
                                          const char *table);
```

Opens a scanner for the given registry and table. Internally:
1. Opens the registry and snapshots the active source list
2. Discovers the table schema from the first available source via `PRAGMA table_info`
3. Builds the base SQL (`SELECT rowid, col1, col2, ... FROM table`)
4. Creates a connection pool

**Returns**: Allocated scanner, or `NULL` if the registry is unreachable, no sources are active, or the table doesn't exist in any source.

### clearprism_scan_next

```c
int clearprism_scan_next(clearprism_scanner *s);
```

Advances to the next row. On the first call, connects to the first source and steps to the first row. When a source is exhausted, automatically advances to the next source with rows (resilient — skips sources that fail to connect or prepare).

**Returns**: 1 if a row is available, 0 when all sources are exhausted.

### clearprism_scan_close

```c
void clearprism_scan_close(clearprism_scanner *s);
```

Closes the scanner. Finalizes the current statement, checks in the connection, frees all internal state (SQL strings, schema, bind values, sources, pool, registry). Safe to call with `NULL`.

### clearprism_scan_int64

```c
int64_t clearprism_scan_int64(clearprism_scanner *s, int col);
```

Returns the value of column `col` (0-indexed) as a 64-bit integer. Returns 0 if the scanner has no current row.

### clearprism_scan_double

```c
double clearprism_scan_double(clearprism_scanner *s, int col);
```

Returns the value of column `col` as a double. Returns 0.0 if the scanner has no current row.

### clearprism_scan_text

```c
const char *clearprism_scan_text(clearprism_scanner *s, int col);
```

Returns the value of column `col` as a UTF-8 string. The returned pointer is **zero-copy** — it points directly into the SQLite statement's memory and is valid until the next `clearprism_scan_next()` call. Returns `NULL` if the scanner has no current row.

### clearprism_scan_blob

```c
const void *clearprism_scan_blob(clearprism_scanner *s, int col, int *len);
```

Returns the value of column `col` as a blob. Sets `*len` to the byte length. The returned pointer is zero-copy (valid until the next `clearprism_scan_next()` call). Returns `NULL` if the scanner has no current row.

### clearprism_scan_type

```c
int clearprism_scan_type(clearprism_scanner *s, int col);
```

Returns the SQLite type code (`SQLITE_INTEGER`, `SQLITE_FLOAT`, `SQLITE_TEXT`, `SQLITE_BLOB`, `SQLITE_NULL`) for column `col` in the current row.

### clearprism_scan_is_null

```c
int clearprism_scan_is_null(clearprism_scanner *s, int col);
```

Returns 1 if column `col` is NULL, 0 otherwise.

### clearprism_scan_source_alias

```c
const char *clearprism_scan_source_alias(clearprism_scanner *s);
```

Returns the alias of the current source database (e.g., `"east"`, `"west"`). Equivalent to the `_source_db` hidden column in the virtual table. Returns `NULL` if no source is active.

### clearprism_scan_source_id

```c
int64_t clearprism_scan_source_id(clearprism_scanner *s);
```

Returns the numeric ID of the current source database (from the registry's `id` column).

### clearprism_scan_column_count

```c
int clearprism_scan_column_count(clearprism_scanner *s);
```

Returns the number of columns in the scanned table (excluding rowid).

### clearprism_scan_column_name

```c
const char *clearprism_scan_column_name(clearprism_scanner *s, int col);
```

Returns the name of column `col` (0-indexed). Returns `NULL` if `col` is out of range.

### clearprism_scan_filter

```c
int clearprism_scan_filter(clearprism_scanner *s, const char *where_expr);
```

Sets a WHERE clause expression for the scan. Must be called **before** the first `clearprism_scan_next()` call (returns `SQLITE_MISUSE` otherwise). The expression is appended as `WHERE <where_expr>` to the base SQL. Use `?` placeholders for parameter binding.

**Returns**: `SQLITE_OK` on success.

### clearprism_scan_bind_int64 / bind_double / bind_text / bind_null

```c
int clearprism_scan_bind_int64(clearprism_scanner *s, int idx, int64_t val);
int clearprism_scan_bind_double(clearprism_scanner *s, int idx, double val);
int clearprism_scan_bind_text(clearprism_scanner *s, int idx, const char *val);
int clearprism_scan_bind_null(clearprism_scanner *s, int idx);
```

Bind parameter values for the WHERE expression set by `clearprism_scan_filter()`. `idx` is 1-based (matching SQLite's `?` numbering). Maximum 64 bind parameters (`CLEARPRISM_SCAN_MAX_BINDS`).

**Returns**: `SQLITE_OK` on success, `SQLITE_MISUSE` if `idx` is out of range.

### clearprism_scan_each

```c
int clearprism_scan_each(clearprism_scanner *s,
                          int (*callback)(clearprism_scanner *s, void *ctx),
                          void *ctx);
```

Callback-driven iteration. Calls `callback` for each row. If the callback returns non-zero, iteration stops and that value is returned. Useful for aggregation or early termination patterns.

**Returns**: `SQLITE_OK` if all rows were consumed, or the callback's non-zero return value.

### clearprism_scan_row_fn (callback type)

```c
typedef int (*clearprism_scan_row_fn)(
    sqlite3_stmt *stmt,
    int n_cols,
    const char *source_alias,
    int thread_id,
    void *user_ctx
);
```

Callback type for `clearprism_scan_parallel()`. Called once per row from a worker thread. The `stmt` is positioned on the current row — access columns via `sqlite3_column_*(stmt, col + CLEARPRISM_COL_OFFSET)` (column 0 in the statement is the rowid).

Return 0 to continue iteration, non-zero to stop the calling thread's iteration.

**Thread safety**: The callback may be called concurrently from multiple threads. The `thread_id` parameter (0 to n_threads-1) can be used to index into per-thread state without synchronization.

### clearprism_scan_parallel

```c
int clearprism_scan_parallel(clearprism_scanner *s, int n_threads,
                              clearprism_scan_row_fn row_cb,
                              void *user_ctx);
```

Parallel scan: distributes sources across `n_threads` worker threads using work-stealing. Each worker opens its own database connection (not from the pool), prepares the query, iterates rows, and calls `row_cb` for each row. Zero-copy — column data is accessed directly from the `sqlite3_stmt` with no `sqlite3_value_dup()` overhead.

Must be called before the first `clearprism_scan_next()` — the scanner must not have started sequential iteration. After completion, the scanner is marked as consumed (EOF).

The number of threads is clamped to `min(n_threads, n_sources)`. Each thread uses a 256KB stack.

**Returns**: `SQLITE_OK` on success, `SQLITE_MISUSE` if the scanner has already started.

## Admin Functions

Clearprism registers five SQL scalar functions for runtime diagnostics and management. These are available after loading the extension (or calling `clearprism_init`).

### clearprism_status

```sql
SELECT clearprism_status('vtab_name');
```

Returns a JSON object with live statistics for the named virtual table:

```json
{
  "l1_entries": 5,
  "l1_rows": 150,
  "l1_bytes": 4096,
  "l1_max_rows": 10000,
  "l1_max_bytes": 67108864,
  "l1_hits": 42,
  "l1_misses": 7,
  "pool_open": 3,
  "pool_max": 32,
  "pool_checked_out": 0,
  "pool_total_checkouts": 15,
  "registry_sources": 10,
  "registry_last_reload": 1700000000,
  "l2_active": 1,
  "warnings": ""
}
```

**Returns**: JSON text on success, error if the vtab is not found.

### clearprism_init_registry

```sql
SELECT clearprism_init_registry('/path/to/registry.db');
```

Creates a registry database at the given path with the correct schema (`clearprism_sources` and `clearprism_table_overrides` tables). Uses `CREATE TABLE IF NOT EXISTS`, so it is safe to call on an existing registry.

**Returns**: `'ok'` on success, error on failure.

### clearprism_add_source

```sql
SELECT clearprism_add_source('vtab_name', '/path/to/source.db', 'alias');
```

Adds a source database to the registry of the named virtual table. Opens the vtab's registry database, inserts the source with the given path and alias, and closes.

**Returns**: `'ok'` on success, error on failure (e.g., duplicate alias, vtab not found).

### clearprism_flush_cache

```sql
SELECT clearprism_flush_cache('vtab_name');
```

Flushes all entries from the L1 in-memory cache for the named virtual table. Lifetime hit/miss counters are preserved. Useful for forcing fresh reads from source databases.

**Returns**: `'ok'` on success, error if the vtab is not found.

### clearprism_reload_registry

```sql
SELECT clearprism_reload_registry('vtab_name');
```

Forces an immediate reload of the source list from the registry database. Normally the registry auto-reloads every 60 seconds — this function triggers it immediately.

**Returns**: `'ok'` on success, error if the vtab is not found or reload fails.

## L1 Cache Management

### clearprism_l1_flush

```c
void clearprism_l1_flush(clearprism_l1_cache *l1);
```

Removes all entries from the L1 cache, resetting entry count, row count, byte count, and LRU pointers. Lifetime hit and miss counters are preserved. Safe to call with `NULL`.

Thread-safe — acquires `l1->lock` during the operation.

## Connection Pool Stats

### clearprism_connpool_stats

```c
void clearprism_connpool_stats(clearprism_connpool *pool,
                                int *out_open,
                                int *out_max,
                                int *out_checked_out,
                                int64_t *out_total_checkouts);
```

Reads connection pool statistics under the pool lock. Any output pointer may be `NULL` to skip that stat. Safe to call with a `NULL` pool.

| Output | Description |
|--------|-------------|
| `out_open` | Number of currently open connections |
| `out_max` | Maximum pool capacity |
| `out_checked_out` | Connections currently checked out by queries |
| `out_total_checkouts` | Lifetime total checkout count |

## WHERE Clause Handling

### clearprism_where_encode

```c
char *clearprism_where_encode(sqlite3_index_info *info, int nCol, int *out_flags);
```

Called from `xBestIndex`. Scans SQLite's constraint array, marks usable constraints with `argvIndex`, and builds the `idxStr` encoding. Sets `*out_flags` with `CLEARPRISM_PLAN_*` bitmask.

### clearprism_where_decode

```c
int clearprism_where_decode(const char *idx_str, clearprism_query_plan *plan);
```

Called from `xFilter`. Parses the `idxStr` back into a `clearprism_query_plan` structure.

### clearprism_where_generate_sql

```c
char *clearprism_where_generate_sql(const char *table,
                                     clearprism_col_def *cols, int nCol,
                                     clearprism_query_plan *plan);
```

Generates a parameterized `SELECT` statement with pushed-down WHERE constraints. The generated SQL always prepends `rowid` to the column list for composite rowid encoding. The `_source_db` column constraint is excluded from the generated SQL. IN constraints generate `col IN (?,?,?)` with the correct number of placeholders. If the plan has ORDER BY columns, an `ORDER BY` clause is appended.

### clearprism_query_plan_clear

```c
void clearprism_query_plan_clear(clearprism_query_plan *plan);
```

Frees the constraint array, ORDER BY columns, and source alias in a query plan, then zeroes the structure.

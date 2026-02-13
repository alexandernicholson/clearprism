# Configuration

All Clearprism configuration is specified as `key=value` pairs in the `CREATE VIRTUAL TABLE` statement. Values can be optionally quoted with single or double quotes.

## Parameters

### registry_db (required)

Path to the registry database file containing the `clearprism_sources` table.

```sql
CREATE VIRTUAL TABLE t USING clearprism(
    registry_db='/var/data/registry.db',
    table='users'
);
```

The registry is opened read-only. See [Registry](registry.md) for schema details.

### table (required)

Name of the table to federate across all source databases. Every active source database must contain a table with this name and an identical schema.

The schema is discovered at `CREATE VIRTUAL TABLE` time by opening the first active source (sorted by priority) and running `PRAGMA table_info`.

### cache_db (optional)

Path to a database file used for L2 shadow table caching. If omitted, only L1 (in-memory) caching is available.

```sql
CREATE VIRTUAL TABLE t USING clearprism(
    registry_db='registry.db',
    table='users',
    cache_db='/tmp/clearprism_cache.db'
);
```

The cache database is created if it doesn't exist and is opened in WAL mode for concurrent read/write access. See [Caching](caching.md) for details.

### l1_max_rows (optional)

Maximum total number of rows stored across all entries in the L1 in-memory cache. When this limit is exceeded, the least recently used (LRU) entry is evicted.

| | |
|---|---|
| **Default** | `10000` |
| **Type** | Integer |

### l1_max_bytes (optional)

Maximum total bytes consumed by the L1 in-memory cache. This is an estimate based on `sqlite3_value` sizes. When exceeded, LRU eviction occurs.

| | |
|---|---|
| **Default** | `67108864` (64 MiB) |
| **Type** | Integer |

### pool_max_open (optional)

Maximum number of simultaneously open connections to source databases. When this limit is reached and a new connection is needed, idle connections are evicted via LRU. If all connections are actively in use, the requesting thread waits up to 5 seconds before returning `SQLITE_BUSY`.

| | |
|---|---|
| **Default** | `32` |
| **Type** | Integer |

Choose this value based on your operating system's file descriptor limits and the number of source databases. A value of 32 is conservative; systems with higher `ulimit -n` values can safely use 64 or more.

### l2_refresh_sec (optional)

Interval in seconds between L2 shadow table refreshes. The background refresh thread sleeps for this duration between full refresh cycles.

| | |
|---|---|
| **Default** | `300` (5 minutes) |
| **Type** | Integer |

Lower values keep the shadow table more current but increase I/O load on source databases. Set to a very large value (e.g., `86400`) if you only want L2 populated once at startup.

## Example: Full Configuration

```sql
CREATE VIRTUAL TABLE all_events USING clearprism(
    registry_db='/etc/myapp/clearprism_registry.db',
    table='events',
    cache_db='/var/cache/clearprism/events_cache.db',
    l1_max_rows=50000,
    l1_max_bytes=134217728,
    pool_max_open=64,
    l2_refresh_sec=120
);
```

## Example: Minimal Configuration

```sql
CREATE VIRTUAL TABLE all_users USING clearprism(
    registry_db='registry.db',
    table='users'
);
```

This uses all defaults: 10K row L1 cache, 64 MiB memory limit, 32 max connections, no L2 disk cache, 60-second registry auto-reload interval.

## Tuning Guidelines

### High read throughput

Increase `l1_max_rows` and `l1_max_bytes` to cache more query results. If queries are diverse (many different WHERE clauses), consider enabling L2 with a short `l2_refresh_sec` so the shadow table stays current.

### Many source databases (100+)

Increase `pool_max_open` to avoid constant connection open/close churn. Monitor file descriptor usage with `lsof` or `/proc/self/fd`. Each open connection uses one file descriptor.

### Large source tables

If each source has millions of rows, keep `l1_max_rows` moderate to avoid excessive memory use. Rely on WHERE pushdown to limit the result set size. The L2 shadow table can act as a pre-materialized join point.

### Infrequently changing data

Set `l2_refresh_sec` to a high value (e.g., 3600 for hourly) and increase L1 TTL by restarting the virtual table periodically. The L1 TTL is fixed at 60 seconds and is not currently user-configurable.

## Internal Defaults

These values are compiled into the extension and not currently user-configurable:

| Parameter | Default | Description |
|-----------|---------|-------------|
| L1 TTL | 60s | Time-to-live for L1 cache entries |
| Registry reload interval | 60s | How often the source list is re-read from the registry DB |
| Connection pool timeout | 5000ms | Max wait time when all connections are checked out |
| L1 per-query budget | 25% of `l1_max_rows` | Max rows a single query can buffer into L1 |
| Module iVersion | 3 (SQLite 3.38.0+), 0 otherwise | Enables `sqlite3_vtab_in` for IN pushdown |
| Composite rowid shift | 40 bits | `(source_id << 40) \| source_rowid` — supports ~16M sources and ~1T rows per source |

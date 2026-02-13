# Registry Database

The registry database is a standard SQLite database that tells Clearprism which source databases to federate. It is opened read-only by the extension and managed externally (by your application, admin scripts, etc.).

## Schema

### clearprism_sources (required)

The primary table listing all source databases.

```sql
CREATE TABLE clearprism_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL UNIQUE,            -- Absolute path to the source .db file
    alias TEXT NOT NULL UNIQUE,           -- Human-readable name (exposed via _source_db)
    active INTEGER NOT NULL DEFAULT 1,    -- 1 = included in queries, 0 = skipped
    priority INTEGER NOT NULL DEFAULT 0,  -- Lower values are queried first
    added_at TEXT NOT NULL DEFAULT (datetime('now')),
    notes TEXT                            -- Freeform notes (ignored by extension)
);
```

| Column | Description |
|--------|-------------|
| `path` | Absolute filesystem path to the source database. Must be accessible by the process running SQLite. |
| `alias` | Unique identifier for this source. Returned in the `_source_db` hidden column on every row. |
| `active` | Set to `0` to temporarily exclude a source without removing it. Only rows with `active = 1` are queried. |
| `priority` | Controls the order in which sources are queried. Lower values come first. Sources with equal priority are ordered by `id`. |

### clearprism_table_overrides (optional)

Per-table overrides that can disable specific sources for specific tables. This is useful when not all source databases contain all tables.

```sql
CREATE TABLE clearprism_table_overrides (
    source_id INTEGER NOT NULL REFERENCES clearprism_sources(id),
    table_name TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,    -- 0 = skip this source for this table
    PRIMARY KEY (source_id, table_name)
);
```

If this table doesn't exist in the registry, no overrides are applied (all active sources are used for all tables).

**Example**: Skip the "archive" source when querying the "users" table:

```sql
INSERT INTO clearprism_table_overrides (source_id, table_name, active)
VALUES (5, 'users', 0);
```

## Source Loading

Sources are loaded from the registry with the following query:

```sql
SELECT id, path, alias, active, priority
FROM clearprism_sources
WHERE active = 1
ORDER BY priority ASC, id ASC;
```

Table overrides are then applied:

```sql
SELECT source_id
FROM clearprism_table_overrides
WHERE table_name = ? AND active = 0;
```

Any source whose `id` appears in the overrides exclusion list is removed from the result set.

## When Sources Are Read

The registry is read in two contexts:

1. **At `xCreate`/`xConnect` time** — to discover the schema from the first active source
2. **At `xFilter` time** — to take a snapshot of active sources for the current scan

Each scan gets its own snapshot, so changes to the registry take effect on the next query. Concurrent queries see independent snapshots.

## Managing Sources

### Adding a new source

```sql
INSERT INTO clearprism_sources (path, alias, priority)
VALUES ('/data/new_region.db', 'new_region', 10);
```

The new source will be included in the next query. No restart needed.

### Temporarily disabling a source

```sql
UPDATE clearprism_sources SET active = 0 WHERE alias = 'west_region';
```

### Removing a source permanently

```sql
DELETE FROM clearprism_sources WHERE alias = 'old_region';
```

### Changing query order

```sql
UPDATE clearprism_sources SET priority = 0 WHERE alias = 'primary_region';
UPDATE clearprism_sources SET priority = 100 WHERE alias = 'archive_region';
```

## Schema Discovery

When the virtual table is created, Clearprism opens the first active source (by priority order) and runs:

```sql
PRAGMA table_info("{table_name}");
```

This determines the column names, types, NOT NULL constraints, and primary keys. The discovered schema is used for all source databases — they must match.

The virtual table is then declared to SQLite with the discovered columns plus the hidden `_source_db` column:

```sql
CREATE TABLE x("id" INTEGER, "name" TEXT, "email" TEXT, _source_db TEXT HIDDEN);
```

## Best Practices

- Use **absolute paths** for `path` values to avoid ambiguity
- Choose **short, descriptive aliases** — they appear in query results via `_source_db`
- Set `priority` values with gaps (0, 10, 20, ...) to make reordering easier
- Use `active = 0` instead of `DELETE` to preserve source history
- Keep the registry database small — it's read on every query

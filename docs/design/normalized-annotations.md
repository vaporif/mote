# Normalized Annotations for SQLite Backends

## Problem

Annotations are stored as JSON blobs in SQLite TEXT columns:
```
entities_latest.string_annotations = '[["token","USDC"],["chain","ethereum"]]'
entities_latest.numeric_annotations = '[["price",3500]]'
```

This means:
- SQLite can't filter on annotation values (no index pushdown)
- Every query parses JSON blobs in Rust, then DataFusion filters in memory
- The `SqliteLatestTableProvider` filter pushdown to SQL is useless for annotation queries

## Proposed Change

Replace JSON blob columns with normalized annotation tables.

### Schema

```sql
-- Drop string_annotations and numeric_annotations TEXT columns from entities_latest.
-- Keep everything else (entity_key, owner, expires_at_block, content_type, payload, etc.)

CREATE TABLE entity_string_annotations (
    entity_key BLOB NOT NULL,
    ann_key TEXT NOT NULL,
    ann_value TEXT NOT NULL,
    PRIMARY KEY (entity_key, ann_key)
);
CREATE INDEX idx_str_ann_lookup ON entity_string_annotations (ann_key, ann_value);

CREATE TABLE entity_numeric_annotations (
    entity_key BLOB NOT NULL,
    ann_key TEXT NOT NULL,
    ann_value INTEGER NOT NULL,
    PRIMARY KEY (entity_key, ann_key)
);
CREATE INDEX idx_num_ann_lookup ON entity_numeric_annotations (ann_key, ann_value);
CREATE INDEX idx_num_ann_range ON entity_numeric_annotations (ann_key, ann_value);
```

### Query Flow (before)

1. `SELECT * FROM entities_latest WHERE owner = ?` (filter pushdown works for owner)
2. Returns rows with JSON annotation blobs
3. Rust parses JSON into Arrow MapArrays
4. DataFusion evaluates `str_ann('token') = 'USDC'` by scanning all rows

### Query Flow (after)

1. `SELECT e.*, s.ann_key, s.ann_value, n.ann_key, n.ann_value FROM entities_latest e LEFT JOIN entity_string_annotations s USING (entity_key) LEFT JOIN entity_numeric_annotations n USING (entity_key) WHERE s.ann_key = 'token' AND s.ann_value = 'USDC'`
2. SQLite uses index on `(ann_key, ann_value)` to find matching entity_keys
3. Only matching rows returned
4. Arrow arrays built from already-filtered results

### Write Flow

On entity create/update, instead of:
```rust
serde_json::to_string(&pairs)  // serialize to JSON
conn.execute("INSERT INTO entities_latest ... VALUES (?, ?, ?json_blob?)")
```

Do:
```rust
conn.execute("INSERT INTO entities_latest ... VALUES (?, ?, ...)")  // no annotation columns
for ann in string_annotations {
    conn.execute("INSERT INTO entity_string_annotations VALUES (?, ?, ?)", [entity_key, ann.key, ann.value])
}
for ann in numeric_annotations {
    conn.execute("INSERT INTO entity_numeric_annotations VALUES (?, ?, ?)", [entity_key, ann.key, ann.value])
}
```

On entity delete: `DELETE FROM entity_string_annotations WHERE entity_key = ?` (same for numeric).

### Scope

Only affects SQLite-backed code:
- `crates/sidecar-live-sql/` (applier.rs, provider.rs, schema.rs)
- `crates/sidecar-history/` (writer.rs, provider.rs) — same pattern for entity_events table

Does NOT affect:
- Memory backend (`crates/sidecar-live/`) — already uses roaring bitmap indexes on Arrow data
- Engine (`crates/engine/`) — no SQLite involvement
- ExEx (`crates/exex/`) — Arrow IPC, no JSON

### Tradeoffs

**Wins:**
- Annotation filter pushdown to SQLite (index-backed)
- No JSON parsing on read path
- No JSON serialization on write path
- Cleaner data model

**Costs:**
- More rows in SQLite (up to 64 per entity, but each row is tiny)
- JOINs on every read (but indexed, and SQLite is good at this)
- Entity deletes need cascading deletes across tables (or use `ON DELETE CASCADE`)
- Migration needed for existing data

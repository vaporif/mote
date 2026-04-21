use eyre::WrapErr;
use rusqlite::{Connection, OptionalExtension};

// TODO: support migrations instead of drop-and-recreate on schema version bump
const SCHEMA_VERSION: &str = "3";

pub fn configure_pragmas(conn: &Connection) -> eyre::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "mmap_size", 256 * 1024 * 1024)?;
    conn.pragma_update(None, "cache_size", -64000)?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

pub fn create_tables(conn: &Connection) -> eyre::Result<()> {
    configure_pragmas(conn)?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS entity_events (
            block_number        INTEGER NOT NULL,
            block_hash          BLOB NOT NULL,
            tx_index            INTEGER NOT NULL,
            tx_hash             BLOB NOT NULL,
            log_index           INTEGER NOT NULL,
            event_type          INTEGER NOT NULL,
            entity_key          BLOB NOT NULL,
            owner               BLOB,
            expires_at_block    INTEGER,
            old_expires_at_block INTEGER,
            content_type        TEXT,
            payload             BLOB,
            extend_policy       INTEGER,
            operator            BLOB,
            gas_cost            INTEGER,
            PRIMARY KEY (entity_key, block_number, log_index)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_block_number ON entity_events(block_number);
        CREATE INDEX IF NOT EXISTS idx_expires_at ON entity_events(expires_at_block);
        CREATE INDEX IF NOT EXISTS idx_owner ON entity_events(owner);

        CREATE TABLE IF NOT EXISTS event_string_annotations (
            entity_key   BLOB NOT NULL,
            block_number INTEGER NOT NULL,
            log_index    INTEGER NOT NULL,
            ann_key      TEXT NOT NULL,
            ann_value    TEXT NOT NULL,
            PRIMARY KEY (entity_key, block_number, log_index, ann_key)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_evt_str_ann_block
            ON event_string_annotations(block_number, ann_key, ann_value);

        CREATE TABLE IF NOT EXISTS event_numeric_annotations (
            entity_key   BLOB NOT NULL,
            block_number INTEGER NOT NULL,
            log_index    INTEGER NOT NULL,
            ann_key      TEXT NOT NULL,
            ann_value    INTEGER NOT NULL,
            PRIMARY KEY (entity_key, block_number, log_index, ann_key)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_evt_num_ann_block
            ON event_numeric_annotations(block_number, ann_key, ann_value);

        CREATE TABLE IF NOT EXISTS sidecar_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS entities_history (
            entity_key        BLOB NOT NULL,
            valid_from_block  INTEGER NOT NULL,
            valid_to_block    INTEGER,
            owner             BLOB NOT NULL,
            expires_at_block  INTEGER NOT NULL,
            content_type      TEXT NOT NULL,
            payload           BLOB NOT NULL,
            created_at_block  INTEGER NOT NULL,
            tx_hash           BLOB NOT NULL,
            extend_policy     INTEGER NOT NULL,
            operator          BLOB,
            PRIMARY KEY (entity_key, valid_from_block)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_eh_valid_range ON entities_history(entity_key, valid_to_block);
        CREATE INDEX IF NOT EXISTS idx_eh_owner ON entities_history(owner);

        CREATE TABLE IF NOT EXISTS history_string_annotations (
            entity_key       BLOB NOT NULL,
            valid_from_block INTEGER NOT NULL,
            ann_key          TEXT NOT NULL,
            ann_value        TEXT NOT NULL,
            PRIMARY KEY (entity_key, valid_from_block, ann_key)
        ) STRICT;

        CREATE TABLE IF NOT EXISTS history_numeric_annotations (
            entity_key       BLOB NOT NULL,
            valid_from_block INTEGER NOT NULL,
            ann_key          TEXT NOT NULL,
            ann_value        INTEGER NOT NULL,
            PRIMARY KEY (entity_key, valid_from_block, ann_key)
        ) STRICT;
        ",
    )
    .wrap_err("creating SQLite tables")?;

    conn.execute(
        "INSERT OR IGNORE INTO sidecar_meta (key, value) VALUES ('schema_version', ?1)",
        [SCHEMA_VERSION],
    )?;

    Ok(())
}

pub fn check_schema_version(conn: &Connection) -> eyre::Result<()> {
    let version: String = conn
        .query_row(
            "SELECT value FROM sidecar_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .wrap_err("reading schema_version from sidecar_meta")?;

    if version != SCHEMA_VERSION {
        eyre::bail!(
            "schema version mismatch: expected {SCHEMA_VERSION}, found {version}. \
             Run `glint db rebuild` to recreate the database."
        );
    }

    Ok(())
}

pub fn get_last_processed_block(conn: &Connection) -> eyre::Result<Option<u64>> {
    let result: Option<String> = conn
        .query_row(
            "SELECT value FROM sidecar_meta WHERE key = 'last_processed_block'",
            [],
            |row| row.get(0),
        )
        .optional()
        .wrap_err("reading last_processed_block")?;

    result
        .map(|s| s.parse::<u64>().wrap_err("parsing last_processed_block"))
        .transpose()
}

pub fn set_last_processed_block(conn: &Connection, block: u64) -> eyre::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO sidecar_meta (key, value) VALUES ('last_processed_block', ?1)",
        [block.to_string()],
    )?;
    Ok(())
}

pub fn drop_and_recreate(conn: &Connection) -> eyre::Result<()> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS event_string_annotations;
        DROP TABLE IF EXISTS event_numeric_annotations;
        DROP TABLE IF EXISTS entities_latest;
        DROP TABLE IF EXISTS entity_events;
        DROP TABLE IF EXISTS history_string_annotations;
        DROP TABLE IF EXISTS history_numeric_annotations;
        DROP TABLE IF EXISTS entities_history;
        DROP TABLE IF EXISTS sidecar_meta;
        ",
    )?;
    create_tables(conn)
}

pub fn delete_history_from_block(conn: &Connection, block: u64) -> eyre::Result<()> {
    let block = i64::try_from(block)?;
    conn.execute(
        "DELETE FROM history_string_annotations WHERE valid_from_block >= ?1",
        [block],
    )?;
    conn.execute(
        "DELETE FROM history_numeric_annotations WHERE valid_from_block >= ?1",
        [block],
    )?;
    conn.execute(
        "UPDATE entities_history SET valid_to_block = NULL WHERE valid_to_block >= ?1",
        [block],
    )?;
    conn.execute(
        "DELETE FROM entities_history WHERE valid_from_block >= ?1",
        [block],
    )?;
    Ok(())
}

pub fn delete_events_from_block(conn: &Connection, block_number: u64) -> eyre::Result<usize> {
    let block = i64::try_from(block_number).wrap_err("block number overflows i64")?;
    conn.execute(
        "DELETE FROM event_string_annotations WHERE block_number >= ?1",
        [block],
    )?;
    conn.execute(
        "DELETE FROM event_numeric_annotations WHERE block_number >= ?1",
        [block],
    )?;
    let count = conn.execute(
        "DELETE FROM entity_events WHERE block_number >= ?1",
        [block],
    )?;
    delete_history_from_block(conn, block_number)?;
    Ok(count)
}

pub fn prune_before_block(conn: &Connection, block_number: u64) -> eyre::Result<usize> {
    let block = i64::try_from(block_number).wrap_err("block number overflows i64")?;
    conn.execute(
        "DELETE FROM event_string_annotations WHERE block_number < ?1",
        [block],
    )?;
    conn.execute(
        "DELETE FROM event_numeric_annotations WHERE block_number < ?1",
        [block],
    )?;
    let count = conn.execute("DELETE FROM entity_events WHERE block_number < ?1", [block])?;
    Ok(count)
}

pub fn event_count(conn: &Connection) -> eyre::Result<u64> {
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entity_events", [], |row| row.get(0))
        .wrap_err("counting entity_events")?;
    u64::try_from(count).wrap_err("event count overflows u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_tables_succeeds() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();
        check_schema_version(&conn).unwrap();
    }

    #[test]
    fn last_processed_block_roundtrip() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();
        assert_eq!(get_last_processed_block(&conn).unwrap(), None);
        set_last_processed_block(&conn, 42).unwrap();
        assert_eq!(get_last_processed_block(&conn).unwrap(), Some(42));
    }

    #[test]
    fn drop_and_recreate_resets() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();
        set_last_processed_block(&conn, 100).unwrap();
        drop_and_recreate(&conn).unwrap();
        assert_eq!(get_last_processed_block(&conn).unwrap(), None);
    }

    #[test]
    fn schema_version_mismatch_errors() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();
        conn.execute(
            "UPDATE sidecar_meta SET value = 'wrong' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
        assert!(check_schema_version(&conn).is_err());
    }

    #[test]
    fn drop_and_recreate_also_drops_entities_latest() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();

        conn.execute_batch("CREATE TABLE entities_latest (entity_key BLOB PRIMARY KEY) STRICT;")
            .unwrap();
        conn.execute(
            "INSERT INTO entities_latest (entity_key) VALUES (X'01')",
            [],
        )
        .unwrap();

        drop_and_recreate(&conn).unwrap();

        let table_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='entities_latest'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!table_exists);
    }

    #[test]
    fn delete_events_from_block_removes_correct_rows() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();

        conn.execute(
            "INSERT INTO entity_events (block_number, block_hash, tx_index, tx_hash, log_index, event_type, entity_key)
             VALUES (10, X'00', 0, X'00', 0, 0, X'01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO entity_events (block_number, block_hash, tx_index, tx_hash, log_index, event_type, entity_key)
             VALUES (20, X'00', 0, X'00', 0, 0, X'02')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO entity_events (block_number, block_hash, tx_index, tx_hash, log_index, event_type, entity_key)
             VALUES (30, X'00', 0, X'00', 0, 0, X'03')",
            [],
        )
        .unwrap();

        let deleted = delete_events_from_block(&conn, 20).unwrap();
        assert_eq!(deleted, 2);

        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 1);
    }

    #[test]
    fn event_annotation_tables_created() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();
        let str_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM event_string_annotations", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(str_count, 0);
        let num_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM event_numeric_annotations", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(num_count, 0);
    }

    #[test]
    fn delete_events_cascades_to_annotations() {
        let conn = Connection::open_in_memory().unwrap();
        create_tables(&conn).unwrap();
        conn.execute(
            "INSERT INTO entity_events (block_number, block_hash, tx_index, tx_hash, log_index, event_type, entity_key)
             VALUES (20, X'00', 0, X'00', 0, 0, X'01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO event_string_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (X'01', 20, 0, 'k', 'v')",
            [],
        )
        .unwrap();
        delete_events_from_block(&conn, 20).unwrap();
        let ann_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM event_string_annotations", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(ann_count, 0);
    }
}

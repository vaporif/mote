use eyre::WrapErr;
use rusqlite::{Connection, OptionalExtension};

// TODO: support migrations instead of drop-and-recreate on schema version bump
const SCHEMA_VERSION: &str = "1";

pub fn create_tables(conn: &Connection) -> eyre::Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

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
            string_annotations  TEXT,
            numeric_annotations TEXT,
            extend_policy       INTEGER,
            operator            BLOB,
            PRIMARY KEY (entity_key, block_number, log_index)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_block_number ON entity_events(block_number);
        CREATE INDEX IF NOT EXISTS idx_expires_at ON entity_events(expires_at_block);
        CREATE INDEX IF NOT EXISTS idx_owner ON entity_events(owner);

        CREATE TABLE IF NOT EXISTS sidecar_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
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
        DROP TABLE IF EXISTS entity_events;
        DROP TABLE IF EXISTS sidecar_meta;
        ",
    )?;
    create_tables(conn)
}

pub fn delete_events_from_block(conn: &Connection, block_number: u64) -> eyre::Result<usize> {
    let block = i64::try_from(block_number).wrap_err("block number overflows i64")?;
    let count = conn.execute(
        "DELETE FROM entity_events WHERE block_number >= ?1",
        [block],
    )?;
    Ok(count)
}

pub fn prune_before_block(conn: &Connection, block_number: u64) -> eyre::Result<usize> {
    let block = i64::try_from(block_number).wrap_err("block number overflows i64")?;
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
}

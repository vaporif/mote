use rusqlite::Connection;

use crate::sql_queries::HistoryDb;

pub fn configure_pragmas(conn: &Connection) -> eyre::Result<()> {
    HistoryDb::new(conn).configure_pragmas()
}

pub fn create_tables(conn: &Connection) -> eyre::Result<()> {
    HistoryDb::new(conn).create_tables()
}

pub fn check_schema_version(conn: &Connection) -> eyre::Result<()> {
    HistoryDb::new(conn).check_schema_version()
}

pub fn get_last_processed_block(conn: &Connection) -> eyre::Result<Option<u64>> {
    HistoryDb::new(conn).get_last_processed_block()
}

pub fn set_last_processed_block(conn: &Connection, block: u64) -> eyre::Result<()> {
    HistoryDb::new(conn).set_last_processed_block(block)
}

pub fn drop_and_recreate(conn: &Connection) -> eyre::Result<()> {
    HistoryDb::new(conn).drop_and_recreate()
}

pub fn delete_history_from_block(conn: &Connection, block: u64) -> eyre::Result<()> {
    HistoryDb::new(conn).delete_history_from_block(block)
}

pub fn delete_events_from_block(conn: &Connection, block_number: u64) -> eyre::Result<usize> {
    HistoryDb::new(conn).delete_events_from_block(block_number)
}

pub fn prune_before_block(conn: &Connection, block_number: u64) -> eyre::Result<usize> {
    HistoryDb::new(conn).prune_before_block(block_number)
}

pub fn event_count(conn: &Connection) -> eyre::Result<u64> {
    HistoryDb::new(conn).event_count()
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

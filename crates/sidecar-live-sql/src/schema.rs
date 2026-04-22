use rusqlite::Connection;

use crate::sql_queries::LiveDb;

pub fn create_table(conn: &Connection) -> eyre::Result<()> {
    LiveDb::new(conn).create_table()
}

pub fn check_and_init_schema(conn: &Connection) -> eyre::Result<()> {
    LiveDb::new(conn).check_and_init_schema()
}

pub fn clear_entities_latest(conn: &Connection) -> eyre::Result<()> {
    LiveDb::new(conn).clear_entities_latest()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sidecar_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;",
        )
        .unwrap();
        conn
    }

    #[test]
    fn create_table_succeeds() {
        let conn = setup_db();
        create_table(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entities_latest", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn create_table_sets_schema_version() {
        let conn = setup_db();
        create_table(&conn).unwrap();

        let version: String = conn
            .query_row(
                "SELECT value FROM sidecar_meta WHERE key = 'entities_latest_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "2");
    }

    #[test]
    fn create_table_idempotent() {
        let conn = setup_db();
        create_table(&conn).unwrap();
        create_table(&conn).unwrap();
    }

    #[test]
    fn check_and_init_version_mismatch_recreates() {
        let conn = setup_db();
        create_table(&conn).unwrap();

        conn.execute(
            "INSERT INTO entities_latest (entity_key, owner, expires_at_block, content_type, payload, created_at_block, tx_hash, extend_policy)
             VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', X'0202020202020202020202020202020202020202', 100, 'text/plain', X'00', 1, X'0303030303030303030303030303030303030303030303030303030303030303', 0)",
            [],
        )
        .unwrap();

        conn.execute(
            "UPDATE sidecar_meta SET value = 'old' WHERE key = 'entities_latest_schema_version'",
            [],
        )
        .unwrap();

        check_and_init_schema(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entities_latest", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn clear_entities_latest_removes_all_rows() {
        let conn = setup_db();
        create_table(&conn).unwrap();

        conn.execute(
            "INSERT INTO entities_latest (entity_key, owner, expires_at_block, content_type, payload, created_at_block, tx_hash, extend_policy)
             VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', X'0202020202020202020202020202020202020202', 100, 'text/plain', X'00', 1, X'0303030303030303030303030303030303030303030303030303030303030333', 0)",
            [],
        )
        .unwrap();

        clear_entities_latest(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entities_latest", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn annotation_tables_created() {
        let conn = setup_db();
        create_table(&conn).unwrap();
        let str_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_string_annotations", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(str_count, 0);
        let num_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_numeric_annotations", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(num_count, 0);
    }

    #[test]
    fn clear_entities_latest_also_clears_annotations() {
        let conn = setup_db();
        create_table(&conn).unwrap();
        conn.execute(
            "INSERT INTO entities_latest (entity_key, owner, expires_at_block, content_type, payload, created_at_block, tx_hash, extend_policy)
             VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', X'0202020202020202020202020202020202020202', 100, 'text/plain', X'00', 1, X'0303030303030303030303030303030303030303030303030303030303030303', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO entity_string_annotations (entity_key, ann_key, ann_value) VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', 'k', 'v')",
            [],
        )
        .unwrap();
        clear_entities_latest(&conn).unwrap();
        let ann_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_string_annotations", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(ann_count, 0);
    }
}

use eyre::WrapErr;
use rusqlite::{Connection, OptionalExtension};
use tracing::info;

const SCHEMA_VERSION: &str = "2";
const VERSION_KEY: &str = "entities_latest_schema_version";

pub fn create_table(conn: &Connection) -> eyre::Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS entities_latest (
            entity_key        BLOB PRIMARY KEY,
            owner             BLOB NOT NULL,
            expires_at_block  INTEGER NOT NULL,
            content_type      TEXT NOT NULL,
            payload           BLOB NOT NULL,
            created_at_block  INTEGER NOT NULL,
            tx_hash           BLOB NOT NULL,
            extend_policy     INTEGER NOT NULL,
            operator          BLOB
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_entities_latest_owner
            ON entities_latest(owner);

        CREATE TABLE IF NOT EXISTS entity_string_annotations (
            entity_key BLOB NOT NULL,
            ann_key    TEXT NOT NULL,
            ann_value  TEXT NOT NULL,
            PRIMARY KEY (entity_key, ann_key)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_str_ann_lookup
            ON entity_string_annotations(ann_key, ann_value);

        CREATE TABLE IF NOT EXISTS entity_numeric_annotations (
            entity_key BLOB NOT NULL,
            ann_key    TEXT NOT NULL,
            ann_value  INTEGER NOT NULL,
            PRIMARY KEY (entity_key, ann_key)
        ) STRICT;

        CREATE INDEX IF NOT EXISTS idx_num_ann_lookup
            ON entity_numeric_annotations(ann_key, ann_value);
        ",
    )
    .wrap_err("creating entities_latest table")?;

    conn.execute(
        "INSERT OR IGNORE INTO sidecar_meta (key, value) VALUES (?1, ?2)",
        [VERSION_KEY, SCHEMA_VERSION],
    )?;

    Ok(())
}

pub fn check_and_init_schema(conn: &Connection) -> eyre::Result<()> {
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM sidecar_meta WHERE key = ?1",
            [VERSION_KEY],
            |row| row.get(0),
        )
        .optional()
        .wrap_err("reading entities_latest schema version")?;

    match version.as_deref() {
        Some(v) if v == SCHEMA_VERSION => Ok(()),
        None => {
            info!("entities_latest schema version missing, creating table");
            create_table(conn)
        }
        Some(v) => {
            info!(
                found = v,
                expected = SCHEMA_VERSION,
                "entities_latest schema version mismatch, recreating"
            );
            conn.execute_batch(
                "DROP TABLE IF EXISTS entity_string_annotations;
                 DROP TABLE IF EXISTS entity_numeric_annotations;
                 DROP TABLE IF EXISTS entities_latest;",
            )?;
            conn.execute("DELETE FROM sidecar_meta WHERE key = ?1", [VERSION_KEY])?;
            create_table(conn)
        }
    }
}

pub fn clear_entities_latest(conn: &Connection) -> eyre::Result<()> {
    conn.execute_batch(
        "DELETE FROM entity_string_annotations;
         DELETE FROM entity_numeric_annotations;
         DELETE FROM entities_latest;",
    )?;
    Ok(())
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
        assert_eq!(version, SCHEMA_VERSION);
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

use std::sync::Arc;

use arrow::array::{
    BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt64Builder, UInt8Builder,
};
use eyre::WrapErr;
use glint_primitives::exex_schema::{
    entities_latest_schema, numeric_annotations_schema, string_annotations_schema,
};
use rusqlite::{params, Connection, OptionalExtension};
use tracing::info;

const SCHEMA_VERSION: &str = "2";
const VERSION_KEY: &str = "entities_latest_schema_version";

#[derive(Debug, Default)]
pub struct AnnotationFilters {
    pub key: Option<String>,
    pub string_value: Option<String>,
    pub uint_value: Option<u64>,
}

pub struct EntityRow<'a> {
    pub entity_key: &'a [u8],
    pub owner: &'a [u8],
    pub expires_at_block: i64,
    pub content_type: Option<&'a str>,
    pub payload: Option<&'a [u8]>,
    pub created_at_block: i64,
    pub tx_hash: &'a [u8],
    pub extend_policy: i64,
    pub operator: Option<&'a [u8]>,
}

pub struct LiveDb<'a> {
    conn: &'a Connection,
}

impl<'a> LiveDb<'a> {
    pub const fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    pub fn create_table(&self) -> eyre::Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS entities_latest (
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
                    ON entity_numeric_annotations(ann_key, ann_value);",
            )
            .wrap_err("creating entities_latest table")?;

        self.conn.execute(
            "INSERT OR IGNORE INTO sidecar_meta (key, value) VALUES (?1, ?2)",
            [VERSION_KEY, SCHEMA_VERSION],
        )?;

        Ok(())
    }

    pub fn check_and_init_schema(&self) -> eyre::Result<()> {
        let version: Option<String> = self
            .conn
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
                self.create_table()
            }
            Some(v) => {
                info!(
                    found = v,
                    expected = SCHEMA_VERSION,
                    "entities_latest schema version mismatch, recreating"
                );
                self.conn.execute_batch(
                    "DROP TABLE IF EXISTS entity_string_annotations;
                     DROP TABLE IF EXISTS entity_numeric_annotations;
                     DROP TABLE IF EXISTS entities_latest;",
                )?;
                self.conn
                    .execute("DELETE FROM sidecar_meta WHERE key = ?1", [VERSION_KEY])?;
                self.create_table()
            }
        }
    }

    pub fn clear_entities_latest(&self) -> eyre::Result<()> {
        self.conn.execute_batch(
            "DELETE FROM entity_string_annotations;
             DELETE FROM entity_numeric_annotations;
             DELETE FROM entities_latest;",
        )?;
        Ok(())
    }

    pub fn insert_entity(&self, row: &EntityRow<'_>) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT OR REPLACE INTO entities_latest (
                    entity_key, owner, expires_at_block, content_type, payload,
                    created_at_block, tx_hash, extend_policy, operator
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?
            .execute(params![
                row.entity_key,
                row.owner,
                row.expires_at_block,
                row.content_type,
                row.payload,
                row.created_at_block,
                row.tx_hash,
                row.extend_policy,
                row.operator,
            ])?;
        Ok(())
    }

    pub fn upsert_entity(&self, row: &EntityRow<'_>) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT INTO entities_latest (
                    entity_key, owner, expires_at_block, content_type, payload,
                    created_at_block, tx_hash, extend_policy, operator
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(entity_key) DO UPDATE SET
                    owner = excluded.owner,
                    expires_at_block = excluded.expires_at_block,
                    content_type = excluded.content_type,
                    payload = excluded.payload,
                    tx_hash = excluded.tx_hash,
                    extend_policy = excluded.extend_policy,
                    operator = excluded.operator",
            )?
            .execute(params![
                row.entity_key,
                row.owner,
                row.expires_at_block,
                row.content_type,
                row.payload,
                row.created_at_block,
                row.tx_hash,
                row.extend_policy,
                row.operator,
            ])?;
        Ok(())
    }

    pub fn insert_string_annotation(
        &self,
        entity_key: &[u8],
        key: &str,
        value: &str,
    ) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT OR REPLACE INTO entity_string_annotations (entity_key, ann_key, ann_value) VALUES (?1, ?2, ?3)",
            )?
            .execute(params![entity_key, key, value])?;
        Ok(())
    }

    pub fn insert_numeric_annotation(
        &self,
        entity_key: &[u8],
        key: &str,
        value: i64,
    ) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT OR REPLACE INTO entity_numeric_annotations (entity_key, ann_key, ann_value) VALUES (?1, ?2, ?3)",
            )?
            .execute(params![entity_key, key, value])?;
        Ok(())
    }

    pub fn delete_annotations(&self, entity_key: &[u8]) -> eyre::Result<()> {
        self.conn
            .prepare_cached("DELETE FROM entity_string_annotations WHERE entity_key = ?1")?
            .execute([entity_key])?;
        self.conn
            .prepare_cached("DELETE FROM entity_numeric_annotations WHERE entity_key = ?1")?
            .execute([entity_key])?;
        Ok(())
    }

    pub fn delete_entity(&self, entity_key: &[u8]) -> eyre::Result<()> {
        self.conn
            .prepare_cached("DELETE FROM entities_latest WHERE entity_key = ?1")?
            .execute([entity_key])?;
        Ok(())
    }

    pub fn update_entity_expiry(
        &self,
        entity_key: &[u8],
        new_expires: i64,
        tx_hash: &[u8],
    ) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "UPDATE entities_latest SET expires_at_block = ?1, tx_hash = ?2 WHERE entity_key = ?3",
            )?
            .execute(params![new_expires, tx_hash, entity_key])?;
        Ok(())
    }

    pub fn update_entity_permissions(
        &self,
        entity_key: &[u8],
        owner: &[u8],
        extend_policy: i64,
        operator: Option<&[u8]>,
    ) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "UPDATE entities_latest SET owner = ?1, extend_policy = ?2, operator = ?3 WHERE entity_key = ?4",
            )?
            .execute(params![owner, extend_policy, operator, entity_key])?;
        Ok(())
    }

    pub fn revert_created(&self, entity_key: &[u8]) -> eyre::Result<()> {
        self.delete_annotations(entity_key)?;
        self.delete_entity(entity_key)?;
        Ok(())
    }

    pub fn revert_extended(&self, entity_key: &[u8], old_expires: i64) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "UPDATE entities_latest SET expires_at_block = ?1 WHERE entity_key = ?2",
            )?
            .execute(params![old_expires, entity_key])?;
        Ok(())
    }

    #[allow(clippy::option_if_let_else)]
    pub fn query_entities_latest(
        &self,
        current_block: u64,
        owner_filter: Option<&[u8]>,
    ) -> eyre::Result<RecordBatch> {
        let current_block_i64 = i64::try_from(current_block).wrap_err("block number overflow")?;

        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match owner_filter {
            Some(owner) => (
                "SELECT entity_key, owner, expires_at_block, content_type, payload, \
                        created_at_block, tx_hash, extend_policy, operator \
                 FROM entities_latest \
                 WHERE expires_at_block > ?1 AND owner = ?2"
                    .to_owned(),
                vec![Box::new(current_block_i64), Box::new(owner.to_vec())],
            ),
            None => (
                "SELECT entity_key, owner, expires_at_block, content_type, payload, \
                        created_at_block, tx_hash, extend_policy, operator \
                 FROM entities_latest \
                 WHERE expires_at_block > ?1"
                    .to_owned(),
                vec![Box::new(current_block_i64)],
            ),
        };

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(AsRef::as_ref).collect();
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let mut rows = stmt.query(params_refs.as_slice())?;

        let mut entity_key_builder = FixedSizeBinaryBuilder::new(32);
        let mut owner_builder = FixedSizeBinaryBuilder::new(20);
        let mut expires_builder = UInt64Builder::new();
        let mut content_type_builder = StringBuilder::new();
        let mut payload_builder = BinaryBuilder::new();
        let mut created_at_builder = UInt64Builder::new();
        let mut tx_hash_builder = FixedSizeBinaryBuilder::new(32);
        let mut extend_policy_builder = UInt8Builder::new();
        let mut operator_builder = FixedSizeBinaryBuilder::new(20);

        while let Some(row) = rows.next()? {
            let entity_key: Vec<u8> = row.get(0)?;
            let owner: Vec<u8> = row.get(1)?;
            let expires: i64 = row.get(2)?;
            let content_type: String = row.get(3)?;
            let payload: Vec<u8> = row.get(4)?;
            let created_at: i64 = row.get(5)?;
            let tx_hash: Vec<u8> = row.get(6)?;
            let extend_policy: i64 = row.get(7)?;
            let operator: Option<Vec<u8>> = row.get(8)?;

            entity_key_builder.append_value(&entity_key)?;
            owner_builder.append_value(&owner)?;
            expires_builder.append_value(u64::try_from(expires)?);
            content_type_builder.append_value(&content_type);
            payload_builder.append_value(&payload);
            created_at_builder.append_value(u64::try_from(created_at)?);
            tx_hash_builder.append_value(&tx_hash)?;
            extend_policy_builder.append_value(u8::try_from(extend_policy)?);

            match operator {
                Some(op) => operator_builder.append_value(&op)?,
                None => operator_builder.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            entities_latest_schema(),
            vec![
                Arc::new(entity_key_builder.finish()),
                Arc::new(owner_builder.finish()),
                Arc::new(expires_builder.finish()),
                Arc::new(content_type_builder.finish()),
                Arc::new(payload_builder.finish()),
                Arc::new(created_at_builder.finish()),
                Arc::new(tx_hash_builder.finish()),
                Arc::new(extend_policy_builder.finish()),
                Arc::new(operator_builder.finish()),
            ],
        )?;

        Ok(batch)
    }

    pub fn query_string_annotations(
        &self,
        ann_filters: &AnnotationFilters,
    ) -> eyre::Result<RecordBatch> {
        let mut sql =
            String::from("SELECT entity_key, ann_key, ann_value FROM entity_string_annotations");
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut conditions: Vec<String> = Vec::new();

        if let Some(ref key) = ann_filters.key {
            params.push(Box::new(key.clone()));
            conditions.push(format!("ann_key = ?{}", params.len()));
        }
        if let Some(ref value) = ann_filters.string_value {
            params.push(Box::new(value.clone()));
            conditions.push(format!("ann_value = ?{}", params.len()));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(AsRef::as_ref).collect();
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let mut rows = stmt.query(params_refs.as_slice())?;

        let mut entity_key_builder = FixedSizeBinaryBuilder::new(32);
        let mut key_builder = StringBuilder::new();
        let mut value_builder = StringBuilder::new();

        while let Some(row) = rows.next()? {
            let entity_key: Vec<u8> = row.get(0)?;
            let ann_key: String = row.get(1)?;
            let ann_value: String = row.get(2)?;

            entity_key_builder.append_value(&entity_key)?;
            key_builder.append_value(&ann_key);
            value_builder.append_value(&ann_value);
        }

        let batch = RecordBatch::try_new(
            string_annotations_schema(),
            vec![
                Arc::new(entity_key_builder.finish()),
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )?;

        Ok(batch)
    }

    pub fn query_numeric_annotations(
        &self,
        ann_filters: &AnnotationFilters,
    ) -> eyre::Result<RecordBatch> {
        let mut sql =
            String::from("SELECT entity_key, ann_key, ann_value FROM entity_numeric_annotations");
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut conditions: Vec<String> = Vec::new();

        if let Some(ref key) = ann_filters.key {
            params.push(Box::new(key.clone()));
            conditions.push(format!("ann_key = ?{}", params.len()));
        }
        if let Some(ref value) = ann_filters.uint_value {
            params.push(Box::new(i64::try_from(*value)?));
            conditions.push(format!("ann_value = ?{}", params.len()));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(AsRef::as_ref).collect();
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let mut rows = stmt.query(params_refs.as_slice())?;

        let mut entity_key_builder = FixedSizeBinaryBuilder::new(32);
        let mut key_builder = StringBuilder::new();
        let mut value_builder = UInt64Builder::new();

        while let Some(row) = rows.next()? {
            let entity_key: Vec<u8> = row.get(0)?;
            let ann_key: String = row.get(1)?;
            let ann_value: i64 = row.get(2)?;

            entity_key_builder.append_value(&entity_key)?;
            key_builder.append_value(&ann_key);
            value_builder.append_value(u64::try_from(ann_value)?);
        }

        let batch = RecordBatch::try_new(
            numeric_annotations_schema(),
            vec![
                Arc::new(entity_key_builder.finish()),
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )?;

        Ok(batch)
    }
}

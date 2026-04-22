use core::fmt::Write as _;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use arrow::datatypes::SchemaRef;
use eyre::WrapErr;
use glint_primitives::exex_types::EntityEventType;
use rusqlite::{Connection, OptionalExtension};

use crate::error::{arrow_err, IntoDataFusionError as _};

const SCHEMA_VERSION: &str = "3";

pub struct EntityEvent<'a> {
    pub event_type: EntityEventType,
    pub block_number: i64,
    pub entity_key: &'a [u8],
    pub owner: Option<&'a [u8]>,
    pub expires_at_block: Option<i64>,
    pub content_type: Option<&'a str>,
    pub payload: Option<&'a [u8]>,
    pub tx_hash: &'a [u8],
    pub extend_policy: Option<i64>,
    pub operator: Option<&'a [u8]>,
    pub string_annotations: &'a [(String, String)],
    pub numeric_annotations: &'a [(String, i64)],
}

pub struct EventRow<'a> {
    pub block_number: i64,
    pub block_hash: &'a [u8],
    pub tx_index: i64,
    pub tx_hash: &'a [u8],
    pub log_index: i64,
    pub event_type: i64,
    pub entity_key: &'a [u8],
    pub owner: Option<&'a [u8]>,
    pub expires_at: Option<i64>,
    pub old_expires_at: Option<i64>,
    pub content_type: Option<&'a str>,
    pub payload: Option<&'a [u8]>,
    pub extend_policy: Option<i64>,
    pub operator: Option<&'a [u8]>,
    pub gas_cost: Option<i64>,
}

struct PreviousRow {
    owner: Vec<u8>,
    expires_at_block: i64,
    content_type: String,
    payload: Vec<u8>,
    created_at_block: i64,
    tx_hash: Vec<u8>,
    extend_policy: i64,
    operator: Option<Vec<u8>>,
    string_annotations: Vec<(String, String)>,
    numeric_annotations: Vec<(String, i64)>,
}

pub struct HistoryDb<'a> {
    conn: &'a Connection,
}

impl<'a> HistoryDb<'a> {
    pub const fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    pub fn configure_pragmas(&self) -> eyre::Result<()> {
        self.conn.pragma_update(None, "journal_mode", "WAL")?;
        self.conn.pragma_update(None, "synchronous", "NORMAL")?;
        self.conn
            .pragma_update(None, "mmap_size", 256 * 1024 * 1024)?;
        self.conn.pragma_update(None, "cache_size", -64000)?;
        self.conn.pragma_update(None, "temp_store", "MEMORY")?;
        Ok(())
    }

    pub fn create_tables(&self) -> eyre::Result<()> {
        self.configure_pragmas()?;

        self.conn
            .execute_batch(
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

        self.conn.execute(
            "INSERT OR IGNORE INTO sidecar_meta (key, value) VALUES ('schema_version', ?1)",
            [SCHEMA_VERSION],
        )?;

        Ok(())
    }

    pub fn check_schema_version(&self) -> eyre::Result<()> {
        let version: String = self
            .conn
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

    pub fn drop_and_recreate(&self) -> eyre::Result<()> {
        self.conn.execute_batch(
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
        self.create_tables()
    }

    pub fn get_last_processed_block(&self) -> eyre::Result<Option<u64>> {
        let result: Option<String> = self
            .conn
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

    pub fn set_last_processed_block(&self, block: u64) -> eyre::Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO sidecar_meta (key, value) VALUES ('last_processed_block', ?1)",
            [block.to_string()],
        )?;
        Ok(())
    }

    pub fn delete_history_from_block(&self, block: u64) -> eyre::Result<()> {
        let block = i64::try_from(block)?;
        self.conn.execute(
            "DELETE FROM history_string_annotations WHERE valid_from_block >= ?1",
            [block],
        )?;
        self.conn.execute(
            "DELETE FROM history_numeric_annotations WHERE valid_from_block >= ?1",
            [block],
        )?;
        self.conn.execute(
            "UPDATE entities_history SET valid_to_block = NULL WHERE valid_to_block >= ?1",
            [block],
        )?;
        self.conn.execute(
            "DELETE FROM entities_history WHERE valid_from_block >= ?1",
            [block],
        )?;
        Ok(())
    }

    pub fn delete_events_from_block(&self, block_number: u64) -> eyre::Result<usize> {
        let block = i64::try_from(block_number).wrap_err("block number overflows i64")?;
        self.conn.execute(
            "DELETE FROM event_string_annotations WHERE block_number >= ?1",
            [block],
        )?;
        self.conn.execute(
            "DELETE FROM event_numeric_annotations WHERE block_number >= ?1",
            [block],
        )?;
        let count = self.conn.execute(
            "DELETE FROM entity_events WHERE block_number >= ?1",
            [block],
        )?;
        self.delete_history_from_block(block_number)?;
        Ok(count)
    }

    pub fn prune_before_block(&self, block_number: u64) -> eyre::Result<usize> {
        let block = i64::try_from(block_number).wrap_err("block number overflows i64")?;
        self.conn.execute(
            "DELETE FROM event_string_annotations WHERE block_number < ?1",
            [block],
        )?;
        self.conn.execute(
            "DELETE FROM event_numeric_annotations WHERE block_number < ?1",
            [block],
        )?;
        let count = self
            .conn
            .execute("DELETE FROM entity_events WHERE block_number < ?1", [block])?;
        Ok(count)
    }

    pub fn event_count(&self) -> eyre::Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |row| row.get(0))
            .wrap_err("counting entity_events")?;
        u64::try_from(count).wrap_err("event count overflows u64")
    }

    pub fn upsert_event(&self, row: &EventRow<'_>) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT INTO entity_events (
                block_number, block_hash, tx_index, tx_hash, log_index,
                event_type, entity_key, owner, expires_at_block, old_expires_at_block,
                content_type, payload, extend_policy, operator, gas_cost
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT (entity_key, block_number, log_index) DO UPDATE SET
                block_hash = excluded.block_hash,
                tx_index = excluded.tx_index,
                tx_hash = excluded.tx_hash,
                event_type = excluded.event_type,
                owner = excluded.owner,
                expires_at_block = excluded.expires_at_block,
                old_expires_at_block = excluded.old_expires_at_block,
                content_type = excluded.content_type,
                payload = excluded.payload,
                extend_policy = excluded.extend_policy,
                operator = excluded.operator,
                gas_cost = excluded.gas_cost",
            )?
            .execute(rusqlite::params![
                row.block_number,
                row.block_hash,
                row.tx_index,
                row.tx_hash,
                row.log_index,
                row.event_type,
                row.entity_key,
                row.owner,
                row.expires_at,
                row.old_expires_at,
                row.content_type,
                row.payload,
                row.extend_policy,
                row.operator,
                row.gas_cost,
            ])?;
        Ok(())
    }

    pub fn upsert_event_string_annotation(
        &self,
        entity_key: &[u8],
        block_number: i64,
        log_index: i64,
        key: &str,
        value: &str,
    ) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT INTO event_string_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (entity_key, block_number, log_index, ann_key) DO UPDATE SET ann_value = excluded.ann_value",
            )?
            .execute(rusqlite::params![
                entity_key,
                block_number,
                log_index,
                key,
                value,
            ])?;
        Ok(())
    }

    pub fn upsert_event_numeric_annotation(
        &self,
        entity_key: &[u8],
        block_number: i64,
        log_index: i64,
        key: &str,
        value: i64,
    ) -> eyre::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT INTO event_numeric_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (entity_key, block_number, log_index, ann_key) DO UPDATE SET ann_value = excluded.ann_value",
            )?
            .execute(rusqlite::params![
                entity_key,
                block_number,
                log_index,
                key,
                value,
            ])?;
        Ok(())
    }

    pub fn write_history(&self, event: &EntityEvent<'_>) -> eyre::Result<()> {
        match event.event_type {
            EntityEventType::Created => {
                let Some(owner) = event.owner else {
                    return Ok(());
                };
                let Some(expires) = event.expires_at_block else {
                    return Ok(());
                };
                let Some(ct) = event.content_type else {
                    return Ok(());
                };
                let Some(pl) = event.payload else {
                    return Ok(());
                };
                let ep = event.extend_policy.unwrap_or(0);

                self.insert_open_row(
                    event.entity_key,
                    event.block_number,
                    owner,
                    expires,
                    ct,
                    pl,
                    event.block_number,
                    event.tx_hash,
                    ep,
                    event.operator,
                    event.string_annotations,
                    event.numeric_annotations,
                )?;
            }
            EntityEventType::Updated => {
                let prev = self.close_current_row(event.entity_key, event.block_number)?;

                let Some(owner) = event.owner else {
                    return Ok(());
                };
                let Some(expires) = event.expires_at_block else {
                    return Ok(());
                };
                let Some(ct) = event.content_type else {
                    return Ok(());
                };
                let Some(pl) = event.payload else {
                    return Ok(());
                };
                let ep = event.extend_policy.unwrap_or(0);

                let created_at = prev
                    .as_ref()
                    .map_or(event.block_number, |p| p.created_at_block);

                self.insert_open_row(
                    event.entity_key,
                    event.block_number,
                    owner,
                    expires,
                    ct,
                    pl,
                    created_at,
                    event.tx_hash,
                    ep,
                    event.operator,
                    event.string_annotations,
                    event.numeric_annotations,
                )?;
            }
            EntityEventType::Deleted | EntityEventType::Expired => {
                self.close_current_row(event.entity_key, event.block_number)?;
            }
            EntityEventType::Extended => {
                let prev = self.close_current_row(event.entity_key, event.block_number)?;
                let Some(prev) = prev else {
                    return Ok(());
                };
                let new_expires = event.expires_at_block.unwrap_or(prev.expires_at_block);

                self.insert_open_row(
                    event.entity_key,
                    event.block_number,
                    &prev.owner,
                    new_expires,
                    &prev.content_type,
                    &prev.payload,
                    prev.created_at_block,
                    &prev.tx_hash,
                    prev.extend_policy,
                    prev.operator.as_deref(),
                    &prev.string_annotations,
                    &prev.numeric_annotations,
                )?;
            }
            EntityEventType::PermissionsChanged => {
                let prev = self.close_current_row(event.entity_key, event.block_number)?;
                let Some(prev) = prev else {
                    return Ok(());
                };

                let new_owner = event.owner.unwrap_or(&prev.owner);
                let new_ep = event.extend_policy.unwrap_or(prev.extend_policy);
                let new_operator = event.operator.or(prev.operator.as_deref());

                self.insert_open_row(
                    event.entity_key,
                    event.block_number,
                    new_owner,
                    prev.expires_at_block,
                    &prev.content_type,
                    &prev.payload,
                    prev.created_at_block,
                    &prev.tx_hash,
                    new_ep,
                    new_operator,
                    &prev.string_annotations,
                    &prev.numeric_annotations,
                )?;
            }
        }

        Ok(())
    }

    fn close_current_row(
        &self,
        entity_key: &[u8],
        block: i64,
    ) -> eyre::Result<Option<PreviousRow>> {
        let mut query = self.conn.prepare_cached(
            "SELECT owner, expires_at_block, content_type, payload, created_at_block,
                tx_hash, extend_policy, operator, valid_from_block
         FROM entities_history
         WHERE entity_key = ?1 AND valid_to_block IS NULL",
        )?;

        let row = query
            .query_row([entity_key], |r| {
                Ok((
                    PreviousRow {
                        owner: r.get(0)?,
                        expires_at_block: r.get(1)?,
                        content_type: r.get(2)?,
                        payload: r.get(3)?,
                        created_at_block: r.get(4)?,
                        tx_hash: r.get(5)?,
                        extend_policy: r.get(6)?,
                        operator: r.get(7)?,
                        string_annotations: Vec::new(),
                        numeric_annotations: Vec::new(),
                    },
                    r.get::<_, i64>(8)?,
                ))
            })
            .optional()
            .wrap_err("reading current open history row")?;

        let Some((mut prev, valid_from)) = row else {
            return Ok(None);
        };

        prev.string_annotations = self.read_string_annotations(entity_key, valid_from)?;
        prev.numeric_annotations = self.read_numeric_annotations(entity_key, valid_from)?;

        self.conn.execute(
            "UPDATE entities_history SET valid_to_block = ?1
         WHERE entity_key = ?2 AND valid_to_block IS NULL",
            rusqlite::params![block, entity_key],
        )?;

        Ok(Some(prev))
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_open_row(
        &self,
        entity_key: &[u8],
        block: i64,
        owner: &[u8],
        expires_at_block: i64,
        content_type: &str,
        payload: &[u8],
        created_at_block: i64,
        tx_hash: &[u8],
        extend_policy: i64,
        operator: Option<&[u8]>,
        string_annotations: &[(String, String)],
        numeric_annotations: &[(String, i64)],
    ) -> eyre::Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO entities_history (
            entity_key, valid_from_block, valid_to_block, owner, expires_at_block,
            content_type, payload, created_at_block, tx_hash, extend_policy, operator
        ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                entity_key,
                block,
                owner,
                expires_at_block,
                content_type,
                payload,
                created_at_block,
                tx_hash,
                extend_policy,
                operator,
            ],
        )?;

        let mut insert_str = self.conn.prepare_cached(
            "INSERT OR REPLACE INTO history_string_annotations (entity_key, valid_from_block, ann_key, ann_value)
         VALUES (?1, ?2, ?3, ?4)",
        )?;
        for (k, v) in string_annotations {
            insert_str.execute(rusqlite::params![entity_key, block, k, v])?;
        }

        let mut insert_num = self.conn.prepare_cached(
            "INSERT OR REPLACE INTO history_numeric_annotations (entity_key, valid_from_block, ann_key, ann_value)
         VALUES (?1, ?2, ?3, ?4)",
        )?;
        for (k, v) in numeric_annotations {
            insert_num.execute(rusqlite::params![entity_key, block, k, v])?;
        }

        Ok(())
    }

    fn read_string_annotations(
        &self,
        entity_key: &[u8],
        valid_from_block: i64,
    ) -> eyre::Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT ann_key, ann_value FROM history_string_annotations
         WHERE entity_key = ?1 AND valid_from_block = ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![entity_key, valid_from_block], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .wrap_err("reading history string annotations")
    }

    fn read_numeric_annotations(
        &self,
        entity_key: &[u8],
        valid_from_block: i64,
    ) -> eyre::Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT ann_key, ann_value FROM history_numeric_annotations
         WHERE entity_key = ?1 AND valid_from_block = ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![entity_key, valid_from_block], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .wrap_err("reading history numeric annotations")
    }

    pub fn query_events(
        &self,
        from_block: u64,
        to_block: u64,
        schema: &SchemaRef,
    ) -> datafusion::error::Result<RecordBatch> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT block_number, log_index, event_type, entity_key, owner, expires_at_block,
                    content_type, payload, extend_policy, operator, gas_cost
             FROM entity_events
             WHERE block_number >= ?1 AND block_number <= ?2
             ORDER BY block_number, log_index",
            )
            .df_err()?;

        let from_i64 = i64::try_from(from_block).df_err()?;
        let to_i64 = i64::try_from(to_block).df_err()?;
        let mut rows = stmt.query([from_i64, to_i64]).df_err()?;

        let mut block_number = UInt64Builder::new();
        let mut log_index = UInt32Builder::new();
        let mut event_type = UInt8Builder::new();
        let mut entity_key = FixedSizeBinaryBuilder::new(32);
        let mut owner = FixedSizeBinaryBuilder::new(20);
        let mut expires_at = UInt64Builder::new();
        let mut content_type = StringBuilder::new();
        let mut payload = BinaryBuilder::new();
        let mut extend_policy = UInt8Builder::new();
        let mut operator = FixedSizeBinaryBuilder::new(20);
        let mut gas_cost = UInt64Builder::new();

        while let Some(row) = rows.next().df_err()? {
            block_number.append_value(u64::try_from(row.get::<_, i64>(0).df_err()?).df_err()?);
            log_index.append_value(u32::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
            event_type.append_value(u8::try_from(row.get::<_, i64>(2).df_err()?).df_err()?);
            entity_key
                .append_value(row.get_ref(3).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;

            match row.get_ref(4).df_err()?.as_blob_or_null() {
                Ok(Some(v)) => owner.append_value(v).map_err(arrow_err)?,
                _ => owner.append_null(),
            }

            match row.get::<_, Option<i64>>(5).df_err()? {
                Some(v) => expires_at.append_value(u64::try_from(v).df_err()?),
                None => expires_at.append_null(),
            }

            match row.get_ref(6).df_err()?.as_str_or_null() {
                Ok(Some(v)) => content_type.append_value(v),
                _ => content_type.append_null(),
            }

            match row.get_ref(7).df_err()?.as_blob_or_null() {
                Ok(Some(v)) => payload.append_value(v),
                _ => payload.append_null(),
            }

            match row.get::<_, Option<i64>>(8).df_err()? {
                Some(v) => extend_policy.append_value(u8::try_from(v).df_err()?),
                None => extend_policy.append_null(),
            }

            match row.get_ref(9).df_err()?.as_blob_or_null() {
                Ok(Some(v)) => operator.append_value(v).map_err(arrow_err)?,
                _ => operator.append_null(),
            }

            match row.get::<_, Option<i64>>(10).df_err()? {
                Some(v) => gas_cost.append_value(u64::try_from(v).df_err()?),
                None => gas_cost.append_null(),
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(block_number.finish()),
            Arc::new(log_index.finish()),
            Arc::new(event_type.finish()),
            Arc::new(entity_key.finish()),
            Arc::new(owner.finish()),
            Arc::new(expires_at.finish()),
            Arc::new(content_type.finish()),
            Arc::new(payload.finish()),
            Arc::new(extend_policy.finish()),
            Arc::new(operator.finish()),
            Arc::new(gas_cost.finish()),
        ];

        RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
    }

    pub fn query_entities_history(
        &self,
        valid_from_upper: u64,
        valid_to_lower: u64,
        schema: &SchemaRef,
    ) -> datafusion::error::Result<RecordBatch> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT entity_key, valid_from_block, valid_to_block, owner, expires_at_block,
                    content_type, payload, created_at_block, tx_hash, extend_policy, operator
             FROM entities_history
             WHERE valid_from_block <= ?1
               AND (valid_to_block IS NULL OR valid_to_block > ?2)
             ORDER BY entity_key, valid_from_block",
            )
            .df_err()?;

        let upper_i64 = i64::try_from(valid_from_upper).df_err()?;
        let lower_i64 = i64::try_from(valid_to_lower).df_err()?;
        let mut rows = stmt.query([upper_i64, lower_i64]).df_err()?;

        let mut entity_key = FixedSizeBinaryBuilder::new(32);
        let mut valid_from = UInt64Builder::new();
        let mut valid_to = UInt64Builder::new();
        let mut owner = FixedSizeBinaryBuilder::new(20);
        let mut expires_at = UInt64Builder::new();
        let mut content_type = StringBuilder::new();
        let mut payload = BinaryBuilder::new();
        let mut created_at = UInt64Builder::new();
        let mut tx_hash = FixedSizeBinaryBuilder::new(32);
        let mut extend_policy = UInt8Builder::new();
        let mut operator = FixedSizeBinaryBuilder::new(20);
        let mut block_number = UInt64Builder::new();

        while let Some(row) = rows.next().df_err()? {
            entity_key
                .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;

            let vf = u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?;
            valid_from.append_value(vf);

            match row.get::<_, Option<i64>>(2).df_err()? {
                Some(v) => valid_to.append_value(u64::try_from(v).df_err()?),
                None => valid_to.append_null(),
            }

            owner
                .append_value(row.get_ref(3).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;
            expires_at.append_value(u64::try_from(row.get::<_, i64>(4).df_err()?).df_err()?);
            content_type.append_value(row.get_ref(5).df_err()?.as_str().df_err()?);
            payload.append_value(row.get_ref(6).df_err()?.as_blob().df_err()?);
            created_at.append_value(u64::try_from(row.get::<_, i64>(7).df_err()?).df_err()?);
            tx_hash
                .append_value(row.get_ref(8).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;
            extend_policy.append_value(u8::try_from(row.get::<_, i64>(9).df_err()?).df_err()?);

            match row.get_ref(10).df_err()?.as_blob_or_null() {
                Ok(Some(v)) => operator.append_value(v).map_err(arrow_err)?,
                _ => operator.append_null(),
            }

            block_number.append_value(vf);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(entity_key.finish()),
            Arc::new(valid_from.finish()),
            Arc::new(valid_to.finish()),
            Arc::new(owner.finish()),
            Arc::new(expires_at.finish()),
            Arc::new(content_type.finish()),
            Arc::new(payload.finish()),
            Arc::new(created_at.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(extend_policy.finish()),
            Arc::new(operator.finish()),
            Arc::new(block_number.finish()),
        ];

        RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
    }

    pub fn query_event_string_annotations(
        &self,
        from_block: u64,
        to_block: u64,
        ann_key: Option<&str>,
        ann_value: Option<&str>,
        schema: &SchemaRef,
    ) -> datafusion::error::Result<RecordBatch> {
        let mut sql = String::from(
            "SELECT entity_key, block_number, log_index, ann_key, ann_value \
         FROM event_string_annotations \
         WHERE block_number >= ?1 AND block_number <= ?2",
        );

        let mut param_idx = 3;
        let key_idx = if ann_key.is_some() {
            let _ = write!(sql, " AND ann_key = ?{param_idx}");
            let idx = param_idx;
            param_idx += 1;
            Some(idx)
        } else {
            None
        };
        let value_idx = if ann_value.is_some() {
            let _ = write!(sql, " AND ann_value = ?{param_idx}");
            Some(param_idx)
        } else {
            None
        };

        sql.push_str(" ORDER BY block_number, log_index");

        let mut stmt = self.conn.prepare_cached(&sql).df_err()?;

        let from_i64 = i64::try_from(from_block).df_err()?;
        let to_i64 = i64::try_from(to_block).df_err()?;

        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(from_i64), Box::new(to_i64)];
        if let (Some(_), Some(k)) = (key_idx, &ann_key) {
            params.push(Box::new(k.to_string()));
        }
        if let (Some(_), Some(v)) = (value_idx, &ann_value) {
            params.push(Box::new(v.to_string()));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(AsRef::as_ref).collect();
        let mut rows = stmt.query(param_refs.as_slice()).df_err()?;

        let mut entity_key = FixedSizeBinaryBuilder::new(32);
        let mut block_number = UInt64Builder::new();
        let mut log_index = UInt32Builder::new();
        let mut key_builder = StringBuilder::new();
        let mut value_builder = StringBuilder::new();

        while let Some(row) = rows.next().df_err()? {
            entity_key
                .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;
            block_number.append_value(u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
            log_index.append_value(u32::try_from(row.get::<_, i64>(2).df_err()?).df_err()?);
            key_builder.append_value(row.get_ref(3).df_err()?.as_str().df_err()?);
            value_builder.append_value(row.get_ref(4).df_err()?.as_str().df_err()?);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(entity_key.finish()),
            Arc::new(block_number.finish()),
            Arc::new(log_index.finish()),
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ];

        RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
    }

    pub fn query_event_numeric_annotations(
        &self,
        from_block: u64,
        to_block: u64,
        ann_key: Option<&str>,
        ann_value: Option<u64>,
        schema: &SchemaRef,
    ) -> datafusion::error::Result<RecordBatch> {
        let mut sql = String::from(
            "SELECT entity_key, block_number, log_index, ann_key, ann_value \
         FROM event_numeric_annotations \
         WHERE block_number >= ?1 AND block_number <= ?2",
        );

        let mut param_idx = 3;
        let key_idx = if ann_key.is_some() {
            let _ = write!(sql, " AND ann_key = ?{param_idx}");
            let idx = param_idx;
            param_idx += 1;
            Some(idx)
        } else {
            None
        };
        let value_idx = if ann_value.is_some() {
            let _ = write!(sql, " AND ann_value = ?{param_idx}");
            Some(param_idx)
        } else {
            None
        };

        sql.push_str(" ORDER BY block_number, log_index");

        let mut stmt = self.conn.prepare_cached(&sql).df_err()?;

        let from_i64 = i64::try_from(from_block).df_err()?;
        let to_i64 = i64::try_from(to_block).df_err()?;

        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(from_i64), Box::new(to_i64)];
        if let (Some(_), Some(k)) = (key_idx, &ann_key) {
            params.push(Box::new(k.to_string()));
        }
        if let (Some(_), Some(v)) = (value_idx, ann_value) {
            params.push(Box::new(i64::try_from(v).df_err()?));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(AsRef::as_ref).collect();
        let mut rows = stmt.query(param_refs.as_slice()).df_err()?;

        let mut entity_key = FixedSizeBinaryBuilder::new(32);
        let mut block_number = UInt64Builder::new();
        let mut log_index = UInt32Builder::new();
        let mut key_builder = StringBuilder::new();
        let mut value_builder = UInt64Builder::new();

        while let Some(row) = rows.next().df_err()? {
            entity_key
                .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;
            block_number.append_value(u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
            log_index.append_value(u32::try_from(row.get::<_, i64>(2).df_err()?).df_err()?);
            key_builder.append_value(row.get_ref(3).df_err()?.as_str().df_err()?);
            value_builder.append_value(u64::try_from(row.get::<_, i64>(4).df_err()?).df_err()?);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(entity_key.finish()),
            Arc::new(block_number.finish()),
            Arc::new(log_index.finish()),
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ];

        RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
    }

    pub fn query_history_string_annotations(
        &self,
        from_block: u64,
        to_block: u64,
        ann_key: Option<&str>,
        ann_value: Option<&str>,
        schema: &SchemaRef,
    ) -> datafusion::error::Result<RecordBatch> {
        let mut sql = String::from(
            "SELECT entity_key, valid_from_block, ann_key, ann_value \
         FROM history_string_annotations \
         WHERE valid_from_block >= ?1 AND valid_from_block <= ?2",
        );

        let mut param_idx = 3;
        let key_idx = if ann_key.is_some() {
            let _ = write!(sql, " AND ann_key = ?{param_idx}");
            let idx = param_idx;
            param_idx += 1;
            Some(idx)
        } else {
            None
        };
        let value_idx = if ann_value.is_some() {
            let _ = write!(sql, " AND ann_value = ?{param_idx}");
            Some(param_idx)
        } else {
            None
        };

        sql.push_str(" ORDER BY entity_key, valid_from_block");

        let mut stmt = self.conn.prepare_cached(&sql).df_err()?;

        let from_i64 = i64::try_from(from_block).df_err()?;
        let to_i64 = i64::try_from(to_block).df_err()?;

        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(from_i64), Box::new(to_i64)];
        if let (Some(_), Some(k)) = (key_idx, &ann_key) {
            params.push(Box::new(k.to_string()));
        }
        if let (Some(_), Some(v)) = (value_idx, &ann_value) {
            params.push(Box::new(v.to_string()));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(AsRef::as_ref).collect();
        let mut rows = stmt.query(param_refs.as_slice()).df_err()?;

        let mut entity_key = FixedSizeBinaryBuilder::new(32);
        let mut valid_from = UInt64Builder::new();
        let mut key_builder = StringBuilder::new();
        let mut value_builder = StringBuilder::new();

        while let Some(row) = rows.next().df_err()? {
            entity_key
                .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;
            valid_from.append_value(u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
            key_builder.append_value(row.get_ref(2).df_err()?.as_str().df_err()?);
            value_builder.append_value(row.get_ref(3).df_err()?.as_str().df_err()?);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(entity_key.finish()),
            Arc::new(valid_from.finish()),
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ];

        RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
    }

    pub fn query_history_numeric_annotations(
        &self,
        from_block: u64,
        to_block: u64,
        ann_key: Option<&str>,
        ann_value: Option<u64>,
        schema: &SchemaRef,
    ) -> datafusion::error::Result<RecordBatch> {
        let mut sql = String::from(
            "SELECT entity_key, valid_from_block, ann_key, ann_value \
         FROM history_numeric_annotations \
         WHERE valid_from_block >= ?1 AND valid_from_block <= ?2",
        );

        let mut param_idx = 3;
        let key_idx = if ann_key.is_some() {
            let _ = write!(sql, " AND ann_key = ?{param_idx}");
            let idx = param_idx;
            param_idx += 1;
            Some(idx)
        } else {
            None
        };
        let value_idx = if ann_value.is_some() {
            let _ = write!(sql, " AND ann_value = ?{param_idx}");
            Some(param_idx)
        } else {
            None
        };

        sql.push_str(" ORDER BY entity_key, valid_from_block");

        let mut stmt = self.conn.prepare_cached(&sql).df_err()?;

        let from_i64 = i64::try_from(from_block).df_err()?;
        let to_i64 = i64::try_from(to_block).df_err()?;

        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(from_i64), Box::new(to_i64)];
        if let (Some(_), Some(k)) = (key_idx, &ann_key) {
            params.push(Box::new(k.to_string()));
        }
        if let (Some(_), Some(v)) = (value_idx, ann_value) {
            params.push(Box::new(i64::try_from(v).df_err()?));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(AsRef::as_ref).collect();
        let mut rows = stmt.query(param_refs.as_slice()).df_err()?;

        let mut entity_key = FixedSizeBinaryBuilder::new(32);
        let mut valid_from = UInt64Builder::new();
        let mut key_builder = StringBuilder::new();
        let mut value_builder = UInt64Builder::new();

        while let Some(row) = rows.next().df_err()? {
            entity_key
                .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
                .map_err(arrow_err)?;
            valid_from.append_value(u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
            key_builder.append_value(row.get_ref(2).df_err()?.as_str().df_err()?);
            value_builder.append_value(u64::try_from(row.get::<_, i64>(3).df_err()?).df_err()?);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(entity_key.finish()),
            Arc::new(valid_from.finish()),
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ];

        RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
    }
}

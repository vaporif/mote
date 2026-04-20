use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::array::{
    Array, AsArray, BinaryArray, FixedSizeBinaryArray, MapArray, StringArray, UInt8Array,
    UInt64Array,
};
use arrow::record_batch::RecordBatch;
use eyre::WrapErr;
use glint_primitives::columns;
use glint_primitives::exex_types::{BatchOp, EntityEventType, WATERMARK_OP};
use parking_lot::Mutex;
use rusqlite::Connection;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResult {
    Applied,
    Watermark,
    NeedsReplay,
}

pub fn apply_batch(
    conn: &Arc<Mutex<Connection>>,
    current_block: &Arc<AtomicU64>,
    batch: &RecordBatch,
) -> eyre::Result<ApplyResult> {
    if batch.num_rows() == 0 {
        return Ok(ApplyResult::Applied);
    }

    let op_col = col_u8(batch, columns::OP)?;
    let op_val = op_col.value(0);
    if op_val == WATERMARK_OP {
        return Ok(ApplyResult::Watermark);
    }

    let op = BatchOp::try_from(op_val).map_err(|v| eyre::eyre!("unknown BatchOp value: {v}"))?;

    let block = batch_block_number(batch);
    if let Some(b) = block {
        match op {
            BatchOp::Commit => current_block.store(b, Ordering::Relaxed),
            BatchOp::Revert => current_block.store(b.saturating_sub(1), Ordering::Relaxed),
        }
    }

    let nrows = batch.num_rows();
    match op {
        BatchOp::Commit => {
            apply_commit(conn, batch, nrows).wrap_err("applying commit batch")?;
            Ok(ApplyResult::Applied)
        }
        BatchOp::Revert => apply_revert(conn, batch, nrows).wrap_err("applying revert batch"),
    }
}

#[allow(clippy::significant_drop_tightening)]
fn apply_commit(
    conn: &Arc<Mutex<Connection>>,
    batch: &RecordBatch,
    nrows: usize,
) -> eyre::Result<()> {
    let block_number_col = col_u64(batch, columns::BLOCK_NUMBER)?;
    let tx_hash_col = col_fsb(batch, columns::TX_HASH)?;
    let event_type_col = col_u8(batch, columns::EVENT_TYPE)?;
    let entity_key_col = col_fsb(batch, columns::ENTITY_KEY)?;
    let owner_col = col_fsb(batch, columns::OWNER)?;
    let expires_col = col_u64(batch, columns::EXPIRES_AT_BLOCK)?;
    let content_type_col = col_string(batch, columns::CONTENT_TYPE)?;
    let payload_col = col_binary(batch, columns::PAYLOAD)?;
    let str_ann_col = col_map(batch, columns::STRING_ANNOTATIONS)?;
    let num_ann_col = col_map(batch, columns::NUMERIC_ANNOTATIONS)?;
    let extend_policy_col = col_u8(batch, columns::EXTEND_POLICY)?;
    let operator_col = col_fsb(batch, columns::OPERATOR)?;

    let guard = conn.lock();
    let tx = guard
        .unchecked_transaction()
        .wrap_err("starting transaction")?;

    {
        let mut insert_stmt = tx.prepare_cached(
            "INSERT OR REPLACE INTO entities_latest (
                entity_key, owner, expires_at_block, content_type, payload,
                created_at_block, tx_hash, extend_policy, operator
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;

        let mut upsert_stmt = tx.prepare_cached(
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
        )?;

        let mut insert_str_ann = tx.prepare_cached(
            "INSERT OR REPLACE INTO entity_string_annotations (entity_key, ann_key, ann_value) VALUES (?1, ?2, ?3)",
        )?;
        let mut insert_num_ann = tx.prepare_cached(
            "INSERT OR REPLACE INTO entity_numeric_annotations (entity_key, ann_key, ann_value) VALUES (?1, ?2, ?3)",
        )?;
        let mut delete_str_ann =
            tx.prepare_cached("DELETE FROM entity_string_annotations WHERE entity_key = ?1")?;
        let mut delete_num_ann =
            tx.prepare_cached("DELETE FROM entity_numeric_annotations WHERE entity_key = ?1")?;

        let mut delete_stmt =
            tx.prepare_cached("DELETE FROM entities_latest WHERE entity_key = ?1")?;

        let mut extend_stmt = tx.prepare_cached(
            "UPDATE entities_latest SET expires_at_block = ?1, tx_hash = ?2
             WHERE entity_key = ?3",
        )?;

        let mut perms_stmt = tx.prepare_cached(
            "UPDATE entities_latest SET owner = ?1, extend_policy = ?2, operator = ?3
             WHERE entity_key = ?4",
        )?;

        for i in 0..nrows {
            let event_type_raw = event_type_col.value(i);
            let event_type = EntityEventType::try_from(event_type_raw)
                .map_err(|v| eyre::eyre!("unknown EntityEventType value: {v}"))?;

            let entity_key = entity_key_col.value(i);

            match event_type {
                EntityEventType::Created | EntityEventType::Updated => {
                    let block_number = i64::try_from(block_number_col.value(i))?;
                    let owner = owner_col.value(i);
                    let expires = i64::try_from(expires_col.value(i))?;
                    let content_type =
                        (!content_type_col.is_null(i)).then(|| content_type_col.value(i));
                    let payload: Option<&[u8]> =
                        (!payload_col.is_null(i)).then(|| payload_col.value(i));
                    let tx_hash = tx_hash_col.value(i);
                    let extend_policy = i64::from(extend_policy_col.value(i));
                    let operator: Option<&[u8]> =
                        (!operator_col.is_null(i)).then(|| operator_col.value(i));

                    let params = rusqlite::params![
                        entity_key,
                        owner,
                        expires,
                        content_type,
                        payload,
                        block_number,
                        tx_hash,
                        extend_policy,
                        operator,
                    ];

                    if event_type == EntityEventType::Created {
                        insert_stmt.execute(params)?;
                    } else {
                        upsert_stmt.execute(params)?;
                    }

                    // clear old annotations before re-inserting
                    delete_str_ann.execute([entity_key])?;
                    delete_num_ann.execute([entity_key])?;

                    if !str_ann_col.is_null(i) {
                        let offsets = str_ann_col.value_offsets();
                        let start = usize::try_from(offsets[i])?;
                        let end = usize::try_from(offsets[i + 1])?;
                        let keys = str_ann_col.keys().as_string::<i32>();
                        let values = str_ann_col.values().as_string::<i32>();
                        for j in start..end {
                            insert_str_ann.execute(rusqlite::params![
                                entity_key,
                                keys.value(j),
                                values.value(j),
                            ])?;
                        }
                    }

                    if !num_ann_col.is_null(i) {
                        let offsets = num_ann_col.value_offsets();
                        let start = usize::try_from(offsets[i])?;
                        let end = usize::try_from(offsets[i + 1])?;
                        let keys = num_ann_col.keys().as_string::<i32>();
                        let values = num_ann_col
                            .values()
                            .as_primitive::<arrow::datatypes::UInt64Type>();
                        for j in start..end {
                            insert_num_ann.execute(rusqlite::params![
                                entity_key,
                                keys.value(j),
                                i64::try_from(values.value(j))?,
                            ])?;
                        }
                    }
                }
                EntityEventType::Deleted | EntityEventType::Expired => {
                    delete_str_ann.execute([entity_key])?;
                    delete_num_ann.execute([entity_key])?;
                    delete_stmt.execute([entity_key])?;
                }
                EntityEventType::Extended => {
                    let new_expires = i64::try_from(expires_col.value(i))?;
                    let tx_hash = tx_hash_col.value(i);
                    extend_stmt.execute(rusqlite::params![new_expires, tx_hash, entity_key])?;
                }
                EntityEventType::PermissionsChanged => {
                    let new_owner = owner_col.value(i);
                    let extend_policy = i64::from(extend_policy_col.value(i));
                    let operator: Option<&[u8]> =
                        (!operator_col.is_null(i)).then(|| operator_col.value(i));
                    perms_stmt.execute(rusqlite::params![
                        new_owner,
                        extend_policy,
                        operator,
                        entity_key
                    ])?;
                }
            }
        }
    }

    tx.commit()
        .wrap_err("committing entities_latest transaction")?;
    Ok(())
}

#[allow(clippy::significant_drop_tightening)]
fn apply_revert(
    conn: &Arc<Mutex<Connection>>,
    batch: &RecordBatch,
    nrows: usize,
) -> eyre::Result<ApplyResult> {
    let event_type_col = col_u8(batch, columns::EVENT_TYPE)?;
    let entity_key_col = col_fsb(batch, columns::ENTITY_KEY)?;
    let old_expires_col = col_u64(batch, columns::OLD_EXPIRES_AT_BLOCK)?;

    let guard = conn.lock();
    let tx = guard
        .unchecked_transaction()
        .wrap_err("starting revert transaction")?;

    for i in (0..nrows).rev() {
        let event_type_raw = event_type_col.value(i);
        let event_type = EntityEventType::try_from(event_type_raw)
            .map_err(|v| eyre::eyre!("unknown EntityEventType value: {v}"))?;

        let entity_key = entity_key_col.value(i);

        match event_type {
            EntityEventType::Created => {
                tx.execute(
                    "DELETE FROM entity_string_annotations WHERE entity_key = ?1",
                    [entity_key],
                )?;
                tx.execute(
                    "DELETE FROM entity_numeric_annotations WHERE entity_key = ?1",
                    [entity_key],
                )?;
                tx.execute(
                    "DELETE FROM entities_latest WHERE entity_key = ?1",
                    [entity_key],
                )?;
            }
            EntityEventType::Extended => {
                let old_expires = i64::try_from(old_expires_col.value(i))?;
                tx.execute(
                    "UPDATE entities_latest SET expires_at_block = ?1 WHERE entity_key = ?2",
                    rusqlite::params![old_expires, entity_key],
                )?;
            }
            EntityEventType::Updated
            | EntityEventType::Deleted
            | EntityEventType::Expired
            | EntityEventType::PermissionsChanged => {
                warn!(
                    event_type = ?event_type,
                    "revert of event type requires full replay"
                );
                tx.commit().wrap_err("committing partial revert")?;
                return Ok(ApplyResult::NeedsReplay);
            }
        }
    }

    tx.commit().wrap_err("committing revert transaction")?;
    Ok(ApplyResult::Applied)
}

fn batch_block_number(batch: &RecordBatch) -> Option<u64> {
    if batch.num_rows() == 0 {
        return None;
    }
    col_u64(batch, columns::BLOCK_NUMBER)
        .ok()
        .map(|col| col.value(0))
}

macro_rules! col {
    ($batch:expr, $name:expr, $ty:ty) => {
        $batch
            .column_by_name($name)
            .ok_or_else(|| eyre::eyre!("missing column: {}", $name))?
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| eyre::eyre!("column {} is not {}", $name, stringify!($ty)))
    };
}

fn col_u8<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt8Array> {
    col!(batch, name, UInt8Array)
}

fn col_u64<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt64Array> {
    col!(batch, name, UInt64Array)
}

fn col_fsb<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a FixedSizeBinaryArray> {
    col!(batch, name, FixedSizeBinaryArray)
}

fn col_string<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a StringArray> {
    col!(batch, name, StringArray)
}

fn col_binary<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a BinaryArray> {
    col!(batch, name, BinaryArray)
}

fn col_map<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a MapArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| eyre::eyre!("missing column: {name}"))?
        .as_map_opt()
        .ok_or_else(|| eyre::eyre!("column {name} is not MapArray"))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::schema;
    use alloy_primitives::{Address, B256};
    use glint_primitives::test_utils::{EventBuilder, build_batch, build_watermark_batch};
    use rusqlite::Connection;

    const KEY: B256 = B256::repeat_byte(0x01);
    const OWNER: Address = Address::repeat_byte(0x02);
    const TX: B256 = B256::repeat_byte(0xAA);

    fn setup() -> (Arc<Mutex<Connection>>, Arc<AtomicU64>) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sidecar_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;",
        )
        .unwrap();
        schema::create_table(&conn).unwrap();
        (Arc::new(Mutex::new(conn)), Arc::new(AtomicU64::new(0)))
    }

    fn count_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM entities_latest", [], |r| r.get(0))
            .unwrap()
    }

    fn get_expires(conn: &Connection, key: &[u8]) -> i64 {
        conn.query_row(
            "SELECT expires_at_block FROM entities_latest WHERE entity_key = ?1",
            [key],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn get_owner(conn: &Connection, key: &[u8]) -> Vec<u8> {
        conn.query_row(
            "SELECT owner FROM entities_latest WHERE entity_key = ?1",
            [key],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn get_created_at(conn: &Connection, key: &[u8]) -> i64 {
        conn.query_row(
            "SELECT created_at_block FROM entities_latest WHERE entity_key = ?1",
            [key],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn created_inserts_row() {
        let (conn, block) = setup();
        let batch = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)
            .with_string_annotations(vec![("sk".into(), "sv".into())])
            .with_numeric_annotations(vec![("nk".into(), 99)])]);

        let result = apply_batch(&conn, &block, &batch).unwrap();
        assert_eq!(result, ApplyResult::Applied);

        let guard = conn.lock();
        assert_eq!(count_rows(&guard), 1);
        assert_eq!(get_expires(&guard, KEY.as_slice()), 200);
        assert_eq!(get_created_at(&guard, KEY.as_slice()), 10);
        assert_eq!(block.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn deleted_removes_row() {
        let (conn, block) = setup();

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();

        let delete = build_batch(&[EventBuilder::deleted(11, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &delete).unwrap();

        assert_eq!(count_rows(&conn.lock()), 0);
    }

    #[test]
    fn expired_removes_row() {
        let (conn, block) = setup();

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();

        let expired = build_batch(&[EventBuilder::expired(11, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &expired).unwrap();

        assert_eq!(count_rows(&conn.lock()), 0);
    }

    #[test]
    fn extended_updates_only_expiry_and_tx_hash() {
        let (conn, block) = setup();

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();

        let new_tx = B256::repeat_byte(0xBB);
        let extend = build_batch(&[EventBuilder::extended(11, 0x01, 200, 500)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(new_tx)]);
        apply_batch(&conn, &block, &extend).unwrap();

        let guard = conn.lock();
        assert_eq!(get_expires(&guard, KEY.as_slice()), 500);
        assert_eq!(get_created_at(&guard, KEY.as_slice()), 10);
    }

    #[test]
    fn updated_preserves_created_at_block() {
        let (conn, block) = setup();

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();

        let update = build_batch(&[EventBuilder::updated(20, 0x01, 200, 300)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(TX)
            .with_payload(b"updated")
            .with_string_annotations(vec![])
            .with_numeric_annotations(vec![])]);
        apply_batch(&conn, &block, &update).unwrap();

        let guard = conn.lock();
        assert_eq!(count_rows(&guard), 1);
        assert_eq!(get_expires(&guard, KEY.as_slice()), 300);
        assert_eq!(get_created_at(&guard, KEY.as_slice()), 10);
    }

    #[test]
    fn permissions_changed_updates_owner_policy_operator() {
        let (conn, block) = setup();

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();

        let new_owner = Address::repeat_byte(0x42);
        let pc = build_batch(&[EventBuilder::permissions_changed(
            11,
            0x01,
            new_owner,
            1,
            Address::repeat_byte(0x99),
        )
        .with_entity_key(KEY)
        .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &pc).unwrap();

        let guard = conn.lock();
        assert_eq!(count_rows(&guard), 1);
        assert_eq!(get_owner(&guard, KEY.as_slice()), new_owner.as_slice());
        assert_eq!(get_expires(&guard, KEY.as_slice()), 200);
    }

    #[test]
    fn watermark_returns_watermark() {
        let (conn, block) = setup();
        let batch = build_watermark_batch();
        let result = apply_batch(&conn, &block, &batch).unwrap();
        assert_eq!(result, ApplyResult::Watermark);
    }

    #[test]
    fn revert_created_deletes_row() {
        let (conn, block) = setup();
        use glint_primitives::exex_types::BatchOp;

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();
        assert_eq!(count_rows(&conn.lock()), 1);

        let revert = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)
            .with_op(BatchOp::Revert)]);
        let result = apply_batch(&conn, &block, &revert).unwrap();
        assert_eq!(result, ApplyResult::Applied);
        assert_eq!(count_rows(&conn.lock()), 0);
    }

    #[test]
    fn revert_extended_restores_old_expiry() {
        let (conn, block) = setup();
        use glint_primitives::exex_types::BatchOp;

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();

        let extend = build_batch(&[EventBuilder::extended(11, 0x01, 200, 500)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(B256::repeat_byte(0xBB))]);
        apply_batch(&conn, &block, &extend).unwrap();
        assert_eq!(get_expires(&conn.lock(), KEY.as_slice()), 500);

        let revert = build_batch(&[EventBuilder::extended(11, 0x01, 200, 500)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(B256::repeat_byte(0xBB))
            .with_op(BatchOp::Revert)]);
        let result = apply_batch(&conn, &block, &revert).unwrap();
        assert_eq!(result, ApplyResult::Applied);
        assert_eq!(get_expires(&conn.lock(), KEY.as_slice()), 200);
    }

    #[test]
    fn revert_updated_triggers_needs_replay() {
        let (conn, block) = setup();
        use glint_primitives::exex_types::BatchOp;

        let create = build_batch(&[EventBuilder::created(10, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_expires_at(200)
            .with_tx_hash(TX)]);
        apply_batch(&conn, &block, &create).unwrap();

        let revert = build_batch(&[EventBuilder::updated(11, 0x01, 200, 300)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(TX)
            .with_payload(b"updated")
            .with_string_annotations(vec![])
            .with_numeric_annotations(vec![])
            .with_op(BatchOp::Revert)]);
        let result = apply_batch(&conn, &block, &revert).unwrap();
        assert_eq!(result, ApplyResult::NeedsReplay);
    }

    #[test]
    fn revert_deleted_triggers_needs_replay() {
        let (conn, block) = setup();
        use glint_primitives::exex_types::BatchOp;

        let revert = build_batch(&[EventBuilder::deleted(11, 0x01)
            .with_entity_key(KEY)
            .with_owner(OWNER)
            .with_tx_hash(TX)
            .with_op(BatchOp::Revert)]);
        let result = apply_batch(&conn, &block, &revert).unwrap();
        assert_eq!(result, ApplyResult::NeedsReplay);
    }

    #[test]
    fn revert_permissions_changed_triggers_needs_replay() {
        let (conn, block) = setup();
        use glint_primitives::exex_types::BatchOp;

        let revert = build_batch(&[EventBuilder::permissions_changed(
            11,
            0x01,
            Address::repeat_byte(0x42),
            1,
            Address::ZERO,
        )
        .with_entity_key(KEY)
        .with_tx_hash(TX)
        .with_op(BatchOp::Revert)]);
        let result = apply_batch(&conn, &block, &revert).unwrap();
        assert_eq!(result, ApplyResult::NeedsReplay);
    }
}

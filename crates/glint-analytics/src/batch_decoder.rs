mod columns;

use alloy_primitives::Bytes;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use eyre::WrapErr;
use glint_primitives::exex_schema::columns as col_names;
use glint_primitives::exex_types::{BatchOp, EntityEventType};
use tracing::warn;

use crate::entity_store::{EntityRow, EntityStore};
use columns::{
    addr_from_fsb, b256_from_fsb, col_binary, col_fsb, col_map, col_string, col_u8, col_u64,
    decode_numeric_map, decode_string_map,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResult {
    Applied,
    Watermark,
    NeedsReplay,
}

pub fn apply_batch(store: &mut EntityStore, batch: &RecordBatch) -> eyre::Result<ApplyResult> {
    if batch.num_rows() == 0 {
        return Ok(ApplyResult::Applied);
    }

    let op_col = col_u8(batch, col_names::OP)?;

    // Batches are homogeneous by op.
    let op_val = op_col.value(0);
    if op_val == 0xFF {
        return Ok(ApplyResult::Watermark);
    }

    let op = BatchOp::try_from(op_val).map_err(|v| eyre::eyre!("unknown BatchOp value: {v}"))?;

    let nrows = batch.num_rows();

    match op {
        BatchOp::Commit => {
            apply_commit(store, batch, nrows).wrap_err("applying commit batch")?;
            Ok(ApplyResult::Applied)
        }
        BatchOp::Revert => apply_revert(store, batch, nrows).wrap_err("applying revert batch"),
    }
}

fn apply_commit(store: &mut EntityStore, batch: &RecordBatch, nrows: usize) -> eyre::Result<()> {
    let block_number_col = col_u64(batch, col_names::BLOCK_NUMBER)?;
    let tx_hash_col = col_fsb(batch, col_names::TX_HASH)?;
    let event_type_col = col_u8(batch, col_names::EVENT_TYPE)?;
    let entity_key_col = col_fsb(batch, col_names::ENTITY_KEY)?;
    let owner_col = col_fsb(batch, col_names::OWNER)?;
    let expires_col = col_u64(batch, col_names::EXPIRES_AT_BLOCK)?;
    let content_type_col = col_string(batch, col_names::CONTENT_TYPE)?;
    let payload_col = col_binary(batch, col_names::PAYLOAD)?;
    let str_ann_col = col_map(batch, col_names::STRING_ANNOTATIONS)?;
    let num_ann_col = col_map(batch, col_names::NUMERIC_ANNOTATIONS)?;
    let extend_policy_col = col_u8(batch, col_names::EXTEND_POLICY)?;
    let operator_col = col_fsb(batch, col_names::OPERATOR)?;

    for i in 0..nrows {
        let event_type_raw = event_type_col.value(i);
        let event_type = EntityEventType::try_from(event_type_raw)
            .map_err(|v| eyre::eyre!("unknown EntityEventType value: {v}"))?;

        let entity_key = b256_from_fsb(entity_key_col, i);
        let tx_hash = b256_from_fsb(tx_hash_col, i);
        let block_number = block_number_col.value(i);

        match event_type {
            EntityEventType::Created | EntityEventType::Updated => {
                let owner = addr_from_fsb(owner_col, i);
                let expires_at_block = expires_col.value(i);
                let content_type = content_type_col.value(i).to_owned();
                let payload = Bytes::copy_from_slice(payload_col.value(i));
                let string_annotations = decode_string_map(str_ann_col, i)?;
                let numeric_annotations = decode_numeric_map(num_ann_col, i)?;

                let created_at_block = if event_type == EntityEventType::Created {
                    block_number
                } else {
                    store
                        .get(&entity_key)
                        .map_or(block_number, |r| r.created_at_block)
                };

                let extend_policy = extend_policy_col.value(i);
                let operator = if operator_col.is_null(i) {
                    None
                } else {
                    Some(addr_from_fsb(operator_col, i))
                };

                store.insert(EntityRow {
                    entity_key,
                    owner,
                    expires_at_block,
                    content_type,
                    payload,
                    string_annotations,
                    numeric_annotations,
                    created_at_block,
                    tx_hash,
                    extend_policy,
                    operator,
                });
            }
            EntityEventType::Deleted | EntityEventType::Expired => {
                store.remove(&entity_key);
            }
            EntityEventType::Extended => {
                let new_expires_at_block = expires_col.value(i);

                if let Some(row) = store.get_mut(&entity_key) {
                    row.expires_at_block = new_expires_at_block;
                    row.tx_hash = tx_hash;
                }
            }
            EntityEventType::PermissionsChanged => {
                let new_owner = addr_from_fsb(owner_col, i);
                let extend_policy = extend_policy_col.value(i);
                let operator = if operator_col.is_null(i) {
                    None
                } else {
                    Some(addr_from_fsb(operator_col, i))
                };

                if let Some(existing) = store.get(&entity_key).cloned() {
                    store.insert(EntityRow {
                        entity_key,
                        owner: new_owner,
                        extend_policy,
                        operator,
                        tx_hash,
                        expires_at_block: existing.expires_at_block,
                        content_type: existing.content_type,
                        payload: existing.payload,
                        string_annotations: existing.string_annotations,
                        numeric_annotations: existing.numeric_annotations,
                        created_at_block: existing.created_at_block,
                    });
                } else {
                    warn!(?entity_key, "PermissionsChanged for missing entity");
                }
            }
        }
    }

    Ok(())
}

fn apply_revert(
    store: &mut EntityStore,
    batch: &RecordBatch,
    nrows: usize,
) -> eyre::Result<ApplyResult> {
    let event_type_col = col_u8(batch, col_names::EVENT_TYPE)?;
    let entity_key_col = col_fsb(batch, col_names::ENTITY_KEY)?;
    let old_expires_col = col_u64(batch, col_names::OLD_EXPIRES_AT_BLOCK)?;

    for i in (0..nrows).rev() {
        let event_type_raw = event_type_col.value(i);
        let event_type = EntityEventType::try_from(event_type_raw)
            .map_err(|v| eyre::eyre!("unknown EntityEventType value: {v}"))?;

        let entity_key = b256_from_fsb(entity_key_col, i);

        match event_type {
            EntityEventType::Created => {
                store.remove(&entity_key);
            }
            EntityEventType::Extended => {
                let old_expires = old_expires_col.value(i);

                if let Some(row) = store.get_mut(&entity_key) {
                    row.expires_at_block = old_expires;
                    // tx_hash can't be fully restored — the revert payload doesn't carry
                    // the pre-extension tx_hash, so we leave it as-is.
                } else {
                    warn!(?entity_key, "reverting Extended event for missing entity");
                }
            }
            EntityEventType::Updated
            | EntityEventType::Deleted
            | EntityEventType::Expired
            | EntityEventType::PermissionsChanged => {
                return Ok(ApplyResult::NeedsReplay);
            }
        }
    }

    Ok(ApplyResult::Applied)
}

pub fn batch_block_number(batch: &RecordBatch) -> Option<u64> {
    if batch.num_rows() == 0 {
        return None;
    }
    col_u64(batch, col_names::BLOCK_NUMBER)
        .ok()
        .map(|col| col.value(0))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, Bytes};
    use glint_primitives::exex_types::BatchOp;
    use glint_primitives::test_utils::{EventBuilder, build_batch};

    use super::*;
    use crate::entity_store::EntityStore;

    fn default_key() -> B256 {
        B256::repeat_byte(0x01)
    }

    fn default_owner() -> Address {
        Address::repeat_byte(0x02)
    }

    fn default_tx() -> B256 {
        B256::repeat_byte(0xAA)
    }

    fn created_event(expires_at: u64) -> EventBuilder {
        EventBuilder::created(0, 0x01)
            .with_entity_key(default_key())
            .with_owner(default_owner())
            .with_expires_at(expires_at)
            .with_string_annotations(vec![("sk".into(), "sv".into())])
            .with_numeric_annotations(vec![("nk".into(), 99)])
            .with_tx_hash(default_tx())
    }

    #[test]
    fn decode_created_inserts_entity() {
        let mut store = EntityStore::new();
        let batch = build_batch(&[created_event(200).with_op(BatchOp::Commit)]);
        let result = apply_batch(&mut store, &batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert_eq!(store.len(), 1);

        let row = store.get(&default_key()).unwrap();
        assert_eq!(row.owner, default_owner());
        assert_eq!(row.expires_at_block, 200);
        assert_eq!(row.content_type, "text/plain");
        assert_eq!(row.payload, Bytes::from_static(b"hello"));
        assert_eq!(row.tx_hash, default_tx());
        assert_eq!(
            row.string_annotations,
            vec![("sk".to_owned(), "sv".to_owned())]
        );
        assert_eq!(row.numeric_annotations, vec![("nk".to_owned(), 99u64)]);
        assert_eq!(row.extend_policy, 0);
        assert_eq!(row.operator, Some(Address::ZERO));
    }

    #[test]
    fn decode_deleted_removes_entity() {
        let mut store = EntityStore::new();

        let create_batch = build_batch(&[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();
        assert_eq!(store.len(), 1);

        let delete_batch = build_batch(&[EventBuilder::deleted(11, 0x01)
            .with_entity_key(default_key())
            .with_owner(default_owner())
            .with_tx_hash(default_tx())]);
        let result = apply_batch(&mut store, &delete_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert!(store.is_empty());
    }

    #[test]
    fn decode_extended_updates_expiry() {
        let mut store = EntityStore::new();

        let create_batch = build_batch(&[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let new_tx = B256::repeat_byte(0xBB);
        let extend_batch = build_batch(&[EventBuilder::extended(11, 0x01, 200, 500)
            .with_entity_key(default_key())
            .with_owner(default_owner())
            .with_tx_hash(new_tx)]);
        let result = apply_batch(&mut store, &extend_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        let row = store.get(&default_key()).unwrap();
        assert_eq!(row.expires_at_block, 500);
        assert_eq!(row.tx_hash, new_tx);
    }

    #[test]
    fn revert_created_removes_entity() {
        let mut store = EntityStore::new();

        let create_batch = build_batch(&[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();
        assert_eq!(store.len(), 1);

        let revert_batch = build_batch(&[created_event(200).with_op(BatchOp::Revert)]);
        let result = apply_batch(&mut store, &revert_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert!(store.is_empty());
    }

    #[test]
    fn revert_extended_restores_old_expiry() {
        let mut store = EntityStore::new();

        let create_batch = build_batch(&[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let new_tx = B256::repeat_byte(0xBB);
        let extend_batch = build_batch(&[EventBuilder::extended(11, 0x01, 200, 500)
            .with_entity_key(default_key())
            .with_owner(default_owner())
            .with_tx_hash(new_tx)]);
        apply_batch(&mut store, &extend_batch).unwrap();
        assert_eq!(store.get(&default_key()).unwrap().expires_at_block, 500);

        let revert_extend_batch = build_batch(&[EventBuilder::extended(11, 0x01, 200, 500)
            .with_entity_key(default_key())
            .with_owner(default_owner())
            .with_tx_hash(new_tx)
            .with_op(BatchOp::Revert)]);
        let result = apply_batch(&mut store, &revert_extend_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert_eq!(store.get(&default_key()).unwrap().expires_at_block, 200);
    }

    #[test]
    fn revert_updated_triggers_needs_replay() {
        let mut store = EntityStore::new();

        let create_batch = build_batch(&[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let revert_batch = build_batch(&[EventBuilder::updated(11, 0x01, 200, 300)
            .with_entity_key(default_key())
            .with_owner(default_owner())
            .with_tx_hash(default_tx())
            .with_payload(b"updated")
            .with_string_annotations(vec![])
            .with_numeric_annotations(vec![])
            .with_op(BatchOp::Revert)]);
        let result = apply_batch(&mut store, &revert_batch).unwrap();

        assert_eq!(result, ApplyResult::NeedsReplay);
    }

    #[test]
    fn decode_permissions_changed_updates_owner() {
        let mut store = EntityStore::new();

        let create_batch = build_batch(&[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let new_owner = Address::repeat_byte(0x42);
        let pc_batch = build_batch(&[EventBuilder::permissions_changed(
            11,
            0x01,
            new_owner,
            1,
            Address::repeat_byte(0x99),
        )
        .with_entity_key(default_key())
        .with_tx_hash(default_tx())]);
        let result = apply_batch(&mut store, &pc_batch).unwrap();
        assert_eq!(result, ApplyResult::Applied);

        let row = store.get(&default_key()).unwrap();
        assert_eq!(row.owner, new_owner);
        assert_eq!(row.extend_policy, 1);
        assert_eq!(row.operator, Some(Address::repeat_byte(0x99)));
    }

    #[test]
    fn revert_permissions_changed_triggers_replay() {
        let mut store = EntityStore::new();

        let create_batch = build_batch(&[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let revert_batch = build_batch(&[EventBuilder::permissions_changed(
            11,
            0x01,
            Address::repeat_byte(0x42),
            1,
            Address::ZERO,
        )
        .with_entity_key(default_key())
        .with_tx_hash(default_tx())
        .with_op(BatchOp::Revert)]);
        let result = apply_batch(&mut store, &revert_batch).unwrap();
        assert_eq!(result, ApplyResult::NeedsReplay);
    }
}

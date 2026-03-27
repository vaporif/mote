mod columns;

use alloy_primitives::Bytes;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use eyre::WrapErr;
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

    let op_col = col_u8(batch, "op")?;

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
    let block_number_col = col_u64(batch, "block_number")?;
    let tx_hash_col = col_fsb(batch, "tx_hash")?;
    let event_type_col = col_u8(batch, "event_type")?;
    let entity_key_col = col_fsb(batch, "entity_key")?;
    let owner_col = col_fsb(batch, "owner")?;
    let expires_col = col_u64(batch, "expires_at_block")?;
    let content_type_col = col_string(batch, "content_type")?;
    let payload_col = col_binary(batch, "payload")?;
    let str_ann_col = col_map(batch, "string_annotations")?;
    let num_ann_col = col_map(batch, "numeric_annotations")?;
    let extend_policy_col = col_u8(batch, "extend_policy")?;
    let operator_col = col_fsb(batch, "operator")?;

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
        }
    }

    Ok(())
}

fn apply_revert(
    store: &mut EntityStore,
    batch: &RecordBatch,
    nrows: usize,
) -> eyre::Result<ApplyResult> {
    let event_type_col = col_u8(batch, "event_type")?;
    let entity_key_col = col_fsb(batch, "entity_key")?;
    let old_expires_col = col_u64(batch, "old_expires_at_block")?;

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
            EntityEventType::Updated | EntityEventType::Deleted | EntityEventType::Expired => {
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
    col_u64(batch, "block_number").ok().map(|col| col.value(0))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_primitives::{Address, B256, Bytes};
    use arrow::{
        array::{
            ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt8Builder,
            UInt32Builder, UInt64Builder,
            builder::{MapBuilder, MapFieldNames},
        },
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use glint_primitives::exex_types::{BatchOp, EntityEventType};

    use super::*;
    use crate::entity_store::EntityStore;

    enum TestEvent {
        Created {
            entity_key: B256,
            owner: Address,
            expires_at: u64,
            content_type: String,
            payload: Bytes,
            string_keys: Vec<String>,
            string_values: Vec<String>,
            numeric_keys: Vec<String>,
            numeric_values: Vec<u64>,
            extend_policy: u8,
            operator: Address,
        },
        Updated {
            entity_key: B256,
            owner: Address,
            old_expires_at: u64,
            new_expires_at: u64,
            content_type: String,
            payload: Bytes,
            string_keys: Vec<String>,
            string_values: Vec<String>,
            numeric_keys: Vec<String>,
            numeric_values: Vec<u64>,
            extend_policy: u8,
            operator: Address,
        },
        Deleted {
            entity_key: B256,
            owner: Address,
        },
        #[allow(dead_code)] // matched in patterns but no test constructs an Expired event yet
        Expired {
            entity_key: B256,
            owner: Address,
        },
        Extended {
            entity_key: B256,
            old_expires_at: u64,
            new_expires_at: u64,
            owner: Address,
        },
    }

    fn map_field_names() -> MapFieldNames {
        MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }
    }

    fn build_exex_schema() -> Schema {
        Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_hash", DataType::FixedSizeBinary(32), false),
            Field::new("tx_index", DataType::UInt32, false),
            Field::new("tx_hash", DataType::FixedSizeBinary(32), false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("event_type", DataType::UInt8, false),
            Field::new("entity_key", DataType::FixedSizeBinary(32), false),
            Field::new("owner", DataType::FixedSizeBinary(20), true),
            Field::new("expires_at_block", DataType::UInt64, true),
            Field::new("old_expires_at_block", DataType::UInt64, true),
            Field::new("content_type", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
            Field::new(
                "string_annotations",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "numeric_annotations",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::UInt64, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new("extend_policy", DataType::UInt8, true),
            Field::new("operator", DataType::FixedSizeBinary(20), true),
            Field::new("tip_block", DataType::UInt64, false),
            Field::new("op", DataType::UInt8, false),
        ])
    }

    fn build_test_batch(
        block_number: u64,
        tx_hash: B256,
        op: BatchOp,
        events: &[TestEvent],
    ) -> RecordBatch {
        let cap = events.len();
        let op_val = op as u8;

        let mut block_number_b = UInt64Builder::with_capacity(cap);
        let mut block_hash_b = FixedSizeBinaryBuilder::with_capacity(cap, 32);
        let mut tx_index_b = UInt32Builder::with_capacity(cap);
        let mut tx_hash_b = FixedSizeBinaryBuilder::with_capacity(cap, 32);
        let mut log_index_b = UInt32Builder::with_capacity(cap);
        let mut event_type_b = UInt8Builder::with_capacity(cap);
        let mut entity_key_b = FixedSizeBinaryBuilder::with_capacity(cap, 32);
        let mut owner_b = FixedSizeBinaryBuilder::with_capacity(cap, 20);
        let mut expires_b = UInt64Builder::with_capacity(cap);
        let mut old_expires_b = UInt64Builder::with_capacity(cap);
        let mut content_type_b = StringBuilder::with_capacity(cap, 64);
        let mut payload_b = BinaryBuilder::with_capacity(cap, 256);
        let mut str_ann_b = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        let mut num_ann_b = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            UInt64Builder::new(),
        );
        let mut extend_policy_b = UInt8Builder::with_capacity(cap);
        let mut operator_b = FixedSizeBinaryBuilder::with_capacity(cap, 20);
        let mut tip_block_b = UInt64Builder::with_capacity(cap);
        let mut op_b = UInt8Builder::with_capacity(cap);

        for ev in events {
            block_number_b.append_value(block_number);
            block_hash_b.append_value(B256::ZERO.as_slice()).unwrap();
            tx_index_b.append_value(0);
            tx_hash_b.append_value(tx_hash.as_slice()).unwrap();
            log_index_b.append_value(0);
            tip_block_b.append_value(block_number);
            op_b.append_value(op_val);

            match ev {
                TestEvent::Created {
                    entity_key,
                    owner,
                    expires_at,
                    content_type,
                    payload,
                    string_keys,
                    string_values,
                    numeric_keys,
                    numeric_values,
                    extend_policy,
                    operator,
                } => {
                    event_type_b.append_value(EntityEventType::Created as u8);
                    entity_key_b.append_value(entity_key.as_slice()).unwrap();
                    owner_b.append_value(owner.as_slice()).unwrap();
                    expires_b.append_value(*expires_at);
                    old_expires_b.append_null();
                    content_type_b.append_value(content_type);
                    payload_b.append_value(payload.as_ref());
                    for (k, v) in string_keys.iter().zip(string_values.iter()) {
                        str_ann_b.keys().append_value(k);
                        str_ann_b.values().append_value(v);
                    }
                    str_ann_b.append(true).unwrap();
                    for (k, v) in numeric_keys.iter().zip(numeric_values.iter()) {
                        num_ann_b.keys().append_value(k);
                        num_ann_b.values().append_value(*v);
                    }
                    num_ann_b.append(true).unwrap();
                    extend_policy_b.append_value(*extend_policy);
                    operator_b.append_value(operator.as_slice()).unwrap();
                }
                TestEvent::Updated {
                    entity_key,
                    owner,
                    old_expires_at,
                    new_expires_at,
                    content_type,
                    payload,
                    string_keys,
                    string_values,
                    numeric_keys,
                    numeric_values,
                    extend_policy,
                    operator,
                } => {
                    event_type_b.append_value(EntityEventType::Updated as u8);
                    entity_key_b.append_value(entity_key.as_slice()).unwrap();
                    owner_b.append_value(owner.as_slice()).unwrap();
                    expires_b.append_value(*new_expires_at);
                    old_expires_b.append_value(*old_expires_at);
                    content_type_b.append_value(content_type);
                    payload_b.append_value(payload.as_ref());
                    for (k, v) in string_keys.iter().zip(string_values.iter()) {
                        str_ann_b.keys().append_value(k);
                        str_ann_b.values().append_value(v);
                    }
                    str_ann_b.append(true).unwrap();
                    for (k, v) in numeric_keys.iter().zip(numeric_values.iter()) {
                        num_ann_b.keys().append_value(k);
                        num_ann_b.values().append_value(*v);
                    }
                    num_ann_b.append(true).unwrap();
                    extend_policy_b.append_value(*extend_policy);
                    operator_b.append_value(operator.as_slice()).unwrap();
                }
                TestEvent::Deleted { entity_key, owner }
                | TestEvent::Expired { entity_key, owner } => {
                    let et = match ev {
                        TestEvent::Deleted { .. } => EntityEventType::Deleted,
                        TestEvent::Expired { .. } => EntityEventType::Expired,
                        _ => unreachable!(),
                    };
                    event_type_b.append_value(et as u8);
                    entity_key_b.append_value(entity_key.as_slice()).unwrap();
                    owner_b.append_value(owner.as_slice()).unwrap();
                    expires_b.append_null();
                    old_expires_b.append_null();
                    content_type_b.append_null();
                    payload_b.append_null();
                    str_ann_b.append(false).unwrap();
                    num_ann_b.append(false).unwrap();
                    extend_policy_b.append_null();
                    operator_b.append_null();
                }
                TestEvent::Extended {
                    entity_key,
                    old_expires_at,
                    new_expires_at,
                    owner,
                } => {
                    event_type_b.append_value(EntityEventType::Extended as u8);
                    entity_key_b.append_value(entity_key.as_slice()).unwrap();
                    owner_b.append_value(owner.as_slice()).unwrap();
                    expires_b.append_value(*new_expires_at);
                    old_expires_b.append_value(*old_expires_at);
                    content_type_b.append_null();
                    payload_b.append_null();
                    str_ann_b.append(false).unwrap();
                    num_ann_b.append(false).unwrap();
                    extend_policy_b.append_null();
                    operator_b.append_null();
                }
            }
        }

        let schema = Arc::new(build_exex_schema());
        let columns: Vec<ArrayRef> = vec![
            Arc::new(block_number_b.finish()),
            Arc::new(block_hash_b.finish()),
            Arc::new(tx_index_b.finish()),
            Arc::new(tx_hash_b.finish()),
            Arc::new(log_index_b.finish()),
            Arc::new(event_type_b.finish()),
            Arc::new(entity_key_b.finish()),
            Arc::new(owner_b.finish()),
            Arc::new(expires_b.finish()),
            Arc::new(old_expires_b.finish()),
            Arc::new(content_type_b.finish()),
            Arc::new(payload_b.finish()),
            Arc::new(str_ann_b.finish()),
            Arc::new(num_ann_b.finish()),
            Arc::new(extend_policy_b.finish()),
            Arc::new(operator_b.finish()),
            Arc::new(tip_block_b.finish()),
            Arc::new(op_b.finish()),
        ];
        RecordBatch::try_new(schema, columns).expect("columns must match schema")
    }

    fn default_key() -> B256 {
        B256::repeat_byte(0x01)
    }

    fn default_owner() -> Address {
        Address::repeat_byte(0x02)
    }

    fn default_tx() -> B256 {
        B256::repeat_byte(0xAA)
    }

    fn created_event(expires_at: u64) -> TestEvent {
        TestEvent::Created {
            entity_key: default_key(),
            owner: default_owner(),
            expires_at,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_keys: vec!["sk".into()],
            string_values: vec!["sv".into()],
            numeric_keys: vec!["nk".into()],
            numeric_values: vec![99],
            extend_policy: 0,
            operator: Address::ZERO,
        }
    }

    #[test]
    fn decode_created_inserts_entity() {
        let mut store = EntityStore::new();
        let batch = build_test_batch(10, default_tx(), BatchOp::Commit, &[created_event(200)]);
        let result = apply_batch(&mut store, &batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert_eq!(store.len(), 1);

        let row = store.get(&default_key()).unwrap();
        assert_eq!(row.owner, default_owner());
        assert_eq!(row.expires_at_block, 200);
        assert_eq!(row.content_type, "text/plain");
        assert_eq!(row.payload, Bytes::from_static(b"hello"));
        assert_eq!(row.created_at_block, 10);
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

        let create_batch =
            build_test_batch(10, default_tx(), BatchOp::Commit, &[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();
        assert_eq!(store.len(), 1);

        let delete_batch = build_test_batch(
            11,
            default_tx(),
            BatchOp::Commit,
            &[TestEvent::Deleted {
                entity_key: default_key(),
                owner: default_owner(),
            }],
        );
        let result = apply_batch(&mut store, &delete_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert!(store.is_empty());
    }

    #[test]
    fn decode_extended_updates_expiry() {
        let mut store = EntityStore::new();

        let create_batch =
            build_test_batch(10, default_tx(), BatchOp::Commit, &[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let new_tx = B256::repeat_byte(0xBB);
        let extend_batch = build_test_batch(
            11,
            new_tx,
            BatchOp::Commit,
            &[TestEvent::Extended {
                entity_key: default_key(),
                old_expires_at: 200,
                new_expires_at: 500,
                owner: default_owner(),
            }],
        );
        let result = apply_batch(&mut store, &extend_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        let row = store.get(&default_key()).unwrap();
        assert_eq!(row.expires_at_block, 500);
        assert_eq!(row.tx_hash, new_tx);
    }

    #[test]
    fn revert_created_removes_entity() {
        let mut store = EntityStore::new();

        let create_batch =
            build_test_batch(10, default_tx(), BatchOp::Commit, &[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();
        assert_eq!(store.len(), 1);

        let revert_batch =
            build_test_batch(10, default_tx(), BatchOp::Revert, &[created_event(200)]);
        let result = apply_batch(&mut store, &revert_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert!(store.is_empty());
    }

    #[test]
    fn revert_extended_restores_old_expiry() {
        let mut store = EntityStore::new();

        let create_batch =
            build_test_batch(10, default_tx(), BatchOp::Commit, &[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let new_tx = B256::repeat_byte(0xBB);
        let extend_batch = build_test_batch(
            11,
            new_tx,
            BatchOp::Commit,
            &[TestEvent::Extended {
                entity_key: default_key(),
                old_expires_at: 200,
                new_expires_at: 500,
                owner: default_owner(),
            }],
        );
        apply_batch(&mut store, &extend_batch).unwrap();
        assert_eq!(store.get(&default_key()).unwrap().expires_at_block, 500);

        let revert_extend_batch = build_test_batch(
            11,
            new_tx,
            BatchOp::Revert,
            &[TestEvent::Extended {
                entity_key: default_key(),
                old_expires_at: 200,
                new_expires_at: 500,
                owner: default_owner(),
            }],
        );
        let result = apply_batch(&mut store, &revert_extend_batch).unwrap();

        assert_eq!(result, ApplyResult::Applied);
        assert_eq!(store.get(&default_key()).unwrap().expires_at_block, 200);
    }

    #[test]
    fn revert_updated_triggers_needs_replay() {
        let mut store = EntityStore::new();

        let create_batch =
            build_test_batch(10, default_tx(), BatchOp::Commit, &[created_event(200)]);
        apply_batch(&mut store, &create_batch).unwrap();

        let revert_batch = build_test_batch(
            11,
            default_tx(),
            BatchOp::Revert,
            &[TestEvent::Updated {
                entity_key: default_key(),
                owner: default_owner(),
                old_expires_at: 200,
                new_expires_at: 300,
                content_type: "text/plain".into(),
                payload: Bytes::from_static(b"updated"),
                string_keys: vec![],
                string_values: vec![],
                numeric_keys: vec![],
                numeric_values: vec![],
                extend_policy: 0,
                operator: Address::ZERO,
            }],
        );
        let result = apply_batch(&mut store, &revert_batch).unwrap();

        assert_eq!(result, ApplyResult::NeedsReplay);
    }
}

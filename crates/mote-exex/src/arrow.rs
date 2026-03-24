use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, MapBuilder, MapFieldNames, StringBuilder,
    UInt8Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use alloy_primitives::B256;
use mote_primitives::exex_types::{BatchOp, EntityEventType};

use crate::parse::EntityEvent;

/// Row wrapper pairing an [`EntityEvent`] with its transaction-level metadata.
#[derive(Debug, Clone)]
pub struct EventRow {
    pub event: EntityEvent,
    pub tx_index: u32,
    pub tx_hash: B256,
    pub log_index: u32,
}

/// Returns the canonical 16-column Arrow schema for entity event batches.
#[must_use]
pub fn entity_events_schema() -> Schema {
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
        Field::new("tip_block", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
    ])
}

/// Build a [`RecordBatch`] from a slice of [`EventRow`]s with block-level metadata.
pub fn build_record_batch(
    block_number: u64,
    block_hash: B256,
    tip_block: u64,
    op: BatchOp,
    events: &[EventRow],
) -> eyre::Result<RecordBatch> {
    let len = events.len();
    let schema = Arc::new(entity_events_schema());

    let mut block_number_builder = UInt64Builder::with_capacity(len);
    let mut block_hash_builder = FixedSizeBinaryBuilder::with_capacity(len, 32);
    let mut tx_index_builder = UInt32Builder::with_capacity(len);
    let mut tx_hash_builder = FixedSizeBinaryBuilder::with_capacity(len, 32);
    let mut log_index_builder = UInt32Builder::with_capacity(len);
    let mut event_type_builder = UInt8Builder::with_capacity(len);
    let mut entity_key_builder = FixedSizeBinaryBuilder::with_capacity(len, 32);
    let mut owner_builder = FixedSizeBinaryBuilder::with_capacity(len, 20);
    let mut expires_at_builder = UInt64Builder::with_capacity(len);
    let mut old_expires_at_builder = UInt64Builder::with_capacity(len);
    let mut content_type_builder = StringBuilder::with_capacity(len, 64);
    let mut payload_builder = BinaryBuilder::with_capacity(len, 256);
    let mut string_ann_builder = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut numeric_ann_builder = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        UInt64Builder::new(),
    );
    let mut tip_block_builder = UInt64Builder::with_capacity(len);
    let mut op_builder = UInt8Builder::with_capacity(len);

    let op_val = op as u8;

    for row in events {
        block_number_builder.append_value(block_number);
        block_hash_builder.append_value(block_hash.as_slice())?;
        tx_index_builder.append_value(row.tx_index);
        tx_hash_builder.append_value(row.tx_hash.as_slice())?;
        log_index_builder.append_value(row.log_index);
        tip_block_builder.append_value(tip_block);
        op_builder.append_value(op_val);

        match &row.event {
            EntityEvent::Created {
                entity_key,
                owner,
                expires_at,
                content_type,
                payload,
                string_keys,
                string_values,
                numeric_keys,
                numeric_values,
            } => {
                event_type_builder.append_value(EntityEventType::Created as u8);
                entity_key_builder.append_value(entity_key.as_slice())?;
                owner_builder.append_value(owner.as_slice())?;
                expires_at_builder.append_value(*expires_at);
                old_expires_at_builder.append_null();
                content_type_builder.append_value(content_type);
                payload_builder.append_value(payload.as_ref());

                append_string_annotations(&mut string_ann_builder, string_keys, string_values);
                append_numeric_annotations(&mut numeric_ann_builder, numeric_keys, numeric_values);
            }
            EntityEvent::Updated {
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
            } => {
                event_type_builder.append_value(EntityEventType::Updated as u8);
                entity_key_builder.append_value(entity_key.as_slice())?;
                owner_builder.append_value(owner.as_slice())?;
                expires_at_builder.append_value(*new_expires_at);
                old_expires_at_builder.append_value(*old_expires_at);
                content_type_builder.append_value(content_type);
                payload_builder.append_value(payload.as_ref());

                append_string_annotations(&mut string_ann_builder, string_keys, string_values);
                append_numeric_annotations(&mut numeric_ann_builder, numeric_keys, numeric_values);
            }
            EntityEvent::Deleted {
                entity_key, owner, ..
            } => {
                event_type_builder.append_value(EntityEventType::Deleted as u8);
                entity_key_builder.append_value(entity_key.as_slice())?;
                owner_builder.append_value(owner.as_slice())?;
                expires_at_builder.append_null();
                old_expires_at_builder.append_null();
                content_type_builder.append_null();
                payload_builder.append_null();
                string_ann_builder.append(false)?;
                numeric_ann_builder.append(false)?;
            }
            EntityEvent::Expired {
                entity_key, owner, ..
            } => {
                event_type_builder.append_value(EntityEventType::Expired as u8);
                entity_key_builder.append_value(entity_key.as_slice())?;
                owner_builder.append_value(owner.as_slice())?;
                expires_at_builder.append_null();
                old_expires_at_builder.append_null();
                content_type_builder.append_null();
                payload_builder.append_null();
                string_ann_builder.append(false)?;
                numeric_ann_builder.append(false)?;
            }
            EntityEvent::Extended {
                entity_key,
                old_expires_at,
                new_expires_at,
            } => {
                event_type_builder.append_value(EntityEventType::Extended as u8);
                entity_key_builder.append_value(entity_key.as_slice())?;
                owner_builder.append_null();
                expires_at_builder.append_value(*new_expires_at);
                old_expires_at_builder.append_value(*old_expires_at);
                content_type_builder.append_null();
                payload_builder.append_null();
                string_ann_builder.append(false)?;
                numeric_ann_builder.append(false)?;
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(block_number_builder.finish()),
        Arc::new(block_hash_builder.finish()),
        Arc::new(tx_index_builder.finish()),
        Arc::new(tx_hash_builder.finish()),
        Arc::new(log_index_builder.finish()),
        Arc::new(event_type_builder.finish()),
        Arc::new(entity_key_builder.finish()),
        Arc::new(owner_builder.finish()),
        Arc::new(expires_at_builder.finish()),
        Arc::new(old_expires_at_builder.finish()),
        Arc::new(content_type_builder.finish()),
        Arc::new(payload_builder.finish()),
        Arc::new(string_ann_builder.finish()),
        Arc::new(numeric_ann_builder.finish()),
        Arc::new(tip_block_builder.finish()),
        Arc::new(op_builder.finish()),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Build a single-row watermark batch with `op = 0xFF` and all other fields zeroed/null.
pub fn build_watermark_batch(tip_block: u64) -> eyre::Result<RecordBatch> {
    let schema = Arc::new(entity_events_schema());

    let mut block_number_builder = UInt64Builder::with_capacity(1);
    let mut block_hash_builder = FixedSizeBinaryBuilder::with_capacity(1, 32);
    let mut tx_index_builder = UInt32Builder::with_capacity(1);
    let mut tx_hash_builder = FixedSizeBinaryBuilder::with_capacity(1, 32);
    let mut log_index_builder = UInt32Builder::with_capacity(1);
    let mut event_type_builder = UInt8Builder::with_capacity(1);
    let mut entity_key_builder = FixedSizeBinaryBuilder::with_capacity(1, 32);
    let mut owner_builder = FixedSizeBinaryBuilder::with_capacity(1, 20);
    let mut expires_at_builder = UInt64Builder::with_capacity(1);
    let mut old_expires_at_builder = UInt64Builder::with_capacity(1);
    let mut content_type_builder = StringBuilder::with_capacity(1, 0);
    let mut payload_builder = BinaryBuilder::with_capacity(1, 0);
    let mut string_ann_builder = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut numeric_ann_builder = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        UInt64Builder::new(),
    );
    let mut tip_block_builder = UInt64Builder::with_capacity(1);
    let mut op_builder = UInt8Builder::with_capacity(1);

    block_number_builder.append_value(0);
    block_hash_builder.append_value([0u8; 32])?;
    tx_index_builder.append_value(0);
    tx_hash_builder.append_value([0u8; 32])?;
    log_index_builder.append_value(0);
    event_type_builder.append_value(0);
    entity_key_builder.append_value([0u8; 32])?;
    owner_builder.append_null();
    expires_at_builder.append_null();
    old_expires_at_builder.append_null();
    content_type_builder.append_null();
    payload_builder.append_null();
    string_ann_builder.append(false)?;
    numeric_ann_builder.append(false)?;
    tip_block_builder.append_value(tip_block);
    op_builder.append_value(0xFF);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(block_number_builder.finish()),
        Arc::new(block_hash_builder.finish()),
        Arc::new(tx_index_builder.finish()),
        Arc::new(tx_hash_builder.finish()),
        Arc::new(log_index_builder.finish()),
        Arc::new(event_type_builder.finish()),
        Arc::new(entity_key_builder.finish()),
        Arc::new(owner_builder.finish()),
        Arc::new(expires_at_builder.finish()),
        Arc::new(old_expires_at_builder.finish()),
        Arc::new(content_type_builder.finish()),
        Arc::new(payload_builder.finish()),
        Arc::new(string_ann_builder.finish()),
        Arc::new(numeric_ann_builder.finish()),
        Arc::new(tip_block_builder.finish()),
        Arc::new(op_builder.finish()),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".into(),
        key: "key".into(),
        value: "value".into(),
    }
}

fn append_string_annotations(
    builder: &mut MapBuilder<StringBuilder, StringBuilder>,
    keys: &[String],
    values: &[String],
) {
    for (k, v) in keys.iter().zip(values.iter()) {
        builder.keys().append_value(k);
        builder.values().append_value(v);
    }
    // append(true) marks the map entry as non-null
    #[allow(clippy::expect_used)]
    builder
        .append(true)
        .expect("string map append should not fail");
}

fn append_numeric_annotations(
    builder: &mut MapBuilder<StringBuilder, UInt64Builder>,
    keys: &[String],
    values: &[u64],
) {
    for (k, v) in keys.iter().zip(values.iter()) {
        builder.keys().append_value(k);
        builder.values().append_value(*v);
    }
    #[allow(clippy::expect_used)]
    builder
        .append(true)
        .expect("numeric map append should not fail");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::EntityEvent;
    use alloy_primitives::{Address, B256, Bytes};
    use mote_primitives::exex_types::BatchOp;

    fn sample_created() -> EntityEvent {
        EntityEvent::Created {
            entity_key: B256::repeat_byte(0x01),
            owner: Address::repeat_byte(0x02),
            expires_at: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_keys: vec!["k1".into()],
            string_values: vec!["v1".into()],
            numeric_keys: vec!["n1".into()],
            numeric_values: vec![42],
        }
    }

    fn sample_deleted() -> EntityEvent {
        EntityEvent::Deleted {
            entity_key: B256::repeat_byte(0x03),
            owner: Address::repeat_byte(0x04),
        }
    }

    fn sample_extended() -> EntityEvent {
        EntityEvent::Extended {
            entity_key: B256::repeat_byte(0x05),
            old_expires_at: 10,
            new_expires_at: 20,
        }
    }

    #[test]
    fn schema_has_expected_columns() {
        let schema = entity_events_schema();
        assert_eq!(schema.fields().len(), 16);
        assert!(schema.field_with_name("block_number").is_ok());
        assert!(schema.field_with_name("block_hash").is_ok());
        assert!(schema.field_with_name("tx_index").is_ok());
        assert!(schema.field_with_name("tx_hash").is_ok());
        assert!(schema.field_with_name("log_index").is_ok());
        assert!(schema.field_with_name("event_type").is_ok());
        assert!(schema.field_with_name("entity_key").is_ok());
        assert!(schema.field_with_name("owner").is_ok());
        assert!(schema.field_with_name("expires_at_block").is_ok());
        assert!(schema.field_with_name("old_expires_at_block").is_ok());
        assert!(schema.field_with_name("content_type").is_ok());
        assert!(schema.field_with_name("payload").is_ok());
        assert!(schema.field_with_name("string_annotations").is_ok());
        assert!(schema.field_with_name("numeric_annotations").is_ok());
        assert!(schema.field_with_name("tip_block").is_ok());
        assert!(schema.field_with_name("op").is_ok());
    }

    #[test]
    fn build_batch_single_created_event() {
        let events = vec![EventRow {
            event: sample_created(),
            tx_index: 0,
            tx_hash: B256::repeat_byte(0xAA),
            log_index: 0,
        }];
        let batch = build_record_batch(
            1000,
            B256::repeat_byte(0xBB),
            1000,
            BatchOp::Commit,
            &events,
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 16);
    }

    #[test]
    fn build_batch_mixed_events() {
        let events = vec![
            EventRow {
                event: sample_created(),
                tx_index: 0,
                tx_hash: B256::repeat_byte(0xAA),
                log_index: 0,
            },
            EventRow {
                event: sample_deleted(),
                tx_index: 1,
                tx_hash: B256::repeat_byte(0xBB),
                log_index: 1,
            },
            EventRow {
                event: sample_extended(),
                tx_index: 2,
                tx_hash: B256::repeat_byte(0xCC),
                log_index: 2,
            },
        ];
        let batch = build_record_batch(
            2000,
            B256::repeat_byte(0xDD),
            2000,
            BatchOp::Commit,
            &events,
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn extended_event_has_null_owner() {
        let events = vec![EventRow {
            event: sample_extended(),
            tx_index: 0,
            tx_hash: B256::ZERO,
            log_index: 0,
        }];
        let batch = build_record_batch(1000, B256::ZERO, 1000, BatchOp::Commit, &events).unwrap();
        let owner_col = batch.column_by_name("owner").unwrap();
        assert!(owner_col.is_null(0));
    }

    #[test]
    fn deleted_event_has_null_expiration_and_payload() {
        let events = vec![EventRow {
            event: sample_deleted(),
            tx_index: 0,
            tx_hash: B256::ZERO,
            log_index: 0,
        }];
        let batch = build_record_batch(1000, B256::ZERO, 1000, BatchOp::Commit, &events).unwrap();
        assert!(batch.column_by_name("expires_at_block").unwrap().is_null(0));
        assert!(batch.column_by_name("payload").unwrap().is_null(0));
        assert!(batch.column_by_name("content_type").unwrap().is_null(0));
    }

    #[test]
    fn watermark_batch_has_sentinel_op() {
        let batch = build_watermark_batch(5000).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let op_col = batch
            .column_by_name("op")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        assert_eq!(op_col.value(0), 0xFF);
    }
}

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, MapBuilder, StringBuilder, UInt8Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::record_batch::RecordBatch;

use alloy_primitives::B256;
use glint_primitives::exex_schema::{entity_events_schema, map_field_names};
use glint_primitives::exex_types::{BatchOp, EntityEventType};
use glint_primitives::parse::EntityEvent;

#[derive(Debug, Clone)]
pub struct EventRow {
    pub event: EntityEvent,
    pub tx_index: u32,
    pub tx_hash: B256,
    pub log_index: u32,
}

struct BatchBuilders {
    block_number: UInt64Builder,
    block_hash: FixedSizeBinaryBuilder,
    tx_index: UInt32Builder,
    tx_hash: FixedSizeBinaryBuilder,
    log_index: UInt32Builder,
    event_type: UInt8Builder,
    entity_key: FixedSizeBinaryBuilder,
    owner: FixedSizeBinaryBuilder,
    expires_at: UInt64Builder,
    old_expires_at: UInt64Builder,
    content_type: StringBuilder,
    payload: BinaryBuilder,
    string_ann: MapBuilder<StringBuilder, StringBuilder>,
    numeric_ann: MapBuilder<StringBuilder, UInt64Builder>,
    extend_policy: UInt8Builder,
    operator: FixedSizeBinaryBuilder,
    tip_block: UInt64Builder,
    op: UInt8Builder,
}

impl BatchBuilders {
    fn with_capacity(cap: usize) -> Self {
        Self {
            block_number: UInt64Builder::with_capacity(cap),
            block_hash: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            tx_index: UInt32Builder::with_capacity(cap),
            tx_hash: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            log_index: UInt32Builder::with_capacity(cap),
            event_type: UInt8Builder::with_capacity(cap),
            entity_key: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            owner: FixedSizeBinaryBuilder::with_capacity(cap, 20),
            expires_at: UInt64Builder::with_capacity(cap),
            old_expires_at: UInt64Builder::with_capacity(cap),
            content_type: StringBuilder::with_capacity(cap, 64),
            payload: BinaryBuilder::with_capacity(cap, 256),
            string_ann: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            numeric_ann: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                UInt64Builder::new(),
            ),
            extend_policy: UInt8Builder::with_capacity(cap),
            operator: FixedSizeBinaryBuilder::with_capacity(cap, 20),
            tip_block: UInt64Builder::with_capacity(cap),
            op: UInt8Builder::with_capacity(cap),
        }
    }

    fn finish(mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_number.finish()),
            Arc::new(self.block_hash.finish()),
            Arc::new(self.tx_index.finish()),
            Arc::new(self.tx_hash.finish()),
            Arc::new(self.log_index.finish()),
            Arc::new(self.event_type.finish()),
            Arc::new(self.entity_key.finish()),
            Arc::new(self.owner.finish()),
            Arc::new(self.expires_at.finish()),
            Arc::new(self.old_expires_at.finish()),
            Arc::new(self.content_type.finish()),
            Arc::new(self.payload.finish()),
            Arc::new(self.string_ann.finish()),
            Arc::new(self.numeric_ann.finish()),
            Arc::new(self.extend_policy.finish()),
            Arc::new(self.operator.finish()),
            Arc::new(self.tip_block.finish()),
            Arc::new(self.op.finish()),
        ];
        RecordBatch::try_new(entity_events_schema(), columns).expect("columns must match SCHEMA")
    }
}

pub fn build_record_batch(
    block_number: u64,
    block_hash: B256,
    tip_block: u64,
    op: BatchOp,
    events: &[EventRow],
) -> eyre::Result<RecordBatch> {
    let mut b = BatchBuilders::with_capacity(events.len());
    let op_val = op as u8;

    for row in events {
        b.block_number.append_value(block_number);
        b.block_hash.append_value(block_hash.as_slice())?;
        b.tx_index.append_value(row.tx_index);
        b.tx_hash.append_value(row.tx_hash.as_slice())?;
        b.log_index.append_value(row.log_index);
        b.tip_block.append_value(tip_block);
        b.op.append_value(op_val);

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
                extend_policy,
                operator,
            } => {
                b.event_type.append_value(EntityEventType::Created as u8);
                b.entity_key.append_value(entity_key.as_slice())?;
                b.owner.append_value(owner.as_slice())?;
                b.expires_at.append_value(*expires_at);
                b.old_expires_at.append_null();
                b.content_type.append_value(content_type);
                b.payload.append_value(payload.as_ref());

                append_string_annotations(&mut b.string_ann, string_keys, string_values)?;
                append_numeric_annotations(&mut b.numeric_ann, numeric_keys, numeric_values)?;

                b.extend_policy.append_value(*extend_policy);
                b.operator.append_value(operator.as_slice())?;
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
                extend_policy,
                operator,
            } => {
                b.event_type.append_value(EntityEventType::Updated as u8);
                b.entity_key.append_value(entity_key.as_slice())?;
                b.owner.append_value(owner.as_slice())?;
                b.expires_at.append_value(*new_expires_at);
                b.old_expires_at.append_value(*old_expires_at);
                b.content_type.append_value(content_type);
                b.payload.append_value(payload.as_ref());

                append_string_annotations(&mut b.string_ann, string_keys, string_values)?;
                append_numeric_annotations(&mut b.numeric_ann, numeric_keys, numeric_values)?;

                b.extend_policy.append_value(*extend_policy);
                b.operator.append_value(operator.as_slice())?;
            }
            EntityEvent::Deleted {
                entity_key, owner, ..
            } => {
                b.event_type.append_value(EntityEventType::Deleted as u8);
                b.entity_key.append_value(entity_key.as_slice())?;
                b.owner.append_value(owner.as_slice())?;
                append_null_fields(&mut b)?;
            }
            EntityEvent::Expired {
                entity_key, owner, ..
            } => {
                b.event_type.append_value(EntityEventType::Expired as u8);
                b.entity_key.append_value(entity_key.as_slice())?;
                b.owner.append_value(owner.as_slice())?;
                append_null_fields(&mut b)?;
            }
            EntityEvent::Extended {
                entity_key,
                old_expires_at,
                new_expires_at,
                owner,
            } => {
                b.event_type.append_value(EntityEventType::Extended as u8);
                b.entity_key.append_value(entity_key.as_slice())?;
                b.owner.append_value(owner.as_slice())?;
                b.expires_at.append_value(*new_expires_at);
                b.old_expires_at.append_value(*old_expires_at);
                b.content_type.append_null();
                b.payload.append_null();
                b.string_ann.append(false)?;
                b.numeric_ann.append(false)?;
                b.extend_policy.append_null();
                b.operator.append_null();
            }
            EntityEvent::PermissionsChanged {
                entity_key,
                old_owner: _,
                new_owner,
                extend_policy,
                operator,
            } => {
                b.event_type
                    .append_value(EntityEventType::PermissionsChanged as u8);
                b.entity_key.append_value(entity_key.as_slice())?;
                b.owner.append_value(new_owner.as_slice())?;
                b.expires_at.append_null();
                b.old_expires_at.append_null();
                b.content_type.append_null();
                b.payload.append_null();
                b.string_ann.append(false)?;
                b.numeric_ann.append(false)?;
                b.extend_policy.append_value(*extend_policy);
                b.operator.append_value(operator.as_slice())?;
            }
        }
    }

    Ok(b.finish())
}

pub fn build_watermark_batch(tip_block: u64) -> eyre::Result<RecordBatch> {
    let mut b = BatchBuilders::with_capacity(1);

    b.block_number.append_value(0);
    b.block_hash.append_value([0u8; 32])?;
    b.tx_index.append_value(0);
    b.tx_hash.append_value([0u8; 32])?;
    b.log_index.append_value(0);
    b.event_type.append_value(0);
    b.entity_key.append_value([0u8; 32])?;
    b.owner.append_null();
    b.expires_at.append_null();
    b.old_expires_at.append_null();
    b.content_type.append_null();
    b.payload.append_null();
    b.string_ann.append(false)?;
    b.numeric_ann.append(false)?;
    b.extend_policy.append_null();
    b.operator.append_null();
    b.tip_block.append_value(tip_block);
    b.op.append_value(0xFF);

    Ok(b.finish())
}

fn append_null_fields(b: &mut BatchBuilders) -> arrow::error::Result<()> {
    b.expires_at.append_null();
    b.old_expires_at.append_null();
    b.content_type.append_null();
    b.payload.append_null();
    b.string_ann.append(false)?;
    b.numeric_ann.append(false)?;
    b.extend_policy.append_null();
    b.operator.append_null();
    Ok(())
}

fn append_string_annotations(
    builder: &mut MapBuilder<StringBuilder, StringBuilder>,
    keys: &[String],
    values: &[String],
) -> arrow::error::Result<()> {
    for (k, v) in keys.iter().zip(values.iter()) {
        builder.keys().append_value(k);
        builder.values().append_value(v);
    }
    builder.append(true)
}

fn append_numeric_annotations(
    builder: &mut MapBuilder<StringBuilder, UInt64Builder>,
    keys: &[String],
    values: &[u64],
) -> arrow::error::Result<()> {
    for (k, v) in keys.iter().zip(values.iter()) {
        builder.keys().append_value(k);
        builder.values().append_value(*v);
    }
    builder.append(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes};
    use glint_primitives::exex_schema::columns;
    use glint_primitives::exex_types::BatchOp;

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
            extend_policy: 0,
            operator: Address::ZERO,
        }
    }

    fn sample_deleted() -> EntityEvent {
        EntityEvent::Deleted {
            entity_key: B256::repeat_byte(0x03),
            owner: Address::repeat_byte(0x04),
            sender: Address::repeat_byte(0x04),
        }
    }

    fn sample_extended() -> EntityEvent {
        EntityEvent::Extended {
            entity_key: B256::repeat_byte(0x05),
            old_expires_at: 10,
            new_expires_at: 20,
            owner: Address::ZERO,
        }
    }

    #[test]
    fn schema_column_count() {
        let schema = entity_events_schema();
        assert_eq!(schema.fields().len(), 18);
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
        assert!(schema.field_with_name("extend_policy").is_ok());
        assert!(schema.field_with_name("operator").is_ok());
        assert!(schema.field_with_name("tip_block").is_ok());
        assert!(schema.field_with_name("op").is_ok());
    }

    #[test]
    fn single_created_roundtrip() {
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
        assert_eq!(batch.num_columns(), 18);
    }

    #[test]
    fn mixed_event_types() {
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
    fn extended_populates_owner() {
        let events = vec![EventRow {
            event: sample_extended(),
            tx_index: 0,
            tx_hash: B256::ZERO,
            log_index: 0,
        }];
        let batch = build_record_batch(1000, B256::ZERO, 1000, BatchOp::Commit, &events).unwrap();
        let owner_col = batch.column_by_name(columns::OWNER).unwrap();
        assert!(!owner_col.is_null(0));
        assert!(
            batch
                .column_by_name(columns::EXTEND_POLICY)
                .unwrap()
                .is_null(0)
        );
        assert!(batch.column_by_name(columns::OPERATOR).unwrap().is_null(0));
    }

    #[test]
    fn deleted_nulls_optional_fields() {
        let events = vec![EventRow {
            event: sample_deleted(),
            tx_index: 0,
            tx_hash: B256::ZERO,
            log_index: 0,
        }];
        let batch = build_record_batch(1000, B256::ZERO, 1000, BatchOp::Commit, &events).unwrap();
        assert!(
            batch
                .column_by_name(columns::EXPIRES_AT_BLOCK)
                .unwrap()
                .is_null(0)
        );
        assert!(batch.column_by_name(columns::PAYLOAD).unwrap().is_null(0));
        assert!(
            batch
                .column_by_name(columns::CONTENT_TYPE)
                .unwrap()
                .is_null(0)
        );
        assert!(
            batch
                .column_by_name(columns::EXTEND_POLICY)
                .unwrap()
                .is_null(0)
        );
        assert!(batch.column_by_name(columns::OPERATOR).unwrap().is_null(0));
    }

    fn sample_permissions_changed() -> EntityEvent {
        EntityEvent::PermissionsChanged {
            entity_key: B256::repeat_byte(0x06),
            old_owner: Address::repeat_byte(0x01),
            new_owner: Address::repeat_byte(0x02),
            extend_policy: 1,
            operator: Address::repeat_byte(0x03),
        }
    }

    #[test]
    fn permissions_changed_populates_fields() {
        let events = vec![EventRow {
            event: sample_permissions_changed(),
            tx_index: 0,
            tx_hash: B256::ZERO,
            log_index: 0,
        }];
        let batch = build_record_batch(1000, B256::ZERO, 1000, BatchOp::Commit, &events).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(!batch.column_by_name(columns::OWNER).unwrap().is_null(0));
        assert!(
            !batch
                .column_by_name(columns::EXTEND_POLICY)
                .unwrap()
                .is_null(0)
        );
        assert!(!batch.column_by_name(columns::OPERATOR).unwrap().is_null(0));
        assert!(
            batch
                .column_by_name(columns::EXPIRES_AT_BLOCK)
                .unwrap()
                .is_null(0)
        );
        assert!(batch.column_by_name(columns::PAYLOAD).unwrap().is_null(0));
    }

    #[test]
    fn watermark_op_is_0xff() {
        let batch = build_watermark_batch(5000).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let op_col = batch
            .column_by_name(columns::OP)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        assert_eq!(op_col.value(0), 0xFF);
    }
}

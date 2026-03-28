use std::sync::Arc;

use alloy_primitives::{Address, B256};
use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt8Builder, UInt32Builder,
    UInt64Builder, builder::MapBuilder,
};
use arrow::record_batch::RecordBatch;

use crate::exex_schema::{entity_events_schema, map_field_names};
use crate::exex_types::{BatchOp, EntityEventType};

pub struct EventBuilder {
    pub block_number: u64,
    pub entity_key: B256,
    pub owner: Address,
    pub event_type: EntityEventType,
    pub expires_at: Option<u64>,
    pub old_expires_at: Option<u64>,
    pub content_type: Option<String>,
    pub payload: Option<Vec<u8>>,
    pub string_annotations: Vec<(String, String)>,
    pub numeric_annotations: Vec<(String, u64)>,
    pub extend_policy: Option<u8>,
    pub operator: Option<Address>,
    pub tx_index: u32,
    pub tx_hash: B256,
    pub log_index: u32,
    pub op: BatchOp,
}

impl EventBuilder {
    #[must_use]
    pub fn created(block_number: u64, entity_byte: u8) -> Self {
        Self {
            block_number,
            entity_key: B256::repeat_byte(entity_byte),
            owner: Address::repeat_byte(entity_byte),
            event_type: EntityEventType::Created,
            expires_at: Some(block_number + 100),
            old_expires_at: None,
            content_type: Some("text/plain".into()),
            payload: Some(b"hello".to_vec()),
            string_annotations: vec![("tag".into(), "test".into())],
            numeric_annotations: vec![("priority".into(), 1)],
            extend_policy: Some(0),
            operator: Some(Address::ZERO),
            tx_index: 0,
            tx_hash: B256::repeat_byte(0xAA),
            log_index: 0,
            op: BatchOp::Commit,
        }
    }

    #[must_use]
    pub const fn deleted(block_number: u64, entity_byte: u8) -> Self {
        Self {
            block_number,
            entity_key: B256::repeat_byte(entity_byte),
            owner: Address::repeat_byte(entity_byte),
            event_type: EntityEventType::Deleted,
            expires_at: None,
            old_expires_at: None,
            content_type: None,
            payload: None,
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: None,
            tx_index: 0,
            tx_hash: B256::repeat_byte(0xAA),
            log_index: 0,
            op: BatchOp::Commit,
        }
    }

    #[must_use]
    pub const fn extended(block_number: u64, entity_byte: u8, old_exp: u64, new_exp: u64) -> Self {
        Self {
            block_number,
            entity_key: B256::repeat_byte(entity_byte),
            owner: Address::repeat_byte(entity_byte),
            event_type: EntityEventType::Extended,
            expires_at: Some(new_exp),
            old_expires_at: Some(old_exp),
            content_type: None,
            payload: None,
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: None,
            tx_index: 0,
            tx_hash: B256::repeat_byte(0xAA),
            log_index: 0,
            op: BatchOp::Commit,
        }
    }

    #[must_use]
    pub fn updated(block_number: u64, entity_byte: u8, old_exp: u64, new_exp: u64) -> Self {
        Self {
            block_number,
            entity_key: B256::repeat_byte(entity_byte),
            owner: Address::repeat_byte(entity_byte),
            event_type: EntityEventType::Updated,
            expires_at: Some(new_exp),
            old_expires_at: Some(old_exp),
            content_type: Some("text/plain".into()),
            payload: Some(b"hello".to_vec()),
            string_annotations: vec![("tag".into(), "test".into())],
            numeric_annotations: vec![("priority".into(), 1)],
            extend_policy: Some(0),
            operator: Some(Address::ZERO),
            tx_index: 0,
            tx_hash: B256::repeat_byte(0xAA),
            log_index: 0,
            op: BatchOp::Commit,
        }
    }

    #[must_use]
    pub const fn permissions_changed(
        block_number: u64,
        entity_byte: u8,
        new_owner: Address,
        extend_policy: u8,
        operator: Address,
    ) -> Self {
        Self {
            block_number,
            entity_key: B256::repeat_byte(entity_byte),
            owner: new_owner,
            event_type: EntityEventType::PermissionsChanged,
            expires_at: None,
            old_expires_at: None,
            content_type: None,
            payload: None,
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: Some(extend_policy),
            operator: Some(operator),
            tx_index: 0,
            tx_hash: B256::repeat_byte(0xAA),
            log_index: 0,
            op: BatchOp::Commit,
        }
    }

    #[must_use]
    pub const fn with_log_index(mut self, log_index: u32) -> Self {
        self.log_index = log_index;
        self
    }

    #[must_use]
    pub const fn with_tx_hash(mut self, tx_hash: B256) -> Self {
        self.tx_hash = tx_hash;
        self
    }

    #[must_use]
    pub const fn with_op(mut self, op: BatchOp) -> Self {
        self.op = op;
        self
    }

    #[must_use]
    pub const fn with_entity_key(mut self, entity_key: B256) -> Self {
        self.entity_key = entity_key;
        self
    }

    #[must_use]
    pub const fn with_owner(mut self, owner: Address) -> Self {
        self.owner = owner;
        self
    }

    #[must_use]
    pub fn with_string_annotations(mut self, annotations: Vec<(String, String)>) -> Self {
        self.string_annotations = annotations;
        self
    }

    #[must_use]
    pub fn with_numeric_annotations(mut self, annotations: Vec<(String, u64)>) -> Self {
        self.numeric_annotations = annotations;
        self
    }

    #[must_use]
    pub fn with_content_type(mut self, content_type: &str) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    #[must_use]
    pub fn with_payload(mut self, payload: &[u8]) -> Self {
        self.payload = Some(payload.to_vec());
        self
    }

    #[must_use]
    pub const fn with_expires_at(mut self, expires_at: u64) -> Self {
        self.expires_at = Some(expires_at);
        self
    }
}

#[allow(clippy::cast_possible_truncation)]
pub fn build_batch(events: &[EventBuilder]) -> RecordBatch {
    let schema = entity_events_schema();
    let n = events.len();

    let mut block_number_b = UInt64Builder::with_capacity(n);
    let mut block_hash_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut tx_index_b = UInt32Builder::with_capacity(n);
    let mut tx_hash_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut log_index_b = UInt32Builder::with_capacity(n);
    let mut event_type_b = UInt8Builder::with_capacity(n);
    let mut entity_key_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut owner_b = FixedSizeBinaryBuilder::with_capacity(n, 20);
    let mut expires_b = UInt64Builder::with_capacity(n);
    let mut old_expires_b = UInt64Builder::with_capacity(n);
    let mut content_type_b = StringBuilder::with_capacity(n, 64);
    let mut payload_b = BinaryBuilder::with_capacity(n, 256);
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
    let mut extend_policy_b = UInt8Builder::with_capacity(n);
    let mut operator_b = FixedSizeBinaryBuilder::with_capacity(n, 20);
    let mut tip_block_b = UInt64Builder::with_capacity(n);
    let mut op_b = UInt8Builder::with_capacity(n);

    for ev in events {
        block_number_b.append_value(ev.block_number);
        block_hash_b
            .append_value(B256::repeat_byte(ev.block_number as u8).as_slice())
            .unwrap();
        tx_index_b.append_value(ev.tx_index);
        tx_hash_b.append_value(ev.tx_hash.as_slice()).unwrap();
        log_index_b.append_value(ev.log_index);
        event_type_b.append_value(ev.event_type as u8);
        entity_key_b.append_value(ev.entity_key.as_slice()).unwrap();
        owner_b.append_value(ev.owner.as_slice()).unwrap();

        match ev.expires_at {
            Some(v) => expires_b.append_value(v),
            None => expires_b.append_null(),
        }
        match ev.old_expires_at {
            Some(v) => old_expires_b.append_value(v),
            None => old_expires_b.append_null(),
        }
        match &ev.content_type {
            Some(v) => content_type_b.append_value(v),
            None => content_type_b.append_null(),
        }
        match &ev.payload {
            Some(v) => payload_b.append_value(v),
            None => payload_b.append_null(),
        }

        for (k, v) in &ev.string_annotations {
            str_ann_b.keys().append_value(k);
            str_ann_b.values().append_value(v);
        }
        str_ann_b.append(!ev.string_annotations.is_empty()).unwrap();

        for (k, v) in &ev.numeric_annotations {
            num_ann_b.keys().append_value(k);
            num_ann_b.values().append_value(*v);
        }
        num_ann_b
            .append(!ev.numeric_annotations.is_empty())
            .unwrap();

        match ev.extend_policy {
            Some(v) => extend_policy_b.append_value(v),
            None => extend_policy_b.append_null(),
        }
        match ev.operator {
            Some(addr) => operator_b.append_value(addr.as_slice()).unwrap(),
            None => operator_b.append_null(),
        }
        tip_block_b.append_value(ev.block_number);
        op_b.append_value(ev.op as u8);
    }

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
    RecordBatch::try_new(schema, columns).unwrap()
}

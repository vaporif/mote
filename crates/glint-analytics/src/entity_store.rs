use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock},
};

use alloy_primitives::{Address, B256, Bytes};
use arrow::{
    array::{
        ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt8Builder,
        UInt64Builder,
        builder::{MapBuilder, MapFieldNames},
    },
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    record_batch::RecordBatch,
};

#[derive(Debug, Clone)]
pub struct EntityRow {
    pub entity_key: B256,
    pub owner: Address,
    pub expires_at_block: u64,
    pub content_type: String,
    pub payload: Bytes,
    pub string_annotations: Vec<(String, String)>,
    pub numeric_annotations: Vec<(String, u64)>,
    pub created_at_block: u64,
    pub tx_hash: B256,
    pub extend_policy: u8,
    pub operator: Option<Address>,
}

#[derive(Debug, Default)]
pub struct EntityStore {
    entities: HashMap<B256, EntityRow>,
    by_owner: HashMap<Address, HashSet<B256>>,
}

impl EntityStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, row: EntityRow) {
        let key = row.entity_key;

        if let Some(existing) = self.entities.get(&key)
            && existing.owner != row.owner
            && let Some(set) = self.by_owner.get_mut(&existing.owner)
        {
            set.remove(&key);
            if set.is_empty() {
                self.by_owner.remove(&existing.owner);
            }
        }

        self.by_owner.entry(row.owner).or_default().insert(key);
        self.entities.insert(key, row);
    }

    pub fn remove(&mut self, key: &B256) -> Option<EntityRow> {
        let row = self.entities.remove(key)?;

        if let Some(set) = self.by_owner.get_mut(&row.owner) {
            set.remove(key);
            if set.is_empty() {
                self.by_owner.remove(&row.owner);
            }
        }

        Some(row)
    }

    pub fn get(&self, key: &B256) -> Option<&EntityRow> {
        self.entities.get(key)
    }

    pub fn get_mut(&mut self, key: &B256) -> Option<&mut EntityRow> {
        self.entities.get_mut(key)
    }

    pub fn get_by_owner(&self, owner: &Address) -> Vec<B256> {
        self.by_owner
            .get(owner)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.entities.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    pub fn clear(&mut self) {
        self.entities.clear();
        self.by_owner.clear();
    }

    // O(n) rebuild every block; fine up to ~1M entities.
    // Skip when no mutations occurred.
    pub fn to_record_batch(&self) -> RecordBatch {
        let n = self.entities.len();

        let mut entity_key_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
        let mut owner_b = FixedSizeBinaryBuilder::with_capacity(n, 20);
        let mut expires_b = UInt64Builder::with_capacity(n);
        let mut content_type_b = StringBuilder::with_capacity(n, n * 16);
        let mut payload_b = BinaryBuilder::with_capacity(n, n * 64);

        let mut string_ann_b = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        let mut numeric_ann_b = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            UInt64Builder::new(),
        );

        let mut created_b = UInt64Builder::with_capacity(n);
        let mut tx_hash_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
        let mut extend_policy_b = UInt8Builder::with_capacity(n);
        let mut operator_b = FixedSizeBinaryBuilder::with_capacity(n, 20);

        for row in self.entities.values() {
            entity_key_b
                .append_value(row.entity_key.as_slice())
                .expect("B256 is always 32 bytes");
            owner_b
                .append_value(row.owner.as_slice())
                .expect("Address is always 20 bytes");
            expires_b.append_value(row.expires_at_block);
            content_type_b.append_value(&row.content_type);
            payload_b.append_value(&row.payload);

            for (k, v) in &row.string_annotations {
                string_ann_b.keys().append_value(k);
                string_ann_b.values().append_value(v);
            }
            string_ann_b
                .append(true)
                .expect("keys and values must stay in sync");

            for (k, v) in &row.numeric_annotations {
                numeric_ann_b.keys().append_value(k);
                numeric_ann_b.values().append_value(*v);
            }
            numeric_ann_b
                .append(true)
                .expect("keys and values must stay in sync");

            created_b.append_value(row.created_at_block);
            tx_hash_b
                .append_value(row.tx_hash.as_slice())
                .expect("B256 is always 32 bytes");
            extend_policy_b.append_value(row.extend_policy);
            match row.operator {
                Some(op) => operator_b
                    .append_value(op.as_slice())
                    .expect("Address is always 20 bytes"),
                None => operator_b.append_null(),
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(entity_key_b.finish()),
            Arc::new(owner_b.finish()),
            Arc::new(expires_b.finish()),
            Arc::new(content_type_b.finish()),
            Arc::new(payload_b.finish()),
            Arc::new(string_ann_b.finish()),
            Arc::new(numeric_ann_b.finish()),
            Arc::new(created_b.finish()),
            Arc::new(tx_hash_b.finish()),
            Arc::new(extend_policy_b.finish()),
            Arc::new(operator_b.finish()),
        ];

        RecordBatch::try_new(entity_schema(), columns).expect("columns must match entity schema")
    }
}

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_owned(),
        key: "key".to_owned(),
        value: "value".to_owned(),
    }
}

fn build_entity_schema() -> Schema {
    let str_ann_entry = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    );
    let num_ann_entry = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::UInt64, true),
        ])),
        false,
    );

    Schema::new(vec![
        Field::new("entity_key", DataType::FixedSizeBinary(32), false),
        Field::new("owner", DataType::FixedSizeBinary(20), false),
        Field::new("expires_at_block", DataType::UInt64, false),
        Field::new("content_type", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
        Field::new(
            "string_annotations",
            DataType::Map(Arc::new(str_ann_entry), false),
            true,
        ),
        Field::new(
            "numeric_annotations",
            DataType::Map(Arc::new(num_ann_entry), false),
            true,
        ),
        Field::new("created_at_block", DataType::UInt64, false),
        Field::new("tx_hash", DataType::FixedSizeBinary(32), false),
        Field::new("extend_policy", DataType::UInt8, false),
        Field::new("operator", DataType::FixedSizeBinary(20), true),
    ])
}

static ENTITY_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(build_entity_schema()));

pub fn entity_schema() -> SchemaRef {
    Arc::clone(&ENTITY_SCHEMA)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes};

    fn sample_row(byte: u8) -> EntityRow {
        EntityRow {
            entity_key: B256::repeat_byte(byte),
            owner: Address::repeat_byte(byte),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_annotations: vec![("k1".into(), "v1".into())],
            numeric_annotations: vec![("n1".into(), 42)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: Some(Address::ZERO),
        }
    }

    #[test]
    fn insert_and_get() {
        let mut store = EntityStore::new();
        let row = sample_row(0x01);
        store.insert(row);
        assert_eq!(store.len(), 1);
        let got = store.get(&B256::repeat_byte(0x01)).unwrap();
        assert_eq!(got.owner, Address::repeat_byte(0x01));
    }

    #[test]
    fn insert_updates_by_owner_index() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let keys = store.get_by_owner(&Address::repeat_byte(0x01));
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn remove_cleans_both_indexes() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.remove(&B256::repeat_byte(0x01));
        assert_eq!(store.len(), 0);
        assert!(store.get_by_owner(&Address::repeat_byte(0x01)).is_empty());
    }

    #[test]
    fn update_changes_owner_index() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let mut updated = sample_row(0x01);
        updated.owner = Address::repeat_byte(0x02);
        store.insert(updated);
        assert!(store.get_by_owner(&Address::repeat_byte(0x01)).is_empty());
        assert_eq!(store.get_by_owner(&Address::repeat_byte(0x02)).len(), 1);
    }

    #[test]
    fn to_record_batch_schema() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let batch = store.to_record_batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);
        assert!(batch.schema().field_with_name("entity_key").is_ok());
        assert!(batch.schema().field_with_name("owner").is_ok());
        assert!(batch.schema().field_with_name("expires_at_block").is_ok());
        assert!(batch.schema().field_with_name("content_type").is_ok());
        assert!(batch.schema().field_with_name("payload").is_ok());
        assert!(batch.schema().field_with_name("string_annotations").is_ok());
        assert!(
            batch
                .schema()
                .field_with_name("numeric_annotations")
                .is_ok()
        );
        assert!(batch.schema().field_with_name("created_at_block").is_ok());
        assert!(batch.schema().field_with_name("tx_hash").is_ok());
        assert!(batch.schema().field_with_name("extend_policy").is_ok());
        assert!(batch.schema().field_with_name("operator").is_ok());
    }

    #[test]
    fn to_record_batch_empty_store() {
        let store = EntityStore::new();
        let batch = store.to_record_batch();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 11);
    }
}

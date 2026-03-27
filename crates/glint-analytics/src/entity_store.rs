use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
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
use roaring::RoaringBitmap;

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
struct SlotAllocator {
    next_id: u32,
    free_list: Vec<u32>,
}

impl SlotAllocator {
    fn allocate(&mut self) -> u32 {
        self.free_list.pop().unwrap_or_else(|| {
            let id = self.next_id;
            self.next_id += 1;
            id
        })
    }

    fn release(&mut self, id: u32) {
        self.free_list.push(id);
    }

    fn reset(&mut self) {
        self.next_id = 0;
        self.free_list.clear();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexSnapshot {
    pub(crate) string_ann_index: HashMap<(String, String), RoaringBitmap>,
    pub(crate) numeric_ann_index: HashMap<(String, u64), RoaringBitmap>,
    pub(crate) numeric_ann_range: HashMap<String, BTreeMap<u64, RoaringBitmap>>,
    pub(crate) owner_index: HashMap<Address, RoaringBitmap>,
    pub(crate) all_live_slots: RoaringBitmap,
    pub(crate) slot_to_row: HashMap<u32, usize>,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) batch: Arc<RecordBatch>,
    pub(crate) indexes: Arc<IndexSnapshot>,
}

#[derive(Debug, Default)]
pub struct EntityStore {
    entities: HashMap<B256, EntityRow>,
    slots: SlotAllocator,
    entity_to_slot: HashMap<B256, u32>,
    slot_to_entity: BTreeMap<u32, B256>,
    all_live_slots: RoaringBitmap,
    string_ann_index: HashMap<(String, String), RoaringBitmap>,
    numeric_ann_index: HashMap<(String, u64), RoaringBitmap>,
    numeric_ann_range: HashMap<String, BTreeMap<u64, RoaringBitmap>>,
    owner_index: HashMap<Address, RoaringBitmap>,
}

impl EntityStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, row: EntityRow) {
        let key = row.entity_key;

        if let Some(&existing_slot) = self.entity_to_slot.get(&key) {
            let old = self
                .entities
                .get(&key)
                .expect("slot implies entity exists")
                .clone();
            self.clear_index_bits(existing_slot, &old);
            self.set_index_bits(existing_slot, &row);
            self.entities.insert(key, row);
        } else {
            let slot = self.slots.allocate();
            self.entity_to_slot.insert(key, slot);
            self.slot_to_entity.insert(slot, key);
            self.all_live_slots.insert(slot);
            self.set_index_bits(slot, &row);
            self.entities.insert(key, row);
        }
    }

    pub fn remove(&mut self, key: &B256) -> Option<EntityRow> {
        let row = self.entities.remove(key)?;

        if let Some(&slot) = self.entity_to_slot.get(key) {
            self.clear_index_bits(slot, &row);
            self.all_live_slots.remove(slot);
            self.slot_to_entity.remove(&slot);
            self.entity_to_slot.remove(key);
            self.slots.release(slot);
        }

        Some(row)
    }

    pub fn get(&self, key: &B256) -> Option<&EntityRow> {
        self.entities.get(key)
    }

    /// Only non-indexed fields (`expires_at_block`, `tx_hash`) may be mutated
    /// through this reference. Mutating indexed fields (owner, annotations)
    /// will silently corrupt the bitmap indexes.
    pub(crate) fn get_mut(&mut self, key: &B256) -> Option<&mut EntityRow> {
        self.entities.get_mut(key)
    }

    pub fn get_by_owner(&self, owner: &Address) -> Vec<B256> {
        self.owner_index
            .get(owner)
            .map(|bm| {
                bm.iter()
                    .filter_map(|slot| self.slot_to_entity.get(&slot).copied())
                    .collect()
            })
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
        self.slots.reset();
        self.entity_to_slot.clear();
        self.slot_to_entity.clear();
        self.all_live_slots.clear();
        self.string_ann_index.clear();
        self.numeric_ann_index.clear();
        self.numeric_ann_range.clear();
        self.owner_index.clear();
    }

    fn set_index_bits(&mut self, slot: u32, row: &EntityRow) {
        for (k, v) in &row.string_annotations {
            self.string_ann_index
                .entry((k.clone(), v.clone()))
                .or_default()
                .insert(slot);
        }
        for (k, v) in &row.numeric_annotations {
            self.numeric_ann_index
                .entry((k.clone(), *v))
                .or_default()
                .insert(slot);
            self.numeric_ann_range
                .entry(k.clone())
                .or_default()
                .entry(*v)
                .or_default()
                .insert(slot);
        }
        self.owner_index.entry(row.owner).or_default().insert(slot);
    }

    fn clear_index_bits(&mut self, slot: u32, row: &EntityRow) {
        for (k, v) in &row.string_annotations {
            remove_from_bitmap(&mut self.string_ann_index, &(k.clone(), v.clone()), slot);
        }
        for (k, v) in &row.numeric_annotations {
            remove_from_bitmap(&mut self.numeric_ann_index, &(k.clone(), *v), slot);
            if let Entry::Occupied(mut btree_entry) = self.numeric_ann_range.entry(k.clone()) {
                remove_from_btree(btree_entry.get_mut(), v, slot);
                if btree_entry.get().is_empty() {
                    btree_entry.remove();
                }
            }
        }
        remove_from_bitmap(&mut self.owner_index, &row.owner, slot);
    }

    // TODO: COW/incremental approach
    pub fn snapshot(&self) -> eyre::Result<Snapshot> {
        // BTreeMap iterates in key order, so entries are already sorted by slot.
        let entries: Vec<(u32, B256)> = self
            .slot_to_entity
            .iter()
            .map(|(&slot, &key)| (slot, key))
            .collect();

        let slot_to_row: HashMap<u32, usize> = entries
            .iter()
            .enumerate()
            .map(|(row_idx, (slot, _))| (*slot, row_idx))
            .collect();

        let n = entries.len();

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

        for (_, key) in &entries {
            let row = &self.entities[key];
            entity_key_b.append_value(row.entity_key.as_slice())?;
            owner_b.append_value(row.owner.as_slice())?;
            expires_b.append_value(row.expires_at_block);
            content_type_b.append_value(&row.content_type);
            payload_b.append_value(&row.payload);

            for (k, v) in &row.string_annotations {
                string_ann_b.keys().append_value(k);
                string_ann_b.values().append_value(v);
            }
            string_ann_b.append(true)?;

            for (k, v) in &row.numeric_annotations {
                numeric_ann_b.keys().append_value(k);
                numeric_ann_b.values().append_value(*v);
            }
            numeric_ann_b.append(true)?;

            created_b.append_value(row.created_at_block);
            tx_hash_b.append_value(row.tx_hash.as_slice())?;
            extend_policy_b.append_value(row.extend_policy);
            match row.operator {
                Some(op) => operator_b.append_value(op.as_slice())?,
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

        let batch = Arc::new(RecordBatch::try_new(entity_schema(), columns)?);

        let indexes = Arc::new(IndexSnapshot {
            string_ann_index: self.string_ann_index.clone(),
            numeric_ann_index: self.numeric_ann_index.clone(),
            numeric_ann_range: self.numeric_ann_range.clone(),
            owner_index: self.owner_index.clone(),
            all_live_slots: self.all_live_slots.clone(),
            slot_to_row,
        });

        Ok(Snapshot { batch, indexes })
    }
}

fn remove_from_bitmap<K: Eq + std::hash::Hash + Clone>(
    map: &mut HashMap<K, RoaringBitmap>,
    key: &K,
    slot: u32,
) {
    if let Entry::Occupied(mut entry) = map.entry(key.clone()) {
        entry.get_mut().remove(slot);
        if entry.get().is_empty() {
            entry.remove();
        }
    }
}

fn remove_from_btree<K: Ord + Clone>(map: &mut BTreeMap<K, RoaringBitmap>, key: &K, slot: u32) {
    if let std::collections::btree_map::Entry::Occupied(mut entry) = map.entry(key.clone()) {
        entry.get_mut().remove(slot);
        if entry.get().is_empty() {
            entry.remove();
        }
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
    fn snapshot_batch_schema() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let snap = store.snapshot().expect("snapshot should succeed");
        let batch = &*snap.batch;
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
    fn snapshot_batch_empty_store() {
        let store = EntityStore::new();
        let snap = store.snapshot().expect("snapshot should succeed");
        assert_eq!(snap.batch.num_rows(), 0);
        assert_eq!(snap.batch.num_columns(), 11);
    }

    #[test]
    fn slot_allocator_sequential() {
        let mut alloc = SlotAllocator::default();
        assert_eq!(alloc.allocate(), 0);
        assert_eq!(alloc.allocate(), 1);
        assert_eq!(alloc.allocate(), 2);
    }

    #[test]
    fn slot_allocator_reuse() {
        let mut alloc = SlotAllocator::default();
        let s0 = alloc.allocate();
        let s1 = alloc.allocate();
        alloc.release(s0);
        let reused = alloc.allocate();
        assert_eq!(reused, s0);
        let fresh = alloc.allocate();
        assert_eq!(fresh, 2);
        assert_ne!(fresh, s1);
    }

    #[test]
    fn slot_allocator_reset() {
        let mut alloc = SlotAllocator::default();
        alloc.allocate();
        alloc.allocate();
        alloc.reset();
        assert_eq!(alloc.allocate(), 0);
    }

    #[test]
    fn insert_populates_indexes() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));

        let slot = store.entity_to_slot[&B256::repeat_byte(0x01)];

        assert!(store.all_live_slots.contains(slot));
        assert_eq!(store.all_live_slots.len(), 1);

        let str_bm = &store.string_ann_index[&("k1".to_owned(), "v1".to_owned())];
        assert!(str_bm.contains(slot));

        let num_bm = &store.numeric_ann_index[&("n1".to_owned(), 42)];
        assert!(num_bm.contains(slot));

        let range_bm = &store.numeric_ann_range["n1"][&42];
        assert!(range_bm.contains(slot));

        let owner_bm = &store.owner_index[&Address::repeat_byte(0x01)];
        assert!(owner_bm.contains(slot));
    }

    #[test]
    fn all_live_slots_cardinality_matches_len() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.insert(sample_row(0x02));
        assert_eq!(store.all_live_slots.len(), store.len() as u64);
        store.remove(&B256::repeat_byte(0x01));
        assert_eq!(store.all_live_slots.len(), store.len() as u64);
    }

    #[test]
    fn update_clears_old_annotation_indexes() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));

        let mut updated = sample_row(0x01);
        updated.string_annotations = vec![("k1".into(), "v2".into())];
        updated.numeric_annotations = vec![("n1".into(), 99)];
        store.insert(updated);

        let slot = store.entity_to_slot[&B256::repeat_byte(0x01)];

        assert!(
            !store
                .string_ann_index
                .contains_key(&("k1".to_owned(), "v1".to_owned()))
        );
        assert!(!store.numeric_ann_index.contains_key(&("n1".to_owned(), 42)));
        assert!(
            store
                .numeric_ann_range
                .get("n1")
                .and_then(|bt| bt.get(&42))
                .is_none()
        );

        let str_bm = &store.string_ann_index[&("k1".to_owned(), "v2".to_owned())];
        assert!(str_bm.contains(slot));
        let num_bm = &store.numeric_ann_index[&("n1".to_owned(), 99)];
        assert!(num_bm.contains(slot));

        assert_eq!(store.all_live_slots.len(), 1);
    }

    #[test]
    fn update_owner_clears_old_owner_index() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));

        let mut updated = sample_row(0x01);
        updated.owner = Address::repeat_byte(0x02);
        store.insert(updated);

        assert!(!store.owner_index.contains_key(&Address::repeat_byte(0x01)));

        let slot = store.entity_to_slot[&B256::repeat_byte(0x01)];
        let bm = &store.owner_index[&Address::repeat_byte(0x02)];
        assert!(bm.contains(slot));
    }

    #[test]
    fn remove_clears_all_indexes() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.remove(&B256::repeat_byte(0x01));

        assert!(store.all_live_slots.is_empty());
        assert!(store.entity_to_slot.is_empty());
        assert!(store.slot_to_entity.is_empty());
        assert!(store.string_ann_index.is_empty());
        assert!(store.numeric_ann_index.is_empty());
        assert!(store.numeric_ann_range.is_empty());
        assert!(store.owner_index.is_empty());
    }

    #[test]
    fn clear_resets_all_indexes() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.insert(sample_row(0x02));
        store.clear();

        assert!(store.all_live_slots.is_empty());
        assert!(store.entity_to_slot.is_empty());
        assert!(store.slot_to_entity.is_empty());
        assert!(store.string_ann_index.is_empty());
        assert!(store.numeric_ann_index.is_empty());
        assert!(store.numeric_ann_range.is_empty());
        assert!(store.owner_index.is_empty());
        assert_eq!(store.slots.allocate(), 0);
    }

    #[test]
    fn snapshot_consistency() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.insert(sample_row(0x02));

        let snap = store.snapshot().expect("snapshot should succeed");

        assert_eq!(snap.batch.num_rows(), 2);

        for slot in snap.indexes.all_live_slots.iter() {
            let row_idx = snap.indexes.slot_to_row[&slot];
            assert!(row_idx < snap.batch.num_rows());
        }

        assert_eq!(snap.indexes.slot_to_row.len(), 2);
    }

    #[test]
    fn snapshot_slot_to_row_after_removal() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.insert(sample_row(0x02));
        store.insert(sample_row(0x03));
        store.remove(&B256::repeat_byte(0x02));

        let snap = store.snapshot().expect("snapshot should succeed");
        assert_eq!(snap.batch.num_rows(), 2);

        assert_eq!(snap.indexes.slot_to_row.len(), 2);
        assert!(snap.indexes.slot_to_row.contains_key(&0));
        assert!(!snap.indexes.slot_to_row.contains_key(&1));
        assert!(snap.indexes.slot_to_row.contains_key(&2));

        let mut row_indices: Vec<usize> = snap.indexes.slot_to_row.values().copied().collect();
        row_indices.sort();
        assert_eq!(row_indices, vec![0, 1]);
    }
}

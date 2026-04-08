use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    sync::Arc,
};

use alloy_primitives::{Address, Bytes, B256};
use arrow::{
    array::{BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt64Builder, UInt8Builder},
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

// TODO: re-enable when bitmap pushdown is added to table_provider
#[allow(dead_code)]
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
    pub(crate) entities: Arc<RecordBatch>,
    pub(crate) string_annotations: Arc<RecordBatch>,
    pub(crate) numeric_annotations: Arc<RecordBatch>,
    // TODO: re-enable when bitmap pushdown is added to table_provider
    #[allow(dead_code)]
    pub(crate) indexes: Arc<IndexSnapshot>,
}

/// Mutable ref to [`EntityRow`] that hides indexed fields (owner, annotations)
/// to prevent bitmap corruption.
pub(crate) struct EntityRowMut<'a>(&'a mut EntityRow);

impl EntityRowMut<'_> {
    pub(crate) const fn set_expires_at_block(&mut self, block: u64) {
        self.0.expires_at_block = block;
    }

    pub(crate) const fn set_tx_hash(&mut self, hash: B256) {
        self.0.tx_hash = hash;
    }
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
    current_block: u64,
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

    /// Mutable ref hiding indexed fields. See [`EntityRowMut`].
    pub(crate) fn get_mut(&mut self, key: &B256) -> Option<EntityRowMut<'_>> {
        self.entities.get_mut(key).map(EntityRowMut)
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

    pub const fn current_block(&self) -> u64 {
        self.current_block
    }

    pub const fn set_current_block(&mut self, block: u64) {
        self.current_block = block;
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
        self.current_block = 0;
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
        // BTreeMap gives us slot-sorted order; skip expired
        let current = self.current_block;
        let entries: Vec<(u32, B256)> = self
            .slot_to_entity
            .iter()
            .filter(|(_, key)| {
                self.entities
                    .get(*key)
                    .is_some_and(|row| row.expires_at_block > current)
            })
            .map(|(&slot, &key)| (slot, key))
            .collect();

        let slot_to_row: HashMap<u32, usize> = entries
            .iter()
            .enumerate()
            .map(|(row_idx, (slot, _))| (*slot, row_idx))
            .collect();

        let n = entries.len();

        // entity columns (no annotations)
        let mut entity_key_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
        let mut owner_b = FixedSizeBinaryBuilder::with_capacity(n, 20);
        let mut expires_b = UInt64Builder::with_capacity(n);
        let mut content_type_b = StringBuilder::with_capacity(n, n * 16);
        let mut payload_b = BinaryBuilder::with_capacity(n, n * 64);
        let mut created_b = UInt64Builder::with_capacity(n);
        let mut tx_hash_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
        let mut extend_policy_b = UInt8Builder::with_capacity(n);
        let mut operator_b = FixedSizeBinaryBuilder::with_capacity(n, 20);

        // annotation columns
        let mut str_ek_b = FixedSizeBinaryBuilder::new(32);
        let mut str_key_b = StringBuilder::new();
        let mut str_val_b = StringBuilder::new();
        let mut num_ek_b = FixedSizeBinaryBuilder::new(32);
        let mut num_key_b = StringBuilder::new();
        let mut num_val_b = UInt64Builder::new();

        for (_, key) in &entries {
            let row = &self.entities[key];
            entity_key_b.append_value(row.entity_key.as_slice())?;
            owner_b.append_value(row.owner.as_slice())?;
            expires_b.append_value(row.expires_at_block);
            content_type_b.append_value(&row.content_type);
            payload_b.append_value(&row.payload);
            created_b.append_value(row.created_at_block);
            tx_hash_b.append_value(row.tx_hash.as_slice())?;
            extend_policy_b.append_value(row.extend_policy);
            match row.operator {
                Some(op) => operator_b.append_value(op.as_slice())?,
                None => operator_b.append_null(),
            }

            for (k, v) in &row.string_annotations {
                str_ek_b.append_value(row.entity_key.as_slice())?;
                str_key_b.append_value(k);
                str_val_b.append_value(v);
            }
            for (k, v) in &row.numeric_annotations {
                num_ek_b.append_value(row.entity_key.as_slice())?;
                num_key_b.append_value(k);
                num_val_b.append_value(*v);
            }
        }

        let entities_schema = glint_primitives::exex_schema::entities_latest_schema();
        let entities = Arc::new(RecordBatch::try_new(
            entities_schema,
            vec![
                Arc::new(entity_key_b.finish()),
                Arc::new(owner_b.finish()),
                Arc::new(expires_b.finish()),
                Arc::new(content_type_b.finish()),
                Arc::new(payload_b.finish()),
                Arc::new(created_b.finish()),
                Arc::new(tx_hash_b.finish()),
                Arc::new(extend_policy_b.finish()),
                Arc::new(operator_b.finish()),
            ],
        )?);

        let str_ann_schema = glint_primitives::exex_schema::string_annotations_schema();
        let string_annotations = Arc::new(
            RecordBatch::try_new(
                str_ann_schema.clone(),
                vec![
                    Arc::new(str_ek_b.finish()),
                    Arc::new(str_key_b.finish()),
                    Arc::new(str_val_b.finish()),
                ],
            )
            .unwrap_or_else(|_| RecordBatch::new_empty(str_ann_schema)),
        );

        let num_ann_schema = glint_primitives::exex_schema::numeric_annotations_schema();
        let numeric_annotations = Arc::new(
            RecordBatch::try_new(
                num_ann_schema.clone(),
                vec![
                    Arc::new(num_ek_b.finish()),
                    Arc::new(num_key_b.finish()),
                    Arc::new(num_val_b.finish()),
                ],
            )
            .unwrap_or_else(|_| RecordBatch::new_empty(num_ann_schema)),
        );

        // restrict indexes to live slots
        let live_slots: RoaringBitmap = entries.iter().map(|(slot, _)| *slot).collect();

        let indexes = Arc::new(IndexSnapshot {
            string_ann_index: intersect_index(&self.string_ann_index, &live_slots),
            numeric_ann_index: intersect_index(&self.numeric_ann_index, &live_slots),
            numeric_ann_range: intersect_range_index(&self.numeric_ann_range, &live_slots),
            owner_index: intersect_index(&self.owner_index, &live_slots),
            all_live_slots: live_slots,
            slot_to_row,
        });

        Ok(Snapshot {
            entities,
            string_annotations,
            numeric_annotations,
            indexes,
        })
    }
}

fn intersect_index<K: Eq + std::hash::Hash + Clone>(
    index: &HashMap<K, RoaringBitmap>,
    live: &RoaringBitmap,
) -> HashMap<K, RoaringBitmap> {
    index
        .iter()
        .filter_map(|(k, bm)| {
            let filtered = bm & live;
            if filtered.is_empty() {
                None
            } else {
                Some((k.clone(), filtered))
            }
        })
        .collect()
}

fn intersect_range_index(
    index: &HashMap<String, BTreeMap<u64, RoaringBitmap>>,
    live: &RoaringBitmap,
) -> HashMap<String, BTreeMap<u64, RoaringBitmap>> {
    index
        .iter()
        .filter_map(|(k, btree)| {
            let filtered: BTreeMap<u64, RoaringBitmap> = btree
                .iter()
                .filter_map(|(v, bm)| {
                    let f = bm & live;
                    if f.is_empty() {
                        None
                    } else {
                        Some((*v, f))
                    }
                })
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some((k.clone(), filtered))
            }
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes, B256};

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
    fn snapshot_entity_schema() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let snap = store.snapshot().expect("snapshot should succeed");
        assert_eq!(snap.entities.num_columns(), 9);
        assert_eq!(snap.entities.num_rows(), 1);
        assert!(snap
            .entities
            .schema()
            .field_with_name("string_annotations")
            .is_err());
        assert!(snap
            .entities
            .schema()
            .field_with_name("numeric_annotations")
            .is_err());
    }

    #[test]
    fn snapshot_annotation_batches() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01)); // has 1 string ann, 1 numeric ann
        let snap = store.snapshot().expect("snapshot should succeed");
        assert_eq!(snap.string_annotations.num_rows(), 1);
        assert_eq!(snap.string_annotations.num_columns(), 3);
        assert_eq!(snap.numeric_annotations.num_rows(), 1);
        assert_eq!(snap.numeric_annotations.num_columns(), 3);
    }

    #[test]
    fn snapshot_empty_store() {
        let store = EntityStore::new();
        let snap = store.snapshot().expect("snapshot should succeed");
        assert_eq!(snap.entities.num_rows(), 0);
        assert_eq!(snap.entities.num_columns(), 9);
        assert_eq!(snap.string_annotations.num_rows(), 0);
        assert_eq!(snap.numeric_annotations.num_rows(), 0);
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

        assert!(!store
            .string_ann_index
            .contains_key(&("k1".to_owned(), "v1".to_owned())));
        assert!(!store.numeric_ann_index.contains_key(&("n1".to_owned(), 42)));
        assert!(store
            .numeric_ann_range
            .get("n1")
            .and_then(|bt| bt.get(&42))
            .is_none());

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

        assert_eq!(snap.entities.num_rows(), 2);

        for slot in &snap.indexes.all_live_slots {
            let row_idx = snap.indexes.slot_to_row[&slot];
            assert!(row_idx < snap.entities.num_rows());
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
        assert_eq!(snap.entities.num_rows(), 2);

        assert_eq!(snap.indexes.slot_to_row.len(), 2);
        assert!(snap.indexes.slot_to_row.contains_key(&0));
        assert!(!snap.indexes.slot_to_row.contains_key(&1));
        assert!(snap.indexes.slot_to_row.contains_key(&2));

        let mut row_indices: Vec<usize> = snap.indexes.slot_to_row.values().copied().collect();
        row_indices.sort_unstable();
        assert_eq!(row_indices, vec![0, 1]);
    }

    #[test]
    fn snapshot_filters_expired_entities() {
        let mut store = EntityStore::new();

        store.insert(sample_row(0x01)); // expires at 100
        let mut row2 = sample_row(0x02);
        row2.expires_at_block = 200; // expires at 200
        store.insert(row2);

        let snap = store.snapshot().expect("snapshot");
        assert_eq!(snap.entities.num_rows(), 2);
        assert_eq!(snap.indexes.all_live_slots.len(), 2);

        // 0x01 now expired
        store.set_current_block(100);
        let snap = store.snapshot().expect("snapshot");
        assert_eq!(snap.entities.num_rows(), 1);
        assert_eq!(snap.indexes.all_live_slots.len(), 1);

        // both expired
        store.set_current_block(200);
        let snap = store.snapshot().expect("snapshot");
        assert_eq!(snap.entities.num_rows(), 0);
        assert_eq!(snap.indexes.all_live_slots.len(), 0);
    }

    #[test]
    fn snapshot_expired_entities_excluded_from_indexes() {
        let mut store = EntityStore::new();

        store.insert(sample_row(0x01));
        let mut row2 = sample_row(0x02);
        row2.expires_at_block = 200;
        row2.owner = Address::repeat_byte(0x02);
        row2.string_annotations = vec![("k2".into(), "v2".into())];
        row2.numeric_annotations = vec![("n2".into(), 77)];
        store.insert(row2);

        store.set_current_block(100); // expire 0x01
        let snap = store.snapshot().expect("snapshot");

        // only 0x02's owner remains
        assert!(!snap
            .indexes
            .owner_index
            .contains_key(&Address::repeat_byte(0x01)));
        assert!(snap
            .indexes
            .owner_index
            .contains_key(&Address::repeat_byte(0x02)));

        // k1/v1 gone with expired entity
        assert!(!snap
            .indexes
            .string_ann_index
            .contains_key(&("k1".to_owned(), "v1".to_owned())));
        assert!(snap
            .indexes
            .string_ann_index
            .contains_key(&("k2".to_owned(), "v2".to_owned())));

        // n1/42 gone with expired entity
        assert!(!snap
            .indexes
            .numeric_ann_index
            .contains_key(&("n1".to_owned(), 42)));
        assert!(snap
            .indexes
            .numeric_ann_index
            .contains_key(&("n2".to_owned(), 77)));
    }

    #[test]
    fn get_mut_only_exposes_non_indexed_fields() {
        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));

        let key = B256::repeat_byte(0x01);
        let new_hash = B256::repeat_byte(0xFF);

        let mut row = store.get_mut(&key).expect("entity exists");
        row.set_expires_at_block(999);
        row.set_tx_hash(new_hash);

        let entity = store.get(&key).expect("entity exists");
        assert_eq!(entity.expires_at_block, 999);
        assert_eq!(entity.tx_hash, new_hash);
        // owner and annotations unchanged
        assert_eq!(entity.owner, Address::repeat_byte(0x01));
    }
}

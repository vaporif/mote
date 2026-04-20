use alloy_primitives::B256;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// How many drained blocks we keep around so reorgs can undo expirations.
/// A reorg deeper than this loses drain history — fine for OP Stack L3.
pub(crate) const DRAIN_HISTORY_CAPACITY: usize = 256;

#[derive(Debug)]
struct DrainedBlock {
    block_number: u64,
    entity_keys: BTreeSet<B256>,
}

/// Not persisted - rebuilt from event logs on cold start.
#[derive(Debug)]
pub struct ExpirationIndex {
    index: HashMap<u64, BTreeSet<B256>>,
    last_drained: Option<u64>,
    drain_history: VecDeque<DrainedBlock>,
}

impl Default for ExpirationIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpirationIndex {
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            last_drained: None,
            drain_history: VecDeque::with_capacity(DRAIN_HISTORY_CAPACITY),
        }
    }

    pub fn insert(&mut self, block_number: u64, entity_key: B256) {
        self.index
            .entry(block_number)
            .or_default()
            .insert(entity_key);
    }

    pub fn remove(&mut self, block_number: u64, entity_key: &B256) {
        if let Some(keys) = self.index.get_mut(&block_number) {
            keys.remove(entity_key);
            if keys.is_empty() {
                self.index.remove(&block_number);
            }
        }
    }

    pub fn get_expired(&self, block_number: u64) -> Option<&BTreeSet<B256>> {
        self.index.get(&block_number)
    }

    pub fn drain_block(&mut self, block_number: u64) -> Vec<B256> {
        self.last_drained = Some(block_number);

        let Some(set) = self.index.remove(&block_number) else {
            return vec![];
        };
        if self.drain_history.len() >= DRAIN_HISTORY_CAPACITY {
            self.drain_history.pop_front();
        }
        let keys: Vec<B256> = set.iter().copied().collect();
        self.drain_history.push_back(DrainedBlock {
            block_number,
            entity_keys: set,
        });
        keys
    }

    pub const fn last_drained_block(&self) -> Option<u64> {
        self.last_drained
    }

    pub fn restore_drained_since(&mut self, block_number: u64) -> usize {
        let mut restored = 0;
        while let Some(entry) = self.drain_history.back() {
            if entry.block_number < block_number {
                break;
            }
            let entry = self.drain_history.pop_back().unwrap();
            restored += entry.entity_keys.len();
            self.index
                .entry(entry.block_number)
                .or_default()
                .extend(entry.entity_keys);
        }
        self.last_drained = None;
        restored
    }

    pub fn clear_range(&mut self, range: std::ops::RangeInclusive<u64>) {
        self.index.retain(|block, _| !range.contains(block));
    }

    /// Cold-start rebuild from event logs. Caller must pre-filter to only alive
    /// entities with their latest expiration.
    pub fn rebuild_from_logs(&mut self, logs: impl Iterator<Item = (B256, u64)>) {
        for (entity_key, expires_at_block) in logs {
            self.insert(expires_at_block, entity_key);
        }
    }

    /// Drop all occurrences of `keys` from every block.
    pub fn remove_entities(&mut self, keys: &HashSet<B256>) {
        self.index.retain(|_block, set| {
            set.retain(|k| !keys.contains(k));
            !set.is_empty()
        });
    }

    pub fn iter_entries(&self) -> impl Iterator<Item = (&u64, &BTreeSet<B256>)> {
        self.index.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn insert_and_get() {
        let mut idx = ExpirationIndex::new();
        let key = B256::repeat_byte(0x01);
        idx.insert(100, key);
        let expired = idx.get_expired(100).unwrap();
        assert!(expired.contains(&key));
        assert_eq!(expired.len(), 1);
    }

    #[test]
    fn get_nonexistent_block_returns_none() {
        let idx = ExpirationIndex::new();
        assert_eq!(idx.get_expired(999), None);
    }

    #[test]
    fn drain_returns_sorted_keys() {
        let mut idx = ExpirationIndex::new();
        let key_a = B256::repeat_byte(0xAA);
        let key_b = B256::repeat_byte(0x11);
        let key_c = B256::repeat_byte(0x55);
        idx.insert(100, key_a);
        idx.insert(100, key_b);
        idx.insert(100, key_c);

        let drained = idx.drain_block(100);
        let expected = vec![key_b, key_c, key_a]; // 0x11 < 0x55 < 0xAA
        assert_eq!(drained, expected);
    }

    #[test]
    fn drain_removes_block_entry() {
        let mut idx = ExpirationIndex::new();
        idx.insert(100, B256::repeat_byte(0x01));
        idx.drain_block(100);
        assert_eq!(idx.get_expired(100), None);
    }

    #[test]
    fn remove_specific_entity() {
        let mut idx = ExpirationIndex::new();
        let key_a = B256::repeat_byte(0x01);
        let key_b = B256::repeat_byte(0x02);
        idx.insert(100, key_a);
        idx.insert(100, key_b);

        idx.remove(100, &key_a);

        let remaining = idx.get_expired(100).unwrap();
        assert!(remaining.contains(&key_b));
        assert!(!remaining.contains(&key_a));
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn remove_last_entity_removes_block_entry() {
        let mut idx = ExpirationIndex::new();
        let key = B256::repeat_byte(0x01);
        idx.insert(100, key);
        idx.remove(100, &key);
        assert_eq!(idx.get_expired(100), None);
    }

    #[test]
    fn clear_range_removes_blocks_in_range() {
        let mut idx = ExpirationIndex::new();
        idx.insert(100, B256::repeat_byte(0x01));
        idx.insert(101, B256::repeat_byte(0x02));
        idx.insert(102, B256::repeat_byte(0x03));
        idx.insert(200, B256::repeat_byte(0x04));

        idx.clear_range(100..=102);

        assert_eq!(idx.get_expired(100), None);
        assert_eq!(idx.get_expired(101), None);
        assert_eq!(idx.get_expired(102), None);
        assert_eq!(idx.get_expired(200).map(BTreeSet::len), Some(1));
    }

    #[test]
    fn last_drained_block_tracks_highest_drain() {
        let mut idx = ExpirationIndex::new();
        assert_eq!(idx.last_drained_block(), None);

        idx.insert(100, B256::repeat_byte(0x01));
        idx.drain_block(100);
        assert_eq!(idx.last_drained_block(), Some(100));

        idx.insert(200, B256::repeat_byte(0x02));
        idx.drain_block(200);
        assert_eq!(idx.last_drained_block(), Some(200));
    }

    #[test]
    fn rebuild_from_logs() {
        let mut idx = ExpirationIndex::new();
        let key_a = B256::repeat_byte(0x01);
        let key_b = B256::repeat_byte(0x02);

        let logs = vec![(key_a, 100), (key_b, 200), (B256::repeat_byte(0x03), 100)];

        idx.rebuild_from_logs(logs.into_iter());

        assert_eq!(idx.get_expired(100).map(BTreeSet::len), Some(2));
        assert_eq!(idx.get_expired(200).map(BTreeSet::len), Some(1));
        assert_eq!(idx.get_expired(300), None);
    }

    #[test]
    fn iter_entries_yields_all_blocks() {
        let mut idx = ExpirationIndex::new();
        let key_a = B256::repeat_byte(0x01);
        let key_b = B256::repeat_byte(0x02);
        let key_c = B256::repeat_byte(0x03);

        idx.insert(100, key_a);
        idx.insert(100, key_b);
        idx.insert(200, key_c);

        let mut entries: Vec<_> = idx.iter_entries().map(|(b, s)| (*b, s.len())).collect();
        entries.sort_by_key(|(b, _)| *b);
        assert_eq!(entries, vec![(100, 2), (200, 1)]);
    }

    #[test]
    fn new_index_has_drain_history_capacity() {
        let idx = ExpirationIndex::new();
        assert_eq!(idx.drain_history.capacity(), DRAIN_HISTORY_CAPACITY);
    }

    #[test]
    fn drain_records_history() {
        let mut idx = ExpirationIndex::new();
        let key = B256::repeat_byte(0x01);
        idx.insert(10, key);

        idx.drain_block(10);

        assert_eq!(idx.drain_history.len(), 1);
        assert_eq!(idx.drain_history[0].block_number, 10);
        assert!(idx.drain_history[0].entity_keys.contains(&key));
    }

    #[test]
    fn empty_drain_not_recorded() {
        let mut idx = ExpirationIndex::new();
        idx.drain_block(10);
        assert_eq!(idx.drain_history.len(), 0);
    }

    #[test]
    fn drain_history_capacity_eviction() {
        let mut idx = ExpirationIndex::new();

        for block in 0..=(DRAIN_HISTORY_CAPACITY as u64) {
            idx.insert(block, B256::repeat_byte(block as u8));
            idx.drain_block(block);
        }

        assert_eq!(idx.drain_history.len(), DRAIN_HISTORY_CAPACITY);
        assert_eq!(idx.drain_history.front().unwrap().block_number, 1);
    }

    #[test]
    fn restore_drained_basic() {
        let mut idx = ExpirationIndex::new();
        let key = B256::repeat_byte(0x01);
        idx.insert(10, key);

        idx.drain_block(10);
        assert_eq!(idx.get_expired(10), None);

        let restored = idx.restore_drained_since(10);
        assert_eq!(restored, 1);
        assert!(idx.get_expired(10).unwrap().contains(&key));
        assert_eq!(idx.last_drained_block(), None);
    }

    #[test]
    fn restore_drained_partial() {
        let mut idx = ExpirationIndex::new();
        idx.insert(5, B256::repeat_byte(0x05));
        idx.insert(6, B256::repeat_byte(0x06));
        idx.insert(7, B256::repeat_byte(0x07));
        idx.insert(8, B256::repeat_byte(0x08));

        idx.drain_block(5);
        idx.drain_block(6);
        idx.drain_block(7);
        idx.drain_block(8);

        let restored = idx.restore_drained_since(7);
        assert_eq!(restored, 2);

        assert!(
            idx.get_expired(7)
                .unwrap()
                .contains(&B256::repeat_byte(0x07))
        );
        assert!(
            idx.get_expired(8)
                .unwrap()
                .contains(&B256::repeat_byte(0x08))
        );

        assert_eq!(idx.get_expired(5), None);
        assert_eq!(idx.get_expired(6), None);
        assert_eq!(idx.drain_history.len(), 2);

        assert_eq!(idx.last_drained_block(), None);
    }

    #[test]
    fn restore_after_eviction_only_recovers_buffered() {
        let mut idx = ExpirationIndex::new();

        for block in 0..(DRAIN_HISTORY_CAPACITY as u64 + 10) {
            idx.insert(block, B256::repeat_byte(block as u8));
            idx.drain_block(block);
        }

        let restored = idx.restore_drained_since(0);
        assert_eq!(restored, DRAIN_HISTORY_CAPACITY);
        assert_eq!(idx.get_expired(0), None);
        assert!(idx.get_expired(10).is_some());
    }

    #[test]
    fn re_drain_after_restore() {
        let mut idx = ExpirationIndex::new();
        let key = B256::repeat_byte(0x01);
        idx.insert(10, key);

        idx.drain_block(10);
        idx.restore_drained_since(10);
        let drained = idx.drain_block(10);

        assert_eq!(drained, vec![key]);
        assert_eq!(idx.drain_history.len(), 1);
        assert_eq!(idx.drain_history[0].block_number, 10);
    }

    #[test]
    fn integration_reorg_flow() {
        let mut idx = ExpirationIndex::new();
        let key7 = B256::repeat_byte(0x07);
        let key8 = B256::repeat_byte(0x08);
        let key9 = B256::repeat_byte(0x09);
        let key10 = B256::repeat_byte(0x0A);

        idx.insert(7, key7);
        idx.insert(8, key8);
        idx.insert(9, key9);
        idx.insert(10, key10);

        for block in 1..=10 {
            idx.drain_block(block);
        }
        assert_eq!(idx.last_drained_block(), Some(10));
        assert_eq!(idx.drain_history.len(), 4);

        let restored = idx.restore_drained_since(7);
        assert_eq!(restored, 4);

        assert_eq!(idx.last_drained_block(), None);
        assert!(idx.get_expired(7).unwrap().contains(&key7));
        assert!(idx.get_expired(8).unwrap().contains(&key8));
        assert!(idx.get_expired(9).unwrap().contains(&key9));
        assert!(idx.get_expired(10).unwrap().contains(&key10));
        for block in 1..=6 {
            assert_eq!(idx.get_expired(block), None);
        }
        assert!(idx.drain_history.is_empty());
    }

    #[test]
    fn consecutive_reorg() {
        let mut idx = ExpirationIndex::new();
        let key5 = B256::repeat_byte(0x05);
        let key6 = B256::repeat_byte(0x06);
        let key7 = B256::repeat_byte(0x07);
        let key8 = B256::repeat_byte(0x08);
        let key9 = B256::repeat_byte(0x09);
        let key10 = B256::repeat_byte(0x0A);

        idx.insert(5, key5);
        idx.insert(6, key6);
        idx.insert(7, key7);
        idx.insert(8, key8);
        idx.insert(9, key9);
        idx.insert(10, key10);
        for block in 5..=10 {
            idx.drain_block(block);
        }
        assert_eq!(idx.drain_history.len(), 6);

        idx.restore_drained_since(7);
        assert_eq!(idx.drain_history.len(), 2);

        let drained7 = idx.drain_block(7);
        assert_eq!(drained7, vec![key7]);
        assert_eq!(idx.drain_history.len(), 3);

        let restored = idx.restore_drained_since(8);
        assert_eq!(restored, 0);
        assert_eq!(idx.drain_history.len(), 3);

        assert!(idx.get_expired(8).unwrap().contains(&key8));
        assert!(idx.get_expired(9).unwrap().contains(&key9));
        assert!(idx.get_expired(10).unwrap().contains(&key10));
        assert_eq!(idx.get_expired(7), None);
    }

    #[test]
    fn restore_beyond_history_restores_nothing() {
        let mut idx = ExpirationIndex::new();
        idx.insert(5, B256::repeat_byte(0x05));
        idx.drain_block(5);
        assert_eq!(idx.last_drained_block(), Some(5));

        let restored = idx.restore_drained_since(100);
        assert_eq!(restored, 0);
        assert_eq!(idx.drain_history.len(), 1);
        assert_eq!(idx.last_drained_block(), None);
    }

    #[test]
    fn restore_on_empty_history() {
        let mut idx = ExpirationIndex::new();
        let restored = idx.restore_drained_since(0);
        assert_eq!(restored, 0);
        assert!(idx.drain_history.is_empty());
        assert_eq!(idx.last_drained_block(), None);
    }

    #[test]
    fn restore_multiple_entities_same_block() {
        let mut idx = ExpirationIndex::new();
        let key_a = B256::repeat_byte(0x0A);
        let key_b = B256::repeat_byte(0x0B);
        let key_c = B256::repeat_byte(0x0C);
        idx.insert(10, key_a);
        idx.insert(10, key_b);
        idx.insert(10, key_c);

        idx.drain_block(10);
        assert_eq!(idx.drain_history.len(), 1);

        let restored = idx.restore_drained_since(10);
        assert_eq!(restored, 3);

        let expired = idx.get_expired(10).unwrap();
        assert!(expired.contains(&key_a));
        assert!(expired.contains(&key_b));
        assert!(expired.contains(&key_c));
    }

    #[test]
    fn restore_merges_with_new_inserts() {
        let mut idx = ExpirationIndex::new();
        let old_key = B256::repeat_byte(0x01);
        let new_key = B256::repeat_byte(0x02);

        idx.insert(10, old_key);
        idx.drain_block(10);

        // re-insert at the same block after drain (reorg replay)
        idx.insert(10, new_key);

        let restored = idx.restore_drained_since(10);
        assert_eq!(restored, 1);

        let expired = idx.get_expired(10).unwrap();
        assert!(expired.contains(&old_key));
        assert!(expired.contains(&new_key));
    }
}

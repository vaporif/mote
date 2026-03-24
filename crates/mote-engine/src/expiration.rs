use alloy_primitives::B256;
use std::collections::HashMap;

/// Not persisted — rebuilt from event logs on cold start.
#[derive(Debug, Default)]
pub struct ExpirationIndex {
    index: HashMap<u64, Vec<B256>>,
}

impl ExpirationIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, block_number: u64, entity_key: B256) {
        self.index.entry(block_number).or_default().push(entity_key);
    }

    /// Called on delete/update to cancel a pending expiration.
    pub fn remove(&mut self, block_number: u64, entity_key: &B256) {
        if let Some(keys) = self.index.get_mut(&block_number) {
            keys.retain(|k| k != entity_key);
            if keys.is_empty() {
                self.index.remove(&block_number);
            }
        }
    }

    pub fn get_expired(&self, block_number: u64) -> Option<&[B256]> {
        self.index.get(&block_number).map(|v| v.as_slice())
    }

    /// Sorted for deterministic consensus ordering.
    pub fn drain_block(&mut self, block_number: u64) -> Vec<B256> {
        let mut keys = self.index.remove(&block_number).unwrap_or_default();
        keys.sort();
        keys
    }

    /// Cold-start rebuild: scan the last MAX_BTL blocks of create/update/extend
    /// logs and repopulate the index. Caller must pre-filter to only alive
    /// entities with their latest expiration — duplicates or stale entries here
    /// are consensus bugs.
    pub fn rebuild_from_logs(&mut self, logs: impl Iterator<Item = (B256, u64)>) {
        for (entity_key, expires_at_block) in logs {
            self.insert(expires_at_block, entity_key);
        }
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
        let entities = idx.get_expired(100);
        assert_eq!(entities, Some(vec![key].as_slice()));
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
        let mut expected = vec![key_a, key_b, key_c];
        expected.sort();
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

        let remaining = idx.get_expired(100);
        assert_eq!(remaining, Some(vec![key_b].as_slice()));
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
    fn rebuild_from_logs() {
        let mut idx = ExpirationIndex::new();
        let key_a = B256::repeat_byte(0x01);
        let key_b = B256::repeat_byte(0x02);

        let logs = vec![(key_a, 100), (key_b, 200), (B256::repeat_byte(0x03), 100)];

        idx.rebuild_from_logs(logs.into_iter());

        assert_eq!(idx.get_expired(100).map(<[B256]>::len), Some(2));
        assert_eq!(idx.get_expired(200).map(<[B256]>::len), Some(1));
        assert_eq!(idx.get_expired(300), None);
    }
}

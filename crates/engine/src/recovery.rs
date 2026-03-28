use std::collections::{HashMap, HashSet};
use std::path::Path;

use alloy_eips::BlockHashOrNumber;
use alloy_primitives::B256;
use glint_primitives::config::GlintChainConfig;
use glint_primitives::constants::PROCESSOR_ADDRESS;
use glint_primitives::parse::{EntityEvent, parse_log};
use reth_provider::ReceiptProvider;
use tracing::{info, warn};

use crate::checkpoint::ExpirationCheckpoint;
use crate::expiration::ExpirationIndex;

#[derive(Debug, Default)]
pub struct LiveEntityTracker {
    entities: HashMap<B256, u64>,
    all_seen: HashSet<B256>,
}

impl LiveEntityTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_event(&mut self, event: &EntityEvent) {
        let key = match event {
            EntityEvent::Created {
                entity_key,
                expires_at,
                ..
            } => {
                self.entities.insert(*entity_key, *expires_at);
                *entity_key
            }
            EntityEvent::Updated {
                entity_key,
                new_expires_at,
                ..
            }
            | EntityEvent::Extended {
                entity_key,
                new_expires_at,
                ..
            } => {
                self.entities.insert(*entity_key, *new_expires_at);
                *entity_key
            }
            EntityEvent::Deleted { entity_key, .. } | EntityEvent::Expired { entity_key, .. } => {
                self.entities.remove(entity_key);
                *entity_key
            }
            EntityEvent::PermissionsChanged { entity_key, .. } => *entity_key,
        };
        self.all_seen.insert(key);
    }

    #[must_use]
    pub const fn live_entities(&self) -> &HashMap<B256, u64> {
        &self.entities
    }

    #[must_use]
    pub const fn all_seen_keys(&self) -> &HashSet<B256> {
        &self.all_seen
    }

    #[must_use]
    pub fn into_inner(self) -> (HashMap<B256, u64>, HashSet<B256>) {
        (self.entities, self.all_seen)
    }
}

pub fn rebuild_expiration_index<P>(
    provider: &P,
    config: &GlintChainConfig,
    tip_block: u64,
) -> eyre::Result<ExpirationIndex>
where
    P: ReceiptProvider,
{
    if tip_block == 0 {
        info!("fresh node, skipping cold-start recovery");
        return Ok(ExpirationIndex::new());
    }

    let start_block = tip_block.saturating_sub(config.max_btl);
    let total = tip_block - start_block;
    info!(
        start_block,
        tip_block, "starting cold-start recovery ({total} blocks)"
    );

    let mut tracker = LiveEntityTracker::new();

    for block_num in start_block..=tip_block {
        if let Some(receipts) = provider.receipts_by_block(BlockHashOrNumber::Number(block_num))? {
            let events = receipts
                .iter()
                .flat_map(alloy_consensus::TxReceipt::logs)
                .filter(|log| log.address == PROCESSOR_ADDRESS)
                .filter_map(|log| {
                    parse_log(log)
                        .inspect_err(|e| {
                            warn!(block_num, ?e, "skipping unparsable log during recovery");
                        })
                        .ok()
                        .flatten()
                });

            for event in events {
                tracker.apply_event(&event);
            }
        }

        let blocks_scanned = block_num - start_block + 1;
        if total > 0 && blocks_scanned.is_multiple_of(10_000) {
            let pct = (blocks_scanned * 100) / total;
            info!("cold-start recovery: {blocks_scanned}/{total} blocks scanned ({pct}%)");
        }
    }

    let entity_count = tracker.live_entities().len();
    let mut index = ExpirationIndex::new();
    let (entities, _seen) = tracker.into_inner();
    index.rebuild_from_logs(entities.into_iter());

    info!(entity_count, "expiration index rebuilt");
    Ok(index)
}

/// Like `rebuild_expiration_index` but replays only blocks after `start_block`.
pub fn rebuild_expiration_index_partial<P>(
    provider: &P,
    config: &GlintChainConfig,
    tip_block: u64,
    start_block: u64,
    mut index: ExpirationIndex,
) -> eyre::Result<ExpirationIndex>
where
    P: ReceiptProvider,
{
    let replay_from = start_block + 1;
    if replay_from > tip_block {
        info!(
            start_block,
            tip_block, "checkpoint is current, no replay needed"
        );
        return Ok(index);
    }

    let total = tip_block - replay_from + 1;
    info!(
        replay_from,
        tip_block, "partial recovery from checkpoint ({total} blocks)"
    );

    let mut tracker = LiveEntityTracker::new();

    for block_num in replay_from..=tip_block {
        if let Some(receipts) = provider.receipts_by_block(BlockHashOrNumber::Number(block_num))? {
            let events = receipts
                .iter()
                .flat_map(alloy_consensus::TxReceipt::logs)
                .filter(|log| log.address == PROCESSOR_ADDRESS)
                .filter_map(|log| {
                    parse_log(log)
                        .inspect_err(|e| {
                            warn!(block_num, ?e, "skipping unparsable log during recovery");
                        })
                        .ok()
                        .flatten()
                });

            for event in events {
                tracker.apply_event(&event);
            }
        }

        let blocks_scanned = block_num - replay_from + 1;
        if total > 0 && blocks_scanned.is_multiple_of(10_000) {
            let pct = (blocks_scanned * 100) / total;
            info!("partial recovery: {blocks_scanned}/{total} blocks scanned ({pct}%)");
        }
    }

    let (live_entities, all_seen) = tracker.into_inner();

    // Remove stale entries for entities that were touched during the replay window.
    // Without this, entities that were deleted/expired or had their expiration changed
    // would leave phantom entries in the index from the checkpoint.
    index.remove_entities(&all_seen);

    for (entity_key, expires_at) in live_entities {
        index.insert(expires_at, entity_key);
    }

    // Prune entries that are now expired (below the safe window)
    let earliest_valid = tip_block.saturating_sub(config.max_btl);
    if earliest_valid > 0 {
        index.clear_range(0..=earliest_valid.saturating_sub(1));
    }

    info!(tip_block, "expiration index partially rebuilt");
    Ok(index)
}

pub fn save_checkpoint(
    index: &ExpirationIndex,
    tip_block: u64,
    tip_block_hash: &B256,
    path: &Path,
) -> eyre::Result<()> {
    let mut entries: Vec<(u64, Vec<B256>)> = index
        .iter_entries()
        .map(|(&block, keys)| (block, keys.iter().copied().collect()))
        .collect();
    entries.sort_by_key(|(block, _)| *block);

    let checkpoint = ExpirationCheckpoint {
        version: 1,
        tip_block,
        tip_block_hash: *tip_block_hash,
        entries,
    };

    let data = checkpoint.serialize();
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, &data)?;
    std::fs::rename(&tmp_path, path)?;
    info!(tip_block, path = %path.display(), "expiration checkpoint saved");
    Ok(())
}

pub fn load_checkpoint(path: &Path) -> eyre::Result<(ExpirationIndex, u64, B256)> {
    let data = std::fs::read(path)?;
    let checkpoint = ExpirationCheckpoint::deserialize(&data)?;

    let mut index = ExpirationIndex::new();
    for (block, keys) in &checkpoint.entries {
        for key in keys {
            index.insert(*block, *key);
        }
    }

    Ok((index, checkpoint.tip_block, checkpoint.tip_block_hash))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, Bytes};

    use super::*;

    fn created(key: B256, expires_at: u64) -> EntityEvent {
        EntityEvent::Created {
            entity_key: key,
            owner: Address::ZERO,
            expires_at,
            content_type: String::new(),
            payload: Bytes::default(),
            string_keys: vec![],
            string_values: vec![],
            numeric_keys: vec![],
            numeric_values: vec![],
            extend_policy: 0,
            operator: Address::ZERO,
        }
    }

    #[test]
    fn created_entity_tracked() {
        let mut tracker = LiveEntityTracker::new();
        let key = B256::repeat_byte(0x01);
        tracker.apply_event(&created(key, 1000));
        assert_eq!(tracker.live_entities().get(&key), Some(&1000));
    }

    #[test]
    fn deleted_entity_removed() {
        let mut tracker = LiveEntityTracker::new();
        let key = B256::repeat_byte(0x01);
        tracker.apply_event(&created(key, 1000));
        tracker.apply_event(&EntityEvent::Deleted {
            entity_key: key,
            owner: Address::ZERO,
            sender: Address::ZERO,
        });
        assert!(tracker.live_entities().is_empty());
    }

    #[test]
    fn extended_entity_updates_expiry() {
        let mut tracker = LiveEntityTracker::new();
        let key = B256::repeat_byte(0x01);
        tracker.apply_event(&created(key, 1000));
        tracker.apply_event(&EntityEvent::Extended {
            entity_key: key,
            old_expires_at: 1000,
            new_expires_at: 1500,
            owner: Address::ZERO,
        });
        assert_eq!(tracker.live_entities().get(&key), Some(&1500));
    }

    #[test]
    fn updated_entity_resets_expiry() {
        let mut tracker = LiveEntityTracker::new();
        let key = B256::repeat_byte(0x01);
        tracker.apply_event(&created(key, 1000));
        tracker.apply_event(&EntityEvent::Updated {
            entity_key: key,
            owner: Address::ZERO,
            old_expires_at: 1000,
            new_expires_at: 2000,
            content_type: String::new(),
            payload: Bytes::default(),
            string_keys: vec![],
            string_values: vec![],
            numeric_keys: vec![],
            numeric_values: vec![],
            extend_policy: 0,
            operator: Address::ZERO,
        });
        assert_eq!(tracker.live_entities().get(&key), Some(&2000));
    }

    #[test]
    fn extended_without_prior_create_still_tracked() {
        let mut tracker = LiveEntityTracker::new();
        let key = B256::repeat_byte(0x01);
        tracker.apply_event(&EntityEvent::Extended {
            entity_key: key,
            old_expires_at: 500,
            new_expires_at: 1500,
            owner: Address::ZERO,
        });
        assert_eq!(tracker.live_entities().get(&key), Some(&1500));
    }

    #[test]
    fn expired_entity_removed() {
        let mut tracker = LiveEntityTracker::new();
        let key = B256::repeat_byte(0x01);
        tracker.apply_event(&created(key, 1000));
        tracker.apply_event(&EntityEvent::Expired {
            entity_key: key,
            owner: Address::ZERO,
        });
        assert!(tracker.live_entities().is_empty());
    }

    #[test]
    fn save_load_checkpoint_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("expiration.bin");

        let mut index = ExpirationIndex::new();
        let key_a = B256::repeat_byte(0x01);
        let key_b = B256::repeat_byte(0x02);
        let key_c = B256::repeat_byte(0x03);
        index.insert(100, key_a);
        index.insert(100, key_b);
        index.insert(200, key_c);

        let tip_block = 500;
        let tip_hash = B256::repeat_byte(0xAB);

        save_checkpoint(&index, tip_block, &tip_hash, &path).unwrap();

        let (loaded_index, loaded_tip, loaded_hash) = load_checkpoint(&path).unwrap();
        assert_eq!(loaded_tip, tip_block);
        assert_eq!(loaded_hash, tip_hash);

        let expired_100 = loaded_index.get_expired(100).unwrap();
        assert!(expired_100.contains(&key_a));
        assert!(expired_100.contains(&key_b));
        assert_eq!(expired_100.len(), 2);

        let expired_200 = loaded_index.get_expired(200).unwrap();
        assert!(expired_200.contains(&key_c));
        assert_eq!(expired_200.len(), 1);
    }

    #[test]
    fn load_checkpoint_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.bin");
        assert!(load_checkpoint(&path).is_err());
    }

    #[test]
    fn load_checkpoint_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.bin");
        std::fs::write(&path, b"not a valid checkpoint").unwrap();
        assert!(load_checkpoint(&path).is_err());
    }
}

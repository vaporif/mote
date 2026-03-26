use std::collections::HashMap;

use alloy_eips::BlockHashOrNumber;
use alloy_primitives::B256;
use mote_primitives::config::MoteChainConfig;
use mote_primitives::constants::PROCESSOR_ADDRESS;
use mote_primitives::parse::{EntityEvent, parse_log};
use reth_provider::ReceiptProvider;
use tracing::{debug, info};

use crate::expiration::ExpirationIndex;

#[derive(Debug, Default)]
pub struct LiveEntityTracker {
    entities: HashMap<B256, u64>,
}

impl LiveEntityTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_event(&mut self, event: &EntityEvent) {
        match event {
            EntityEvent::Created {
                entity_key,
                expires_at,
                ..
            } => {
                self.entities.insert(*entity_key, *expires_at);
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
            }
            EntityEvent::Deleted { entity_key, .. } | EntityEvent::Expired { entity_key, .. } => {
                self.entities.remove(entity_key);
            }
        }
    }

    #[must_use]
    pub const fn live_entities(&self) -> &HashMap<B256, u64> {
        &self.entities
    }

    pub fn into_expiration_pairs(self) -> impl Iterator<Item = (B256, u64)> {
        self.entities.into_iter()
    }
}

pub fn rebuild_expiration_index<P>(
    provider: &P,
    config: &MoteChainConfig,
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
    let mut blocks_scanned: u64 = 0;

    for block_num in start_block..=tip_block {
        if let Some(receipts) = provider.receipts_by_block(BlockHashOrNumber::Number(block_num))? {
            let events = receipts
                .iter()
                .flat_map(alloy_consensus::TxReceipt::logs)
                .filter(|log| log.address == PROCESSOR_ADDRESS)
                .filter_map(|log| match parse_log(log) {
                    Ok(Some(event)) => Some(event),
                    Ok(None) => None,
                    Err(e) => {
                        debug!(block_num, ?e, "skipping unparseable log during recovery");
                        None
                    }
                });

            for event in events {
                tracker.apply_event(&event);
            }
        }

        blocks_scanned += 1;
        if total > 0 && blocks_scanned.is_multiple_of(10_000) {
            let pct = (blocks_scanned * 100) / total;
            info!("cold-start recovery: {blocks_scanned}/{total} blocks scanned ({pct}%)");
        }
    }

    let entity_count = tracker.live_entities().len();
    let mut index = ExpirationIndex::new();
    index.rebuild_from_logs(tracker.into_expiration_pairs());

    info!(entity_count, "expiration index rebuilt");
    Ok(index)
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
}

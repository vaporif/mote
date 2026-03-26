use alloy_primitives::{B256, keccak256};
use std::sync::LazyLock;

pub static USED_SLOTS_KEY: LazyLock<B256> = LazyLock::new(|| keccak256(b"glintUsedSlots"));
pub static ENTITY_COUNT_KEY: LazyLock<B256> = LazyLock::new(|| keccak256(b"glintEntityCount"));

/// Metadata + content hash.
pub const SLOTS_PER_ENTITY: u64 = 2;

#[must_use]
pub const fn slots_for_entity(has_operator: bool) -> u64 {
    if has_operator { 3 } else { SLOTS_PER_ENTITY }
}

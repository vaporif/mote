use alloy_primitives::{B256, keccak256};

pub fn used_slots_key() -> B256 {
    keccak256(b"moteUsedSlots")
}

/// Metadata + content hash.
pub const SLOTS_PER_ENTITY: u64 = 2;

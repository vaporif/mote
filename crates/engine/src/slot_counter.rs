use alloy_primitives::B256;

/// `keccak256(b"glintUsedSlots")`
pub const USED_SLOTS_KEY: B256 = B256::new([
    0x5a, 0xa3, 0x06, 0x74, 0x32, 0x9f, 0x14, 0x59, 0x79, 0x48, 0x2f, 0xf2, 0x77, 0x4e, 0xba, 0x47,
    0x84, 0xd3, 0x4f, 0x3c, 0xf7, 0x86, 0xf3, 0x0d, 0xed, 0x07, 0xcc, 0x87, 0x71, 0x35, 0xaf, 0x96,
]);

/// `keccak256(b"glintEntityCount")`
pub const ENTITY_COUNT_KEY: B256 = B256::new([
    0xa3, 0xbb, 0x8f, 0x8a, 0xc6, 0xdc, 0x17, 0x7e, 0x01, 0x50, 0x08, 0x8a, 0x43, 0xbc, 0xd7, 0x7e,
    0x0d, 0xd6, 0x9a, 0xb4, 0xd3, 0xd9, 0x6a, 0x11, 0x55, 0xa1, 0x19, 0x0d, 0x3a, 0xce, 0x65, 0x19,
]);

/// Metadata + content hash.
pub const SLOTS_PER_ENTITY: u64 = 2;

#[must_use]
pub const fn slots_for_entity(has_operator: bool) -> u64 {
    if has_operator { 3 } else { SLOTS_PER_ENTITY }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::keccak256;

    #[test]
    fn used_slots_key_matches_keccak() {
        assert_eq!(USED_SLOTS_KEY, keccak256(b"glintUsedSlots"));
    }

    #[test]
    fn entity_count_key_matches_keccak() {
        assert_eq!(ENTITY_COUNT_KEY, keccak256(b"glintEntityCount"));
    }

    #[test]
    fn entity_count_key_matches_primitives() {
        assert_eq!(
            ENTITY_COUNT_KEY,
            glint_primitives::storage::entity_count_key()
        );
    }
}

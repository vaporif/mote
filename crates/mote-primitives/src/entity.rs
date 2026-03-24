use alloy_primitives::{keccak256, Address, B256};

pub type EntityKey = B256;

/// Packed into 32 bytes in the trie (slot 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityMetadata {
    pub owner: Address,
    pub expires_at_block: u64,
}

impl EntityMetadata {
    /// `owner (20) || reserved (4) || expires_at_block (8)`
    pub fn encode(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..20].copy_from_slice(self.owner.as_slice());
        // bytes 20..24 reserved (zero)
        buf[24..32].copy_from_slice(&self.expires_at_block.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8; 32]) -> Self {
        let owner = Address::from_slice(&bytes[0..20]);
        let mut expire_bytes = [0u8; 8];
        expire_bytes.copy_from_slice(&bytes[24..32]);
        let expires_at_block = u64::from_be_bytes(expire_bytes);
        Self {
            owner,
            expires_at_block,
        }
    }
}

/// `keccak256(tx_hash || len(payload) as u32 || payload || left_pad_32(op_index))`
pub fn derive_entity_key(tx_hash: &B256, payload: &[u8], operation_index: u32) -> EntityKey {
    let mut preimage = Vec::with_capacity(32 + 4 + payload.len() + 32);
    preimage.extend_from_slice(tx_hash.as_slice());
    preimage.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("payload length exceeds u32")
            .to_be_bytes(),
    );
    preimage.extend_from_slice(payload);
    let mut padded_index = [0u8; 32];
    padded_index[28..32].copy_from_slice(&operation_index.to_be_bytes());
    preimage.extend_from_slice(&padded_index);
    keccak256(&preimage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_key_derivation_is_deterministic() {
        let tx_hash = B256::repeat_byte(0x01);
        let payload = b"hello world";
        let op_index: u32 = 0;

        let key1 = derive_entity_key(&tx_hash, payload, op_index);
        let key2 = derive_entity_key(&tx_hash, payload, op_index);
        assert_eq!(key1, key2);
    }

    #[test]
    fn entity_key_differs_by_op_index() {
        let tx_hash = B256::repeat_byte(0x01);
        let payload = b"hello world";

        let key0 = derive_entity_key(&tx_hash, payload, 0);
        let key1 = derive_entity_key(&tx_hash, payload, 1);
        assert_ne!(key0, key1);
    }

    #[test]
    fn entity_key_differs_by_payload() {
        let tx_hash = B256::repeat_byte(0x01);

        let key_a = derive_entity_key(&tx_hash, b"aaa", 0);
        let key_b = derive_entity_key(&tx_hash, b"bbb", 0);
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn entity_key_matches_spec_formula() {
        let tx_hash = B256::repeat_byte(0xAA);
        let payload = b"test";
        let op_index: u32 = 5;

        let mut preimage = Vec::new();
        preimage.extend_from_slice(tx_hash.as_slice());
        preimage.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("payload length exceeds u32")
                .to_be_bytes(),
        );
        preimage.extend_from_slice(payload);
        let mut padded_index = [0u8; 32];
        padded_index[28..32].copy_from_slice(&op_index.to_be_bytes());
        preimage.extend_from_slice(&padded_index);

        let expected = keccak256(&preimage);
        let actual = derive_entity_key(&tx_hash, payload, op_index);
        assert_eq!(actual, expected);
    }

    #[test]
    fn metadata_roundtrip() {
        let meta = EntityMetadata {
            owner: Address::repeat_byte(0x42),
            expires_at_block: 302_400,
        };
        let encoded = meta.encode();
        let decoded = EntityMetadata::decode(&encoded);
        assert_eq!(meta, decoded);
    }

    #[test]
    fn metadata_reserved_bytes_are_zero() {
        let meta = EntityMetadata {
            owner: Address::repeat_byte(0xFF),
            expires_at_block: u64::MAX,
        };
        let encoded = meta.encode();
        assert_eq!(&encoded[20..24], &[0, 0, 0, 0]);
    }
}

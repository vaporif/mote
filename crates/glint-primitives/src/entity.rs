use alloy_primitives::{Address, B256, keccak256};

use crate::transaction::ExtendPolicy;

pub type EntityKey = B256;

// Metadata layout: `owner (20) || flags (1) || reserved (3) || expires_at_block (8)` = 32 bytes
const OWNER_END: usize = 20;
const FLAGS_OFFSET: usize = 20;
const EXPIRATION_START: usize = 24;
const FLAG_ANYONE_CAN_EXTEND: u8 = 0b01;
const FLAG_HAS_OPERATOR: u8 = 0b10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityMetadata {
    pub owner: Address,
    pub expires_at_block: u64,
    pub extend_policy: ExtendPolicy,
    pub has_operator: bool,
}

impl EntityMetadata {
    pub fn encode(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[..OWNER_END].copy_from_slice(self.owner.as_slice());
        let mut flags: u8 = 0;
        if self.extend_policy == ExtendPolicy::AnyoneCanExtend {
            flags |= FLAG_ANYONE_CAN_EXTEND;
        }
        if self.has_operator {
            flags |= FLAG_HAS_OPERATOR;
        }
        buf[FLAGS_OFFSET] = flags;
        buf[EXPIRATION_START..].copy_from_slice(&self.expires_at_block.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8; 32]) -> Self {
        let owner = Address::from_slice(&bytes[..OWNER_END]);
        let flags = bytes[FLAGS_OFFSET];
        let extend_policy = if flags & FLAG_ANYONE_CAN_EXTEND != 0 {
            ExtendPolicy::AnyoneCanExtend
        } else {
            ExtendPolicy::OwnerOnly
        };
        let has_operator = flags & FLAG_HAS_OPERATOR != 0;
        let expires_at_block =
            u64::from_be_bytes(bytes[EXPIRATION_START..].try_into().expect("8 bytes"));
        Self {
            owner,
            expires_at_block,
            extend_policy,
            has_operator,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityInfo {
    pub owner: Address,
    pub expires_at_block: u64,
    pub extend_policy: ExtendPolicy,
    pub operator: Option<Address>,
    pub content_hash: B256,
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
            extend_policy: ExtendPolicy::OwnerOnly,
            has_operator: false,
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
            extend_policy: ExtendPolicy::OwnerOnly,
            has_operator: false,
        };
        let encoded = meta.encode();
        assert_eq!(&encoded[21..24], &[0, 0, 0]);
    }

    #[test]
    fn metadata_flags_roundtrip_anyone_can_extend() {
        let meta = EntityMetadata {
            owner: Address::repeat_byte(0x01),
            expires_at_block: 1000,
            extend_policy: ExtendPolicy::AnyoneCanExtend,
            has_operator: false,
        };
        let encoded = meta.encode();
        assert_eq!(encoded[20] & 0b01, 1);
        assert_eq!(encoded[20] & 0b10, 0);
        let decoded = EntityMetadata::decode(&encoded);
        assert_eq!(meta, decoded);
    }

    #[test]
    fn metadata_flags_roundtrip_has_operator() {
        let meta = EntityMetadata {
            owner: Address::repeat_byte(0x02),
            expires_at_block: 2000,
            extend_policy: ExtendPolicy::OwnerOnly,
            has_operator: true,
        };
        let encoded = meta.encode();
        assert_eq!(encoded[20] & 0b01, 0);
        assert_eq!(encoded[20] & 0b10, 2);
        let decoded = EntityMetadata::decode(&encoded);
        assert_eq!(meta, decoded);
    }

    #[test]
    fn metadata_flags_roundtrip_both() {
        let meta = EntityMetadata {
            owner: Address::repeat_byte(0x03),
            expires_at_block: 3000,
            extend_policy: ExtendPolicy::AnyoneCanExtend,
            has_operator: true,
        };
        let encoded = meta.encode();
        assert_eq!(encoded[20], 0b11);
        let decoded = EntityMetadata::decode(&encoded);
        assert_eq!(meta, decoded);
    }

    #[test]
    fn legacy_metadata_decodes_as_defaults() {
        let meta = EntityMetadata {
            owner: Address::repeat_byte(0x42),
            expires_at_block: 302_400,
            extend_policy: ExtendPolicy::OwnerOnly,
            has_operator: false,
        };
        let mut legacy = [0u8; 32];
        legacy[0..20].copy_from_slice(meta.owner.as_slice());
        legacy[24..32].copy_from_slice(&meta.expires_at_block.to_be_bytes());

        let decoded = EntityMetadata::decode(&legacy);
        assert_eq!(decoded.owner, meta.owner);
        assert_eq!(decoded.expires_at_block, meta.expires_at_block);
        assert_eq!(decoded.extend_policy, ExtendPolicy::OwnerOnly);
        assert!(!decoded.has_operator);
    }
}

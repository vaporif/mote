use alloy_primitives::{Address, B256, U256, keccak256};

const STORAGE_PREFIX: &[u8] = b"glintEntityMetaData";
const CONTENT_HASH_PREFIX: &[u8] = b"glintEntityContentHash";
const OPERATOR_PREFIX: &[u8] = b"glintOperator";

/// `keccak256("glintEntityMetaData" || entity_key)`
pub fn entity_storage_key(entity_key: &B256) -> B256 {
    let mut preimage = Vec::with_capacity(STORAGE_PREFIX.len() + 32);
    preimage.extend_from_slice(STORAGE_PREFIX);
    preimage.extend_from_slice(entity_key.as_slice());
    keccak256(&preimage)
}

/// `keccak256("glintEntityContentHash" || entity_key)`
pub fn entity_content_hash_key(entity_key: &B256) -> B256 {
    let mut preimage = Vec::with_capacity(CONTENT_HASH_PREFIX.len() + 32);
    preimage.extend_from_slice(CONTENT_HASH_PREFIX);
    preimage.extend_from_slice(entity_key.as_slice());
    keccak256(&preimage)
}

#[must_use]
pub fn entity_count_key() -> B256 {
    keccak256(b"glintEntityCount")
}

/// `keccak256("glintOperator" || entity_key)`
pub fn entity_operator_key(entity_key: &B256) -> B256 {
    let mut preimage = Vec::with_capacity(OPERATOR_PREFIX.len() + 32);
    preimage.extend_from_slice(OPERATOR_PREFIX);
    preimage.extend_from_slice(entity_key.as_slice());
    keccak256(&preimage)
}

const ADDRESS_LEN: usize = 20;

/// Left-aligned: `address (20) || zero-pad (12)`.
#[must_use]
pub fn encode_operator_value(addr: Address) -> U256 {
    let mut bytes = [0u8; 32];
    bytes[..ADDRESS_LEN].copy_from_slice(addr.as_slice());
    U256::from_be_bytes(bytes)
}

/// Inverse of [`encode_operator_value`].
#[must_use]
pub fn decode_operator_value(value: U256) -> Address {
    let bytes: [u8; 32] = value.to_be_bytes();
    Address::from_slice(&bytes[..ADDRESS_LEN])
}

/// Compute the on-chain content hash from raw RLP field slices.
///
/// Formula: `keccak256(len(f1) || f1 || len(f2) || f2 || len(f3) || f3 || len(f4) || f4)`
///
/// where `len(f)` is the field's byte length as a big-endian `u32`, and
/// `f1..f4` are the raw RLP encodings of `payload`, `content_type`,
/// `string_annotations`, and `numeric_annotations` respectively.
///
/// Length-prefixing each field provides domain separation — the hash is
/// unambiguous even if the raw slices are not perfectly canonical RLP.
pub fn compute_content_hash_from_raw(
    payload_rlp: &[u8],
    content_type_rlp: &[u8],
    string_annotations_rlp: &[u8],
    numeric_annotations_rlp: &[u8],
) -> B256 {
    let mut preimage = Vec::with_capacity(
        4 * 4
            + payload_rlp.len()
            + content_type_rlp.len()
            + string_annotations_rlp.len()
            + numeric_annotations_rlp.len(),
    );
    // Field lengths are bounded by MAX_PAYLOAD_SIZE (128 KB), well within u32.
    #[allow(clippy::cast_possible_truncation)]
    for field in [
        payload_rlp,
        content_type_rlp,
        string_annotations_rlp,
        numeric_annotations_rlp,
    ] {
        preimage.extend_from_slice(&(field.len() as u32).to_be_bytes());
        preimage.extend_from_slice(field);
    }
    keccak256(&preimage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_key_is_deterministic() {
        let entity_key = B256::repeat_byte(0x42);
        let key1 = entity_storage_key(&entity_key);
        let key2 = entity_storage_key(&entity_key);
        assert_eq!(key1, key2);
    }

    #[test]
    fn storage_key_matches_spec() {
        let entity_key = B256::repeat_byte(0x01);
        let mut preimage = Vec::new();
        preimage.extend_from_slice(b"glintEntityMetaData");
        preimage.extend_from_slice(entity_key.as_slice());
        let expected = keccak256(&preimage);
        assert_eq!(entity_storage_key(&entity_key), expected);
    }

    #[test]
    fn content_hash_from_raw_bytes() {
        let payload_rlp = &[0x85, b'h', b'e', b'l', b'l', b'o'];
        let content_type_rlp = &[
            0x8a, b't', b'e', b'x', b't', b'/', b'p', b'l', b'a', b'i', b'n',
        ];
        let string_annotations_rlp = &[0xc0];
        let numeric_annotations_rlp = &[0xc0];

        let hash = compute_content_hash_from_raw(
            payload_rlp,
            content_type_rlp,
            string_annotations_rlp,
            numeric_annotations_rlp,
        );

        let mut expected_preimage = Vec::new();
        for field in [
            payload_rlp.as_slice(),
            content_type_rlp.as_slice(),
            string_annotations_rlp.as_slice(),
            numeric_annotations_rlp.as_slice(),
        ] {
            #[allow(clippy::cast_possible_truncation)]
            expected_preimage.extend_from_slice(&(field.len() as u32).to_be_bytes());
            expected_preimage.extend_from_slice(field);
        }
        assert_eq!(hash, keccak256(&expected_preimage));
    }

    #[test]
    fn operator_key_is_deterministic() {
        let entity_key = B256::repeat_byte(0x42);
        let key1 = entity_operator_key(&entity_key);
        let key2 = entity_operator_key(&entity_key);
        assert_eq!(key1, key2);
    }

    #[test]
    fn operator_key_matches_spec_preimage() {
        let entity_key = B256::repeat_byte(0x01);
        let mut preimage = Vec::new();
        preimage.extend_from_slice(b"glintOperator");
        preimage.extend_from_slice(entity_key.as_slice());
        let expected = keccak256(&preimage);
        assert_eq!(entity_operator_key(&entity_key), expected);
    }

    #[test]
    fn operator_value_roundtrip() {
        use alloy_primitives::Address;

        let addr = Address::repeat_byte(0x42);
        let encoded = super::encode_operator_value(addr);
        let decoded = super::decode_operator_value(encoded);
        assert_eq!(decoded, addr);
    }

    #[test]
    fn operator_value_zero_is_zero_address() {
        let decoded = super::decode_operator_value(U256::ZERO);
        assert_eq!(decoded, alloy_primitives::Address::ZERO);
    }

    #[test]
    fn operator_key_differs_from_storage_key() {
        let entity_key = B256::repeat_byte(0x01);
        assert_ne!(
            entity_operator_key(&entity_key),
            entity_storage_key(&entity_key)
        );
    }

    #[test]
    fn content_hash_key_matches_spec_preimage() {
        let entity_key = B256::repeat_byte(0x01);
        let mut preimage = Vec::new();
        preimage.extend_from_slice(b"glintEntityContentHash");
        preimage.extend_from_slice(entity_key.as_slice());
        let expected = keccak256(&preimage);
        assert_eq!(entity_content_hash_key(&entity_key), expected);
    }

    #[test]
    fn content_hash_key_differs_from_meta_and_operator() {
        let entity_key = B256::repeat_byte(0x01);
        let meta = entity_storage_key(&entity_key);
        let content = entity_content_hash_key(&entity_key);
        let op = entity_operator_key(&entity_key);
        assert_ne!(meta, content);
        assert_ne!(meta, op);
        assert_ne!(content, op);
    }
}

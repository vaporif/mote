use alloy_primitives::B256;
use borsh::{BorshDeserialize, BorshSerialize};

const BLAKE3_HASH_LEN: usize = 32;

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct ExpirationCheckpoint {
    pub version: u8,
    pub tip_block: u64,
    pub tip_block_hash: B256,
    pub entries: Vec<(u64, Vec<B256>)>,
}

impl ExpirationCheckpoint {
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let borsh_bytes = borsh::to_vec(self).expect("borsh serialization cannot fail");
        let hash = blake3::hash(&borsh_bytes);
        let mut out = Vec::with_capacity(borsh_bytes.len() + BLAKE3_HASH_LEN);
        out.extend_from_slice(&borsh_bytes);
        out.extend_from_slice(hash.as_bytes());
        out
    }

    pub fn deserialize(data: &[u8]) -> eyre::Result<Self> {
        eyre::ensure!(data.len() > BLAKE3_HASH_LEN, "checkpoint file too short");
        let (borsh_bytes, hash_bytes) = data.split_at(data.len() - BLAKE3_HASH_LEN);
        let computed = blake3::hash(borsh_bytes);
        eyre::ensure!(
            computed.as_bytes() == hash_bytes,
            "checkpoint integrity check failed"
        );
        let checkpoint: Self = borsh::from_slice(borsh_bytes)?;
        eyre::ensure!(checkpoint.version == 1, "unsupported checkpoint version");
        Ok(checkpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn roundtrip_serialize_deserialize() {
        let checkpoint = ExpirationCheckpoint {
            version: 1,
            tip_block: 1000,
            tip_block_hash: B256::repeat_byte(0xAB),
            entries: vec![
                (100, vec![B256::repeat_byte(0x01), B256::repeat_byte(0x02)]),
                (200, vec![B256::repeat_byte(0x03)]),
            ],
        };
        let bytes = checkpoint.serialize();
        let loaded = ExpirationCheckpoint::deserialize(&bytes).unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.tip_block, 1000);
        assert_eq!(loaded.tip_block_hash, B256::repeat_byte(0xAB));
        assert_eq!(loaded.entries.len(), 2);
    }

    #[test]
    fn corrupt_checkpoint_rejected() {
        let checkpoint = ExpirationCheckpoint {
            version: 1,
            tip_block: 100,
            tip_block_hash: B256::ZERO,
            entries: vec![],
        };
        let mut bytes = checkpoint.serialize();
        let len = bytes.len();
        bytes[len - 1] ^= 0xFF;
        assert!(ExpirationCheckpoint::deserialize(&bytes).is_err());
    }
}

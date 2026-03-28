use alloy_primitives::{Address, address};

/// ASCII "glint" right-aligned. Spec v0.1 has a typo (21 bytes) — we drop
/// the leading zero byte so it fits 20.
pub const PROCESSOR_ADDRESS: Address = address!("000000000000000000000000000000676c696e74");

/// ~1 week at 2s blocks.
pub const MAX_BTL: u64 = 302_400;

pub const MAX_OPS_PER_TX: usize = 100;
/// 128 KB.
pub const MAX_PAYLOAD_SIZE: usize = 131_072;
pub const MAX_CONTENT_TYPE_SIZE: usize = 128;
/// String + numeric combined.
pub const MAX_ANNOTATIONS_PER_ENTITY: usize = 64;
pub const MAX_ANNOTATION_KEY_SIZE: usize = 256;
/// String annotations only.
pub const MAX_ANNOTATION_VALUE_SIZE: usize = 1024;

/// metadata slot + content-hash slot + operator slot (separate keccak key, not contiguous).
pub const SLOTS_PER_ENTITY_WITH_OPERATOR: u64 = 3;

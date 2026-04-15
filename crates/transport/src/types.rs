use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct HandshakeInfo {
    pub oldest_block: u64,
    pub tip_block: u64,
}

/// Live stats returned by IPC probe. Atomics are shared with the ring buffer.
#[derive(Clone, Default)]
pub struct ProbeState {
    pub consumer_connected: Arc<AtomicBool>,
    pub tip_block: Arc<AtomicU64>,
    pub oldest_block: Arc<AtomicU64>,
    pub ring_buffer_entries: Arc<AtomicU64>,
    pub ring_buffer_memory_bytes: Arc<AtomicU64>,
}

impl ProbeState {
    pub fn snapshot(&self) -> ProbeSnapshot {
        ProbeSnapshot {
            consumer_connected: self.consumer_connected.load(Ordering::Acquire),
            tip_block: self.tip_block.load(Ordering::Relaxed),
            oldest_block: self.oldest_block.load(Ordering::Relaxed),
            ring_buffer_entries: self.ring_buffer_entries.load(Ordering::Relaxed),
            ring_buffer_memory_bytes: self.ring_buffer_memory_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub struct ProbeSnapshot {
    pub consumer_connected: bool,
    pub tip_block: u64,
    pub oldest_block: u64,
    pub ring_buffer_entries: u64,
    pub ring_buffer_memory_bytes: u64,
}

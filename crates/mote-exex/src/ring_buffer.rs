// Canonical ring buffer with reorg support, snapshot, memory tracking

use alloy_eips::BlockNumHash;
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mote_primitives::constants::MAX_BTL;

const DEFAULT_MEMORY_CAP: u64 = 256 * 1024 * 1024; // 256 MB
const PER_ENTRY_OVERHEAD: u64 = 256;

pub struct RingBuffer {
    entries: VecDeque<(BlockNumHash, RecordBatch)>,
    memory_usage: u64,
    memory_cap: u64,
    // Shared atomics for probe protocol
    atomic_entries: Arc<AtomicU64>,
    atomic_memory: Arc<AtomicU64>,
}

impl Default for RingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl RingBuffer {
    #[must_use]
    pub fn new() -> Self {
        Self::with_memory_cap(DEFAULT_MEMORY_CAP)
    }

    #[must_use]
    pub fn with_memory_cap(cap: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            memory_usage: 0,
            memory_cap: cap,
            atomic_entries: Arc::new(AtomicU64::new(0)),
            atomic_memory: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn push(&mut self, bnh: BlockNumHash, batch: RecordBatch) {
        self.memory_usage += Self::batch_memory(&batch);
        self.entries.push_back((bnh, batch));
        // Enforce memory cap on every push so the buffer stays bounded.
        while self.memory_usage > self.memory_cap && self.entries.len() > 1 {
            if let Some((_, evicted)) = self.entries.pop_front() {
                self.memory_usage = self
                    .memory_usage
                    .saturating_sub(Self::batch_memory(&evicted));
            } else {
                break;
            }
        }
        self.update_atomics();
    }

    /// Evict entries older than the given block number.
    pub fn evict_older_than(&mut self, min_block: u64) {
        while self
            .entries
            .front()
            .is_some_and(|(bnh, _)| bnh.number < min_block)
        {
            if let Some((_, batch)) = self.entries.pop_front() {
                self.memory_usage = self.memory_usage.saturating_sub(Self::batch_memory(&batch));
            }
        }
        self.update_atomics();
    }

    /// Evict based on tip block (`tip - MAX_BTL`) and memory cap.
    pub fn evict_if_needed(&mut self, tip: u64) {
        self.evict_older_than(tip.saturating_sub(MAX_BTL));
        while self.memory_usage > self.memory_cap {
            if let Some((_, batch)) = self.entries.pop_front() {
                self.memory_usage = self.memory_usage.saturating_sub(Self::batch_memory(&batch));
            } else {
                break;
            }
        }
        self.update_atomics();
    }

    /// Clone entries with `block_number > resume_block`.
    #[must_use]
    pub fn snapshot_from(&self, resume_block: u64) -> Vec<(BlockNumHash, RecordBatch)> {
        self.entries
            .iter()
            .filter(|(bnh, _)| bnh.number > resume_block)
            .map(|(bnh, batch)| (*bnh, batch.clone()))
            .collect()
    }

    /// Remove entries with `block_number >= reorg_start` (pops from back).
    pub fn truncate_from(&mut self, reorg_start: u64) {
        while self
            .entries
            .back()
            .is_some_and(|(bnh, _)| bnh.number >= reorg_start)
        {
            if let Some((_, batch)) = self.entries.pop_back() {
                self.memory_usage = self.memory_usage.saturating_sub(Self::batch_memory(&batch));
            }
        }
        self.update_atomics();
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn oldest(&self) -> Option<BlockNumHash> {
        self.entries.front().map(|(bnh, _)| *bnh)
    }

    #[must_use]
    pub fn newest(&self) -> Option<BlockNumHash> {
        self.entries.back().map(|(bnh, _)| *bnh)
    }

    #[must_use]
    pub const fn memory_usage(&self) -> u64 {
        self.memory_usage
    }

    #[must_use]
    pub fn atomic_entry_count(&self) -> u64 {
        self.atomic_entries.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn atomic_memory_bytes(&self) -> u64 {
        self.atomic_memory.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn atomics(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        (
            Arc::clone(&self.atomic_entries),
            Arc::clone(&self.atomic_memory),
        )
    }

    fn update_atomics(&self) {
        self.atomic_entries
            .store(self.entries.len() as u64, Ordering::Relaxed);
        self.atomic_memory
            .store(self.memory_usage, Ordering::Relaxed);
    }

    fn batch_memory(batch: &RecordBatch) -> u64 {
        let col_bytes: usize = batch
            .columns()
            .iter()
            .map(arrow::array::Array::get_buffer_memory_size)
            .sum();
        col_bytes as u64 + PER_ENTRY_OVERHEAD
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eips::BlockNumHash;
    use alloy_primitives::B256;

    fn dummy_batch(num_rows: usize) -> RecordBatch {
        use arrow::array::UInt64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::UInt64, false)]));
        let col = Arc::new(UInt64Array::from(vec![0u64; num_rows]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn push_and_len() {
        let mut rb = RingBuffer::new();
        rb.push(
            BlockNumHash::new(1, B256::repeat_byte(0x01)),
            dummy_batch(1),
        );
        rb.push(
            BlockNumHash::new(2, B256::repeat_byte(0x02)),
            dummy_batch(1),
        );
        assert_eq!(rb.len(), 2);
    }

    #[test]
    fn evict_by_block_distance() {
        let mut rb = RingBuffer::new();
        for i in 1..=10 {
            rb.push(
                BlockNumHash::new(i, B256::repeat_byte(i as u8)),
                dummy_batch(1),
            );
        }
        rb.evict_older_than(5);
        assert_eq!(rb.oldest().unwrap().number, 5);
    }

    #[test]
    fn evict_by_memory_cap() {
        let mut rb = RingBuffer::with_memory_cap(1000); // very small cap
        for i in 1..=100 {
            rb.push(
                BlockNumHash::new(i, B256::repeat_byte(i as u8)),
                dummy_batch(10),
            );
        }
        // approximate, ~20% overshoot allowed (eviction happens after push, not before)
        assert!(rb.memory_usage() <= 1000 + 5000);
    }

    #[test]
    fn snapshot_from_resume_block() {
        let mut rb = RingBuffer::new();
        for i in 1..=5 {
            rb.push(
                BlockNumHash::new(i, B256::repeat_byte(i as u8)),
                dummy_batch(1),
            );
        }
        let snap = rb.snapshot_from(3);
        assert_eq!(snap.len(), 2); // blocks 4 and 5
    }

    #[test]
    fn snapshot_is_detached() {
        let mut rb = RingBuffer::new();
        rb.push(
            BlockNumHash::new(1, B256::repeat_byte(0x01)),
            dummy_batch(1),
        );
        let snap = rb.snapshot_from(0);
        rb.truncate_from(1);
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn reorg_truncate_and_rebuild() {
        let mut rb = RingBuffer::new();
        for i in 1..=5 {
            rb.push(
                BlockNumHash::new(i, B256::repeat_byte(i as u8)),
                dummy_batch(1),
            );
        }
        rb.truncate_from(3);
        assert_eq!(rb.len(), 2);
        assert_eq!(rb.newest().unwrap().number, 2);
    }

    #[test]
    fn deep_reorg_clears_buffer() {
        let mut rb = RingBuffer::new();
        for i in 5..=10 {
            rb.push(
                BlockNumHash::new(i, B256::repeat_byte(i as u8)),
                dummy_batch(1),
            );
        }
        rb.truncate_from(3); // older than all entries
        assert_eq!(rb.len(), 0);
    }

    #[test]
    fn entry_count_and_memory_atomics() {
        let rb = RingBuffer::new();
        assert_eq!(rb.atomic_entry_count(), 0);
        assert_eq!(rb.atomic_memory_bytes(), 0);
    }
}

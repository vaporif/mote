use alloy_eips::BlockNumHash;
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use glint_primitives::constants::MAX_BTL;

const DEFAULT_MEMORY_CAP: u64 = 256 * 1024 * 1024; // 256 MB
const PER_ENTRY_OVERHEAD: u64 = 256;

#[derive(Clone)]
pub struct RingBufferStats {
    pub entries: Arc<AtomicU64>,
    pub memory: Arc<AtomicU64>,
    pub tip: Arc<AtomicU64>,
    pub oldest: Arc<AtomicU64>,
}

impl RingBufferStats {
    fn new() -> Self {
        Self {
            entries: Arc::new(AtomicU64::new(0)),
            memory: Arc::new(AtomicU64::new(0)),
            tip: Arc::new(AtomicU64::new(0)),
            oldest: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub struct RingBuffer {
    entries: VecDeque<(BlockNumHash, RecordBatch)>,
    memory_usage: u64,
    memory_cap: u64,
    stats: RingBufferStats,
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
            stats: RingBufferStats::new(),
        }
    }

    pub fn push(&mut self, bnh: BlockNumHash, batch: RecordBatch) {
        self.memory_usage += Self::batch_memory(&batch);
        self.entries.push_back((bnh, batch));
        while self.memory_usage > self.memory_cap && self.entries.len() > 1 {
            if let Some((_, evicted)) = self.entries.pop_front() {
                self.memory_usage = self
                    .memory_usage
                    .saturating_sub(Self::batch_memory(&evicted));
            }
        }
        self.update_atomics();
    }

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

    #[must_use]
    pub fn snapshot_from(&self, resume_block: u64) -> Vec<(BlockNumHash, RecordBatch)> {
        self.entries
            .iter()
            .filter(|(bnh, _)| bnh.number > resume_block)
            .map(|(bnh, batch)| (*bnh, batch.clone()))
            .collect()
    }

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
    pub fn first_at_or_after(&self, min_block: u64) -> Option<BlockNumHash> {
        self.entries
            .iter()
            .find(|(bnh, _)| bnh.number >= min_block)
            .map(|(bnh, _)| *bnh)
    }

    #[must_use]
    pub const fn memory_usage(&self) -> u64 {
        self.memory_usage
    }

    #[must_use]
    pub fn stats(&self) -> RingBufferStats {
        self.stats.clone()
    }

    fn update_atomics(&self) {
        self.stats
            .entries
            .store(self.entries.len() as u64, Ordering::Relaxed);
        self.stats
            .memory
            .store(self.memory_usage, Ordering::Relaxed);

        let tip = self.entries.back().map_or(0, |(bnh, _)| bnh.number);
        let oldest = self.entries.front().map_or(0, |(bnh, _)| bnh.number);
        self.stats.tip.store(tip, Ordering::Relaxed);
        self.stats.oldest.store(oldest, Ordering::Relaxed);
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
        for i in 1..=10_u8 {
            rb.push(
                BlockNumHash::new(i.into(), B256::repeat_byte(i)),
                dummy_batch(1),
            );
        }
        rb.evict_older_than(5);
        assert_eq!(rb.oldest().unwrap().number, 5);
    }

    #[test]
    fn evict_by_memory_cap() {
        let mut rb = RingBuffer::with_memory_cap(1000); // very small cap
        for i in 1..=100_u8 {
            rb.push(
                BlockNumHash::new(i.into(), B256::repeat_byte(i)),
                dummy_batch(10),
            );
        }
        assert!(rb.memory_usage() <= 1000 + 5000);
    }

    #[test]
    fn snapshot_from_resume_block() {
        let mut rb = RingBuffer::new();
        for i in 1..=5_u8 {
            rb.push(
                BlockNumHash::new(i.into(), B256::repeat_byte(i)),
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
        for i in 1..=5_u8 {
            rb.push(
                BlockNumHash::new(i.into(), B256::repeat_byte(i)),
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
        for i in 5..=10_u8 {
            rb.push(
                BlockNumHash::new(i.into(), B256::repeat_byte(i)),
                dummy_batch(1),
            );
        }
        rb.truncate_from(3); // older than all entries
        assert_eq!(rb.len(), 0);
    }

    #[test]
    fn stats_atomics_update() {
        let mut rb = RingBuffer::new();
        let stats = rb.stats();
        assert_eq!(stats.entries.load(Ordering::Relaxed), 0);

        rb.push(
            BlockNumHash::new(5, B256::repeat_byte(0x05)),
            dummy_batch(1),
        );
        assert_eq!(stats.entries.load(Ordering::Relaxed), 1);
        assert_eq!(stats.tip.load(Ordering::Relaxed), 5);
        assert_eq!(stats.oldest.load(Ordering::Relaxed), 5);
    }
}

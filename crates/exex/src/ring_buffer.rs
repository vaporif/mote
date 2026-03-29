use alloy_eips::BlockNumHash;
use arrow::record_batch::RecordBatch;
use glint_primitives::exex_types::BatchOp;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_MEMORY_CAP: u64 = 256 * 1024 * 1024; // 256 MB
const PER_ENTRY_OVERHEAD: u64 = 256;

#[derive(Clone)]
pub struct RingBufferEntry {
    pub bnh: BlockNumHash,
    pub op: BatchOp,
    pub batch: RecordBatch,
}

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
    entries: VecDeque<RingBufferEntry>,
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

    pub fn push(&mut self, bnh: BlockNumHash, op: BatchOp, batch: RecordBatch) {
        self.memory_usage += Self::batch_memory(&batch);
        self.entries.push_back(RingBufferEntry { bnh, op, batch });
        while self.memory_usage > self.memory_cap && self.entries.len() > 1 {
            if let Some(evicted) = self.entries.pop_front() {
                self.memory_usage = self
                    .memory_usage
                    .saturating_sub(Self::batch_memory(&evicted.batch));
            }
        }
        self.update_atomics();
    }

    /// Drop entries from the front until we're under the memory cap.
    /// We can't evict by block number — a revert for block 5 might have
    /// been appended moments ago.
    pub fn evict_if_needed(&mut self) {
        while self.memory_usage > self.memory_cap {
            if let Some(evicted) = self.entries.pop_front() {
                self.memory_usage = self
                    .memory_usage
                    .saturating_sub(Self::batch_memory(&evicted.batch));
            } else {
                break;
            }
        }
        self.update_atomics();
    }

    /// Everything the consumer missed since `resume_block`.
    /// Finds the first commit matching that block and returns the tail after it.
    /// Falls back to the whole buffer when the block is 0 or already evicted.
    #[must_use]
    pub fn snapshot_from(&self, resume_block: u64) -> Vec<(BlockNumHash, RecordBatch)> {
        let start = if resume_block == 0 {
            0
        } else {
            self.entries
                .iter()
                .position(|e| e.op == BatchOp::Commit && e.bnh.number == resume_block)
                .map_or(0, |pos| pos + 1)
        };

        self.entries
            .iter()
            .skip(start)
            .map(|e| (e.bnh, e.batch.clone()))
            .collect()
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
        self.entries.front().map(|e| e.bnh)
    }

    #[must_use]
    pub fn newest(&self) -> Option<BlockNumHash> {
        self.entries.back().map(|e| e.bnh)
    }

    #[must_use]
    pub fn first_at_or_after(&self, min_block: u64) -> Option<BlockNumHash> {
        self.entries
            .iter()
            .find(|e| e.bnh.number >= min_block)
            .map(|e| e.bnh)
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

        let tip = self.entries.back().map_or(0, |e| e.bnh.number);
        let oldest = self.entries.front().map_or(0, |e| e.bnh.number);
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

    fn bnh(n: u64) -> BlockNumHash {
        BlockNumHash::new(n, B256::repeat_byte(n as u8))
    }

    #[test]
    fn push_and_len() {
        let mut rb = RingBuffer::new();
        rb.push(bnh(1), BatchOp::Commit, dummy_batch(1));
        rb.push(bnh(2), BatchOp::Commit, dummy_batch(1));
        assert_eq!(rb.len(), 2);
    }

    #[test]
    fn evict_by_memory_cap() {
        let mut rb = RingBuffer::with_memory_cap(1000);
        for i in 1..=100_u64 {
            rb.push(bnh(i), BatchOp::Commit, dummy_batch(10));
        }
        assert!(rb.memory_usage() <= 1000 + 5000);
    }

    #[test]
    fn snapshot_from_resume_block() {
        let mut rb = RingBuffer::new();
        for i in 1..=5_u64 {
            rb.push(bnh(i), BatchOp::Commit, dummy_batch(1));
        }
        let snap = rb.snapshot_from(3);
        assert_eq!(snap.len(), 2); // blocks 4 and 5
    }

    #[test]
    fn snapshot_from_zero_returns_everything() {
        let mut rb = RingBuffer::new();
        for i in 1..=3_u64 {
            rb.push(bnh(i), BatchOp::Commit, dummy_batch(1));
        }
        let snap = rb.snapshot_from(0);
        assert_eq!(snap.len(), 3);
    }

    #[test]
    fn snapshot_from_missing_block_returns_everything() {
        let mut rb = RingBuffer::new();
        for i in 5..=8_u64 {
            rb.push(bnh(i), BatchOp::Commit, dummy_batch(1));
        }
        // resume_block=2 is not in the buffer
        let snap = rb.snapshot_from(2);
        assert_eq!(snap.len(), 4);
    }

    #[test]
    fn snapshot_includes_reverts_after_resume_point() {
        let mut rb = RingBuffer::new();
        // commit 8, 9, 10 then revert 10, commit 10'
        rb.push(bnh(8), BatchOp::Commit, dummy_batch(1));
        rb.push(bnh(9), BatchOp::Commit, dummy_batch(1));
        rb.push(bnh(10), BatchOp::Commit, dummy_batch(1));
        rb.push(bnh(10), BatchOp::Revert, dummy_batch(1));
        rb.push(
            BlockNumHash::new(10, B256::repeat_byte(0xAA)),
            BatchOp::Commit,
            dummy_batch(1),
        );

        // Consumer at block 9 — should get: commit(10), revert(10), commit(10')
        let snap = rb.snapshot_from(9);
        assert_eq!(snap.len(), 3);
    }

    #[test]
    fn snapshot_from_reverted_block_includes_revert() {
        let mut rb = RingBuffer::new();
        rb.push(bnh(8), BatchOp::Commit, dummy_batch(1));
        rb.push(bnh(9), BatchOp::Commit, dummy_batch(1));
        rb.push(bnh(10), BatchOp::Commit, dummy_batch(1));
        rb.push(bnh(10), BatchOp::Revert, dummy_batch(1));
        rb.push(bnh(9), BatchOp::Revert, dummy_batch(1));
        rb.push(
            BlockNumHash::new(9, B256::repeat_byte(0xBB)),
            BatchOp::Commit,
            dummy_batch(1),
        );
        rb.push(
            BlockNumHash::new(10, B256::repeat_byte(0xCC)),
            BatchOp::Commit,
            dummy_batch(1),
        );

        // Consumer had block 10 (original). Finds first commit(10) at pos 2.
        // Returns: revert(10), revert(9), commit(9'), commit(10')
        let snap = rb.snapshot_from(10);
        assert_eq!(snap.len(), 4);
    }

    #[test]
    fn snapshot_multi_reorg_correct_order() {
        let mut rb = RingBuffer::new();
        // chain: 5, 6, 7 → revert 7,6,5 → 5', 6', 7'
        for i in 5..=7_u64 {
            rb.push(bnh(i), BatchOp::Commit, dummy_batch(1));
        }
        for i in (5..=7_u64).rev() {
            rb.push(bnh(i), BatchOp::Revert, dummy_batch(1));
        }
        for i in 5..=7_u64 {
            rb.push(
                BlockNumHash::new(i, B256::repeat_byte(0xF0 + i as u8)),
                BatchOp::Commit,
                dummy_batch(1),
            );
        }

        // Consumer at block 4 — gets all 9 entries
        let snap = rb.snapshot_from(4);
        assert_eq!(snap.len(), 9);

        // Consumer at block 7 — finds first commit(7) at pos 2,
        // gets: revert(7), revert(6), revert(5), commit(5'), commit(6'), commit(7')
        let snap = rb.snapshot_from(7);
        assert_eq!(snap.len(), 6);
    }

    #[test]
    fn evict_if_needed_respects_memory_cap() {
        let mut rb = RingBuffer::with_memory_cap(1000);
        for i in 1..=50_u64 {
            rb.push(bnh(i), BatchOp::Commit, dummy_batch(10));
        }
        rb.evict_if_needed();
        assert!(rb.memory_usage() <= 1000);
    }

    #[test]
    fn stats_atomics_update() {
        let mut rb = RingBuffer::new();
        let stats = rb.stats();
        assert_eq!(stats.entries.load(Ordering::Relaxed), 0);

        rb.push(bnh(5), BatchOp::Commit, dummy_batch(1));
        assert_eq!(stats.entries.load(Ordering::Relaxed), 1);
        assert_eq!(stats.tip.load(Ordering::Relaxed), 5);
        assert_eq!(stats.oldest.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn snapshot_is_detached() {
        let mut rb = RingBuffer::new();
        rb.push(bnh(1), BatchOp::Commit, dummy_batch(1));
        let snap = rb.snapshot_from(0);
        // Evict everything
        rb.push(bnh(2), BatchOp::Commit, dummy_batch(1));
        assert_eq!(snap.len(), 1);
    }
}

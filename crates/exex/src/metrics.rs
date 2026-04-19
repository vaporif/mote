use reth_metrics::{
    metrics::{Counter, Gauge},
    Metrics,
};

#[derive(Metrics, Clone)]
#[metrics(scope = "glint_exex")]
pub struct ExExMetrics {
    /// Current ring buffer entry count
    pub ring_buffer_entries: Gauge,
    /// Current ring buffer memory usage in bytes
    pub ring_buffer_memory_bytes: Gauge,
    /// Newest block in ring buffer
    pub ring_buffer_tip_block: Gauge,
    /// Oldest block in ring buffer
    pub ring_buffer_oldest_block: Gauge,
    /// Batches delivered to consumer
    pub batches_sent_total: Counter,
    /// Batches dropped due to backpressure
    pub batches_dropped_total: Counter,
    /// 1 if consumer connected, 0 otherwise
    pub consumer_connected: Gauge,
    /// Batches sent during replay
    pub replay_batches_total: Counter,
    /// Consumer disconnected due to sustained backpressure
    pub consumer_disconnects_total: Counter,
}

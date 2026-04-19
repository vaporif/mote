use reth_metrics::{
    Metrics,
    metrics::{Counter, Histogram},
};

#[derive(Metrics, Clone)]
#[metrics(scope = "glint_engine")]
pub(super) struct EngineMetrics {
    /// Entity create operations
    pub(super) entities_created_total: Counter,
    /// Entity update operations
    pub(super) entities_updated_total: Counter,
    /// Entity delete operations
    pub(super) entities_deleted_total: Counter,
    /// Entity extend operations
    pub(super) entities_extended_total: Counter,
    /// Entity expired during housekeeping
    pub(super) entities_expired_total: Counter,
    /// Ownership transfer operations
    pub(super) owner_changes_total: Counter,
    /// Expired entities drained per block
    pub(super) expired_entities_per_block: Histogram,
    /// Total glint-specific gas consumed (includes intrinsic gas)
    pub(super) gas_consumed_total: Counter,
    /// Glint operations per transaction
    pub(super) ops_per_tx: Histogram,
}

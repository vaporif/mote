use reth_metrics::{Metrics, metrics::Gauge};

#[derive(Metrics, Clone)]
#[metrics(scope = "glint_sidecar")]
pub struct EntityStoreMetrics {
    /// Current live entity count
    pub entities_count: Gauge,
}

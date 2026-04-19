use reth_metrics::{Metrics, metrics::Counter};

#[derive(Metrics, Clone)]
#[metrics(scope = "glint_sidecar")]
pub struct HistoricalMetrics {
    /// Total historical events inserted
    pub events_stored_total: Counter,
}

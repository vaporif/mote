use reth_metrics::{
    metrics::{Counter, Gauge, Histogram},
    Metrics,
};

#[derive(Metrics, Clone)]
#[metrics(scope = "glint_sidecar")]
pub struct FlightSqlMetrics {
    /// Total Flight SQL queries served
    pub queries_total: Counter,
    /// Flight SQL queries that returned an error
    pub query_errors_total: Counter,
    /// Query execution time in seconds
    pub query_duration_seconds: Histogram,
    /// Rows returned per query
    pub query_rows_returned: Histogram,
}

#[derive(Metrics, Clone)]
#[metrics(scope = "glint_sidecar")]
pub struct SidecarMetrics {
    /// Time to write a snapshot in seconds
    pub snapshot_duration_seconds: Histogram,
    /// Block number of latest snapshot
    pub snapshot_block: Gauge,
    /// Full replays triggered by `NeedsReplay`
    pub full_replays_total: Counter,
    /// SQLite database file size in bytes
    pub db_size_bytes: Gauge,
}

pub fn install_prometheus_exporter(port: u16) -> eyre::Result<()> {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    builder
        .with_http_listener(addr)
        .install()
        .map_err(|e| eyre::eyre!("failed to install Prometheus exporter: {e}"))?;
    tracing::info!(%addr, "Prometheus metrics endpoint listening");
    Ok(())
}

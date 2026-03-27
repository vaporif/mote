pub mod batch_decoder;
pub mod entity_store;
pub mod flight_sql;
pub mod health;
pub mod ipc_client;
pub mod table_provider;

use std::{path::Path, sync::Arc, time::Duration};

use entity_store::Snapshot;
use tokio::sync::watch;
use tracing::{error, info, warn};

use batch_decoder::ApplyResult;
use entity_store::EntityStore;

enum ConnectionOutcome {
    Closed { last_block: u64 },
    NeedsReplay,
}

pub async fn run(
    exex_socket: std::path::PathBuf,
    flight_port: u16,
    health_port: u16,
) -> eyre::Result<()> {
    let mut store = EntityStore::new();
    let mut resume_block: u64 = 0;

    let (shutdown_tx, _) = watch::channel(false);
    let (ready_tx, ready_rx) = watch::channel(false);

    let initial_snapshot = Arc::new(store.snapshot()?);
    let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot);

    let mut health_handle = tokio::spawn({
        let ready_rx = ready_rx.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        async move {
            if let Err(e) = health::serve_health(health_port, ready_rx, shutdown_rx).await {
                error!(?e, "health server failed");
            }
        }
    });

    let ctx = Arc::new(table_provider::create_session_context(snapshot_rx)?);
    let mut flight_handle = tokio::spawn({
        let ctx = Arc::clone(&ctx);
        let shutdown_rx = shutdown_tx.subscribe();
        async move {
            if let Err(e) = flight_sql::serve_flight_sql(flight_port, ctx, shutdown_rx).await {
                error!(?e, "flight sql server failed");
            }
        }
    });

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                info!("received shutdown signal");
                let _ = shutdown_tx.send(true);
                return Ok(());
            }
            res = &mut health_handle => {
                return Err(eyre::eyre!("health server exited unexpectedly: {res:?}"));
            }
            res = &mut flight_handle => {
                return Err(eyre::eyre!("flight sql server exited unexpectedly: {res:?}"));
            }
            result = run_connection(&exex_socket, &mut store, &snapshot_tx, &ready_tx, resume_block) => {
                match result {
                    Ok(ConnectionOutcome::Closed { last_block }) => {
                        info!(last_block, "connection closed");
                        resume_block = last_block;
                    }
                    Ok(ConnectionOutcome::NeedsReplay) => {
                        warn!("non-reversible revert, full replay needed");
                        resume_block = 0;
                    }
                    Err(e) => {
                        warn!(?e, "connection error");
                        resume_block = 0;
                    }
                }
            }
        }

        store.clear();
        let _ = ready_tx.send(false);
        let _ = snapshot_tx.send(Arc::new(store.snapshot()?));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_connection(
    socket_path: &Path,
    store: &mut EntityStore,
    snapshot_tx: &watch::Sender<Arc<Snapshot>>,
    ready_tx: &watch::Sender<bool>,
    resume_block: u64,
) -> eyre::Result<ConnectionOutcome> {
    let (handshake, std_stream) =
        ipc_client::connect_and_subscribe(socket_path, resume_block).await?;
    info!(
        oldest = handshake.oldest_block,
        tip = handshake.tip_block,
        "connected"
    );

    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(256);

    let reader_handle = tokio::task::spawn_blocking(move || -> eyre::Result<()> {
        let iter = ipc_client::read_batches(std_stream)?;
        for batch_result in iter {
            let batch = batch_result?;
            if batch_tx.blocking_send(batch).is_err() {
                break;
            }
        }
        Ok(())
    });

    let mut is_live = false;
    let mut last_block: u64 = resume_block;

    while let Some(batch) = batch_rx.recv().await {
        match batch_decoder::apply_batch(store, &batch)? {
            ApplyResult::Watermark => {
                info!("watermark received, entering live mode");
                is_live = true;
                let _ = ready_tx.send(true);
                let _ = snapshot_tx.send(Arc::new(store.snapshot()?));
                continue;
            }
            ApplyResult::Applied => {
                if let Some(block) = batch_decoder::batch_block_number(&batch) {
                    last_block = block;
                }
            }
            ApplyResult::NeedsReplay => {
                return Ok(ConnectionOutcome::NeedsReplay);
            }
        }

        if is_live {
            // Drain queued batches so we rebuild the snapshot only once per burst
            while let Ok(queued) = batch_rx.try_recv() {
                match batch_decoder::apply_batch(store, &queued)? {
                    ApplyResult::Applied => {
                        if let Some(block) = batch_decoder::batch_block_number(&queued) {
                            last_block = block;
                        }
                    }
                    ApplyResult::Watermark => {}
                    ApplyResult::NeedsReplay => {
                        return Ok(ConnectionOutcome::NeedsReplay);
                    }
                }
            }
            let _ = snapshot_tx.send(Arc::new(store.snapshot()?));
        }
    }

    match reader_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(eyre::eyre!("IPC reader task panicked: {e}")),
    }

    Ok(ConnectionOutcome::Closed { last_block })
}

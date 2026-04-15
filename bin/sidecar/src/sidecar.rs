use std::{
    path::Path,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use futures::{FutureExt, StreamExt};
use glint_analytics::{
    batch_decoder::{self, ApplyResult},
    entity_store::EntityStore,
    table_provider,
};
use glint_historical::{
    provider::{
        event_numeric_annotations::EventNumericAnnotationsProvider,
        event_string_annotations::EventStringAnnotationsProvider,
        historical_events::HistoricalEventsProvider,
    },
    schema, writer,
};

use crate::{cli::EntitiesBackend, flight_sql, health};
use glint_primitives::exex_schema::columns;
use glint_sidecar_live_sql::{
    applier as sql_applier,
    provider::{
        SqliteEntitiesProvider, SqliteNumericAnnotationsProvider, SqliteStringAnnotationsProvider,
    },
    schema as sql_schema,
};
use rusqlite::Connection;
use tokio::sync::watch;
use tracing::{error, info, warn};

use parking_lot::Mutex;

fn open_read_connection(db_path: &Path) -> eyre::Result<Connection> {
    let conn = Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.pragma_update(None, "mmap_size", 256 * 1024 * 1024)?;
    conn.pragma_update(None, "cache_size", -64000)?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(conn)
}

enum ConnectionOutcome {
    Closed { last_block: u64 },
    NeedsReplay,
}

enum Backend {
    Memory {
        store: Box<EntityStore>,
        snapshot_tx: watch::Sender<Arc<glint_analytics::entity_store::Snapshot>>,
    },
    Sqlite {
        current_block: Arc<AtomicU64>,
    },
}

pub async fn run(args: crate::cli::RunArgs) -> eyre::Result<()> {
    let crate::cli::RunArgs {
        exex_socket,
        exex_grpc,
        flight_port,
        health_port,
        db_path,
        entities_backend,
        genesis,
        snapshot_interval,
    } = args;

    let raw = std::fs::read_to_string(&genesis)
        .map_err(|e| eyre::eyre!("reading genesis at {}: {e}", genesis.display()))?;
    let genesis_json: serde_json::Value = serde_json::from_str(&raw)?;
    let glint_config = glint_primitives::config::GlintChainConfig::from_genesis(&genesis_json)?;

    if glint_config.btl_unlimited() && matches!(entities_backend, EntitiesBackend::Memory) {
        eyre::bail!(
            "memory backend is not supported with unlimited BTL (max_btl=0 in genesis). \
             Use --entities-backend sqlite instead."
        );
    }

    let snapshots_dir = db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("snapshots");

    let write_conn = Connection::open(&db_path)?;
    schema::create_tables(&write_conn)?;
    schema::check_schema_version(&write_conn)?;

    let last_processed = schema::get_last_processed_block(&write_conn)?;
    info!(last_processed_block = ?last_processed, "SQLite state loaded");

    let write_conn = Arc::new(Mutex::new(write_conn));

    let read_conn = Arc::new(Mutex::new(open_read_connection(&db_path)?));

    let mut resume_block: u64 = last_processed.unwrap_or(0);

    let (shutdown_tx, _) = watch::channel(false);
    let (ready_tx, ready_rx) = watch::channel(false);

    let mut health_handle = tokio::spawn({
        let ready_rx = ready_rx.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        async move {
            if let Err(e) = health::serve_health(health_port, ready_rx, shutdown_rx).await {
                error!(?e, "health server failed");
            }
        }
    });

    let ctx = Arc::new(datafusion::prelude::SessionContext::new());
    ctx.register_table(
        "entity_events",
        Arc::new(HistoricalEventsProvider::new(Arc::clone(&read_conn))),
    )?;
    ctx.register_table(
        "event_string_annotations",
        Arc::new(EventStringAnnotationsProvider::new(Arc::clone(&read_conn))),
    )?;
    ctx.register_table(
        "event_numeric_annotations",
        Arc::new(EventNumericAnnotationsProvider::new(Arc::clone(&read_conn))),
    )?;

    let mut backend = match entities_backend {
        EntitiesBackend::Memory => {
            let mut store = if snapshot_interval > 0 {
                match glint_analytics::snapshot_io::load_latest_snapshot(&snapshots_dir) {
                    Ok(Some((snap_block, loaded_store)))
                        if snap_block <= resume_block =>
                    {
                        info!(snap_block, entities = loaded_store.len(), "loaded snapshot");
                        loaded_store
                    }
                    Ok(Some((snap_block, _))) => {
                        warn!(
                            snap_block,
                            resume_block, "snapshot ahead of SQLite, starting empty"
                        );
                        EntityStore::new()
                    }
                    Ok(None) => {
                        info!("no snapshot found, starting empty");
                        EntityStore::new()
                    }
                    Err(e) => {
                        warn!(?e, "failed to load snapshot, starting empty");
                        EntityStore::new()
                    }
                }
            } else {
                EntityStore::new()
            };

            store.set_current_block(resume_block);

            let initial_snapshot = Arc::new(store.snapshot()?);
            let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot);

            table_provider::register_tables(&ctx, snapshot_rx)?;

            Backend::Memory {
                store: Box::new(store),
                snapshot_tx,
            }
        }
        EntitiesBackend::Sqlite => {
            sql_schema::check_and_init_schema(&write_conn.lock())?;

            let current_block = Arc::new(AtomicU64::new(resume_block));

            let entities_read_conn = Arc::new(Mutex::new(open_read_connection(&db_path)?));

            ctx.register_table(
                "entities_latest",
                Arc::new(SqliteEntitiesProvider::new(
                    Arc::clone(&entities_read_conn),
                    Arc::clone(&current_block),
                )),
            )?;
            ctx.register_table(
                "entity_string_annotations",
                Arc::new(SqliteStringAnnotationsProvider::new(Arc::clone(
                    &entities_read_conn,
                ))),
            )?;
            ctx.register_table(
                "entity_numeric_annotations",
                Arc::new(SqliteNumericAnnotationsProvider::new(entities_read_conn)),
            )?;

            Backend::Sqlite { current_block }
        }
    };

    let mut flight_handle = tokio::spawn({
        let ctx = Arc::clone(&ctx);
        let shutdown_rx = shutdown_tx.subscribe();
        async move {
            if let Err(e) = flight_sql::serve_flight_sql(flight_port, ctx, shutdown_rx).await {
                error!(?e, "flight sql server failed");
            }
        }
    });

    let mut last_snapshot_block: u64 = 0;

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                info!("received shutdown signal");
                if snapshot_interval > 0
                    && let Backend::Memory { store, .. } = &backend
                {
                    let current_block = store.current_block();
                    if current_block > 0 && current_block != last_snapshot_block {
                        match store.snapshot() {
                            Ok(snap) => {
                                write_and_prune_snapshot(&snapshots_dir, current_block, &snap);
                            }
                            Err(e) => warn!(?e, "failed to create shutdown snapshot"),
                        }
                    }
                }
                let _ = shutdown_tx.send(true);
                return Ok(());
            }
            res = &mut health_handle => {
                return Err(eyre::eyre!("health server exited unexpectedly: {res:?}"));
            }
            res = &mut flight_handle => {
                return Err(eyre::eyre!("flight sql server exited unexpectedly: {res:?}"));
            }
            result = async {
                let client: Box<dyn glint_transport::ExExTransportClient> =
                    if let Some(ref grpc_url) = exex_grpc {
                        Box::new(glint_transport::grpc::GrpcClient::new(grpc_url.clone()))
                    } else {
                        Box::new(glint_transport::ipc::IpcClient::new(exex_socket.clone()))
                    };
                run_connection(
                    client,
                    &mut backend,
                    &write_conn,
                    &ready_tx,
                    resume_block,
                    &snapshots_dir,
                    snapshot_interval,
                    &mut last_snapshot_block,
                ).await
            } => {
                match result {
                    Ok(ConnectionOutcome::Closed { last_block }) => {
                        info!(last_block, "connection closed");
                        resume_block = last_block;
                    }
                    Ok(ConnectionOutcome::NeedsReplay) => {
                        warn!("non-reversible revert, full replay needed");
                        resume_block = 0;
                        last_snapshot_block = 0;
                        if let Backend::Sqlite { .. } = &backend {
                            sql_schema::clear_entities_latest(&write_conn.lock())?;
                        }
                    }
                    Err(e) => {
                        resume_block = last_processed_resume_block(&write_conn);
                        last_snapshot_block = 0;
                        warn!(?e, resume_block, "connection error, resuming from last processed block");
                    }
                }
            }
        }

        match &mut backend {
            Backend::Memory {
                store, snapshot_tx, ..
            } => {
                // Try reloading from the latest snapshot to avoid full replay.
                // Only use the snapshot if it's not ahead of where we're resuming from.
                if snapshot_interval > 0 {
                    match glint_analytics::snapshot_io::load_latest_snapshot(&snapshots_dir) {
                        Ok(Some((snap_block, loaded_store)))
                            if snap_block <= resume_block =>
                        {
                            info!(
                                snap_block,
                                entities = loaded_store.len(),
                                "reloaded snapshot after reconnect"
                            );
                            **store = loaded_store;
                        }
                        _ => store.clear(),
                    }
                } else {
                    store.clear();
                }
                store.set_current_block(resume_block);
                let _ = snapshot_tx.send(Arc::new(store.snapshot()?));
            }
            Backend::Sqlite { .. } => {}
        }
        let _ = ready_tx.send(false);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[allow(clippy::significant_drop_tightening)]
fn delete_sqlite_from_block(sqlite_conn: &Arc<Mutex<Connection>>, block: u64) -> eyre::Result<()> {
    let conn = sqlite_conn.lock();
    let tx = conn.unchecked_transaction()?;
    schema::delete_events_from_block(&tx, block)?;
    schema::set_last_processed_block(&tx, block.saturating_sub(1))?;
    tx.commit()?;
    Ok(())
}

enum BatchOutcome {
    Continue,
    EnterLive,
    NeedsReplay,
}

fn process_batch(
    backend: &mut Backend,
    sqlite_conn: &Arc<Mutex<Connection>>,
    batch: &arrow::record_batch::RecordBatch,
    last_block: &mut u64,
) -> eyre::Result<BatchOutcome> {
    let is_revert = is_revert_batch(batch);
    let block = batch_decoder::batch_block_number(batch);

    let result = match backend {
        Backend::Memory { store, .. } => batch_decoder::apply_batch(store, batch)?,
        Backend::Sqlite { current_block } => {
            match sql_applier::apply_batch(sqlite_conn, current_block, batch)? {
                sql_applier::ApplyResult::Applied => ApplyResult::Applied,
                sql_applier::ApplyResult::Watermark => ApplyResult::Watermark,
                sql_applier::ApplyResult::NeedsReplay => ApplyResult::NeedsReplay,
            }
        }
    };

    match result {
        ApplyResult::Watermark => return Ok(BatchOutcome::EnterLive),
        ApplyResult::Applied => {
            if let Some(b) = block {
                *last_block = b;
            }
            if !is_revert {
                writer::insert_batch(&sqlite_conn.lock(), batch)?;
            }
            if is_revert && let Some(b) = block {
                delete_sqlite_from_block(sqlite_conn, b)?;
            }
        }
        ApplyResult::NeedsReplay => {
            if let Some(b) = block {
                delete_sqlite_from_block(sqlite_conn, b)?;
            }
            return Ok(BatchOutcome::NeedsReplay);
        }
    }

    Ok(BatchOutcome::Continue)
}

const SNAPSHOTS_TO_KEEP: usize = 2;

/// Write a snapshot to disk and prune old ones. Returns `true` on success.
fn write_and_prune_snapshot(
    snapshots_dir: &Path,
    block: u64,
    snap: &glint_analytics::entity_store::Snapshot,
) -> bool {
    if let Err(e) = glint_analytics::snapshot_io::write_snapshot(snapshots_dir, block, snap) {
        warn!(?e, "failed to write snapshot");
        return false;
    }
    info!(block, "snapshot written");
    if let Err(e) = glint_analytics::snapshot_io::prune_snapshots(snapshots_dir, SNAPSHOTS_TO_KEEP)
    {
        warn!(?e, "failed to prune snapshots");
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn run_connection(
    client: Box<dyn glint_transport::ExExTransportClient>,
    backend: &mut Backend,
    sqlite_conn: &Arc<Mutex<Connection>>,
    ready_tx: &watch::Sender<bool>,
    resume_block: u64,
    snapshots_dir: &Path,
    snapshot_interval: u64,
    last_snapshot_block: &mut u64,
) -> eyre::Result<ConnectionOutcome> {
    let (handshake, mut batch_stream) = client.subscribe(resume_block).await?;
    info!(
        oldest = handshake.oldest_block,
        tip = handshake.tip_block,
        "connected to ExEx"
    );

    check_gap(sqlite_conn, handshake.oldest_block)?;

    let mut is_live = false;
    let mut last_block: u64 = resume_block;

    while let Some(batch_result) = batch_stream.next().await {
        let batch = batch_result?;
        match process_batch(backend, sqlite_conn, &batch, &mut last_block)? {
            BatchOutcome::EnterLive => {
                info!("watermark received, entering live mode");
                is_live = true;
                let _ = ready_tx.send(true);
                if let Backend::Memory { store, snapshot_tx } = backend {
                    let _ = snapshot_tx.send(Arc::new(store.snapshot()?));
                }
                continue;
            }
            BatchOutcome::NeedsReplay => return Ok(ConnectionOutcome::NeedsReplay),
            BatchOutcome::Continue => {}
        }

        if is_live {
            // Batch up queued messages so we publish one snapshot, not N
            while let Some(Some(queued_result)) = batch_stream.next().now_or_never() {
                let queued = queued_result?;
                match process_batch(backend, sqlite_conn, &queued, &mut last_block)? {
                    BatchOutcome::NeedsReplay => return Ok(ConnectionOutcome::NeedsReplay),
                    BatchOutcome::EnterLive | BatchOutcome::Continue => {}
                }
            }
            if let Backend::Memory { store, snapshot_tx } = backend {
                let snap = Arc::new(store.snapshot()?);
                let _ = snapshot_tx.send(Arc::clone(&snap));

                if snapshot_interval > 0
                    && last_block.saturating_sub(*last_snapshot_block) >= snapshot_interval
                    && write_and_prune_snapshot(snapshots_dir, last_block, &snap)
                {
                    *last_snapshot_block = last_block;
                }
            }
        }
    }

    Ok(ConnectionOutcome::Closed { last_block })
}

fn check_gap(sqlite_conn: &Arc<Mutex<Connection>>, oldest_block: u64) -> eyre::Result<()> {
    let last_processed = schema::get_last_processed_block(&sqlite_conn.lock())?;

    if let Some(last) = last_processed
        && oldest_block > last
    {
        eyre::bail!(
            "ExEx ring buffer oldest block ({oldest_block}) > SQLite last_processed_block ({last}). \
             Run `glint db rebuild --from-block {last}` to recover.",
        );
    }

    Ok(())
}

/// Last block persisted in `SQLite`, or 0 if unknown.
fn last_processed_resume_block(sqlite_conn: &Arc<Mutex<Connection>>) -> u64 {
    schema::get_last_processed_block(&sqlite_conn.lock())
        .ok()
        .flatten()
        .unwrap_or(0)
}

fn is_revert_batch(batch: &arrow::record_batch::RecordBatch) -> bool {
    batch
        .column_by_name(columns::OP)
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt8Array>())
        .map(|c| c.value(0))
        == Some(glint_primitives::exex_types::BatchOp::Revert as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes};
    use glint_analytics::entity_store::EntityRow;

    fn setup_sqlite() -> Arc<Mutex<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        schema::create_tables(&conn).unwrap();
        Arc::new(Mutex::new(conn))
    }

    fn sample_row(byte: u8) -> EntityRow {
        EntityRow {
            entity_key: B256::repeat_byte(byte),
            owner: Address::repeat_byte(byte),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_annotations: vec![("color".into(), "red".into())],
            numeric_annotations: vec![("size".into(), 42)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: Some(Address::ZERO),
        }
    }

    fn make_memory_backend() -> (Backend, watch::Receiver<Arc<glint_analytics::entity_store::Snapshot>>) {
        let store = EntityStore::new();
        let initial_snapshot = Arc::new(store.snapshot().unwrap());
        let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot);
        let backend = Backend::Memory {
            store: Box::new(store),
            snapshot_tx,
        };
        (backend, snapshot_rx)
    }

    #[test]
    fn resume_block_after_error_preserves_sqlite_progress() {
        let conn = setup_sqlite();
        schema::set_last_processed_block(&conn.lock(), 500).unwrap();
        assert_eq!(last_processed_resume_block(&conn), 500);
    }

    #[test]
    fn resume_block_after_error_returns_zero_without_progress() {
        let conn = setup_sqlite();
        assert_eq!(last_processed_resume_block(&conn), 0);
    }

    #[test]
    fn write_and_prune_snapshot_writes_and_prunes() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots_dir = dir.path().join("snapshots");

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));

        let snap = store.snapshot().unwrap();
        assert!(write_and_prune_snapshot(&snapshots_dir, 1000, &snap));
        assert!(write_and_prune_snapshot(&snapshots_dir, 2000, &snap));
        assert!(write_and_prune_snapshot(&snapshots_dir, 3000, &snap));

        let entries: Vec<_> = std::fs::read_dir(&snapshots_dir)
            .unwrap()
            .flatten()
            .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
            .collect();
        assert_eq!(entries.len(), SNAPSHOTS_TO_KEEP);
        assert!(!snapshots_dir.join("block-00001000").exists());
        assert!(snapshots_dir.join("block-00002000").exists());
        assert!(snapshots_dir.join("block-00003000").exists());
    }

    #[test]
    fn startup_load_rejects_snapshot_ahead_of_resume_block() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots_dir = dir.path().join("snapshots");

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let snap = store.snapshot().unwrap();
        glint_analytics::snapshot_io::write_snapshot(&snapshots_dir, 5000, &snap).unwrap();

        let resume_block: u64 = 2000;
        let loaded_store = match glint_analytics::snapshot_io::load_latest_snapshot(&snapshots_dir)
        {
            Ok(Some((snap_block, loaded_store))) if snap_block <= resume_block => {
                Some(loaded_store)
            }
            _ => None,
        };

        assert!(loaded_store.is_none());
    }

    #[test]
    fn startup_load_accepts_snapshot_at_or_below_resume_block() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots_dir = dir.path().join("snapshots");

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let snap = store.snapshot().unwrap();
        glint_analytics::snapshot_io::write_snapshot(&snapshots_dir, 2000, &snap).unwrap();

        let resume_block: u64 = 5000;
        let loaded_store = match glint_analytics::snapshot_io::load_latest_snapshot(&snapshots_dir)
        {
            Ok(Some((snap_block, loaded_store))) if snap_block <= resume_block => {
                Some(loaded_store)
            }
            _ => None,
        };

        assert!(loaded_store.is_some());
        assert_eq!(loaded_store.unwrap().len(), 1);
    }

    #[test]
    fn reconnect_reload_rejects_snapshot_ahead_of_resume_block() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots_dir = dir.path().join("snapshots");

        let (mut backend, _rx) = make_memory_backend();
        if let Backend::Memory { store, .. } = &mut backend {
            store.insert(sample_row(0x01));
            store.insert(sample_row(0x02));
        }

        if let Backend::Memory { store, .. } = &backend {
            let snap = store.snapshot().unwrap();
            glint_analytics::snapshot_io::write_snapshot(&snapshots_dir, 8000, &snap).unwrap();
        }

        // resume_block < snap_block — snapshot should be rejected
        let resume_block: u64 = 3000;
        let snapshot_interval: u64 = 1000;

        if let Backend::Memory {
            store, snapshot_tx, ..
        } = &mut backend
        {
            if snapshot_interval > 0 {
                match glint_analytics::snapshot_io::load_latest_snapshot(&snapshots_dir) {
                    Ok(Some((snap_block, loaded_store))) if snap_block <= resume_block => {
                        **store = loaded_store;
                    }
                    _ => store.clear(),
                }
            } else {
                store.clear();
            }
            store.set_current_block(resume_block);
            let _ = snapshot_tx.send(Arc::new(store.snapshot().unwrap()));
        }

        if let Backend::Memory { store, .. } = &backend {
            assert_eq!(store.len(), 0);
            assert_eq!(store.current_block(), resume_block);
        }
    }

    #[test]
    fn reconnect_reload_accepts_valid_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots_dir = dir.path().join("snapshots");

        let mut orig_store = EntityStore::new();
        orig_store.insert(sample_row(0x01));
        orig_store.insert(sample_row(0x02));
        let snap = orig_store.snapshot().unwrap();
        glint_analytics::snapshot_io::write_snapshot(&snapshots_dir, 2000, &snap).unwrap();

        let (mut backend, _rx) = make_memory_backend();

        // resume_block > snap_block — snapshot should be accepted
        let resume_block: u64 = 5000;
        let snapshot_interval: u64 = 1000;

        if let Backend::Memory {
            store, snapshot_tx, ..
        } = &mut backend
        {
            if snapshot_interval > 0 {
                match glint_analytics::snapshot_io::load_latest_snapshot(&snapshots_dir) {
                    Ok(Some((snap_block, loaded_store))) if snap_block <= resume_block => {
                        **store = loaded_store;
                    }
                    _ => store.clear(),
                }
            } else {
                store.clear();
            }
            store.set_current_block(resume_block);
            let _ = snapshot_tx.send(Arc::new(store.snapshot().unwrap()));
        }

        if let Backend::Memory { store, .. } = &backend {
            assert_eq!(store.len(), 2);
            assert_eq!(store.current_block(), resume_block);
            assert!(store.get(&B256::repeat_byte(0x01)).is_some());
            assert!(store.get(&B256::repeat_byte(0x02)).is_some());
        }
    }

    #[test]
    fn shutdown_skips_snapshot_when_block_matches_last_written() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots_dir = dir.path().join("snapshots");

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.set_current_block(5000);

        let snap = store.snapshot().unwrap();
        write_and_prune_snapshot(&snapshots_dir, 5000, &snap);
        let last_snapshot_block: u64 = 5000;

        let count_before = std::fs::read_dir(&snapshots_dir).unwrap().count();

        let current_block = store.current_block();
        if current_block > 0 && current_block != last_snapshot_block {
            write_and_prune_snapshot(&snapshots_dir, current_block, &snap);
        }

        let count_after = std::fs::read_dir(&snapshots_dir).unwrap().count();
        assert_eq!(count_before, count_after);
    }

    #[test]
    fn shutdown_writes_snapshot_when_block_advanced() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots_dir = dir.path().join("snapshots");

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        store.set_current_block(5000);

        let snap = store.snapshot().unwrap();
        write_and_prune_snapshot(&snapshots_dir, 4000, &snap);
        let last_snapshot_block: u64 = 4000;

        let current_block = store.current_block();
        if current_block > 0 && current_block != last_snapshot_block {
            write_and_prune_snapshot(&snapshots_dir, current_block, &snap);
        }

        assert!(snapshots_dir.join("block-00004000").exists());
        assert!(snapshots_dir.join("block-00005000").exists());
    }
}

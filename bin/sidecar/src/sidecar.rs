use std::{
    path::Path,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use glint_analytics::{
    batch_decoder::{self, ApplyResult},
    entity_store::EntityStore,
    table_provider,
};
use glint_historical::{
    provider::{
        event_numeric_annotions::EventNumericAnnotationsProvider,
        event_string_annotations::EventStringAnnotationsProvider,
        historical_events::HistoricalEventsProvider,
    },
    schema, writer,
};

use crate::{cli::EntitiesBackend, flight_sql, health, ipc_client};
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
        flight_port,
        health_port,
        db_path,
        entities_backend,
        genesis,
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
            let store = EntityStore::new();
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
            result = run_connection(
                &exex_socket,
                &mut backend,
                &write_conn,
                &ready_tx,
                resume_block,
            ) => {
                match result {
                    Ok(ConnectionOutcome::Closed { last_block }) => {
                        info!(last_block, "connection closed");
                        resume_block = last_block;
                    }
                    Ok(ConnectionOutcome::NeedsReplay) => {
                        warn!("non-reversible revert, full replay needed");
                        resume_block = 0;
                        if let Backend::Sqlite { .. } = &backend {
                            sql_schema::clear_entities_latest(&write_conn.lock())?;
                        }
                    }
                    Err(e) => {
                        resume_block = last_processed_resume_block(&write_conn);
                        warn!(?e, resume_block, "connection error, resuming from last processed block");
                    }
                }
            }
        }

        match &mut backend {
            Backend::Memory {
                store, snapshot_tx, ..
            } => {
                store.clear();
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

impl BatchOutcome {
    const fn is_needs_replay(&self) -> bool {
        matches!(self, Self::NeedsReplay)
    }
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

async fn run_connection(
    socket_path: &Path,
    backend: &mut Backend,
    sqlite_conn: &Arc<Mutex<Connection>>,
    ready_tx: &watch::Sender<bool>,
    resume_block: u64,
) -> eyre::Result<ConnectionOutcome> {
    let (handshake, std_stream) =
        ipc_client::connect_and_subscribe(socket_path, resume_block).await?;
    info!(
        oldest = handshake.oldest_block,
        tip = handshake.tip_block,
        "connected to ExEx"
    );

    check_gap(sqlite_conn, &handshake)?;

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
            while let Ok(queued) = batch_rx.try_recv() {
                if process_batch(backend, sqlite_conn, &queued, &mut last_block)?.is_needs_replay()
                {
                    return Ok(ConnectionOutcome::NeedsReplay);
                }
            }
            if let Backend::Memory { store, snapshot_tx } = backend {
                let _ = snapshot_tx.send(Arc::new(store.snapshot()?));
            }
        }
    }

    match reader_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(eyre::eyre!("IPC reader task panicked: {e}")),
    }

    Ok(ConnectionOutcome::Closed { last_block })
}

fn check_gap(
    sqlite_conn: &Arc<Mutex<Connection>>,
    handshake: &ipc_client::Handshake,
) -> eyre::Result<()> {
    let last_processed = schema::get_last_processed_block(&sqlite_conn.lock())?;

    if let Some(last) = last_processed
        && handshake.oldest_block > last
    {
        eyre::bail!(
            "ExEx ring buffer oldest block ({}) > SQLite last_processed_block ({}). \
             Run `glint db rebuild --from-block {last}` to recover.",
            handshake.oldest_block,
            last,
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

    fn setup_sqlite() -> Arc<Mutex<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        schema::create_tables(&conn).unwrap();
        Arc::new(Mutex::new(conn))
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
}

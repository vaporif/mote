pub mod arrow;
pub mod parse;
pub mod ring_buffer;
pub mod stream;

use ::arrow::record_batch::RecordBatch;
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use futures::StreamExt;
use mote_primitives::constants::MAX_BTL;
use mote_primitives::exex_types::BatchOp;
use reth_primitives_traits::BlockBody;
use ring_buffer::RingBuffer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::arrow::EventRow;
use crate::parse::parse_log;
use crate::stream::SnapshotRequest;

const BATCH_CHANNEL_SIZE: usize = 1024;
const SNAPSHOT_CHANNEL_SIZE: usize = 1;

/// Returns a closure suitable for `reth_node_builder::install_exex`.
///
/// The returned closure captures `socket_path` and, when called with an
/// `ExExContext`, spawns the full notification loop + socket writer pipeline.
pub fn install<Node>(
    socket_path: std::path::PathBuf,
) -> impl FnOnce(
    reth_exex::ExExContext<Node>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = eyre::Result<()>> + Send>>
where
    Node: reth_node_api::FullNodeComponents,
{
    move |ctx| Box::pin(mote_exex(ctx, socket_path))
}

async fn mote_exex<Node: reth_node_api::FullNodeComponents>(
    mut ctx: reth_exex::ExExContext<Node>,
    socket_path: std::path::PathBuf,
) -> eyre::Result<()> {
    info!(?socket_path, head = ?ctx.head, "mote exex starting");

    // -- channels --
    let (batch_tx, batch_rx) = mpsc::channel::<RecordBatch>(BATCH_CHANNEL_SIZE);
    let (snapshot_tx, mut snapshot_rx) = mpsc::channel::<SnapshotRequest>(SNAPSHOT_CHANNEL_SIZE);
    let (delivered_tx, delivered_rx) = watch::channel::<Option<BlockNumHash>>(None);

    // -- shared state --
    let cancellation_token = CancellationToken::new();
    let mut ring_buffer = RingBuffer::new();
    let (atomic_entries, atomic_memory) = ring_buffer.atomics();
    let consumer_connected = Arc::new(AtomicBool::new(false));

    // -- spawn socket writer --
    let writer_cancel = cancellation_token.clone();
    let writer_consumer = Arc::clone(&consumer_connected);
    let writer_entries = Arc::clone(&atomic_entries);
    let writer_memory = Arc::clone(&atomic_memory);
    tokio::spawn(async move {
        if let Err(e) = stream::socket_writer_task(
            socket_path,
            snapshot_tx,
            batch_rx,
            delivered_tx,
            writer_consumer,
            writer_entries,
            writer_memory,
            writer_cancel,
        )
        .await
        {
            error!(?e, "socket writer task failed");
        }
    });

    // -- replay bookkeeping --
    // When a snapshot request completes, we hold the `replay_done_tx` sender
    // so the socket writer can wait for us to signal readiness for live data.
    let mut pending_replay_done: Option<oneshot::Sender<()>> = None;

    let mut max_reported = BlockNumHash::default();
    let mut grace = stream::GraceState::default();

    // -- notification loop --
    loop {
        tokio::select! {
            biased;

            () = cancellation_token.cancelled() => {
                info!("mote exex shutting down via cancellation");
                break;
            }

            Some(snap_req) = snapshot_rx.recv() => {
                let snapshot = ring_buffer.snapshot_from(snap_req.resume_block);
                debug!(
                    resume_block = snap_req.resume_block,
                    snapshot_len = snapshot.len(),
                    "fulfilling snapshot request"
                );
                let _ = snap_req.reply_tx.send(snapshot);
                // Store replay_done_tx; we signal it below so the socket writer
                // knows the notification loop is caught up and live batches can flow.
                pending_replay_done = Some(snap_req.replay_done_tx);
            }

            maybe_notification = ctx.notifications.next() => {
                let Some(notification) = maybe_notification else {
                    info!("notification stream ended");
                    break;
                };

                let notification = match notification {
                    Ok(n) => n,
                    Err(e) => {
                        error!(?e, "notification stream error");
                        break;
                    }
                };

                let current_tip = match &notification {
                    reth_exex_types::ExExNotification::ChainCommitted { new } => {
                        process_committed_chain(
                            new,
                            &mut ring_buffer,
                            &batch_tx,
                            &consumer_connected,
                            &mut grace,
                        );
                        new.tip().num_hash()
                    }
                    reth_exex_types::ExExNotification::ChainReverted { old } => {
                        let tip = old.tip().num_hash();
                        process_reverted_chain(
                            old,
                            &mut ring_buffer,
                            &batch_tx,
                            &consumer_connected,
                            &mut grace,
                        );
                        tip
                    }
                    reth_exex_types::ExExNotification::ChainReorged { old, new } => {
                        // First revert old chain, then commit new chain.
                        process_reverted_chain(
                            old,
                            &mut ring_buffer,
                            &batch_tx,
                            &consumer_connected,
                            &mut grace,
                        );
                        process_committed_chain(
                            new,
                            &mut ring_buffer,
                            &batch_tx,
                            &consumer_connected,
                            &mut grace,
                        );
                        new.tip().num_hash()
                    }
                };

                ring_buffer.evict_if_needed(current_tip.number);

                // If a replay was pending, signal the socket writer that it can
                // start forwarding live batches.
                if let Some(tx) = pending_replay_done.take() {
                    let _ = tx.send(());
                }

                report_finished_height(
                    &ctx,
                    &ring_buffer,
                    &delivered_rx,
                    current_tip,
                    &mut max_reported,
                );
            }
        }
    }

    cancellation_token.cancel();
    Ok(())
}

// ---------------------------------------------------------------------------
// Chain processing helpers
// ---------------------------------------------------------------------------

fn process_committed_chain<N: reth_primitives_traits::NodePrimitives>(
    chain: &reth_execution_types::Chain<N>,
    ring_buffer: &mut RingBuffer,
    batch_tx: &mpsc::Sender<RecordBatch>,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
) {
    let tip_block = chain.tip().header().number();

    for (block, receipts) in chain.blocks_and_receipts() {
        let block_number = block.header().number();
        let block_hash = block.hash();
        let bnh = BlockNumHash::new(block_number, block_hash);
        let transactions = block.body().transactions();

        let events = collect_events_from_receipts(receipts, transactions);

        match crate::arrow::build_record_batch(
            block_number,
            block_hash,
            tip_block,
            BatchOp::Commit,
            &events,
        ) {
            Ok(batch) => {
                ring_buffer.push(bnh, batch.clone());
                try_send_batch(batch_tx, batch, consumer_connected, grace);
            }
            Err(e) => {
                error!(block_number, ?e, "failed to build commit record batch");
            }
        }
    }
}

fn process_reverted_chain<N: reth_primitives_traits::NodePrimitives>(
    chain: &reth_execution_types::Chain<N>,
    ring_buffer: &mut RingBuffer,
    batch_tx: &mpsc::Sender<RecordBatch>,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
) {
    let tip_block = chain.tip().header().number();

    // Find the lowest reverted block number for ring buffer truncation.
    let first_reverted = chain.blocks_iter().next().map(|b| b.header().number());

    if let Some(reorg_start) = first_reverted {
        ring_buffer.truncate_from(reorg_start);
    }

    // Iterate blocks in reverse order for revert batches.
    // We need to collect because `blocks_and_receipts()` returns a forward
    // iterator and we must process reverts from newest to oldest.
    #[allow(clippy::needless_collect)]
    let blocks: Vec<_> = chain.blocks_and_receipts().collect();
    for (block, receipts) in blocks.into_iter().rev() {
        let block_number = block.header().number();
        let block_hash = block.hash();
        let transactions = block.body().transactions();

        let events = collect_events_from_receipts(receipts, transactions);

        match crate::arrow::build_record_batch(
            block_number,
            block_hash,
            tip_block,
            BatchOp::Revert,
            &events,
        ) {
            Ok(batch) => {
                try_send_batch(batch_tx, batch, consumer_connected, grace);
            }
            Err(e) => {
                error!(block_number, ?e, "failed to build revert record batch");
            }
        }
    }
}

fn collect_events_from_receipts<R, T>(receipts: &[R], transactions: &[T]) -> Vec<EventRow>
where
    R: alloy_consensus::TxReceipt<Log = alloy_primitives::Log>,
    T: alloy_consensus::transaction::TxHashRef,
{
    let mut events = Vec::new();

    for (tx_idx, receipt) in receipts.iter().enumerate() {
        let tx_hash = transactions
            .get(tx_idx)
            .map(alloy_consensus::transaction::TxHashRef::tx_hash)
            .copied()
            .unwrap_or_default();

        for (log_offset, log) in receipt.logs().iter().enumerate() {
            match parse_log(log) {
                Ok(Some(entity_event)) => {
                    #[allow(clippy::cast_possible_truncation)]
                    events.push(EventRow {
                        event: entity_event,
                        tx_index: tx_idx as u32,
                        tx_hash,
                        log_index: log_offset as u32,
                    });
                }
                Ok(None) => {
                    // Not a mote processor log, skip.
                }
                Err(e) => {
                    warn!(tx_index = tx_idx, ?e, "failed to parse log");
                }
            }
        }
    }

    events
}

fn try_send_batch(
    batch_tx: &mpsc::Sender<RecordBatch>,
    batch: RecordBatch,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
) {
    // Only attempt to send if a consumer is connected.
    if !consumer_connected.load(Ordering::Acquire) {
        return;
    }

    match batch_tx.try_send(batch) {
        Ok(()) => {
            stream::reset_grace(grace);
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            debug!("batch channel full, applying backpressure");
            stream::handle_backpressure(grace);
            if grace.should_disconnect {
                warn!("backpressure threshold exceeded in notification loop");
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            debug!("batch channel closed");
        }
    }
}

// ---------------------------------------------------------------------------
// Finished-height reporting
// ---------------------------------------------------------------------------

fn report_finished_height<Node: reth_node_api::FullNodeComponents>(
    ctx: &reth_exex::ExExContext<Node>,
    ring_buffer: &RingBuffer,
    delivered_rx: &watch::Receiver<Option<BlockNumHash>>,
    current_tip: BlockNumHash,
    max_reported: &mut BlockNumHash,
) {
    // Safety floor: never report below tip - MAX_BTL.
    let min_safe_number = current_tip.number.saturating_sub(MAX_BTL);

    // Priority: consumer-delivered > ring buffer oldest - 1 > current tip.
    let delivered = *delivered_rx.borrow();
    let candidate = delivered.unwrap_or_else(|| {
        ring_buffer.oldest().map_or(current_tip, |oldest| {
            BlockNumHash::new(oldest.number.saturating_sub(1), oldest.hash)
        })
    });

    // Apply floor.
    let height = if candidate.number >= min_safe_number {
        candidate
    } else {
        BlockNumHash::new(min_safe_number, current_tip.hash)
    };

    // Monotonic: never go backwards.
    if height.number > max_reported.number {
        *max_reported = height;
        let _ = ctx.send_finished_height(height);
        debug!(number = height.number, "reported finished height");
    }
}

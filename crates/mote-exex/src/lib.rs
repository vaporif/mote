pub mod arrow;
pub mod parse;
pub mod ring_buffer;
pub mod stream;

use ::arrow::record_batch::RecordBatch;
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use futures::StreamExt;
use futures::future::OptionFuture;
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

    let (batch_tx, batch_rx) =
        mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(BATCH_CHANNEL_SIZE);
    let (snapshot_tx, mut snapshot_rx) = mpsc::channel::<SnapshotRequest>(SNAPSHOT_CHANNEL_SIZE);
    let (delivered_tx, delivered_rx) = watch::channel::<Option<BlockNumHash>>(None);

    let cancellation_token = CancellationToken::new();
    let mut ring_buffer = RingBuffer::new();
    let rb_stats = ring_buffer.stats();
    let consumer_connected = Arc::new(AtomicBool::new(false));

    let writer_cancel = cancellation_token.clone();
    let writer_consumer = Arc::clone(&consumer_connected);
    tokio::spawn(async move {
        if let Err(e) = stream::socket_writer_task(
            socket_path,
            snapshot_tx,
            batch_rx,
            delivered_tx,
            writer_consumer,
            rb_stats,
            writer_cancel,
        )
        .await
        {
            error!(?e, "socket writer task failed");
        }
    });

    let mut replay_in_progress = false;
    let mut pending_replay_done_rx: Option<oneshot::Receiver<()>> = None;

    let mut max_reported = BlockNumHash::default();
    let mut grace = stream::GraceState::default();

    loop {
        tokio::select! {
            biased;

            Some(snap_req) = snapshot_rx.recv() => {
                let snapshot = ring_buffer.snapshot_from(snap_req.resume_block);
                debug!(
                    resume_block = snap_req.resume_block,
                    snapshot_len = snapshot.len(),
                    "fulfilling snapshot request"
                );
                let _ = snap_req.reply_tx.send(snapshot);
                pending_replay_done_rx = Some(snap_req.replay_done_rx);
                replay_in_progress = true;
            }

            maybe_notification = ctx.notifications.next() => {
                let notification = match maybe_notification {
                    Some(Ok(n)) => n,
                    Some(Err(e)) => {
                        error!(?e, "notification stream error");
                        break;
                    }
                    None => {
                        info!("notification stream ended");
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
                            replay_in_progress,
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
                            replay_in_progress,
                        );
                        tip
                    }
                    reth_exex_types::ExExNotification::ChainReorged { old, new } => {
                        process_reverted_chain(
                            old,
                            &mut ring_buffer,
                            &batch_tx,
                            &consumer_connected,
                            &mut grace,
                            replay_in_progress,
                        );
                        process_committed_chain(
                            new,
                            &mut ring_buffer,
                            &batch_tx,
                            &consumer_connected,
                            &mut grace,
                            replay_in_progress,
                        );
                        new.tip().num_hash()
                    }
                };

                ring_buffer.evict_if_needed(current_tip.number);

                report_finished_height(
                    &ctx,
                    &ring_buffer,
                    &delivered_rx,
                    current_tip,
                    &mut max_reported,
                );
            }

            Some(result) = OptionFuture::from(pending_replay_done_rx.as_mut().map(|rx| &mut *rx)) => {
                pending_replay_done_rx = None;
                if result.is_ok() {
                    replay_in_progress = false;
                }
            }

            () = cancellation_token.cancelled() => {
                info!("mote exex shutting down via cancellation");
                break;
            }
        }
    }

    cancellation_token.cancel();
    Ok(())
}

fn process_committed_chain<N: reth_primitives_traits::NodePrimitives>(
    chain: &reth_execution_types::Chain<N>,
    ring_buffer: &mut RingBuffer,
    batch_tx: &mpsc::Sender<(Option<BlockNumHash>, RecordBatch)>,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
    replay_in_progress: bool,
) {
    let tip_block = chain.tip().header().number();

    for (block, receipts) in chain.blocks_and_receipts() {
        let block_number = block.header().number();
        let block_hash = block.hash();
        let bnh = BlockNumHash::new(block_number, block_hash);
        let transactions = block.body().transactions();

        let events = collect_events_from_receipts(receipts, transactions);

        if events.is_empty() {
            continue;
        }

        match crate::arrow::build_record_batch(
            block_number,
            block_hash,
            tip_block,
            BatchOp::Commit,
            &events,
        ) {
            Ok(batch) => {
                ring_buffer.push(bnh, batch.clone());
                try_send_batch(
                    batch_tx,
                    Some(bnh),
                    batch,
                    consumer_connected,
                    grace,
                    replay_in_progress,
                );
            }
            Err(e) => {
                // TODO: metrics
                error!(block_number, ?e, "failed to build commit record batch");
            }
        }
    }
}

fn process_reverted_chain<N: reth_primitives_traits::NodePrimitives>(
    chain: &reth_execution_types::Chain<N>,
    ring_buffer: &mut RingBuffer,
    batch_tx: &mpsc::Sender<(Option<BlockNumHash>, RecordBatch)>,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
    replay_in_progress: bool,
) {
    let tip_block = chain.tip().header().number();

    #[allow(clippy::needless_collect)]
    let blocks: Vec<_> = chain.blocks_and_receipts().collect();
    let mut revert_failed = false;

    for (block, receipts) in blocks.into_iter().rev() {
        let block_number = block.header().number();
        let block_hash = block.hash();
        let transactions = block.body().transactions();

        let events = collect_events_from_receipts(receipts, transactions);

        if events.is_empty() {
            continue;
        }

        match crate::arrow::build_record_batch(
            block_number,
            block_hash,
            tip_block,
            BatchOp::Revert,
            &events,
        ) {
            Ok(batch) => {
                if consumer_connected.load(Ordering::Acquire)
                    && !replay_in_progress
                    && batch_tx.try_send((None, batch)).is_err()
                {
                    revert_failed = true;
                    // TODO: metrics
                }
            }
            Err(e) => {
                // TODO: metrics
                error!(block_number, ?e, "failed to build revert record batch");
            }
        }
    }

    if revert_failed {
        warn!("revert delivery failed, disconnecting consumer (corrupt state)");
        // TODO: metrics
        grace.force_disconnect();
    }

    if let Some(first) = chain.blocks_iter().next() {
        ring_buffer.truncate_from(first.header().number());
    }
}

#[allow(clippy::cast_possible_truncation)] // tx/log indices are bounded by block gas limits
fn collect_events_from_receipts<R, T>(receipts: &[R], transactions: &[T]) -> Vec<EventRow>
where
    R: alloy_consensus::TxReceipt<Log = alloy_primitives::Log>,
    T: alloy_consensus::transaction::TxHashRef,
{
    let mut events = Vec::new();
    let mut global_log_index: u32 = 0;

    for (tx_idx, receipt) in receipts.iter().enumerate() {
        let tx_hash = transactions
            .get(tx_idx)
            .map(alloy_consensus::transaction::TxHashRef::tx_hash)
            .copied()
            .unwrap_or_default();

        for log in receipt.logs() {
            match parse_log(log) {
                Ok(Some(entity_event)) => {
                    events.push(EventRow {
                        event: entity_event,
                        tx_index: tx_idx as u32,
                        tx_hash,
                        log_index: global_log_index,
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    // TODO: metrics
                    warn!(tx_index = tx_idx, ?e, "failed to parse log");
                }
            }
            global_log_index += 1;
        }
    }

    events
}

fn try_send_batch(
    batch_tx: &mpsc::Sender<(Option<BlockNumHash>, RecordBatch)>,
    bnh: Option<BlockNumHash>,
    batch: RecordBatch,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
    replay_in_progress: bool,
) {
    if !consumer_connected.load(Ordering::Acquire) {
        return;
    }

    match batch_tx.try_send((bnh, batch)) {
        Ok(()) => {
            // TODO: metrics
            grace.reset();
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            // TODO: metrics
            if replay_in_progress {
                debug!("batch channel full during replay, suppressing disconnect");
            } else {
                debug!("batch channel full, applying backpressure");
                grace.record_failure();
                if grace.should_disconnect {
                    // TODO: metrics
                    warn!("backpressure threshold exceeded in notification loop");
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            debug!("batch channel closed");
        }
    }
}

fn report_finished_height<Node: reth_node_api::FullNodeComponents>(
    ctx: &reth_exex::ExExContext<Node>,
    ring_buffer: &RingBuffer,
    delivered_rx: &watch::Receiver<Option<BlockNumHash>>,
    current_tip: BlockNumHash,
    max_reported: &mut BlockNumHash,
) {
    let consumer_last_delivered = *delivered_rx.borrow();
    let ring_buffer_oldest = ring_buffer.oldest();

    let computed = consumer_last_delivered.unwrap_or_else(|| {
        // No consumer yet — hold at oldest ring buffer entry to prevent WAL pruning
        // of unseen blocks. If ring buffer is empty, report current tip.
        ring_buffer_oldest.unwrap_or(current_tip)
    });

    // Safety cap: never fall behind tip - MAX_BTL to prevent unbounded WAL growth
    let absolute_floor = current_tip.number.saturating_sub(MAX_BTL);
    let height = if computed.number < absolute_floor {
        ring_buffer
            .first_at_or_after(absolute_floor)
            .unwrap_or(current_tip)
    } else {
        computed
    };

    // Monotonic: never report lower than previously reported
    if height.number > max_reported.number {
        *max_reported = height;
        let _ = ctx.send_finished_height(height);
        debug!(number = height.number, "reported finished height");
    }
}

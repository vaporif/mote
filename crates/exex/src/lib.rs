pub mod arrow;
pub mod ring_buffer;
pub mod stream;

use ::arrow::record_batch::RecordBatch;
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use futures::StreamExt;
use futures::future::OptionFuture;
use glint_primitives::constants::MAX_BTL;
use glint_primitives::exex_types::BatchOp;
use reth_primitives_traits::BlockBody;
use ring_buffer::RingBuffer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::arrow::EventRow;
use crate::stream::SnapshotRequest;
use glint_primitives::parse::parse_log;

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
    move |ctx| Box::pin(glint_exex(ctx, socket_path))
}

async fn glint_exex<Node: reth_node_api::FullNodeComponents>(
    mut ctx: reth_exex::ExExContext<Node>,
    socket_path: std::path::PathBuf,
) -> eyre::Result<()> {
    info!(?socket_path, head = ?ctx.head, "glint exex starting");

    let (batch_tx, batch_rx) =
        mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(BATCH_CHANNEL_SIZE);
    let (snapshot_tx, mut snapshot_rx) = mpsc::channel::<SnapshotRequest>(SNAPSHOT_CHANNEL_SIZE);
    let (delivered_tx, delivered_rx) = watch::channel::<Option<BlockNumHash>>(None);

    // TODO: checkpoint persistence — right now we replay from genesis on every
    // restart. Persist the ring buffer and max_reported height to disk.
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
                replay_in_progress = false;
                if result.is_err() {
                    warn!("replay_done channel dropped without completion signal");
                }
            }

            () = cancellation_token.cancelled() => {
                info!("glint exex shutting down via cancellation");
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
                ring_buffer.push(bnh, BatchOp::Commit, batch.clone());
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
    for (block, receipts) in blocks.into_iter().rev() {
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
            BatchOp::Revert,
            &events,
        ) {
            Ok(batch) => {
                ring_buffer.push(bnh, BatchOp::Revert, batch.clone());
                try_send_batch(
                    batch_tx,
                    None,
                    batch,
                    consumer_connected,
                    grace,
                    replay_in_progress,
                );
            }
            Err(e) => {
                // TODO: metrics
                error!(block_number, ?e, "failed to build revert record batch");
            }
        }
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
            global_log_index = global_log_index.saturating_add(1);
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
                    warn!("backpressure threshold exceeded, disconnecting consumer");
                    consumer_connected.store(false, Ordering::Release);
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            debug!("batch channel closed");
            consumer_connected.store(false, Ordering::Release);
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

    // If the consumer told us how far it got, hold there so the WAL keeps
    // everything it hasn't seen. Otherwise just report the tip — the ring
    // buffer keeps its own history for replay.
    let computed = consumer_last_delivered.unwrap_or(current_tip);

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

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_batch() -> RecordBatch {
        crate::arrow::build_watermark_batch(1).unwrap()
    }

    #[test]
    fn backpressure_disconnects_consumer_after_threshold() {
        let (batch_tx, _batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(1);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();

        let _ = batch_tx.try_send((None, dummy_batch()));

        for _ in 0..3 {
            try_send_batch(
                &batch_tx,
                None,
                dummy_batch(),
                &consumer_connected,
                &mut grace,
                false,
            );
        }

        assert!(
            grace.should_disconnect,
            "grace should flag disconnect after threshold"
        );
        assert!(
            !consumer_connected.load(Ordering::Acquire),
            "consumer_connected should be false after backpressure disconnect"
        );
    }

    #[test]
    fn backpressure_suppressed_during_replay() {
        let (batch_tx, _batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(1);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();

        let _ = batch_tx.try_send((None, dummy_batch()));

        for _ in 0..5 {
            try_send_batch(
                &batch_tx,
                None,
                dummy_batch(),
                &consumer_connected,
                &mut grace,
                true, // replay_in_progress
            );
        }

        assert!(
            !grace.should_disconnect,
            "grace should not flag disconnect during replay"
        );
        assert!(
            consumer_connected.load(Ordering::Acquire),
            "consumer should remain connected during replay"
        );
    }

    #[test]
    fn closed_channel_disconnects_consumer() {
        let (batch_tx, batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(1);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();

        drop(batch_rx);

        try_send_batch(
            &batch_tx,
            None,
            dummy_batch(),
            &consumer_connected,
            &mut grace,
            false,
        );

        assert!(
            !consumer_connected.load(Ordering::Acquire),
            "consumer_connected should be false after channel closed"
        );
    }

    #[test]
    fn disconnected_consumer_skips_send() {
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(4);
        let consumer_connected = Arc::new(AtomicBool::new(false));
        let mut grace = stream::GraceState::default();

        try_send_batch(
            &batch_tx,
            None,
            dummy_batch(),
            &consumer_connected,
            &mut grace,
            false,
        );

        assert!(batch_rx.try_recv().is_err());
    }

    #[test]
    fn successful_send_resets_grace() {
        let (batch_tx, _batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(4);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();

        grace.force_disconnect();
        assert!(grace.should_disconnect);

        try_send_batch(
            &batch_tx,
            None,
            dummy_batch(),
            &consumer_connected,
            &mut grace,
            false,
        );

        assert!(!grace.should_disconnect);
    }

    struct MockTx(alloy_primitives::B256);

    impl alloy_consensus::transaction::TxHashRef for MockTx {
        fn tx_hash(&self) -> &alloy_primitives::B256 {
            &self.0
        }
    }

    #[test]
    fn non_glint_logs_are_ignored() {
        use alloy_primitives::{Address, B256, Log, LogData};

        let non_glint_log = Log {
            address: Address::repeat_byte(0xff),
            data: LogData::new_unchecked(vec![B256::ZERO], Default::default()),
        };

        let receipt = alloy_consensus::Receipt {
            status: alloy_consensus::Eip658Value::Eip658(true),
            cumulative_gas_used: 0,
            logs: vec![non_glint_log.clone(), non_glint_log],
        };

        let tx = MockTx(B256::repeat_byte(0x01));
        let events = collect_events_from_receipts(&[receipt], &[tx]);
        assert!(events.is_empty());
    }
}

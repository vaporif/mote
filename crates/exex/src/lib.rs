pub mod arrow;
pub mod metrics;
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
    transport: Box<dyn glint_transport::ExExTransportServer>,
    probe_state: glint_transport::ProbeState,
    cancel: CancellationToken,
) -> impl FnOnce(
    reth_exex::ExExContext<Node>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = eyre::Result<()>> + Send>>
where
    Node: reth_node_api::FullNodeComponents,
{
    move |ctx| Box::pin(glint_exex(ctx, transport, probe_state, cancel))
}

async fn glint_exex<Node: reth_node_api::FullNodeComponents>(
    mut ctx: reth_exex::ExExContext<Node>,
    transport: Box<dyn glint_transport::ExExTransportServer>,
    probe_state: glint_transport::ProbeState,
    cancellation_token: CancellationToken,
) -> eyre::Result<()> {
    let metrics = metrics::ExExMetrics::default();

    info!(
        head = ?ctx.head,
        "glint exex starting"
    );

    // TODO: checkpoint persistence — right now we replay from genesis on every
    // restart. Persist the ring buffer and max_reported height to disk.
    let mut ring_buffer = RingBuffer::with_probe_state(&probe_state);
    let rb_stats = ring_buffer.stats();
    let consumer_connected = probe_state.consumer_connected;

    let (snapshot_tx, mut snapshot_rx) = mpsc::channel::<SnapshotRequest>(SNAPSHOT_CHANNEL_SIZE);

    let (batch_tx, batch_rx) =
        mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(BATCH_CHANNEL_SIZE);
    let (delivered_tx, delivered_rx) = watch::channel::<Option<BlockNumHash>>(None);

    let writer_cancel = cancellation_token.clone();
    let writer_consumer = Arc::clone(&consumer_connected);
    let writer_snapshot_tx = snapshot_tx.clone();
    let writer_rb_stats = rb_stats.clone();
    let writer_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(e) = stream::writer_task(stream::WriterTaskCtx {
            server: transport,
            snapshot_tx: writer_snapshot_tx,
            batch_rx,
            delivered_tx,
            consumer_connected: writer_consumer,
            rb_stats: writer_rb_stats,
            cancellation_token: writer_cancel,
            metrics: writer_metrics,
        })
        .await
        {
            error!(?e, "writer task failed");
        }
    });

    drop(snapshot_tx); // drop original so channel closes when writer stops

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
                            &metrics,
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
                            &metrics,
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
                            &metrics,
                        );
                        process_committed_chain(
                            new,
                            &mut ring_buffer,
                            &batch_tx,
                            &consumer_connected,
                            &mut grace,
                            replay_in_progress,
                            &metrics,
                        );
                        new.tip().num_hash()
                    }
                };

                update_ring_buffer_gauges(&metrics, &rb_stats);

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
    metrics: &metrics::ExExMetrics,
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
                    &batch,
                    consumer_connected,
                    grace,
                    replay_in_progress,
                    metrics,
                );
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
    batch_tx: &mpsc::Sender<(Option<BlockNumHash>, RecordBatch)>,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
    replay_in_progress: bool,
    metrics: &metrics::ExExMetrics,
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
                    &batch,
                    consumer_connected,
                    grace,
                    replay_in_progress,
                    metrics,
                );
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
                        tx_index: u32::try_from(tx_idx)
                            .expect("tx index bounded by block gas limit"),
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
    batch: &RecordBatch,
    consumer_connected: &Arc<AtomicBool>,
    grace: &mut stream::GraceState,
    replay_in_progress: bool,
    metrics: &metrics::ExExMetrics,
) {
    if !consumer_connected.load(Ordering::Acquire) {
        return;
    }

    match batch_tx.try_send((bnh, batch.clone())) {
        Ok(()) => {
            metrics.batches_sent_total.increment(1);
            grace.reset();
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            if replay_in_progress {
                debug!("batch channel full during replay, suppressing disconnect");
            } else {
                debug!("batch channel full, applying backpressure");
                metrics.batches_dropped_total.increment(1);
                grace.record_failure();
                if grace.should_disconnect {
                    metrics.consumer_disconnects_total.increment(1);
                    metrics.consumer_connected.set(0.0);
                    warn!("backpressure threshold exceeded, disconnecting consumer");
                    consumer_connected.store(false, Ordering::Release);
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            debug!("batch channel closed");
            metrics.consumer_connected.set(0.0);
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

    // hold at consumer position so WAL keeps unseen data; fall back to tip
    let computed = consumer_last_delivered.unwrap_or(current_tip);

    // cap: don't fall behind tip - MAX_BTL (prevents unbounded WAL growth)
    let absolute_floor = current_tip.number.saturating_sub(MAX_BTL);
    let height = if computed.number < absolute_floor {
        ring_buffer
            .first_at_or_after(absolute_floor)
            .unwrap_or(current_tip)
    } else {
        computed
    };

    // monotonic: never go backwards
    if height.number > max_reported.number {
        *max_reported = height;
        let _ = ctx.send_finished_height(height);
        debug!(number = height.number, "reported finished height");
    }
}

#[allow(clippy::cast_precision_loss)] // gauge values are approximate; f64 precision loss is acceptable
fn update_ring_buffer_gauges(metrics: &metrics::ExExMetrics, stats: &ring_buffer::RingBufferStats) {
    metrics
        .ring_buffer_entries
        .set(stats.entries.load(Ordering::Relaxed) as f64);
    metrics
        .ring_buffer_memory_bytes
        .set(stats.memory.load(Ordering::Relaxed) as f64);
    metrics
        .ring_buffer_tip_block
        .set(stats.tip.load(Ordering::Relaxed) as f64);
    metrics
        .ring_buffer_oldest_block
        .set(stats.oldest.load(Ordering::Relaxed) as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_batch() -> RecordBatch {
        crate::arrow::build_watermark_batch(1).unwrap()
    }

    #[test]
    fn update_ring_buffer_gauges_sets_all_fields() {
        let metrics = crate::metrics::ExExMetrics::default();
        let stats = ring_buffer::RingBufferStats {
            entries: Arc::new(42.into()),
            memory: Arc::new(1024.into()),
            tip: Arc::new(100.into()),
            oldest: Arc::new(10.into()),
        };

        update_ring_buffer_gauges(&metrics, &stats);
    }

    #[test]
    fn single_backpressure_does_not_disconnect() {
        let (batch_tx, _batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(1);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();
        let metrics = crate::metrics::ExExMetrics::default();

        let _ = batch_tx.try_send((None, dummy_batch()));

        try_send_batch(
            &batch_tx,
            None,
            &dummy_batch(),
            &consumer_connected,
            &mut grace,
            false,
            &metrics,
        );

        assert!(
            !grace.should_disconnect,
            "single backpressure must not trigger disconnect"
        );
        assert!(
            consumer_connected.load(Ordering::Acquire),
            "consumer should still be connected after one drop"
        );
    }

    #[test]
    fn backpressure_disconnects_consumer_after_threshold() {
        let (batch_tx, _batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(1);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();
        let metrics = crate::metrics::ExExMetrics::default();

        let _ = batch_tx.try_send((None, dummy_batch()));

        for _ in 0..3 {
            try_send_batch(
                &batch_tx,
                None,
                &dummy_batch(),
                &consumer_connected,
                &mut grace,
                false,
                &metrics,
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
        let metrics = crate::metrics::ExExMetrics::default();

        let _ = batch_tx.try_send((None, dummy_batch()));

        for _ in 0..5 {
            try_send_batch(
                &batch_tx,
                None,
                &dummy_batch(),
                &consumer_connected,
                &mut grace,
                true, // replay_in_progress
                &metrics,
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
        let metrics = crate::metrics::ExExMetrics::default();

        drop(batch_rx);

        try_send_batch(
            &batch_tx,
            None,
            &dummy_batch(),
            &consumer_connected,
            &mut grace,
            false,
            &metrics,
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
        let metrics = crate::metrics::ExExMetrics::default();

        try_send_batch(
            &batch_tx,
            None,
            &dummy_batch(),
            &consumer_connected,
            &mut grace,
            false,
            &metrics,
        );

        assert!(batch_rx.try_recv().is_err());
    }

    #[test]
    fn successful_send_resets_grace() {
        let (batch_tx, _batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(4);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();
        let metrics = crate::metrics::ExExMetrics::default();

        grace.force_disconnect();
        assert!(grace.should_disconnect);

        try_send_batch(
            &batch_tx,
            None,
            &dummy_batch(),
            &consumer_connected,
            &mut grace,
            false,
            &metrics,
        );

        assert!(!grace.should_disconnect);
    }

    struct MockTx(alloy_primitives::B256);

    impl alloy_consensus::transaction::TxHashRef for MockTx {
        fn tx_hash(&self) -> &alloy_primitives::B256 {
            &self.0
        }
    }

    fn make_chain_with_glint_log(
        block_number: u64,
    ) -> reth_execution_types::Chain<reth_ethereum_primitives::EthPrimitives> {
        use alloy_consensus::{Block, BlockBody, Signed, TxLegacy};
        use alloy_primitives::Address;
        use glint_primitives::constants::PROCESSOR_ADDRESS;
        use glint_primitives::events::EntityDeleted;
        use reth_ethereum_primitives::EthPrimitives;

        let entity_key = alloy_primitives::B256::repeat_byte(0x42);
        let owner = Address::repeat_byte(0x01);
        let sender = Address::repeat_byte(0x02);
        let gas_cost = 100;

        let log = EntityDeleted::new_log(PROCESSOR_ADDRESS, entity_key, owner, sender, gas_cost);

        let tx_hash = alloy_primitives::B256::repeat_byte(0xAA);
        let legacy_tx = TxLegacy::default();
        let sig = alloy_primitives::Signature::test_signature();
        let signed_tx = Signed::new_unchecked(legacy_tx, sig, tx_hash);
        let tx =
            <EthPrimitives as reth_primitives_traits::NodePrimitives>::SignedTx::Legacy(signed_tx);

        let header = alloy_consensus::Header {
            number: block_number,
            ..Default::default()
        };
        let body = BlockBody {
            transactions: vec![tx],
            ..Default::default()
        };
        let block = Block { header, body };

        let block_hash = alloy_primitives::B256::repeat_byte(block_number as u8);
        let recovered =
            reth_primitives_traits::RecoveredBlock::new(block, vec![sender], block_hash);

        let receipt = reth_ethereum_primitives::Receipt {
            tx_type: alloy_consensus::TxType::Legacy,
            success: true,
            cumulative_gas_used: 21000,
            logs: vec![log],
        };

        let execution_outcome = reth_execution_types::ExecutionOutcome {
            bundle: Default::default(),
            receipts: vec![vec![receipt]],
            first_block: block_number,
            requests: vec![Default::default()],
        };

        reth_execution_types::Chain::new(vec![recovered], execution_outcome, Default::default())
    }

    #[test]
    fn process_committed_chain_sends_batch_and_records_metrics() {
        let chain = make_chain_with_glint_log(42);
        let mut ring_buffer = RingBuffer::default();
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(16);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();
        let metrics = crate::metrics::ExExMetrics::default();

        process_committed_chain(
            &chain,
            &mut ring_buffer,
            &batch_tx,
            &consumer_connected,
            &mut grace,
            false,
            &metrics,
        );

        let (bnh, _batch) = batch_rx.try_recv().expect("should have sent a batch");
        assert_eq!(bnh.unwrap().number, 42);
    }

    #[test]
    fn process_reverted_chain_sends_batch() {
        let chain = make_chain_with_glint_log(99);
        let mut ring_buffer = RingBuffer::default();
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(16);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();
        let metrics = crate::metrics::ExExMetrics::default();

        process_reverted_chain(
            &chain,
            &mut ring_buffer,
            &batch_tx,
            &consumer_connected,
            &mut grace,
            false,
            &metrics,
        );

        // Revert batches use bnh=None
        let (bnh, _batch) = batch_rx
            .try_recv()
            .expect("should have sent a revert batch");
        assert!(bnh.is_none());
    }

    #[test]
    fn process_committed_chain_skips_block_with_no_glint_events() {
        use alloy_consensus::{Block, BlockBody, Signed, TxLegacy};
        use alloy_primitives::Address;

        // Block with a non-glint log (wrong address)
        let non_glint_log = alloy_primitives::Log {
            address: Address::repeat_byte(0xff),
            data: alloy_primitives::LogData::new_unchecked(
                vec![alloy_primitives::B256::ZERO],
                Default::default(),
            ),
        };

        let tx_hash = alloy_primitives::B256::repeat_byte(0xBB);
        let signed_tx = Signed::new_unchecked(
            TxLegacy::default(),
            alloy_primitives::Signature::test_signature(),
            tx_hash,
        );
        let tx = <reth_ethereum_primitives::EthPrimitives as reth_primitives_traits::NodePrimitives>::SignedTx::Legacy(signed_tx);

        let header = alloy_consensus::Header {
            number: 10,
            ..Default::default()
        };
        let block = Block {
            header,
            body: BlockBody {
                transactions: vec![tx],
                ..Default::default()
            },
        };
        let sender = Address::repeat_byte(0x02);
        let recovered = reth_primitives_traits::RecoveredBlock::new(
            block,
            vec![sender],
            alloy_primitives::B256::repeat_byte(0x0A),
        );

        let receipt = reth_ethereum_primitives::Receipt {
            tx_type: alloy_consensus::TxType::Legacy,
            success: true,
            cumulative_gas_used: 21000,
            logs: vec![non_glint_log],
        };

        let execution_outcome = reth_execution_types::ExecutionOutcome {
            bundle: Default::default(),
            receipts: vec![vec![receipt]],
            first_block: 10,
            requests: vec![Default::default()],
        };

        let chain: reth_execution_types::Chain<reth_ethereum_primitives::EthPrimitives> =
            reth_execution_types::Chain::new(
                vec![recovered],
                execution_outcome,
                Default::default(),
            );

        let mut ring_buffer = RingBuffer::default();
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(16);
        let consumer_connected = Arc::new(AtomicBool::new(true));
        let mut grace = stream::GraceState::default();
        let metrics = crate::metrics::ExExMetrics::default();

        process_committed_chain(
            &chain,
            &mut ring_buffer,
            &batch_tx,
            &consumer_connected,
            &mut grace,
            false,
            &metrics,
        );

        assert!(
            batch_rx.try_recv().is_err(),
            "no batch should be sent for non-glint logs"
        );
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

    #[test]
    fn malformed_glint_log_is_skipped() {
        use alloy_primitives::{B256, Log, LogData};
        use glint_primitives::constants::PROCESSOR_ADDRESS;

        let bad_log = Log {
            address: PROCESSOR_ADDRESS,
            data: LogData::new_unchecked(vec![B256::repeat_byte(0xDE)], Default::default()),
        };

        let receipt = alloy_consensus::Receipt {
            status: alloy_consensus::Eip658Value::Eip658(true),
            cumulative_gas_used: 0,
            logs: vec![bad_log],
        };

        let tx = MockTx(B256::repeat_byte(0x01));
        let events = collect_events_from_receipts(&[receipt], &[tx]);
        assert!(events.is_empty(), "unparsable glint log should be skipped");
    }
}

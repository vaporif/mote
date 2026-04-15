use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use alloy_eips::BlockNumHash;
use arrow::record_batch::RecordBatch;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::arrow::build_watermark_batch;
use crate::ring_buffer::RingBufferStats;
use glint_primitives::exex_schema::columns;

pub struct SnapshotRequest {
    pub resume_block: u64,
    pub reply_tx: oneshot::Sender<Vec<(BlockNumHash, RecordBatch)>>,
    pub replay_done_rx: oneshot::Receiver<()>,
}

const DEFAULT_GRACE_WINDOW: Duration = Duration::from_secs(1);
const DEFAULT_GRACE_THRESHOLD: u32 = 3;

#[derive(Debug)]
pub struct GraceState {
    first_failure_time: Option<Instant>,
    failure_count: u32,
    pub should_disconnect: bool,
    window: Duration,
    threshold: u32,
}

impl Default for GraceState {
    fn default() -> Self {
        Self {
            first_failure_time: None,
            failure_count: 0,
            should_disconnect: false,
            window: DEFAULT_GRACE_WINDOW,
            threshold: DEFAULT_GRACE_THRESHOLD,
        }
    }
}

impl GraceState {
    pub fn record_failure(&mut self) {
        let now = Instant::now();
        match self.first_failure_time {
            None => {
                self.first_failure_time = Some(now);
                self.failure_count = 1;
            }
            Some(first) if now.duration_since(first) > self.window => {
                self.first_failure_time = Some(now);
                self.failure_count = 1;
            }
            Some(_) => {
                self.failure_count += 1;
            }
        }
        if self.failure_count >= self.threshold {
            self.should_disconnect = true;
        }
    }

    pub const fn force_disconnect(&mut self) {
        self.should_disconnect = true;
    }

    pub const fn reset(&mut self) {
        self.first_failure_time = None;
        self.failure_count = 0;
        self.should_disconnect = false;
    }
}

const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn writer_task(
    server: Box<dyn glint_transport::ExExTransportServer>,
    snapshot_tx: mpsc::Sender<SnapshotRequest>,
    mut batch_rx: mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
    delivered_tx: watch::Sender<Option<BlockNumHash>>,
    consumer_connected: Arc<AtomicBool>,
    rb_stats: RingBufferStats,
    cancellation_token: CancellationToken,
) -> eyre::Result<()> {
    loop {
        let conn = match server.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                if cancellation_token.is_cancelled() {
                    info!("writer task shutting down");
                    break;
                }
                warn!(?e, "accept error");
                continue;
            }
        };

        info!("new connection accepted");
        consumer_connected.store(true, Ordering::Release);

        match handle_transport_connection(
            conn,
            &snapshot_tx,
            &mut batch_rx,
            &delivered_tx,
            &rb_stats,
            &cancellation_token,
        )
        .await
        {
            Ok(()) => {
                debug!("connection handler completed normally");
            }
            Err(e) => {
                warn!(?e, "connection handler error");
            }
        }

        consumer_connected.store(false, Ordering::Release);
        // TODO: metrics
    }

    Ok(())
}

async fn handle_transport_connection(
    mut conn: Box<dyn glint_transport::ExExConnection>,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
    delivered_tx: &watch::Sender<Option<BlockNumHash>>,
    rb_stats: &RingBufferStats,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let resume_block = conn.recv_subscribe().await?;
    // TODO: metrics
    debug!(resume_block, "subscribe received");

    let oldest = rb_stats.oldest.load(Ordering::Relaxed);
    let tip = rb_stats.tip.load(Ordering::Relaxed);
    conn.send_handshake(oldest, tip).await?;

    let replay_tip = replay_via_transport(&mut *conn, resume_block, snapshot_tx, batch_rx).await?;

    stream_live_via_transport(&mut *conn, batch_rx, delivered_tx, cancel, replay_tip).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BatchKey {
    block_number: u64,
    block_hash: [u8; 32],
    op: u8,
}

fn batch_dedup_key(batch: &RecordBatch) -> Option<BatchKey> {
    if batch.num_rows() == 0 {
        return None;
    }
    let block_number = batch
        .column_by_name(columns::BLOCK_NUMBER)
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt64Array>())
        .map(|a| a.value(0))?;
    let block_hash = batch
        .column_by_name(columns::BLOCK_HASH)
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        })
        .map(|a| {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(a.value(0));
            arr
        })?;
    let op = batch
        .column_by_name(columns::OP)
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::UInt8Array>())
        .map(|a| a.value(0))?;
    Some(BatchKey {
        block_number,
        block_hash,
        op,
    })
}

async fn replay_via_transport(
    conn: &mut dyn glint_transport::ExExConnection,
    resume_block: u64,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
) -> eyre::Result<u64> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let (replay_done_tx, replay_done_rx) = oneshot::channel();

    snapshot_tx
        .send(SnapshotRequest {
            resume_block,
            reply_tx,
            replay_done_rx,
        })
        .await
        .map_err(|_| eyre::eyre!("snapshot channel closed"))?;

    let snapshot = reply_rx
        .await
        .map_err(|_| eyre::eyre!("snapshot reply channel dropped"))?;

    info!(
        resume_block,
        snapshot_len = snapshot.len(),
        "received snapshot for replay"
    );

    // drain batches that arrived during snapshot
    let mut buffered = Vec::new();
    while let Ok(item) = batch_rx.try_recv() {
        buffered.push(item);
    }

    // dedup against snapshot
    let snapshot_keys: HashSet<BatchKey> = snapshot
        .iter()
        .filter_map(|(_, batch)| batch_dedup_key(batch))
        .collect();

    let last_bnh = snapshot.last().map(|(bnh, _)| *bnh);
    for (_, batch) in &snapshot {
        conn.send_batch(batch).await?;
    }

    let mut tip = last_bnh.map_or(resume_block, |bnh| bnh.number);
    let watermark = build_watermark_batch(tip)?;
    conn.send_batch(&watermark).await?;

    // forward non-duplicate buffered batches
    for (maybe_bnh, batch) in buffered {
        let dominated = batch_dedup_key(&batch).is_some_and(|key| snapshot_keys.contains(&key));
        if !dominated {
            conn.send_batch(&batch).await?;
            if let Some(bnh) = maybe_bnh {
                tip = bnh.number;
            }
        }
    }

    let _ = replay_done_tx.send(());

    info!(tip, "snapshot replay complete, entering live stream");

    Ok(tip)
}

async fn stream_live_via_transport(
    conn: &mut dyn glint_transport::ExExConnection,
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
    delivered_tx: &watch::Sender<Option<BlockNumHash>>,
    cancel: &CancellationToken,
    initial_tip: u64,
) -> eyre::Result<()> {
    let mut current_tip = initial_tip;

    loop {
        let (maybe_bnh, batch) = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                debug!("live loop cancelled, draining remaining batches");
                while let Ok((_, batch)) = batch_rx.try_recv() {
                    if conn.send_batch(&batch).await.is_err() {
                        break;
                    }
                }
                if let Ok(wm) = build_watermark_batch(current_tip) {
                    let _ = conn.send_batch(&wm).await;
                }
                conn.finish().await?;
                return Ok(());
            }
            maybe_batch = batch_rx.recv() => {
                if let Some(item) = maybe_batch {
                    item
                } else {
                    debug!("batch channel closed");
                    conn.finish().await?;
                    return Ok(());
                }
            }
        };

        match tokio::time::timeout(SEND_TIMEOUT, conn.send_batch(&batch)).await {
            Ok(Ok(())) => {
                if let Some(bnh) = maybe_bnh {
                    current_tip = bnh.number;
                    let _ = delivered_tx.send(Some(bnh));
                }
            }
            Ok(Err(e)) => {
                warn!(?e, "send_batch failed, consumer disconnected");
                return Ok(());
            }
            Err(_) => {
                warn!("send timed out, disconnecting slow consumer");
                let _ = conn.finish().await;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glint_primitives::exex_types::BatchOp;
    use glint_transport::ExExTransportServer;

    fn make_test_batch(block: u64, hash_byte: u8, op: BatchOp) -> (BlockNumHash, RecordBatch) {
        let hash = alloy_primitives::B256::repeat_byte(hash_byte);
        let bnh = BlockNumHash::new(block, hash);
        let batch = crate::arrow::build_record_batch(
            block,
            hash,
            block,
            op,
            &[crate::arrow::EventRow {
                event: glint_primitives::parse::EntityEvent::Deleted {
                    entity_key: alloy_primitives::B256::repeat_byte(0x01),
                    owner: alloy_primitives::Address::ZERO,
                    sender: alloy_primitives::Address::ZERO,
                    gas_cost: 100,
                },
                tx_index: 0,
                tx_hash: alloy_primitives::B256::ZERO,
                log_index: 0,
            }],
        )
        .unwrap();
        (bnh, batch)
    }

    async fn run_replay_test(
        batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
        snapshot: Vec<(BlockNumHash, RecordBatch)>,
    ) -> (u64, Vec<RecordBatch>) {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("test.sock");
        let cancel = CancellationToken::new();
        let ipc_server = glint_transport::ipc::IpcServer::new(
            sock_path.clone(),
            glint_transport::ProbeState::default(),
            cancel.clone(),
        )
        .unwrap();

        let (snapshot_tx, mut snapshot_rx) = mpsc::channel::<SnapshotRequest>(1);
        let snapshot_fulfiller = tokio::spawn(async move {
            let req = snapshot_rx.recv().await.unwrap();
            let _ = req.reply_tx.send(snapshot);
            let _ = req.replay_done_rx.await;
        });

        // Spawn client reader in background
        let client_handle = tokio::spawn(async move {
            use futures::StreamExt;
            let client: Box<dyn glint_transport::ExExTransportClient> =
                Box::new(glint_transport::ipc::IpcClient::new(sock_path));
            let (_, mut stream) = client.subscribe(0).await.unwrap();
            let mut received = Vec::new();
            while let Some(result) = stream.next().await {
                received.push(result.unwrap());
            }
            received
        });

        // Server side: accept, handshake, replay, close
        let mut conn = ipc_server.accept().await.unwrap();
        let resume = conn.recv_subscribe().await.unwrap();
        conn.send_handshake(0, 0).await.unwrap();
        let tip = replay_via_transport(&mut *conn, resume, &snapshot_tx, batch_rx)
            .await
            .unwrap();
        conn.finish().await.unwrap();

        let received = client_handle.await.unwrap();
        snapshot_fulfiller.await.unwrap();

        (tip, received)
    }

    #[test]
    fn backpressure_no_disconnect_under_threshold() {
        let mut state = GraceState::default();
        state.record_failure();
        state.record_failure();
        assert!(!state.should_disconnect);
    }

    #[test]
    fn backpressure_disconnect_at_threshold() {
        let mut state = GraceState::default();
        state.record_failure();
        state.record_failure();
        state.record_failure();
        assert!(state.should_disconnect);
    }

    #[test]
    fn grace_reset_clears_state() {
        let mut state = GraceState::default();
        state.record_failure();
        state.record_failure();
        state.record_failure();
        assert!(state.should_disconnect);

        state.reset();
        assert!(!state.should_disconnect);
        assert_eq!(state.failure_count, 0);
        assert!(state.first_failure_time.is_none());
    }

    #[test]
    fn force_disconnect_sets_flag() {
        let mut state = GraceState::default();
        assert!(!state.should_disconnect);
        state.force_disconnect();
        assert!(state.should_disconnect);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_live_delivers_all_batches() {
        use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("test.sock");
        let cancel = CancellationToken::new();
        let ipc_server = glint_transport::ipc::IpcServer::new(
            sock_path.clone(),
            glint_transport::ProbeState::default(),
            cancel.clone(),
        )
        .unwrap();

        let (batch_tx, mut batch_rx) = mpsc::channel(16);
        let (delivered_tx, mut delivered_rx) = watch::channel(None);

        let writer_count = Arc::new(AtomicU32::new(0));
        let writer_count_clone = Arc::clone(&writer_count);

        // Connect client that counts received batches
        let client_handle = tokio::spawn(async move {
            use futures::StreamExt;
            let client: Box<dyn glint_transport::ExExTransportClient> =
                Box::new(glint_transport::ipc::IpcClient::new(sock_path));
            let (_, mut stream) = client.subscribe(0).await.unwrap();
            while let Some(result) = stream.next().await {
                let _ = result.unwrap();
                writer_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
            }
        });

        // Run server in current task
        let mut conn = ipc_server.accept().await.unwrap();
        let _resume = conn.recv_subscribe().await.unwrap();
        conn.send_handshake(0, 0).await.unwrap();

        let cancel_clone = cancel.clone();
        let server_handle = tokio::spawn(async move {
            stream_live_via_transport(&mut *conn, &mut batch_rx, &delivered_tx, &cancel_clone, 0)
                .await
                .unwrap();
        });

        let batch = build_watermark_batch(1).unwrap();
        let bnh1 = BlockNumHash::new(10, alloy_primitives::B256::repeat_byte(0x0A));
        let bnh2 = BlockNumHash::new(20, alloy_primitives::B256::repeat_byte(0x14));
        let bnh3 = BlockNumHash::new(30, alloy_primitives::B256::repeat_byte(0x1E));

        batch_tx.send((Some(bnh1), batch.clone())).await.unwrap();
        batch_tx.send((Some(bnh2), batch.clone())).await.unwrap();
        batch_tx.send((Some(bnh3), batch.clone())).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                delivered_rx.changed().await.unwrap();
                if delivered_rx.borrow().is_some_and(|bnh| bnh.number == 30) {
                    break;
                }
            }
        })
        .await
        .expect("delivered_tx should advance to block 30");

        assert_eq!(delivered_rx.borrow().unwrap().number, 30);

        cancel.cancel();
        server_handle.await.unwrap();
        client_handle.await.unwrap();

        // 3 batches + 1 shutdown watermark
        assert_eq!(
            writer_count.load(AtomicOrdering::Relaxed),
            4,
            "writer must receive all 3 batches + 1 shutdown watermark (no drops)"
        );
    }

    #[test]
    fn batch_dedup_key_extracts_commit() {
        let (_, batch) = make_test_batch(42, 0xAB, BatchOp::Commit);
        let key = batch_dedup_key(&batch).unwrap();
        assert_eq!(key.block_number, 42);
        assert_eq!(key.block_hash, alloy_primitives::B256::repeat_byte(0xAB).0);
        assert_eq!(key.op, BatchOp::Commit as u8);
    }

    #[test]
    fn batch_dedup_key_extracts_revert() {
        let (_, batch) = make_test_batch(99, 0xCD, BatchOp::Revert);
        let key = batch_dedup_key(&batch).unwrap();
        assert_eq!(key.block_number, 99);
        assert_eq!(key.op, BatchOp::Revert as u8);
    }

    #[test]
    fn batch_dedup_key_watermark() {
        let batch = build_watermark_batch(500).unwrap();
        let key = batch_dedup_key(&batch).unwrap();
        assert_eq!(key.block_number, 0);
        assert_eq!(key.block_hash, [0u8; 32]);
        assert_eq!(key.op, 0xFF);
    }

    #[test]
    fn batch_dedup_key_empty_batch_returns_none() {
        let schema = glint_primitives::exex_schema::entity_events_schema();
        let batch = RecordBatch::new_empty(schema);
        assert!(batch_dedup_key(&batch).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_buffers_and_forwards_non_snapshot_batches() {
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        let (bnh1, batch1) = make_test_batch(1, 0x01, BatchOp::Commit);
        let (bnh2, batch2) = make_test_batch(2, 0x02, BatchOp::Commit);
        let (bnh3, batch3) = make_test_batch(3, 0x03, BatchOp::Commit);

        batch_tx.send((Some(bnh1), batch1.clone())).await.unwrap();
        batch_tx.send((Some(bnh2), batch2.clone())).await.unwrap();
        batch_tx.send((Some(bnh3), batch3.clone())).await.unwrap();

        let snapshot = vec![(bnh1, batch1), (bnh2, batch2)];
        let (tip, received) = run_replay_test(&mut batch_rx, snapshot).await;

        assert_eq!(tip, 3, "tip should advance to block 3 from buffered batch");
        assert_eq!(
            received.len(),
            4,
            "expected snapshot(2) + watermark(1) + buffered(1)"
        );

        let last = &received[3];
        let bn = last
            .column_by_name(columns::BLOCK_NUMBER)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(bn, 3, "forwarded batch should be block 3");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_filters_snapshot_covered_batches() {
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        let (bnh1, batch1) = make_test_batch(1, 0x01, BatchOp::Commit);
        let (bnh2, batch2) = make_test_batch(2, 0x02, BatchOp::Commit);

        // preload channel with same batches as snapshot
        batch_tx.send((Some(bnh1), batch1.clone())).await.unwrap();
        batch_tx.send((Some(bnh2), batch2.clone())).await.unwrap();

        let snapshot = vec![(bnh1, batch1), (bnh2, batch2)];
        let (tip, received) = run_replay_test(&mut batch_rx, snapshot).await;

        assert_eq!(tip, 2);
        // snapshot + watermark only, duplicates filtered
        assert_eq!(
            received.len(),
            3,
            "expected snapshot(2) + watermark(1), no duplicates"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_forwards_reorg_batches_with_different_hash() {
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        // snapshot: block 100, hash 0xAA
        let (bnh100, batch100) = make_test_batch(100, 0xAA, BatchOp::Commit);

        // channel: revert(100, 0xAA) + commit(100', 0xBB)
        let (_, revert_batch) = make_test_batch(100, 0xAA, BatchOp::Revert);
        let (bnh100_prime, commit_batch) = make_test_batch(100, 0xBB, BatchOp::Commit);

        batch_tx.send((None, revert_batch)).await.unwrap();
        batch_tx
            .send((Some(bnh100_prime), commit_batch))
            .await
            .unwrap();

        let snapshot = vec![(bnh100, batch100)];
        let (tip, received) = run_replay_test(&mut batch_rx, snapshot).await;

        assert_eq!(tip, 100, "tip should be 100 from reorg commit");
        // snapshot + watermark + revert + commit = 4
        assert_eq!(received.len(), 4, "reorg batches should be forwarded");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_filters_duplicate_revert_in_buffer() {
        let (batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        // snapshot has revert for block 5
        let (bnh5, revert5) = make_test_batch(5, 0x05, BatchOp::Revert);

        // same revert in channel (duplicate)
        batch_tx.send((None, revert5.clone())).await.unwrap();

        let snapshot = vec![(bnh5, revert5)];
        let (_, received) = run_replay_test(&mut batch_rx, snapshot).await;

        // snapshot + watermark, duplicate revert filtered
        assert_eq!(
            received.len(),
            2,
            "duplicate revert should be filtered, not forwarded"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_empty_buffer_is_noop() {
        let (_batch_tx, mut batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        let (tip, received) = run_replay_test(&mut batch_rx, vec![]).await;

        assert_eq!(tip, 0, "tip should be resume_block with empty snapshot");
        assert_eq!(
            received.len(),
            1,
            "only watermark with empty snapshot and empty buffer"
        );
    }
}

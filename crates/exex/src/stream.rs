use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use alloy_eips::BlockNumHash;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use eyre::ensure;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::arrow::build_watermark_batch;
use crate::ring_buffer::RingBufferStats;
use glint_primitives::exex_schema::{columns, entity_events_schema};

const SUBSCRIBE_MSG_SIZE: usize = 9; // 1 byte msg_type + 8 bytes resume_block

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Probe,
    Subscribe { resume_block: u64 },
    Unknown(u8),
}

impl ClientMessage {
    pub fn parse(buf: &[u8]) -> eyre::Result<Self> {
        ensure!(!buf.is_empty(), "client message buffer is empty");

        let msg_type = buf[0];
        match msg_type {
            0 => Ok(Self::Probe),
            1 => {
                ensure!(
                    buf.len() >= SUBSCRIBE_MSG_SIZE,
                    "Subscribe message requires {SUBSCRIBE_MSG_SIZE} bytes, got {}",
                    buf.len()
                );
                let resume_block =
                    u64::from_le_bytes(buf[1..SUBSCRIBE_MSG_SIZE].try_into().unwrap_or_default());
                Ok(Self::Subscribe { resume_block })
            }
            other => Ok(Self::Unknown(other)),
        }
    }
}

#[derive(Debug, Clone, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub struct ProbeResponse {
    pub consumer_connected: bool,
    pub tip_block: u64,
    pub oldest_block: u64,
    pub ring_buffer_entries: u64,
    pub ring_buffer_memory_bytes: u64,
}

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

const WRITER_CHANNEL_SIZE: usize = 16;
const MAX_CLIENT_MSG_LEN: usize = 64;
pub const HANDSHAKE_RESPONSE_SIZE: usize = 17; // 1 + 8 + 8

pub async fn socket_writer_task(
    socket_path: PathBuf,
    snapshot_tx: mpsc::Sender<SnapshotRequest>,
    mut batch_rx: mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
    delivered_tx: watch::Sender<Option<BlockNumHash>>,
    consumer_connected: Arc<AtomicBool>,
    rb_stats: RingBufferStats,
    cancellation_token: CancellationToken,
) -> eyre::Result<()> {
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!(?socket_path, "unix socket listener bound");

    loop {
        let stream = tokio::select! {
            biased;
            () = cancellation_token.cancelled() => {
                info!("socket writer shutting down");
                break;
            }
            result = listener.accept() => {
                let (stream, _addr) = result?;
                stream
            }
        };

        info!("new connection accepted");

        match handle_connection(
            stream,
            &snapshot_tx,
            &mut batch_rx,
            &delivered_tx,
            &consumer_connected,
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

    let _ = std::fs::remove_file(&socket_path);
    Ok(())
}

async fn handle_connection(
    mut stream: UnixStream,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
    delivered_tx: &watch::Sender<Option<BlockNumHash>>,
    consumer_connected: &Arc<AtomicBool>,
    rb_stats: &RingBufferStats,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let mut buf = vec![0u8; MAX_CLIENT_MSG_LEN];
    let n = stream.read(&mut buf).await?;
    ensure!(n > 0, "client disconnected before sending message");
    buf.truncate(n);

    let msg = ClientMessage::parse(&buf)?;
    debug!(?msg, "parsed client message");

    match msg {
        ClientMessage::Probe => handle_probe(&mut stream, consumer_connected, rb_stats).await,
        ClientMessage::Unknown(v) => {
            warn!(msg_type = v, "unknown client message type");
            stream.write_all(&[0xFF]).await?;
            stream.shutdown().await?;
            Ok(())
        }
        ClientMessage::Subscribe { resume_block } => {
            consumer_connected.store(true, Ordering::Release);
            // TODO: metrics

            let oldest = rb_stats.oldest.load(Ordering::Relaxed);
            let tip = rb_stats.tip.load(Ordering::Relaxed);
            let mut resp = [0u8; HANDSHAKE_RESPONSE_SIZE];
            resp[0] = 1; // protocol version echo
            resp[1..9].copy_from_slice(&oldest.to_le_bytes());
            resp[9..17].copy_from_slice(&tip.to_le_bytes());
            stream.write_all(&resp).await?;

            handle_subscribe(
                stream,
                resume_block,
                snapshot_tx,
                batch_rx,
                delivered_tx,
                cancel,
            )
            .await
        }
    }
}

async fn handle_probe(
    stream: &mut UnixStream,
    consumer_connected: &Arc<AtomicBool>,
    rb_stats: &RingBufferStats,
) -> eyre::Result<()> {
    let resp = ProbeResponse {
        consumer_connected: consumer_connected.load(Ordering::Acquire),
        tip_block: rb_stats.tip.load(Ordering::Relaxed),
        oldest_block: rb_stats.oldest.load(Ordering::Relaxed),
        ring_buffer_entries: rb_stats.entries.load(Ordering::Relaxed),
        ring_buffer_memory_bytes: rb_stats.memory.load(Ordering::Relaxed),
    };
    let bytes = borsh::to_vec(&resp)?;
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;

    debug!("probe response sent");
    Ok(())
}

async fn shutdown_writer(
    writer_tx: mpsc::Sender<RecordBatch>,
    write_handle: JoinHandle<eyre::Result<()>>,
) {
    drop(writer_tx);
    match write_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(?e, "IPC writer error during shutdown"),
        Err(e) => warn!(?e, "IPC writer thread panicked"),
    }
}

async fn handle_subscribe(
    stream: UnixStream,
    resume_block: u64,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
    delivered_tx: &watch::Sender<Option<BlockNumHash>>,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let replay = replay_snapshot(stream, resume_block, snapshot_tx, batch_rx).await?;

    stream_live(
        batch_rx,
        replay.writer_tx,
        replay.write_handle,
        delivered_tx,
        cancel,
        replay.tip,
    )
    .await
}

struct ReplayResult {
    writer_tx: mpsc::Sender<RecordBatch>,
    write_handle: JoinHandle<eyre::Result<()>>,
    tip: u64,
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
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>())
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

async fn replay_snapshot(
    stream: UnixStream,
    resume_block: u64,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
) -> eyre::Result<ReplayResult> {
    // Start the IPC writer immediately so the client can read the schema
    // header while we wait for the snapshot.
    let std_stream = stream.into_std()?;
    std_stream.set_nonblocking(false)?;
    let schema = entity_events_schema();
    let (writer_tx, mut writer_rx) = mpsc::channel::<RecordBatch>(WRITER_CHANNEL_SIZE);

    let write_handle = tokio::task::spawn_blocking(move || -> eyre::Result<()> {
        let mut writer = StreamWriter::try_new(&std_stream, &schema)?;
        while let Some(batch) = writer_rx.blocking_recv() {
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(())
    });

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

    // Buffer any batches that arrived while we waited for the snapshot.
    let mut buffered = Vec::new();
    while let Ok(item) = batch_rx.try_recv() {
        buffered.push(item);
    }

    // Dedup: skip buffered batches already in the snapshot.
    let snapshot_keys: HashSet<BatchKey> = snapshot
        .iter()
        .filter_map(|(_, batch)| batch_dedup_key(batch))
        .collect();

    let last_bnh = snapshot.last().map(|(bnh, _)| *bnh);
    for (_, batch) in &snapshot {
        if writer_tx.send(batch.clone()).await.is_err() {
            return Err(eyre::eyre!("writer closed during snapshot replay"));
        }
    }

    let mut tip = last_bnh.map_or(resume_block, |bnh| bnh.number);
    let watermark = build_watermark_batch(tip)?;
    if writer_tx.send(watermark).await.is_err() {
        return Err(eyre::eyre!("writer closed during watermark send"));
    }

    // Forward batches the snapshot didn't cover.
    for (maybe_bnh, batch) in buffered {
        let dominated = batch_dedup_key(&batch)
            .is_some_and(|key| snapshot_keys.contains(&key));
        if !dominated {
            if writer_tx.send(batch).await.is_err() {
                return Err(eyre::eyre!("writer closed during buffered batch send"));
            }
            if let Some(bnh) = maybe_bnh {
                tip = bnh.number;
            }
        }
    }

    let _ = replay_done_tx.send(());

    info!(tip, "snapshot replay complete, entering live stream");

    Ok(ReplayResult {
        writer_tx,
        write_handle,
        tip,
    })
}

const SEND_TIMEOUT: Duration = Duration::from_secs(5);

async fn stream_live(
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
    writer_tx: mpsc::Sender<RecordBatch>,
    write_handle: JoinHandle<eyre::Result<()>>,
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
                    if writer_tx.send(batch).await.is_err() {
                        break;
                    }
                }
                if let Ok(wm) = build_watermark_batch(current_tip) {
                    let _ = writer_tx.send(wm).await;
                }
                shutdown_writer(writer_tx, write_handle).await;
                return Ok(());
            }
            maybe_batch = batch_rx.recv() => {
                if let Some(item) = maybe_batch {
                    item
                } else {
                    debug!("batch channel closed");
                    shutdown_writer(writer_tx, write_handle).await;
                    return Ok(());
                }
            }
        };

        match tokio::time::timeout(SEND_TIMEOUT, writer_tx.send(batch)).await {
            Ok(Ok(())) => {
                if let Some(bnh) = maybe_bnh {
                    current_tip = bnh.number;
                    let _ = delivered_tx.send(Some(bnh));
                }
            }
            Ok(Err(_)) => {
                warn!("writer channel closed, consumer disconnected");
                return Ok(());
            }
            Err(_) => {
                warn!("send timed out, disconnecting slow consumer");
                shutdown_writer(writer_tx, write_handle).await;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glint_primitives::exex_types::BatchOp;

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
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let client_handle = tokio::spawn(async move {
            let stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
            let std_stream = stream.into_std().unwrap();
            std_stream.set_nonblocking(false).unwrap();
            let reader =
                arrow::ipc::reader::StreamReader::try_new(&std_stream, None).unwrap();
            reader.into_iter().filter_map(Result::ok).collect::<Vec<_>>()
        });

        let (server_stream, _) = listener.accept().await.unwrap();

        let (snapshot_tx, mut snapshot_rx) = mpsc::channel::<SnapshotRequest>(1);
        let snapshot_fulfiller = tokio::spawn(async move {
            let req = snapshot_rx.recv().await.unwrap();
            let _ = req.reply_tx.send(snapshot);
            let _ = req.replay_done_rx.await;
        });

        let result = replay_snapshot(server_stream, 0, &snapshot_tx, batch_rx)
            .await
            .unwrap();

        let tip = result.tip;
        drop(result.writer_tx);
        let _ = result.write_handle.await;
        snapshot_fulfiller.await.unwrap();

        let received = client_handle.await.unwrap();
        (tip, received)
    }

    #[test]
    fn parse_subscribe_message() {
        let mut buf = Vec::new();
        buf.push(1u8);
        buf.extend_from_slice(&100u64.to_le_bytes());
        let msg = ClientMessage::parse(&buf).unwrap();
        assert_eq!(msg, ClientMessage::Subscribe { resume_block: 100 });
    }

    #[test]
    fn parse_probe_message() {
        let buf = vec![0u8];
        let msg = ClientMessage::parse(&buf).unwrap();
        assert_eq!(msg, ClientMessage::Probe);
    }

    #[test]
    fn parse_unknown_message_type() {
        let mut buf = Vec::new();
        buf.push(0xFE);
        buf.extend_from_slice(&0u64.to_le_bytes());
        let msg = ClientMessage::parse(&buf);
        assert!(matches!(msg, Ok(ClientMessage::Unknown(0xFE))));
    }

    #[test]
    fn parse_empty_buffer_is_error() {
        let result = ClientMessage::parse(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_subscribe_too_short_is_error() {
        let buf = vec![1u8, 0x00, 0x01]; // only 3 bytes, need 9
        let result = ClientMessage::parse(&buf);
        assert!(result.is_err());
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

    #[tokio::test]
    async fn stream_live_delivers_all_batches() {
        use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

        let (batch_tx, mut batch_rx) = mpsc::channel(16);
        // Small writer channel to exercise backpressure path
        let (writer_tx, mut writer_rx) = mpsc::channel::<RecordBatch>(1);
        let (delivered_tx, mut delivered_rx) = watch::channel(None);
        let cancel = CancellationToken::new();

        let writer_count = Arc::new(AtomicU32::new(0));
        let writer_count_clone = Arc::clone(&writer_count);

        let write_handle = tokio::spawn(async move {
            while let Some(_batch) = writer_rx.recv().await {
                writer_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
            }
            Ok::<(), eyre::Report>(())
        });

        let cancel_clone = cancel.clone();
        let live_handle = tokio::spawn(async move {
            stream_live(
                &mut batch_rx,
                writer_tx,
                write_handle,
                &delivered_tx,
                &cancel_clone,
                0,
            )
            .await
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
        live_handle.await.unwrap().unwrap();

        // +1 for the watermark sent on cancellation
        assert_eq!(
            writer_count.load(AtomicOrdering::Relaxed),
            4,
            "writer must receive all 3 batches + 1 shutdown watermark (no drops)"
        );
    }

    #[test]
    fn probe_response_roundtrip() {
        let resp = ProbeResponse {
            consumer_connected: true,
            tip_block: 42,
            oldest_block: 1,
            ring_buffer_entries: 100,
            ring_buffer_memory_bytes: 8192,
        };
        let bytes = borsh::to_vec(&resp).unwrap();
        let decoded: ProbeResponse = borsh::from_slice(&bytes).unwrap();
        assert!(decoded.consumer_connected);
        assert_eq!(decoded.tip_block, 42);
        assert_eq!(decoded.ring_buffer_entries, 100);
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
        let (batch_tx, mut batch_rx) =
            mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        let (bnh1, batch1) = make_test_batch(1, 0x01, BatchOp::Commit);
        let (bnh2, batch2) = make_test_batch(2, 0x02, BatchOp::Commit);
        let (bnh3, batch3) = make_test_batch(3, 0x03, BatchOp::Commit);

        batch_tx.send((Some(bnh1), batch1.clone())).await.unwrap();
        batch_tx.send((Some(bnh2), batch2.clone())).await.unwrap();
        batch_tx.send((Some(bnh3), batch3.clone())).await.unwrap();

        let snapshot = vec![(bnh1, batch1), (bnh2, batch2)];
        let (tip, received) = run_replay_test(&mut batch_rx, snapshot).await;

        assert_eq!(
            tip, 3,
            "tip should advance to block 3 from buffered batch"
        );
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
        let (batch_tx, mut batch_rx) =
            mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        let (bnh1, batch1) = make_test_batch(1, 0x01, BatchOp::Commit);
        let (bnh2, batch2) = make_test_batch(2, 0x02, BatchOp::Commit);

        // Pre-load channel with same batches as snapshot
        batch_tx.send((Some(bnh1), batch1.clone())).await.unwrap();
        batch_tx.send((Some(bnh2), batch2.clone())).await.unwrap();

        let snapshot = vec![(bnh1, batch1), (bnh2, batch2)];
        let (tip, received) = run_replay_test(&mut batch_rx, snapshot).await;

        assert_eq!(tip, 2);
        // Only snapshot batches + watermark; no duplicates forwarded
        assert_eq!(
            received.len(),
            3,
            "expected snapshot(2) + watermark(1), no duplicates"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_forwards_reorg_batches_with_different_hash() {
        let (batch_tx, mut batch_rx) =
            mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        // Snapshot covers block 100 with hash 0xAA
        let (bnh100, batch100) = make_test_batch(100, 0xAA, BatchOp::Commit);

        // After snapshot: revert(100, 0xAA) + commit(100', 0xBB)
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
        // snapshot(1) + watermark(1) + revert(1) + commit(1) = 4
        assert_eq!(
            received.len(),
            4,
            "reorg batches should be forwarded"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_filters_duplicate_revert_in_buffer() {
        let (batch_tx, mut batch_rx) =
            mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        // Snapshot contains a revert for block 5
        let (bnh5, revert5) = make_test_batch(5, 0x05, BatchOp::Revert);

        // Channel also has the same revert (duplicate)
        batch_tx
            .send((None, revert5.clone()))
            .await
            .unwrap();

        let snapshot = vec![(bnh5, revert5)];
        let (_, received) = run_replay_test(&mut batch_rx, snapshot).await;

        // snapshot(1) + watermark(1), duplicate revert filtered out
        assert_eq!(
            received.len(),
            2,
            "duplicate revert should be filtered, not forwarded"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_empty_buffer_is_noop() {
        let (_batch_tx, mut batch_rx) =
            mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);

        let (tip, received) = run_replay_test(&mut batch_rx, vec![]).await;

        assert_eq!(
            tip, 0,
            "tip should be resume_block with empty snapshot"
        );
        assert_eq!(
            received.len(),
            1,
            "only watermark with empty snapshot and empty buffer"
        );
    }
}

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

use crate::arrow::{build_watermark_batch, entity_events_schema};
use crate::ring_buffer::RingBufferStats;

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
const HANDSHAKE_RESPONSE_SIZE: usize = 17; // 1 + 8 + 8

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

async fn replay_snapshot(
    stream: UnixStream,
    resume_block: u64,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<(Option<BlockNumHash>, RecordBatch)>,
) -> eyre::Result<ReplayResult> {
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

    while batch_rx.try_recv().is_ok() {}

    let std_stream = stream.into_std()?;
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

    let last_bnh = snapshot.last().map(|(bnh, _)| *bnh);
    for (_, batch) in &snapshot {
        if writer_tx.send(batch.clone()).await.is_err() {
            return Err(eyre::eyre!("writer closed during snapshot replay"));
        }
    }

    let tip = last_bnh.map_or(resume_block, |bnh| bnh.number);
    let watermark = build_watermark_batch(tip)?;
    if writer_tx.send(watermark).await.is_err() {
        return Err(eyre::eyre!("writer closed during watermark send"));
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
}

// Socket listener, handshake (versioned), writer task

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_eips::BlockNumHash;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use eyre::ensure;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::arrow::{build_watermark_batch, entity_events_schema};

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

const HANDSHAKE_V1_SIZE: usize = 9; // 1 byte version + 8 bytes resume_block

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handshake {
    Probe,
    V1 { resume_block: u64 },
    Unsupported(u8),
}

impl Handshake {
    /// Parse a handshake from a raw byte buffer.
    ///
    /// - Version 0 = Probe (no additional data required)
    /// - Version 1 = V1 with an 8-byte little-endian `resume_block`
    /// - Any other version byte = `Unsupported`
    pub fn parse(buf: &[u8]) -> eyre::Result<Self> {
        ensure!(!buf.is_empty(), "handshake buffer is empty");

        let version = buf[0];
        match version {
            0 => Ok(Self::Probe),
            1 => {
                ensure!(
                    buf.len() >= HANDSHAKE_V1_SIZE,
                    "V1 handshake requires {HANDSHAKE_V1_SIZE} bytes, got {}",
                    buf.len()
                );
                let resume_block =
                    u64::from_le_bytes(buf[1..HANDSHAKE_V1_SIZE].try_into().unwrap_or_default());
                Ok(Self::V1 { resume_block })
            }
            other => Ok(Self::Unsupported(other)),
        }
    }
}

// ---------------------------------------------------------------------------
// Handshake response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeResponse {
    pub protocol_version: u8,
    pub oldest_available_block: u64,
    pub current_tip_block: u64,
}

impl HandshakeResponse {
    /// Encode as 17 bytes: 1 byte version + 8 bytes oldest (LE) + 8 bytes tip (LE).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(17);
        out.push(self.protocol_version);
        out.extend_from_slice(&self.oldest_available_block.to_le_bytes());
        out.extend_from_slice(&self.current_tip_block.to_le_bytes());
        out
    }
}

// ---------------------------------------------------------------------------
// Probe response (JSON)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProbeResponse {
    pub consumer_connected: bool,
    pub tip_block: u64,
    pub oldest_block: u64,
    pub ring_buffer_entries: u64,
    pub ring_buffer_memory_bytes: u64,
    pub protocol_version: u8,
}

// ---------------------------------------------------------------------------
// Snapshot request
// ---------------------------------------------------------------------------

pub struct SnapshotRequest {
    pub resume_block: u64,
    pub reply_tx: oneshot::Sender<Vec<(BlockNumHash, RecordBatch)>>,
    pub replay_done_tx: oneshot::Sender<()>,
}

// ---------------------------------------------------------------------------
// Backpressure / grace state
// ---------------------------------------------------------------------------

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

pub fn handle_backpressure(state: &mut GraceState) {
    let now = Instant::now();
    match state.first_failure_time {
        None => {
            state.first_failure_time = Some(now);
            state.failure_count = 1;
        }
        Some(first) if now.duration_since(first) > state.window => {
            // Window expired — reset and start a new window.
            state.first_failure_time = Some(now);
            state.failure_count = 1;
        }
        Some(_) => {
            state.failure_count += 1;
        }
    }
    if state.failure_count >= state.threshold {
        state.should_disconnect = true;
    }
}

pub const fn reset_grace(state: &mut GraceState) {
    state.first_failure_time = None;
    state.failure_count = 0;
    state.should_disconnect = false;
}

// ---------------------------------------------------------------------------
// Socket writer task
// ---------------------------------------------------------------------------

/// Maximum bytes we will read from a connecting client during handshake.
const MAX_HANDSHAKE_LEN: usize = 64;

/// The main socket writer task.
///
/// 1. Binds a `UnixListener` at `socket_path` (removes stale file if needed).
/// 2. Accepts connections; a new connection replaces the old one.
/// 3. Reads handshake, dispatches probe / v1 / unsupported.
/// 4. For V1: requests a snapshot via `snapshot_tx`, replays it, then streams
///    live batches from `batch_rx`.
/// 5. Respects `cancellation_token` for graceful shutdown.
#[allow(clippy::too_many_arguments)]
pub async fn socket_writer_task(
    socket_path: PathBuf,
    snapshot_tx: mpsc::Sender<SnapshotRequest>,
    mut batch_rx: mpsc::Receiver<RecordBatch>,
    delivered_tx: watch::Sender<Option<BlockNumHash>>,
    consumer_connected: Arc<AtomicBool>,
    atomic_entries: Arc<AtomicU64>,
    atomic_memory: Arc<AtomicU64>,
    cancellation_token: CancellationToken,
) -> eyre::Result<()> {
    // Remove stale socket file if present.
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!(?socket_path, "unix socket listener bound");

    loop {
        // Accept a new connection or shut down.
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

        // New connection replaces old — mark consumer as connected.
        consumer_connected.store(true, Ordering::Release);
        info!("new consumer connection accepted");

        match handle_connection(
            stream,
            &snapshot_tx,
            &mut batch_rx,
            &delivered_tx,
            &consumer_connected,
            &atomic_entries,
            &atomic_memory,
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
    }

    // Cleanup socket file on shutdown.
    let _ = std::fs::remove_file(&socket_path);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    mut stream: UnixStream,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<RecordBatch>,
    delivered_tx: &watch::Sender<Option<BlockNumHash>>,
    consumer_connected: &Arc<AtomicBool>,
    atomic_entries: &Arc<AtomicU64>,
    atomic_memory: &Arc<AtomicU64>,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    // Read handshake bytes.
    let mut buf = vec![0u8; MAX_HANDSHAKE_LEN];
    let n = stream.read(&mut buf).await?;
    ensure!(n > 0, "client disconnected before handshake");
    buf.truncate(n);

    let handshake = Handshake::parse(&buf)?;
    debug!(?handshake, "parsed handshake");

    match handshake {
        Handshake::Probe => {
            handle_probe(
                &mut stream,
                consumer_connected,
                atomic_entries,
                atomic_memory,
            )
            .await
        }
        Handshake::Unsupported(v) => {
            warn!(version = v, "unsupported handshake version");
            stream.write_all(&[0xFF]).await?;
            stream.shutdown().await?;
            Ok(())
        }
        Handshake::V1 { resume_block } => {
            handle_v1(
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
    atomic_entries: &Arc<AtomicU64>,
    atomic_memory: &Arc<AtomicU64>,
) -> eyre::Result<()> {
    let resp = ProbeResponse {
        consumer_connected: consumer_connected.load(Ordering::Acquire),
        tip_block: 0, // will be filled from ring buffer newest in production
        oldest_block: 0,
        ring_buffer_entries: atomic_entries.load(Ordering::Relaxed),
        ring_buffer_memory_bytes: atomic_memory.load(Ordering::Relaxed),
        protocol_version: 1,
    };
    let json = serde_json::to_vec(&resp)?;

    // Length-prefixed: u32 LE length + JSON bytes.
    let len_bytes = u32::try_from(json.len())?.to_le_bytes();
    stream.write_all(&len_bytes).await?;
    stream.write_all(&json).await?;
    stream.shutdown().await?;

    debug!("probe response sent");
    Ok(())
}

async fn handle_v1(
    stream: UnixStream,
    resume_block: u64,
    snapshot_tx: &mpsc::Sender<SnapshotRequest>,
    batch_rx: &mut mpsc::Receiver<RecordBatch>,
    delivered_tx: &watch::Sender<Option<BlockNumHash>>,
    cancel: &CancellationToken,
) -> eyre::Result<()> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let (replay_done_tx, replay_done_rx) = oneshot::channel();

    // Request snapshot from the notification loop.
    snapshot_tx
        .send(SnapshotRequest {
            resume_block,
            reply_tx,
            replay_done_tx,
        })
        .await
        .map_err(|_| eyre::eyre!("snapshot channel closed"))?;

    // Receive the snapshot.
    let snapshot = reply_rx
        .await
        .map_err(|_| eyre::eyre!("snapshot reply channel dropped"))?;

    info!(
        resume_block,
        snapshot_len = snapshot.len(),
        "received snapshot for replay"
    );

    // Drain any stale batches that were queued before the snapshot was taken.
    while batch_rx.try_recv().is_ok() {}

    // Convert tokio UnixStream to std for Arrow IPC StreamWriter (needs std::io::Write).
    let std_stream = stream.into_std()?;
    let schema = Arc::new(entity_events_schema());
    let mut writer = StreamWriter::try_new(&std_stream, &schema)?;

    // Write snapshot batches.
    let mut last_bnh: Option<BlockNumHash> = None;
    for (bnh, batch) in &snapshot {
        writer.write(batch)?;
        last_bnh = Some(*bnh);
    }

    // Write watermark after snapshot replay.
    let tip = last_bnh.map_or(resume_block, |bnh| bnh.number);
    let watermark = build_watermark_batch(tip)?;
    writer.write(&watermark)?;

    // Wait for the notification loop to signal that it is ready to queue live
    // batches. The notification loop holds `replay_done_tx` (from the snapshot
    // request) and sends on it once it has finished its bookkeeping.
    replay_done_rx
        .await
        .map_err(|_| eyre::eyre!("replay_done sender dropped"))?;

    info!(tip, "snapshot replay complete, entering live stream");

    // Live streaming loop.
    let mut grace = GraceState::default();

    loop {
        let batch = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                debug!("live loop cancelled, draining remaining batches");
                // Drain remaining batches on shutdown.
                while let Ok(batch) = batch_rx.try_recv() {
                    if writer.write(&batch).is_err() {
                        break;
                    }
                }
                // Final watermark.
                if let Ok(wm) = build_watermark_batch(tip) {
                    let _ = writer.write(&wm);
                }
                let _ = writer.finish();
                return Ok(());
            }
            maybe_batch = batch_rx.recv() => {
                if let Some(batch) = maybe_batch {
                    batch
                } else {
                    debug!("batch channel closed");
                    let _ = writer.finish();
                    return Ok(());
                }
            }
        };

        match writer.write(&batch) {
            Ok(()) => {
                reset_grace(&mut grace);
                // Update delivered watermark from batch metadata if available.
                let _ = delivered_tx.send(last_bnh);
            }
            Err(e) => {
                warn!(?e, "failed to write batch to consumer");
                handle_backpressure(&mut grace);
                if grace.should_disconnect {
                    warn!("backpressure threshold reached, disconnecting consumer");
                    let _ = writer.finish();
                    return Ok(());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_v1_handshake() {
        let mut buf = Vec::new();
        buf.push(1u8);
        buf.extend_from_slice(&100u64.to_le_bytes());
        let hs = Handshake::parse(&buf).unwrap();
        assert_eq!(hs, Handshake::V1 { resume_block: 100 });
    }

    #[test]
    fn parse_probe_handshake() {
        let buf = vec![0u8];
        let hs = Handshake::parse(&buf).unwrap();
        assert_eq!(hs, Handshake::Probe);
    }

    #[test]
    fn parse_unsupported_version() {
        let mut buf = Vec::new();
        buf.push(0xFE);
        buf.extend_from_slice(&0u64.to_le_bytes());
        let hs = Handshake::parse(&buf);
        assert!(matches!(hs, Ok(Handshake::Unsupported(0xFE))));
    }

    #[test]
    fn parse_empty_buffer_is_error() {
        let result = Handshake::parse(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_v1_too_short_is_error() {
        let buf = vec![1u8, 0x00, 0x01]; // only 3 bytes, need 9
        let result = Handshake::parse(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn handshake_response_encoding() {
        let resp = HandshakeResponse {
            protocol_version: 1,
            oldest_available_block: 500,
            current_tip_block: 1000,
        };
        let bytes = resp.encode();
        assert_eq!(bytes.len(), 17);
        assert_eq!(bytes[0], 1);

        // Verify round-trip of encoded values.
        let oldest = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let tip = u64::from_le_bytes(bytes[9..17].try_into().unwrap());
        assert_eq!(oldest, 500);
        assert_eq!(tip, 1000);
    }

    #[test]
    fn backpressure_grace_no_disconnect_under_threshold() {
        let mut state = GraceState::default();
        handle_backpressure(&mut state);
        handle_backpressure(&mut state);
        // 2 failures within window — no disconnect
        assert!(!state.should_disconnect);
    }

    #[test]
    fn backpressure_grace_disconnect_at_threshold() {
        let mut state = GraceState::default();
        handle_backpressure(&mut state);
        handle_backpressure(&mut state);
        handle_backpressure(&mut state);
        // 3 failures within 1 second window
        assert!(state.should_disconnect);
    }

    #[test]
    fn reset_grace_clears_state() {
        let mut state = GraceState::default();
        handle_backpressure(&mut state);
        handle_backpressure(&mut state);
        handle_backpressure(&mut state);
        assert!(state.should_disconnect);

        reset_grace(&mut state);
        assert!(!state.should_disconnect);
        assert_eq!(state.failure_count, 0);
        assert!(state.first_failure_time.is_none());
    }

    #[test]
    fn probe_response_serializes() {
        let resp = ProbeResponse {
            consumer_connected: true,
            tip_block: 42,
            oldest_block: 1,
            ring_buffer_entries: 100,
            ring_buffer_memory_bytes: 8192,
            protocol_version: 1,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"consumer_connected\":true"));
        assert!(json.contains("\"tip_block\":42"));
    }
}

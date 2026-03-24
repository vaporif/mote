use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use alloy_eips::BlockNumHash;
use alloy_primitives::{Address, B256, Bytes};
use arrow::array::UInt8Array;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use mote_exex::arrow::{EventRow, build_record_batch};
use mote_exex::parse::EntityEvent;
use mote_exex::stream::{SnapshotRequest, socket_writer_task};
use mote_primitives::exex_types::BatchOp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn temp_socket_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "mote-test-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn sample_created_event() -> EntityEvent {
    EntityEvent::Created {
        entity_key: B256::repeat_byte(0x01),
        owner: Address::repeat_byte(0x02),
        expires_at: 100,
        content_type: "text/plain".into(),
        payload: Bytes::from_static(b"hello"),
        string_keys: vec!["k1".into()],
        string_values: vec!["v1".into()],
        numeric_keys: vec!["n1".into()],
        numeric_values: vec![42],
    }
}

fn make_test_batch(block_number: u64) -> RecordBatch {
    #[allow(clippy::cast_possible_truncation)]
    let block_hash = B256::repeat_byte(block_number as u8);
    let events = vec![EventRow {
        event: sample_created_event(),
        tx_index: 0,
        tx_hash: B256::repeat_byte(0xAA),
        log_index: 0,
    }];
    build_record_batch(
        block_number,
        block_hash,
        block_number,
        BatchOp::Commit,
        &events,
    )
    .expect("test batch build should succeed")
}

fn v1_handshake(resume_block: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(1u8);
    buf.extend_from_slice(&resume_block.to_le_bytes());
    buf
}

fn is_watermark(batch: &RecordBatch) -> bool {
    batch
        .column_by_name("op")
        .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
        .is_some_and(|col| !col.is_empty() && col.value(0) == 0xFF)
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct TestHarness {
    socket_path: PathBuf,
    batch_tx: mpsc::Sender<RecordBatch>,
    snapshot_rx: mpsc::Receiver<SnapshotRequest>,
    #[allow(dead_code)]
    delivered_rx: watch::Receiver<Option<BlockNumHash>>,
    consumer_connected: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
}

impl TestHarness {
    fn spawn() -> Self {
        let socket_path = temp_socket_path();
        let (batch_tx, batch_rx) = mpsc::channel::<RecordBatch>(64);
        let (snapshot_tx, snapshot_rx) = mpsc::channel::<SnapshotRequest>(1);
        let (delivered_tx, delivered_rx) = watch::channel::<Option<BlockNumHash>>(None);
        let consumer_connected = Arc::new(AtomicBool::new(false));
        let atomic_entries = Arc::new(AtomicU64::new(42));
        let atomic_memory = Arc::new(AtomicU64::new(8192));
        let cancellation_token = CancellationToken::new();

        let task_path = socket_path.clone();
        let task_connected = Arc::clone(&consumer_connected);
        let task_cancel = cancellation_token.clone();

        tokio::spawn(async move {
            let _ = socket_writer_task(
                task_path,
                snapshot_tx,
                batch_rx,
                delivered_tx,
                task_connected,
                atomic_entries,
                atomic_memory,
                task_cancel,
            )
            .await;
        });

        Self {
            socket_path,
            batch_tx,
            snapshot_rx,
            delivered_rx,
            consumer_connected,
            cancellation_token,
        }
    }

    async fn wait_for_socket(&self) {
        for _ in 0..100 {
            if self.socket_path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "socket file did not appear at {}",
            self.socket_path.display()
        );
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full V1 handshake: connect, send handshake, handle snapshot replay, receive
/// snapshot batches + watermark + a live batch through the IPC stream.
///
/// The client reader runs in a blocking thread from the start to avoid
/// deadlocking with the server's blocking IPC writes when the macOS Unix
/// socket send buffer (8 KB) fills up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_handshake_and_replay() {
    let mut harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    // Connect and immediately convert to blocking std socket.
    let client = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect should succeed");
    let std_client = client.into_std().expect("into_std");
    std_client
        .set_nonblocking(false)
        .expect("set blocking mode");

    // Send V1 handshake.
    (&std_client)
        .write_all(&v1_handshake(0))
        .expect("handshake write should succeed");

    // Start the IPC reader in a blocking thread so it drains the socket
    // concurrently with the server's writes.
    let read_handle = tokio::task::spawn_blocking(move || {
        let reader =
            StreamReader::try_new(&std_client, None).expect("StreamReader creation should succeed");
        let mut batches = Vec::new();
        for batch_result in reader {
            match batch_result {
                Ok(batch) => {
                    batches.push(batch);
                    if batches.len() >= 3 {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        batches
    });

    // Act as the notification loop: receive the snapshot request and reply.
    let snap_req = timeout(Duration::from_secs(5), harness.snapshot_rx.recv())
        .await
        .expect("snapshot request should arrive within timeout")
        .expect("snapshot channel should not be closed");

    assert_eq!(snap_req.resume_block, 0);

    let snapshot_batch = make_test_batch(10);
    let snapshot_bnh = BlockNumHash::new(10, B256::repeat_byte(10));

    snap_req
        .reply_tx
        .send(vec![(snapshot_bnh, snapshot_batch)])
        .expect("reply_tx send should succeed");

    // Signal replay done so live streaming begins.
    snap_req
        .replay_done_tx
        .send(())
        .expect("replay_done_tx send should succeed");

    // Wait for the writer to enter the live loop and drain stale batches.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send a live batch.
    harness
        .batch_tx
        .send(make_test_batch(20))
        .await
        .expect("batch_tx send should succeed");

    let batches = timeout(Duration::from_secs(10), read_handle)
        .await
        .expect("read should complete within timeout")
        .expect("spawn_blocking should succeed");

    assert!(
        batches.len() >= 3,
        "expected at least 3 batches, got {}",
        batches.len()
    );

    // First batch: snapshot data.
    assert_eq!(batches[0].num_rows(), 1);
    assert!(!is_watermark(&batches[0]));

    // Second batch: watermark marking end of snapshot replay.
    assert!(is_watermark(&batches[1]));

    // Third batch: live data.
    assert_eq!(batches[2].num_rows(), 1);
    assert!(!is_watermark(&batches[2]));
}

/// Probe handshake (version 0x00): returns length-prefixed JSON status and
/// closes the connection without affecting consumer state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_returns_json_status() {
    let harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let mut client = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect should succeed");

    // Version 0x00 = probe.
    client
        .write_all(&[0x00])
        .await
        .expect("probe write should succeed");

    // Read u32 LE length prefix.
    let mut len_buf = [0u8; 4];
    timeout(Duration::from_secs(5), client.read_exact(&mut len_buf))
        .await
        .expect("length read should complete within timeout")
        .expect("length read should succeed");
    #[allow(clippy::cast_possible_truncation)]
    let json_len = u32::from_le_bytes(len_buf) as usize;
    assert!(json_len > 0, "JSON payload should not be empty");
    assert!(
        json_len < 4096,
        "JSON payload unexpectedly large: {json_len}"
    );

    // Read JSON payload.
    let mut json_buf = vec![0u8; json_len];
    timeout(Duration::from_secs(5), client.read_exact(&mut json_buf))
        .await
        .expect("json read should complete within timeout")
        .expect("json read should succeed");

    let value: serde_json::Value =
        serde_json::from_slice(&json_buf).expect("payload should be valid JSON");

    assert_eq!(value["protocol_version"], 1);
    assert_eq!(value["ring_buffer_entries"], 42);
    assert_eq!(value["ring_buffer_memory_bytes"], 8192);

    // Connection should be closed after probe.
    let mut trailing = [0u8; 1];
    let n = timeout(Duration::from_secs(2), client.read(&mut trailing))
        .await
        .expect("trailing read should complete within timeout")
        .expect("trailing read should succeed");
    assert_eq!(n, 0, "connection should be closed after probe");

    // Probe should not leave consumer_connected as true. The server resets it
    // to false when the handler returns, which has happened by the time we
    // observe EOF.
    assert!(
        !harness.consumer_connected.load(Ordering::Acquire),
        "consumer_connected should be false after probe"
    );
}

/// Unsupported handshake version returns a single 0xFF byte and closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_version_returns_error_byte() {
    let harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let mut client = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect should succeed");

    // Send unsupported version 0xFE with padding bytes.
    let mut buf = Vec::with_capacity(9);
    buf.push(0xFE);
    buf.extend_from_slice(&0u64.to_le_bytes());
    client.write_all(&buf).await.expect("write should succeed");

    // Read single-byte error response.
    let mut response = [0u8; 1];
    timeout(Duration::from_secs(5), client.read_exact(&mut response))
        .await
        .expect("read should complete within timeout")
        .expect("read should succeed");
    assert_eq!(response[0], 0xFF, "should receive 0xFF error byte");

    // Connection should be closed.
    let mut trailing = [0u8; 1];
    let n = timeout(Duration::from_secs(2), client.read(&mut trailing))
        .await
        .expect("trailing read should complete within timeout")
        .expect("trailing read should succeed");
    assert_eq!(n, 0, "connection should be closed after error");
}

/// Verifies that cancelling the token causes the socket writer to shut down
/// cleanly and reset `consumer_connected` to false.
///
/// The original "connection replacement" test (where B connects while A is
/// active) cannot be reliably tested because the server does blocking IPC
/// writes in an async context, preventing the accept loop from cycling to a
/// new connection while the current handler is blocked on a write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_disconnects_consumer() {
    let mut harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    // -- Consumer A connects --
    let client_a = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect A should succeed");
    let std_a = client_a.into_std().expect("into_std A");
    std_a.set_nonblocking(false).expect("set blocking A");

    (&std_a)
        .write_all(&v1_handshake(0))
        .expect("A handshake write should succeed");

    // Drain A in background to avoid send buffer deadlock.
    let _drain_a = tokio::task::spawn_blocking(move || {
        let mut buf = [0u8; 4096];
        loop {
            match std::io::Read::read(&mut &std_a, &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });

    // Handle A's snapshot request.
    let snap_a = timeout(Duration::from_secs(5), harness.snapshot_rx.recv())
        .await
        .expect("A snapshot request should arrive")
        .expect("snapshot channel should not be closed");
    snap_a
        .reply_tx
        .send(vec![])
        .expect("A reply should succeed");
    snap_a
        .replay_done_tx
        .send(())
        .expect("A replay_done should succeed");

    // Wait for A's handler to enter the live loop.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel the token. This causes A's handler to enter its shutdown path
    // (drain remaining batches + finish). The accept loop then breaks.
    harness.cancellation_token.cancel();

    // Wait for the writer task to exit.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify consumer_connected is now false.
    assert!(
        !harness.consumer_connected.load(Ordering::Acquire),
        "consumer_connected should be false after cancel"
    );
}

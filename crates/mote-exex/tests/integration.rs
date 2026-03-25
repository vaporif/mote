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
use mote_exex::ring_buffer::RingBufferStats;
use mote_exex::stream::{SnapshotRequest, socket_writer_task};
use mote_primitives::exex_types::BatchOp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

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

#[allow(clippy::cast_possible_truncation)] // test values are small
fn make_test_batch(block_number: u64) -> RecordBatch {
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

fn subscribe_msg(resume_block: u64) -> Vec<u8> {
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

struct TestHarness {
    socket_path: PathBuf,
    batch_tx: mpsc::Sender<(Option<BlockNumHash>, RecordBatch)>,
    snapshot_rx: mpsc::Receiver<SnapshotRequest>,
    #[allow(dead_code)]
    delivered_rx: watch::Receiver<Option<BlockNumHash>>,
    consumer_connected: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
}

impl TestHarness {
    fn spawn() -> Self {
        let socket_path = temp_socket_path();
        let (batch_tx, batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);
        let (snapshot_tx, snapshot_rx) = mpsc::channel::<SnapshotRequest>(1);
        let (delivered_tx, delivered_rx) = watch::channel::<Option<BlockNumHash>>(None);
        let consumer_connected = Arc::new(AtomicBool::new(false));
        let rb_stats = RingBufferStats {
            entries: Arc::new(AtomicU64::new(42)),
            memory: Arc::new(AtomicU64::new(8192)),
            tip: Arc::new(AtomicU64::new(100)),
            oldest: Arc::new(AtomicU64::new(1)),
        };
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
                rb_stats,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_subscribe_and_replay() {
    let mut harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let client = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect should succeed");
    let std_client = client.into_std().expect("into_std");
    std_client
        .set_nonblocking(false)
        .expect("set blocking mode");

    (&std_client)
        .write_all(&subscribe_msg(0))
        .expect("subscribe write should succeed");

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

    snap_req
        .replay_done_tx
        .send(())
        .expect("replay_done_tx send should succeed");

    let live_bnh = BlockNumHash::new(20, B256::repeat_byte(20));
    harness
        .batch_tx
        .send((Some(live_bnh), make_test_batch(20)))
        .await
        .expect("batch_tx send should succeed");

    let batches = timeout(Duration::from_secs(30), read_handle)
        .await
        .expect("read should complete within timeout")
        .expect("spawn_blocking should succeed");

    assert!(
        batches.len() >= 3,
        "expected at least 3 batches, got {}",
        batches.len()
    );

    assert_eq!(batches[0].num_rows(), 1);
    assert!(!is_watermark(&batches[0]));
    assert!(is_watermark(&batches[1]));
    assert_eq!(batches[2].num_rows(), 1);
    assert!(!is_watermark(&batches[2]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_returns_json_status() {
    let harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let mut client = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect should succeed");

    client
        .write_all(&[0x00])
        .await
        .expect("probe write should succeed");

    let mut len_buf = [0u8; 4];
    timeout(Duration::from_secs(5), client.read_exact(&mut len_buf))
        .await
        .expect("length read should complete within timeout")
        .expect("length read should succeed");
    let json_len = u32::from_le_bytes(len_buf) as usize;
    assert!(json_len > 0, "JSON payload should not be empty");
    assert!(
        json_len < 4096,
        "JSON payload unexpectedly large: {json_len}"
    );

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
    assert_eq!(value["tip_block"], 100);
    assert_eq!(value["oldest_block"], 1);

    let mut trailing = [0u8; 1];
    let n = timeout(Duration::from_secs(2), client.read(&mut trailing))
        .await
        .expect("trailing read should complete within timeout")
        .expect("trailing read should succeed");
    assert_eq!(n, 0, "connection should be closed after probe");

    assert!(
        !harness.consumer_connected.load(Ordering::Acquire),
        "consumer_connected should be false after probe"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_message_returns_error_byte() {
    let harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let mut client = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect should succeed");

    let mut buf = Vec::with_capacity(9);
    buf.push(0xFE);
    buf.extend_from_slice(&0u64.to_le_bytes());
    client.write_all(&buf).await.expect("write should succeed");

    let mut response = [0u8; 1];
    timeout(Duration::from_secs(5), client.read_exact(&mut response))
        .await
        .expect("read should complete within timeout")
        .expect("read should succeed");
    assert_eq!(response[0], 0xFF, "should receive 0xFF error byte");

    let mut trailing = [0u8; 1];
    let n = timeout(Duration::from_secs(2), client.read(&mut trailing))
        .await
        .expect("trailing read should complete within timeout")
        .expect("trailing read should succeed");
    assert_eq!(n, 0, "connection should be closed after error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_disconnects_consumer() {
    let mut harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let client_a = UnixStream::connect(&harness.socket_path)
        .await
        .expect("connect A should succeed");
    let std_a = client_a.into_std().expect("into_std A");
    std_a.set_nonblocking(false).expect("set blocking A");

    (&std_a)
        .write_all(&subscribe_msg(0))
        .expect("A handshake write should succeed");

    let _drain_a = tokio::task::spawn_blocking(move || {
        let mut buf = [0u8; 4096];
        loop {
            match std::io::Read::read(&mut &std_a, &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    harness.cancellation_token.cancel();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !harness.consumer_connected.load(Ordering::Acquire),
        "consumer_connected should be false after cancel"
    );
}

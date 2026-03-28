use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use alloy_eips::BlockNumHash;
use alloy_primitives::{Address, B256, Bytes};
use arrow::array::UInt8Array;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use glint_exex::arrow::{EventRow, build_record_batch};
use glint_exex::ring_buffer::RingBufferStats;
use glint_exex::stream::{ProbeResponse, SnapshotRequest, socket_writer_task};
use glint_primitives::exex_schema::columns;
use glint_primitives::exex_types::BatchOp;
use glint_primitives::parse::EntityEvent;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

fn temp_socket_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "glint-test-{}-{}.sock",
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
        extend_policy: 0,
        operator: Address::ZERO,
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
        .column_by_name(columns::OP)
        .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
        .is_some_and(|col| !col.is_empty() && col.value(0) == 0xFF)
}

fn consume_handshake(stream: &std::os::unix::net::UnixStream) {
    let mut buf = [0u8; glint_exex::stream::HANDSHAKE_RESPONSE_SIZE];
    (&*stream).read_exact(&mut buf).expect("handshake read");
    assert_eq!(buf[0], 1, "protocol version echo should be 1");
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

    async fn connect(&self) -> UnixStream {
        for _ in 0..50 {
            if let Ok(stream) = UnixStream::connect(&self.socket_path).await {
                return stream;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "could not connect to socket at {}",
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

    let client = harness.connect().await;
    let std_client = client.into_std().unwrap();
    std_client.set_nonblocking(false).unwrap();

    (&std_client).write_all(&subscribe_msg(0)).unwrap();

    consume_handshake(&std_client);

    let snap_req = timeout(Duration::from_secs(5), harness.snapshot_rx.recv())
        .await
        .expect("snapshot request timed out")
        .unwrap();

    assert_eq!(snap_req.resume_block, 0);

    let snapshot_batch = make_test_batch(10);
    let snapshot_bnh = BlockNumHash::new(10, B256::repeat_byte(10));

    snap_req
        .reply_tx
        .send(vec![(snapshot_bnh, snapshot_batch)])
        .unwrap();

    let _ = snap_req.replay_done_rx.await;

    let live_bnh = BlockNumHash::new(20, B256::repeat_byte(20));
    harness
        .batch_tx
        .send((Some(live_bnh), make_test_batch(20)))
        .await
        .unwrap();

    // Read on a blocking thread after all data has been sent to the server.
    // All 3 batches (snapshot + watermark + live) are buffered in the IPC
    // writer channel, so blocking reads will find data without racing.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        std_client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let reader = StreamReader::try_new(&std_client, None).unwrap();
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
        let _ = done_tx.send(batches);
    });

    let batches = timeout(Duration::from_secs(10), done_rx)
        .await
        .expect("timed out waiting for reader")
        .unwrap();

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
async fn probe_returns_status() {
    let harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let mut client = harness.connect().await;

    client.write_all(&[0x00]).await.unwrap();

    let mut buf = Vec::new();
    timeout(Duration::from_secs(5), client.read_to_end(&mut buf))
        .await
        .expect("timed out reading probe")
        .unwrap();

    let resp: ProbeResponse = borsh::from_slice(&buf).unwrap();
    assert_eq!(resp.ring_buffer_entries, 42);
    assert_eq!(resp.ring_buffer_memory_bytes, 8192);
    assert_eq!(resp.tip_block, 100);
    assert_eq!(resp.oldest_block, 1);

    assert!(!harness.consumer_connected.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_message_returns_error_byte() {
    let harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let mut client = harness.connect().await;

    let mut buf = Vec::with_capacity(9);
    buf.push(0xFE);
    buf.extend_from_slice(&0u64.to_le_bytes());
    client.write_all(&buf).await.unwrap();

    let mut response = [0u8; 1];
    timeout(Duration::from_secs(5), client.read_exact(&mut response))
        .await
        .expect("timed out")
        .unwrap();
    assert_eq!(response[0], 0xFF);

    let mut trailing = [0u8; 1];
    let n = timeout(Duration::from_secs(2), client.read(&mut trailing))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0, "connection not closed after error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_disconnects_consumer() {
    let mut harness = TestHarness::spawn();
    harness.wait_for_socket().await;

    let client_a = harness.connect().await;
    let std_a = client_a.into_std().unwrap();
    std_a.set_nonblocking(false).unwrap();

    (&std_a).write_all(&subscribe_msg(0)).unwrap();

    consume_handshake(&std_a);

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
        .expect("snapshot request timed out")
        .unwrap();
    snap_a.reply_tx.send(vec![]).unwrap();
    let _ = snap_a.replay_done_rx.await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    harness.cancellation_token.cancel();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!harness.consumer_connected.load(Ordering::Acquire));
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use alloy_eips::BlockNumHash;
use alloy_primitives::{Address, B256, Bytes};
use arrow::array::UInt8Array;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use glint_exex::arrow::{EventRow, build_record_batch};
use glint_exex::metrics::ExExMetrics;
use glint_exex::ring_buffer::RingBufferStats;
use glint_exex::stream::{SnapshotRequest, WriterTaskCtx, writer_task};
use glint_primitives::exex_schema::columns;
use glint_primitives::exex_types::BatchOp;
use glint_primitives::parse::EntityEvent;
use glint_transport::{ExExTransportClient, ExExTransportServer};
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

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
        gas_cost: 50_000,
    }
}

fn make_test_batch(block_number: u64) -> RecordBatch {
    let block_hash = B256::repeat_byte(block_number.to_le_bytes()[0]);
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

fn is_watermark(batch: &RecordBatch) -> bool {
    batch
        .column_by_name(columns::OP)
        .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
        .is_some_and(|col| !col.is_empty() && col.value(0) == 0xFF)
}

struct TestHarness {
    socket_path: std::path::PathBuf,
    batch_tx: mpsc::Sender<(Option<BlockNumHash>, RecordBatch)>,
    snapshot_rx: mpsc::Receiver<SnapshotRequest>,
    #[allow(dead_code)]
    delivered_rx: watch::Receiver<Option<BlockNumHash>>,
    consumer_connected: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
}

impl TestHarness {
    fn spawn() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let (batch_tx, batch_rx) = mpsc::channel::<(Option<BlockNumHash>, RecordBatch)>(64);
        let (snapshot_tx, snapshot_rx) = mpsc::channel::<SnapshotRequest>(1);
        let (delivered_tx, delivered_rx) = watch::channel::<Option<BlockNumHash>>(None);
        let consumer_connected = Arc::new(AtomicBool::new(false));
        let probe_state = glint_transport::ProbeState {
            consumer_connected: Arc::clone(&consumer_connected),
            tip_block: Arc::new(AtomicU64::new(100)),
            oldest_block: Arc::new(AtomicU64::new(1)),
            ring_buffer_entries: Arc::new(AtomicU64::new(42)),
            ring_buffer_memory_bytes: Arc::new(AtomicU64::new(8192)),
        };
        let rb_stats = RingBufferStats {
            entries: Arc::clone(&probe_state.ring_buffer_entries),
            memory: Arc::clone(&probe_state.ring_buffer_memory_bytes),
            tip: Arc::clone(&probe_state.tip_block),
            oldest: Arc::clone(&probe_state.oldest_block),
        };
        let cancellation_token = CancellationToken::new();

        let task_connected = Arc::clone(&consumer_connected);
        let task_cancel = cancellation_token.clone();

        let ipc_server = glint_transport::ipc::IpcServer::new(
            socket_path.clone(),
            probe_state,
            task_cancel.clone(),
        )
        .expect("failed to bind IPC socket");

        let metrics = ExExMetrics::default();
        tokio::spawn(async move {
            let _ = writer_task(WriterTaskCtx {
                server: Box::new(ipc_server),
                snapshot_tx,
                batch_rx,
                delivered_tx,
                consumer_connected: task_connected,
                rb_stats,
                cancellation_token: task_cancel,
                metrics: ExExMetrics::default(),
            })
            .await;
        });

        // Keep tempdir alive by leaking it (cleaned up on process exit)
        std::mem::forget(dir);

        Self {
            socket_path,
            batch_tx,
            snapshot_rx,
            delivered_rx,
            consumer_connected,
            cancellation_token,
        }
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

    let client = Box::new(glint_transport::ipc::IpcClient::new(
        harness.socket_path.clone(),
    ));

    // Client subscribe triggers server accept
    let subscribe_handle = tokio::spawn(async move { client.subscribe(0).await.unwrap() });

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

    // Give the writer task time to send batches
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel to close the stream
    harness.cancellation_token.cancel();

    let (_info, mut stream) = subscribe_handle.await.unwrap();

    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(batch) => batches.push(batch),
            Err(_) => break,
        }
    }

    assert!(
        batches.len() >= 3,
        "expected at least 3 batches (snapshot + watermark + live), got {}",
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

    let client = glint_transport::ipc::IpcClient::new(harness.socket_path.clone());
    let info = client.probe().await.unwrap();
    assert_eq!(info.tip_block, 100);
    assert_eq!(info.oldest_block, 1);
    assert!(!harness.consumer_connected.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_message_returns_error_byte() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    let harness = TestHarness::spawn();

    let mut client = UnixStream::connect(&harness.socket_path).await.unwrap();

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

    let client = Box::new(glint_transport::ipc::IpcClient::new(
        harness.socket_path.clone(),
    ));

    let subscribe_handle = tokio::spawn(async move { client.subscribe(0).await.unwrap() });

    let snap_req = timeout(Duration::from_secs(5), harness.snapshot_rx.recv())
        .await
        .expect("snapshot request timed out")
        .unwrap();
    snap_req.reply_tx.send(vec![]).unwrap();
    let _ = snap_req.replay_done_rx.await;

    let (_info, mut stream) = subscribe_handle.await.unwrap();

    // Drain in background
    let drain_handle = tokio::spawn(async move {
        while let Some(result) = stream.next().await {
            if result.is_err() {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    harness.cancellation_token.cancel();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!harness.consumer_connected.load(Ordering::Acquire));

    let _ = drain_handle.await;
}

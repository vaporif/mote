use std::path::PathBuf;
use std::pin::Pin;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use eyre::{ensure, eyre};
use futures::Stream;
use glint_primitives::exex_schema::entity_events_schema;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{ExExConnection, ExExTransportClient, ExExTransportServer, HandshakeInfo};

const SUBSCRIBE_MSG_SIZE: usize = 9; // 1 byte type + 8 bytes resume_block
const MAX_CLIENT_MSG_LEN: usize = 64;
const HANDSHAKE_RESPONSE_SIZE: usize = 17; // 1 + 8 + 8
const WRITER_CHANNEL_SIZE: usize = 16;

// -- Server ------------------------------------------------------------------

/// IPC transport server over a Unix domain socket.
///
/// Handles probe requests internally and only surfaces subscribe connections
/// through the `ExExTransportServer` trait.
pub struct IpcServer {
    listener: UnixListener,
    cancel: CancellationToken,
}

impl IpcServer {
    /// Bind a new listener. Removes any leftover socket file first.
    #[allow(clippy::needless_pass_by_value)] // owned path is idiomatic for bind
    pub fn new(socket_path: PathBuf, cancel: CancellationToken) -> eyre::Result<Self> {
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }
        let listener = UnixListener::bind(&socket_path)?;
        debug!(?socket_path, "IPC server bound");
        Ok(Self { listener, cancel })
    }
}

#[async_trait]
impl ExExTransportServer for IpcServer {
    async fn accept(&self) -> eyre::Result<Box<dyn ExExConnection>> {
        loop {
            let stream = tokio::select! {
                biased;
                () = self.cancel.cancelled() => {
                    return Err(eyre!("IPC server shutting down"));
                }
                result = self.listener.accept() => {
                    let (stream, _addr) = result?;
                    stream
                }
            };

            match try_classify_connection(stream).await {
                Ok(Classified::Subscribe(conn)) => return Ok(Box::new(conn)),
                Ok(Classified::Handled) => {
                    // probe or unknown -- already responded, loop back
                }
                Err(e) => {
                    warn!(?e, "error classifying connection, skipping");
                }
            }
        }
    }
}

enum Classified {
    Subscribe(IpcConnection),
    /// Probe/unknown already handled inline.
    Handled,
}

/// Read the first message, decide what kind of connection this is.
async fn try_classify_connection(mut stream: UnixStream) -> eyre::Result<Classified> {
    let mut buf = vec![0u8; MAX_CLIENT_MSG_LEN];
    let n = stream.read(&mut buf).await?;
    ensure!(n > 0, "client disconnected before sending a message");
    buf.truncate(n);

    match buf[0] {
        0x00 => {
            // Probe -- respond inline with a minimal borsh-encoded ProbeResponse.
            // The real ExEx server fills in live stats; here we just send zeros
            // since the transport layer doesn't own ring-buffer state.
            // In practice, probes on an IpcServer-based setup will go through
            // the ExExTransportClient::probe path instead.
            //
            // Still, we must not break the protocol: send *something* valid so
            // the client doesn't hang.
            let resp = MinimalProbeResponse {
                consumer_connected: false,
                tip_block: 0,
                oldest_block: 0,
                ring_buffer_entries: 0,
                ring_buffer_memory_bytes: 0,
            };
            let bytes = borsh::to_vec(&resp)?;
            stream.write_all(&bytes).await?;
            stream.shutdown().await?;
            debug!("probe handled inline");
            Ok(Classified::Handled)
        }
        0x01 => {
            ensure!(
                buf.len() >= SUBSCRIBE_MSG_SIZE,
                "subscribe message too short: got {} bytes, need {SUBSCRIBE_MSG_SIZE}",
                buf.len()
            );
            Ok(Classified::Subscribe(IpcConnection {
                stream: Some(stream),
                subscribe_buf: buf,
                writer_tx: None,
                write_handle: None,
            }))
        }
        other => {
            warn!(msg_type = other, "unknown client message type");
            stream.write_all(&[0xFF]).await?;
            stream.shutdown().await?;
            Ok(Classified::Handled)
        }
    }
}

/// Mirror of the exex `ProbeResponse` for borsh encoding.
/// Kept private -- only used for the inline probe reply.
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize)]
struct MinimalProbeResponse {
    consumer_connected: bool,
    tip_block: u64,
    oldest_block: u64,
    ring_buffer_entries: u64,
    ring_buffer_memory_bytes: u64,
}

// -- Connection (server side) ------------------------------------------------

/// A single server-to-client IPC connection after the subscribe handshake byte
/// has been read.
pub struct IpcConnection {
    stream: Option<UnixStream>,
    subscribe_buf: Vec<u8>,
    writer_tx: Option<mpsc::Sender<RecordBatch>>,
    write_handle: Option<JoinHandle<eyre::Result<()>>>,
}

#[async_trait]
impl ExExConnection for IpcConnection {
    async fn recv_subscribe(&mut self) -> eyre::Result<u64> {
        let block = u64::from_le_bytes(
            self.subscribe_buf[1..SUBSCRIBE_MSG_SIZE]
                .try_into()
                .map_err(|_| eyre!("invalid subscribe buffer length"))?,
        );
        Ok(block)
    }

    async fn send_handshake(&mut self, oldest: u64, tip: u64) -> eyre::Result<()> {
        let stream = self
            .stream
            .take()
            .ok_or_else(|| eyre!("handshake already sent"))?;

        let mut resp = [0u8; HANDSHAKE_RESPONSE_SIZE];
        resp[0] = 1; // protocol version
        resp[1..9].copy_from_slice(&oldest.to_le_bytes());
        resp[9..17].copy_from_slice(&tip.to_le_bytes());

        // Write the handshake bytes before handing the stream to the blocking writer.
        let mut async_stream = stream;
        async_stream.write_all(&resp).await?;

        let std_stream = async_stream.into_std()?;
        std_stream.set_nonblocking(false)?;

        let schema = entity_events_schema();
        let (tx, mut rx) = mpsc::channel::<RecordBatch>(WRITER_CHANNEL_SIZE);

        let handle = tokio::task::spawn_blocking(move || -> eyre::Result<()> {
            let mut writer = StreamWriter::try_new(&std_stream, &schema)?;
            while let Some(batch) = rx.blocking_recv() {
                writer.write(&batch)?;
            }
            writer.finish()?;
            Ok(())
        });

        self.writer_tx = Some(tx);
        self.write_handle = Some(handle);
        Ok(())
    }

    async fn send_batch(&mut self, batch: &RecordBatch) -> eyre::Result<()> {
        let tx = self
            .writer_tx
            .as_ref()
            .ok_or_else(|| eyre!("send_batch called before send_handshake"))?;
        tx.send(batch.clone())
            .await
            .map_err(|_| eyre!("IPC writer thread gone"))?;
        Ok(())
    }

    async fn finish(&mut self) -> eyre::Result<()> {
        // Drop the sender so the blocking writer thread sees channel close.
        self.writer_tx.take();

        if let Some(handle) = self.write_handle.take() {
            handle
                .await
                .map_err(|e| eyre!("IPC writer thread panicked: {e}"))??;
        }
        Ok(())
    }
}

// -- Client ------------------------------------------------------------------

/// IPC transport client. Connects to a node's Unix socket.
pub struct IpcClient {
    socket_path: PathBuf,
}

impl IpcClient {
    #[must_use]
    pub const fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }
}

#[async_trait]
impl ExExTransportClient for IpcClient {
    async fn probe(&self) -> eyre::Result<HandshakeInfo> {
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        stream.write_all(&[0x00]).await?;

        // ProbeResponse is borsh-encoded: bool(1) + 4 x u64(32) = 33 bytes
        let mut buf = vec![0u8; 256];
        let n = stream.read(&mut buf).await?;
        ensure!(n >= 17, "probe response too short ({n} bytes)");
        buf.truncate(n);

        let resp: MinimalProbeResponse = borsh::from_slice(&buf)?;
        Ok(HandshakeInfo {
            oldest_block: resp.oldest_block,
            tip_block: resp.tip_block,
        })
    }

    async fn subscribe(
        self: Box<Self>,
        resume_block: u64,
    ) -> eyre::Result<(
        HandshakeInfo,
        Pin<Box<dyn Stream<Item = eyre::Result<RecordBatch>> + Send>>,
    )> {
        let mut stream = UnixStream::connect(&self.socket_path).await?;

        // Send subscribe message: [0x01, resume_block LE]
        let mut msg = [0u8; SUBSCRIBE_MSG_SIZE];
        msg[0] = 0x01;
        msg[1..9].copy_from_slice(&resume_block.to_le_bytes());
        stream.write_all(&msg).await?;

        // Read 17-byte handshake response
        let mut resp = [0u8; HANDSHAKE_RESPONSE_SIZE];
        stream.read_exact(&mut resp).await?;
        ensure!(resp[0] == 1, "unexpected protocol version: {}", resp[0]);

        let oldest_block = u64::from_le_bytes(resp[1..9].try_into()?);
        let tip_block = u64::from_le_bytes(resp[9..17].try_into()?);
        let info = HandshakeInfo {
            oldest_block,
            tip_block,
        };

        // Spawn a blocking reader for the Arrow IPC stream.
        let std_stream = stream.into_std()?;
        std_stream.set_nonblocking(false)?;

        let (tx, rx) = mpsc::channel::<eyre::Result<RecordBatch>>(WRITER_CHANNEL_SIZE);

        tokio::task::spawn_blocking(move || {
            let reader = match StreamReader::try_new(&std_stream, None) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e.into()));
                    return;
                }
            };
            for result in reader {
                match result {
                    Ok(batch) => {
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break; // receiver dropped
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e.into()));
                        break;
                    }
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok((info, Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn test_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
        entity_events_schema()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ipc_server_accepts_and_handshakes() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");
        let cancel = CancellationToken::new();
        let server = IpcServer::new(sock.clone(), cancel.clone()).unwrap();

        let server_handle = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let resume = conn.recv_subscribe().await.unwrap();
            conn.send_handshake(10, 42).await.unwrap();
            conn.finish().await.unwrap();
            resume
        });

        // client side
        let mut stream = UnixStream::connect(&sock).await.unwrap();
        let mut msg = [0u8; SUBSCRIBE_MSG_SIZE];
        msg[0] = 0x01;
        msg[1..9].copy_from_slice(&77u64.to_le_bytes());
        stream.write_all(&msg).await.unwrap();

        let mut resp = [0u8; HANDSHAKE_RESPONSE_SIZE];
        stream.read_exact(&mut resp).await.unwrap();

        assert_eq!(resp[0], 1); // protocol version
        let oldest = u64::from_le_bytes(resp[1..9].try_into().unwrap());
        let tip = u64::from_le_bytes(resp[9..17].try_into().unwrap());
        assert_eq!(oldest, 10);
        assert_eq!(tip, 42);

        let resume = server_handle.await.unwrap();
        assert_eq!(resume, 77);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ipc_client_subscribes_and_reads_stream() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");
        let cancel = CancellationToken::new();
        let server = IpcServer::new(sock.clone(), cancel.clone()).unwrap();

        let schema = test_schema();
        let batch = RecordBatch::new_empty(schema);

        let send_batch = batch.clone();
        let server_handle = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let _resume = conn.recv_subscribe().await.unwrap();
            conn.send_handshake(5, 100).await.unwrap();
            conn.send_batch(&send_batch).await.unwrap();
            conn.send_batch(&send_batch).await.unwrap();
            conn.finish().await.unwrap();
        });

        let client = Box::new(IpcClient::new(sock));
        let (info, mut stream) = client.subscribe(0).await.unwrap();
        assert_eq!(info.oldest_block, 5);
        assert_eq!(info.tip_block, 100);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            let received = result.unwrap();
            assert_eq!(received.schema(), batch.schema());
            count += 1;
        }
        assert_eq!(count, 2);
        server_handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ipc_server_returns_err_on_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");
        let cancel = CancellationToken::new();
        let server = IpcServer::new(sock, cancel.clone()).unwrap();

        cancel.cancel();
        let result = server.accept().await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ipc_probe_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");
        let cancel = CancellationToken::new();
        let server = IpcServer::new(sock.clone(), cancel.clone()).unwrap();

        // The server handles probes inline and loops back to accept.
        // We need a second connection (subscribe) to unblock accept,
        // or we can just talk to the socket directly without going through accept.

        // Spawn a raw listener that handles the probe like the real server does.
        // Actually, IpcServer.accept() handles probes internally. So we spawn
        // accept() in the background and then send a probe + a subscribe to unblock it.
        let server_handle = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let _resume = conn.recv_subscribe().await.unwrap();
            conn.send_handshake(0, 0).await.unwrap();
            conn.finish().await.unwrap();
        });

        // Send probe
        let client = IpcClient::new(sock.clone());
        let info = client.probe().await.unwrap();
        assert_eq!(info.tip_block, 0);
        assert_eq!(info.oldest_block, 0);

        // Now send a subscribe to unblock the server's accept loop
        let mut stream = UnixStream::connect(&sock).await.unwrap();
        let mut msg = [0u8; SUBSCRIBE_MSG_SIZE];
        msg[0] = 0x01;
        stream.write_all(&msg).await.unwrap();

        // Read handshake to let the server finish cleanly
        let mut resp = [0u8; HANDSHAKE_RESPONSE_SIZE];
        stream.read_exact(&mut resp).await.unwrap();

        server_handle.await.unwrap();
    }
}

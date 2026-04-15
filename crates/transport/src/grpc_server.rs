use std::net::SocketAddr;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use eyre::eyre;
use glint_primitives::exex_schema::entity_events_schema;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::proto::ex_ex_stream_server::{ExExStream, ExExStreamServer};
use crate::proto::{ProbeRequest, ProbeResponse, StreamMessage, SubscribeRequest, stream_message};
use crate::{ExExConnection, ExExTransportServer, HandshakeInfo};

const BATCH_CHANNEL_SIZE: usize = 16;

struct IncomingSubscription {
    resume_block: u64,
    batch_tx: mpsc::Sender<Result<StreamMessage, Status>>,
}

struct ExExStreamService {
    probe_info: Arc<tokio::sync::RwLock<HandshakeInfo>>,
    incoming_tx: mpsc::Sender<IncomingSubscription>,
}

#[async_trait]
impl ExExStream for ExExStreamService {
    type SubscribeStream = ReceiverStream<Result<StreamMessage, Status>>;

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        let info = *self.probe_info.read().await;
        Ok(Response::new(ProbeResponse {
            oldest_block: info.oldest_block,
            tip_block: info.tip_block,
        }))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let resume_block = request.into_inner().resume_block;
        let (batch_tx, batch_rx) = mpsc::channel(BATCH_CHANNEL_SIZE);

        self.incoming_tx
            .send(IncomingSubscription {
                resume_block,
                batch_tx,
            })
            .await
            .map_err(|_| Status::unavailable("server shutting down"))?;

        Ok(Response::new(ReceiverStream::new(batch_rx)))
    }
}

pub struct GrpcServer {
    incoming_rx: Mutex<mpsc::Receiver<IncomingSubscription>>,
    pub(crate) probe_info: Arc<tokio::sync::RwLock<HandshakeInfo>>,
    bound_addr: SocketAddr,
    cancel: CancellationToken,
}

impl GrpcServer {
    pub async fn new(addr: SocketAddr, cancel: CancellationToken) -> eyre::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let bound_addr = listener.local_addr()?;
        debug!(%bound_addr, "gRPC server bound");

        let probe_info = Arc::new(tokio::sync::RwLock::new(HandshakeInfo {
            oldest_block: 0,
            tip_block: 0,
        }));

        let (incoming_tx, incoming_rx) = mpsc::channel::<IncomingSubscription>(16);

        let svc = ExExStreamService {
            probe_info: Arc::clone(&probe_info),
            incoming_tx,
        };

        let cancel_bg = cancel.clone();
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(ExExStreamServer::new(svc))
                .serve_with_incoming_shutdown(incoming, cancel_bg.cancelled())
                .await;
        });

        Ok(Self {
            incoming_rx: Mutex::new(incoming_rx),
            probe_info,
            bound_addr,
            cancel,
        })
    }

    #[must_use]
    pub const fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }
}

#[async_trait]
impl ExExTransportServer for GrpcServer {
    #[allow(clippy::significant_drop_tightening)] // must hold lock across select
    async fn accept(&self) -> eyre::Result<Box<dyn ExExConnection>> {
        let mut rx = self.incoming_rx.lock().await;
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => {
                Err(eyre!("gRPC server shutting down"))
            }
            sub = rx.recv() => {
                let sub = sub.ok_or_else(|| eyre!("gRPC server channel closed"))?;
                Ok(Box::new(GrpcConnection {
                    resume_block: sub.resume_block,
                    batch_tx: Some(sub.batch_tx),
                    handshake_sent: false,
                    probe_info: Arc::clone(&self.probe_info),
                }))
            }
        }
    }
}

pub struct GrpcConnection {
    resume_block: u64,
    batch_tx: Option<mpsc::Sender<Result<StreamMessage, Status>>>,
    handshake_sent: bool,
    probe_info: Arc<tokio::sync::RwLock<HandshakeInfo>>,
}

#[async_trait]
impl ExExConnection for GrpcConnection {
    async fn recv_subscribe(&mut self) -> eyre::Result<u64> {
        Ok(self.resume_block)
    }

    async fn send_handshake(&mut self, oldest: u64, tip: u64) -> eyre::Result<()> {
        eyre::ensure!(!self.handshake_sent, "handshake already sent");

        let info = HandshakeInfo {
            oldest_block: oldest,
            tip_block: tip,
        };
        *self.probe_info.write().await = info;

        let tx = self
            .batch_tx
            .as_ref()
            .ok_or_else(|| eyre!("send_handshake called after finish"))?;

        let msg = StreamMessage {
            payload: Some(stream_message::Payload::Handshake(ProbeResponse {
                oldest_block: oldest,
                tip_block: tip,
            })),
        };
        tx.send(Ok(msg))
            .await
            .map_err(|_| eyre!("gRPC batch receiver dropped"))?;
        self.handshake_sent = true;
        Ok(())
    }

    async fn send_batch(&mut self, batch: &RecordBatch) -> eyre::Result<()> {
        let tx = self
            .batch_tx
            .as_ref()
            .ok_or_else(|| eyre!("send_batch called after finish"))?;

        let schema = entity_events_schema();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
            writer.write(batch)?;
            writer.finish()?;
        }

        let msg = StreamMessage {
            payload: Some(stream_message::Payload::ArrowIpc(buf)),
        };
        tx.send(Ok(msg))
            .await
            .map_err(|_| eyre!("gRPC batch receiver dropped"))?;
        Ok(())
    }

    async fn finish(&mut self) -> eyre::Result<()> {
        self.batch_tx.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExExTransportClient;
    use crate::grpc_client::GrpcClient;
    use futures::StreamExt;

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_subscribe_round_trip() {
        let cancel = CancellationToken::new();
        let server = GrpcServer::new("127.0.0.1:0".parse().unwrap(), cancel.clone())
            .await
            .unwrap();
        let addr = server.bound_addr();

        let server_handle = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let resume = conn.recv_subscribe().await.unwrap();
            assert_eq!(resume, 42);
            conn.send_handshake(10, 100).await.unwrap();

            let schema = entity_events_schema();
            let batch = RecordBatch::new_empty(schema);
            conn.send_batch(&batch).await.unwrap();
            conn.finish().await.unwrap();
        });

        let client = Box::new(GrpcClient::new(format!("http://{addr}")));
        let (info, mut stream) = client.subscribe(42).await.unwrap();
        assert_eq!(info.oldest_block, 10);
        assert_eq!(info.tip_block, 100);

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(stream.next().await.is_none());

        server_handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_probe_returns_info() {
        let cancel = CancellationToken::new();
        let server = GrpcServer::new("127.0.0.1:0".parse().unwrap(), cancel.clone())
            .await
            .unwrap();
        let addr = server.bound_addr();

        {
            let mut info = server.probe_info.write().await;
            *info = HandshakeInfo {
                oldest_block: 5,
                tip_block: 99,
            };
        }

        let client = GrpcClient::new(format!("http://{addr}"));
        let info = client.probe().await.unwrap();
        assert_eq!(info.oldest_block, 5);
        assert_eq!(info.tip_block, 99);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_server_returns_err_on_cancel() {
        let cancel = CancellationToken::new();
        let server = GrpcServer::new("127.0.0.1:0".parse().unwrap(), cancel.clone())
            .await
            .unwrap();

        cancel.cancel();
        let result = server.accept().await;
        assert!(result.is_err());
    }
}

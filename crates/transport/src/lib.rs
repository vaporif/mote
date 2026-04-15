#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "grpc")]
pub mod grpc_client;
#[cfg(feature = "grpc")]
pub mod grpc_server;
#[cfg(feature = "ipc")]
pub mod ipc;
pub mod types;

#[cfg(feature = "grpc")]
#[allow(clippy::pedantic, clippy::nursery)]
pub mod proto {
    tonic::include_proto!("glint.exex.v1");
}

use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

pub use types::HandshakeInfo;

#[async_trait]
pub trait ExExTransportServer: Send + Sync + 'static {
    async fn accept(&self) -> eyre::Result<Box<dyn ExExConnection>>;
}

#[async_trait]
pub trait ExExConnection: Send + 'static {
    async fn recv_subscribe(&mut self) -> eyre::Result<u64>;
    async fn send_handshake(&mut self, oldest: u64, tip: u64) -> eyre::Result<()>;
    async fn send_batch(&mut self, batch: &RecordBatch) -> eyre::Result<()>;
    async fn finish(&mut self) -> eyre::Result<()>;
}

/// `subscribe` consumes the client; construct a new instance to reconnect.
#[async_trait]
pub trait ExExTransportClient: Send + 'static {
    async fn probe(&self) -> eyre::Result<HandshakeInfo>;

    async fn subscribe(
        self: Box<Self>,
        resume_block: u64,
    ) -> eyre::Result<(
        HandshakeInfo,
        Pin<Box<dyn futures::Stream<Item = eyre::Result<RecordBatch>> + Send>>,
    )>;
}

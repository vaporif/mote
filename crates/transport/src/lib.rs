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

/// Server-side transport: accepts incoming sidecar connections.
///
/// Implementations receive a `CancellationToken` via their constructor.
/// `accept` returns `Err` when the token fires, breaking the accept loop.
#[async_trait]
pub trait ExExTransportServer: Send + Sync + 'static {
    async fn accept(&self) -> eyre::Result<Box<dyn ExExConnection>>;
}

/// A single server-side connection to one sidecar client.
#[async_trait]
pub trait ExExConnection: Send + 'static {
    /// Wait for the client's subscribe request, return the resume block.
    async fn recv_subscribe(&mut self) -> eyre::Result<u64>;

    /// Send handshake response with ring buffer bounds.
    async fn send_handshake(&mut self, oldest: u64, tip: u64) -> eyre::Result<()>;

    /// Send one Arrow IPC record batch.
    async fn send_batch(&mut self, batch: &RecordBatch) -> eyre::Result<()>;

    /// Signal end of stream.
    async fn finish(&mut self) -> eyre::Result<()>;
}

/// Client-side transport: connects to a node's `ExEx` stream.
///
/// `subscribe` consumes the client. The returned stream owns the connection.
/// For reconnection, construct a new client instance.
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

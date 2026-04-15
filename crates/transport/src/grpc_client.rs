use std::io::Cursor;
use std::pin::Pin;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use eyre::eyre;
use futures::Stream;
use tokio_stream::StreamExt;

use crate::proto::ex_ex_stream_client::ExExStreamClient;
use crate::proto::{ProbeRequest, SubscribeRequest};
use crate::{ExExTransportClient, HandshakeInfo};

pub struct GrpcClient {
    endpoint: String,
}

impl GrpcClient {
    #[must_use]
    pub const fn new(endpoint: String) -> Self {
        Self { endpoint }
    }
}

#[async_trait]
impl ExExTransportClient for GrpcClient {
    async fn probe(&self) -> eyre::Result<HandshakeInfo> {
        let mut client = ExExStreamClient::connect(self.endpoint.clone()).await?;
        let resp = client.probe(ProbeRequest {}).await?.into_inner();
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
        let mut client = ExExStreamClient::connect(self.endpoint.clone()).await?;

        // Subscribe first -- the server blocks the RPC response until handshake
        // completes, so probe_info is guaranteed to be set by the time this returns.
        let stream = client
            .subscribe(SubscribeRequest { resume_block })
            .await?
            .into_inner();

        let probe_resp = client.probe(ProbeRequest {}).await?.into_inner();
        let info = HandshakeInfo {
            oldest_block: probe_resp.oldest_block,
            tip_block: probe_resp.tip_block,
        };

        let mapped = stream.map(|result| {
            let msg = result.map_err(|e| eyre!("gRPC stream error: {e}"))?;
            let cursor = Cursor::new(msg.arrow_ipc);
            let mut reader = StreamReader::try_new(cursor, None)?;
            reader
                .next()
                .ok_or_else(|| eyre!("empty Arrow IPC payload"))?
                .map_err(Into::into)
        });

        Ok((info, Box::pin(mapped)))
    }
}

use std::io::Cursor;
use std::pin::Pin;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use eyre::eyre;
use futures::Stream;
use tokio_stream::StreamExt;

use crate::proto::ex_ex_stream_client::ExExStreamClient;
use crate::proto::{ProbeRequest, SubscribeRequest, stream_message};
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
        let mut stream = client
            .subscribe(SubscribeRequest { resume_block })
            .await?
            .into_inner();

        let first = stream
            .next()
            .await
            .ok_or_else(|| eyre!("stream closed before handshake"))?
            .map_err(|e| eyre!("gRPC error waiting for handshake: {e}"))?;

        let info = match first.payload {
            Some(stream_message::Payload::Handshake(h)) => HandshakeInfo {
                oldest_block: h.oldest_block,
                tip_block: h.tip_block,
            },
            _ => return Err(eyre!("expected handshake as first message")),
        };

        let mapped = stream.map(|result| {
            let msg = result.map_err(|e| eyre!("gRPC stream error: {e}"))?;
            match msg.payload {
                Some(stream_message::Payload::ArrowIpc(bytes)) => {
                    let cursor = Cursor::new(bytes);
                    let mut reader = StreamReader::try_new(cursor, None)?;
                    reader
                        .next()
                        .ok_or_else(|| eyre!("empty Arrow IPC payload"))?
                        .map_err(Into::into)
                }
                _ => Err(eyre!("unexpected non-batch message in stream")),
            }
        });

        Ok((info, Box::pin(mapped)))
    }
}

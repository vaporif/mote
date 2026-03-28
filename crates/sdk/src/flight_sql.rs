// TODO: add sqlx-like typed query interface (bind params, row mapping) over Flight SQL

use arrow::record_batch::RecordBatch;
use arrow_flight::FlightDescriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use futures::TryStreamExt;
use prost::Message;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct GlintFlightClient {
    client: FlightServiceClient<Channel>,
}

impl GlintFlightClient {
    pub async fn connect(url: impl Into<String>) -> eyre::Result<Self> {
        let channel = Channel::from_shared(url.into())?.connect().await?;
        Ok(Self {
            client: FlightServiceClient::new(channel),
        })
    }

    pub async fn query(&mut self, sql: &str) -> eyre::Result<Vec<RecordBatch>> {
        let cmd = CommandStatementQuery {
            query: sql.to_owned(),
            ..Default::default()
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());

        let flight_info = self.client.get_flight_info(descriptor).await?.into_inner();

        let ticket = flight_info
            .endpoint
            .first()
            .and_then(|ep| ep.ticket.as_ref())
            .ok_or_else(|| eyre::eyre!("no endpoint/ticket in flight info"))?
            .clone();

        let stream = self.client.do_get(ticket).await?.into_inner();
        let flight_data: Vec<_> = stream.try_collect().await?;
        let batches = arrow_flight::utils::flight_data_to_batches(&flight_data)?;
        Ok(batches)
    }
}

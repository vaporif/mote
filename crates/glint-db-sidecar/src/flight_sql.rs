use std::sync::Arc;

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::FlightServiceServer,
    sql::{
        CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
        server::FlightSqlService,
    },
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use prost::Message as _;
use tokio::sync::watch;
use tonic::{Request, Response, Status, transport::Server};

const MAX_QUERY_LENGTH: usize = 16_384;

pub async fn serve_flight_sql(
    port: u16,
    ctx: Arc<SessionContext>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> eyre::Result<()> {
    // TODO: add tokio::sync::Semaphore for concurrent query limit
    // TODO: wrap query execution in tokio::time::timeout
    let service = GlintFlightSqlService { ctx };
    let svc = FlightServiceServer::new(service);
    // TODO: configure max gRPC message size via tonic Server::builder()
    let addr = format!("0.0.0.0:{port}").parse()?;
    tracing::info!(%addr, "Flight SQL server listening");
    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, async move {
            while !*shutdown_rx.borrow_and_update() {
                if shutdown_rx.changed().await.is_err() {
                    return;
                }
            }
        })
        .await?;
    Ok(())
}

#[derive(Clone)]
struct GlintFlightSqlService {
    ctx: Arc<SessionContext>,
}

#[tonic::async_trait]
impl FlightSqlService for GlintFlightSqlService {
    type FlightService = Self;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        if query.query.len() > MAX_QUERY_LENGTH {
            return Err(Status::invalid_argument(format!(
                "query exceeds maximum length of {MAX_QUERY_LENGTH} bytes"
            )));
        }

        let descriptor = request.into_inner();

        let ticket_payload = TicketStatementQuery {
            statement_handle: query.query.clone().into(),
        }
        .as_any()
        .encode_to_vec();
        let ticket = Ticket::new(ticket_payload);
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let df = self
            .ctx
            .sql(&query.query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = df.schema().inner().clone();

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("schema serialization failed: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint);

        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let query = String::from_utf8(ticket.statement_handle.into())
            .map_err(|_| Status::invalid_argument("statement handle is not valid UTF-8"))?;

        if query.len() > MAX_QUERY_LENGTH {
            return Err(Status::invalid_argument(format!(
                "query exceeds maximum length of {MAX_QUERY_LENGTH} bytes"
            )));
        }

        let batches = self
            .execute_sql(&query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = FlightDataEncoderBuilder::new()
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

impl GlintFlightSqlService {
    async fn execute_sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, datafusion::error::DataFusionError> {
        self.ctx.sql(query).await?.collect().await
    }
}

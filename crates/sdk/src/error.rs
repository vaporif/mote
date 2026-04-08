use alloy_primitives::B256;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("RPC error: {0}")]
    Rpc(#[from] jsonrpsee::core::ClientError),

    #[error("transport error: {0}")]
    Transport(#[from] alloy_transport::TransportError),

    #[error("transaction error: {0}")]
    PendingTx(#[from] alloy_provider::PendingTransactionError),

    #[error("validation error: {0}")]
    Validation(#[from] glint_primitives::error::GlintError),

    #[error("transaction reverted (tx {0})")]
    Reverted(B256),

    #[error("flight SQL not configured — use builder with .flight_url()")]
    FlightNotConfigured,

    #[error("flight connect error: {0}")]
    FlightConnect(#[from] tonic::transport::Error),

    #[error("flight query error: {0}")]
    FlightStatus(#[from] tonic::Status),

    #[error("flight decode error: {0}")]
    FlightDecode(#[from] arrow::error::ArrowError),

    #[error("flight response missing endpoint/ticket")]
    FlightNoTicket,

    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
}

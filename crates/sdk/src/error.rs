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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use glint_primitives::error::GlintError;

    #[test]
    fn reverted_error_contains_tx_hash() {
        let hash = B256::repeat_byte(0xAB);
        let err = Error::Reverted(hash);
        let msg = err.to_string();
        assert!(msg.contains("reverted"), "expected 'reverted' in: {msg}");
        assert!(
            msg.contains(&format!("{hash}")),
            "expected tx hash in: {msg}"
        );
    }

    #[test]
    fn validation_error_converts_from_glint_error() {
        let glint_err = GlintError::EmptyTransaction;
        let err = Error::from(glint_err);
        assert!(
            matches!(err, Error::Validation(GlintError::EmptyTransaction)),
            "expected Validation variant"
        );
    }

    #[test]
    fn invalid_url_converts() {
        let url_err: Result<url::Url, _> = "not a url".parse();
        let err = Error::from(url_err.unwrap_err());
        assert!(matches!(err, Error::InvalidUrl(_)));
    }
}

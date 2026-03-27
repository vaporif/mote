use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};

pub use glint_primitives::rpc::{BlockTiming, GlintApiClient};

#[derive(Debug, Clone)]
pub struct GlintRpcClient {
    client: HttpClient,
}

impl GlintRpcClient {
    pub fn new(url: &str) -> eyre::Result<Self> {
        let client = HttpClientBuilder::default().build(url)?;
        Ok(Self { client })
    }
}

impl std::ops::Deref for GlintRpcClient {
    type Target = HttpClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

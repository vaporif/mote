use alloy_primitives::B256;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::params::ArrayParams;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};

use glint_primitives::entity::EntityInfo;

#[derive(Debug, Clone)]
pub struct GlintRpcClient {
    client: HttpClient,
}

impl GlintRpcClient {
    pub fn new(url: &str) -> eyre::Result<Self> {
        let client = HttpClientBuilder::default().build(url)?;
        Ok(Self { client })
    }

    pub async fn get_entity(&self, key: B256) -> eyre::Result<Option<EntityInfo>> {
        let mut params = ArrayParams::new();
        params.insert(key)?;
        let result: Option<EntityInfo> = self.client.request("glint_getEntity", params).await?;
        Ok(result)
    }

    pub async fn get_entity_count(&self) -> eyre::Result<u64> {
        let result: u64 = self
            .client
            .request("glint_getEntityCount", ArrayParams::new())
            .await?;
        Ok(result)
    }
}

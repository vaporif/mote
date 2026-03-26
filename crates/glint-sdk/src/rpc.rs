use alloy_primitives::{Address, B256};
use glint_primitives::transaction::ExtendPolicy;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::params::ArrayParams;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use serde::{Deserialize, Serialize};

/// Entity info from the Glint RPC. Decoupled from the node crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityInfo {
    pub owner: Address,
    pub expires_at_block: u64,
    pub extend_policy: ExtendPolicy,
    pub operator: Option<Address>,
    pub content_hash: B256,
}

/// Low-level JSON-RPC client for `glint_*` namespace.
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

use alloy_primitives::B256;

use crate::rpc::{EntityInfo, GlintRpcClient};

/// High-level Glint client. Read-only until alloy-signer is wired up.
// TODO: add `create_entity`, `update_entity`, `extend_entity`, `delete_entity` methods
// once alloy-signer / alloy-network are available as workspace deps for signing + sending txs.
#[derive(Debug, Clone)]
pub struct GlintClient {
    rpc: GlintRpcClient,
}

impl GlintClient {
    pub fn new(url: &str) -> eyre::Result<Self> {
        let rpc = GlintRpcClient::new(url)?;
        Ok(Self { rpc })
    }

    pub async fn get_entity(&self, key: B256) -> eyre::Result<Option<EntityInfo>> {
        self.rpc.get_entity(key).await
    }

    pub async fn get_entity_count(&self) -> eyre::Result<u64> {
        self.rpc.get_entity_count().await
    }

    pub const fn rpc(&self) -> &GlintRpcClient {
        &self.rpc
    }
}

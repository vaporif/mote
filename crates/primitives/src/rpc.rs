use alloy_primitives::B256;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use serde::{Deserialize, Serialize};

use crate::entity::EntityInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockTiming {
    pub block_number: u64,
    pub timestamp: u64,
}

#[rpc(server, client, namespace = "glint")]
pub trait GlintApi {
    #[method(name = "getEntity")]
    async fn get_entity(&self, entity_key: B256) -> RpcResult<Option<EntityInfo>>;

    #[method(name = "getEntityCount")]
    async fn get_entity_count(&self) -> RpcResult<u64>;

    #[method(name = "getUsedSlots")]
    async fn get_used_slots(&self) -> RpcResult<u64>;

    #[method(name = "getBlockTiming")]
    async fn get_block_timing(&self) -> RpcResult<BlockTiming>;
}

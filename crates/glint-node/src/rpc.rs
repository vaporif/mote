use alloy_primitives::{B256, U256};
use jsonrpsee::core::RpcResult;
use jsonrpsee::core::async_trait;
use jsonrpsee::proc_macros::rpc;
use reth_provider::StateProviderFactory;
use tracing::{debug, instrument};

use glint_engine::slot_counter::ENTITY_COUNT_KEY;
use glint_primitives::constants::PROCESSOR_ADDRESS;
use glint_primitives::entity::{EntityInfo, EntityMetadata};
use glint_primitives::storage::{
    decode_operator_value, entity_content_hash_key, entity_operator_key, entity_storage_key,
};

// TODO: glint_getEntitiesByOwner -- needs reverse index (Level B)
// TODO: glint_getUsedSlots -- trivial, read one slot (Level B)
// TODO: glint_getBlockTiming -- needs parent header (Level B)
// TODO: glint_queryEntities -- needs query engine / sidecar proxy (Level C)
// TODO: glint_getEntityPayload -- needs event logs / sidecar (Level C)

#[rpc(server, namespace = "glint")]
pub trait GlintApi {
    #[method(name = "getEntity")]
    async fn get_entity(&self, entity_key: B256) -> RpcResult<Option<EntityInfo>>;

    #[method(name = "getEntityCount")]
    async fn get_entity_count(&self) -> RpcResult<u64>;
}

#[derive(Debug, Clone)]
pub struct GlintRpc<Provider> {
    provider: Provider,
}

impl<Provider> GlintRpc<Provider> {
    pub const fn new(provider: Provider) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<Provider> GlintApiServer for GlintRpc<Provider>
where
    Provider: StateProviderFactory + 'static,
{
    #[instrument(skip(self), fields(%entity_key), name = "glint_rpc::get_entity")]
    async fn get_entity(&self, entity_key: B256) -> RpcResult<Option<EntityInfo>> {
        let state = self
            .provider
            .latest()
            .map_err(|e| internal_err(format!("failed to get latest state: {e}")))?;

        let read = |slot| {
            state
                .storage(PROCESSOR_ADDRESS, slot)
                .map_err(|e| internal_err(format!("storage read failed: {e}")))
        };

        let meta_value = read(entity_storage_key(&entity_key))?;

        let Some(meta_value) = meta_value else {
            debug!("entity not found (no storage value)");
            return Ok(None);
        };

        if meta_value == U256::ZERO {
            debug!("entity not found (zero value)");
            return Ok(None);
        }

        let meta = EntityMetadata::decode(&meta_value.to_be_bytes::<32>());
        debug!(owner = ?meta.owner, expires_at_block = meta.expires_at_block, "found entity metadata");

        let operator = if meta.has_operator {
            read(entity_operator_key(&entity_key))?.map(decode_operator_value)
        } else {
            None
        };

        let content_hash = read(entity_content_hash_key(&entity_key))?
            .map_or(B256::ZERO, |v| B256::from(v.to_be_bytes::<32>()));

        Ok(Some(EntityInfo {
            owner: meta.owner,
            expires_at_block: meta.expires_at_block,
            extend_policy: meta.extend_policy,
            operator,
            content_hash,
        }))
    }

    #[instrument(skip(self), name = "glint_rpc::get_entity_count")]
    async fn get_entity_count(&self) -> RpcResult<u64> {
        let state = self
            .provider
            .latest()
            .map_err(|e| internal_err(format!("failed to get latest state: {e}")))?;

        let count_value = state
            .storage(PROCESSOR_ADDRESS, ENTITY_COUNT_KEY)
            .map_err(|e| internal_err(format!("failed to read entity count slot: {e}")))?;

        let count: u64 = count_value
            .unwrap_or(U256::ZERO)
            .try_into()
            .map_err(|_| internal_err("entity count overflows u64"))?;

        debug!(count, "returning entity count");
        Ok(count)
    }
}

fn internal_err(msg: impl std::fmt::Display) -> jsonrpsee::types::ErrorObject<'static> {
    jsonrpsee::types::ErrorObject::owned(
        jsonrpsee::types::error::INTERNAL_ERROR_CODE,
        msg.to_string(),
        None::<()>,
    )
}

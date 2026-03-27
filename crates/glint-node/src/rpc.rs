use alloy_consensus::BlockHeader;
use alloy_primitives::{B256, U256};
use jsonrpsee::core::RpcResult;
use jsonrpsee::core::async_trait;
use reth_provider::{HeaderProvider, StateProviderFactory};
use tracing::{debug, instrument};

use glint_engine::slot_counter::{ENTITY_COUNT_KEY, USED_SLOTS_KEY};
use glint_primitives::constants::PROCESSOR_ADDRESS;
use glint_primitives::entity::{EntityInfo, EntityMetadata};
pub use glint_primitives::rpc::{BlockTiming, GlintApiServer};
use glint_primitives::storage::{
    decode_operator_value, entity_content_hash_key, entity_operator_key, entity_storage_key,
};

// TODO: glint_getEntitiesByOwner -- needs reverse index or sidecar (use Flight SQL)
// TODO: glint_queryEntities -- use Flight SQL on the analytics sidecar
// TODO: glint_getEntityPayload -- needs event logs / sidecar

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
    Provider: StateProviderFactory + HeaderProvider + 'static,
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
        read_u64_slot(&self.provider, ENTITY_COUNT_KEY, "entity count")
    }

    #[instrument(skip(self), name = "glint_rpc::get_used_slots")]
    async fn get_used_slots(&self) -> RpcResult<u64> {
        read_u64_slot(&self.provider, USED_SLOTS_KEY, "used slots")
    }

    #[instrument(skip(self), name = "glint_rpc::get_block_timing")]
    async fn get_block_timing(&self) -> RpcResult<BlockTiming> {
        let block_number = self
            .provider
            .best_block_number()
            .map_err(|e| internal_err(format!("failed to get best block number: {e}")))?;

        let header = self
            .provider
            .header_by_number(block_number)
            .map_err(|e| internal_err(format!("failed to read header: {e}")))?
            .ok_or_else(|| internal_err(format!("header not found for block {block_number}")))?;

        let timing = BlockTiming {
            block_number,
            timestamp: header.timestamp(),
        };

        debug!(
            block_number,
            timestamp = timing.timestamp,
            "returning block timing"
        );
        Ok(timing)
    }
}

fn read_u64_slot(provider: &impl StateProviderFactory, key: B256, label: &str) -> RpcResult<u64> {
    let state = provider
        .latest()
        .map_err(|e| internal_err(format!("failed to get latest state: {e}")))?;

    let value = state
        .storage(PROCESSOR_ADDRESS, key)
        .map_err(|e| internal_err(format!("failed to read {label} slot: {e}")))?;

    value
        .unwrap_or(U256::ZERO)
        .try_into()
        .map_err(|_| internal_err(format!("{label} overflows u64")))
}

fn internal_err(msg: impl std::fmt::Display) -> jsonrpsee::types::ErrorObject<'static> {
    jsonrpsee::types::ErrorObject::owned(
        jsonrpsee::types::error::INTERNAL_ERROR_CODE,
        msg.to_string(),
        None::<()>,
    )
}

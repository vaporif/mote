use alloy_network::Ethereum;
use alloy_primitives::{B256, U256};
use alloy_provider::{DynProvider, Provider};
use alloy_rlp::Encodable;
use alloy_rpc_types_eth::TransactionReceipt;

use glint_primitives::constants::PROCESSOR_ADDRESS;
use glint_primitives::entity::EntityInfo;

use crate::entity::{ChangeOwnerEntity, CreateEntity, DeleteEntity, ExtendEntity, UpdateEntity};
use crate::tx::build_glint_transaction;

pub struct GlintClient {
    provider: DynProvider<Ethereum>,
}

impl GlintClient {
    pub fn new(provider: impl Provider<Ethereum> + 'static) -> Self {
        Self {
            provider: provider.erased(),
        }
    }

    pub const fn provider(&self) -> &DynProvider<Ethereum> {
        &self.provider
    }

    pub async fn get_entity(&self, key: B256) -> eyre::Result<Option<EntityInfo>> {
        let result: Option<EntityInfo> = self
            .provider
            .raw_request("glint_getEntity".into(), (key,))
            .await?;
        Ok(result)
    }

    pub async fn get_entity_count(&self) -> eyre::Result<u64> {
        let count: u64 = self
            .provider
            .raw_request("glint_getEntityCount".into(), ())
            .await?;
        Ok(count)
    }

    pub async fn send_glint_transaction(
        &self,
        creates: &[CreateEntity],
        updates: &[UpdateEntity],
        deletes: &[DeleteEntity],
        extends: &[ExtendEntity],
        change_owners: &[ChangeOwnerEntity],
    ) -> eyre::Result<TransactionReceipt> {
        let tx = build_glint_transaction(creates, updates, deletes, extends, change_owners)?;

        let mut calldata = Vec::new();
        tx.encode(&mut calldata);

        let tx_request = alloy_rpc_types_eth::TransactionRequest::default()
            .to(PROCESSOR_ADDRESS)
            .value(U256::ZERO)
            .input(alloy_rpc_types_eth::TransactionInput::new(calldata.into()))
            .gas_limit(1_000_000);

        let pending = self.provider.send_transaction(tx_request).await?;
        let receipt = pending.get_receipt().await?;
        Ok(receipt)
    }
}

use std::marker::PhantomData;

use alloy_network::{Ethereum, EthereumWallet};
use alloy_primitives::{B256, U256};
use alloy_provider::{DynProvider, Provider, ProviderBuilder};
use alloy_rlp::Encodable;
use alloy_rpc_types_eth::TransactionReceipt;

use glint_primitives::constants::PROCESSOR_ADDRESS;
use glint_primitives::entity::EntityInfo;
use glint_primitives::rpc::BlockTiming;

use crate::entity::{ChangeOwnerEntity, CreateEntity, DeleteEntity, ExtendEntity, UpdateEntity};
use crate::tx::build_glint_transaction;

const DEFAULT_GAS_LIMIT: u64 = 1_000_000;

mod sealed {
    pub trait State: Send + Sync {}
}

pub struct ReadOnly;
pub struct ReadWrite;

impl sealed::State for ReadOnly {}
impl sealed::State for ReadWrite {}

pub struct Glint<S: sealed::State = ReadOnly> {
    provider: DynProvider<Ethereum>,
    gas_limit: u64,
    #[cfg(feature = "flight_sql")]
    flight: Option<crate::flight_sql::GlintFlightClient>,
    _state: PhantomData<S>,
}

impl Glint<ReadOnly> {
    pub fn connect(rpc_url: &str) -> eyre::Result<Self> {
        let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
        Ok(Self::from_provider(provider))
    }
}

impl Glint<ReadWrite> {
    pub fn with_wallet(rpc_url: &str, wallet: EthereumWallet) -> eyre::Result<Self> {
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(rpc_url.parse()?);
        Ok(Self::from_provider(provider))
    }
}

impl<S: sealed::State> Glint<S> {
    pub fn from_provider(provider: impl Provider<Ethereum> + 'static) -> Self {
        Self {
            provider: provider.erased(),
            gas_limit: DEFAULT_GAS_LIMIT,
            #[cfg(feature = "flight_sql")]
            flight: None,
            _state: PhantomData,
        }
    }

    pub const fn provider(&self) -> &DynProvider<Ethereum> {
        &self.provider
    }
}

impl Glint {
    pub fn builder(rpc_url: &str) -> GlintBuilder<ReadOnly> {
        GlintBuilder {
            rpc_url: rpc_url.to_owned(),
            wallet: None,
            gas_limit: DEFAULT_GAS_LIMIT,
            #[cfg(feature = "flight_sql")]
            flight_url: None,
            _state: PhantomData,
        }
    }
}

impl<S: sealed::State> Glint<S> {
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

    pub async fn get_used_slots(&self) -> eyre::Result<u64> {
        let slots: u64 = self
            .provider
            .raw_request("glint_getUsedSlots".into(), ())
            .await?;
        Ok(slots)
    }

    pub async fn get_block_timing(&self) -> eyre::Result<BlockTiming> {
        let timing: BlockTiming = self
            .provider
            .raw_request("glint_getBlockTiming".into(), ())
            .await?;
        Ok(timing)
    }
}

#[cfg(feature = "flight_sql")]
impl<S: sealed::State> Glint<S> {
    pub async fn query(
        &mut self,
        sql: &str,
    ) -> eyre::Result<Vec<arrow::record_batch::RecordBatch>> {
        let flight = self
            .flight
            .as_mut()
            .ok_or_else(|| eyre::eyre!("flight SQL not configured — use builder with .flight_url()"))?;
        flight.query(sql).await
    }
}

impl Glint<ReadWrite> {
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
            .gas_limit(self.gas_limit);

        let pending = self.provider.send_transaction(tx_request).await?;
        let receipt = pending.get_receipt().await?;
        Ok(receipt)
    }
}

pub struct GlintBuilder<S: sealed::State = ReadOnly> {
    rpc_url: String,
    wallet: Option<EthereumWallet>,
    gas_limit: u64,
    #[cfg(feature = "flight_sql")]
    flight_url: Option<String>,
    _state: PhantomData<S>,
}

impl<S: sealed::State> GlintBuilder<S> {
    #[must_use]
    pub const fn gas_limit(mut self, limit: u64) -> Self {
        self.gas_limit = limit;
        self
    }

    #[cfg(feature = "flight_sql")]
    #[must_use]
    pub fn flight_url(mut self, url: &str) -> Self {
        self.flight_url = Some(url.to_owned());
        self
    }
}

impl GlintBuilder<ReadOnly> {
    #[must_use]
    pub fn wallet(self, wallet: EthereumWallet) -> GlintBuilder<ReadWrite> {
        GlintBuilder {
            rpc_url: self.rpc_url,
            wallet: Some(wallet),
            gas_limit: self.gas_limit,
            #[cfg(feature = "flight_sql")]
            flight_url: self.flight_url,
            _state: PhantomData,
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn build(self) -> eyre::Result<Glint<ReadOnly>> {
        let url = self.rpc_url.parse()?;
        let provider = ProviderBuilder::new().connect_http(url);
        let mut client = Glint::<ReadOnly>::from_provider(provider);
        client.gas_limit = self.gas_limit;
        #[cfg(feature = "flight_sql")]
        if let Some(ref flight_url) = self.flight_url {
            client.flight =
                Some(crate::flight_sql::GlintFlightClient::connect(flight_url.as_str()).await?);
        }
        Ok(client)
    }
}

impl GlintBuilder<ReadWrite> {
    #[allow(clippy::unused_async)]
    pub async fn build(self) -> eyre::Result<Glint<ReadWrite>> {
        let wallet = self.wallet.expect("wallet set by typestate");
        let url = self.rpc_url.parse()?;
        let provider = ProviderBuilder::new().wallet(wallet).connect_http(url);
        let mut client = Glint::<ReadWrite>::from_provider(provider);
        client.gas_limit = self.gas_limit;
        #[cfg(feature = "flight_sql")]
        if let Some(ref flight_url) = self.flight_url {
            client.flight =
                Some(crate::flight_sql::GlintFlightClient::connect(flight_url.as_str()).await?);
        }
        Ok(client)
    }
}

pub type GlintClient = Glint<ReadWrite>;

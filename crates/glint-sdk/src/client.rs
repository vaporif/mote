use std::marker::PhantomData;

use alloy_network::{Ethereum, EthereumWallet};
use alloy_primitives::{B256, U256};
use alloy_provider::{DynProvider, Provider, ProviderBuilder};
use alloy_rlp::Encodable;
use alloy_rpc_types_eth::TransactionReceipt;
use glint_primitives::transaction::GlintTransaction;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use tokio::sync::Mutex;

use glint_primitives::constants::PROCESSOR_ADDRESS;
use glint_primitives::entity::EntityInfo;
use glint_primitives::rpc::{BlockTiming, GlintApiClient};

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
    rpc: HttpClient,
    gas_limit: u64,
    flight: Option<Mutex<crate::flight_sql::GlintFlightClient>>,
    _state: PhantomData<S>,
}

impl Glint<ReadOnly> {
    pub fn connect(rpc_url: &str) -> eyre::Result<Self> {
        let rpc = HttpClientBuilder::default().build(rpc_url)?;
        let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
        Ok(Self::from_parts(provider, rpc))
    }
}

impl Glint<ReadWrite> {
    pub fn connect_with_wallet(rpc_url: &str, wallet: EthereumWallet) -> eyre::Result<Self> {
        let rpc = HttpClientBuilder::default().build(rpc_url)?;
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(rpc_url.parse()?);
        Ok(Self::from_parts(provider, rpc))
    }
}

impl<S: sealed::State> Glint<S> {
    fn from_parts(provider: impl Provider<Ethereum> + 'static, rpc: HttpClient) -> Self {
        Self {
            provider: provider.erased(),
            rpc,
            gas_limit: DEFAULT_GAS_LIMIT,
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
            flight_url: None,
            _state: PhantomData,
        }
    }
}

impl<S: sealed::State> Glint<S> {
    pub async fn get_entity(&self, key: B256) -> eyre::Result<Option<EntityInfo>> {
        Ok(self.rpc.get_entity(key).await?)
    }

    pub async fn get_entity_count(&self) -> eyre::Result<u64> {
        Ok(self.rpc.get_entity_count().await?)
    }

    pub async fn get_used_slots(&self) -> eyre::Result<u64> {
        Ok(self.rpc.get_used_slots().await?)
    }

    pub async fn get_block_timing(&self) -> eyre::Result<BlockTiming> {
        Ok(self.rpc.get_block_timing().await?)
    }

    pub async fn query(&self, sql: &str) -> eyre::Result<Vec<arrow::record_batch::RecordBatch>> {
        let flight = self.flight.as_ref().ok_or_else(|| {
            eyre::eyre!("flight SQL not configured — use builder with .flight_url()")
        })?;
        flight.lock().await.query(sql).await
    }
}

impl Glint<ReadWrite> {
    pub async fn send(&self, tx: &GlintTransaction) -> eyre::Result<TransactionReceipt> {
        tx.validate()?;

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
    flight_url: Option<String>,
    _state: PhantomData<S>,
}

impl<S: sealed::State> GlintBuilder<S> {
    #[must_use]
    pub const fn gas_limit(mut self, limit: u64) -> Self {
        self.gas_limit = limit;
        self
    }

    #[must_use]
    pub fn flight_url(mut self, url: &str) -> Self {
        self.flight_url = Some(url.to_owned());
        self
    }

    async fn finish(
        gas_limit: u64,
        flight_url: Option<String>,
        client: Glint<S>,
    ) -> eyre::Result<Glint<S>> {
        let mut client = client;
        client.gas_limit = gas_limit;
        if let Some(ref flight_url) = flight_url {
            client.flight = Some(Mutex::new(
                crate::flight_sql::GlintFlightClient::connect(flight_url.as_str()).await?,
            ));
        }
        Ok(client)
    }
}

impl GlintBuilder<ReadOnly> {
    #[must_use]
    pub fn wallet(self, wallet: EthereumWallet) -> GlintBuilder<ReadWrite> {
        GlintBuilder {
            rpc_url: self.rpc_url,
            wallet: Some(wallet),
            gas_limit: self.gas_limit,
            flight_url: self.flight_url,
            _state: PhantomData,
        }
    }

    pub async fn build(self) -> eyre::Result<Glint<ReadOnly>> {
        let rpc = HttpClientBuilder::default().build(&self.rpc_url)?;
        let url = self.rpc_url.parse()?;
        let provider = ProviderBuilder::new().connect_http(url);
        Self::finish(
            self.gas_limit,
            self.flight_url,
            Glint::from_parts(provider, rpc),
        )
        .await
    }
}

impl GlintBuilder<ReadWrite> {
    pub async fn build(self) -> eyre::Result<Glint<ReadWrite>> {
        let wallet = self.wallet.expect("wallet set by typestate");
        let rpc = HttpClientBuilder::default().build(&self.rpc_url)?;
        let url = self.rpc_url.parse()?;
        let provider = ProviderBuilder::new().wallet(wallet).connect_http(url);
        Self::finish(
            self.gas_limit,
            self.flight_url,
            Glint::from_parts(provider, rpc),
        )
        .await
    }
}

mod crud;
pub mod decode;

use crate::expiration::ExpirationIndex;

use alloy_consensus::{Transaction, TransactionEnvelope};
use alloy_eips::Encodable2718 as _;
use alloy_evm::{
    Database, EvmFactory, RecoveredTx as _,
    block::{BlockExecutionResult, BlockExecutorFactory, BlockExecutorFor, ExecutableTx, TxResult},
    eth::{
        EthBlockExecutionCtx, EthBlockExecutor, EthBlockExecutorFactory, EthTxResult,
        receipt_builder::ReceiptBuilder, spec::EthExecutorSpec,
    },
    precompiles::PrecompilesMap,
};
use alloy_primitives::{B256, Log, U256};
use mote_primitives::{
    config::MoteChainConfig,
    constants::PROCESSOR_ADDRESS,
    entity::EntityMetadata,
    storage::{entity_content_hash_key, entity_storage_key},
};
use reth_evm::{
    ConfigureEngineEvm, ConfigureEvm, Evm, EvmEnvFor, ExecutableTxIterator, ExecutionCtxFor,
    FromRecoveredTx, FromTxWithEncoded, OnStateHook,
    block::{BlockExecutionError, BlockExecutor, InternalBlockExecutionError},
};
use reth_primitives_traits::{BlockTy, HeaderTy, SealedBlock, SealedHeader};
use revm::{
    DatabaseCommit, Inspector,
    context::result::{ExecutionResult, ResultAndState},
    database::State,
    state::{Account, AccountInfo, AccountStatus, EvmStorageSlot},
};
use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

pub use decode::{DecodedMoteTransaction, decode_with_raw_slices};

pub type SharedExpirationIndex = Arc<Mutex<ExpirationIndex>>;

pub trait MoteTransaction:
    Transaction
    + alloy_eips::Encodable2718
    + TransactionEnvelope<TxType: Default + Clone + Send + Sync + 'static>
{
}
impl<T> MoteTransaction for T where
    T: Transaction
        + alloy_eips::Encodable2718
        + TransactionEnvelope<TxType: Default + Clone + Send + Sync + 'static>
{
}

pub trait MoteResultBuilder: Send + Sync + 'static {
    type HaltReason: Send + Sync + 'static;
    type TxType: Default + Clone + Send + Sync + 'static;
    type Result: TxResult<HaltReason = Self::HaltReason>;

    fn build_crud_result(
        result: ResultAndState<Self::HaltReason>,
        tx_type: Self::TxType,
    ) -> Self::Result;
}

pub struct EthMoteResultBuilder<H, T>(PhantomData<(H, T)>);

impl<H: Send + Sync + 'static, T: Default + Clone + Send + Sync + 'static> MoteResultBuilder
    for EthMoteResultBuilder<H, T>
{
    type HaltReason = H;
    type TxType = T;
    type Result = EthTxResult<H, T>;

    fn build_crud_result(result: ResultAndState<H>, tx_type: T) -> EthTxResult<H, T> {
        EthTxResult {
            result,
            blob_gas_used: 0,
            tx_type,
        }
    }
}

pub struct MoteEvmConfig<Inner: ConfigureEvm> {
    inner: Inner,
    factory: MoteBlockExecutorFactory<Inner::BlockExecutorFactory>,
}

impl<Inner: ConfigureEvm> fmt::Debug for MoteEvmConfig<Inner>
where
    Inner::BlockExecutorFactory: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MoteEvmConfig")
            .field("inner", &self.inner)
            .field("factory", &self.factory)
            .finish()
    }
}

impl<Inner: ConfigureEvm> Clone for MoteEvmConfig<Inner>
where
    Inner::BlockExecutorFactory: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            factory: self.factory.clone(),
        }
    }
}

impl<Inner: ConfigureEvm> MoteEvmConfig<Inner>
where
    Inner::BlockExecutorFactory: Clone + fmt::Debug + Send + Sync + Unpin,
{
    pub fn new(
        inner: Inner,
        config: MoteChainConfig,
        expiration_index: SharedExpirationIndex,
    ) -> Self {
        let inner_factory = inner.block_executor_factory().clone();
        let factory = MoteBlockExecutorFactory {
            inner: inner_factory,
            expiration_index,
            config,
        };
        Self { inner, factory }
    }
}

#[derive(Debug, Clone)]
pub struct MoteBlockExecutorFactory<F> {
    inner: F,
    expiration_index: SharedExpirationIndex,
    config: MoteChainConfig,
}

impl<R, Spec, EvmF> BlockExecutorFactory
    for MoteBlockExecutorFactory<EthBlockExecutorFactory<R, Spec, EvmF>>
where
    R: ReceiptBuilder<Transaction: MoteTransaction, Receipt: alloy_consensus::TxReceipt<Log = Log>>
        + 'static,
    Spec: EthExecutorSpec + 'static,
    EvmF: EvmFactory<
            Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
            Precompiles = PrecompilesMap,
        > + 'static,
{
    type EvmFactory = EvmF;
    type ExecutionCtx<'a> = EthBlockExecutionCtx<'a>;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EvmF::Evm<&'a mut State<DB>, I>,
        ctx: EthBlockExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<EvmF::Context<&'a mut State<DB>>> + 'a,
    {
        let inner =
            EthBlockExecutor::new(evm, ctx, self.inner.spec(), self.inner.receipt_builder());
        MoteBlockExecutor::<_, EthMoteResultBuilder<_, _>> {
            inner,
            _rb: PhantomData,
            expiration_index: self.expiration_index.clone(),
            config: self.config.clone(),
            pending_logs: Vec::new(),
        }
    }
}

#[cfg(feature = "op")]
pub struct OpMoteResultBuilder<H, T>(PhantomData<(H, T)>);

#[cfg(feature = "op")]
impl<H: Send + Sync + 'static, T: Default + Clone + Send + Sync + 'static> MoteResultBuilder
    for OpMoteResultBuilder<H, T>
{
    type HaltReason = H;
    type TxType = T;
    type Result = alloy_op_evm::block::OpTxResult<H, T>;

    fn build_crud_result(
        result: ResultAndState<H>,
        tx_type: T,
    ) -> alloy_op_evm::block::OpTxResult<H, T> {
        alloy_op_evm::block::OpTxResult {
            inner: EthTxResult {
                result,
                blob_gas_used: 0,
                tx_type,
            },
            // CRUD results are system-processed; the real sender is in entity metadata
            is_deposit: false,
            sender: alloy_primitives::Address::ZERO,
        }
    }
}

#[cfg(feature = "op")]
impl<R, Spec, EvmF> BlockExecutorFactory
    for MoteBlockExecutorFactory<reth_optimism_evm::OpBlockExecutorFactory<R, Spec, EvmF>>
where
    R: alloy_op_evm::block::receipt_builder::OpReceiptBuilder<
            Transaction: MoteTransaction,
            Receipt: alloy_consensus::TxReceipt<Log = Log>,
        > + 'static,
    Spec: alloy_op_hardforks::OpHardforks + 'static,
    EvmF: EvmFactory<
            Tx: FromRecoveredTx<R::Transaction>
                    + FromTxWithEncoded<R::Transaction>
                    + alloy_op_evm::block::OpTxEnv,
            Precompiles = PrecompilesMap,
        > + 'static,
{
    type EvmFactory = EvmF;
    type ExecutionCtx<'a> = reth_optimism_evm::OpBlockExecutionCtx;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EvmF::Evm<&'a mut State<DB>, I>,
        ctx: reth_optimism_evm::OpBlockExecutionCtx,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<EvmF::Context<&'a mut State<DB>>> + 'a,
    {
        let inner = alloy_op_evm::block::OpBlockExecutor::new(
            evm,
            ctx,
            self.inner.spec(),
            self.inner.receipt_builder(),
        );
        MoteBlockExecutor::<_, OpMoteResultBuilder<_, _>> {
            inner,
            _rb: PhantomData,
            expiration_index: self.expiration_index.clone(),
            config: self.config.clone(),
            pending_logs: Vec::new(),
        }
    }
}

impl<Inner: ConfigureEvm> ConfigureEvm for MoteEvmConfig<Inner>
where
    Inner::BlockExecutorFactory: Clone + fmt::Debug + Send + Sync + Unpin,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction: MoteTransaction,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt:
        alloy_consensus::TxReceipt<Log = Log>,
    reth_primitives_traits::TxTy<Inner::Primitives>: MoteTransaction,
    MoteBlockExecutorFactory<Inner::BlockExecutorFactory>: for<'a> BlockExecutorFactory<
            EvmFactory = <Inner::BlockExecutorFactory as BlockExecutorFactory>::EvmFactory,
            ExecutionCtx<'a> = <Inner::BlockExecutorFactory as BlockExecutorFactory>::ExecutionCtx<
                'a,
            >,
            Transaction = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction,
            Receipt = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt,
        >,
    Inner::BlockAssembler: reth_evm::execute::BlockAssembler<
            MoteBlockExecutorFactory<Inner::BlockExecutorFactory>,
            Block = BlockTy<Inner::Primitives>,
        >,
{
    type Primitives = Inner::Primitives;
    type Error = Inner::Error;
    type NextBlockEnvCtx = Inner::NextBlockEnvCtx;
    type BlockExecutorFactory = MoteBlockExecutorFactory<Inner::BlockExecutorFactory>;
    type BlockAssembler = Inner::BlockAssembler;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        self.inner.block_assembler()
    }

    fn evm_env(&self, header: &HeaderTy<Self::Primitives>) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env(header)
    }

    fn next_evm_env(
        &self,
        parent: &HeaderTy<Self::Primitives>,
        attributes: &Self::NextBlockEnvCtx,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.next_evm_env(parent, attributes)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<BlockTy<Self::Primitives>>,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.inner.context_for_block(block)
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader<HeaderTy<Self::Primitives>>,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<ExecutionCtxFor<'_, Self>, Self::Error> {
        self.inner.context_for_next_block(parent, attributes)
    }
}

impl<Inner, D> ConfigureEngineEvm<D> for MoteEvmConfig<Inner>
where
    Inner: ConfigureEngineEvm<D>,
    Inner::BlockExecutorFactory: Clone + fmt::Debug + Send + Sync + Unpin,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction: MoteTransaction,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt:
        alloy_consensus::TxReceipt<Log = Log>,
    reth_primitives_traits::TxTy<Inner::Primitives>: MoteTransaction,
    MoteBlockExecutorFactory<Inner::BlockExecutorFactory>: for<'a> BlockExecutorFactory<
            EvmFactory = <Inner::BlockExecutorFactory as BlockExecutorFactory>::EvmFactory,
            ExecutionCtx<'a> = <Inner::BlockExecutorFactory as BlockExecutorFactory>::ExecutionCtx<
                'a,
            >,
            Transaction = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction,
            Receipt = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt,
        >,
    Inner::BlockAssembler: reth_evm::execute::BlockAssembler<
            MoteBlockExecutorFactory<Inner::BlockExecutorFactory>,
            Block = BlockTy<Inner::Primitives>,
        >,
{
    fn evm_env_for_payload(&self, payload: &D) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env_for_payload(payload)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a D,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.inner.context_for_payload(payload)
    }

    fn tx_iterator_for_payload(
        &self,
        payload: &D,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        self.inner.tx_iterator_for_payload(payload)
    }
}

pub struct MoteBlockExecutor<InnerExec, RB> {
    inner: InnerExec,
    _rb: PhantomData<RB>,
    expiration_index: SharedExpirationIndex,
    config: MoteChainConfig,
    pending_logs: Vec<Log>,
}

const MOTE_GAS_PER_CREATE: u64 = 50_000;
const MOTE_GAS_PER_UPDATE: u64 = 40_000;
const MOTE_GAS_PER_DELETE: u64 = 10_000;
const MOTE_GAS_PER_EXTEND: u64 = 10_000;

const INTRINSIC_GAS: u64 = 21_000;
const GAS_PER_DATA_BYTE: u64 = revm::context_interface::cfg::gas::NON_ZERO_BYTE_DATA_COST_ISTANBUL;
const GAS_PER_BTL_BLOCK: u64 = 10;

impl<InnerExec, RB> BlockExecutor for MoteBlockExecutor<InnerExec, RB>
where
    InnerExec:
        BlockExecutor<Transaction: MoteTransaction, Receipt: alloy_consensus::TxReceipt<Log = Log>>,
    InnerExec::Evm: Evm<DB: revm::Database<Error: core::fmt::Display> + DatabaseCommit>,
    RB: MoteResultBuilder<
            Result = InnerExec::Result,
            TxType = <InnerExec::Transaction as TransactionEnvelope>::TxType,
        >,
{
    type Transaction = InnerExec::Transaction;
    type Receipt = InnerExec::Receipt;
    type Evm = InnerExec::Evm;
    type Result = InnerExec::Result;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        self.inner.apply_pre_execution_changes()?;
        self.run_expiration_housekeeping()
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, recovered) = tx.into_parts();

        let tx_ref = recovered.tx();
        if !matches!(tx_ref.to(), Some(addr) if addr == PROCESSOR_ADDRESS) {
            return self
                .inner
                .execute_transaction_without_commit((tx_env, recovered));
        }

        let sender = *recovered.signer();
        let calldata = tx_ref.input();
        let gas_limit = tx_ref.gas_limit();
        let tx_type = tx_ref.tx_type();
        let tx_hash = recovered.tx().trie_hash();

        let staged = self.execute_mote_crud(calldata, sender, tx_hash)?;

        let intrinsic_gas = INTRINSIC_GAS + calldata.len() as u64 * GAS_PER_DATA_BYTE;
        let total_gas = intrinsic_gas.saturating_add(staged.gas_used);

        if gas_limit < total_gas {
            let result = ResultAndState {
                result: ExecutionResult::Revert {
                    gas_used: gas_limit,
                    output: alloy_primitives::Bytes::from_static(
                        b"insufficient gas for mote operations",
                    ),
                },
                state: HashMap::default(),
            };
            return Ok(RB::build_crud_result(result, tx_type));
        }

        let logs = self.commit_crud(staged)?;

        let result = ResultAndState {
            result: ExecutionResult::Success {
                reason: revm::context::result::SuccessReason::Stop,
                gas_used: total_gas,
                gas_refunded: 0,
                logs,
                output: revm::context::result::Output::Call(alloy_primitives::Bytes::new()),
            },
            state: HashMap::default(),
        };

        Ok(RB::build_crud_result(result, tx_type))
    }

    fn commit_transaction(&mut self, output: Self::Result) -> Result<u64, BlockExecutionError> {
        self.inner.commit_transaction(output)
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<InnerExec::Receipt>), BlockExecutionError> {
        if !self.pending_logs.is_empty() {
            let logs = std::mem::take(&mut self.pending_logs);
            let result = ResultAndState {
                result: ExecutionResult::Success {
                    reason: revm::context::result::SuccessReason::Stop,
                    gas_used: 0,
                    gas_refunded: 0,
                    logs,
                    output: revm::context::result::Output::Call(alloy_primitives::Bytes::new()),
                },
                state: HashMap::default(),
            };
            self.inner
                .commit_transaction(RB::build_crud_result(result, Default::default()))?;
        }
        self.inner.finish()
    }

    fn set_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.inner.set_state_hook(hook);
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        self.inner.evm_mut()
    }

    fn evm(&self) -> &Self::Evm {
        self.inner.evm()
    }

    fn receipts(&self) -> &[Self::Receipt] {
        self.inner.receipts()
    }
}

impl<InnerExec, RB> MoteBlockExecutor<InnerExec, RB>
where
    InnerExec:
        BlockExecutor<Transaction: MoteTransaction, Receipt: alloy_consensus::TxReceipt<Log = Log>>,
    InnerExec::Evm: Evm<DB: revm::Database<Error: core::fmt::Display> + DatabaseCommit>,
    RB: MoteResultBuilder<
            Result = InnerExec::Result,
            TxType = <InnerExec::Transaction as TransactionEnvelope>::TxType,
        >,
{
    fn read_entity_metadata(
        &mut self,
        entity_key: &B256,
    ) -> Result<EntityMetadata, BlockExecutionError> {
        use revm::Database as _;

        let meta_slot = entity_storage_key(entity_key);
        let slot_u256 = U256::from_be_bytes(meta_slot.0);

        let value = self
            .inner
            .evm_mut()
            .db_mut()
            .storage(PROCESSOR_ADDRESS, slot_u256)
            .map_err(|e| mote_err(format!("storage read: {e}")))?;

        if value == U256::ZERO {
            return Err(mote_err(format!("entity not found: {entity_key}")));
        }

        let bytes = value.to_be_bytes();
        Ok(EntityMetadata::decode(&bytes))
    }

    fn run_expiration_housekeeping(&mut self) -> Result<(), BlockExecutionError> {
        use alloy_evm::revm::context::Block as _;
        use mote_primitives::events::EntityExpired;
        use revm::Database as _;

        let current_block: u64 = self.inner.evm().block().number().saturating_to();

        let mut exp_idx = self
            .expiration_index
            .lock()
            .map_err(|e| mote_err(format!("expiration index lock: {e}")))?;

        if let Some(last) = exp_idx.last_drained_block()
            && current_block <= last
        {
            exp_idx.clear_range(current_block..=last);
            exp_idx.reset_last_drained();
        }

        let expired_keys = exp_idx.drain_block(current_block);
        drop(exp_idx);

        if expired_keys.is_empty() {
            return Ok(());
        }

        let mut state_changes: HashMap<B256, U256> = HashMap::new();

        for entity_key in &expired_keys {
            let meta_slot = entity_storage_key(entity_key);

            let value = self
                .inner
                .evm_mut()
                .db_mut()
                .storage(PROCESSOR_ADDRESS, U256::from_be_bytes(meta_slot.0))
                .map_err(|e| mote_err(format!("storage read during expiration: {e}")))?;

            if value == U256::ZERO {
                continue;
            }

            let bytes = value.to_be_bytes();
            let meta = EntityMetadata::decode(&bytes);

            if meta.expires_at_block != current_block {
                continue;
            }

            self.pending_logs.push(EntityExpired::new_log(
                PROCESSOR_ADDRESS,
                *entity_key,
                meta.owner,
            ));

            let content_slot = entity_content_hash_key(entity_key);
            state_changes.insert(meta_slot, U256::ZERO);
            state_changes.insert(content_slot, U256::ZERO);
        }

        if !state_changes.is_empty() {
            let expired_slots =
                (state_changes.len() as u64 / 2) * crate::slot_counter::SLOTS_PER_ENTITY;

            commit_storage_changes(self.inner.evm_mut(), &state_changes);
            update_slot_counter(self.inner.evm_mut(), -(expired_slots.cast_signed()))?;
        }

        Ok(())
    }
}

fn update_slot_counter<E: Evm<DB: DatabaseCommit + revm::Database<Error: core::fmt::Display>>>(
    evm: &mut E,
    delta: i64,
) -> Result<(), BlockExecutionError> {
    use crate::slot_counter::used_slots_key;
    use revm::Database as _;

    if delta == 0 {
        return Ok(());
    }

    let counter_slot = used_slots_key();
    let current = evm
        .db_mut()
        .storage(PROCESSOR_ADDRESS, U256::from_be_bytes(counter_slot.0))
        .map_err(|e| mote_err(format!("counter read: {e}")))?;

    let new_value = if delta > 0 {
        current.saturating_add(U256::from(delta.cast_unsigned()))
    } else {
        current.saturating_sub(U256::from((-delta).cast_unsigned()))
    };

    commit_storage_changes(evm, &HashMap::from([(counter_slot, new_value)]));
    Ok(())
}

fn commit_storage_changes<E: Evm<DB: DatabaseCommit>>(evm: &mut E, changes: &HashMap<B256, U256>) {
    let mut storage = revm::state::EvmStorage::default();
    for (&slot, &value) in changes {
        storage.insert(
            U256::from_be_bytes(slot.0),
            EvmStorageSlot::new_changed(U256::ZERO, value, 0),
        );
    }

    let account = Account {
        info: AccountInfo::default(),
        original_info: Box::default(),
        transaction_id: 0,
        storage,
        status: AccountStatus::Touched,
    };

    evm.db_mut()
        .commit_iter(&mut std::iter::once((PROCESSOR_ADDRESS, account)));
}

fn mote_err(msg: impl Into<Box<dyn core::error::Error + Send + Sync>>) -> BlockExecutionError {
    BlockExecutionError::Internal(InternalBlockExecutionError::Other(msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::TxReceipt;
    use alloy_evm::eth::EthEvmBuilder;
    use alloy_primitives::Address;
    use mote_primitives::entity::EntityMetadata;
    use reth_ethereum::{
        chainspec::MAINNET,
        evm::{EthEvmConfig, primitives::EvmEnv},
    };
    use revm::database::CacheDB;

    const TEST_BLOCK: u64 = 1000;

    fn test_evm_config(expiration_index: ExpirationIndex) -> MoteEvmConfig<EthEvmConfig> {
        MoteEvmConfig::new(
            EthEvmConfig::new(MAINNET.clone()),
            MoteChainConfig::default(),
            Arc::new(Mutex::new(expiration_index)),
        )
    }

    fn seed_entity(
        db: &mut CacheDB<revm::database::EmptyDB>,
        entity_key: &B256,
        owner: Address,
        expires_at: u64,
    ) {
        let meta = EntityMetadata {
            owner,
            expires_at_block: expires_at,
        };
        let meta_slot = entity_storage_key(entity_key);
        let content_slot = entity_content_hash_key(entity_key);

        let account = db
            .cache
            .accounts
            .entry(PROCESSOR_ADDRESS)
            .or_insert_with(|| revm::database::DbAccount {
                info: AccountInfo::default(),
                ..Default::default()
            });
        account.storage.insert(
            U256::from_be_bytes(meta_slot.0),
            U256::from_be_bytes(meta.encode()),
        );
        account
            .storage
            .insert(U256::from_be_bytes(content_slot.0), U256::from(0xDEADu64));
    }

    #[test]
    fn finish_emits_system_receipt_for_expiration_logs() {
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);

        let mut exp_idx = ExpirationIndex::new();
        exp_idx.insert(TEST_BLOCK, entity_key);
        let config = test_evm_config(exp_idx);

        let mut db = CacheDB::new(revm::database::EmptyDB::default());
        seed_entity(&mut db, &entity_key, owner, TEST_BLOCK);
        let mut state = State::builder()
            .with_database(db)
            .with_bundle_update()
            .build();

        let mut block_env = revm::context::BlockEnv::default();
        block_env.number = U256::from(TEST_BLOCK);

        let env = EvmEnv {
            block_env,
            cfg_env: revm::context::CfgEnv::default(),
        };
        let evm = EthEvmBuilder::new(&mut state, env).build();

        let ctx = EthBlockExecutionCtx {
            parent_hash: B256::ZERO,
            parent_beacon_block_root: None,
            ommers: &[],
            withdrawals: None,
            extra_data: alloy_primitives::Bytes::new(),
            tx_count_hint: None,
        };

        let mut executor = config.create_executor(evm, ctx);

        executor.apply_pre_execution_changes().unwrap();

        let (_evm, result) = executor.finish().unwrap();

        assert_eq!(
            result.receipts.len(),
            1,
            "expected one system receipt for expiration logs"
        );
        let receipt = &result.receipts[0];
        assert!(receipt.status(), "system receipt should be successful");
        assert!(
            !receipt.logs().is_empty(),
            "system receipt should contain EntityExpired logs"
        );
    }

    #[test]
    fn finish_no_receipt_when_no_expirations() {
        let config = test_evm_config(ExpirationIndex::new());

        let mut state = State::builder()
            .with_database(CacheDB::new(revm::database::EmptyDB::default()))
            .with_bundle_update()
            .build();

        let mut block_env = revm::context::BlockEnv::default();
        block_env.number = U256::from(TEST_BLOCK);

        let env = EvmEnv {
            block_env,
            cfg_env: revm::context::CfgEnv::default(),
        };
        let evm = EthEvmBuilder::new(&mut state, env).build();

        let ctx = EthBlockExecutionCtx {
            parent_hash: B256::ZERO,
            parent_beacon_block_root: None,
            ommers: &[],
            withdrawals: None,
            extra_data: alloy_primitives::Bytes::new(),
            tx_count_hint: None,
        };

        let mut executor = config.create_executor(evm, ctx);

        executor.apply_pre_execution_changes().unwrap();

        let (_evm, result) = executor.finish().unwrap();

        assert!(
            result.receipts.is_empty(),
            "no receipts expected when nothing expires"
        );
    }

    #[cfg(feature = "op")]
    #[test]
    fn op_mote_evm_config_constructs() {
        use reth_optimism_evm::OpEvmConfig;

        let chain_spec = reth_optimism_chainspec::BASE_MAINNET.clone();
        let inner = OpEvmConfig::optimism(chain_spec);
        let _config: MoteEvmConfig<OpEvmConfig> = MoteEvmConfig::new(
            inner,
            MoteChainConfig::default(),
            Arc::new(Mutex::new(ExpirationIndex::new())),
        );
    }
}

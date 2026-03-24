mod crud;
pub mod decode;

use crate::expiration::ExpirationIndex;

use alloy_consensus::Transaction;
use alloy_evm::{
    Database, EthEvm, EthEvmFactory, FromRecoveredTx, FromTxWithEncoded, RecoveredTx,
    block::{BlockExecutionResult, BlockExecutorFactory, BlockExecutorFor, ExecutableTx},
    eth::{EthBlockExecutionCtx, EthBlockExecutor, EthTxResult},
    precompiles::PrecompilesMap,
};
use alloy_primitives::{B256, Log, U256};
use mote_primitives::{
    constants::PROCESSOR_ADDRESS,
    entity::EntityMetadata,
    storage::{entity_content_hash_key, entity_storage_key},
};
use reth_ethereum::network::types::Encodable2718;
use reth_ethereum::{
    Block, EthPrimitives, Receipt, TransactionSigned, TxType,
    chainspec::ChainSpec,
    evm::{
        EthBlockAssembler, EthEvmConfig, RethReceiptBuilder,
        primitives::{
            Evm, EvmEnv, EvmEnvFor, ExecutionCtxFor, NextBlockEnvAttributes, OnStateHook,
            execute::{BlockExecutionError, BlockExecutor, InternalBlockExecutionError},
        },
    },
    node::api::{ConfigureEngineEvm, ConfigureEvm, ExecutableTxIterator, FullNodeTypes, NodeTypes},
    node::builder::{BuilderContext, components::ExecutorBuilder},
    primitives::{Header, SealedBlock, SealedHeader},
    rpc::types::engine::ExecutionData,
};
use revm::{
    DatabaseCommit, Inspector,
    context::result::{ExecutionResult, ResultAndState},
    database::State,
    state::{Account, AccountInfo, AccountStatus, EvmStorageSlot},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub use decode::{DecodedMoteTransaction, decode_with_raw_slices};

pub type SharedExpirationIndex = Arc<Mutex<ExpirationIndex>>;

#[derive(Debug, Clone)]
pub struct MoteExecutorBuilder {
    expiration_index: SharedExpirationIndex,
}

impl MoteExecutorBuilder {
    pub const fn new(expiration_index: SharedExpirationIndex) -> Self {
        Self { expiration_index }
    }
}

impl<Types, Node> ExecutorBuilder<Node> for MoteExecutorBuilder
where
    Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>,
    Node: FullNodeTypes<Types = Types>,
{
    type EVM = MoteEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> color_eyre::Result<Self::EVM> {
        Ok(MoteEvmConfig {
            inner: EthEvmConfig::new(ctx.chain_spec()),
            expiration_index: self.expiration_index,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MoteEvmConfig {
    inner: EthEvmConfig,
    expiration_index: SharedExpirationIndex,
}

impl BlockExecutorFactory for MoteEvmConfig {
    type EvmFactory = EthEvmFactory;
    type ExecutionCtx<'a> = EthBlockExecutionCtx<'a>;
    type Transaction = TransactionSigned;
    type Receipt = Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.executor_factory.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EthEvm<&'a mut State<DB>, I, PrecompilesMap>,
        ctx: EthBlockExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<<EthEvmFactory as alloy_evm::EvmFactory>::Context<&'a mut State<DB>>> + 'a,
    {
        MoteBlockExecutor {
            inner: EthBlockExecutor::new(
                evm,
                ctx,
                self.inner.executor_factory.spec(),
                self.inner.executor_factory.receipt_builder(),
            ),
            expiration_index: self.expiration_index.clone(),
            pending_logs: Vec::new(),
        }
    }
}

impl ConfigureEvm for MoteEvmConfig {
    type Primitives = <EthEvmConfig as ConfigureEvm>::Primitives;
    type Error = <EthEvmConfig as ConfigureEvm>::Error;
    type NextBlockEnvCtx = <EthEvmConfig as ConfigureEvm>::NextBlockEnvCtx;
    type BlockExecutorFactory = Self;
    type BlockAssembler = EthBlockAssembler<ChainSpec>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        self
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        self.inner.block_assembler()
    }

    fn evm_env(
        &self,
        header: &Header,
    ) -> Result<EvmEnv<revm::primitives::hardfork::SpecId>, Self::Error> {
        self.inner.evm_env(header)
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnv<revm::primitives::hardfork::SpecId>, Self::Error> {
        self.inner.next_evm_env(parent, attributes)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<EthBlockExecutionCtx<'a>, Self::Error> {
        self.inner.context_for_block(block)
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<EthBlockExecutionCtx<'_>, Self::Error> {
        self.inner.context_for_next_block(parent, attributes)
    }
}

impl ConfigureEngineEvm<ExecutionData> for MoteEvmConfig {
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.inner.evm_env_for_payload(payload)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.inner.context_for_payload(payload)
    }

    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        self.inner.tx_iterator_for_payload(payload)
    }
}

pub struct MoteBlockExecutor<'a, Evm> {
    inner: EthBlockExecutor<'a, Evm, &'a Arc<ChainSpec>, &'a RethReceiptBuilder>,
    expiration_index: SharedExpirationIndex,
    pending_logs: Vec<Log>,
}

const MOTE_GAS_PER_CREATE: u64 = 50_000;
const MOTE_GAS_PER_UPDATE: u64 = 40_000;
const MOTE_GAS_PER_DELETE: u64 = 10_000;
const MOTE_GAS_PER_EXTEND: u64 = 10_000;

const INTRINSIC_GAS: u64 = 21_000;
const GAS_PER_DATA_BYTE: u64 = revm::context_interface::cfg::gas::NON_ZERO_BYTE_DATA_COST_ISTANBUL;
const GAS_PER_BTL_BLOCK: u64 = 10;

impl<'db, DB, E> BlockExecutor for MoteBlockExecutor<'_, E>
where
    DB: Database + 'db,
    E: Evm<
            DB = &'db mut State<DB>,
            Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>,
        >,
{
    type Transaction = TransactionSigned;
    type Receipt = Receipt;
    type Evm = E;
    type Result = EthTxResult<E::HaltReason, TxType>;

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
            let mut result = self
                .inner
                .execute_transaction_without_commit((tx_env, recovered))?;

            if !self.pending_logs.is_empty() {
                let expiration_logs = std::mem::take(&mut self.pending_logs);
                match &mut result.result.result {
                    ExecutionResult::Success { logs, .. } => {
                        let mut all = expiration_logs;
                        all.append(logs);
                        *logs = all;
                    }
                    ExecutionResult::Revert { .. } | ExecutionResult::Halt { .. } => {
                        self.pending_logs = expiration_logs;
                    }
                }
            }

            return Ok(result);
        }

        let sender = *recovered.signer();
        let calldata = tx_ref.input();
        let gas_limit = tx_ref.gas_limit();
        let tx_type = tx_ref.tx_type();
        let tx_hash = tx_ref.trie_hash();

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
            return Ok(EthTxResult {
                result,
                blob_gas_used: 0,
                tx_type,
            });
        }

        let mut logs = self.commit_crud(staged)?;
        if !self.pending_logs.is_empty() {
            let mut all_logs = std::mem::take(&mut self.pending_logs);
            all_logs.append(&mut logs);
            logs = all_logs;
        }

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

        Ok(EthTxResult {
            result,
            blob_gas_used: 0,
            tx_type,
        })
    }

    fn commit_transaction(&mut self, output: Self::Result) -> Result<u64, BlockExecutionError> {
        self.inner.commit_transaction(output)
    }

    fn finish(self) -> Result<(Self::Evm, BlockExecutionResult<Receipt>), BlockExecutionError> {
        // Empty blocks drop expiration logs - BlockExecutionResult has no logs field.
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

impl<'db, DB, E> MoteBlockExecutor<'_, E>
where
    DB: Database + 'db,
    E: Evm<
            DB = &'db mut State<DB>,
            Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>,
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
            // Each entity produces 2 entries (meta_slot + content_slot)
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

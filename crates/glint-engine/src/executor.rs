mod crud;
pub mod decode;
mod eth;
#[cfg(feature = "op")]
mod op;

use crate::expiration::ExpirationIndex;

use alloy_consensus::{Transaction, TransactionEnvelope};
use alloy_eips::Encodable2718 as _;
use alloy_evm::{
    RecoveredTx as _,
    block::{BlockExecutionResult, BlockExecutorFactory, ExecutableTx, TxResult},
};
use alloy_primitives::{B256, Log, U256};
use glint_primitives::{
    config::GlintChainConfig,
    constants::PROCESSOR_ADDRESS,
    entity::EntityMetadata,
    storage::{entity_content_hash_key, entity_operator_key, entity_storage_key},
};
use parking_lot::Mutex;
use reth_evm::{
    ConfigureEngineEvm, ConfigureEvm, Evm, EvmEnvFor, ExecutableTxIterator, ExecutionCtxFor,
    OnStateHook,
    block::{BlockExecutionError, BlockExecutor, InternalBlockExecutionError},
};
use reth_primitives_traits::{BlockTy, HeaderTy, SealedBlock, SealedHeader};
use revm::{
    DatabaseCommit,
    context::result::{ExecutionResult, ResultAndState},
    state::{Account, AccountInfo, AccountStatus, EvmStorageSlot},
};
use std::{collections::HashMap, fmt, marker::PhantomData, sync::Arc};
use tracing::{debug, info, instrument, warn};

pub use decode::{DecodedGlintTransaction, decode_with_raw_slices};
pub use eth::EthGlintResultBuilder;
#[cfg(feature = "op")]
pub use op::OpGlintResultBuilder;

pub type SharedExpirationIndex = Arc<Mutex<ExpirationIndex>>;

pub trait GlintTransaction:
    Transaction
    + alloy_eips::Encodable2718
    + TransactionEnvelope<TxType: Default + Clone + Send + Sync + 'static>
{
}
impl<T> GlintTransaction for T where
    T: Transaction
        + alloy_eips::Encodable2718
        + TransactionEnvelope<TxType: Default + Clone + Send + Sync + 'static>
{
}

pub trait GlintResultBuilder: Send + Sync + 'static {
    type HaltReason: Send + Sync + 'static;
    type TxType: Default + Clone + Send + Sync + 'static;
    type Result: TxResult<HaltReason = Self::HaltReason>;

    fn build_crud_result(
        result: ResultAndState<Self::HaltReason>,
        tx_type: Self::TxType,
        sender: alloy_primitives::Address,
    ) -> Self::Result;
}

#[derive(Debug, Clone)]
pub struct GlintEvmConfig<Inner: ConfigureEvm> {
    inner: Inner,
    factory: GlintBlockExecutorFactory<Inner::BlockExecutorFactory>,
}

impl<Inner: ConfigureEvm> GlintEvmConfig<Inner>
where
    Inner::BlockExecutorFactory: Clone + fmt::Debug + Send + Sync + Unpin,
{
    pub fn new(
        inner: Inner,
        config: GlintChainConfig,
        expiration_index: SharedExpirationIndex,
    ) -> Self {
        let inner_factory = inner.block_executor_factory().clone();
        let factory = GlintBlockExecutorFactory {
            inner: inner_factory,
            expiration_index,
            config: Arc::new(config),
        };
        Self { inner, factory }
    }
}

#[derive(Debug, Clone)]
pub struct GlintBlockExecutorFactory<F> {
    inner: F,
    expiration_index: SharedExpirationIndex,
    config: Arc<GlintChainConfig>,
}

impl<Inner: ConfigureEvm> ConfigureEvm for GlintEvmConfig<Inner>
where
    Inner::BlockExecutorFactory: Clone + fmt::Debug + Send + Sync + Unpin,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction: GlintTransaction,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt:
        alloy_consensus::TxReceipt<Log = Log>,
    reth_primitives_traits::TxTy<Inner::Primitives>: GlintTransaction,
    GlintBlockExecutorFactory<Inner::BlockExecutorFactory>: for<'a> BlockExecutorFactory<
            EvmFactory = <Inner::BlockExecutorFactory as BlockExecutorFactory>::EvmFactory,
            ExecutionCtx<'a> = <Inner::BlockExecutorFactory as BlockExecutorFactory>::ExecutionCtx<
                'a,
            >,
            Transaction = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction,
            Receipt = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt,
        >,
    Inner::BlockAssembler: reth_evm::execute::BlockAssembler<
            GlintBlockExecutorFactory<Inner::BlockExecutorFactory>,
            Block = BlockTy<Inner::Primitives>,
        >,
{
    type Primitives = Inner::Primitives;
    type Error = Inner::Error;
    type NextBlockEnvCtx = Inner::NextBlockEnvCtx;
    type BlockExecutorFactory = GlintBlockExecutorFactory<Inner::BlockExecutorFactory>;
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

impl<Inner, D> ConfigureEngineEvm<D> for GlintEvmConfig<Inner>
where
    Inner: ConfigureEngineEvm<D>,
    Inner::BlockExecutorFactory: Clone + fmt::Debug + Send + Sync + Unpin,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction: GlintTransaction,
    <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt:
        alloy_consensus::TxReceipt<Log = Log>,
    reth_primitives_traits::TxTy<Inner::Primitives>: GlintTransaction,
    GlintBlockExecutorFactory<Inner::BlockExecutorFactory>: for<'a> BlockExecutorFactory<
            EvmFactory = <Inner::BlockExecutorFactory as BlockExecutorFactory>::EvmFactory,
            ExecutionCtx<'a> = <Inner::BlockExecutorFactory as BlockExecutorFactory>::ExecutionCtx<
                'a,
            >,
            Transaction = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Transaction,
            Receipt = <Inner::BlockExecutorFactory as BlockExecutorFactory>::Receipt,
        >,
    Inner::BlockAssembler: reth_evm::execute::BlockAssembler<
            GlintBlockExecutorFactory<Inner::BlockExecutorFactory>,
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

pub struct GlintBlockExecutor<InnerExec, RB> {
    inner: InnerExec,
    expiration_index: SharedExpirationIndex,
    config: Arc<GlintChainConfig>,
    pending_logs: Vec<Log>,
    pending_state: revm::state::EvmState,
    _marker: PhantomData<RB>,
}

const GLINT_GAS_PER_CREATE: u64 = 50_000;
const GLINT_GAS_PER_UPDATE: u64 = 40_000;
const GLINT_GAS_PER_DELETE: u64 = 10_000;
const GLINT_GAS_PER_EXTEND: u64 = 10_000;
const GLINT_GAS_PER_CHANGE_OWNER: u64 = 10_000;
const GAS_OPERATOR_WRITE: u64 = 20_000;
const GAS_SLOAD: u64 = 2_100;

const INTRINSIC_GAS: u64 = 21_000;
const GAS_PER_DATA_BYTE: u64 = revm::context_interface::cfg::gas::NON_ZERO_BYTE_DATA_COST_ISTANBUL;
const GAS_PER_BTL_BLOCK: u64 = 10;

impl<InnerExec, RB> BlockExecutor for GlintBlockExecutor<InnerExec, RB>
where
    InnerExec: BlockExecutor<Transaction: GlintTransaction, Receipt: alloy_consensus::TxReceipt<Log = Log>>,
    InnerExec::Evm: Evm<DB: revm::Database<Error: core::fmt::Display> + DatabaseCommit>,
    RB: GlintResultBuilder<
            Result = InnerExec::Result,
            TxType = <InnerExec::Transaction as TransactionEnvelope>::TxType,
        >,
{
    type Transaction = InnerExec::Transaction;
    type Receipt = InnerExec::Receipt;
    type Evm = InnerExec::Evm;
    type Result = InnerExec::Result;

    #[instrument(skip_all, name = "glint::apply_pre_execution_changes")]
    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        self.inner.apply_pre_execution_changes()?;
        self.run_expiration_housekeeping()
    }

    #[instrument(skip_all, name = "glint::execute_tx")]
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

        info!(%sender, %tx_hash, calldata_len = calldata.len(), gas_limit, "intercepted glint tx");

        // We need the sender's current nonce/balance so we can update them below.
        let sender_info = {
            use revm::Database as _;
            self.inner
                .evm_mut()
                .db_mut()
                .basic(sender)
                .map_err(|e| glint_err(format!("sender account read: {e}")))?
                .unwrap_or_default()
        };

        let staged = self.execute_glint_crud(calldata, sender, tx_hash)?;

        let intrinsic_gas = INTRINSIC_GAS + calldata.len() as u64 * GAS_PER_DATA_BYTE;
        let total_gas = intrinsic_gas.saturating_add(staged.gas_used);

        if gas_limit < total_gas {
            warn!(
                gas_limit,
                total_gas, "insufficient gas for glint ops, reverting"
            );
            // Revert still burns gas and bumps the nonce.
            let mut state = HashMap::default();
            let gas_cost = self.compute_gas_cost(gas_limit, tx_ref);
            insert_sender_account(&mut state, sender, &sender_info, gas_cost);
            let result = ResultAndState {
                result: ExecutionResult::Revert {
                    gas_used: gas_limit,
                    output: alloy_primitives::Bytes::from_static(
                        b"insufficient gas for glint operations",
                    ),
                },
                state,
            };
            return Ok(RB::build_crud_result(result, tx_type, sender));
        }

        let log_count = staged.logs.len();
        let (logs, mut state) = self.commit_crud(staged)?;

        // Charge gas and bump nonce — without this a second tx from the same sender can't land.
        let gas_cost = self.compute_gas_cost(total_gas, tx_ref);
        insert_sender_account(&mut state, sender, &sender_info, gas_cost);

        info!(total_gas, log_count, "glint tx executed successfully");

        let result = ResultAndState {
            result: ExecutionResult::Success {
                reason: revm::context::result::SuccessReason::Stop,
                gas_used: total_gas,
                gas_refunded: 0,
                logs,
                output: revm::context::result::Output::Call(alloy_primitives::Bytes::new()),
            },
            state,
        };

        Ok(RB::build_crud_result(result, tx_type, sender))
    }

    fn commit_transaction(&mut self, output: Self::Result) -> Result<u64, BlockExecutionError> {
        self.inner.commit_transaction(output)
    }

    #[instrument(skip_all, name = "glint::finish")]
    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<InnerExec::Receipt>), BlockExecutionError> {
        if !self.pending_logs.is_empty() {
            debug!(
                pending_log_count = self.pending_logs.len(),
                "flushing pending expiration logs as system receipt"
            );
            let logs = std::mem::take(&mut self.pending_logs);
            let state = std::mem::take(&mut self.pending_state);
            let result = ResultAndState {
                result: ExecutionResult::Success {
                    reason: revm::context::result::SuccessReason::Stop,
                    gas_used: 0,
                    gas_refunded: 0,
                    logs,
                    output: revm::context::result::Output::Call(alloy_primitives::Bytes::new()),
                },
                state,
            };
            self.inner.commit_transaction(RB::build_crud_result(
                result,
                Default::default(),
                alloy_primitives::Address::ZERO,
            ))?;
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

impl<InnerExec, RB> GlintBlockExecutor<InnerExec, RB>
where
    InnerExec: BlockExecutor<Transaction: GlintTransaction, Receipt: alloy_consensus::TxReceipt<Log = Log>>,
    InnerExec::Evm: Evm<DB: revm::Database<Error: core::fmt::Display> + DatabaseCommit>,
    RB: GlintResultBuilder<
            Result = InnerExec::Result,
            TxType = <InnerExec::Transaction as TransactionEnvelope>::TxType,
        >,
{
    /// `gas_used * min(max_fee, base_fee + priority_fee)`.
    fn compute_gas_cost<Tx: Transaction>(&self, gas_used: u64, tx: &Tx) -> U256 {
        use alloy_evm::revm::context::Block as _;
        let base_fee: u128 = u128::from(self.inner.evm().block().basefee());

        let max_fee = tx.max_fee_per_gas();
        let priority = tx.max_priority_fee_per_gas().unwrap_or(0);
        let effective_gas_price = max_fee.min(base_fee.saturating_add(priority));
        U256::from(gas_used).saturating_mul(U256::from(effective_gas_price))
    }

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
            .map_err(|e| glint_err(format!("storage read: {e}")))?;

        if value == U256::ZERO {
            return Err(glint_err(format!("entity not found: {entity_key}")));
        }

        let bytes = value.to_be_bytes();
        Ok(EntityMetadata::decode(&bytes))
    }

    #[instrument(skip_all, name = "glint::expiration_housekeeping")]
    fn run_expiration_housekeeping(&mut self) -> Result<(), BlockExecutionError> {
        use alloy_evm::revm::context::Block as _;
        use glint_primitives::events::EntityExpired;
        use revm::Database as _;

        let current_block: u64 = self.inner.evm().block().number().saturating_to();
        debug!(current_block, "running expiration housekeeping");

        let mut exp_idx = self.expiration_index.lock();

        if let Some(last) = exp_idx.last_drained_block()
            && current_block <= last
        {
            // Reorg detected: current_block <= last_drained.
            // Only reset the drain cursor — do NOT clear future expiration entries.
            // Already-drained blocks have empty entries (removed by drain_block).
            // Un-drained blocks contain valid future expirations that must be preserved,
            // otherwise entities become immortal after a reorg.
            exp_idx.reset_last_drained();
        }

        let expired_keys = exp_idx.drain_block(current_block);
        drop(exp_idx);

        if expired_keys.is_empty() {
            return Ok(());
        }
        info!(
            current_block,
            count = expired_keys.len(),
            "expiring entities"
        );

        let mut state_changes: HashMap<B256, U256> = HashMap::new();
        let mut expired_slot_count: u64 = 0;
        let mut expired_entity_count: u64 = 0;

        for entity_key in &expired_keys {
            let meta_slot = entity_storage_key(entity_key);

            let value = self
                .inner
                .evm_mut()
                .db_mut()
                .storage(PROCESSOR_ADDRESS, U256::from_be_bytes(meta_slot.0))
                .map_err(|e| glint_err(format!("storage read during expiration: {e}")))?;

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

            if meta.has_operator {
                let op_slot = entity_operator_key(entity_key);
                state_changes.insert(op_slot, U256::ZERO);
            }

            expired_slot_count += crate::slot_counter::slots_for_entity(meta.has_operator);
            expired_entity_count += 1;
        }

        if !state_changes.is_empty() {
            update_slot_counter(
                self.inner.evm_mut(),
                -(expired_slot_count.cast_signed()),
                &mut state_changes,
            )?;
            update_entity_counter(
                self.inner.evm_mut(),
                -(expired_entity_count.cast_signed()),
                &mut state_changes,
            )?;
            self.pending_state = build_processor_state(&state_changes);
        }

        Ok(())
    }
}

fn update_slot_counter<E: Evm<DB: DatabaseCommit + revm::Database<Error: core::fmt::Display>>>(
    evm: &mut E,
    delta: i64,
    state_changes: &mut HashMap<B256, U256>,
) -> Result<(), BlockExecutionError> {
    use revm::Database as _;

    if delta == 0 {
        return Ok(());
    }

    let counter_slot = crate::slot_counter::USED_SLOTS_KEY;
    let current = evm
        .db_mut()
        .storage(PROCESSOR_ADDRESS, U256::from_be_bytes(counter_slot.0))
        .map_err(|e| glint_err(format!("counter read: {e}")))?;

    let new_value = if delta > 0 {
        current
            .checked_add(U256::from(delta.cast_unsigned()))
            .ok_or_else(|| glint_err("slot counter overflow"))?
    } else {
        current
            .checked_sub(U256::from((-delta).cast_unsigned()))
            .ok_or_else(|| glint_err("slot counter underflow"))?
    };

    state_changes.insert(counter_slot, new_value);
    Ok(())
}

fn update_entity_counter<E: Evm<DB: DatabaseCommit + revm::Database<Error: core::fmt::Display>>>(
    evm: &mut E,
    delta: i64,
    state_changes: &mut HashMap<B256, U256>,
) -> Result<(), BlockExecutionError> {
    use revm::Database as _;

    if delta == 0 {
        return Ok(());
    }

    let counter_slot = crate::slot_counter::ENTITY_COUNT_KEY;
    let current = evm
        .db_mut()
        .storage(PROCESSOR_ADDRESS, U256::from_be_bytes(counter_slot.0))
        .map_err(|e| glint_err(format!("entity counter read: {e}")))?;

    let new_value = if delta > 0 {
        current
            .checked_add(U256::from(delta.cast_unsigned()))
            .ok_or_else(|| glint_err("entity counter overflow"))?
    } else {
        current
            .checked_sub(U256::from((-delta).cast_unsigned()))
            .ok_or_else(|| glint_err("entity counter underflow"))?
    };

    state_changes.insert(counter_slot, new_value);
    Ok(())
}

fn build_processor_state(changes: &HashMap<B256, U256>) -> revm::state::EvmState {
    let mut storage = revm::state::EvmStorage::default();
    for (&slot, &value) in changes {
        // original must differ from present so is_changed() returns true
        // and State::commit doesn't silently drop the slot.
        let original = if value == U256::ZERO {
            U256::from(1)
        } else {
            U256::ZERO
        };
        storage.insert(
            U256::from_be_bytes(slot.0),
            EvmStorageSlot::new_changed(original, value, 0),
        );
    }

    // nonce=1 prevents EIP-161 state clear from treating the account as empty
    // and discarding its storage.
    let info = AccountInfo {
        nonce: 1,
        ..Default::default()
    };

    let account = Account {
        info,
        original_info: Box::default(),
        transaction_id: 0,
        storage,
        status: AccountStatus::Touched,
    };

    revm::state::EvmState::from_iter([(PROCESSOR_ADDRESS, account)])
}

/// Bump nonce and subtract gas from the sender so subsequent txs see the right state.
fn insert_sender_account(
    state: &mut revm::state::EvmState,
    sender: alloy_primitives::Address,
    original_info: &AccountInfo,
    gas_cost: U256,
) {
    let mut info = original_info.clone();
    info.nonce = info.nonce.saturating_add(1);
    info.balance = info.balance.saturating_sub(gas_cost);

    let account = Account {
        info,
        original_info: Box::new(original_info.clone()),
        transaction_id: 0,
        storage: revm::state::EvmStorage::default(),
        status: AccountStatus::Touched,
    };
    state.insert(sender, account);
}

fn glint_err(msg: impl Into<Box<dyn core::error::Error + Send + Sync>>) -> BlockExecutionError {
    BlockExecutionError::Internal(InternalBlockExecutionError::Other(msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::TxReceipt;
    use alloy_evm::eth::{EthBlockExecutionCtx, EthEvmBuilder};
    use alloy_primitives::Address;
    use glint_primitives::entity::EntityMetadata;
    use reth_ethereum::{
        chainspec::MAINNET,
        evm::{EthEvmConfig, primitives::EvmEnv},
    };
    use revm::database::{CacheDB, State};

    const TEST_BLOCK: u64 = 1000;

    fn test_evm_config(expiration_index: ExpirationIndex) -> GlintEvmConfig<EthEvmConfig> {
        GlintEvmConfig::new(
            EthEvmConfig::new(MAINNET.clone()),
            GlintChainConfig::default(),
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
            extend_policy: glint_primitives::transaction::ExtendPolicy::OwnerOnly,
            has_operator: false,
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

        // Seed counters so expiration housekeeping can decrement without underflow.
        let slot_counter_key = crate::slot_counter::USED_SLOTS_KEY;
        let entity_counter_key = crate::slot_counter::ENTITY_COUNT_KEY;
        let current_slots = account
            .storage
            .get(&U256::from_be_bytes(slot_counter_key.0))
            .copied()
            .unwrap_or(U256::ZERO);
        account.storage.insert(
            U256::from_be_bytes(slot_counter_key.0),
            current_slots + U256::from(crate::slot_counter::SLOTS_PER_ENTITY),
        );
        let current_entities = account
            .storage
            .get(&U256::from_be_bytes(entity_counter_key.0))
            .copied()
            .unwrap_or(U256::ZERO);
        account.storage.insert(
            U256::from_be_bytes(entity_counter_key.0),
            current_entities + U256::from(1),
        );
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

        let block_env = revm::context::BlockEnv {
            number: U256::from(TEST_BLOCK),
            ..revm::context::BlockEnv::default()
        };

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

        let block_env = revm::context::BlockEnv {
            number: U256::from(TEST_BLOCK),
            ..revm::context::BlockEnv::default()
        };

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

    #[test]
    fn build_processor_state_sets_nonce_to_prevent_eip161_clear() {
        let mut changes = HashMap::new();
        let slot_a = B256::repeat_byte(0xAA);
        let slot_b = B256::repeat_byte(0xBB);
        changes.insert(slot_a, U256::from(42));
        changes.insert(slot_b, U256::ZERO);

        let state = build_processor_state(&changes);

        let account = state
            .get(&PROCESSOR_ADDRESS)
            .expect("processor account should be present");

        assert_eq!(
            account.info.nonce, 1,
            "nonce must be non-zero to prevent EIP-161 state clear"
        );
        assert!(account.is_touched(), "account must be marked as touched");

        let evm_slot_a = U256::from_be_bytes(slot_a.0);
        let storage_a = account.storage.get(&evm_slot_a).unwrap();
        assert!(storage_a.is_changed(), "non-zero slot should be changed");
        assert_eq!(storage_a.present_value(), U256::from(42));

        let evm_slot_b = U256::from_be_bytes(slot_b.0);
        let storage_b = account.storage.get(&evm_slot_b).unwrap();
        assert!(
            storage_b.is_changed(),
            "zero-value slot must be marked as changed to persist deletion"
        );
        assert_eq!(storage_b.present_value(), U256::ZERO);
    }

    #[test]
    fn expiration_persists_state_through_result() {
        use revm::Database as _;
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

        let block_env = revm::context::BlockEnv {
            number: U256::from(TEST_BLOCK),
            ..revm::context::BlockEnv::default()
        };

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
        let (evm, _result) = executor.finish().unwrap();

        let meta_slot = entity_storage_key(&entity_key);
        let db = evm.into_db();
        let value = db
            .storage(PROCESSOR_ADDRESS, U256::from_be_bytes(meta_slot.0))
            .expect("storage read should succeed");
        assert_eq!(
            value,
            U256::ZERO,
            "expired entity metadata slot should be zeroed in persisted state"
        );
    }

    #[test]
    fn glint_tx_increments_sender_nonce_and_deducts_gas() {
        use alloy_consensus::{
            EthereumTxEnvelope, EthereumTypedTransaction, TxEip1559, transaction::Recovered,
        };
        use glint_primitives::transaction::{Create, GlintTransaction as GlintTx};
        use revm::Database as _;

        let config = test_evm_config(ExpirationIndex::new());
        let sender = Address::repeat_byte(0x11);
        let initial_balance = U256::from(10u64).pow(U256::from(18)); // 1 ETH

        let mut db = CacheDB::new(revm::database::EmptyDB::default());
        db.cache
            .accounts
            .entry(sender)
            .or_insert_with(|| revm::database::DbAccount {
                info: AccountInfo {
                    nonce: 0,
                    balance: initial_balance,
                    ..Default::default()
                },
                ..Default::default()
            });

        let base_fee = 1_000_000_000u64; // 1 gwei
        let mut state = State::builder()
            .with_database(db)
            .with_bundle_update()
            .build();

        let block_env = revm::context::BlockEnv {
            number: U256::from(TEST_BLOCK),
            basefee: base_fee,
            ..revm::context::BlockEnv::default()
        };
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

        // Build a glint create tx
        let mut calldata = Vec::new();
        let glint_tx = GlintTx::new().create(Create::new("text/plain", b"test", 100));
        alloy_rlp::Encodable::encode(&glint_tx, &mut calldata);

        let max_fee = u128::from(base_fee) * 2;
        let max_priority = 1_000_000_000u128; // 1 gwei
        let gas_limit = 1_000_000u64;

        let inner = TxEip1559 {
            chain_id: 1,
            nonce: 0,
            gas_limit,
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: max_priority,
            to: alloy_primitives::TxKind::Call(PROCESSOR_ADDRESS),
            input: calldata.into(),
            ..Default::default()
        };
        let typed = EthereumTypedTransaction::Eip1559(inner);
        let sig = alloy_primitives::Signature::new(U256::ZERO, U256::ZERO, false);
        let signed: EthereumTxEnvelope<_> =
            EthereumTxEnvelope::new_unchecked(typed, sig, B256::repeat_byte(0xAA));
        let recovered = Recovered::new_unchecked(signed, sender);

        executor.execute_transaction(&recovered).unwrap();
        let (_evm, _result) = executor.finish().unwrap();

        // Verify sender nonce was incremented and balance decreased
        let sender_info = state
            .basic(sender)
            .expect("db read should succeed")
            .expect("sender should exist");
        assert_eq!(sender_info.nonce, 1, "sender nonce must be incremented");
        assert!(
            sender_info.balance < initial_balance,
            "sender balance must decrease (was {initial_balance}, now {})",
            sender_info.balance,
        );
    }

    #[cfg(feature = "op")]
    #[test]
    fn op_glint_evm_config_constructs() {
        use reth_optimism_evm::OpEvmConfig;

        let chain_spec = reth_optimism_chainspec::BASE_MAINNET.clone();
        let inner = OpEvmConfig::optimism(chain_spec);
        let _config: GlintEvmConfig<OpEvmConfig> = GlintEvmConfig::new(
            inner,
            GlintChainConfig::default(),
            Arc::new(Mutex::new(ExpirationIndex::new())),
        );
    }
}

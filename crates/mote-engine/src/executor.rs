use crate::expiration::ExpirationIndex;

use alloy_consensus::Transaction;
use alloy_evm::{
    block::{BlockExecutionResult, BlockExecutorFactory, BlockExecutorFor, ExecutableTx},
    eth::{EthBlockExecutionCtx, EthBlockExecutor, EthTxResult},
    precompiles::PrecompilesMap,
    Database, EthEvm, EthEvmFactory, FromRecoveredTx, FromTxWithEncoded, RecoveredTx,
};
use alloy_primitives::{B256, Log, U256};
use reth_ethereum::network::types::Encodable2718;
use mote_primitives::{
    constants::PROCESSOR_ADDRESS,
    entity::{derive_entity_key, EntityMetadata},
    events::{EntityCreated, EntityDeleted, EntityExtended, EntityUpdated},
    storage::{compute_content_hash_from_raw, entity_content_hash_key, entity_storage_key},
};
use reth_ethereum::{
    chainspec::ChainSpec,
    evm::{
        primitives::{
            execute::{BlockExecutionError, BlockExecutor, InternalBlockExecutionError},
            Evm, EvmEnv, EvmEnvFor, ExecutionCtxFor, NextBlockEnvAttributes, OnStateHook,
        },
        EthBlockAssembler, EthEvmConfig, RethReceiptBuilder,
    },
    node::api::{ConfigureEngineEvm, ConfigureEvm, ExecutableTxIterator, FullNodeTypes, NodeTypes},
    node::builder::{components::ExecutorBuilder, BuilderContext},
    primitives::{Header, SealedBlock, SealedHeader},
    rpc::types::engine::ExecutionData,
    Block, EthPrimitives, Receipt, TransactionSigned, TxType,
};
use revm::{
    context::result::{ExecutionResult, ResultAndState},
    database::State,
    state::{Account, AccountStatus, EvmStorageSlot},
    DatabaseCommit, Inspector,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub type SharedExpirationIndex = Arc<Mutex<ExpirationIndex>>;

/// Plugs into reth's node builder to wire up `MoteEvmConfig`.
#[derive(Debug, Clone)]
pub struct MoteExecutorBuilder {
    expiration_index: SharedExpirationIndex,
}

impl MoteExecutorBuilder {
    pub fn new(expiration_index: SharedExpirationIndex) -> Self {
        Self { expiration_index }
    }
}

impl<Types, Node> ExecutorBuilder<Node> for MoteExecutorBuilder
where
    Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>,
    Node: FullNodeTypes<Types = Types>,
{
    type EVM = MoteEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::EVM> {
        Ok(MoteEvmConfig {
            inner: EthEvmConfig::new(ctx.chain_spec()),
            expiration_index: self.expiration_index,
        })
    }
}

/// Thin wrapper around `EthEvmConfig` that swaps in `MoteBlockExecutor`
/// for block execution while delegating everything else.
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
    fn evm_env_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
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
}

// Per-op gas costs (flat, no metering).
const MOTE_GAS_PER_CREATE: u64 = 50_000;
const MOTE_GAS_PER_UPDATE: u64 = 40_000;
const MOTE_GAS_PER_DELETE: u64 = 10_000;
const MOTE_GAS_PER_EXTEND: u64 = 10_000;

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
            return self
                .inner
                .execute_transaction_without_commit((tx_env, recovered));
        }

        let sender = *recovered.signer();
        let calldata = tx_ref.input();
        let gas_limit = tx_ref.gas_limit();
        let tx_type = tx_ref.tx_type();
        let tx_hash = tx_ref.trie_hash();

        let (logs, mote_gas_used) = self.execute_mote_crud(calldata, sender, tx_hash)?;

        // TODO: should fail if gas_limit < total — currently caps instead of reverting
        let intrinsic_gas = 21_000u64 + calldata.len() as u64 * 16;
        let total_gas = intrinsic_gas.saturating_add(mote_gas_used).min(gas_limit);

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
    /// Decode calldata, run all CRUD ops, commit state, return (logs, gas).
    fn execute_mote_crud(
        &mut self,
        calldata: &[u8],
        sender: alloy_primitives::Address,
        tx_hash: alloy_primitives::B256,
    ) -> Result<(Vec<Log>, u64), BlockExecutionError> {
        use alloy_evm::revm::context::Block as _;
        use mote_primitives::validation::validate_transaction;

        let current_block: u64 = self.inner.evm().block().number().saturating_to();

        let decoded = decode_with_raw_slices(calldata)
            .map_err(|e| mote_err(format!("RLP decode: {e}")))?;

        validate_transaction(&decoded.tx)
            .map_err(|e| mote_err(format!("validation: {e}")))?;

        let mut logs = Vec::new();
        let mut gas_used = 0u64;
        let mut exp_changes = Vec::new();
        let mut state_changes = HashMap::new();
        let mut op_index = 0u32;

        for (create, slices) in decoded.tx.creates.iter().zip(&decoded.create_slices) {
            let entity_key = derive_entity_key(&tx_hash, &create.payload, op_index);
            let expires_at = current_block + create.btl;

            let metadata = EntityMetadata {
                owner: sender,
                expires_at_block: expires_at,
            };
            let content_hash = compute_content_hash_from_raw(
                slices.payload_rlp,
                slices.content_type_rlp,
                slices.string_annotations_rlp,
                slices.numeric_annotations_rlp,
            );

            let meta_slot = entity_storage_key(&entity_key);
            let content_slot = entity_content_hash_key(&entity_key);
            state_changes.insert(meta_slot, U256::from_be_bytes(metadata.encode()));
            state_changes.insert(content_slot, U256::from_be_bytes(content_hash.0));

            exp_changes.push(ExpirationChange::Insert(expires_at, entity_key));

            let (ann_keys_s, ann_vals_s, ann_keys_n, ann_vals_n) =
                unzip_annotations(&create.string_annotations, &create.numeric_annotations);

            logs.push(EntityCreated::new_log(
                PROCESSOR_ADDRESS,
                entity_key,
                sender,
                expires_at,
                create.content_type.clone(),
                create.payload.clone().into(),
                ann_keys_s,
                ann_vals_s,
                ann_keys_n,
                ann_vals_n,
            ));

            gas_used += MOTE_GAS_PER_CREATE;
            op_index += 1;
        }

        for (update, slices) in decoded.tx.updates.iter().zip(&decoded.update_slices) {

            let old_meta = self.read_entity_metadata(&update.entity_key)?;
            if old_meta.owner != sender {
                return Err(mote_err("sender is not the entity owner"));
            }

            let new_expires = current_block + update.btl;
            let new_meta = EntityMetadata {
                owner: old_meta.owner,
                expires_at_block: new_expires,
            };
            let content_hash = compute_content_hash_from_raw(
                slices.payload_rlp,
                slices.content_type_rlp,
                slices.string_annotations_rlp,
                slices.numeric_annotations_rlp,
            );

            let meta_slot = entity_storage_key(&update.entity_key);
            let content_slot = entity_content_hash_key(&update.entity_key);
            state_changes.insert(meta_slot, U256::from_be_bytes(new_meta.encode()));
            state_changes.insert(content_slot, U256::from_be_bytes(content_hash.0));

            exp_changes.push(ExpirationChange::Remove(
                old_meta.expires_at_block,
                update.entity_key,
            ));
            exp_changes.push(ExpirationChange::Insert(new_expires, update.entity_key));

            let (ann_keys_s, ann_vals_s, ann_keys_n, ann_vals_n) =
                unzip_annotations(&update.string_annotations, &update.numeric_annotations);

            logs.push(EntityUpdated::new_log(
                PROCESSOR_ADDRESS,
                update.entity_key,
                sender,
                old_meta.expires_at_block,
                new_expires,
                update.content_type.clone(),
                update.payload.clone().into(),
                ann_keys_s,
                ann_vals_s,
                ann_keys_n,
                ann_vals_n,
            ));

            gas_used += MOTE_GAS_PER_UPDATE;
        }

        for entity_key in &decoded.tx.deletes {
            let meta = self.read_entity_metadata(entity_key)?;
            if meta.owner != sender {
                return Err(mote_err("sender is not the entity owner"));
            }

            let meta_slot = entity_storage_key(entity_key);
            let content_slot = entity_content_hash_key(entity_key);
            state_changes.insert(meta_slot, U256::ZERO);
            state_changes.insert(content_slot, U256::ZERO);

            exp_changes.push(ExpirationChange::Remove(
                meta.expires_at_block,
                *entity_key,
            ));

            logs.push(EntityDeleted::new_log(
                PROCESSOR_ADDRESS,
                *entity_key,
                meta.owner,
            ));

            gas_used += MOTE_GAS_PER_DELETE;
        }

        for extend in &decoded.tx.extends {
            let old_meta = self.read_entity_metadata(&extend.entity_key)?;
            let new_expires = old_meta
                .expires_at_block
                .saturating_add(extend.additional_blocks);

            let max_expires = current_block + mote_primitives::constants::MAX_BTL;
            if new_expires > max_expires {
                return Err(mote_err("extend would exceed MAX_BTL from current block"));
            }

            let new_meta = EntityMetadata {
                owner: old_meta.owner,
                expires_at_block: new_expires,
            };
            let meta_slot = entity_storage_key(&extend.entity_key);
            state_changes.insert(meta_slot, U256::from_be_bytes(new_meta.encode()));

            exp_changes.push(ExpirationChange::Remove(
                old_meta.expires_at_block,
                extend.entity_key,
            ));
            exp_changes.push(ExpirationChange::Insert(new_expires, extend.entity_key));

            logs.push(EntityExtended::new_log(
                PROCESSOR_ADDRESS,
                extend.entity_key,
                old_meta.expires_at_block,
                new_expires,
            ));

            gas_used += MOTE_GAS_PER_EXTEND;
        }

        commit_storage_changes(self.inner.evm_mut(), &state_changes);

        let mut exp_idx = self
            .expiration_index
            .lock()
            .map_err(|e| mote_err(format!("lock: {e}")))?;
        for change in exp_changes {
            match change {
                ExpirationChange::Insert(block, key) => exp_idx.insert(block, key),
                ExpirationChange::Remove(block, key) => exp_idx.remove(block, &key),
            }
        }

        Ok((logs, gas_used))
    }

    fn read_entity_metadata(
        &mut self,
        entity_key: &alloy_primitives::B256,
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

    /// Zero out entities whose BTL has elapsed. Runs once at block start.
    fn run_expiration_housekeeping(&mut self) -> Result<(), BlockExecutionError> {
        use alloy_evm::revm::context::Block as _;
        use revm::Database as _;

        let current_block: u64 = self.inner.evm().block().number().saturating_to();

        let expired_keys = self
            .expiration_index
            .lock()
            .map_err(|e| mote_err(format!("expiration index lock: {e}")))?
            .drain_block(current_block);

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

            let content_slot = entity_content_hash_key(entity_key);
            state_changes.insert(meta_slot, U256::ZERO);
            state_changes.insert(content_slot, U256::ZERO);
        }

        if !state_changes.is_empty() {
            commit_storage_changes(self.inner.evm_mut(), &state_changes);
        }

        Ok(())
    }
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
        info: Default::default(),
        original_info: Box::default(),
        transaction_id: 0,
        storage,
        status: AccountStatus::Touched,
    };

    evm.db_mut()
        .commit_iter(&mut std::iter::once((PROCESSOR_ADDRESS, account)));
}

fn unzip_annotations(
    string_annotations: &[mote_primitives::transaction::StringAnnotationWire],
    numeric_annotations: &[mote_primitives::transaction::NumericAnnotationWire],
) -> (Vec<String>, Vec<String>, Vec<String>, Vec<u64>) {
    let (keys_s, vals_s) = string_annotations
        .iter()
        .map(|a| (a.key.clone(), a.value.clone()))
        .unzip();
    let (keys_n, vals_n) = numeric_annotations
        .iter()
        .map(|a| (a.key.clone(), a.value))
        .unzip();
    (keys_s, vals_s, keys_n, vals_n)
}

/// Staged until all ops succeed, then applied atomically.
enum ExpirationChange {
    Insert(u64, alloy_primitives::B256),
    Remove(u64, alloy_primitives::B256),
}

fn mote_err(msg: impl Into<Box<dyn core::error::Error + Send + Sync>>) -> BlockExecutionError {
    BlockExecutionError::Internal(InternalBlockExecutionError::Other(msg.into()))
}

/// Pointers into the original calldata — never re-encoded, so the content
/// hash stays deterministic across nodes.
pub struct RawContentSlices<'a> {
    pub payload_rlp: &'a [u8],
    pub content_type_rlp: &'a [u8],
    pub string_annotations_rlp: &'a [u8],
    pub numeric_annotations_rlp: &'a [u8],
}

/// Decoded tx + the raw slices needed for content hashing.
pub struct DecodedMoteTransaction<'a> {
    pub tx: mote_primitives::transaction::MoteTransaction,
    pub create_slices: Vec<RawContentSlices<'a>>,
    pub update_slices: Vec<RawContentSlices<'a>>,
}

/// Advance past one RLP item, returning the full byte range (header + payload).
fn skip_rlp_item<'a>(data: &'a [u8], cursor: &mut &'a [u8]) -> Result<&'a [u8], alloy_rlp::Error> {
    use alloy_rlp::Header;
    let start = data.len() - cursor.len();
    let header = Header::decode(cursor)?;
    *cursor = &cursor[header.payload_length..];
    let end = data.len() - cursor.len();
    Ok(&data[start..end])
}

/// Two-pass decode: first the normal `Decodable` path for the typed struct,
/// then a manual header walk to grab the raw byte slices we need for hashing.
pub fn decode_with_raw_slices(
    calldata: &[u8],
) -> Result<DecodedMoteTransaction<'_>, alloy_rlp::Error> {
    use alloy_rlp::{Decodable, Header};

    let tx = mote_primitives::transaction::MoteTransaction::decode(&mut &calldata[..])?;

    let mut cursor = calldata;
    let outer = Header::decode(&mut cursor)?;
    if !outer.list {
        return Err(alloy_rlp::Error::UnexpectedString);
    }

    let mut create_slices = Vec::with_capacity(tx.creates.len());
    let mut update_slices = Vec::with_capacity(tx.updates.len());

    let creates_header = Header::decode(&mut cursor)?;
    if creates_header.list && creates_header.payload_length > 0 {
        let creates_end = calldata.len() - cursor.len() + creates_header.payload_length;
        while calldata.len() - cursor.len() < creates_end {
            // [btl, content_type, payload, string_anns, numeric_anns]
            let _item_header = Header::decode(&mut cursor)?;
            let _btl = skip_rlp_item(calldata, &mut cursor)?;
            let ct = skip_rlp_item(calldata, &mut cursor)?;
            let payload = skip_rlp_item(calldata, &mut cursor)?;
            let sa = skip_rlp_item(calldata, &mut cursor)?;
            let na = skip_rlp_item(calldata, &mut cursor)?;

            create_slices.push(RawContentSlices {
                content_type_rlp: ct,
                payload_rlp: payload,
                string_annotations_rlp: sa,
                numeric_annotations_rlp: na,
            });
        }
    }

    let updates_header = Header::decode(&mut cursor)?;
    if updates_header.list && updates_header.payload_length > 0 {
        let updates_end = calldata.len() - cursor.len() + updates_header.payload_length;
        while calldata.len() - cursor.len() < updates_end {
            // [entity_key, btl, content_type, payload, string_anns, numeric_anns]
            let _item_header = Header::decode(&mut cursor)?;
            let _ek = skip_rlp_item(calldata, &mut cursor)?;
            let _btl = skip_rlp_item(calldata, &mut cursor)?;
            let ct = skip_rlp_item(calldata, &mut cursor)?;
            let payload = skip_rlp_item(calldata, &mut cursor)?;
            let sa = skip_rlp_item(calldata, &mut cursor)?;
            let na = skip_rlp_item(calldata, &mut cursor)?;

            update_slices.push(RawContentSlices {
                content_type_rlp: ct,
                payload_rlp: payload,
                string_annotations_rlp: sa,
                numeric_annotations_rlp: na,
            });
        }
    }

    // deletes/extends have no content to hash

    Ok(DecodedMoteTransaction {
        tx,
        create_slices,
        update_slices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::Encodable;
    use mote_primitives::{
        storage::compute_content_hash_from_raw,
        transaction::{
            Create, MoteTransaction, NumericAnnotationWire, StringAnnotationWire, Update,
        },
    };

    #[test]
    fn raw_slices_match_encoded_fields() {
        let tx = MoteTransaction {
            creates: vec![Create {
                btl: 100,
                content_type: "text/plain".into(),
                payload: b"hello world".to_vec(),
                string_annotations: vec![StringAnnotationWire {
                    key: "app".into(),
                    value: "test".into(),
                }],
                numeric_annotations: vec![NumericAnnotationWire {
                    key: "priority".into(),
                    value: 1,
                }],
            }],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };

        let mut calldata = Vec::new();
        tx.encode(&mut calldata);

        let decoded = decode_with_raw_slices(&calldata).unwrap();
        assert_eq!(decoded.tx, tx);
        assert_eq!(decoded.create_slices.len(), 1);

        let slices = &decoded.create_slices[0];

        let mut payload_encoded = Vec::new();
        tx.creates[0].payload.encode(&mut payload_encoded);
        assert_eq!(slices.payload_rlp, payload_encoded.as_slice());

        let mut ct_encoded = Vec::new();
        tx.creates[0].content_type.encode(&mut ct_encoded);
        assert_eq!(slices.content_type_rlp, ct_encoded.as_slice());

        let hash = compute_content_hash_from_raw(
            slices.payload_rlp,
            slices.content_type_rlp,
            slices.string_annotations_rlp,
            slices.numeric_annotations_rlp,
        );
        assert_ne!(hash, alloy_primitives::B256::ZERO);
    }

    #[test]
    fn raw_slices_roundtrip_multiple_creates() {
        let tx = MoteTransaction {
            creates: vec![
                Create {
                    btl: 50,
                    content_type: "application/json".into(),
                    payload: b"{\"a\":1}".to_vec(),
                    string_annotations: vec![],
                    numeric_annotations: vec![],
                },
                Create {
                    btl: 200,
                    content_type: "text/plain".into(),
                    payload: b"second entity".to_vec(),
                    string_annotations: vec![StringAnnotationWire {
                        key: "tag".into(),
                        value: "v2".into(),
                    }],
                    numeric_annotations: vec![],
                },
            ],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };

        let mut calldata = Vec::new();
        tx.encode(&mut calldata);

        let decoded = decode_with_raw_slices(&calldata).unwrap();
        assert_eq!(decoded.create_slices.len(), 2);

        let hash0 = compute_content_hash_from_raw(
            decoded.create_slices[0].payload_rlp,
            decoded.create_slices[0].content_type_rlp,
            decoded.create_slices[0].string_annotations_rlp,
            decoded.create_slices[0].numeric_annotations_rlp,
        );
        let hash1 = compute_content_hash_from_raw(
            decoded.create_slices[1].payload_rlp,
            decoded.create_slices[1].content_type_rlp,
            decoded.create_slices[1].string_annotations_rlp,
            decoded.create_slices[1].numeric_annotations_rlp,
        );
        assert_ne!(hash0, hash1);
    }

    #[test]
    fn raw_slices_with_updates() {
        let tx = MoteTransaction {
            creates: vec![],
            updates: vec![Update {
                entity_key: alloy_primitives::B256::repeat_byte(0x01),
                btl: 200,
                content_type: "application/json".into(),
                payload: b"{\"updated\":true}".to_vec(),
                string_annotations: vec![StringAnnotationWire {
                    key: "v".into(),
                    value: "2".into(),
                }],
                numeric_annotations: vec![],
            }],
            deletes: vec![],
            extends: vec![],
        };

        let mut calldata = Vec::new();
        tx.encode(&mut calldata);

        let decoded = decode_with_raw_slices(&calldata).unwrap();
        assert_eq!(decoded.update_slices.len(), 1);

        let slices = &decoded.update_slices[0];
        let mut payload_encoded = Vec::new();
        tx.updates[0].payload.encode(&mut payload_encoded);
        assert_eq!(slices.payload_rlp, payload_encoded.as_slice());
    }

    #[test]
    fn skip_rlp_item_handles_single_byte_values() {
        let data = &[0x2a];
        let mut cursor = &data[..];
        let slice = skip_rlp_item(data, &mut cursor).unwrap();
        assert_eq!(slice, &[0x2a]);
        assert!(cursor.is_empty());
    }

    #[test]
    fn skip_rlp_item_handles_short_string() {
        let data = &[0x85, b'h', b'e', b'l', b'l', b'o'];
        let mut cursor = &data[..];
        let slice = skip_rlp_item(data, &mut cursor).unwrap();
        assert_eq!(slice, data);
        assert!(cursor.is_empty());
    }
}

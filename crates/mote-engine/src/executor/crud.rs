use alloy_evm::block::BlockExecutor as _;
use alloy_evm::{Database, FromRecoveredTx, FromTxWithEncoded};
use alloy_primitives::{B256, Log, U256};
use mote_primitives::{
    constants::PROCESSOR_ADDRESS,
    entity::{EntityMetadata, derive_entity_key},
    events::{EntityCreated, EntityDeleted, EntityExtended, EntityUpdated, LogAnnotations},
    storage::{compute_content_hash_from_raw, entity_content_hash_key, entity_storage_key},
};
use reth_ethereum::TransactionSigned;
use reth_ethereum::evm::primitives::{Evm, execute::BlockExecutionError};
use revm::database::State;
use std::collections::HashMap;

use super::decode::{DecodedMoteTransaction, decode_with_raw_slices};
use super::{MoteBlockExecutor, commit_storage_changes, mote_err};

use super::{MOTE_GAS_PER_CREATE, MOTE_GAS_PER_DELETE, MOTE_GAS_PER_EXTEND, MOTE_GAS_PER_UPDATE};

/// Staged until all ops succeed, then applied atomically.
pub(super) enum ExpirationChange {
    Insert(u64, B256),
    Remove(u64, B256),
}

pub(super) struct CrudAccumulator {
    pub logs: Vec<Log>,
    pub gas_used: u64,
    pub exp_changes: Vec<ExpirationChange>,
    pub state_changes: HashMap<B256, U256>,
}

impl<'db, DB, E> MoteBlockExecutor<'_, E>
where
    DB: Database + 'db,
    E: Evm<
            DB = &'db mut State<DB>,
            Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>,
        >,
{
    pub(super) fn execute_mote_crud(
        &mut self,
        calldata: &[u8],
        sender: alloy_primitives::Address,
        tx_hash: B256,
    ) -> Result<(Vec<Log>, u64), BlockExecutionError> {
        use alloy_evm::revm::context::Block as _;
        use mote_primitives::validation::validate_transaction;

        let current_block: u64 = self.inner.evm().block().number().saturating_to();

        let decoded =
            decode_with_raw_slices(calldata).map_err(|e| mote_err(format!("RLP decode: {e}")))?;

        validate_transaction(&decoded.tx).map_err(|e| mote_err(format!("validation: {e}")))?;

        let mut acc = CrudAccumulator {
            logs: Vec::new(),
            gas_used: 0,
            exp_changes: Vec::new(),
            state_changes: HashMap::new(),
        };

        Self::process_creates(&mut acc, &decoded, sender, tx_hash, current_block);
        self.process_updates(&mut acc, &decoded, sender, current_block)?;
        self.process_deletes(&mut acc, &decoded.tx.deletes, sender)?;
        self.process_extends(&mut acc, &decoded.tx.extends, current_block)?;

        commit_storage_changes(self.inner.evm_mut(), &acc.state_changes);

        let mut exp_idx = self
            .expiration_index
            .lock()
            .map_err(|e| mote_err(format!("lock: {e}")))?;
        for change in acc.exp_changes {
            match change {
                ExpirationChange::Insert(block, key) => exp_idx.insert(block, key),
                ExpirationChange::Remove(block, key) => exp_idx.remove(block, &key),
            }
        }

        Ok((acc.logs, acc.gas_used))
    }

    fn process_creates(
        acc: &mut CrudAccumulator,
        decoded: &DecodedMoteTransaction<'_>,
        sender: alloy_primitives::Address,
        tx_hash: B256,
        current_block: u64,
    ) {
        for (op_index, (create, slices)) in decoded
            .tx
            .creates
            .iter()
            .zip(&decoded.create_slices)
            .enumerate()
        {
            let entity_key = derive_entity_key(
                &tx_hash,
                &create.payload,
                u32::try_from(op_index).expect("op count bounded by MAX_OPS_PER_TX"),
            );
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
            acc.state_changes
                .insert(meta_slot, U256::from_be_bytes(metadata.encode()));
            acc.state_changes
                .insert(content_slot, U256::from_be_bytes(content_hash.0));

            acc.exp_changes
                .push(ExpirationChange::Insert(expires_at, entity_key));

            let annotations =
                unzip_annotations(&create.string_annotations, &create.numeric_annotations);

            acc.logs.push(EntityCreated::new_log(
                PROCESSOR_ADDRESS,
                entity_key,
                sender,
                expires_at,
                create.content_type.clone(),
                create.payload.clone().into(),
                annotations,
            ));

            acc.gas_used += MOTE_GAS_PER_CREATE;
        }
    }

    fn process_updates(
        &mut self,
        acc: &mut CrudAccumulator,
        decoded: &DecodedMoteTransaction<'_>,
        sender: alloy_primitives::Address,
        current_block: u64,
    ) -> Result<(), BlockExecutionError> {
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
            acc.state_changes
                .insert(meta_slot, U256::from_be_bytes(new_meta.encode()));
            acc.state_changes
                .insert(content_slot, U256::from_be_bytes(content_hash.0));

            acc.exp_changes.push(ExpirationChange::Remove(
                old_meta.expires_at_block,
                update.entity_key,
            ));
            acc.exp_changes
                .push(ExpirationChange::Insert(new_expires, update.entity_key));

            let annotations =
                unzip_annotations(&update.string_annotations, &update.numeric_annotations);

            acc.logs.push(EntityUpdated::new_log(
                PROCESSOR_ADDRESS,
                update.entity_key,
                sender,
                (old_meta.expires_at_block, new_expires),
                update.content_type.clone(),
                update.payload.clone().into(),
                annotations,
            ));

            acc.gas_used += MOTE_GAS_PER_UPDATE;
        }
        Ok(())
    }

    fn process_deletes(
        &mut self,
        acc: &mut CrudAccumulator,
        deletes: &[B256],
        sender: alloy_primitives::Address,
    ) -> Result<(), BlockExecutionError> {
        for entity_key in deletes {
            let meta = self.read_entity_metadata(entity_key)?;
            if meta.owner != sender {
                return Err(mote_err("sender is not the entity owner"));
            }

            let meta_slot = entity_storage_key(entity_key);
            let content_slot = entity_content_hash_key(entity_key);
            acc.state_changes.insert(meta_slot, U256::ZERO);
            acc.state_changes.insert(content_slot, U256::ZERO);

            acc.exp_changes
                .push(ExpirationChange::Remove(meta.expires_at_block, *entity_key));

            acc.logs.push(EntityDeleted::new_log(
                PROCESSOR_ADDRESS,
                *entity_key,
                meta.owner,
            ));

            acc.gas_used += MOTE_GAS_PER_DELETE;
        }
        Ok(())
    }

    fn process_extends(
        &mut self,
        acc: &mut CrudAccumulator,
        extends: &[mote_primitives::transaction::Extend],
        current_block: u64,
    ) -> Result<(), BlockExecutionError> {
        for extend in extends {
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
            acc.state_changes
                .insert(meta_slot, U256::from_be_bytes(new_meta.encode()));

            acc.exp_changes.push(ExpirationChange::Remove(
                old_meta.expires_at_block,
                extend.entity_key,
            ));
            acc.exp_changes
                .push(ExpirationChange::Insert(new_expires, extend.entity_key));

            acc.logs.push(EntityExtended::new_log(
                PROCESSOR_ADDRESS,
                extend.entity_key,
                old_meta.expires_at_block,
                new_expires,
            ));

            acc.gas_used += MOTE_GAS_PER_EXTEND;
        }
        Ok(())
    }
}

fn unzip_annotations(
    string_annotations: &[mote_primitives::transaction::StringAnnotationWire],
    numeric_annotations: &[mote_primitives::transaction::NumericAnnotationWire],
) -> LogAnnotations {
    let (string_keys, string_values) = string_annotations
        .iter()
        .map(|a| (a.key.clone(), a.value.clone()))
        .unzip();
    let (numeric_keys, numeric_values) = numeric_annotations
        .iter()
        .map(|a| (a.key.clone(), a.value))
        .unzip();
    LogAnnotations {
        string_keys,
        string_values,
        numeric_keys,
        numeric_values,
    }
}

use alloy_primitives::{Address, B256, Log, U256};
use glint_primitives::{
    constants::PROCESSOR_ADDRESS,
    entity::{EntityMetadata, derive_entity_key},
    events::{EntityCreated, EntityDeleted, EntityExtended, EntityUpdated, LogAnnotations},
    storage::{
        compute_content_hash_from_raw, decode_operator_value, encode_operator_value,
        entity_content_hash_key, entity_operator_key, entity_storage_key,
    },
};
use reth_evm::{Evm, block::BlockExecutionError};
use revm::DatabaseCommit;
use std::collections::HashMap;
use tracing::{debug, info, instrument};

use super::decode::{DecodedGlintTransaction, decode_with_raw_slices};
use super::{GlintBlockExecutor, GlintResultBuilder, glint_err};

use super::{
    GAS_PER_BTL_BLOCK, GAS_PER_DATA_BYTE, GLINT_GAS_PER_CREATE, GLINT_GAS_PER_DELETE,
    GLINT_GAS_PER_EXTEND, GLINT_GAS_PER_UPDATE,
};

pub(super) enum ExpirationChange {
    Insert(u64, B256),
    Remove(u64, B256),
}

pub(super) struct CrudAccumulator {
    pub(super) logs: Vec<Log>,
    pub(super) gas_used: u64,
    pub(super) exp_changes: Vec<ExpirationChange>,
    pub(super) state_changes: HashMap<B256, U256>,
    pub(super) slot_counter_delta: i64,
    pub(super) entity_counter_delta: i64,
}

impl<InnerExec, RB> GlintBlockExecutor<InnerExec, RB>
where
    InnerExec: reth_evm::block::BlockExecutor<
        Transaction: super::GlintTransaction,
        Receipt: alloy_consensus::TxReceipt<Log = Log>,
    >,
    InnerExec::Evm: Evm<DB: revm::Database<Error: core::fmt::Display> + DatabaseCommit>,
    RB: GlintResultBuilder<
        Result = InnerExec::Result,
        TxType = <<InnerExec as reth_evm::block::BlockExecutor>::Transaction as alloy_consensus::TransactionEnvelope>::TxType,
    >,
{
    #[instrument(skip_all, fields(%sender, %tx_hash), name = "glint::execute_crud")]
    pub(super) fn execute_glint_crud(
        &mut self,
        calldata: &[u8],
        sender: alloy_primitives::Address,
        tx_hash: B256,
    ) -> Result<CrudAccumulator, BlockExecutionError> {
        use alloy_evm::revm::context::Block as _;
        use glint_primitives::validation::validate_transaction;

        let current_block: u64 = self.inner.evm().block().number().saturating_to();

        let decoded =
            decode_with_raw_slices(calldata).map_err(|e| glint_err(format!("RLP decode: {e}")))?;

        info!(
            creates = decoded.tx.creates.len(),
            updates = decoded.tx.updates.len(),
            deletes = decoded.tx.deletes.len(),
            extends = decoded.tx.extends.len(),
            "decoded glint transaction"
        );

        validate_transaction(&decoded.tx, &self.config)
            .map_err(|e| glint_err(format!("validation: {e}")))?;

        let mut acc = CrudAccumulator {
            logs: Vec::new(),
            gas_used: 0,
            exp_changes: Vec::new(),
            state_changes: HashMap::new(),
            slot_counter_delta: 0,
            entity_counter_delta: 0,
        };

        Self::process_creates(&mut acc, &decoded, sender, tx_hash, current_block)?;
        self.process_updates(&mut acc, &decoded, sender, current_block)?;
        self.process_deletes(&mut acc, &decoded.tx.deletes, sender)?;
        self.process_extends(&mut acc, &decoded.tx.extends, current_block, sender)?;

        debug!(
            state_changes = acc.state_changes.len(),
            exp_changes = acc.exp_changes.len(),
            entity_counter_delta = acc.entity_counter_delta,
            slot_counter_delta = acc.slot_counter_delta,
            gas_used = acc.gas_used,
            "crud execution complete"
        );

        Ok(acc)
    }

    #[instrument(skip_all, name = "glint::commit_crud")]
    pub(super) fn commit_crud(
        &mut self,
        mut acc: CrudAccumulator,
    ) -> Result<(Vec<Log>, revm::state::EvmState), BlockExecutionError> {
        super::update_slot_counter(
            self.inner.evm_mut(),
            acc.slot_counter_delta,
            &mut acc.state_changes,
        )?;
        super::update_entity_counter(
            self.inner.evm_mut(),
            acc.entity_counter_delta,
            &mut acc.state_changes,
        )?;

        let state = super::build_processor_state(&acc.state_changes);

        let mut exp_idx = self.expiration_index.lock();
        for change in acc.exp_changes {
            match change {
                ExpirationChange::Insert(block, key) => exp_idx.insert(block, key),
                ExpirationChange::Remove(block, key) => exp_idx.remove(block, &key),
            }
        }

        Ok((acc.logs, state))
    }

    #[instrument(skip_all, fields(%sender, %tx_hash, current_block), name = "glint::process_creates")]
    fn process_creates(
        acc: &mut CrudAccumulator,
        decoded: &DecodedGlintTransaction<'_>,
        sender: alloy_primitives::Address,
        tx_hash: B256,
        current_block: u64,
    ) -> Result<(), BlockExecutionError> {
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
            let expires_at = current_block
                .checked_add(create.btl)
                .ok_or_else(|| glint_err("block + btl overflow"))?;
            info!(
                %entity_key,
                op_index,
                btl = create.btl,
                expires_at,
                payload_len = create.payload.len(),
                content_type = %create.content_type,
                "creating entity"
            );

            let has_operator = create.operator.is_some();
            let metadata = EntityMetadata {
                owner: sender,
                expires_at_block: expires_at,
                extend_policy: create.extend_policy,
                has_operator,
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

            if let Some(operator_addr) = create.operator {
                let op_slot = entity_operator_key(&entity_key);
                acc.state_changes
                    .insert(op_slot, encode_operator_value(operator_addr));
            }

            acc.exp_changes
                .push(ExpirationChange::Insert(expires_at, entity_key));

            let annotations =
                unzip_annotations(&create.string_annotations, &create.numeric_annotations);

            let extend_policy_u8 = create.extend_policy as u8;
            let operator_for_log = create.operator.unwrap_or(Address::ZERO);
            acc.logs.push(EntityCreated::new_log(
                PROCESSOR_ADDRESS,
                entity_key,
                sender,
                expires_at,
                create.content_type.clone(),
                create.payload.clone().into(),
                annotations,
                extend_policy_u8,
                operator_for_log,
            ));

            acc.gas_used += GLINT_GAS_PER_CREATE
                + create.payload.len() as u64 * GAS_PER_DATA_BYTE
                + create.btl * GAS_PER_BTL_BLOCK
                + annotation_gas_bytes(&create.string_annotations, &create.numeric_annotations)
                    * GAS_PER_DATA_BYTE;

            if has_operator {
                acc.gas_used += super::GAS_OPERATOR_WRITE;
            }

            let slots = if has_operator {
                glint_primitives::constants::SLOTS_PER_ENTITY_WITH_OPERATOR
            } else {
                crate::slot_counter::SLOTS_PER_ENTITY
            };
            acc.slot_counter_delta += slots.cast_signed();
            acc.entity_counter_delta += 1;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(%sender, current_block), name = "glint::process_updates")]
    fn process_updates(
        &mut self,
        acc: &mut CrudAccumulator,
        decoded: &DecodedGlintTransaction<'_>,
        sender: alloy_primitives::Address,
        current_block: u64,
    ) -> Result<(), BlockExecutionError> {
        for (update, slices) in decoded.tx.updates.iter().zip(&decoded.update_slices) {
            let old_meta = self.read_entity_metadata(&update.entity_key)?;

            let operator = if old_meta.has_operator {
                acc.gas_used += super::GAS_SLOAD;
                read_operator_slot(self.inner.evm_mut(), &update.entity_key)?
            } else {
                None
            };

            if !authorize_mutation(sender, old_meta.owner, operator) {
                return Err(glint_err("sender is not authorized to update entity"));
            }

            let is_owner = old_meta.owner == sender;

            if !is_owner && (update.extend_policy.is_some() || update.operator.is_some()) {
                return Err(glint_err("operator cannot change permissions"));
            }

            let new_extend_policy = if is_owner {
                update.extend_policy.unwrap_or(old_meta.extend_policy)
            } else {
                old_meta.extend_policy
            };

            let new_has_operator = if is_owner {
                match update.operator {
                    Some(None) => false,
                    Some(Some(_)) => true,
                    None => old_meta.has_operator,
                }
            } else {
                old_meta.has_operator
            };

            let new_expires = current_block
                .checked_add(update.btl)
                .ok_or_else(|| glint_err("block + btl overflow"))?;
            let new_meta = EntityMetadata {
                owner: old_meta.owner,
                expires_at_block: new_expires,
                extend_policy: new_extend_policy,
                has_operator: new_has_operator,
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

            if is_owner {
                match update.operator {
                    Some(None) => {
                        if old_meta.has_operator {
                            let op_slot = entity_operator_key(&update.entity_key);
                            acc.state_changes.insert(op_slot, U256::ZERO);
                            acc.slot_counter_delta -= 1;
                        }
                    }
                    Some(Some(addr)) => {
                        let op_slot = entity_operator_key(&update.entity_key);
                        acc.state_changes
                            .insert(op_slot, encode_operator_value(addr));
                        acc.gas_used += super::GAS_OPERATOR_WRITE;

                        if !old_meta.has_operator {
                            acc.slot_counter_delta += 1;
                        }
                    }
                    None => {}
                }
            }

            acc.exp_changes.push(ExpirationChange::Remove(
                old_meta.expires_at_block,
                update.entity_key,
            ));
            acc.exp_changes
                .push(ExpirationChange::Insert(new_expires, update.entity_key));

            let annotations =
                unzip_annotations(&update.string_annotations, &update.numeric_annotations);

            let extend_policy_u8 = new_extend_policy as u8;
            let operator_for_log = match update.operator {
                Some(Some(addr)) => addr,
                _ if new_has_operator => operator.unwrap_or(Address::ZERO),
                _ => Address::ZERO,
            };
            acc.logs.push(EntityUpdated::new_log(
                PROCESSOR_ADDRESS,
                update.entity_key,
                sender,
                (old_meta.expires_at_block, new_expires),
                update.content_type.clone(),
                update.payload.clone().into(),
                annotations,
                extend_policy_u8,
                operator_for_log,
            ));

            acc.gas_used += GLINT_GAS_PER_UPDATE
                + update.payload.len() as u64 * GAS_PER_DATA_BYTE
                + update.btl * GAS_PER_BTL_BLOCK
                + annotation_gas_bytes(&update.string_annotations, &update.numeric_annotations)
                    * GAS_PER_DATA_BYTE;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(%sender, count = deletes.len()), name = "glint::process_deletes")]
    fn process_deletes(
        &mut self,
        acc: &mut CrudAccumulator,
        deletes: &[B256],
        sender: alloy_primitives::Address,
    ) -> Result<(), BlockExecutionError> {
        for entity_key in deletes {
            let meta = self.read_entity_metadata(entity_key)?;

            let operator = if meta.has_operator {
                acc.gas_used += super::GAS_SLOAD;
                read_operator_slot(self.inner.evm_mut(), entity_key)?
            } else {
                None
            };

            if !authorize_mutation(sender, meta.owner, operator) {
                return Err(glint_err("sender is not authorized to delete entity"));
            }

            let meta_slot = entity_storage_key(entity_key);
            let content_slot = entity_content_hash_key(entity_key);
            acc.state_changes.insert(meta_slot, U256::ZERO);
            acc.state_changes.insert(content_slot, U256::ZERO);

            if meta.has_operator {
                let op_slot = entity_operator_key(entity_key);
                acc.state_changes.insert(op_slot, U256::ZERO);
            }

            acc.exp_changes
                .push(ExpirationChange::Remove(meta.expires_at_block, *entity_key));

            acc.logs.push(EntityDeleted::new_log(
                PROCESSOR_ADDRESS,
                *entity_key,
                meta.owner,
                sender,
            ));

            acc.gas_used += GLINT_GAS_PER_DELETE;

            acc.slot_counter_delta -=
                crate::slot_counter::slots_for_entity(meta.has_operator).cast_signed();
            acc.entity_counter_delta -= 1;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(%sender, current_block, count = extends.len()), name = "glint::process_extends")]
    fn process_extends(
        &mut self,
        acc: &mut CrudAccumulator,
        extends: &[glint_primitives::transaction::Extend],
        current_block: u64,
        sender: Address,
    ) -> Result<(), BlockExecutionError> {
        use glint_primitives::transaction::ExtendPolicy;

        for extend in extends {
            let old_meta = self.read_entity_metadata(&extend.entity_key)?;

            if old_meta.extend_policy != ExtendPolicy::AnyoneCanExtend
                && sender != old_meta.owner
            {
                if old_meta.has_operator {
                    acc.gas_used += super::GAS_SLOAD;
                    let operator =
                        read_operator_slot(self.inner.evm_mut(), &extend.entity_key)?;
                    if operator.is_none_or(|op| op != sender) {
                        return Err(glint_err("not authorized to extend"));
                    }
                } else {
                    return Err(glint_err("not authorized to extend"));
                }
            }

            let new_expires = old_meta
                .expires_at_block
                .checked_add(extend.additional_blocks)
                .ok_or_else(|| glint_err("extend expiration overflow"))?;

            let max_expires = current_block
                .checked_add(self.config.max_btl)
                .ok_or_else(|| glint_err("block + max_btl overflow"))?;
            if new_expires > max_expires {
                return Err(glint_err("extend would exceed MAX_BTL from current block"));
            }

            let new_meta = EntityMetadata {
                owner: old_meta.owner,
                expires_at_block: new_expires,
                extend_policy: old_meta.extend_policy,
                has_operator: old_meta.has_operator,
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
                old_meta.owner,
            ));

            acc.gas_used = acc
                .gas_used
                .saturating_add(GLINT_GAS_PER_EXTEND)
                .saturating_add(extend.additional_blocks.saturating_mul(GAS_PER_BTL_BLOCK));
        }
        Ok(())
    }
}

fn read_operator_slot<E: Evm<DB: revm::Database<Error: core::fmt::Display>>>(
    evm: &mut E,
    entity_key: &B256,
) -> Result<Option<Address>, BlockExecutionError> {
    use revm::Database as _;

    let slot = entity_operator_key(entity_key);
    let value = evm
        .db_mut()
        .storage(PROCESSOR_ADDRESS, U256::from_be_bytes(slot.0))
        .map_err(|e| glint_err(format!("operator read: {e}")))?;

    if value == U256::ZERO {
        Ok(None)
    } else {
        Ok(Some(decode_operator_value(value)))
    }
}

fn authorize_mutation(sender: Address, owner: Address, operator: Option<Address>) -> bool {
    sender == owner || operator.is_some_and(|op| op == sender)
}

fn annotation_gas_bytes(
    string_annotations: &[glint_primitives::transaction::StringAnnotationWire],
    numeric_annotations: &[glint_primitives::transaction::NumericAnnotationWire],
) -> u64 {
    string_annotations
        .iter()
        .map(|a| a.key.len() as u64 + a.value.len() as u64)
        .sum::<u64>()
        + numeric_annotations
            .iter()
            .map(|a| a.key.len() as u64 + 8u64)
            .sum::<u64>()
}

fn unzip_annotations(
    string_annotations: &[glint_primitives::transaction::StringAnnotationWire],
    numeric_annotations: &[glint_primitives::transaction::NumericAnnotationWire],
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

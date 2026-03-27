use alloy_primitives::{Address, B256, U256};
use glint_primitives::{
    entity::{EntityKey, EntityMetadata, derive_entity_key},
    error::GlintError,
    storage::{
        compute_content_hash_from_raw, decode_operator_value, encode_operator_value,
        entity_content_hash_key, entity_operator_key, entity_storage_key,
    },
    transaction::ExtendPolicy,
};

pub struct RawContentSlices<'a> {
    pub payload_rlp: &'a [u8],
    pub content_type_rlp: &'a [u8],
    pub string_annotations_rlp: &'a [u8],
    pub numeric_annotations_rlp: &'a [u8],
}

/// Mockable trie interface for the real executor (which goes through revm).
pub trait EntityState {
    fn read_slot(&self, key: &B256) -> Option<U256>;
    fn write_slot(&mut self, key: B256, value: U256);
}

fn read_metadata(
    state: &impl EntityState,
    entity_key: &B256,
) -> Result<EntityMetadata, GlintError> {
    let slot_key = entity_storage_key(entity_key);
    let value = state
        .read_slot(&slot_key)
        .ok_or(GlintError::EntityNotFound(*entity_key))?;
    if value == U256::ZERO {
        return Err(GlintError::EntityNotFound(*entity_key));
    }
    let bytes: [u8; 32] = value.to_be_bytes();
    Ok(EntityMetadata::decode(&bytes))
}

pub fn read_operator(state: &impl EntityState, entity_key: &B256) -> Option<Address> {
    let slot = entity_operator_key(entity_key);
    let value = state.read_slot(&slot)?;
    if value == U256::ZERO {
        return None;
    }
    Some(decode_operator_value(value))
}

pub fn write_operator(state: &mut impl EntityState, entity_key: &B256, addr: Address) {
    let slot = entity_operator_key(entity_key);
    state.write_slot(slot, encode_operator_value(addr));
}

pub fn delete_operator(state: &mut impl EntityState, entity_key: &B256) {
    let slot = entity_operator_key(entity_key);
    state.write_slot(slot, U256::ZERO);
}

fn write_entity(
    state: &mut impl EntityState,
    entity_key: &B256,
    metadata: &EntityMetadata,
    content_hash: B256,
) {
    let meta_slot = entity_storage_key(entity_key);
    state.write_slot(meta_slot, U256::from_be_bytes(metadata.encode()));

    let content_slot = entity_content_hash_key(entity_key);
    state.write_slot(content_slot, U256::from_be_bytes(content_hash.0));
}

fn delete_entity(state: &mut impl EntityState, entity_key: &B256) {
    let meta_slot = entity_storage_key(entity_key);
    state.write_slot(meta_slot, U256::ZERO);

    let content_slot = entity_content_hash_key(entity_key);
    state.write_slot(content_slot, U256::ZERO);
}

pub fn execute_create(
    state: &mut impl EntityState,
    tx_hash: &B256,
    sender: Address,
    current_block: u64,
    op_index: u32,
    create: &glint_primitives::transaction::Create,
    slices: &RawContentSlices<'_>,
) -> Result<EntityKey, GlintError> {
    let entity_key = derive_entity_key(tx_hash, &create.payload, op_index);

    let metadata = EntityMetadata {
        owner: sender,
        expires_at_block: current_block
            .checked_add(create.btl)
            .ok_or(GlintError::ExceedsMaxBtl)?,
        extend_policy: create.extend_policy,
        has_operator: create.operator.is_some(),
    };

    let content_hash = compute_content_hash_from_raw(
        slices.payload_rlp,
        slices.content_type_rlp,
        slices.string_annotations_rlp,
        slices.numeric_annotations_rlp,
    );

    write_entity(state, &entity_key, &metadata, content_hash);

    if let Some(op) = create.operator {
        write_operator(state, &entity_key, op);
    }

    Ok(entity_key)
}

pub fn execute_update(
    state: &mut impl EntityState,
    sender: Address,
    current_block: u64,
    update: &glint_primitives::transaction::Update,
    slices: &RawContentSlices<'_>,
) -> Result<EntityMetadata, GlintError> {
    let old_meta = read_metadata(state, &update.entity_key)?;

    let operator = if old_meta.has_operator {
        read_operator(state, &update.entity_key)
    } else {
        None
    };

    let is_owner = old_meta.owner == sender;
    let is_operator = operator.is_some_and(|op| op == sender);

    if !is_owner && !is_operator {
        return Err(GlintError::NotAuthorizedToUpdate);
    }

    if is_operator && !is_owner && (update.extend_policy.is_some() || update.operator.is_some()) {
        return Err(GlintError::OperatorCannotChangePermissions);
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

    let new_metadata = EntityMetadata {
        owner: old_meta.owner,
        expires_at_block: current_block
            .checked_add(update.btl)
            .ok_or(GlintError::ExceedsMaxBtl)?,
        extend_policy: new_extend_policy,
        has_operator: new_has_operator,
    };

    let content_hash = compute_content_hash_from_raw(
        slices.payload_rlp,
        slices.content_type_rlp,
        slices.string_annotations_rlp,
        slices.numeric_annotations_rlp,
    );

    write_entity(state, &update.entity_key, &new_metadata, content_hash);

    if is_owner {
        match update.operator {
            Some(None) => delete_operator(state, &update.entity_key),
            Some(Some(addr)) => write_operator(state, &update.entity_key, addr),
            None => {}
        }
    }

    Ok(old_meta)
}

pub fn execute_delete(
    state: &mut impl EntityState,
    entity_key: &B256,
    sender: Address,
) -> Result<EntityMetadata, GlintError> {
    let meta = read_metadata(state, entity_key)?;

    let operator = if meta.has_operator {
        read_operator(state, entity_key)
    } else {
        None
    };

    let is_owner = meta.owner == sender;
    let is_operator = operator.is_some_and(|op| op == sender);

    if !is_owner && !is_operator {
        return Err(GlintError::NotOwner);
    }

    delete_entity(state, entity_key);

    if meta.has_operator {
        delete_operator(state, entity_key);
    }

    Ok(meta)
}

pub fn execute_extend(
    state: &mut impl EntityState,
    sender: Address,
    extend: &glint_primitives::transaction::Extend,
    current_block: u64,
    max_btl: u64,
) -> Result<(EntityMetadata, u64), GlintError> {
    let old_meta = read_metadata(state, &extend.entity_key)?;

    let is_owner = old_meta.owner == sender;
    match old_meta.extend_policy {
        ExtendPolicy::OwnerOnly => {
            if !is_owner {
                let operator = if old_meta.has_operator {
                    read_operator(state, &extend.entity_key)
                } else {
                    None
                };
                let is_operator = operator.is_some_and(|op| op == sender);
                if !is_operator {
                    return Err(GlintError::NotAuthorizedToExtend);
                }
            }
        }
        ExtendPolicy::AnyoneCanExtend => {}
    }

    let new_expires = old_meta
        .expires_at_block
        .checked_add(extend.additional_blocks)
        .ok_or(GlintError::ExceedsMaxBtl)?;

    let max_expires = current_block
        .checked_add(max_btl)
        .ok_or(GlintError::ExceedsMaxBtl)?;
    if new_expires > max_expires {
        return Err(GlintError::ExceedsMaxBtl);
    }

    let new_metadata = EntityMetadata {
        owner: old_meta.owner,
        expires_at_block: new_expires,
        extend_policy: old_meta.extend_policy,
        has_operator: old_meta.has_operator,
    };

    let meta_slot = entity_storage_key(&extend.entity_key);
    state.write_slot(meta_slot, U256::from_be_bytes(new_metadata.encode()));

    Ok((old_meta, new_expires))
}

#[cfg(test)]
mod tests {
    use super::*;
    use glint_primitives::constants::MAX_BTL;
    use glint_primitives::storage::entity_operator_key;
    use glint_primitives::transaction::{Create, Extend, Update};
    use std::collections::HashMap;

    #[derive(Default)]
    struct MockState {
        slots: HashMap<B256, U256>,
    }

    impl EntityState for MockState {
        fn read_slot(&self, key: &B256) -> Option<U256> {
            self.slots.get(key).copied().filter(|v| *v != U256::ZERO)
        }

        fn write_slot(&mut self, key: B256, value: U256) {
            self.slots.insert(key, value);
        }
    }

    fn default_slices() -> RawContentSlices<'static> {
        RawContentSlices {
            payload_rlp: &[0x85, b'h', b'e', b'l', b'l', b'o'],
            content_type_rlp: &[
                0x8a, b't', b'e', b'x', b't', b'/', b'p', b'l', b'a', b'i', b'n',
            ],
            string_annotations_rlp: &[0xc0],
            numeric_annotations_rlp: &[0xc0],
        }
    }

    fn seed_entity(
        state: &mut MockState,
        entity_key: &B256,
        owner: Address,
        expires_at_block: u64,
        extend_policy: ExtendPolicy,
        has_operator: bool,
        operator: Option<Address>,
    ) {
        let meta = EntityMetadata {
            owner,
            expires_at_block,
            extend_policy,
            has_operator,
        };
        let meta_slot = entity_storage_key(entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let content_slot = entity_content_hash_key(entity_key);
        state.write_slot(content_slot, U256::from(0xDEAD));

        if let Some(op) = operator {
            write_operator(state, entity_key, op);
        }
    }

    #[test]
    fn execute_create_writes_two_slots() {
        let mut state = MockState::default();
        let tx_hash = B256::repeat_byte(0x01);
        let sender = Address::repeat_byte(0x42);
        let current_block = 1000;
        let create = Create {
            btl: 100,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: ExtendPolicy::OwnerOnly,
            operator: None,
        };

        let slices = default_slices();
        let result = execute_create(
            &mut state,
            &tx_hash,
            sender,
            current_block,
            0,
            &create,
            &slices,
        );

        assert!(result.is_ok());
        let entity_key = result.expect("create should succeed");

        let meta_slot = entity_storage_key(&entity_key);
        assert!(state.read_slot(&meta_slot).is_some());

        let content_slot = entity_content_hash_key(&entity_key);
        assert!(state.read_slot(&content_slot).is_some());
    }

    #[test]
    fn execute_create_with_operator_writes_three_slots() {
        let mut state = MockState::default();
        let tx_hash = B256::repeat_byte(0x01);
        let sender = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);
        let current_block = 1000;
        let create = Create {
            btl: 100,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: ExtendPolicy::OwnerOnly,
            operator: Some(operator_addr),
        };

        let slices = default_slices();
        let entity_key = execute_create(
            &mut state,
            &tx_hash,
            sender,
            current_block,
            0,
            &create,
            &slices,
        )
        .expect("create should succeed");

        let meta_slot = entity_storage_key(&entity_key);
        assert!(state.read_slot(&meta_slot).is_some());

        let content_slot = entity_content_hash_key(&entity_key);
        assert!(state.read_slot(&content_slot).is_some());

        let op_slot = entity_operator_key(&entity_key);
        assert!(state.read_slot(&op_slot).is_some());

        let meta = read_metadata(&state, &entity_key).expect("metadata should exist");
        assert!(meta.has_operator);

        let stored_op = read_operator(&state, &entity_key);
        assert_eq!(stored_op, Some(operator_addr));
    }

    #[test]
    fn execute_update_by_owner_succeeds() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let update = Update {
            entity_key,
            btl: 200,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: Some(ExtendPolicy::AnyoneCanExtend),
            operator: Some(None),
        };

        let slices = default_slices();
        let result = execute_update(&mut state, owner, 1000, &update, &slices);
        assert!(result.is_ok());

        let new_meta = read_metadata(&state, &entity_key).expect("metadata should exist");
        assert_eq!(new_meta.extend_policy, ExtendPolicy::AnyoneCanExtend);
        assert!(!new_meta.has_operator);
        assert!(read_operator(&state, &entity_key).is_none());
    }

    #[test]
    fn execute_update_by_operator_content_only_succeeds() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let update = Update {
            entity_key,
            btl: 200,
            content_type: "text/plain".into(),
            payload: b"updated".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: None,
        };

        let slices = default_slices();
        let result = execute_update(&mut state, operator_addr, 1000, &update, &slices);
        assert!(result.is_ok());
    }

    #[test]
    fn execute_update_operator_rejected_from_changing_extend_policy() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let update = Update {
            entity_key,
            btl: 200,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: Some(ExtendPolicy::AnyoneCanExtend),
            operator: None,
        };

        let slices = default_slices();
        let result = execute_update(&mut state, operator_addr, 1000, &update, &slices);
        assert_eq!(result, Err(GlintError::OperatorCannotChangePermissions));
    }

    #[test]
    fn execute_update_operator_rejected_from_changing_operator() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let update = Update {
            entity_key,
            btl: 200,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: Some(Some(Address::repeat_byte(0xBB))),
        };

        let slices = default_slices();
        let result = execute_update(&mut state, operator_addr, 1000, &update, &slices);
        assert_eq!(result, Err(GlintError::OperatorCannotChangePermissions));
    }

    #[test]
    fn execute_update_stranger_rejected() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let stranger = Address::repeat_byte(0xFF);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            false,
            None,
        );

        let update = Update {
            entity_key,
            btl: 200,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: None,
        };

        let slices = default_slices();
        let result = execute_update(&mut state, stranger, 1000, &update, &slices);
        assert_eq!(result, Err(GlintError::NotAuthorizedToUpdate));
    }

    #[test]
    fn execute_delete_zeroes_both_slots() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);

        let meta = EntityMetadata {
            owner,
            expires_at_block: 1100,
            extend_policy: ExtendPolicy::default(),
            has_operator: false,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));
        let content_slot = entity_content_hash_key(&entity_key);
        state.write_slot(content_slot, U256::from(0xDEAD));

        let result = execute_delete(&mut state, &entity_key, owner);
        assert!(result.is_ok());

        assert!(state.read_slot(&meta_slot).is_none());
        assert!(matches!(
            read_metadata(&state, &entity_key),
            Err(GlintError::EntityNotFound(_))
        ));
    }

    #[test]
    fn execute_delete_wrong_owner_fails() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let attacker = Address::repeat_byte(0xFF);

        let meta = EntityMetadata {
            owner,
            expires_at_block: 1100,
            extend_policy: ExtendPolicy::default(),
            has_operator: false,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let result = execute_delete(&mut state, &entity_key, attacker);
        assert_eq!(result, Err(GlintError::NotOwner));
    }

    #[test]
    fn execute_delete_nonexistent_fails() {
        let mut state = MockState::default();
        let result = execute_delete(&mut state, &B256::repeat_byte(0x01), Address::ZERO);
        assert!(matches!(result, Err(GlintError::EntityNotFound(_))));
    }

    #[test]
    fn execute_delete_by_operator_succeeds() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let result = execute_delete(&mut state, &entity_key, operator_addr);
        assert!(result.is_ok());

        assert!(read_metadata(&state, &entity_key).is_err());
    }

    #[test]
    fn execute_delete_by_stranger_fails() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);
        let stranger = Address::repeat_byte(0xFF);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let result = execute_delete(&mut state, &entity_key, stranger);
        assert_eq!(result, Err(GlintError::NotOwner));
    }

    #[test]
    fn execute_delete_zeros_operator_slot() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let op_slot = entity_operator_key(&entity_key);
        assert!(state.read_slot(&op_slot).is_some());

        let result = execute_delete(&mut state, &entity_key, owner);
        assert!(result.is_ok());

        assert!(state.read_slot(&op_slot).is_none());
    }

    #[test]
    fn execute_extend_updates_expiration() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let current_block = 1000;

        let meta = EntityMetadata {
            owner,
            expires_at_block: 1100,
            extend_policy: ExtendPolicy::default(),
            has_operator: false,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let extend = Extend {
            entity_key,
            additional_blocks: 50,
        };

        let result = execute_extend(&mut state, owner, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());
        let (old_meta, new_expires) = result.expect("extend should succeed");
        assert_eq!(old_meta.expires_at_block, 1100);
        assert_eq!(new_expires, 1150);
    }

    #[test]
    fn execute_extend_exceeds_max_btl_fails() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let current_block = 1000;

        let meta = EntityMetadata {
            owner,
            expires_at_block: current_block + MAX_BTL - 10,
            extend_policy: ExtendPolicy::default(),
            has_operator: false,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let extend = Extend {
            entity_key,
            additional_blocks: 11,
        };

        let result = execute_extend(&mut state, owner, &extend, current_block, MAX_BTL);
        assert_eq!(result, Err(GlintError::ExceedsMaxBtl));
    }

    #[test]
    fn extend_owner_only_by_stranger_fails() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let stranger = Address::repeat_byte(0xFF);
        let current_block = 1000;

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            false,
            None,
        );

        let extend = Extend {
            entity_key,
            additional_blocks: 50,
        };

        let result = execute_extend(&mut state, stranger, &extend, current_block, MAX_BTL);
        assert_eq!(result, Err(GlintError::NotAuthorizedToExtend));
    }

    #[test]
    fn extend_anyone_can_extend_by_stranger_succeeds() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let stranger = Address::repeat_byte(0xFF);
        let current_block = 1000;

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::AnyoneCanExtend,
            false,
            None,
        );

        let extend = Extend {
            entity_key,
            additional_blocks: 50,
        };

        let result = execute_extend(&mut state, stranger, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());
    }

    #[test]
    fn extend_by_operator_succeeds() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);
        let current_block = 1000;

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::OwnerOnly,
            true,
            Some(operator_addr),
        );

        let extend = Extend {
            entity_key,
            additional_blocks: 50,
        };

        let result = execute_extend(&mut state, operator_addr, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());
    }

    #[test]
    fn extend_preserves_flags_byte() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let operator_addr = Address::repeat_byte(0xAA);
        let current_block = 1000;

        seed_entity(
            &mut state,
            &entity_key,
            owner,
            1100,
            ExtendPolicy::AnyoneCanExtend,
            true,
            Some(operator_addr),
        );

        let extend = Extend {
            entity_key,
            additional_blocks: 50,
        };

        let result = execute_extend(&mut state, owner, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());

        let new_meta = read_metadata(&state, &entity_key).expect("metadata should exist");
        assert_eq!(new_meta.extend_policy, ExtendPolicy::AnyoneCanExtend);
        assert!(new_meta.has_operator);
        assert_eq!(new_meta.expires_at_block, 1150);
    }

    #[test]
    fn execute_extend_respects_custom_max_btl() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let current_block = 1000;
        let custom_max_btl: u64 = 500;

        let meta = EntityMetadata {
            owner,
            expires_at_block: current_block + custom_max_btl - 10,
            extend_policy: ExtendPolicy::default(),
            has_operator: false,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let extend = Extend {
            entity_key,
            additional_blocks: 11,
        };

        let result = execute_extend(&mut state, owner, &extend, current_block, custom_max_btl);
        assert_eq!(result, Err(GlintError::ExceedsMaxBtl));

        let result = execute_extend(&mut state, owner, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());
    }
}

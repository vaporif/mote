use alloy_primitives::{Address, B256, U256};
use glint_primitives::{
    entity::{EntityKey, EntityMetadata, derive_entity_key},
    error::GlintError,
    storage::{compute_content_hash_from_raw, entity_content_hash_key, entity_storage_key},
};

pub struct RawContentSlices<'a> {
    pub payload_rlp: &'a [u8],
    pub content_type_rlp: &'a [u8],
    pub string_annotations_rlp: &'a [u8],
    pub numeric_annotations_rlp: &'a [u8],
}

/// Testable interface for trie reads/writes. The real executor
/// goes through revm directly.
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
        expires_at_block: current_block + create.btl,
    };

    let content_hash = compute_content_hash_from_raw(
        slices.payload_rlp,
        slices.content_type_rlp,
        slices.string_annotations_rlp,
        slices.numeric_annotations_rlp,
    );

    write_entity(state, &entity_key, &metadata, content_hash);
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
    if old_meta.owner != sender {
        return Err(GlintError::NotOwner);
    }

    let new_metadata = EntityMetadata {
        owner: old_meta.owner,
        expires_at_block: current_block + update.btl,
    };

    let content_hash = compute_content_hash_from_raw(
        slices.payload_rlp,
        slices.content_type_rlp,
        slices.string_annotations_rlp,
        slices.numeric_annotations_rlp,
    );

    write_entity(state, &update.entity_key, &new_metadata, content_hash);
    Ok(old_meta)
}

pub fn execute_delete(
    state: &mut impl EntityState,
    entity_key: &B256,
    sender: Address,
) -> Result<EntityMetadata, GlintError> {
    let meta = read_metadata(state, entity_key)?;
    if meta.owner != sender {
        return Err(GlintError::NotOwner);
    }
    delete_entity(state, entity_key);
    Ok(meta)
}

pub fn execute_extend(
    state: &mut impl EntityState,
    extend: &glint_primitives::transaction::Extend,
    current_block: u64,
    max_btl: u64,
) -> Result<(EntityMetadata, u64), GlintError> {
    let old_meta = read_metadata(state, &extend.entity_key)?;
    let new_expires = old_meta
        .expires_at_block
        .saturating_add(extend.additional_blocks);

    let max_expires = current_block + max_btl;
    if new_expires > max_expires {
        return Err(GlintError::ExceedsMaxBtl);
    }

    let new_metadata = EntityMetadata {
        owner: old_meta.owner,
        expires_at_block: new_expires,
    };

    let meta_slot = entity_storage_key(&extend.entity_key);
    state.write_slot(meta_slot, U256::from_be_bytes(new_metadata.encode()));

    Ok((old_meta, new_expires))
}

#[cfg(test)]
mod tests {
    use super::*;
    use glint_primitives::constants::MAX_BTL;
    use glint_primitives::transaction::{Create, Extend};
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
        };

        let slices = RawContentSlices {
            payload_rlp: &[0x85, b'h', b'e', b'l', b'l', b'o'],
            content_type_rlp: &[
                0x8a, b't', b'e', b'x', b't', b'/', b'p', b'l', b'a', b'i', b'n',
            ],
            string_annotations_rlp: &[0xc0],
            numeric_annotations_rlp: &[0xc0],
        };
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
        let entity_key = result.unwrap();

        let meta_slot = entity_storage_key(&entity_key);
        assert!(state.read_slot(&meta_slot).is_some());

        let content_slot = entity_content_hash_key(&entity_key);
        assert!(state.read_slot(&content_slot).is_some());
    }

    #[test]
    fn execute_delete_zeroes_both_slots() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);

        let meta = EntityMetadata {
            owner,
            expires_at_block: 1100,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));
        let content_slot = entity_content_hash_key(&entity_key);
        state.write_slot(content_slot, U256::from(0xDEAD));

        let result = execute_delete(&mut state, &entity_key, owner);
        assert!(result.is_ok());

        // Slots are zeroed, not removed (EVM semantics: write U256::ZERO)
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
    fn execute_extend_updates_expiration() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let current_block = 1000;

        let meta = EntityMetadata {
            owner,
            expires_at_block: 1100,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let extend = Extend {
            entity_key,
            additional_blocks: 50,
        };

        let result = execute_extend(&mut state, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());
        let (old_meta, new_expires) = result.unwrap();
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
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let extend = Extend {
            entity_key,
            additional_blocks: 11,
        };

        let result = execute_extend(&mut state, &extend, current_block, MAX_BTL);
        assert_eq!(result, Err(GlintError::ExceedsMaxBtl));
    }

    #[test]
    fn extend_by_non_owner_succeeds() {
        let mut state = MockState::default();
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x42);
        let current_block = 1000;

        let meta = EntityMetadata {
            owner,
            expires_at_block: 1100,
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let extend = Extend {
            entity_key,
            additional_blocks: 50,
        };

        let result = execute_extend(&mut state, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());
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
        };
        let meta_slot = entity_storage_key(&entity_key);
        state.write_slot(meta_slot, U256::from_be_bytes(meta.encode()));

        let extend = Extend {
            entity_key,
            additional_blocks: 11,
        };

        let result = execute_extend(&mut state, &extend, current_block, custom_max_btl);
        assert_eq!(result, Err(GlintError::ExceedsMaxBtl));

        let result = execute_extend(&mut state, &extend, current_block, MAX_BTL);
        assert!(result.is_ok());
    }
}

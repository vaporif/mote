use std::collections::HashSet;

use alloy_primitives::Address;

use crate::annotations::{is_reserved_annotation_key, is_valid_annotation_key};
use crate::config::GlintChainConfig;
use crate::error::GlintError;
use crate::transaction::{
    ChangeOwner, Create, Extend, GlintTransaction, NumericAnnotation, StringAnnotation, Update,
};

const fn validate_btl(btl: u64, max_btl: u64) -> Result<(), GlintError> {
    if btl == 0 || btl > max_btl {
        return Err(GlintError::InvalidBtl);
    }
    Ok(())
}

const fn validate_content_type(ct: &str, max_size: usize) -> Result<(), GlintError> {
    if ct.is_empty() || ct.len() > max_size {
        return Err(GlintError::InvalidContentType);
    }
    Ok(())
}

const fn validate_payload(payload: &[u8], max_size: usize) -> Result<(), GlintError> {
    if payload.len() > max_size {
        return Err(GlintError::PayloadTooLarge);
    }
    Ok(())
}

fn validate_annotation_key(key: &str, max_key_size: usize) -> Result<(), GlintError> {
    if is_reserved_annotation_key(key) {
        return Err(GlintError::ReservedAnnotationKey(key.to_owned()));
    }
    if key.len() > max_key_size {
        return Err(GlintError::AnnotationKeyTooLarge(key.len()));
    }
    if !is_valid_annotation_key(key) {
        return Err(GlintError::InvalidAnnotationKey(key.to_owned()));
    }
    Ok(())
}

fn validate_annotations(
    string_annotations: &[StringAnnotation],
    numeric_annotations: &[NumericAnnotation],
    config: &GlintChainConfig,
) -> Result<(), GlintError> {
    let total = string_annotations.len() + numeric_annotations.len();
    if total > config.max_annotations_per_entity {
        return Err(GlintError::TooManyAnnotations(total));
    }

    let mut seen_keys = HashSet::with_capacity(total);

    for ann in string_annotations {
        validate_annotation_key(&ann.key, config.max_annotation_key_size)?;
        if !seen_keys.insert(&ann.key) {
            return Err(GlintError::DuplicateAnnotationKey(ann.key.clone()));
        }
        if ann.value.len() > config.max_annotation_value_size {
            return Err(GlintError::AnnotationValueTooLarge(ann.value.len()));
        }
    }
    for ann in numeric_annotations {
        validate_annotation_key(&ann.key, config.max_annotation_key_size)?;
        if !seen_keys.insert(&ann.key) {
            return Err(GlintError::DuplicateAnnotationKey(ann.key.clone()));
        }
    }
    Ok(())
}

fn validate_operator(operator: Option<Address>) -> Result<(), GlintError> {
    if operator == Some(Address::ZERO) {
        return Err(GlintError::InvalidOperatorAddress);
    }
    Ok(())
}

pub fn validate_create(c: &Create, config: &GlintChainConfig) -> Result<(), GlintError> {
    validate_btl(c.btl, config.max_btl)?;
    validate_content_type(&c.content_type, config.max_content_type_size)?;
    validate_payload(&c.payload, config.max_payload_size)?;
    validate_annotations(&c.string_annotations, &c.numeric_annotations, config)?;
    validate_operator(c.operator)
}

pub fn validate_update(u: &Update, config: &GlintChainConfig) -> Result<(), GlintError> {
    validate_btl(u.btl, config.max_btl)?;
    validate_content_type(&u.content_type, config.max_content_type_size)?;
    validate_payload(&u.payload, config.max_payload_size)?;
    validate_annotations(&u.string_annotations, &u.numeric_annotations, config)?;
    if let Some(Some(addr)) = u.operator
        && addr == Address::ZERO
    {
        return Err(GlintError::InvalidOperatorAddress);
    }
    Ok(())
}

pub const fn validate_extend(e: &Extend, max_btl: u64) -> Result<(), GlintError> {
    if e.additional_blocks == 0 {
        return Err(GlintError::InvalidExtend);
    }
    if e.additional_blocks > max_btl {
        return Err(GlintError::ExceedsMaxBtl);
    }
    Ok(())
}

pub fn validate_change_owner(co: &ChangeOwner) -> Result<(), GlintError> {
    if co.new_owner.is_none() && co.extend_policy.is_none() && co.operator.is_none() {
        return Err(GlintError::EmptyChangeOwner);
    }
    if co.new_owner == Some(Address::ZERO) {
        return Err(GlintError::InvalidOwnerAddress);
    }
    if let Some(Some(addr)) = co.operator
        && addr == Address::ZERO
    {
        return Err(GlintError::InvalidOperatorAddress);
    }
    Ok(())
}

pub fn validate_transaction(
    tx: &GlintTransaction,
    config: &GlintChainConfig,
) -> Result<(), GlintError> {
    let total = tx.total_operations();
    if total == 0 {
        return Err(GlintError::EmptyTransaction);
    }
    if total > config.max_ops_per_tx {
        return Err(GlintError::TooManyOperations(total));
    }
    for c in &tx.creates {
        validate_create(c, config)?;
    }
    for u in &tx.updates {
        validate_update(u, config)?;
    }
    for e in &tx.extends {
        validate_extend(e, config.max_btl)?;
    }
    for co in &tx.change_owners {
        validate_change_owner(co)?;
    }

    let mut seen_keys = HashSet::with_capacity(
        tx.deletes.len() + tx.updates.len() + tx.extends.len() + tx.change_owners.len(),
    );
    for key in &tx.deletes {
        if !seen_keys.insert(key) {
            return Err(GlintError::DuplicateEntityKey(*key));
        }
    }
    for u in &tx.updates {
        if !seen_keys.insert(&u.entity_key) {
            return Err(GlintError::DuplicateEntityKey(u.entity_key));
        }
    }
    for e in &tx.extends {
        if !seen_keys.insert(&e.entity_key) {
            return Err(GlintError::DuplicateEntityKey(e.entity_key));
        }
    }
    for co in &tx.change_owners {
        if !seen_keys.insert(&co.entity_key) {
            return Err(GlintError::DuplicateEntityKey(co.entity_key));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::*;
    use alloy_primitives::B256;

    fn cfg() -> GlintChainConfig {
        GlintChainConfig::default()
    }

    fn valid_create() -> Create {
        Create {
            btl: 100,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: ExtendPolicy::OwnerOnly,
            operator: None,
        }
    }

    #[test]
    fn valid_create_passes() {
        assert!(validate_create(&valid_create(), &cfg()).is_ok());
    }

    #[test]
    fn zero_btl_rejected() {
        let mut c = valid_create();
        c.btl = 0;
        assert_eq!(validate_create(&c, &cfg()), Err(GlintError::InvalidBtl));
    }

    #[test]
    fn btl_over_max_rejected() {
        let mut c = valid_create();
        c.btl = crate::constants::MAX_BTL + 1;
        assert_eq!(validate_create(&c, &cfg()), Err(GlintError::InvalidBtl));
    }

    #[test]
    fn empty_content_type_rejected() {
        let mut c = valid_create();
        c.content_type = String::new();
        assert_eq!(
            validate_create(&c, &cfg()),
            Err(GlintError::InvalidContentType)
        );
    }

    #[test]
    fn content_type_too_long_rejected() {
        let mut c = valid_create();
        c.content_type = "x".repeat(129);
        assert_eq!(
            validate_create(&c, &cfg()),
            Err(GlintError::InvalidContentType)
        );
    }

    #[test]
    fn payload_too_large_rejected() {
        let mut c = valid_create();
        c.payload = vec![0u8; crate::constants::MAX_PAYLOAD_SIZE + 1];
        assert_eq!(
            validate_create(&c, &cfg()),
            Err(GlintError::PayloadTooLarge)
        );
    }

    #[test]
    fn reserved_annotation_key_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotation {
            key: "$owner".into(),
            value: "x".into(),
        }];
        assert!(matches!(
            validate_create(&c, &cfg()),
            Err(GlintError::ReservedAnnotationKey(_))
        ));
    }

    #[test]
    fn invalid_annotation_key_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotation {
            key: "123bad".into(),
            value: "x".into(),
        }];
        assert!(matches!(
            validate_create(&c, &cfg()),
            Err(GlintError::InvalidAnnotationKey(_))
        ));
    }

    #[test]
    fn too_many_annotations_rejected() {
        let mut c = valid_create();
        c.string_annotations = (0..65)
            .map(|i| StringAnnotation {
                key: format!("key_{i}"),
                value: "v".into(),
            })
            .collect();
        assert!(matches!(
            validate_create(&c, &cfg()),
            Err(GlintError::TooManyAnnotations(_))
        ));
    }

    #[test]
    fn annotation_value_too_large_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotation {
            key: "big".into(),
            value: "x".repeat(crate::constants::MAX_ANNOTATION_VALUE_SIZE + 1),
        }];
        assert!(matches!(
            validate_create(&c, &cfg()),
            Err(GlintError::AnnotationValueTooLarge(_))
        ));
    }

    #[test]
    fn annotation_key_too_large_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotation {
            key: "k".repeat(crate::constants::MAX_ANNOTATION_KEY_SIZE + 1),
            value: "v".into(),
        }];
        assert!(matches!(
            validate_create(&c, &cfg()),
            Err(GlintError::AnnotationKeyTooLarge(_))
        ));
    }

    #[test]
    fn duplicate_annotation_key_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![
            StringAnnotation {
                key: "dup".into(),
                value: "a".into(),
            },
            StringAnnotation {
                key: "dup".into(),
                value: "b".into(),
            },
        ];
        assert!(matches!(
            validate_create(&c, &cfg()),
            Err(GlintError::DuplicateAnnotationKey(_))
        ));
    }

    #[test]
    fn duplicate_key_across_annotation_types_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotation {
            key: "shared".into(),
            value: "a".into(),
        }];
        c.numeric_annotations = vec![NumericAnnotation {
            key: "shared".into(),
            value: 1,
        }];
        assert!(matches!(
            validate_create(&c, &cfg()),
            Err(GlintError::DuplicateAnnotationKey(_))
        ));
    }

    #[test]
    fn extend_zero_blocks_rejected() {
        let tx = GlintTransaction {
            creates: vec![],
            updates: vec![],
            deletes: vec![],
            extends: vec![Extend {
                entity_key: B256::repeat_byte(0x01),
                additional_blocks: 0,
            }],
            change_owners: vec![],
        };
        assert!(matches!(
            validate_transaction(&tx, &cfg()),
            Err(GlintError::InvalidExtend)
        ));
    }

    #[test]
    fn too_many_operations_rejected() {
        let tx = GlintTransaction {
            creates: (0..101).map(|_| valid_create()).collect(),
            updates: vec![],
            deletes: vec![],
            extends: vec![],
            change_owners: vec![],
        };
        assert!(matches!(
            validate_transaction(&tx, &cfg()),
            Err(GlintError::TooManyOperations(_))
        ));
    }

    #[test]
    fn empty_transaction_rejected() {
        let tx = GlintTransaction {
            creates: vec![],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
            change_owners: vec![],
        };
        assert_eq!(
            validate_transaction(&tx, &cfg()),
            Err(GlintError::EmptyTransaction)
        );
    }

    #[test]
    fn valid_transaction_passes() {
        let tx = GlintTransaction {
            creates: vec![valid_create()],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
            change_owners: vec![],
        };
        assert!(validate_transaction(&tx, &cfg()).is_ok());
    }

    #[test]
    fn create_zero_operator_rejected() {
        let mut c = valid_create();
        c.operator = Some(Address::ZERO);
        assert_eq!(
            validate_create(&c, &cfg()),
            Err(GlintError::InvalidOperatorAddress)
        );
    }

    #[test]
    fn create_valid_operator_passes() {
        let mut c = valid_create();
        c.operator = Some(Address::repeat_byte(0x42));
        assert!(validate_create(&c, &cfg()).is_ok());
    }

    #[test]
    fn update_zero_operator_rejected() {
        let u = Update {
            entity_key: B256::repeat_byte(0x01),
            btl: 100,
            content_type: "text/plain".into(),
            payload: b"test".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: Some(Some(Address::ZERO)),
        };
        assert_eq!(
            validate_update(&u, &cfg()),
            Err(GlintError::InvalidOperatorAddress)
        );
    }

    #[test]
    fn update_valid_operator_passes() {
        let u = Update {
            entity_key: B256::repeat_byte(0x01),
            btl: 100,
            content_type: "text/plain".into(),
            payload: b"test".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: Some(Some(Address::repeat_byte(0x42))),
        };
        assert!(validate_update(&u, &cfg()).is_ok());
    }

    #[test]
    fn update_remove_operator_passes() {
        let u = Update {
            entity_key: B256::repeat_byte(0x01),
            btl: 100,
            content_type: "text/plain".into(),
            payload: b"test".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: Some(None),
        };
        assert!(validate_update(&u, &cfg()).is_ok());
    }

    #[test]
    fn duplicate_delete_keys_rejected() {
        let key = B256::repeat_byte(0x01);
        let tx = GlintTransaction {
            creates: vec![],
            updates: vec![],
            deletes: vec![key, key],
            extends: vec![],
            change_owners: vec![],
        };
        assert!(matches!(
            validate_transaction(&tx, &cfg()),
            Err(GlintError::DuplicateEntityKey(_))
        ));
    }

    #[test]
    fn change_owner_empty_rejected() {
        let co = ChangeOwner {
            entity_key: B256::repeat_byte(0x01),
            new_owner: None,
            extend_policy: None,
            operator: None,
        };
        assert_eq!(
            validate_change_owner(&co),
            Err(GlintError::EmptyChangeOwner)
        );
    }

    #[test]
    fn change_owner_zero_new_owner_rejected() {
        let co = ChangeOwner {
            entity_key: B256::repeat_byte(0x01),
            new_owner: Some(Address::ZERO),
            extend_policy: None,
            operator: None,
        };
        assert_eq!(
            validate_change_owner(&co),
            Err(GlintError::InvalidOwnerAddress)
        );
    }

    #[test]
    fn change_owner_zero_operator_rejected() {
        let co = ChangeOwner {
            entity_key: B256::repeat_byte(0x01),
            new_owner: None,
            extend_policy: None,
            operator: Some(Some(Address::ZERO)),
        };
        assert_eq!(
            validate_change_owner(&co),
            Err(GlintError::InvalidOperatorAddress)
        );
    }

    #[test]
    fn change_owner_valid_passes() {
        let co = ChangeOwner {
            entity_key: B256::repeat_byte(0x01),
            new_owner: Some(Address::repeat_byte(0x42)),
            extend_policy: None,
            operator: None,
        };
        assert!(validate_change_owner(&co).is_ok());
    }

    #[test]
    fn change_owner_duplicate_key_rejected() {
        let key = B256::repeat_byte(0x01);
        let tx = GlintTransaction {
            creates: vec![],
            updates: vec![],
            deletes: vec![key],
            extends: vec![],
            change_owners: vec![ChangeOwner {
                entity_key: key,
                new_owner: Some(Address::repeat_byte(0x42)),
                extend_policy: None,
                operator: None,
            }],
        };
        assert!(matches!(
            validate_transaction(&tx, &cfg()),
            Err(GlintError::DuplicateEntityKey(_))
        ));
    }

    #[test]
    fn delete_and_update_same_key_rejected() {
        let key = B256::repeat_byte(0x01);
        let tx = GlintTransaction {
            creates: vec![],
            updates: vec![Update {
                entity_key: key,
                btl: 100,
                content_type: "text/plain".into(),
                payload: b"test".to_vec(),
                string_annotations: vec![],
                numeric_annotations: vec![],
                extend_policy: None,
                operator: None,
            }],
            deletes: vec![key],
            extends: vec![],
            change_owners: vec![],
        };
        assert!(matches!(
            validate_transaction(&tx, &cfg()),
            Err(GlintError::DuplicateEntityKey(_))
        ));
    }
}

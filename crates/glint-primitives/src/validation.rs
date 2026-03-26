use std::collections::HashSet;

use alloy_primitives::Address;

use crate::annotations::{is_reserved_annotation_key, is_valid_annotation_key};
use crate::constants::{
    MAX_ANNOTATION_KEY_SIZE, MAX_ANNOTATION_VALUE_SIZE, MAX_ANNOTATIONS_PER_ENTITY, MAX_BTL,
    MAX_CONTENT_TYPE_SIZE, MAX_OPS_PER_TX, MAX_PAYLOAD_SIZE,
};
use crate::error::GlintError;
use crate::transaction::{
    Create, Extend, GlintTransaction, NumericAnnotationWire, StringAnnotationWire, Update,
};

const fn validate_btl(btl: u64) -> Result<(), GlintError> {
    if btl == 0 || btl > MAX_BTL {
        return Err(GlintError::InvalidBtl);
    }
    Ok(())
}

const fn validate_content_type(ct: &str) -> Result<(), GlintError> {
    if ct.is_empty() || ct.len() > MAX_CONTENT_TYPE_SIZE {
        return Err(GlintError::InvalidContentType);
    }
    Ok(())
}

const fn validate_payload(payload: &[u8]) -> Result<(), GlintError> {
    if payload.len() > MAX_PAYLOAD_SIZE {
        return Err(GlintError::PayloadTooLarge);
    }
    Ok(())
}

fn validate_annotation_key(key: &str) -> Result<(), GlintError> {
    if is_reserved_annotation_key(key) {
        return Err(GlintError::ReservedAnnotationKey(key.to_owned()));
    }
    if !is_valid_annotation_key(key) {
        return Err(GlintError::InvalidAnnotationKey(key.to_owned()));
    }
    if key.len() > MAX_ANNOTATION_KEY_SIZE {
        return Err(GlintError::AnnotationKeyTooLarge(key.len()));
    }
    Ok(())
}

fn validate_annotations(
    string_annotations: &[StringAnnotationWire],
    numeric_annotations: &[NumericAnnotationWire],
) -> Result<(), GlintError> {
    let total = string_annotations.len() + numeric_annotations.len();
    if total > MAX_ANNOTATIONS_PER_ENTITY {
        return Err(GlintError::TooManyAnnotations(total));
    }

    let mut seen_keys = HashSet::with_capacity(total);

    for ann in string_annotations {
        validate_annotation_key(&ann.key)?;
        if !seen_keys.insert(&ann.key) {
            return Err(GlintError::DuplicateAnnotationKey(ann.key.clone()));
        }
        if ann.value.len() > MAX_ANNOTATION_VALUE_SIZE {
            return Err(GlintError::AnnotationValueTooLarge(ann.value.len()));
        }
    }
    for ann in numeric_annotations {
        validate_annotation_key(&ann.key)?;
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

pub fn validate_create(c: &Create) -> Result<(), GlintError> {
    validate_btl(c.btl)?;
    validate_content_type(&c.content_type)?;
    validate_payload(&c.payload)?;
    validate_annotations(&c.string_annotations, &c.numeric_annotations)?;
    validate_operator(c.operator)
}

pub fn validate_update(u: &Update) -> Result<(), GlintError> {
    validate_btl(u.btl)?;
    validate_content_type(&u.content_type)?;
    validate_payload(&u.payload)?;
    validate_annotations(&u.string_annotations, &u.numeric_annotations)?;
    if let Some(Some(addr)) = u.operator
        && addr == Address::ZERO
    {
        return Err(GlintError::InvalidOperatorAddress);
    }
    Ok(())
}

pub const fn validate_extend(e: &Extend) -> Result<(), GlintError> {
    if e.additional_blocks == 0 {
        return Err(GlintError::InvalidExtend);
    }
    Ok(())
}

pub fn validate_transaction(tx: &GlintTransaction) -> Result<(), GlintError> {
    let total = tx.total_operations();
    if total == 0 {
        return Err(GlintError::EmptyTransaction);
    }
    if total > MAX_OPS_PER_TX {
        return Err(GlintError::TooManyOperations(total));
    }
    for c in &tx.creates {
        validate_create(c)?;
    }
    for u in &tx.updates {
        validate_update(u)?;
    }
    for e in &tx.extends {
        validate_extend(e)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::*;
    use alloy_primitives::B256;

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
        assert!(validate_create(&valid_create()).is_ok());
    }

    #[test]
    fn zero_btl_rejected() {
        let mut c = valid_create();
        c.btl = 0;
        assert_eq!(validate_create(&c), Err(GlintError::InvalidBtl));
    }

    #[test]
    fn btl_over_max_rejected() {
        let mut c = valid_create();
        c.btl = crate::constants::MAX_BTL + 1;
        assert_eq!(validate_create(&c), Err(GlintError::InvalidBtl));
    }

    #[test]
    fn empty_content_type_rejected() {
        let mut c = valid_create();
        c.content_type = String::new();
        assert_eq!(validate_create(&c), Err(GlintError::InvalidContentType));
    }

    #[test]
    fn content_type_too_long_rejected() {
        let mut c = valid_create();
        c.content_type = "x".repeat(129);
        assert_eq!(validate_create(&c), Err(GlintError::InvalidContentType));
    }

    #[test]
    fn payload_too_large_rejected() {
        let mut c = valid_create();
        c.payload = vec![0u8; crate::constants::MAX_PAYLOAD_SIZE + 1];
        assert_eq!(validate_create(&c), Err(GlintError::PayloadTooLarge));
    }

    #[test]
    fn reserved_annotation_key_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotationWire {
            key: "$owner".into(),
            value: "x".into(),
        }];
        assert!(matches!(
            validate_create(&c),
            Err(GlintError::ReservedAnnotationKey(_))
        ));
    }

    #[test]
    fn invalid_annotation_key_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotationWire {
            key: "123bad".into(),
            value: "x".into(),
        }];
        assert!(matches!(
            validate_create(&c),
            Err(GlintError::InvalidAnnotationKey(_))
        ));
    }

    #[test]
    fn too_many_annotations_rejected() {
        let mut c = valid_create();
        c.string_annotations = (0..65)
            .map(|i| StringAnnotationWire {
                key: format!("key_{i}"),
                value: "v".into(),
            })
            .collect();
        assert!(matches!(
            validate_create(&c),
            Err(GlintError::TooManyAnnotations(_))
        ));
    }

    #[test]
    fn annotation_value_too_large_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotationWire {
            key: "big".into(),
            value: "x".repeat(crate::constants::MAX_ANNOTATION_VALUE_SIZE + 1),
        }];
        assert!(matches!(
            validate_create(&c),
            Err(GlintError::AnnotationValueTooLarge(_))
        ));
    }

    #[test]
    fn annotation_key_too_large_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotationWire {
            key: "k".repeat(crate::constants::MAX_ANNOTATION_KEY_SIZE + 1),
            value: "v".into(),
        }];
        assert!(matches!(
            validate_create(&c),
            Err(GlintError::AnnotationKeyTooLarge(_))
        ));
    }

    #[test]
    fn duplicate_annotation_key_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![
            StringAnnotationWire {
                key: "dup".into(),
                value: "a".into(),
            },
            StringAnnotationWire {
                key: "dup".into(),
                value: "b".into(),
            },
        ];
        assert!(matches!(
            validate_create(&c),
            Err(GlintError::DuplicateAnnotationKey(_))
        ));
    }

    #[test]
    fn duplicate_key_across_annotation_types_rejected() {
        let mut c = valid_create();
        c.string_annotations = vec![StringAnnotationWire {
            key: "shared".into(),
            value: "a".into(),
        }];
        c.numeric_annotations = vec![NumericAnnotationWire {
            key: "shared".into(),
            value: 1,
        }];
        assert!(matches!(
            validate_create(&c),
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
        };
        assert!(matches!(
            validate_transaction(&tx),
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
        };
        assert!(matches!(
            validate_transaction(&tx),
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
        };
        assert_eq!(validate_transaction(&tx), Err(GlintError::EmptyTransaction));
    }

    #[test]
    fn valid_transaction_passes() {
        let tx = GlintTransaction {
            creates: vec![valid_create()],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };
        assert!(validate_transaction(&tx).is_ok());
    }

    #[test]
    fn create_zero_operator_rejected() {
        let mut c = valid_create();
        c.operator = Some(Address::ZERO);
        assert_eq!(validate_create(&c), Err(GlintError::InvalidOperatorAddress));
    }

    #[test]
    fn create_valid_operator_passes() {
        let mut c = valid_create();
        c.operator = Some(Address::repeat_byte(0x42));
        assert!(validate_create(&c).is_ok());
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
        assert_eq!(validate_update(&u), Err(GlintError::InvalidOperatorAddress));
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
        assert!(validate_update(&u).is_ok());
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
        assert!(validate_update(&u).is_ok());
    }
}

use glint_primitives::transaction::{
    Create, Extend, GlintTransaction, NumericAnnotationWire, StringAnnotationWire, Update,
};

use crate::entity::{CreateEntity, DeleteEntity, ExtendEntity, UpdateEntity};

impl From<&CreateEntity> for Create {
    fn from(e: &CreateEntity) -> Self {
        Self {
            btl: e.btl,
            content_type: e.content_type.clone(),
            payload: e.payload.clone(),
            string_annotations: e
                .string_annotations
                .iter()
                .map(|(k, v)| StringAnnotationWire {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            numeric_annotations: e
                .numeric_annotations
                .iter()
                .map(|(k, v)| NumericAnnotationWire {
                    key: k.clone(),
                    value: *v,
                })
                .collect(),
            extend_policy: e.extend_policy,
            operator: e.operator,
        }
    }
}

impl From<&UpdateEntity> for Update {
    fn from(e: &UpdateEntity) -> Self {
        Self {
            entity_key: e.entity_key,
            btl: e.btl,
            content_type: e.content_type.clone(),
            payload: e.payload.clone(),
            string_annotations: e
                .string_annotations
                .iter()
                .map(|(k, v)| StringAnnotationWire {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            numeric_annotations: e
                .numeric_annotations
                .iter()
                .map(|(k, v)| NumericAnnotationWire {
                    key: k.clone(),
                    value: *v,
                })
                .collect(),
            extend_policy: e.extend_policy,
            operator: e.operator,
        }
    }
}

impl From<&ExtendEntity> for Extend {
    fn from(e: &ExtendEntity) -> Self {
        Self {
            entity_key: e.entity_key,
            additional_blocks: e.additional_blocks,
        }
    }
}

pub fn build_glint_transaction(
    creates: &[CreateEntity],
    updates: &[UpdateEntity],
    deletes: &[DeleteEntity],
    extends: &[ExtendEntity],
) -> GlintTransaction {
    GlintTransaction {
        creates: creates.iter().map(Create::from).collect(),
        updates: updates.iter().map(Update::from).collect(),
        deletes: deletes.iter().map(|d| d.entity_key).collect(),
        extends: extends.iter().map(Extend::from).collect(),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};
    use alloy_rlp::{Decodable, Encodable};
    use glint_primitives::transaction::{ExtendPolicy, GlintTransaction};

    use super::*;

    #[test]
    fn create_roundtrip() {
        let builder = CreateEntity::new("application/json", b"{\"key\":\"value\"}", 100)
            .operator(Address::repeat_byte(0x99))
            .anyone_can_extend(true)
            .string_annotation("app", "test")
            .numeric_annotation("priority", 1);

        let tx = build_glint_transaction(&[builder], &[], &[], &[]);

        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice())
            .expect("should decode roundtripped transaction");
        assert_eq!(tx, decoded);
        assert_eq!(decoded.creates.len(), 1);
        assert_eq!(decoded.creates[0].content_type, "application/json");
        assert_eq!(
            decoded.creates[0].extend_policy,
            ExtendPolicy::AnyoneCanExtend
        );
        assert_eq!(
            decoded.creates[0].operator,
            Some(Address::repeat_byte(0x99))
        );
        assert_eq!(decoded.creates[0].string_annotations.len(), 1);
        assert_eq!(decoded.creates[0].numeric_annotations.len(), 1);
    }

    #[test]
    fn update_roundtrip() {
        let key = B256::repeat_byte(0x01);
        let builder = UpdateEntity::new(key, "text/plain", b"updated", 200)
            .extend_policy(ExtendPolicy::OwnerOnly)
            .operator(Some(Address::repeat_byte(0xAB)));

        let tx = build_glint_transaction(&[], &[builder], &[], &[]);

        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice())
            .expect("should decode roundtripped transaction");
        assert_eq!(tx, decoded);
        assert_eq!(decoded.updates[0].entity_key, key);
    }

    #[test]
    fn extend_roundtrip() {
        let key = B256::repeat_byte(0x03);
        let builder = ExtendEntity::new(key, 50);

        let tx = build_glint_transaction(&[], &[], &[], &[builder]);

        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice())
            .expect("should decode roundtripped transaction");
        assert_eq!(tx, decoded);
        assert_eq!(decoded.extends[0].entity_key, key);
        assert_eq!(decoded.extends[0].additional_blocks, 50);
    }

    #[test]
    fn delete_roundtrip() {
        let key = B256::repeat_byte(0x04);
        let builder = DeleteEntity::new(key);

        let tx = build_glint_transaction(&[], &[], &[builder], &[]);

        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice())
            .expect("should decode roundtripped transaction");
        assert_eq!(tx, decoded);
        assert_eq!(decoded.deletes[0], key);
    }

    #[test]
    fn mixed_operations_roundtrip() {
        let create = CreateEntity::new("text/plain", b"hello", 100);
        let update = UpdateEntity::new(B256::repeat_byte(0x01), "text/plain", b"upd", 200);
        let delete = DeleteEntity::new(B256::repeat_byte(0x02));
        let extend = ExtendEntity::new(B256::repeat_byte(0x03), 42);

        let tx = build_glint_transaction(&[create], &[update], &[delete], &[extend]);

        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice())
            .expect("should decode roundtripped transaction");
        assert_eq!(tx, decoded);
        assert_eq!(decoded.creates.len(), 1);
        assert_eq!(decoded.updates.len(), 1);
        assert_eq!(decoded.deletes.len(), 1);
        assert_eq!(decoded.extends.len(), 1);
    }
}

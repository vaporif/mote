use glint_primitives::transaction::{
    Create, Extend, GlintTransaction, NumericAnnotationWire, StringAnnotationWire, Update,
};

use crate::entity::{CreateEntity, DeleteEntity, ExtendEntity, UpdateEntity};

fn to_string_wire(anns: &[(String, String)]) -> Vec<StringAnnotationWire> {
    anns.iter()
        .map(|(k, v)| StringAnnotationWire {
            key: k.clone(),
            value: v.clone(),
        })
        .collect()
}

fn to_numeric_wire(anns: &[(String, u64)]) -> Vec<NumericAnnotationWire> {
    anns.iter()
        .map(|(k, v)| NumericAnnotationWire {
            key: k.clone(),
            value: *v,
        })
        .collect()
}

impl From<&CreateEntity> for Create {
    fn from(e: &CreateEntity) -> Self {
        Self {
            btl: e.btl,
            content_type: e.content_type.clone(),
            payload: e.payload.clone(),
            string_annotations: to_string_wire(&e.string_annotations),
            numeric_annotations: to_numeric_wire(&e.numeric_annotations),
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
            string_annotations: to_string_wire(&e.string_annotations),
            numeric_annotations: to_numeric_wire(&e.numeric_annotations),
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
) -> eyre::Result<GlintTransaction> {
    let tx = GlintTransaction {
        creates: creates.iter().map(Create::from).collect(),
        updates: updates.iter().map(Update::from).collect(),
        deletes: deletes.iter().map(|d| d.entity_key).collect(),
        extends: extends.iter().map(Extend::from).collect(),
    };
    glint_primitives::validation::validate_transaction(
        &tx,
        &glint_primitives::config::GlintChainConfig::default(),
    )?;
    Ok(tx)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};
    use alloy_rlp::{Decodable, Encodable};
    use glint_primitives::transaction::{ExtendPolicy, GlintTransaction};

    use super::*;

    fn roundtrip(tx: &GlintTransaction) -> GlintTransaction {
        let mut buf = Vec::new();
        tx.encode(&mut buf);
        GlintTransaction::decode(&mut buf.as_slice()).unwrap()
    }

    #[test]
    fn create_with_all_fields_roundtrips() {
        let builder = CreateEntity::new("application/json", b"{\"key\":\"value\"}", 100)
            .operator(Address::repeat_byte(0x99))
            .anyone_can_extend(true)
            .string_annotation("app", "test")
            .numeric_annotation("priority", 1);

        let tx = build_glint_transaction(&[builder], &[], &[], &[]).unwrap();
        let decoded = roundtrip(&tx);
        assert_eq!(tx, decoded);
        assert_eq!(
            decoded.creates[0].extend_policy,
            ExtendPolicy::AnyoneCanExtend
        );
        assert_eq!(
            decoded.creates[0].operator,
            Some(Address::repeat_byte(0x99))
        );
    }

    #[test]
    fn mixed_ops_roundtrip() {
        let create = CreateEntity::new("text/plain", b"hello", 100);
        let update = UpdateEntity::new(B256::repeat_byte(0x01), "text/plain", b"upd", 200)
            .extend_policy(ExtendPolicy::OwnerOnly)
            .operator(Some(Address::repeat_byte(0xAB)));
        let delete = DeleteEntity::new(B256::repeat_byte(0x02));
        let extend = ExtendEntity::new(B256::repeat_byte(0x03), 42);

        let tx = build_glint_transaction(&[create], &[update], &[delete], &[extend]).unwrap();
        assert_eq!(tx, roundtrip(&tx));
    }
}

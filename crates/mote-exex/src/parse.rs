use alloy_primitives::{Address, B256, Bytes, Log};
use alloy_sol_types::SolEvent;
use mote_primitives::constants::PROCESSOR_ADDRESS;
use mote_primitives::events::{
    EntityCreated, EntityDeleted, EntityExpired, EntityExtended, EntityUpdated,
};
use tracing::warn;

#[derive(Debug, Clone)]
pub enum EntityEvent {
    Created {
        entity_key: B256,
        owner: Address,
        expires_at: u64,
        content_type: String,
        payload: Bytes,
        string_keys: Vec<String>,
        string_values: Vec<String>,
        numeric_keys: Vec<String>,
        numeric_values: Vec<u64>,
    },
    Updated {
        entity_key: B256,
        owner: Address,
        old_expires_at: u64,
        new_expires_at: u64,
        content_type: String,
        payload: Bytes,
        string_keys: Vec<String>,
        string_values: Vec<String>,
        numeric_keys: Vec<String>,
        numeric_values: Vec<u64>,
    },
    Deleted {
        entity_key: B256,
        owner: Address,
    },
    Expired {
        entity_key: B256,
        owner: Address,
    },
    Extended {
        entity_key: B256,
        old_expires_at: u64,
        new_expires_at: u64,
    },
}

pub fn parse_log(log: &Log) -> eyre::Result<Option<EntityEvent>> {
    if log.address != PROCESSOR_ADDRESS {
        return Ok(None);
    }

    let selector = *log
        .data
        .topics()
        .first()
        .ok_or_else(|| eyre::eyre!("log has no topics"))?;

    match selector {
        s if s == EntityCreated::SIGNATURE_HASH => {
            let d = EntityCreated::decode_log(log)?.data;
            validate_annotations(
                &d.string_annotation_keys,
                &d.string_annotation_values,
                &d.numeric_annotation_keys,
                &d.numeric_annotation_values,
            )?;
            Ok(Some(EntityEvent::Created {
                entity_key: d.entity_key,
                owner: d.owner,
                expires_at: d.expires_at,
                content_type: d.content_type,
                payload: d.payload,
                string_keys: d.string_annotation_keys,
                string_values: d.string_annotation_values,
                numeric_keys: d.numeric_annotation_keys,
                numeric_values: d.numeric_annotation_values,
            }))
        }
        s if s == EntityUpdated::SIGNATURE_HASH => {
            let d = EntityUpdated::decode_log(log)?.data;
            validate_annotations(
                &d.string_annotation_keys,
                &d.string_annotation_values,
                &d.numeric_annotation_keys,
                &d.numeric_annotation_values,
            )?;
            Ok(Some(EntityEvent::Updated {
                entity_key: d.entity_key,
                owner: d.owner,
                old_expires_at: d.old_expires_at,
                new_expires_at: d.new_expires_at,
                content_type: d.content_type,
                payload: d.payload,
                string_keys: d.string_annotation_keys,
                string_values: d.string_annotation_values,
                numeric_keys: d.numeric_annotation_keys,
                numeric_values: d.numeric_annotation_values,
            }))
        }
        s if s == EntityDeleted::SIGNATURE_HASH => {
            let d = EntityDeleted::decode_log(log)?.data;
            Ok(Some(EntityEvent::Deleted {
                entity_key: d.entity_key,
                owner: d.owner,
            }))
        }
        s if s == EntityExpired::SIGNATURE_HASH => {
            let d = EntityExpired::decode_log(log)?.data;
            Ok(Some(EntityEvent::Expired {
                entity_key: d.entity_key,
                owner: d.owner,
            }))
        }
        s if s == EntityExtended::SIGNATURE_HASH => {
            let d = EntityExtended::decode_log(log)?.data;
            Ok(Some(EntityEvent::Extended {
                entity_key: d.entity_key,
                old_expires_at: d.old_expires_at,
                new_expires_at: d.new_expires_at,
            }))
        }
        _ => {
            warn!(?selector, "unknown event selector, skipping log");
            Ok(None)
        }
    }
}

fn validate_annotations(
    str_keys: &[String],
    str_values: &[String],
    num_keys: &[String],
    num_values: &[u64],
) -> eyre::Result<()> {
    eyre::ensure!(
        str_keys.len() == str_values.len(),
        "string annotation key/value length mismatch"
    );
    eyre::ensure!(
        num_keys.len() == num_values.len(),
        "numeric annotation key/value length mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes};
    use mote_primitives::constants::PROCESSOR_ADDRESS;
    use mote_primitives::events::{
        EntityCreated, EntityDeleted, EntityExpired, EntityExtended, EntityUpdated, LogAnnotations,
    };

    fn empty_annotations() -> LogAnnotations {
        LogAnnotations {
            string_keys: vec![],
            string_values: vec![],
            numeric_keys: vec![],
            numeric_values: vec![],
        }
    }

    fn make_created_log() -> alloy_primitives::Log {
        EntityCreated::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x01),
            Address::repeat_byte(0x02),
            100,
            "text/plain".into(),
            Bytes::from_static(b"hello"),
            LogAnnotations {
                string_keys: vec!["k1".into()],
                string_values: vec!["v1".into()],
                numeric_keys: vec!["n1".into()],
                numeric_values: vec![42],
            },
        )
    }

    #[test]
    fn created_roundtrip() {
        let log = make_created_log();
        let event = parse_log(&log).unwrap().unwrap();
        match event {
            EntityEvent::Created {
                entity_key,
                owner,
                expires_at,
                content_type,
                payload,
                ..
            } => {
                assert_eq!(entity_key, B256::repeat_byte(0x01));
                assert_eq!(owner, Address::repeat_byte(0x02));
                assert_eq!(expires_at, 100);
                assert_eq!(content_type, "text/plain");
                assert_eq!(payload.as_ref(), b"hello");
            }
            _ => panic!("expected Created"),
        }
    }

    #[test]
    fn updated_preserves_both_expiry_values() {
        let log = EntityUpdated::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x01),
            Address::repeat_byte(0x02),
            (50, 100),
            "application/json".into(),
            Bytes::from_static(b"updated"),
            LogAnnotations {
                string_keys: vec!["k1".into()],
                string_values: vec!["v1".into()],
                numeric_keys: vec![],
                numeric_values: vec![],
            },
        );
        let EntityEvent::Updated {
            old_expires_at,
            new_expires_at,
            content_type,
            payload,
            ..
        } = parse_log(&log).unwrap().unwrap()
        else {
            panic!("expected Updated");
        };
        assert_eq!(old_expires_at, 50);
        assert_eq!(new_expires_at, 100);
        assert_eq!(content_type, "application/json");
        assert_eq!(payload.as_ref(), b"updated");
    }

    #[test]
    fn deleted_and_expired_are_simple() {
        let del_log = EntityDeleted::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x03),
            Address::repeat_byte(0x04),
        );
        assert!(matches!(
            parse_log(&del_log).unwrap().unwrap(),
            EntityEvent::Deleted { entity_key, .. } if entity_key == B256::repeat_byte(0x03)
        ));

        let exp_log = EntityExpired::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x06),
            Address::repeat_byte(0x07),
        );
        assert!(matches!(
            parse_log(&exp_log).unwrap().unwrap(),
            EntityEvent::Expired { owner, .. } if owner == Address::repeat_byte(0x07)
        ));
    }

    #[test]
    fn extended_has_no_owner() {
        let log = EntityExtended::new_log(PROCESSOR_ADDRESS, B256::repeat_byte(0x05), 10, 20);
        let EntityEvent::Extended {
            old_expires_at,
            new_expires_at,
            ..
        } = parse_log(&log).unwrap().unwrap()
        else {
            panic!("expected Extended");
        };
        assert_eq!(old_expires_at, 10);
        assert_eq!(new_expires_at, 20);
    }

    #[test]
    fn wrong_address_skipped() {
        let log = EntityCreated::new_log(
            Address::repeat_byte(0xFF),
            B256::repeat_byte(0x01),
            Address::repeat_byte(0x02),
            100,
            "text/plain".into(),
            Bytes::from_static(b"hello"),
            empty_annotations(),
        );
        assert!(parse_log(&log).unwrap().is_none());
    }

    #[test]
    fn unknown_selector_skipped() {
        let mut log = make_created_log();
        log.data.topics_mut()[0] = B256::repeat_byte(0xFF);
        assert!(parse_log(&log).unwrap().is_none());
    }

    #[test]
    fn annotation_key_value_mismatch() {
        let log = EntityCreated::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x01),
            Address::repeat_byte(0x02),
            100,
            "text/plain".into(),
            Bytes::from_static(b"hello"),
            LogAnnotations {
                string_keys: vec!["k1".into(), "k2".into()],
                string_values: vec!["v1".into()], // mismatch
                numeric_keys: vec![],
                numeric_values: vec![],
            },
        );
        assert!(parse_log(&log).is_err());
    }
}

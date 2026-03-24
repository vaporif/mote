use alloy_primitives::{Address, B256, Bytes, Log};
use alloy_sol_types::SolEvent;
use mote_primitives::constants::PROCESSOR_ADDRESS;
use mote_primitives::events::{
    EntityCreated, EntityDeleted, EntityExpired, EntityExtended, EntityUpdated,
};

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

/// Parse a single log into an [`EntityEvent`].
///
/// Returns `Ok(None)` if the log address doesn't match [`PROCESSOR_ADDRESS`].
/// Returns `Err` if the selector is unrecognized or decoding fails.
#[must_use = "discarding a parse result silently loses events"]
pub fn parse_log(log: &Log) -> eyre::Result<Option<EntityEvent>> {
    if log.address != PROCESSOR_ADDRESS {
        return Ok(None);
    }

    let selector = log
        .data
        .topics()
        .first()
        .ok_or_else(|| eyre::eyre!("log has no topics"))?;

    if *selector == EntityCreated::SIGNATURE_HASH {
        let decoded = EntityCreated::decode_log_data(&log.data)?;
        if decoded.string_annotation_keys.len() != decoded.string_annotation_values.len() {
            eyre::bail!("string annotation key/value length mismatch");
        }
        if decoded.numeric_annotation_keys.len() != decoded.numeric_annotation_values.len() {
            eyre::bail!("numeric annotation key/value length mismatch");
        }
        Ok(Some(EntityEvent::Created {
            entity_key: decoded.entity_key,
            owner: decoded.owner,
            expires_at: decoded.expires_at,
            content_type: decoded.content_type,
            payload: decoded.payload,
            string_keys: decoded.string_annotation_keys,
            string_values: decoded.string_annotation_values,
            numeric_keys: decoded.numeric_annotation_keys,
            numeric_values: decoded.numeric_annotation_values,
        }))
    } else if *selector == EntityUpdated::SIGNATURE_HASH {
        let decoded = EntityUpdated::decode_log_data(&log.data)?;
        if decoded.string_annotation_keys.len() != decoded.string_annotation_values.len() {
            eyre::bail!("string annotation key/value length mismatch");
        }
        if decoded.numeric_annotation_keys.len() != decoded.numeric_annotation_values.len() {
            eyre::bail!("numeric annotation key/value length mismatch");
        }
        Ok(Some(EntityEvent::Updated {
            entity_key: decoded.entity_key,
            owner: decoded.owner,
            old_expires_at: decoded.old_expires_at,
            new_expires_at: decoded.new_expires_at,
            content_type: decoded.content_type,
            payload: decoded.payload,
            string_keys: decoded.string_annotation_keys,
            string_values: decoded.string_annotation_values,
            numeric_keys: decoded.numeric_annotation_keys,
            numeric_values: decoded.numeric_annotation_values,
        }))
    } else if *selector == EntityDeleted::SIGNATURE_HASH {
        let decoded = EntityDeleted::decode_log_data(&log.data)?;
        Ok(Some(EntityEvent::Deleted {
            entity_key: decoded.entity_key,
            owner: decoded.owner,
        }))
    } else if *selector == EntityExpired::SIGNATURE_HASH {
        let decoded = EntityExpired::decode_log_data(&log.data)?;
        Ok(Some(EntityEvent::Expired {
            entity_key: decoded.entity_key,
            owner: decoded.owner,
        }))
    } else if *selector == EntityExtended::SIGNATURE_HASH {
        let decoded = EntityExtended::decode_log_data(&log.data)?;
        Ok(Some(EntityEvent::Extended {
            entity_key: decoded.entity_key,
            old_expires_at: decoded.old_expires_at,
            new_expires_at: decoded.new_expires_at,
        }))
    } else {
        eyre::bail!("unrecognized event selector: {selector}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes};
    use mote_primitives::constants::PROCESSOR_ADDRESS;
    use mote_primitives::events::{
        EntityCreated, EntityDeleted, EntityExpired, EntityExtended, EntityUpdated, LogAnnotations,
    };

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
    fn parse_created_event() {
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
    fn parse_updated_event() {
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
        let event = parse_log(&log).unwrap().unwrap();
        match event {
            EntityEvent::Updated {
                entity_key,
                owner,
                old_expires_at,
                new_expires_at,
                content_type,
                payload,
                ..
            } => {
                assert_eq!(entity_key, B256::repeat_byte(0x01));
                assert_eq!(owner, Address::repeat_byte(0x02));
                assert_eq!(old_expires_at, 50);
                assert_eq!(new_expires_at, 100);
                assert_eq!(content_type, "application/json");
                assert_eq!(payload.as_ref(), b"updated");
            }
            _ => panic!("expected Updated"),
        }
    }

    #[test]
    fn parse_deleted_event() {
        let log = EntityDeleted::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x03),
            Address::repeat_byte(0x04),
        );
        let event = parse_log(&log).unwrap().unwrap();
        match event {
            EntityEvent::Deleted { entity_key, owner } => {
                assert_eq!(entity_key, B256::repeat_byte(0x03));
                assert_eq!(owner, Address::repeat_byte(0x04));
            }
            _ => panic!("expected Deleted"),
        }
    }

    #[test]
    fn parse_expired_event() {
        let log = EntityExpired::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x06),
            Address::repeat_byte(0x07),
        );
        let event = parse_log(&log).unwrap().unwrap();
        match event {
            EntityEvent::Expired { entity_key, owner } => {
                assert_eq!(entity_key, B256::repeat_byte(0x06));
                assert_eq!(owner, Address::repeat_byte(0x07));
            }
            _ => panic!("expected Expired"),
        }
    }

    #[test]
    fn parse_extended_event() {
        let log = EntityExtended::new_log(PROCESSOR_ADDRESS, B256::repeat_byte(0x05), 10, 20);
        let event = parse_log(&log).unwrap().unwrap();
        match event {
            EntityEvent::Extended {
                entity_key,
                old_expires_at,
                new_expires_at,
            } => {
                assert_eq!(entity_key, B256::repeat_byte(0x05));
                assert_eq!(old_expires_at, 10);
                assert_eq!(new_expires_at, 20);
            }
            _ => panic!("expected Extended"),
        }
    }

    #[test]
    fn skip_wrong_address() {
        let log = EntityCreated::new_log(
            Address::repeat_byte(0xFF), // wrong address
            B256::repeat_byte(0x01),
            Address::repeat_byte(0x02),
            100,
            "text/plain".into(),
            Bytes::from_static(b"hello"),
            LogAnnotations {
                string_keys: vec![],
                string_values: vec![],
                numeric_keys: vec![],
                numeric_values: vec![],
            },
        );
        assert!(parse_log(&log).unwrap().is_none());
    }

    #[test]
    fn skip_unknown_selector() {
        let mut log = make_created_log();
        // corrupt the selector topic
        log.data.topics_mut()[0] = B256::repeat_byte(0xFF);
        let result = parse_log(&log);
        assert!(result.is_err()); // decode error for unknown selector
    }

    #[test]
    fn annotation_length_mismatch_returns_error() {
        let log = EntityCreated::new_log(
            PROCESSOR_ADDRESS,
            B256::repeat_byte(0x01),
            Address::repeat_byte(0x02),
            100,
            "text/plain".into(),
            Bytes::from_static(b"hello"),
            LogAnnotations {
                string_keys: vec!["k1".into(), "k2".into()],
                string_values: vec!["v1".into()], // mismatch!
                numeric_keys: vec![],
                numeric_values: vec![],
            },
        );
        let result = parse_log(&log);
        assert!(result.is_err());
    }
}

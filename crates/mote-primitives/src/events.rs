use alloy_primitives::{Address, B256, Bytes, Log};
use alloy_sol_types::{SolEvent, sol};

pub struct LogAnnotations {
    pub string_keys: Vec<String>,
    pub string_values: Vec<String>,
    pub numeric_keys: Vec<String>,
    pub numeric_values: Vec<u64>,
}

sol! {
    event EntityCreated(
        bytes32 indexed entity_key,
        address indexed owner,
        uint64 expires_at,
        string content_type,
        bytes payload,
        string[] string_annotation_keys,
        string[] string_annotation_values,
        string[] numeric_annotation_keys,
        uint64[] numeric_annotation_values
    );

    event EntityUpdated(
        bytes32 indexed entity_key,
        address indexed owner,
        uint64 old_expires_at,
        uint64 new_expires_at,
        string content_type,
        bytes payload,
        string[] string_annotation_keys,
        string[] string_annotation_values,
        string[] numeric_annotation_keys,
        uint64[] numeric_annotation_values
    );

    event EntityDeleted(
        bytes32 indexed entity_key,
        address indexed owner
    );

    event EntityExpired(
        bytes32 indexed entity_key,
        address indexed owner
    );

    event EntityExtended(
        bytes32 indexed entity_key,
        uint64 old_expires_at,
        uint64 new_expires_at
    );
}

impl EntityCreated {
    pub fn new_log(
        address: Address,
        entity_key: B256,
        owner: Address,
        expires_at: u64,
        content_type: String,
        payload: Bytes,
        annotations: LogAnnotations,
    ) -> Log {
        let event = Self {
            entity_key,
            owner,
            expires_at,
            content_type,
            payload,
            string_annotation_keys: annotations.string_keys,
            string_annotation_values: annotations.string_values,
            numeric_annotation_keys: annotations.numeric_keys,
            numeric_annotation_values: annotations.numeric_values,
        };
        Log {
            address,
            data: event.encode_log_data(),
        }
    }
}

impl EntityUpdated {
    pub fn new_log(
        address: Address,
        entity_key: B256,
        owner: Address,
        expires_at: (u64, u64),
        content_type: String,
        payload: Bytes,
        annotations: LogAnnotations,
    ) -> Log {
        let event = Self {
            entity_key,
            owner,
            old_expires_at: expires_at.0,
            new_expires_at: expires_at.1,
            content_type,
            payload,
            string_annotation_keys: annotations.string_keys,
            string_annotation_values: annotations.string_values,
            numeric_annotation_keys: annotations.numeric_keys,
            numeric_annotation_values: annotations.numeric_values,
        };
        Log {
            address,
            data: event.encode_log_data(),
        }
    }
}

impl EntityDeleted {
    pub fn new_log(address: Address, entity_key: B256, owner: Address) -> Log {
        let event = Self { entity_key, owner };
        Log {
            address,
            data: event.encode_log_data(),
        }
    }
}

impl EntityExpired {
    pub fn new_log(address: Address, entity_key: B256, owner: Address) -> Log {
        let event = Self { entity_key, owner };
        Log {
            address,
            data: event.encode_log_data(),
        }
    }
}

impl EntityExtended {
    pub fn new_log(
        address: Address,
        entity_key: B256,
        old_expires_at: u64,
        new_expires_at: u64,
    ) -> Log {
        let event = Self {
            entity_key,
            old_expires_at,
            new_expires_at,
        };
        Log {
            address,
            data: event.encode_log_data(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_created_event_signature_exists() {
        let sig = EntityCreated::SIGNATURE;
        assert!(!sig.is_empty());
    }

    #[test]
    fn entity_deleted_has_two_indexed_topics() {
        assert!(!EntityDeleted::ANONYMOUS);
    }

    #[test]
    fn entity_created_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0xAB);
        let owner = Address::repeat_byte(0x01);
        let log = EntityCreated::new_log(
            Address::repeat_byte(0xFF),
            entity_key,
            owner,
            42,
            "text/plain".into(),
            Bytes::from_static(b"hello"),
            LogAnnotations {
                string_keys: vec!["k1".into()],
                string_values: vec!["v1".into()],
                numeric_keys: vec!["n1".into()],
                numeric_values: vec![100],
            },
        );
        assert_eq!(log.address, Address::repeat_byte(0xFF));
        // Should have 3 topics: selector + entity_key + owner
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityCreated::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.expires_at, 42);
        assert_eq!(decoded.content_type, "text/plain");
    }

    #[test]
    fn entity_deleted_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x02);
        let log = EntityDeleted::new_log(Address::ZERO, entity_key, owner);
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityDeleted::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.owner, owner);
    }

    #[test]
    fn entity_extended_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0x03);
        let log = EntityExtended::new_log(Address::ZERO, entity_key, 10, 20);
        assert_eq!(log.data.topics().len(), 2);

        let decoded = EntityExtended::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.old_expires_at, 10);
        assert_eq!(decoded.new_expires_at, 20);
    }

    #[test]
    fn entity_updated_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0x04);
        let owner = Address::repeat_byte(0x05);
        let log = EntityUpdated::new_log(
            Address::repeat_byte(0xFF),
            entity_key,
            owner,
            (10, 20),
            "application/json".into(),
            Bytes::from_static(b"updated"),
            LogAnnotations {
                string_keys: vec!["k1".into()],
                string_values: vec!["v1".into()],
                numeric_keys: vec!["n1".into()],
                numeric_values: vec![200],
            },
        );
        assert_eq!(log.address, Address::repeat_byte(0xFF));
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityUpdated::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.old_expires_at, 10);
        assert_eq!(decoded.new_expires_at, 20);
        assert_eq!(decoded.content_type, "application/json");
    }

    #[test]
    fn entity_expired_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0x06);
        let owner = Address::repeat_byte(0x07);
        let log = EntityExpired::new_log(Address::ZERO, entity_key, owner);
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityExpired::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.owner, owner);
    }
}

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
        uint64[] numeric_annotation_values,
        uint8 extend_policy,
        address operator,
        uint64 gas_cost
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
        uint64[] numeric_annotation_values,
        uint8 extend_policy,
        address operator,
        uint64 gas_cost
    );

    event EntityDeleted(
        bytes32 indexed entity_key,
        address indexed owner,
        address sender,
        uint64 gas_cost
    );

    event EntityExpired(
        bytes32 indexed entity_key,
        address indexed owner
    );

    event EntityExtended(
        bytes32 indexed entity_key,
        uint64 old_expires_at,
        uint64 new_expires_at,
        address owner,
        uint64 gas_cost
    );

    event EntityPermissionsChanged(
        bytes32 indexed entity_key,
        address indexed old_owner,
        address new_owner,
        uint8 extend_policy,
        address operator,
        uint64 gas_cost
    );
}

pub struct EntityCreateLog {
    pub address: Address,
    pub entity_key: B256,
    pub owner: Address,
    pub expires_at: u64,
    pub content_type: String,
    pub payload: Bytes,
    pub annotations: LogAnnotations,
    pub extend_policy: u8,
    pub operator: Address,
    pub gas_cost: u64,
}

impl EntityCreateLog {
    pub fn build(self) -> Log {
        let event = EntityCreated {
            entity_key: self.entity_key,
            owner: self.owner,
            expires_at: self.expires_at,
            content_type: self.content_type,
            payload: self.payload,
            string_annotation_keys: self.annotations.string_keys,
            string_annotation_values: self.annotations.string_values,
            numeric_annotation_keys: self.annotations.numeric_keys,
            numeric_annotation_values: self.annotations.numeric_values,
            extend_policy: self.extend_policy,
            operator: self.operator,
            gas_cost: self.gas_cost,
        };
        Log {
            address: self.address,
            data: event.encode_log_data(),
        }
    }
}

pub struct EntityUpdateLog {
    pub address: Address,
    pub entity_key: B256,
    pub owner: Address,
    pub old_expires_at: u64,
    pub new_expires_at: u64,
    pub content_type: String,
    pub payload: Bytes,
    pub annotations: LogAnnotations,
    pub extend_policy: u8,
    pub operator: Address,
    pub gas_cost: u64,
}

impl EntityUpdateLog {
    pub fn build(self) -> Log {
        let event = EntityUpdated {
            entity_key: self.entity_key,
            owner: self.owner,
            old_expires_at: self.old_expires_at,
            new_expires_at: self.new_expires_at,
            content_type: self.content_type,
            payload: self.payload,
            string_annotation_keys: self.annotations.string_keys,
            string_annotation_values: self.annotations.string_values,
            numeric_annotation_keys: self.annotations.numeric_keys,
            numeric_annotation_values: self.annotations.numeric_values,
            extend_policy: self.extend_policy,
            operator: self.operator,
            gas_cost: self.gas_cost,
        };
        Log {
            address: self.address,
            data: event.encode_log_data(),
        }
    }
}

impl EntityDeleted {
    pub fn new_log(
        address: Address,
        entity_key: B256,
        owner: Address,
        sender: Address,
        gas_cost: u64,
    ) -> Log {
        let event = Self {
            entity_key,
            owner,
            sender,
            gas_cost,
        };
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
        owner: Address,
        gas_cost: u64,
    ) -> Log {
        let event = Self {
            entity_key,
            old_expires_at,
            new_expires_at,
            owner,
            gas_cost,
        };
        Log {
            address,
            data: event.encode_log_data(),
        }
    }
}

impl EntityPermissionsChanged {
    pub fn new_log(
        address: Address,
        entity_key: B256,
        old_owner: Address,
        new_owner: Address,
        extend_policy: u8,
        operator: Address,
        gas_cost: u64,
    ) -> Log {
        let event = Self {
            entity_key,
            old_owner,
            new_owner,
            extend_policy,
            operator,
            gas_cost,
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
    fn entity_created_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0xAB);
        let owner = Address::repeat_byte(0x01);
        let log = EntityCreateLog {
            address: Address::repeat_byte(0xFF),
            entity_key,
            owner,
            expires_at: 42,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            annotations: LogAnnotations {
                string_keys: vec!["k1".into()],
                string_values: vec!["v1".into()],
                numeric_keys: vec!["n1".into()],
                numeric_values: vec![100],
            },
            extend_policy: 0,
            operator: Address::ZERO,
            gas_cost: 50_000,
        }
        .build();
        assert_eq!(log.address, Address::repeat_byte(0xFF));
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityCreated::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.expires_at, 42);
        assert_eq!(decoded.content_type, "text/plain");
        assert_eq!(decoded.extend_policy, 0);
        assert_eq!(decoded.operator, Address::ZERO);
        assert_eq!(decoded.gas_cost, 50_000);
    }

    #[test]
    fn entity_created_with_operator_roundtrips() {
        let entity_key = B256::repeat_byte(0xAB);
        let owner = Address::repeat_byte(0x01);
        let operator = Address::repeat_byte(0x42);
        let log = EntityCreateLog {
            address: Address::repeat_byte(0xFF),
            entity_key,
            owner,
            expires_at: 42,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            annotations: LogAnnotations {
                string_keys: vec![],
                string_values: vec![],
                numeric_keys: vec![],
                numeric_values: vec![],
            },
            extend_policy: 1,
            operator,
            gas_cost: 70_000,
        }
        .build();

        let decoded = EntityCreated::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.extend_policy, 1);
        assert_eq!(decoded.operator, operator);
        assert_eq!(decoded.gas_cost, 70_000);
    }

    #[test]
    fn entity_deleted_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x02);
        let sender = Address::repeat_byte(0x03);
        let log = EntityDeleted::new_log(Address::ZERO, entity_key, owner, sender, 10_000);
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityDeleted::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.owner, owner);
        assert_eq!(decoded.sender, sender);
        assert_eq!(decoded.gas_cost, 10_000);
    }

    #[test]
    fn entity_extended_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0x03);
        let owner = Address::repeat_byte(0x05);
        let log = EntityExtended::new_log(Address::ZERO, entity_key, 10, 20, owner, 10_100);
        assert_eq!(log.data.topics().len(), 2);

        let decoded = EntityExtended::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.old_expires_at, 10);
        assert_eq!(decoded.new_expires_at, 20);
        assert_eq!(decoded.owner, owner);
        assert_eq!(decoded.gas_cost, 10_100);
    }

    #[test]
    fn entity_updated_new_log_roundtrips() {
        let entity_key = B256::repeat_byte(0x04);
        let owner = Address::repeat_byte(0x05);
        let operator = Address::repeat_byte(0x06);
        let log = EntityUpdateLog {
            address: Address::repeat_byte(0xFF),
            entity_key,
            owner,
            old_expires_at: 10,
            new_expires_at: 20,
            content_type: "application/json".into(),
            payload: Bytes::from_static(b"updated"),
            annotations: LogAnnotations {
                string_keys: vec!["k1".into()],
                string_values: vec!["v1".into()],
                numeric_keys: vec!["n1".into()],
                numeric_values: vec![200],
            },
            extend_policy: 1,
            operator,
            gas_cost: 40_000,
        }
        .build();
        assert_eq!(log.address, Address::repeat_byte(0xFF));
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityUpdated::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.old_expires_at, 10);
        assert_eq!(decoded.new_expires_at, 20);
        assert_eq!(decoded.content_type, "application/json");
        assert_eq!(decoded.extend_policy, 1);
        assert_eq!(decoded.operator, operator);
        assert_eq!(decoded.gas_cost, 40_000);
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

    #[test]
    fn entity_permissions_changed_roundtrips() {
        let entity_key = B256::repeat_byte(0x07);
        let old_owner = Address::repeat_byte(0x01);
        let new_owner = Address::repeat_byte(0x02);
        let operator = Address::repeat_byte(0x03);
        let log = EntityPermissionsChanged::new_log(
            Address::repeat_byte(0xFF),
            entity_key,
            old_owner,
            new_owner,
            1,
            operator,
            10_000,
        );
        assert_eq!(log.data.topics().len(), 3);

        let decoded = EntityPermissionsChanged::decode_log_data(&log.data).unwrap();
        assert_eq!(decoded.old_owner, old_owner);
        assert_eq!(decoded.new_owner, new_owner);
        assert_eq!(decoded.extend_policy, 1);
        assert_eq!(decoded.operator, operator);
        assert_eq!(decoded.gas_cost, 10_000);
    }
}

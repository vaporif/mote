use alloy_primitives::B256;
use alloy_rlp::{RlpDecodable, RlpEncodable};

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Create {
    pub btl: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub string_annotations: Vec<StringAnnotationWire>,
    pub numeric_annotations: Vec<NumericAnnotationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct StringAnnotationWire {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NumericAnnotationWire {
    pub key: String,
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Update {
    pub entity_key: B256,
    pub btl: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub string_annotations: Vec<StringAnnotationWire>,
    pub numeric_annotations: Vec<NumericAnnotationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Extend {
    pub entity_key: B256,
    pub additional_blocks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct MoteTransaction {
    pub creates: Vec<Create>,
    pub updates: Vec<Update>,
    pub deletes: Vec<B256>,
    pub extends: Vec<Extend>,
}

impl From<(String, String)> for StringAnnotationWire {
    fn from((key, value): (String, String)) -> Self {
        Self { key, value }
    }
}

impl From<(&str, &str)> for StringAnnotationWire {
    fn from((key, value): (&str, &str)) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl From<(String, u64)> for NumericAnnotationWire {
    fn from((key, value): (String, u64)) -> Self {
        Self { key, value }
    }
}

impl From<(&str, u64)> for NumericAnnotationWire {
    fn from((key, value): (&str, u64)) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

impl MoteTransaction {
    pub fn total_operations(&self) -> usize {
        self.creates.len() + self.updates.len() + self.deletes.len() + self.extends.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::{Decodable, Encodable};

    #[test]
    fn create_rlp_roundtrip() {
        let create = Create {
            btl: 100,
            content_type: "application/json".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![StringAnnotationWire {
                key: "key".into(),
                value: "val".into(),
            }],
            numeric_annotations: vec![NumericAnnotationWire {
                key: "count".into(),
                value: 42,
            }],
        };
        let mut buf = Vec::new();
        create.encode(&mut buf);
        let decoded = Create::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(create, decoded);
    }

    #[test]
    fn mote_transaction_rlp_roundtrip() {
        let tx = MoteTransaction {
            creates: vec![Create {
                btl: 50,
                content_type: "text/plain".into(),
                payload: b"test".to_vec(),
                string_annotations: vec![],
                numeric_annotations: vec![],
            }],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };
        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = MoteTransaction::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(tx, decoded);
    }

    #[test]
    fn full_transaction_roundtrip() {
        let tx = MoteTransaction {
            creates: vec![Create {
                btl: 100,
                content_type: "application/json".into(),
                payload: b"{\"key\":\"value\"}".to_vec(),
                string_annotations: vec![StringAnnotationWire {
                    key: "app".into(),
                    value: "test".into(),
                }],
                numeric_annotations: vec![NumericAnnotationWire {
                    key: "priority".into(),
                    value: 1,
                }],
            }],
            updates: vec![Update {
                entity_key: B256::repeat_byte(0x01),
                btl: 200,
                content_type: "text/plain".into(),
                payload: b"updated".to_vec(),
                string_annotations: vec![],
                numeric_annotations: vec![],
            }],
            deletes: vec![B256::repeat_byte(0x02)],
            extends: vec![Extend {
                entity_key: B256::repeat_byte(0x03),
                additional_blocks: 50,
            }],
        };
        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = MoteTransaction::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(tx, decoded);
    }

    #[test]
    fn empty_transaction_roundtrip() {
        let tx = MoteTransaction {
            creates: vec![],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };
        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = MoteTransaction::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(tx, decoded);
    }
}

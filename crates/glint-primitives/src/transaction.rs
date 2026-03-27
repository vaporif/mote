use alloy_primitives::{Address, B256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum ExtendPolicy {
    #[default]
    OwnerOnly = 0,
    AnyoneCanExtend = 1,
}

impl Encodable for ExtendPolicy {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        (*self as u8).encode(out);
    }
    fn length(&self) -> usize {
        (*self as u8).length()
    }
}

impl Decodable for ExtendPolicy {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let val = u8::decode(buf)?;
        match val {
            0 => Ok(Self::OwnerOnly),
            1 => Ok(Self::AnyoneCanExtend),
            _ => Err(alloy_rlp::Error::Custom("invalid ExtendPolicy")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    pub btl: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub string_annotations: Vec<StringAnnotationWire>,
    pub numeric_annotations: Vec<NumericAnnotationWire>,
    pub extend_policy: ExtendPolicy,
    pub operator: Option<Address>,
}

// RLP: [btl, content_type, payload, string_anns, numeric_anns, extend_policy, operator]
// operator: Address::ZERO encodes None
impl Create {
    fn payload_length(&self) -> usize {
        self.btl.length()
            + self.content_type.length()
            + self.payload.length()
            + self.string_annotations.length()
            + self.numeric_annotations.length()
            + self.extend_policy.length()
            + self.operator.unwrap_or(Address::ZERO).length()
    }
}

impl Encodable for Create {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        alloy_rlp::Header {
            list: true,
            payload_length: self.payload_length(),
        }
        .encode(out);
        self.btl.encode(out);
        self.content_type.encode(out);
        self.payload.encode(out);
        self.string_annotations.encode(out);
        self.numeric_annotations.encode(out);
        self.extend_policy.encode(out);
        self.operator.unwrap_or(Address::ZERO).encode(out);
    }

    fn length(&self) -> usize {
        let list_len = self.payload_length();
        alloy_rlp::length_of_length(list_len) + list_len
    }
}

impl Decodable for Create {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let header = alloy_rlp::Header::decode(buf)?;
        if !header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }
        let remaining_before = buf.len();
        let btl = u64::decode(buf)?;
        let content_type = String::decode(buf)?;
        let payload = Vec::<u8>::decode(buf)?;
        let string_annotations = Vec::<StringAnnotationWire>::decode(buf)?;
        let numeric_annotations = Vec::<NumericAnnotationWire>::decode(buf)?;

        let consumed = remaining_before - buf.len();
        let (extend_policy, operator) = if consumed < header.payload_length {
            let ep = ExtendPolicy::decode(buf)?;
            let addr = Address::decode(buf)?;
            let op = if addr == Address::ZERO {
                None
            } else {
                Some(addr)
            };
            (ep, op)
        } else {
            (ExtendPolicy::default(), None)
        };

        let total_consumed = remaining_before - buf.len();
        if total_consumed != header.payload_length {
            return Err(alloy_rlp::Error::ListLengthMismatch {
                expected: header.payload_length,
                got: total_consumed,
            });
        }

        Ok(Self {
            btl,
            content_type,
            payload,
            string_annotations,
            numeric_annotations,
            extend_policy,
            operator,
        })
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub entity_key: B256,
    pub btl: u64,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub string_annotations: Vec<StringAnnotationWire>,
    pub numeric_annotations: Vec<NumericAnnotationWire>,
    pub extend_policy: Option<ExtendPolicy>,
    pub operator: Option<Option<Address>>,
}

const EXTEND_POLICY_NO_CHANGE: u8 = 0xFF;
const OPERATOR_TAG_UNCHANGED: u8 = 0;
const OPERATOR_TAG_REMOVE: u8 = 1;
const OPERATOR_TAG_SET: u8 = 2;

impl Update {
    const fn encoding_parts(&self) -> (u8, u8, Option<Address>) {
        let ep_val: u8 = match self.extend_policy {
            None => EXTEND_POLICY_NO_CHANGE,
            Some(ExtendPolicy::OwnerOnly) => ExtendPolicy::OwnerOnly as u8,
            Some(ExtendPolicy::AnyoneCanExtend) => ExtendPolicy::AnyoneCanExtend as u8,
        };
        let (op_tag, op_addr): (u8, Option<Address>) = match self.operator {
            None => (OPERATOR_TAG_UNCHANGED, None),
            Some(None) => (OPERATOR_TAG_REMOVE, None),
            Some(Some(addr)) => (OPERATOR_TAG_SET, Some(addr)),
        };
        (ep_val, op_tag, op_addr)
    }

    fn payload_length(&self) -> usize {
        let (ep_val, op_tag, op_addr) = self.encoding_parts();
        self.entity_key.length()
            + self.btl.length()
            + self.content_type.length()
            + self.payload.length()
            + self.string_annotations.length()
            + self.numeric_annotations.length()
            + ep_val.length()
            + op_tag.length()
            + op_addr.map_or(0, |a| a.length())
    }
}

impl Encodable for Update {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let (ep_val, op_tag, op_addr) = self.encoding_parts();
        alloy_rlp::Header {
            list: true,
            payload_length: self.payload_length(),
        }
        .encode(out);
        self.entity_key.encode(out);
        self.btl.encode(out);
        self.content_type.encode(out);
        self.payload.encode(out);
        self.string_annotations.encode(out);
        self.numeric_annotations.encode(out);
        ep_val.encode(out);
        op_tag.encode(out);
        if let Some(addr) = op_addr {
            addr.encode(out);
        }
    }

    fn length(&self) -> usize {
        let list_len = self.payload_length();
        alloy_rlp::length_of_length(list_len) + list_len
    }
}

impl Decodable for Update {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let header = alloy_rlp::Header::decode(buf)?;
        if !header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }
        let remaining_before = buf.len();
        let entity_key = B256::decode(buf)?;
        let btl = u64::decode(buf)?;
        let content_type = String::decode(buf)?;
        let payload = Vec::<u8>::decode(buf)?;
        let string_annotations = Vec::<StringAnnotationWire>::decode(buf)?;
        let numeric_annotations = Vec::<NumericAnnotationWire>::decode(buf)?;

        let consumed = remaining_before - buf.len();
        let (extend_policy, operator) = if consumed < header.payload_length {
            let ep_val = u8::decode(buf)?;
            let ep = match ep_val {
                EXTEND_POLICY_NO_CHANGE => None,
                v if v == ExtendPolicy::OwnerOnly as u8 => Some(ExtendPolicy::OwnerOnly),
                v if v == ExtendPolicy::AnyoneCanExtend as u8 => {
                    Some(ExtendPolicy::AnyoneCanExtend)
                }
                _ => return Err(alloy_rlp::Error::Custom("invalid ExtendPolicy sentinel")),
            };

            let op_tag = u8::decode(buf)?;
            let op = match op_tag {
                OPERATOR_TAG_UNCHANGED => None,
                OPERATOR_TAG_REMOVE => Some(None),
                OPERATOR_TAG_SET => {
                    let addr = Address::decode(buf)?;
                    Some(Some(addr))
                }
                _ => return Err(alloy_rlp::Error::Custom("invalid operator tag")),
            };
            (ep, op)
        } else {
            (None, None)
        };

        let total_consumed = remaining_before - buf.len();
        if total_consumed != header.payload_length {
            return Err(alloy_rlp::Error::ListLengthMismatch {
                expected: header.payload_length,
                got: total_consumed,
            });
        }

        Ok(Self {
            entity_key,
            btl,
            content_type,
            payload,
            string_annotations,
            numeric_annotations,
            extend_policy,
            operator,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Extend {
    pub entity_key: B256,
    pub additional_blocks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct GlintTransaction {
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

impl GlintTransaction {
    pub const fn total_operations(&self) -> usize {
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
            extend_policy: ExtendPolicy::OwnerOnly,
            operator: None,
        };
        let mut buf = Vec::new();
        create.encode(&mut buf);
        let decoded = Create::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(create, decoded);
    }

    #[test]
    fn create_rlp_roundtrip_with_operator() {
        let operator = Address::repeat_byte(0x42);
        let create = Create {
            btl: 100,
            content_type: "text/plain".into(),
            payload: b"hello".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: ExtendPolicy::AnyoneCanExtend,
            operator: Some(operator),
        };
        let mut buf = Vec::new();
        create.encode(&mut buf);
        let decoded = Create::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(create, decoded);
        assert_eq!(decoded.extend_policy, ExtendPolicy::AnyoneCanExtend);
        assert_eq!(decoded.operator, Some(operator));
    }

    #[test]
    fn create_rlp_no_operator_encodes_zero_address() {
        let create = Create {
            btl: 50,
            content_type: "text/plain".into(),
            payload: b"test".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: ExtendPolicy::OwnerOnly,
            operator: None,
        };
        let mut buf = Vec::new();
        create.encode(&mut buf);
        let decoded = Create::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded.operator, None);
    }

    #[test]
    fn update_rlp_roundtrip_keep_current() {
        let update = Update {
            entity_key: B256::repeat_byte(0x01),
            btl: 200,
            content_type: "text/plain".into(),
            payload: b"updated".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: None,
            operator: None,
        };
        let mut buf = Vec::new();
        update.encode(&mut buf);
        let decoded = Update::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(update, decoded);
    }

    #[test]
    fn update_rlp_roundtrip_set_policy_and_operator() {
        let operator = Address::repeat_byte(0xAB);
        let update = Update {
            entity_key: B256::repeat_byte(0x01),
            btl: 100,
            content_type: "application/json".into(),
            payload: b"data".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: Some(ExtendPolicy::AnyoneCanExtend),
            operator: Some(Some(operator)),
        };
        let mut buf = Vec::new();
        update.encode(&mut buf);
        let decoded = Update::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(update, decoded);
    }

    #[test]
    fn update_rlp_roundtrip_remove_operator() {
        let update = Update {
            entity_key: B256::repeat_byte(0x02),
            btl: 50,
            content_type: "text/plain".into(),
            payload: b"test".to_vec(),
            string_annotations: vec![],
            numeric_annotations: vec![],
            extend_policy: Some(ExtendPolicy::OwnerOnly),
            operator: Some(None),
        };
        let mut buf = Vec::new();
        update.encode(&mut buf);
        let decoded = Update::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(update, decoded);
    }

    #[test]
    fn glint_transaction_rlp_roundtrip() {
        let tx = GlintTransaction {
            creates: vec![Create {
                btl: 50,
                content_type: "text/plain".into(),
                payload: b"test".to_vec(),
                string_annotations: vec![],
                numeric_annotations: vec![],
                extend_policy: ExtendPolicy::OwnerOnly,
                operator: None,
            }],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };
        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(tx, decoded);
    }

    #[test]
    fn full_transaction_roundtrip() {
        let tx = GlintTransaction {
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
                extend_policy: ExtendPolicy::AnyoneCanExtend,
                operator: Some(Address::repeat_byte(0x99)),
            }],
            updates: vec![Update {
                entity_key: B256::repeat_byte(0x01),
                btl: 200,
                content_type: "text/plain".into(),
                payload: b"updated".to_vec(),
                string_annotations: vec![],
                numeric_annotations: vec![],
                extend_policy: None,
                operator: None,
            }],
            deletes: vec![B256::repeat_byte(0x02)],
            extends: vec![Extend {
                entity_key: B256::repeat_byte(0x03),
                additional_blocks: 50,
            }],
        };
        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(tx, decoded);
    }

    #[test]
    fn empty_transaction_roundtrip() {
        let tx = GlintTransaction {
            creates: vec![],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };
        let mut buf = Vec::new();
        tx.encode(&mut buf);
        let decoded = GlintTransaction::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(tx, decoded);
    }

    #[test]
    fn extend_policy_serde_roundtrip() {
        let policy = ExtendPolicy::AnyoneCanExtend;
        let json = serde_json::to_string(&policy).unwrap();
        let decoded: ExtendPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(policy, decoded);
    }
}

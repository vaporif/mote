use crate::processor::RawContentSlices;

pub struct DecodedGlintTransaction<'a> {
    pub tx: glint_primitives::transaction::GlintTransaction,
    pub create_slices: Vec<RawContentSlices<'a>>,
    pub update_slices: Vec<RawContentSlices<'a>>,
}

fn skip_rlp_item<'a>(data: &'a [u8], cursor: &mut &'a [u8]) -> Result<&'a [u8], alloy_rlp::Error> {
    use alloy_rlp::Header;
    let start = data.len() - cursor.len();
    let header = Header::decode(cursor)?;
    *cursor = &cursor[header.payload_length..];
    let end = data.len() - cursor.len();
    Ok(&data[start..end])
}

/// Decode tx and extract raw RLP slices for deterministic content hashing.
pub fn decode_with_raw_slices(
    calldata: &[u8],
) -> Result<DecodedGlintTransaction<'_>, alloy_rlp::Error> {
    use alloy_rlp::{Decodable, Header};

    let tx = glint_primitives::transaction::GlintTransaction::decode(&mut &calldata[..])?;

    let mut cursor = calldata;
    let outer = Header::decode(&mut cursor)?;
    if !outer.list {
        return Err(alloy_rlp::Error::UnexpectedString);
    }

    let mut create_slices = Vec::with_capacity(tx.creates.len());
    let mut update_slices = Vec::with_capacity(tx.updates.len());

    let creates_header = Header::decode(&mut cursor)?;
    if creates_header.list && creates_header.payload_length > 0 {
        let creates_end = calldata.len() - cursor.len() + creates_header.payload_length;
        while calldata.len() - cursor.len() < creates_end {
            let item_header = Header::decode(&mut cursor)?;
            let item_end = calldata.len() - cursor.len() + item_header.payload_length;
            let _btl = skip_rlp_item(calldata, &mut cursor)?;
            let ct = skip_rlp_item(calldata, &mut cursor)?;
            let payload = skip_rlp_item(calldata, &mut cursor)?;
            let sa = skip_rlp_item(calldata, &mut cursor)?;
            let na = skip_rlp_item(calldata, &mut cursor)?;
            while calldata.len() - cursor.len() < item_end {
                let _ = skip_rlp_item(calldata, &mut cursor)?;
            }

            create_slices.push(RawContentSlices {
                content_type_rlp: ct,
                payload_rlp: payload,
                string_annotations_rlp: sa,
                numeric_annotations_rlp: na,
            });
        }
    }

    let updates_header = Header::decode(&mut cursor)?;
    if updates_header.list && updates_header.payload_length > 0 {
        let updates_end = calldata.len() - cursor.len() + updates_header.payload_length;
        while calldata.len() - cursor.len() < updates_end {
            let item_header = Header::decode(&mut cursor)?;
            let item_end = calldata.len() - cursor.len() + item_header.payload_length;
            let _ek = skip_rlp_item(calldata, &mut cursor)?;
            let _btl = skip_rlp_item(calldata, &mut cursor)?;
            let ct = skip_rlp_item(calldata, &mut cursor)?;
            let payload = skip_rlp_item(calldata, &mut cursor)?;
            let sa = skip_rlp_item(calldata, &mut cursor)?;
            let na = skip_rlp_item(calldata, &mut cursor)?;
            while calldata.len() - cursor.len() < item_end {
                let _ = skip_rlp_item(calldata, &mut cursor)?;
            }

            update_slices.push(RawContentSlices {
                content_type_rlp: ct,
                payload_rlp: payload,
                string_annotations_rlp: sa,
                numeric_annotations_rlp: na,
            });
        }
    }

    // deletes/extends have no content to hash

    Ok(DecodedGlintTransaction {
        tx,
        create_slices,
        update_slices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::Encodable;
    use glint_primitives::{
        storage::compute_content_hash_from_raw,
        transaction::{
            Create, GlintTransaction, NumericAnnotationWire, StringAnnotationWire, Update,
        },
    };

    #[test]
    fn raw_slices_match_encoded_fields() {
        let tx = GlintTransaction {
            creates: vec![Create {
                btl: 100,
                content_type: "text/plain".into(),
                payload: b"hello world".to_vec(),
                string_annotations: vec![StringAnnotationWire {
                    key: "app".into(),
                    value: "test".into(),
                }],
                numeric_annotations: vec![NumericAnnotationWire {
                    key: "priority".into(),
                    value: 1,
                }],
                extend_policy: Default::default(),
                operator: None,
            }],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };

        let mut calldata = Vec::new();
        tx.encode(&mut calldata);

        let decoded = decode_with_raw_slices(&calldata).unwrap();
        assert_eq!(decoded.tx, tx);
        assert_eq!(decoded.create_slices.len(), 1);

        let slices = &decoded.create_slices[0];

        let mut payload_encoded = Vec::new();
        tx.creates[0].payload.encode(&mut payload_encoded);
        assert_eq!(slices.payload_rlp, payload_encoded.as_slice());

        let mut ct_encoded = Vec::new();
        tx.creates[0].content_type.encode(&mut ct_encoded);
        assert_eq!(slices.content_type_rlp, ct_encoded.as_slice());

        let hash = compute_content_hash_from_raw(
            slices.payload_rlp,
            slices.content_type_rlp,
            slices.string_annotations_rlp,
            slices.numeric_annotations_rlp,
        );
        assert_ne!(hash, alloy_primitives::B256::ZERO);
    }

    #[test]
    fn raw_slices_roundtrip_multiple_creates() {
        let tx = GlintTransaction {
            creates: vec![
                Create {
                    btl: 50,
                    content_type: "application/json".into(),
                    payload: b"{\"a\":1}".to_vec(),
                    string_annotations: vec![],
                    numeric_annotations: vec![],
                    extend_policy: Default::default(),
                    operator: None,
                },
                Create {
                    btl: 200,
                    content_type: "text/plain".into(),
                    payload: b"second entity".to_vec(),
                    string_annotations: vec![StringAnnotationWire {
                        key: "tag".into(),
                        value: "v2".into(),
                    }],
                    numeric_annotations: vec![],
                    extend_policy: Default::default(),
                    operator: None,
                },
            ],
            updates: vec![],
            deletes: vec![],
            extends: vec![],
        };

        let mut calldata = Vec::new();
        tx.encode(&mut calldata);

        let decoded = decode_with_raw_slices(&calldata).unwrap();
        assert_eq!(decoded.create_slices.len(), 2);

        let hash0 = compute_content_hash_from_raw(
            decoded.create_slices[0].payload_rlp,
            decoded.create_slices[0].content_type_rlp,
            decoded.create_slices[0].string_annotations_rlp,
            decoded.create_slices[0].numeric_annotations_rlp,
        );
        let hash1 = compute_content_hash_from_raw(
            decoded.create_slices[1].payload_rlp,
            decoded.create_slices[1].content_type_rlp,
            decoded.create_slices[1].string_annotations_rlp,
            decoded.create_slices[1].numeric_annotations_rlp,
        );
        assert_ne!(hash0, hash1);
    }

    #[test]
    fn raw_slices_with_updates() {
        let tx = GlintTransaction {
            creates: vec![],
            updates: vec![Update {
                entity_key: alloy_primitives::B256::repeat_byte(0x01),
                btl: 200,
                content_type: "application/json".into(),
                payload: b"{\"updated\":true}".to_vec(),
                string_annotations: vec![StringAnnotationWire {
                    key: "v".into(),
                    value: "2".into(),
                }],
                numeric_annotations: vec![],
                extend_policy: None,
                operator: None,
            }],
            deletes: vec![],
            extends: vec![],
        };

        let mut calldata = Vec::new();
        tx.encode(&mut calldata);

        let decoded = decode_with_raw_slices(&calldata).unwrap();
        assert_eq!(decoded.update_slices.len(), 1);

        let slices = &decoded.update_slices[0];
        let mut payload_encoded = Vec::new();
        tx.updates[0].payload.encode(&mut payload_encoded);
        assert_eq!(slices.payload_rlp, payload_encoded.as_slice());
    }

    #[test]
    fn skip_rlp_item_handles_single_byte_values() {
        let data = &[0x2a];
        let mut cursor = &data[..];
        let slice = skip_rlp_item(data, &mut cursor).unwrap();
        assert_eq!(slice, &[0x2a]);
        assert!(cursor.is_empty());
    }

    #[test]
    fn skip_rlp_item_handles_short_string() {
        let data = &[0x85, b'h', b'e', b'l', b'l', b'o'];
        let mut cursor = &data[..];
        let slice = skip_rlp_item(data, &mut cursor).unwrap();
        assert_eq!(slice, data);
        assert!(cursor.is_empty());
    }
}

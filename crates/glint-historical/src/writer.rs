use arrow::{
    array::{
        Array, AsArray, BinaryArray, FixedSizeBinaryArray, MapArray, StringArray, UInt8Array,
        UInt32Array, UInt64Array,
    },
    record_batch::RecordBatch,
};
use eyre::WrapErr;
use glint_primitives::exex_schema::columns;
use rusqlite::Connection;

use crate::schema;

#[allow(clippy::cast_possible_wrap)]
pub fn insert_batch(conn: &Connection, batch: &RecordBatch) -> eyre::Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let block_number_col = col_u64(batch, columns::BLOCK_NUMBER)?;
    let block_hash_col = col_fsb(batch, columns::BLOCK_HASH)?;
    let tx_index_col = col_u32(batch, columns::TX_INDEX)?;
    let tx_hash_col = col_fsb(batch, columns::TX_HASH)?;
    let log_index_col = col_u32(batch, columns::LOG_INDEX)?;
    let event_type_col = col_u8(batch, columns::EVENT_TYPE)?;
    let entity_key_col = col_fsb(batch, columns::ENTITY_KEY)?;
    let owner_col = col_fsb(batch, columns::OWNER)?;
    let expires_col = col_u64(batch, columns::EXPIRES_AT_BLOCK)?;
    let old_expires_col = col_u64(batch, columns::OLD_EXPIRES_AT_BLOCK)?;
    let content_type_col = col_string(batch, columns::CONTENT_TYPE)?;
    let payload_col = col_binary(batch, columns::PAYLOAD)?;
    let str_ann_col = col_map(batch, columns::STRING_ANNOTATIONS)?;
    let num_ann_col = col_map(batch, columns::NUMERIC_ANNOTATIONS)?;
    let extend_policy_col = col_u8(batch, columns::EXTEND_POLICY)?;
    let operator_col = col_fsb(batch, columns::OPERATOR)?;

    let tx = conn
        .unchecked_transaction()
        .wrap_err("starting SQLite transaction")?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR REPLACE INTO entity_events (
                block_number, block_hash, tx_index, tx_hash, log_index,
                event_type, entity_key, owner, expires_at_block, old_expires_at_block,
                content_type, payload, string_annotations, numeric_annotations,
                extend_policy, operator
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        )?;

        for i in 0..batch.num_rows() {
            let block_number = block_number_col.value(i);
            let block_hash = block_hash_col.value(i);
            let tx_index = i64::from(tx_index_col.value(i));
            let tx_hash = tx_hash_col.value(i);
            let log_index = i64::from(log_index_col.value(i));
            let event_type = i64::from(event_type_col.value(i));
            let entity_key = entity_key_col.value(i);

            validate_blob_len(block_hash, 32, "block_hash")?;
            validate_blob_len(tx_hash, 32, "tx_hash")?;
            validate_blob_len(entity_key, 32, "entity_key")?;

            let owner: Option<&[u8]> = nullable_blob(owner_col, i, 20, "owner")?;
            let expires_at: Option<i64> = nullable_u64_as_i64(expires_col, i);
            let old_expires_at: Option<i64> = nullable_u64_as_i64(old_expires_col, i);
            let content_type: Option<&str> = nullable_str(content_type_col, i);
            let payload: Option<&[u8]> = nullable_bytes(payload_col, i);

            let string_annotations = encode_string_map(str_ann_col, i)?;
            let numeric_annotations = encode_numeric_map(num_ann_col, i)?;

            let extend_policy: Option<i64> = nullable_u8_as_i64(extend_policy_col, i);
            let operator: Option<&[u8]> = nullable_blob(operator_col, i, 20, "operator")?;

            stmt.execute(rusqlite::params![
                (block_number as i64),
                block_hash,
                tx_index,
                tx_hash,
                log_index,
                event_type,
                entity_key,
                owner,
                expires_at,
                old_expires_at,
                content_type,
                payload,
                string_annotations,
                numeric_annotations,
                extend_policy,
                operator,
            ])?;
        }
    }

    let max_block = (0..batch.num_rows())
        .map(|i| block_number_col.value(i))
        .max()
        .expect("non-empty batch");
    schema::set_last_processed_block(&tx, max_block)?;

    tx.commit().wrap_err("committing SQLite transaction")?;
    Ok(())
}

fn validate_blob_len(blob: &[u8], expected: usize, name: &str) -> eyre::Result<()> {
    if blob.len() != expected {
        eyre::bail!("{name} blob length {}, expected {expected}", blob.len());
    }
    Ok(())
}

fn nullable_str(col: &StringArray, i: usize) -> Option<&str> {
    (!col.is_null(i)).then(|| col.value(i))
}

fn nullable_bytes(col: &BinaryArray, i: usize) -> Option<&[u8]> {
    (!col.is_null(i)).then(|| col.value(i))
}

fn nullable_blob<'a>(
    col: &'a FixedSizeBinaryArray,
    i: usize,
    expected_len: usize,
    name: &str,
) -> eyre::Result<Option<&'a [u8]>> {
    if col.is_null(i) {
        return Ok(None);
    }
    let v = col.value(i);
    validate_blob_len(v, expected_len, name)?;
    Ok(Some(v))
}

#[allow(clippy::cast_possible_wrap)]
fn nullable_u64_as_i64(col: &UInt64Array, i: usize) -> Option<i64> {
    (!col.is_null(i)).then(|| col.value(i) as i64)
}

fn nullable_u8_as_i64(col: &UInt8Array, i: usize) -> Option<i64> {
    (!col.is_null(i)).then(|| i64::from(col.value(i)))
}

macro_rules! col {
    ($batch:expr, $name:expr, $ty:ty) => {
        $batch
            .column_by_name($name)
            .ok_or_else(|| eyre::eyre!("missing column: {}", $name))?
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| eyre::eyre!("column {} is not {}", $name, stringify!($ty)))
    };
}

fn col_u8<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt8Array> {
    col!(batch, name, UInt8Array)
}

fn col_u32<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt32Array> {
    col!(batch, name, UInt32Array)
}

fn col_u64<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt64Array> {
    col!(batch, name, UInt64Array)
}

fn col_fsb<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a FixedSizeBinaryArray> {
    col!(batch, name, FixedSizeBinaryArray)
}

fn col_string<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a StringArray> {
    col!(batch, name, StringArray)
}

fn col_binary<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a BinaryArray> {
    col!(batch, name, BinaryArray)
}

fn col_map<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a MapArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| eyre::eyre!("missing column: {name}"))?
        .as_map_opt()
        .ok_or_else(|| eyre::eyre!("column {name} is not MapArray"))
}

#[allow(clippy::cast_sign_loss)]
fn encode_string_map(col: &MapArray, i: usize) -> eyre::Result<Option<String>> {
    if col.is_null(i) {
        return Ok(None);
    }
    let offsets = col.value_offsets();
    let start = offsets[i] as usize;
    let end = offsets[i + 1] as usize;
    if start == end {
        return Ok(Some("[]".to_owned()));
    }
    let keys = col.keys().as_string::<i32>();
    let values = col.values().as_string::<i32>();
    let pairs: Vec<[&str; 2]> = (start..end)
        .map(|j| [keys.value(j), values.value(j)])
        .collect();
    Ok(Some(serde_json::to_string(&pairs)?))
}

#[allow(clippy::cast_sign_loss)]
fn encode_numeric_map(col: &MapArray, i: usize) -> eyre::Result<Option<String>> {
    if col.is_null(i) {
        return Ok(None);
    }
    let offsets = col.value_offsets();
    let start = offsets[i] as usize;
    let end = offsets[i + 1] as usize;
    if start == end {
        return Ok(Some("[]".to_owned()));
    }
    let keys = col.keys().as_string::<i32>();
    let values = col.values().as_primitive::<arrow::datatypes::UInt64Type>();
    let pairs: Vec<serde_json::Value> = (start..end)
        .map(|j| serde_json::json!([keys.value(j), values.value(j)]))
        .collect();
    Ok(Some(serde_json::to_string(&pairs)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;

    use glint_primitives::exex_schema::entity_events_schema;
    use glint_primitives::test_utils::{EventBuilder, build_batch};

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::create_tables(&conn).unwrap();
        conn
    }

    #[test]
    fn insert_batch_single_row() {
        let conn = setup_db();
        let batch = build_batch(&[EventBuilder::created(10, 0x01)]);
        insert_batch(&conn, &batch).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let block: i64 = conn
            .query_row("SELECT block_number FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(block, 10);

        assert_eq!(schema::get_last_processed_block(&conn).unwrap(), Some(10));
    }

    #[test]
    fn insert_batch_empty_is_noop() {
        let conn = setup_db();
        let schema = entity_events_schema();
        let batch = RecordBatch::new_empty(schema);
        insert_batch(&conn, &batch).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn insert_batch_duplicate_is_idempotent() {
        let conn = setup_db();
        let batch = build_batch(&[EventBuilder::created(10, 0x01)]);
        insert_batch(&conn, &batch).unwrap();
        insert_batch(&conn, &batch).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn annotations_stored_as_json() {
        let conn = setup_db();
        let batch = build_batch(&[EventBuilder::created(10, 0x01)
            .with_string_annotations(vec![("sk".into(), "sv".into())])
            .with_numeric_annotations(vec![("nk".into(), 99)])]);
        insert_batch(&conn, &batch).unwrap();

        let str_ann: String = conn
            .query_row("SELECT string_annotations FROM entity_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        let parsed: Vec<Vec<String>> = serde_json::from_str(&str_ann).unwrap();
        assert_eq!(parsed, vec![vec!["sk", "sv"]]);

        let num_ann: String = conn
            .query_row("SELECT numeric_annotations FROM entity_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&num_ann).unwrap();
        assert_eq!(parsed, vec![serde_json::json!(["nk", 99])]);
    }
}

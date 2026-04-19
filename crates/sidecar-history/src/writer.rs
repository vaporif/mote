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
    let gas_cost_col = col_u64(batch, columns::GAS_COST)?;

    let tx = conn
        .unchecked_transaction()
        .wrap_err("starting SQLite transaction")?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO entity_events (
                block_number, block_hash, tx_index, tx_hash, log_index,
                event_type, entity_key, owner, expires_at_block, old_expires_at_block,
                content_type, payload, extend_policy, operator, gas_cost
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT (entity_key, block_number, log_index) DO UPDATE SET
                block_hash = excluded.block_hash,
                tx_index = excluded.tx_index,
                tx_hash = excluded.tx_hash,
                event_type = excluded.event_type,
                owner = excluded.owner,
                expires_at_block = excluded.expires_at_block,
                old_expires_at_block = excluded.old_expires_at_block,
                content_type = excluded.content_type,
                payload = excluded.payload,
                extend_policy = excluded.extend_policy,
                operator = excluded.operator,
                gas_cost = excluded.gas_cost",
        )?;

        let mut insert_str_ann = tx.prepare_cached(
            "INSERT INTO event_string_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (entity_key, block_number, log_index, ann_key) DO UPDATE SET ann_value = excluded.ann_value",
        )?;

        let mut insert_num_ann = tx.prepare_cached(
            "INSERT INTO event_numeric_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (entity_key, block_number, log_index, ann_key) DO UPDATE SET ann_value = excluded.ann_value",
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
            let expires_at: Option<i64> = nullable_u64_as_i64(expires_col, i)?;
            let old_expires_at: Option<i64> = nullable_u64_as_i64(old_expires_col, i)?;
            let content_type: Option<&str> = nullable_str(content_type_col, i);
            let payload: Option<&[u8]> = nullable_bytes(payload_col, i);

            let extend_policy: Option<i64> = nullable_u8_as_i64(extend_policy_col, i);
            let operator: Option<&[u8]> = nullable_blob(operator_col, i, 20, "operator")?;
            let gas_cost: Option<i64> = nullable_u64_as_i64(gas_cost_col, i)?;

            stmt.execute(rusqlite::params![
                i64::try_from(block_number)?,
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
                extend_policy,
                operator,
                gas_cost,
            ])?;

            let block_number_i64 = i64::try_from(block_number)?;

            if !str_ann_col.is_null(i) {
                let offsets = str_ann_col.value_offsets();
                let start = usize::try_from(offsets[i])?;
                let end = usize::try_from(offsets[i + 1])?;
                let keys = str_ann_col.keys().as_string::<i32>();
                let values = str_ann_col.values().as_string::<i32>();
                for j in start..end {
                    insert_str_ann.execute(rusqlite::params![
                        entity_key,
                        block_number_i64,
                        log_index,
                        keys.value(j),
                        values.value(j),
                    ])?;
                }
            }
            if !num_ann_col.is_null(i) {
                let offsets = num_ann_col.value_offsets();
                let start = usize::try_from(offsets[i])?;
                let end = usize::try_from(offsets[i + 1])?;
                let keys = num_ann_col.keys().as_string::<i32>();
                let values = num_ann_col
                    .values()
                    .as_primitive::<arrow::datatypes::UInt64Type>();
                for j in start..end {
                    insert_num_ann.execute(rusqlite::params![
                        entity_key,
                        block_number_i64,
                        log_index,
                        keys.value(j),
                        i64::try_from(values.value(j))?,
                    ])?;
                }
            }
        }
    }

    let max_block = (0..batch.num_rows())
        .map(|i| block_number_col.value(i))
        .max()
        .expect("non-empty batch");
    schema::set_last_processed_block(&tx, max_block)?;

    tx.commit().wrap_err("committing SQLite transaction")?;

    let metrics = crate::metrics::HistoricalMetrics::default();
    metrics
        .events_stored_total
        .increment(batch.num_rows() as u64);

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

fn nullable_u64_as_i64(col: &UInt64Array, i: usize) -> eyre::Result<Option<i64>> {
    if col.is_null(i) {
        return Ok(None);
    }
    Ok(Some(i64::try_from(col.value(i))?))
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
    fn annotations_stored_normalized() {
        let conn = setup_db();
        let batch = build_batch(&[EventBuilder::created(10, 0x01)
            .with_string_annotations(vec![("sk".into(), "sv".into())])
            .with_numeric_annotations(vec![("nk".into(), 99)])]);
        insert_batch(&conn, &batch).unwrap();

        let (ann_key, ann_value): (String, String) = conn
            .query_row(
                "SELECT ann_key, ann_value FROM event_string_annotations",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ann_key, "sk");
        assert_eq!(ann_value, "sv");

        let (ann_key, ann_value): (String, i64) = conn
            .query_row(
                "SELECT ann_key, ann_value FROM event_numeric_annotations",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ann_key, "nk");
        assert_eq!(ann_value, 99);
    }
}

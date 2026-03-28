use std::sync::Arc;

use parking_lot::Mutex;

use datafusion::prelude::*;
use glint_historical::{provider::HistoricalTableProvider, schema, writer};
use glint_primitives::test_utils::{EventBuilder, build_batch};

#[tokio::test]
async fn write_then_query_via_datafusion() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    writer::insert_batch(&conn, &build_batch(&[EventBuilder::created(10, 0x01)])).unwrap();
    writer::insert_batch(&conn, &build_batch(&[EventBuilder::created(20, 0x02)])).unwrap();
    writer::insert_batch(&conn, &build_batch(&[EventBuilder::created(30, 0x03)])).unwrap();

    let conn = Arc::new(Mutex::new(conn));
    let provider = HistoricalTableProvider::new(conn);

    let ctx = SessionContext::new();
    ctx.register_table("entities", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT block_number, event_type FROM entities WHERE block_number BETWEEN 10 AND 20")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "expected 2 events in block range 10-20");
}

#[tokio::test]
async fn query_without_block_range_errors() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    let conn = Arc::new(Mutex::new(conn));
    let provider = HistoricalTableProvider::new(conn);

    let ctx = SessionContext::new();
    ctx.register_table("entities", Arc::new(provider)).unwrap();

    let result = ctx
        .sql("SELECT * FROM entities WHERE event_type = 0")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(
        result.is_err(),
        "expected error for query without block range"
    );
}

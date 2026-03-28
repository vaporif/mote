#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::missing_const_for_fn,
    clippy::redundant_closure_for_method_calls
)]

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use datafusion::prelude::*;
use glint_historical::{provider::HistoricalTableProvider, schema, writer};
use glint_primitives::exex_schema::columns;
use glint_primitives::exex_types::EntityEventType;
use glint_primitives::test_utils::{EventBuilder, build_batch};
use parking_lot::Mutex;

fn setup_db_with_events(events: &[EventBuilder]) -> Arc<Mutex<rusqlite::Connection>> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();
    let batch = build_batch(events);
    writer::insert_batch(&conn, &batch).unwrap();
    Arc::new(Mutex::new(conn))
}

fn setup_session(conn: Arc<Mutex<rusqlite::Connection>>) -> SessionContext {
    let provider = HistoricalTableProvider::new(conn);
    let ctx = SessionContext::new();
    ctx.register_table("entities", Arc::new(provider)).unwrap();
    ctx
}

async fn query_count(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn query_u64_column(ctx: &SessionContext, sql: &str) -> Vec<u64> {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut result = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        for i in 0..col.len() {
            result.push(col.value(i));
        }
    }
    result
}

async fn query_u8_column(ctx: &SessionContext, sql: &str) -> Vec<u8> {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut result = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt8Type>();
        for i in 0..col.len() {
            result.push(col.value(i));
        }
    }
    result
}

#[tokio::test]
async fn e2e_write_multiple_blocks_query_range() {
    let events: Vec<_> = (1..=10)
        .map(|i| EventBuilder::created(i * 10, i as u8))
        .collect();

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let count = query_count(
        &ctx,
        "SELECT * FROM entities WHERE block_number BETWEEN 30 AND 70",
    )
    .await;
    assert_eq!(count, 5, "blocks 30,40,50,60,70");
}

#[tokio::test]
async fn e2e_block_range_with_gt_lt_operators() {
    let events: Vec<_> = (1..=5)
        .map(|i| EventBuilder::created(i * 100, i as u8))
        .collect();

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let blocks = query_u64_column(
        &ctx,
        "SELECT block_number FROM entities WHERE block_number >= 200 AND block_number <= 400 ORDER BY block_number",
    )
    .await;
    assert_eq!(blocks, vec![200, 300, 400]);
}

#[tokio::test]
async fn e2e_multiple_events_same_block() {
    let events = vec![
        EventBuilder::created(100, 0x01).with_log_index(0),
        EventBuilder::created(100, 0x02).with_log_index(1),
        EventBuilder::created(100, 0x03).with_log_index(2),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let count = query_count(
        &ctx,
        "SELECT * FROM entities WHERE block_number BETWEEN 100 AND 100",
    )
    .await;
    assert_eq!(count, 3);
}

#[tokio::test]
async fn e2e_event_types_roundtrip() {
    let events = vec![
        EventBuilder::created(10, 0x01),
        EventBuilder::extended(20, 0x01, 110, 220),
        EventBuilder::deleted(30, 0x01).with_log_index(0),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let types = query_u8_column(
        &ctx,
        "SELECT event_type FROM entities WHERE block_number BETWEEN 10 AND 30 ORDER BY block_number",
    )
    .await;
    assert_eq!(
        types,
        vec![
            EntityEventType::Created as u8,
            EntityEventType::Extended as u8,
            EntityEventType::Deleted as u8,
        ]
    );

    let types = query_u8_column(
        &ctx,
        "SELECT event_type FROM entities WHERE block_number BETWEEN 10 AND 10",
    )
    .await;
    assert_eq!(types, vec![EntityEventType::Created as u8]);

    let types = query_u8_column(
        &ctx,
        "SELECT event_type FROM entities WHERE block_number BETWEEN 20 AND 20",
    )
    .await;
    assert_eq!(types, vec![EntityEventType::Extended as u8]);

    let types = query_u8_column(
        &ctx,
        "SELECT event_type FROM entities WHERE block_number BETWEEN 30 AND 30",
    )
    .await;
    assert_eq!(types, vec![EntityEventType::Deleted as u8]);
}

#[tokio::test]
async fn e2e_annotations_survive_roundtrip() {
    let events = vec![
        EventBuilder::created(50, 0x01)
            .with_string_annotations(vec![
                ("color".into(), "blue".into()),
                ("shape".into(), "circle".into()),
            ])
            .with_numeric_annotations(vec![("weight".into(), 42), ("height".into(), 100)]),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let df = ctx
        .sql("SELECT string_annotations, numeric_annotations FROM entities WHERE block_number BETWEEN 50 AND 50")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let str_map = batches[0]
        .column_by_name(columns::STRING_ANNOTATIONS)
        .unwrap();
    let str_map = str_map.as_map();
    let keys = str_map.keys().as_string::<i32>();
    let values = str_map.values().as_string::<i32>();
    let offsets = str_map.value_offsets();
    let start = offsets[0] as usize;
    let end = offsets[1] as usize;

    let pairs: Vec<_> = (start..end)
        .map(|j| (keys.value(j).to_owned(), values.value(j).to_owned()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("color".to_owned(), "blue".to_owned()),
            ("shape".to_owned(), "circle".to_owned()),
        ]
    );

    let num_map = batches[0]
        .column_by_name(columns::NUMERIC_ANNOTATIONS)
        .unwrap();
    let num_map = num_map.as_map();
    let keys = num_map.keys().as_string::<i32>();
    let values = num_map
        .values()
        .as_primitive::<arrow::datatypes::UInt64Type>();
    let offsets = num_map.value_offsets();
    let start = offsets[0] as usize;
    let end = offsets[1] as usize;

    let pairs: Vec<_> = (start..end)
        .map(|j| (keys.value(j).to_owned(), values.value(j)))
        .collect();
    assert_eq!(
        pairs,
        vec![("weight".to_owned(), 42), ("height".to_owned(), 100),]
    );
}

#[tokio::test]
async fn e2e_empty_range_returns_nothing() {
    let events = vec![
        EventBuilder::created(10, 0x01),
        EventBuilder::created(20, 0x02),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let count = query_count(
        &ctx,
        "SELECT * FROM entities WHERE block_number BETWEEN 50 AND 100",
    )
    .await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn e2e_single_block_range() {
    let events = vec![
        EventBuilder::created(10, 0x01),
        EventBuilder::created(20, 0x02),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let blocks = query_u64_column(
        &ctx,
        "SELECT block_number FROM entities WHERE block_number BETWEEN 10 AND 10",
    )
    .await;
    assert_eq!(blocks, vec![10]);
}

#[tokio::test]
async fn e2e_query_without_block_range_fails() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();
    let conn = Arc::new(Mutex::new(conn));
    let ctx = setup_session(conn);

    let result = ctx
        .sql("SELECT * FROM entities WHERE event_type = 0")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn e2e_large_batch_insert_and_query() {
    let events: Vec<_> = (0..100)
        .map(|i| EventBuilder::created(i + 1, ((i % 255) + 1) as u8).with_log_index(i as u32))
        .collect();

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let count = query_count(
        &ctx,
        "SELECT * FROM entities WHERE block_number BETWEEN 1 AND 100",
    )
    .await;
    assert_eq!(count, 100);

    let count = query_count(
        &ctx,
        "SELECT * FROM entities WHERE block_number BETWEEN 50 AND 60",
    )
    .await;
    assert_eq!(count, 11);
}

#[tokio::test]
async fn e2e_multiple_batches_insert_then_query() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    let batch1 = build_batch(&[
        EventBuilder::created(10, 0x01),
        EventBuilder::created(20, 0x02),
    ]);
    let batch2 = build_batch(&[
        EventBuilder::created(30, 0x03),
        EventBuilder::created(40, 0x04),
    ]);

    writer::insert_batch(&conn, &batch1).unwrap();
    writer::insert_batch(&conn, &batch2).unwrap();

    let conn = Arc::new(Mutex::new(conn));
    let ctx = setup_session(conn);

    let blocks = query_u64_column(
        &ctx,
        "SELECT block_number FROM entities WHERE block_number BETWEEN 1 AND 50 ORDER BY block_number",
    )
    .await;
    assert_eq!(blocks, vec![10, 20, 30, 40]);
}

#[test]
fn e2e_last_processed_block_updated() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    assert_eq!(schema::get_last_processed_block(&conn).unwrap(), None);

    let batch = build_batch(&[EventBuilder::created(42, 0x01)]);
    writer::insert_batch(&conn, &batch).unwrap();
    assert_eq!(schema::get_last_processed_block(&conn).unwrap(), Some(42));

    let batch = build_batch(&[EventBuilder::created(99, 0x02)]);
    writer::insert_batch(&conn, &batch).unwrap();
    assert_eq!(schema::get_last_processed_block(&conn).unwrap(), Some(99));
}

#[tokio::test]
async fn e2e_delete_events_from_block_and_requery() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    let batch = build_batch(&[
        EventBuilder::created(10, 0x01),
        EventBuilder::created(20, 0x02),
        EventBuilder::created(30, 0x03),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    schema::delete_events_from_block(&conn, 20).unwrap();

    let conn = Arc::new(Mutex::new(conn));
    let ctx = setup_session(conn);

    let blocks = query_u64_column(
        &ctx,
        "SELECT block_number FROM entities WHERE block_number BETWEEN 1 AND 100 ORDER BY block_number",
    )
    .await;
    assert_eq!(blocks, vec![10]);
}

#[tokio::test]
async fn e2e_prune_before_block_and_requery() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    let batch = build_batch(&[
        EventBuilder::created(10, 0x01),
        EventBuilder::created(20, 0x02),
        EventBuilder::created(30, 0x03),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    schema::prune_before_block(&conn, 20).unwrap();

    let conn = Arc::new(Mutex::new(conn));
    let ctx = setup_session(conn);

    let blocks = query_u64_column(
        &ctx,
        "SELECT block_number FROM entities WHERE block_number BETWEEN 1 AND 100 ORDER BY block_number",
    )
    .await;
    assert_eq!(blocks, vec![20, 30]);
}

#[test]
fn e2e_drop_and_recreate_clears_data() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    let batch = build_batch(&[EventBuilder::created(10, 0x01)]);
    writer::insert_batch(&conn, &batch).unwrap();
    assert_eq!(schema::event_count(&conn).unwrap(), 1);

    schema::drop_and_recreate(&conn).unwrap();
    assert_eq!(schema::event_count(&conn).unwrap(), 0);
    assert_eq!(schema::get_last_processed_block(&conn).unwrap(), None);
}

#[tokio::test]
async fn e2e_owner_and_entity_key_readable() {
    let events = vec![EventBuilder::created(50, 0xAB)];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let df = ctx
        .sql("SELECT entity_key, owner FROM entities WHERE block_number BETWEEN 50 AND 50")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let entity_key_col = batches[0]
        .column_by_name(columns::ENTITY_KEY)
        .unwrap()
        .as_fixed_size_binary();
    assert_eq!(
        entity_key_col.value(0),
        alloy_primitives::B256::repeat_byte(0xAB).as_slice()
    );

    let owner_col = batches[0]
        .column_by_name(columns::OWNER)
        .unwrap()
        .as_fixed_size_binary();
    assert_eq!(
        owner_col.value(0),
        alloy_primitives::Address::repeat_byte(0xAB).as_slice()
    );
}

#[tokio::test]
async fn e2e_ordering_by_block_and_log_index() {
    let events = vec![
        EventBuilder::created(20, 0x03).with_log_index(0),
        EventBuilder::created(10, 0x01).with_log_index(0),
        EventBuilder::created(10, 0x02).with_log_index(1),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let blocks = query_u64_column(
        &ctx,
        "SELECT block_number FROM entities WHERE block_number BETWEEN 1 AND 100",
    )
    .await;
    assert_eq!(blocks, vec![10, 10, 20]);
}

#[tokio::test]
async fn e2e_null_optional_fields() {
    let events = vec![EventBuilder::deleted(10, 0x01)];

    let conn = setup_db_with_events(&events);
    let ctx = setup_session(conn);

    let df = ctx
        .sql("SELECT expires_at_block, content_type, payload FROM entities WHERE block_number BETWEEN 10 AND 10")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    assert!(
        batches[0]
            .column_by_name(columns::EXPIRES_AT_BLOCK)
            .unwrap()
            .is_null(0)
    );
    assert!(
        batches[0]
            .column_by_name(columns::CONTENT_TYPE)
            .unwrap()
            .is_null(0)
    );
    assert!(
        batches[0]
            .column_by_name(columns::PAYLOAD)
            .unwrap()
            .is_null(0)
    );
}

#[tokio::test]
async fn e2e_file_backed_db_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        schema::create_tables(&conn).unwrap();
        let batch = build_batch(&[
            EventBuilder::created(10, 0x01),
            EventBuilder::created(20, 0x02),
        ]);
        writer::insert_batch(&conn, &batch).unwrap();
    }

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    schema::check_schema_version(&conn).unwrap();
    assert_eq!(schema::get_last_processed_block(&conn).unwrap(), Some(20));

    let conn = Arc::new(Mutex::new(conn));
    let ctx = setup_session(conn);

    let count = query_count(
        &ctx,
        "SELECT * FROM entities WHERE block_number BETWEEN 1 AND 100",
    )
    .await;
    assert_eq!(count, 2);
}

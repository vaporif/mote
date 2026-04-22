#![allow(
    clippy::missing_const_for_fn,
    clippy::redundant_closure_for_method_calls
)]

use std::sync::Arc;

use datafusion::prelude::*;
use glint_historical::provider::entities_history::EntitiesHistoryProvider;
use glint_historical::provider::history_numeric_annotations::HistoryNumericAnnotationsProvider;
use glint_historical::provider::history_string_annotations::HistoryStringAnnotationsProvider;
use glint_historical::{schema, writer};
use glint_primitives::test_utils::{EventBuilder, build_batch};
use parking_lot::Mutex;

fn setup_db_with_events(events: &[EventBuilder]) -> Arc<Mutex<rusqlite::Connection>> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();
    let batch = build_batch(events);
    writer::insert_batch(&conn, &batch).unwrap();
    Arc::new(Mutex::new(conn))
}

fn register_history_tables(ctx: &SessionContext, conn: Arc<Mutex<rusqlite::Connection>>) {
    ctx.register_table(
        "entities_history",
        Arc::new(EntitiesHistoryProvider::new(Arc::clone(&conn))),
    )
    .unwrap();
    ctx.register_table(
        "history_string_annotations",
        Arc::new(HistoryStringAnnotationsProvider::new(Arc::clone(&conn))),
    )
    .unwrap();
    ctx.register_table(
        "history_numeric_annotations",
        Arc::new(HistoryNumericAnnotationsProvider::new(conn)),
    )
    .unwrap();
}

async fn query_count(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn point_in_time_query_returns_active_version() {
    // Entity 0x01 created at block 10, updated at block 20.
    // The history writer closes the block-10 row (valid_to_block=20) and opens a
    // new row at block 20 (valid_to_block=NULL).
    let events = vec![
        EventBuilder::created(10, 0x01),
        EventBuilder::updated(20, 0x01, 110, 220),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = SessionContext::new();
    register_history_tables(&ctx, conn);

    // At block 15 only the version from block 10 should be visible
    let count = query_count(
        &ctx,
        "SELECT * FROM entities_history WHERE block_number = 15",
    )
    .await;
    assert_eq!(count, 1, "block 15 should see version from block 10");

    // At block 25 only the version from block 20 should be visible
    let count = query_count(
        &ctx,
        "SELECT * FROM entities_history WHERE block_number = 25",
    )
    .await;
    assert_eq!(count, 1, "block 25 should see version from block 20");
}

#[tokio::test]
async fn query_without_block_filter_errors() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();
    let conn = Arc::new(Mutex::new(conn));
    let ctx = SessionContext::new();
    register_history_tables(&ctx, conn);

    let result = ctx
        .sql("SELECT * FROM entities_history WHERE expires_at_block = 100")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(result.is_err(), "query without block filter should fail");
}

#[tokio::test]
async fn deleted_entity_not_visible_after_deletion() {
    let events = vec![
        EventBuilder::created(10, 0x01),
        EventBuilder::deleted(20, 0x01),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = SessionContext::new();
    register_history_tables(&ctx, conn);

    // At block 15 the entity should be visible
    let count = query_count(
        &ctx,
        "SELECT * FROM entities_history WHERE block_number = 15",
    )
    .await;
    assert_eq!(count, 1, "block 15 should see entity before deletion");

    // At block 25 the entity should not be visible (deleted at 20)
    let count = query_count(
        &ctx,
        "SELECT * FROM entities_history WHERE block_number = 25",
    )
    .await;
    assert_eq!(count, 0, "block 25 should not see deleted entity");
}

#[tokio::test]
async fn history_annotations_queryable() {
    let events = vec![
        EventBuilder::created(10, 0x01)
            .with_string_annotations(vec![("color".into(), "red".into())])
            .with_numeric_annotations(vec![("weight".into(), 42)]),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = SessionContext::new();
    register_history_tables(&ctx, conn);

    let count = query_count(
        &ctx,
        "SELECT * FROM history_string_annotations \
         WHERE valid_from_block >= 10 AND valid_from_block <= 10",
    )
    .await;
    assert_eq!(count, 1, "should find string annotation");

    let count = query_count(
        &ctx,
        "SELECT * FROM history_numeric_annotations \
         WHERE valid_from_block >= 10 AND valid_from_block <= 10",
    )
    .await;
    assert_eq!(count, 1, "should find numeric annotation");

    // Verify pushdown filtering works
    let count = query_count(
        &ctx,
        "SELECT * FROM history_string_annotations \
         WHERE valid_from_block >= 10 AND valid_from_block <= 10 \
           AND ann_key = 'color' AND ann_value = 'red'",
    )
    .await;
    assert_eq!(count, 1, "pushdown filter should match");

    let count = query_count(
        &ctx,
        "SELECT * FROM history_string_annotations \
         WHERE valid_from_block >= 10 AND valid_from_block <= 10 \
           AND ann_key = 'nonexistent'",
    )
    .await;
    assert_eq!(count, 0, "pushdown filter should exclude non-matching");
}

#[tokio::test]
async fn join_entities_history_with_annotations() {
    let events = vec![
        EventBuilder::created(10, 0x01)
            .with_string_annotations(vec![("env".into(), "prod".into())]),
    ];

    let conn = setup_db_with_events(&events);
    let ctx = SessionContext::new();
    register_history_tables(&ctx, conn);

    let count = query_count(
        &ctx,
        "SELECT e.entity_key, s.ann_key, s.ann_value \
         FROM entities_history e \
         JOIN history_string_annotations s \
           ON e.entity_key = s.entity_key AND e.valid_from_block = s.valid_from_block \
         WHERE e.block_number = 10 \
           AND s.valid_from_block >= 10 AND s.valid_from_block <= 10 \
           AND s.ann_key = 'env'",
    )
    .await;
    assert_eq!(count, 1, "join should produce one matching row");
}

mod entities;
mod filter_pushdown;
mod numeric_annotations;
mod string_annotations;

use std::sync::Arc;

use datafusion::{common::Result as DfResult, execution::context::SessionContext};
use tokio::sync::watch;

use crate::entity_store::Snapshot;

pub fn register_tables(
    ctx: &SessionContext,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
) -> DfResult<()> {
    ctx.register_table(
        "entities_latest",
        Arc::new(entities::EntitiesTable::new(snapshot_rx.clone())),
    )?;
    ctx.register_table(
        "entity_string_annotations",
        Arc::new(string_annotations::StringAnnotationsTable::new(
            snapshot_rx.clone(),
        )),
    )?;
    ctx.register_table(
        "entity_numeric_annotations",
        Arc::new(numeric_annotations::NumericAnnotationsTable::new(
            snapshot_rx,
        )),
    )?;
    Ok(())
}

pub fn create_session_context(
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
) -> DfResult<SessionContext> {
    let ctx = SessionContext::new();
    register_tables(&ctx, snapshot_rx)?;
    Ok(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_store::{EntityRow, EntityStore};
    use alloy_primitives::{Address, B256, Bytes};

    fn sample_store() -> EntityStore {
        let mut store = EntityStore::new();
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x01),
            owner: Address::repeat_byte(0x02),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_annotations: vec![("category".into(), "nft".into())],
            numeric_annotations: vec![("price".into(), 1000)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: Some(Address::ZERO),
        });
        store
    }

    fn make_ctx() -> SessionContext {
        let store = sample_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot"));
        let (tx, _) = tokio::sync::watch::channel(snapshot);
        create_session_context(tx.subscribe()).unwrap()
    }

    #[tokio::test]
    async fn query_entities_table() {
        let ctx = make_ctx();
        let df = ctx
            .sql("SELECT content_type FROM entities_latest")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
        assert_eq!(results[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn entities_table_has_no_annotation_columns() {
        let ctx = make_ctx();
        let df = ctx.sql("SELECT * FROM entities_latest").await.unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_columns(), 9);
    }

    #[tokio::test]
    async fn join_string_annotation() {
        let ctx = make_ctx();
        let df = ctx
            .sql(
                "SELECT e.content_type FROM entities_latest e \
                 JOIN entity_string_annotations s USING (entity_key) \
                 WHERE s.ann_key = 'category' AND s.ann_value = 'nft'",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn join_numeric_annotation() {
        let ctx = make_ctx();
        let df = ctx
            .sql(
                "SELECT e.content_type FROM entities_latest e \
                 JOIN entity_numeric_annotations n USING (entity_key) \
                 WHERE n.ann_key = 'price' AND n.ann_value > 500",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn no_annotation_match_returns_empty() {
        let ctx = make_ctx();
        let df = ctx
            .sql(
                "SELECT e.content_type FROM entities_latest e \
                 JOIN entity_string_annotations s USING (entity_key) \
                 WHERE s.ann_key = 'category' AND s.ann_value = 'defi'",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn empty_store_all_tables_zero_rows() {
        let store = EntityStore::new();
        let snapshot = Arc::new(store.snapshot().unwrap());
        let (tx, _) = tokio::sync::watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();

        for table in &[
            "entities_latest",
            "entity_string_annotations",
            "entity_numeric_annotations",
        ] {
            let df = ctx.sql(&format!("SELECT * FROM {table}")).await.unwrap();
            let results = df.collect().await.unwrap();
            let total: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 0, "table {table} should be empty");
        }
    }

    #[tokio::test]
    async fn owner_filter_returns_only_matching_entity() {
        let mut store = EntityStore::new();
        let owner_a = Address::repeat_byte(0x0A);
        let owner_b = Address::repeat_byte(0x0B);

        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x01),
            owner: owner_a,
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"a"),
            string_annotations: vec![],
            numeric_annotations: vec![],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: None,
        });
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x02),
            owner: owner_b,
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"b"),
            string_annotations: vec![],
            numeric_annotations: vec![],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xBB),
            extend_policy: 0,
            operator: None,
        });

        let snapshot = Arc::new(store.snapshot().unwrap());
        let (tx, _) = tokio::sync::watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();

        let hex = alloy_primitives::hex::encode(owner_a.as_slice());
        let sql = format!("SELECT entity_key FROM entities_latest WHERE owner = X'{hex}'");
        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "should return exactly 1 entity for owner_a");
    }

    #[tokio::test]
    async fn unknown_owner_returns_empty() {
        let ctx = make_ctx();
        let unknown = Address::repeat_byte(0xFF);
        let hex = alloy_primitives::hex::encode(unknown.as_slice());
        let sql = format!("SELECT entity_key FROM entities_latest WHERE owner = X'{hex}'");
        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn string_annotation_filter_narrows_results() {
        let mut store = EntityStore::new();
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x01),
            owner: Address::repeat_byte(0x01),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"a"),
            string_annotations: vec![("tag".into(), "alpha".into())],
            numeric_annotations: vec![],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: None,
        });
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x02),
            owner: Address::repeat_byte(0x02),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"b"),
            string_annotations: vec![("tag".into(), "beta".into())],
            numeric_annotations: vec![],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xBB),
            extend_policy: 0,
            operator: None,
        });

        let snapshot = Arc::new(store.snapshot().unwrap());
        let (tx, _) = tokio::sync::watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();

        let df = ctx
            .sql("SELECT * FROM entity_string_annotations WHERE ann_key = 'tag' AND ann_value = 'alpha'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    fn make_numeric_store() -> EntityStore {
        let mut store = EntityStore::new();
        for (byte, price) in [(0x01, 100), (0x02, 500), (0x03, 1000)] {
            store.insert(EntityRow {
                entity_key: B256::repeat_byte(byte),
                owner: Address::repeat_byte(byte),
                expires_at_block: 200,
                content_type: "text/plain".into(),
                payload: Bytes::from_static(b"x"),
                string_annotations: vec![],
                numeric_annotations: vec![("price".into(), price)],
                created_at_block: 1,
                tx_hash: B256::repeat_byte(byte),
                extend_policy: 0,
                operator: None,
            });
        }
        store
    }

    fn make_numeric_ctx() -> SessionContext {
        let store = make_numeric_store();
        let snapshot = Arc::new(store.snapshot().unwrap());
        let (tx, _) = tokio::sync::watch::channel(snapshot);
        create_session_context(tx.subscribe()).unwrap()
    }

    #[tokio::test]
    async fn numeric_gt_filter() {
        let ctx = make_numeric_ctx();
        let df = ctx
            .sql("SELECT * FROM entity_numeric_annotations WHERE ann_key = 'price' AND ann_value > 100")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "price > 100 should match 2 entities");
    }

    #[tokio::test]
    async fn numeric_lteq_filter() {
        let ctx = make_numeric_ctx();
        let df = ctx
            .sql("SELECT * FROM entity_numeric_annotations WHERE ann_key = 'price' AND ann_value <= 500")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "price <= 500 should match 2 entities");
    }

    #[tokio::test]
    async fn numeric_eq_filter() {
        let ctx = make_numeric_ctx();
        let df = ctx
            .sql("SELECT * FROM entity_numeric_annotations WHERE ann_key = 'price' AND ann_value = 500")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "price = 500 should match exactly one");
    }

    #[tokio::test]
    async fn no_pushdown_filter_returns_full_batch() {
        let ctx = make_ctx();
        let df = ctx
            .sql("SELECT * FROM entities_latest WHERE content_type = 'text/plain'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }
}

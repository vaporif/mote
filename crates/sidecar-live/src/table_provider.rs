use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::Session,
    common::Result as DfResult,
    datasource::{MemTable, TableProvider, TableType},
    execution::context::SessionContext,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use tokio::sync::watch;

use crate::entity_store::Snapshot;

pub struct SnapshotTable {
    name: &'static str,
    schema: SchemaRef,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
    select: fn(&Snapshot) -> &RecordBatch,
}

impl fmt::Debug for SnapshotTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotTable")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl SnapshotTable {
    fn new(
        name: &'static str,
        schema: SchemaRef,
        snapshot_rx: watch::Receiver<Arc<Snapshot>>,
        select: fn(&Snapshot) -> &RecordBatch,
    ) -> Self {
        Self {
            name,
            schema,
            snapshot_rx,
            select,
        }
    }
}

#[async_trait]
impl TableProvider for SnapshotTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // TODO: bitmap pushdown for annotation providers
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let snapshot = Arc::clone(&*self.snapshot_rx.borrow());
        let batch = (self.select)(&snapshot).clone();
        let schema = Arc::clone(&self.schema);
        let mem = MemTable::try_new(schema, vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

pub fn register_tables(
    ctx: &SessionContext,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
) -> DfResult<()> {
    ctx.register_table(
        "entities_latest",
        Arc::new(SnapshotTable::new(
            "entities_latest",
            glint_primitives::exex_schema::entities_latest_schema(),
            snapshot_rx.clone(),
            |s| &s.entities,
        )),
    )?;
    ctx.register_table(
        "entity_string_annotations",
        Arc::new(SnapshotTable::new(
            "entity_string_annotations",
            glint_primitives::exex_schema::string_annotations_schema(),
            snapshot_rx.clone(),
            |s| &s.string_annotations,
        )),
    )?;
    ctx.register_table(
        "entity_numeric_annotations",
        Arc::new(SnapshotTable::new(
            "entity_numeric_annotations",
            glint_primitives::exex_schema::numeric_annotations_schema(),
            snapshot_rx,
            |s| &s.numeric_annotations,
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
}

use std::{any::Any, fmt, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Result as DfResult,
    datasource::{MemTable, TableProvider, TableType},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use tokio::sync::watch;

use crate::entity_store::Snapshot;

pub struct StringAnnotationsTable {
    schema: SchemaRef,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
}

impl fmt::Debug for StringAnnotationsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StringAnnotationsTable")
            .finish_non_exhaustive()
    }
}

impl StringAnnotationsTable {
    pub fn new(snapshot_rx: watch::Receiver<Arc<Snapshot>>) -> Self {
        Self {
            schema: glint_primitives::exex_schema::string_annotations_schema(),
            snapshot_rx,
        }
    }
}

#[async_trait]
impl TableProvider for StringAnnotationsTable {
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
        let has_ann_filter = super::filter_pushdown::extract_string_ann_eq(filters).is_some();
        Ok(filters
            .iter()
            .map(|f| {
                if has_ann_filter && super::filter_pushdown::is_ann_column(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
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
        let filter_refs: Vec<&Expr> = filters.iter().collect();

        let batch = if let Some((key, value)) =
            super::filter_pushdown::extract_string_ann_eq(&filter_refs)
        {
            if let Some(bitmap) = snapshot.indexes.string_ann_index.get(&(key, value)) {
                super::filter_pushdown::filter_annotation_batch_by_bitmap(
                    &snapshot.entities,
                    &snapshot.string_annotations,
                    bitmap,
                    &snapshot.indexes,
                )?
            } else {
                RecordBatch::new_empty(Arc::clone(&self.schema))
            }
        } else {
            snapshot.string_annotations.as_ref().clone()
        };

        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

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

pub struct EntitiesTable {
    schema: SchemaRef,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
}

impl fmt::Debug for EntitiesTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntitiesTable").finish_non_exhaustive()
    }
}

impl EntitiesTable {
    pub fn new(snapshot_rx: watch::Receiver<Arc<Snapshot>>) -> Self {
        Self {
            schema: glint_primitives::exex_schema::entities_latest_schema(),
            snapshot_rx,
        }
    }
}

#[async_trait]
impl TableProvider for EntitiesTable {
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
        Ok(filters
            .iter()
            .map(|f| {
                if super::filter_pushdown::extract_owner_eq(f).is_some() {
                    TableProviderFilterPushDown::Exact
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

        let batch = if let Some(addr) = filters
            .iter()
            .find_map(super::filter_pushdown::extract_owner_eq)
        {
            if let Some(bitmap) = snapshot.indexes.owner_index.get(&addr) {
                let indices =
                    super::filter_pushdown::bitmap_to_row_indices(bitmap, &snapshot.indexes);
                let taken_cols: Vec<arrow::array::ArrayRef> = snapshot
                    .entities
                    .columns()
                    .iter()
                    .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
                    .collect::<Result<_, _>>()
                    .map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                RecordBatch::try_new(snapshot.entities.schema(), taken_cols).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?
            } else {
                RecordBatch::new_empty(Arc::clone(&self.schema))
            }
        } else {
            snapshot.entities.as_ref().clone()
        };

        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        mem.scan(state, projection, &[], limit).await
    }
}

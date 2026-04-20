use std::{
    any::Any,
    fmt,
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};

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
use roaring::RoaringBitmap;
use tokio::sync::watch;

use super::filter_pushdown::NumericOp;
use crate::entity_store::{IndexSnapshot, Snapshot};

pub struct NumericAnnotationsTable {
    schema: SchemaRef,
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
}

impl fmt::Debug for NumericAnnotationsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NumericAnnotationsTable")
            .finish_non_exhaustive()
    }
}

impl NumericAnnotationsTable {
    pub fn new(snapshot_rx: watch::Receiver<Arc<Snapshot>>) -> Self {
        Self {
            schema: glint_primitives::exex_schema::numeric_annotations_schema(),
            snapshot_rx,
        }
    }
}

fn resolve_numeric_bitmap(
    key: &str,
    op: NumericOp,
    value: u64,
    indexes: &IndexSnapshot,
) -> RoaringBitmap {
    match op {
        NumericOp::Eq => indexes
            .numeric_ann_index
            .get(&(key.to_owned(), value))
            .cloned()
            .unwrap_or_default(),
        NumericOp::Gt | NumericOp::GtEq | NumericOp::Lt | NumericOp::LtEq => {
            let Some(btree) = indexes.numeric_ann_range.get(key) else {
                return RoaringBitmap::new();
            };
            let range: Box<dyn Iterator<Item = (&u64, &RoaringBitmap)>> = match op {
                NumericOp::Gt => Box::new(btree.range((Excluded(value), Unbounded))),
                NumericOp::GtEq => Box::new(btree.range((Included(value), Unbounded))),
                NumericOp::Lt => Box::new(btree.range(..value)),
                NumericOp::LtEq => Box::new(btree.range(..=value)),
                NumericOp::Eq => unreachable!(),
            };
            let mut result = RoaringBitmap::new();
            for (_, bm) in range {
                result |= bm;
            }
            result
        }
    }
}

#[async_trait]
impl TableProvider for NumericAnnotationsTable {
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
        let has_pred = super::filter_pushdown::extract_numeric_ann_pred(filters).is_some();
        Ok(filters
            .iter()
            .map(|f| {
                if has_pred && super::filter_pushdown::is_ann_column(f) {
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

        let batch = if let Some((key, op, value)) =
            super::filter_pushdown::extract_numeric_ann_pred(&filter_refs)
        {
            let bitmap = resolve_numeric_bitmap(&key, op, value, &snapshot.indexes);
            if bitmap.is_empty() {
                RecordBatch::new_empty(Arc::clone(&self.schema))
            } else {
                super::filter_pushdown::filter_annotation_batch_by_bitmap(
                    &snapshot.entities,
                    &snapshot.numeric_annotations,
                    &bitmap,
                    &snapshot.indexes,
                )?
            }
        } else {
            snapshot.numeric_annotations.as_ref().clone()
        };

        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

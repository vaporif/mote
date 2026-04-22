use crate::error::IntoDataFusionError as _;
use core::fmt;
use std::{any::Any, sync::Arc};

use super::{
    annotation_extractors::{extract_string_eq, extract_u64_eq},
    references_column,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{
        Session, TableProvider,
        memory::{DataSourceExec, MemorySourceConfig},
    },
    datasource::TableType,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use glint_primitives::exex_schema::ann_columns;
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::sql_queries::HistoryDb;

pub struct HistoryNumericAnnotationsProvider {
    conn: Arc<Mutex<Connection>>,
    schema: SchemaRef,
}

impl fmt::Debug for HistoryNumericAnnotationsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryNumericAnnotationsProvider").finish()
    }
}

impl HistoryNumericAnnotationsProvider {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            schema: glint_primitives::exex_schema::history_numeric_annotations_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for HistoryNumericAnnotationsProvider {
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
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if references_column(f, "valid_from_block")
                    || references_column(f, ann_columns::ANN_KEY)
                    || references_column(f, ann_columns::ANN_VALUE)
                {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let conn = Arc::clone(&self.conn);
        let schema = Arc::clone(&self.schema);
        let out_schema = Arc::clone(&self.schema);
        let filters = filters.to_vec();
        let projection = projection.cloned();

        let range = super::entities_history::require_valid_from_range(&filters)?;
        let ann_key = extract_string_eq(&filters, ann_columns::ANN_KEY);
        let ann_value = extract_u64_eq(&filters, ann_columns::ANN_VALUE);

        let batch = tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            HistoryDb::new(&conn).query_history_numeric_annotations(
                range.0,
                range.1,
                ann_key.as_deref(),
                ann_value,
                &schema,
            )
        })
        .await
        .df_err()??;

        let source = MemorySourceConfig::try_new(&[vec![batch]], out_schema, projection)?;
        let exec = DataSourceExec::new(Arc::new(source));
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}

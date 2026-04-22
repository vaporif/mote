use crate::error::{IntoDataFusionError as _, arrow_err};
use core::fmt;
use core::fmt::Write as _;
use std::{any::Any, sync::Arc};

use super::{
    annotation_extractors::{extract_string_eq, extract_u64_eq},
    references_column,
};
use arrow::{
    array::{ArrayRef, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt64Builder},
    datatypes::SchemaRef,
};
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
            query_history_numeric_annotations(
                &conn,
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

fn query_history_numeric_annotations(
    conn: &Connection,
    from_block: u64,
    to_block: u64,
    ann_key: Option<&str>,
    ann_value: Option<u64>,
    schema: &SchemaRef,
) -> datafusion::error::Result<RecordBatch> {
    let mut sql = String::from(
        "SELECT entity_key, valid_from_block, ann_key, ann_value \
         FROM history_numeric_annotations \
         WHERE valid_from_block >= ?1 AND valid_from_block <= ?2",
    );

    let mut param_idx = 3;
    let key_idx = if ann_key.is_some() {
        let _ = write!(sql, " AND ann_key = ?{param_idx}");
        let idx = param_idx;
        param_idx += 1;
        Some(idx)
    } else {
        None
    };
    let value_idx = if ann_value.is_some() {
        let _ = write!(sql, " AND ann_value = ?{param_idx}");
        Some(param_idx)
    } else {
        None
    };

    sql.push_str(" ORDER BY entity_key, valid_from_block");

    let mut stmt = conn.prepare_cached(&sql).df_err()?;

    let from_i64 = i64::try_from(from_block).df_err()?;
    let to_i64 = i64::try_from(to_block).df_err()?;

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
        vec![Box::new(from_i64), Box::new(to_i64)];
    if let (Some(_), Some(k)) = (key_idx, &ann_key) {
        params.push(Box::new(k.to_string()));
    }
    if let (Some(_), Some(v)) = (value_idx, ann_value) {
        params.push(Box::new(i64::try_from(v).df_err()?));
    }

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(std::convert::AsRef::as_ref).collect();
    let mut rows = stmt.query(param_refs.as_slice()).df_err()?;

    let mut entity_key = FixedSizeBinaryBuilder::new(32);
    let mut valid_from = UInt64Builder::new();
    let mut key_builder = StringBuilder::new();
    let mut value_builder = UInt64Builder::new();

    while let Some(row) = rows.next().df_err()? {
        entity_key
            .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;
        valid_from.append_value(u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
        key_builder.append_value(row.get_ref(2).df_err()?.as_str().df_err()?);
        value_builder.append_value(u64::try_from(row.get::<_, i64>(3).df_err()?).df_err()?);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(entity_key.finish()),
        Arc::new(valid_from.finish()),
        Arc::new(key_builder.finish()),
        Arc::new(value_builder.finish()),
    ];

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
}

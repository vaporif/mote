use crate::error::IntoDataFusionError as _;
use core::fmt;
use core::fmt::Write as _;
use glint_primitives::exex_schema::ann_columns;
use std::{any::Any, sync::Arc};

use super::{
    annotation_extractors::{extract_string_eq, extract_u64_eq},
    references_block_number, references_column, require_block_range,
};
use crate::error::arrow_err;
use arrow::{
    array::{
        ArrayRef, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt32Builder, UInt64Builder,
    },
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
use parking_lot::Mutex;
use rusqlite::Connection;

pub struct EventNumericAnnotationsProvider {
    conn: Arc<Mutex<Connection>>,
    schema: SchemaRef,
}

impl fmt::Debug for EventNumericAnnotationsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventNumericAnnotationsProvider").finish()
    }
}

impl EventNumericAnnotationsProvider {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            schema: glint_primitives::exex_schema::event_numeric_annotations_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for EventNumericAnnotationsProvider {
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
                if references_block_number(f)
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

        let range = require_block_range(&filters)?;
        let ann_key = extract_string_eq(&filters, ann_columns::ANN_KEY);
        let ann_value = extract_u64_eq(&filters, ann_columns::ANN_VALUE);

        let batch = tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            query_numeric_annotations(
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

fn query_numeric_annotations(
    conn: &Connection,
    from_block: u64,
    to_block: u64,
    ann_key: Option<&str>,
    ann_value: Option<u64>,
    schema: &SchemaRef,
) -> datafusion::error::Result<RecordBatch> {
    let mut sql = String::from(
        "SELECT entity_key, block_number, log_index, ann_key, ann_value \
         FROM event_numeric_annotations \
         WHERE block_number >= ?1 AND block_number <= ?2",
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

    sql.push_str(" ORDER BY block_number, log_index");

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
    let mut block_number = UInt64Builder::new();
    let mut log_index = UInt32Builder::new();
    let mut key_builder = StringBuilder::new();
    let mut value_builder = UInt64Builder::new();

    while let Some(row) = rows.next().df_err()? {
        entity_key
            .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;
        block_number.append_value(u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
        log_index.append_value(u32::try_from(row.get::<_, i64>(2).df_err()?).df_err()?);
        key_builder.append_value(row.get_ref(3).df_err()?.as_str().df_err()?);
        value_builder.append_value(u64::try_from(row.get::<_, i64>(4).df_err()?).df_err()?);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(entity_key.finish()),
        Arc::new(block_number.finish()),
        Arc::new(log_index.finish()),
        Arc::new(key_builder.finish()),
        Arc::new(value_builder.finish()),
    ];

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
}

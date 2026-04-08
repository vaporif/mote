use crate::error::IntoDataFusionError as _;
use core::fmt;
use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt8Builder,
        UInt32Builder, UInt64Builder,
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

use crate::{
    error::arrow_err,
    provider::{references_block_number, require_block_range},
};

pub struct HistoricalEventsProvider {
    conn: Arc<Mutex<Connection>>,
    schema: SchemaRef,
}

impl fmt::Debug for HistoricalEventsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoricalEventsProvider").finish()
    }
}

impl HistoricalEventsProvider {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            schema: glint_primitives::exex_schema::entity_events_output_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for HistoricalEventsProvider {
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
                if references_block_number(f) {
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

        let batch = tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            query_events(&conn, range.0, range.1, &schema)
        })
        .await
        .df_err()??;

        let source = MemorySourceConfig::try_new(&[vec![batch]], out_schema, projection)?;
        let exec = DataSourceExec::new(Arc::new(source));
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}

fn query_events(
    conn: &Connection,
    from_block: u64,
    to_block: u64,
    schema: &SchemaRef,
) -> datafusion::error::Result<RecordBatch> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT block_number, log_index, event_type, entity_key, owner, expires_at_block,
                    content_type, payload, extend_policy, operator, gas_cost
             FROM entity_events
             WHERE block_number >= ?1 AND block_number <= ?2
             ORDER BY block_number, log_index",
        )
        .df_err()?;

    let from_i64 = i64::try_from(from_block).df_err()?;
    let to_i64 = i64::try_from(to_block).df_err()?;
    let mut rows = stmt.query([from_i64, to_i64]).df_err()?;

    let mut block_number = UInt64Builder::new();
    let mut log_index = UInt32Builder::new();
    let mut event_type = UInt8Builder::new();
    let mut entity_key = FixedSizeBinaryBuilder::new(32);
    let mut owner = FixedSizeBinaryBuilder::new(20);
    let mut expires_at = UInt64Builder::new();
    let mut content_type = StringBuilder::new();
    let mut payload = BinaryBuilder::new();
    let mut extend_policy = UInt8Builder::new();
    let mut operator = FixedSizeBinaryBuilder::new(20);
    let mut gas_cost = UInt64Builder::new();

    while let Some(row) = rows.next().df_err()? {
        block_number.append_value(u64::try_from(row.get::<_, i64>(0).df_err()?).df_err()?);
        log_index.append_value(u32::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
        event_type.append_value(u8::try_from(row.get::<_, i64>(2).df_err()?).df_err()?);
        entity_key
            .append_value(row.get_ref(3).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;

        match row.get_ref(4).df_err()?.as_blob_or_null() {
            Ok(Some(v)) => owner.append_value(v).map_err(arrow_err)?,
            _ => owner.append_null(),
        }

        match row.get::<_, Option<i64>>(5).df_err()? {
            Some(v) => expires_at.append_value(u64::try_from(v).df_err()?),
            None => expires_at.append_null(),
        }

        match row.get_ref(6).df_err()?.as_str_or_null() {
            Ok(Some(v)) => content_type.append_value(v),
            _ => content_type.append_null(),
        }

        match row.get_ref(7).df_err()?.as_blob_or_null() {
            Ok(Some(v)) => payload.append_value(v),
            _ => payload.append_null(),
        }

        match row.get::<_, Option<i64>>(8).df_err()? {
            Some(v) => extend_policy.append_value(u8::try_from(v).df_err()?),
            None => extend_policy.append_null(),
        }

        match row.get_ref(9).df_err()?.as_blob_or_null() {
            Ok(Some(v)) => operator.append_value(v).map_err(arrow_err)?,
            _ => operator.append_null(),
        }

        match row.get::<_, Option<i64>>(10).df_err()? {
            Some(v) => gas_cost.append_value(u64::try_from(v).df_err()?),
            None => gas_cost.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(block_number.finish()),
        Arc::new(log_index.finish()),
        Arc::new(event_type.finish()),
        Arc::new(entity_key.finish()),
        Arc::new(owner.finish()),
        Arc::new(expires_at.finish()),
        Arc::new(content_type.finish()),
        Arc::new(payload.finish()),
        Arc::new(extend_policy.finish()),
        Arc::new(operator.finish()),
        Arc::new(gas_cost.finish()),
    ];

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
}

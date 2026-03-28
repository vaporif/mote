use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt8Builder,
        UInt64Builder, builder::MapBuilder,
    },
    datatypes::SchemaRef,
};
use datafusion::{
    catalog::Session,
    catalog::memory::{DataSourceExec, MemorySourceConfig},
    common::Result as DfResult,
    datasource::{TableProvider, TableType},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use rusqlite::Connection;

use parking_lot::Mutex;

pub struct HistoricalTableProvider {
    conn: Arc<Mutex<Connection>>,
    schema: SchemaRef,
}

impl fmt::Debug for HistoricalTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoricalTableProvider").finish()
    }
}

impl HistoricalTableProvider {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            schema: glint_primitives::exex_schema::historical_output_schema(),
        }
    }
}

pub fn extract_block_range(filters: &[Expr]) -> Option<(u64, u64)> {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    let mut lower: Option<u64> = None;
    let mut upper: Option<u64> = None;

    for filter in filters {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::And {
                    if let Some((l, u)) = extract_block_range(&[*left.clone(), *right.clone()]) {
                        lower = Some(lower.map_or(l, |cur| cur.max(l)));
                        upper = Some(upper.map_or(u, |cur| cur.min(u)));
                    }
                } else if is_block_number_col(left) {
                    match op {
                        Operator::Eq => {
                            if let Some(v) = extract_u64_literal(right) {
                                lower = Some(lower.map_or(v, |cur| cur.max(v)));
                                upper = Some(upper.map_or(v, |cur| cur.min(v)));
                            }
                        }
                        Operator::GtEq => {
                            if let Some(v) = extract_u64_literal(right) {
                                lower = Some(lower.map_or(v, |cur| cur.max(v)));
                            }
                        }
                        Operator::Gt => {
                            if let Some(v) = extract_u64_literal(right) {
                                lower = Some(lower.map_or(v + 1, |cur| cur.max(v + 1)));
                            }
                        }
                        Operator::LtEq => {
                            if let Some(v) = extract_u64_literal(right) {
                                upper = Some(upper.map_or(v, |cur| cur.min(v)));
                            }
                        }
                        Operator::Lt => {
                            if let Some(v) = extract_u64_literal(right) {
                                upper = Some(upper.map_or_else(
                                    || v.saturating_sub(1),
                                    |cur| cur.min(v.saturating_sub(1)),
                                ));
                            }
                        }
                        _ => {}
                    }
                } else if is_block_number_col(right) {
                    match op {
                        Operator::LtEq => {
                            if let Some(v) = extract_u64_literal(left) {
                                lower = Some(lower.map_or(v, |cur| cur.max(v)));
                            }
                        }
                        Operator::Lt => {
                            if let Some(v) = extract_u64_literal(left) {
                                lower = Some(lower.map_or(v + 1, |cur| cur.max(v + 1)));
                            }
                        }
                        Operator::GtEq => {
                            if let Some(v) = extract_u64_literal(left) {
                                upper = Some(upper.map_or(v, |cur| cur.min(v)));
                            }
                        }
                        Operator::Gt => {
                            if let Some(v) = extract_u64_literal(left) {
                                upper = Some(upper.map_or_else(
                                    || v.saturating_sub(1),
                                    |cur| cur.min(v.saturating_sub(1)),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
            Expr::Between(between) if is_block_number_col(&between.expr) => {
                if let (Some(lo), Some(hi)) = (
                    extract_u64_literal(&between.low),
                    extract_u64_literal(&between.high),
                ) {
                    lower = Some(lower.map_or(lo, |cur| cur.max(lo)));
                    upper = Some(upper.map_or(hi, |cur| cur.min(hi)));
                }
            }
            _ => {}
        }
    }

    match (lower, upper) {
        (Some(l), Some(u)) => Some((l, u)),
        _ => None,
    }
}

fn is_block_number_col(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name() == columns::BLOCK_NUMBER)
}

const fn extract_u64_literal(expr: &Expr) -> Option<u64> {
    match expr {
        #[allow(clippy::cast_sign_loss)]
        Expr::Literal(datafusion::common::ScalarValue::Int64(Some(v)), _) if *v >= 0 => {
            Some(*v as u64)
        }
        Expr::Literal(datafusion::common::ScalarValue::UInt64(Some(v)), _) => Some(*v),
        _ => None,
    }
}

fn references_block_number(expr: &Expr) -> bool {
    match expr {
        Expr::Column(c) => c.name() == columns::BLOCK_NUMBER,
        Expr::BinaryExpr(be) => {
            references_block_number(&be.left) || references_block_number(&be.right)
        }
        Expr::Between(b) => references_block_number(&b.expr),
        _ => false,
    }
}

#[async_trait]
impl TableProvider for HistoricalTableProvider {
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
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let conn = Arc::clone(&self.conn);
        let schema = Arc::clone(&self.schema);
        let out_schema = Arc::clone(&self.schema);
        let filters = filters.to_vec();
        let projection = projection.cloned();

        let range = extract_block_range(&filters).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "historical queries require both lower and upper block_number bounds \
                 (e.g., WHERE block_number BETWEEN 100 AND 500)"
                    .to_owned(),
            )
        })?;

        let batch = tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            query_block_range(&conn, range.0, range.1, &schema)
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))??;

        let source = MemorySourceConfig::try_new(&[vec![batch]], out_schema, projection)?;
        let exec = DataSourceExec::new(Arc::new(source));
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}

use glint_primitives::exex_schema::{columns, map_field_names};

#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]
fn query_block_range(
    conn: &Connection,
    from_block: u64,
    to_block: u64,
    schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT block_number, event_type, entity_key, owner, expires_at_block,
                    content_type, payload, string_annotations, numeric_annotations,
                    extend_policy, operator
             FROM entity_events
             WHERE block_number >= ?1 AND block_number <= ?2
             ORDER BY block_number, log_index",
        )
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let rows = stmt
        .query_map([from_block as i64, to_block as i64], |row| {
            Ok(EventSqlRow {
                block_number: row.get::<_, i64>(0)? as u64,
                event_type: row.get::<_, i64>(1)? as u8,
                entity_key: row.get::<_, Vec<u8>>(2)?,
                owner: row.get::<_, Option<Vec<u8>>>(3)?,
                expires_at_block: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                content_type: row.get::<_, Option<String>>(5)?,
                payload: row.get::<_, Option<Vec<u8>>>(6)?,
                string_annotations: row.get::<_, Option<String>>(7)?,
                numeric_annotations: row.get::<_, Option<String>>(8)?,
                extend_policy: row.get::<_, Option<i64>>(9)?.map(|v| v as u8),
                operator: row.get::<_, Option<Vec<u8>>>(10)?,
            })
        })
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let collected: Vec<_> = rows
        .map(|r| r.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e))))
        .collect::<DfResult<_>>()?;

    build_result_batch(&collected, schema)
}

struct EventSqlRow {
    block_number: u64,
    event_type: u8,
    entity_key: Vec<u8>,
    owner: Option<Vec<u8>>,
    expires_at_block: Option<u64>,
    content_type: Option<String>,
    payload: Option<Vec<u8>>,
    string_annotations: Option<String>,
    numeric_annotations: Option<String>,
    extend_policy: Option<u8>,
    operator: Option<Vec<u8>>,
}

fn arrow_err(e: arrow::error::ArrowError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
}

fn build_result_batch(rows: &[EventSqlRow], schema: &SchemaRef) -> DfResult<RecordBatch> {
    let n = rows.len();

    let mut block_number_b = UInt64Builder::with_capacity(n);
    let mut event_type_b = UInt8Builder::with_capacity(n);
    let mut entity_key_b = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut owner_b = FixedSizeBinaryBuilder::with_capacity(n, 20);
    let mut expires_b = UInt64Builder::with_capacity(n);
    let mut content_type_b = StringBuilder::with_capacity(n, n * 16);
    let mut payload_b = BinaryBuilder::with_capacity(n, n * 64);
    let mut str_ann_b = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut num_ann_b = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        UInt64Builder::new(),
    );
    let mut extend_policy_b = UInt8Builder::with_capacity(n);
    let mut operator_b = FixedSizeBinaryBuilder::with_capacity(n, 20);

    for row in rows {
        block_number_b.append_value(row.block_number);
        event_type_b.append_value(row.event_type);
        entity_key_b
            .append_value(&row.entity_key)
            .map_err(arrow_err)?;

        match &row.owner {
            Some(v) => owner_b.append_value(v).map_err(arrow_err)?,
            None => owner_b.append_null(),
        }

        match row.expires_at_block {
            Some(v) => expires_b.append_value(v),
            None => expires_b.append_null(),
        }

        match &row.content_type {
            Some(v) => content_type_b.append_value(v),
            None => content_type_b.append_null(),
        }

        match &row.payload {
            Some(v) => payload_b.append_value(v),
            None => payload_b.append_null(),
        }

        match &row.string_annotations {
            Some(json_str) => {
                let pairs: Vec<Vec<String>> = serde_json::from_str(json_str)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                for pair in &pairs {
                    if pair.len() != 2 {
                        return Err(datafusion::error::DataFusionError::Internal(format!(
                            "malformed string annotation pair in DB: expected [key, value], got {pair:?}"
                        )));
                    }
                    str_ann_b.keys().append_value(&pair[0]);
                    str_ann_b.values().append_value(&pair[1]);
                }
                str_ann_b.append(true).map_err(arrow_err)?;
            }
            None => {
                str_ann_b.append(false).map_err(arrow_err)?;
            }
        }

        match &row.numeric_annotations {
            Some(json_str) => {
                let pairs: Vec<serde_json::Value> = serde_json::from_str(json_str)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                for pair in &pairs {
                    let (Some(k), Some(v)) = (
                        pair.get(0).and_then(|v| v.as_str()),
                        pair.get(1).and_then(serde_json::Value::as_u64),
                    ) else {
                        return Err(datafusion::error::DataFusionError::Internal(format!(
                            "malformed numeric annotation pair in DB: expected [key, value], got {pair}"
                        )));
                    };
                    num_ann_b.keys().append_value(k);
                    num_ann_b.values().append_value(v);
                }
                num_ann_b.append(true).map_err(arrow_err)?;
            }
            None => {
                num_ann_b.append(false).map_err(arrow_err)?;
            }
        }

        match row.extend_policy {
            Some(v) => extend_policy_b.append_value(v),
            None => extend_policy_b.append_null(),
        }

        match &row.operator {
            Some(v) => operator_b.append_value(v).map_err(arrow_err)?,
            None => operator_b.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(block_number_b.finish()),
        Arc::new(event_type_b.finish()),
        Arc::new(entity_key_b.finish()),
        Arc::new(owner_b.finish()),
        Arc::new(expires_b.finish()),
        Arc::new(content_type_b.finish()),
        Arc::new(payload_b.finish()),
        Arc::new(str_ann_b.finish()),
        Arc::new(num_ann_b.finish()),
        Arc::new(extend_policy_b.finish()),
        Arc::new(operator_b.finish()),
    ];

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_block_range_between() {
        use datafusion::prelude::*;
        let expr = col("block_number")
            .gt_eq(lit(100u64))
            .and(col("block_number").lt_eq(lit(500u64)));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, Some((100, 500)));
    }

    #[test]
    fn extract_block_range_equality() {
        use datafusion::prelude::*;
        let expr = col("block_number").eq(lit(42u64));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, Some((42, 42)));
    }

    #[test]
    fn extract_block_range_missing_upper() {
        use datafusion::prelude::*;
        let expr = col("block_number").gt_eq(lit(100u64));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, None);
    }

    fn has_block_range_filter(filters: &[Expr]) -> bool {
        filters.iter().any(super::references_block_number)
    }

    #[test]
    fn has_block_range_filter_detects_column() {
        use datafusion::prelude::*;
        let expr = col("block_number").gt_eq(lit(100u64));
        assert!(has_block_range_filter(&[expr]));
    }

    #[test]
    fn has_block_range_filter_false_for_other_columns() {
        use datafusion::prelude::*;
        let expr = col("owner").eq(lit("foo"));
        assert!(!has_block_range_filter(&[expr]));
    }
}

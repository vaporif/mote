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

use glint_primitives::exex_schema::{columns, map_field_names};

trait IntoDfError<T> {
    fn df_err(self) -> DfResult<T>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> IntoDfError<T> for Result<T, E> {
    fn df_err(self) -> DfResult<T> {
        self.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    }
}

fn arrow_err(e: arrow::error::ArrowError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
}

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
                } else if let Some((col_side_op, literal)) = normalize_comparison(left, *op, right)
                {
                    apply_bound(col_side_op, literal, &mut lower, &mut upper);
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

fn normalize_comparison(
    left: &Expr,
    op: datafusion::logical_expr::Operator,
    right: &Expr,
) -> Option<(datafusion::logical_expr::Operator, u64)> {
    use datafusion::logical_expr::Operator;

    if is_block_number_col(left) {
        extract_u64_literal(right).map(|v| (op, v))
    } else if is_block_number_col(right) {
        let flipped = match op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => other,
        };
        extract_u64_literal(left).map(|v| (flipped, v))
    } else {
        None
    }
}

fn apply_bound(
    op: datafusion::logical_expr::Operator,
    v: u64,
    lower: &mut Option<u64>,
    upper: &mut Option<u64>,
) {
    use datafusion::logical_expr::Operator;

    match op {
        Operator::Eq => {
            *lower = Some(lower.map_or(v, |cur| cur.max(v)));
            *upper = Some(upper.map_or(v, |cur| cur.min(v)));
        }
        Operator::GtEq => {
            *lower = Some(lower.map_or(v, |cur| cur.max(v)));
        }
        Operator::Gt => {
            *lower = Some(lower.map_or(v + 1, |cur| cur.max(v + 1)));
        }
        Operator::LtEq => {
            *upper = Some(upper.map_or(v, |cur| cur.min(v)));
        }
        Operator::Lt => {
            let bound = v.saturating_sub(1);
            *upper = Some(upper.map_or(bound, |cur| cur.min(bound)));
        }
        _ => {}
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
        .df_err()??;

        let source = MemorySourceConfig::try_new(&[vec![batch]], out_schema, projection)?;
        let exec = DataSourceExec::new(Arc::new(source));
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}

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
        .df_err()?;

    let mut builders = BatchBuilders::new();

    let mut rows = stmt.query([from_block as i64, to_block as i64]).df_err()?;

    while let Some(row) = rows.next().df_err()? {
        builders.append_row(row)?;
    }

    builders.finish(schema)
}

struct BatchBuilders {
    block_number: UInt64Builder,
    event_type: UInt8Builder,
    entity_key: FixedSizeBinaryBuilder,
    owner: FixedSizeBinaryBuilder,
    expires_at: UInt64Builder,
    content_type: StringBuilder,
    payload: BinaryBuilder,
    str_ann: MapBuilder<StringBuilder, StringBuilder>,
    num_ann: MapBuilder<StringBuilder, UInt64Builder>,
    extend_policy: UInt8Builder,
    operator: FixedSizeBinaryBuilder,
}

impl BatchBuilders {
    fn new() -> Self {
        Self {
            block_number: UInt64Builder::new(),
            event_type: UInt8Builder::new(),
            entity_key: FixedSizeBinaryBuilder::new(32),
            owner: FixedSizeBinaryBuilder::new(20),
            expires_at: UInt64Builder::new(),
            content_type: StringBuilder::new(),
            payload: BinaryBuilder::new(),
            str_ann: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            num_ann: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                UInt64Builder::new(),
            ),
            extend_policy: UInt8Builder::new(),
            operator: FixedSizeBinaryBuilder::new(20),
        }
    }

    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::needless_pass_by_value
    )]
    fn append_row(&mut self, row: &rusqlite::Row<'_>) -> DfResult<()> {
        self.block_number
            .append_value(row.get::<_, i64>(0).df_err()? as u64);
        self.event_type
            .append_value(row.get::<_, i64>(1).df_err()? as u8);
        self.entity_key
            .append_value(row.get_ref(2).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;

        match row.get_ref(3).df_err()?.as_blob_or_null() {
            Ok(Some(v)) => self.owner.append_value(v).map_err(arrow_err)?,
            _ => self.owner.append_null(),
        }

        match row.get::<_, Option<i64>>(4).df_err()? {
            Some(v) => self.expires_at.append_value(v as u64),
            None => self.expires_at.append_null(),
        }

        match row.get_ref(5).df_err()?.as_str_or_null() {
            Ok(Some(v)) => self.content_type.append_value(v),
            _ => self.content_type.append_null(),
        }

        match row.get_ref(6).df_err()?.as_blob_or_null() {
            Ok(Some(v)) => self.payload.append_value(v),
            _ => self.payload.append_null(),
        }

        self.append_string_annotations(row.get_ref(7).df_err()?)?;
        self.append_numeric_annotations(row.get_ref(8).df_err()?)?;

        match row.get::<_, Option<i64>>(9).df_err()? {
            Some(v) => self.extend_policy.append_value(v as u8),
            None => self.extend_policy.append_null(),
        }

        match row.get_ref(10).df_err()?.as_blob_or_null() {
            Ok(Some(v)) => self.operator.append_value(v).map_err(arrow_err)?,
            _ => self.operator.append_null(),
        }

        Ok(())
    }

    fn append_string_annotations(&mut self, value: rusqlite::types::ValueRef<'_>) -> DfResult<()> {
        match value.as_str_or_null() {
            Ok(Some(json_str)) => {
                let pairs: Vec<Vec<String>> = serde_json::from_str(json_str).df_err()?;
                for pair in &pairs {
                    if pair.len() != 2 {
                        return Err(datafusion::error::DataFusionError::Internal(format!(
                            "malformed string annotation pair in DB: expected [key, value], got {pair:?}"
                        )));
                    }
                    self.str_ann.keys().append_value(&pair[0]);
                    self.str_ann.values().append_value(&pair[1]);
                }
                self.str_ann.append(true).map_err(arrow_err)?;
            }
            _ => {
                self.str_ann.append(false).map_err(arrow_err)?;
            }
        }
        Ok(())
    }

    fn append_numeric_annotations(&mut self, value: rusqlite::types::ValueRef<'_>) -> DfResult<()> {
        match value.as_str_or_null() {
            Ok(Some(json_str)) => {
                let pairs: Vec<serde_json::Value> = serde_json::from_str(json_str).df_err()?;
                for pair in &pairs {
                    let (Some(k), Some(v)) = (
                        pair.get(0).and_then(|v| v.as_str()),
                        pair.get(1).and_then(serde_json::Value::as_u64),
                    ) else {
                        return Err(datafusion::error::DataFusionError::Internal(format!(
                            "malformed numeric annotation pair in DB: expected [key, value], got {pair}"
                        )));
                    };
                    self.num_ann.keys().append_value(k);
                    self.num_ann.values().append_value(v);
                }
                self.num_ann.append(true).map_err(arrow_err)?;
            }
            _ => {
                self.num_ann.append(false).map_err(arrow_err)?;
            }
        }
        Ok(())
    }

    fn finish(mut self, schema: &SchemaRef) -> DfResult<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_number.finish()),
            Arc::new(self.event_type.finish()),
            Arc::new(self.entity_key.finish()),
            Arc::new(self.owner.finish()),
            Arc::new(self.expires_at.finish()),
            Arc::new(self.content_type.finish()),
            Arc::new(self.payload.finish()),
            Arc::new(self.str_ann.finish()),
            Arc::new(self.num_ann.finish()),
            Arc::new(self.extend_policy.finish()),
            Arc::new(self.operator.finish()),
        ];

        RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
    }
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

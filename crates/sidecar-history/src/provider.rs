use std::{any::Any, fmt, fmt::Write as _, sync::Arc};

use async_trait::async_trait;

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt8Builder,
        UInt32Builder, UInt64Builder,
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

use glint_primitives::exex_schema::{ann_columns, columns};

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

// ---------------------------------------------------------------------------
// Block-range filter extraction (shared by all three providers)
// ---------------------------------------------------------------------------

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
        #[allow(clippy::cast_sign_loss)] // guarded by *v >= 0
        Expr::Literal(datafusion::common::ScalarValue::Int64(Some(v)), _) if *v >= 0 => {
            Some(*v as u64)
        }
        Expr::Literal(datafusion::common::ScalarValue::UInt64(Some(v)), _) => Some(*v),
        _ => None,
    }
}

fn references_column(expr: &Expr, col: &str) -> bool {
    match expr {
        Expr::Column(c) => c.name() == col,
        Expr::BinaryExpr(be) => {
            references_column(&be.left, col) || references_column(&be.right, col)
        }
        Expr::Between(b) => references_column(&b.expr, col),
        _ => false,
    }
}

fn references_block_number(expr: &Expr) -> bool {
    references_column(expr, columns::BLOCK_NUMBER)
}

/// Require `block_number` bounds, return error if missing or inverted.
fn require_block_range(filters: &[Expr]) -> DfResult<(u64, u64)> {
    let range = extract_block_range(filters).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(
            "historical queries require both lower and upper block_number bounds \
             (e.g., WHERE block_number BETWEEN 100 AND 500)"
                .to_owned(),
        )
    })?;

    if range.0 > range.1 {
        return Err(datafusion::error::DataFusionError::Plan(format!(
            "invalid block range: lower ({}) > upper ({})",
            range.0, range.1
        )));
    }

    Ok(range)
}

// ---------------------------------------------------------------------------
// Annotation filter extraction helpers
// ---------------------------------------------------------------------------

/// Extract a string literal equality filter for a given column name.
fn extract_string_eq(filters: &[Expr], col_name: &str) -> Option<String> {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    for filter in filters {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let (
                    Expr::Column(c),
                    Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(v)), _),
                ) = (left.as_ref(), right.as_ref())
                    && c.name() == col_name
                {
                    return Some(v.clone());
                }
                if let (
                    Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(v)), _),
                    Expr::Column(c),
                ) = (left.as_ref(), right.as_ref())
                    && c.name() == col_name
                {
                    return Some(v.clone());
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                if let Some(v) = extract_string_eq(&[*left.clone()], col_name) {
                    return Some(v);
                }
                if let Some(v) = extract_string_eq(&[*right.clone()], col_name) {
                    return Some(v);
                }
            }
            _ => {}
        }
    }
    None
}

/// Extract a u64 literal equality filter for a given column name.
fn extract_u64_eq(filters: &[Expr], col_name: &str) -> Option<u64> {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    for filter in filters {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let Expr::Column(c) = left.as_ref()
                    && c.name() == col_name
                    && let Some(v) = extract_u64_literal(right)
                {
                    return Some(v);
                }
                if let Expr::Column(c) = right.as_ref()
                    && c.name() == col_name
                    && let Some(v) = extract_u64_literal(left)
                {
                    return Some(v);
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                if let Some(v) = extract_u64_eq(&[*left.clone()], col_name) {
                    return Some(v);
                }
                if let Some(v) = extract_u64_eq(&[*right.clone()], col_name) {
                    return Some(v);
                }
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// HistoricalEventsProvider
// ---------------------------------------------------------------------------

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
) -> DfResult<RecordBatch> {
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

// ---------------------------------------------------------------------------
// EventStringAnnotationsProvider
// ---------------------------------------------------------------------------

pub struct EventStringAnnotationsProvider {
    conn: Arc<Mutex<Connection>>,
    schema: SchemaRef,
}

impl fmt::Debug for EventStringAnnotationsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStringAnnotationsProvider").finish()
    }
}

impl EventStringAnnotationsProvider {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            schema: glint_primitives::exex_schema::event_string_annotations_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for EventStringAnnotationsProvider {
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
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let conn = Arc::clone(&self.conn);
        let schema = Arc::clone(&self.schema);
        let out_schema = Arc::clone(&self.schema);
        let filters = filters.to_vec();
        let projection = projection.cloned();

        let range = require_block_range(&filters)?;
        let ann_key = extract_string_eq(&filters, ann_columns::ANN_KEY);
        let ann_value = extract_string_eq(&filters, ann_columns::ANN_VALUE);

        let batch = tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            query_string_annotations(
                &conn,
                range.0,
                range.1,
                ann_key.as_deref(),
                ann_value.as_deref(),
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

fn query_string_annotations(
    conn: &Connection,
    from_block: u64,
    to_block: u64,
    ann_key: Option<&str>,
    ann_value: Option<&str>,
    schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    let mut sql = String::from(
        "SELECT entity_key, block_number, log_index, ann_key, ann_value \
         FROM event_string_annotations \
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

    // Build dynamic params
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
        vec![Box::new(from_i64), Box::new(to_i64)];
    if let (Some(_), Some(k)) = (key_idx, &ann_key) {
        params.push(Box::new(k.to_string()));
    }
    if let (Some(_), Some(v)) = (value_idx, &ann_value) {
        params.push(Box::new(v.to_string()));
    }

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(std::convert::AsRef::as_ref).collect();
    let mut rows = stmt.query(param_refs.as_slice()).df_err()?;

    let mut entity_key = FixedSizeBinaryBuilder::new(32);
    let mut block_number = UInt64Builder::new();
    let mut log_index = UInt32Builder::new();
    let mut key_builder = StringBuilder::new();
    let mut value_builder = StringBuilder::new();

    while let Some(row) = rows.next().df_err()? {
        entity_key
            .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;
        block_number.append_value(u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?);
        log_index.append_value(u32::try_from(row.get::<_, i64>(2).df_err()?).df_err()?);
        key_builder.append_value(row.get_ref(3).df_err()?.as_str().df_err()?);
        value_builder.append_value(row.get_ref(4).df_err()?.as_str().df_err()?);
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

// ---------------------------------------------------------------------------
// EventNumericAnnotationsProvider
// ---------------------------------------------------------------------------

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
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
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
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
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
) -> DfResult<RecordBatch> {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;
    use datafusion::prelude::SessionContext;

    fn setup() -> Arc<Mutex<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        schema::create_tables(&conn).unwrap();
        Arc::new(Mutex::new(conn))
    }

    fn insert_event_with_annotations(conn: &Connection) {
        conn.execute(
            "INSERT INTO entity_events (block_number, block_hash, tx_index, tx_hash, log_index, event_type, entity_key, owner, expires_at_block, content_type, payload, extend_policy, gas_cost)
             VALUES (10, X'AA', 0, X'BB', 0, 0, X'0101010101010101010101010101010101010101010101010101010101010101', X'0202020202020202020202020202020202020202', 100, 'text/plain', X'00', 0, 21000)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO event_string_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', 10, 0, 'token', 'USDC')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO event_numeric_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', 10, 0, 'price', 3000)",
            [],
        ).unwrap();
    }

    fn register_all(ctx: &SessionContext, conn: Arc<Mutex<Connection>>) {
        ctx.register_table(
            "entity_events",
            Arc::new(HistoricalEventsProvider::new(Arc::clone(&conn))),
        )
        .unwrap();
        ctx.register_table(
            "event_string_annotations",
            Arc::new(EventStringAnnotationsProvider::new(Arc::clone(&conn))),
        )
        .unwrap();
        ctx.register_table(
            "event_numeric_annotations",
            Arc::new(EventNumericAnnotationsProvider::new(conn)),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn events_table_no_annotation_columns() {
        let conn = setup();
        {
            let guard = conn.lock();
            insert_event_with_annotations(&guard);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn);
        let df = ctx
            .sql("SELECT * FROM entity_events WHERE block_number >= 0 AND block_number <= 100")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
        assert!(
            results[0]
                .schema()
                .field_with_name("string_annotations")
                .is_err()
        );
    }

    #[tokio::test]
    async fn join_event_annotations() {
        let conn = setup();
        {
            let guard = conn.lock();
            insert_event_with_annotations(&guard);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn);
        let df = ctx
            .sql(
                "SELECT e.entity_key FROM entity_events e \
                 JOIN event_string_annotations s ON e.entity_key = s.entity_key AND e.block_number = s.block_number \
                 WHERE e.block_number >= 0 AND e.block_number <= 100 AND s.block_number >= 0 AND s.block_number <= 100 \
                 AND s.ann_key = 'token' AND s.ann_value = 'USDC'",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

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
        filters.iter().any(|f| references_block_number(f))
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

    #[test]
    fn extract_block_range_inverted_returns_valid_tuple() {
        use datafusion::prelude::*;
        // lower > upper produces a valid tuple (validation happens at scan time)
        let expr = col("block_number")
            .gt_eq(lit(500u64))
            .and(col("block_number").lt_eq(lit(100u64)));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, Some((500, 100)));
    }
}

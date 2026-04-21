use crate::error::{IntoDataFusionError as _, arrow_err};
use core::fmt;
use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt8Builder,
        UInt64Builder,
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

use crate::provider::block_range_filter::extract_block_range;

use super::{references_block_number, references_column};

pub struct EntitiesHistoryProvider {
    conn: Arc<Mutex<Connection>>,
    schema: SchemaRef,
}

impl fmt::Debug for EntitiesHistoryProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntitiesHistoryProvider").finish()
    }
}

impl EntitiesHistoryProvider {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            schema: glint_primitives::exex_schema::entities_history_schema(),
        }
    }
}

/// Extract a range from filters targeting `valid_from_block` using the same
/// approach as `extract_block_range` but keyed to a different column name.
pub fn extract_valid_from_range(filters: &[Expr]) -> Option<(u64, u64)> {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    let mut lower: Option<u64> = None;
    let mut upper: Option<u64> = None;

    for filter in filters {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
                if let Some((l, u)) = extract_valid_from_range(&[*left.clone(), *right.clone()]) {
                    lower = Some(lower.map_or(l, |cur| cur.max(l)));
                    upper = Some(upper.map_or(u, |cur| cur.min(u)));
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if let Some((normalized_op, lit)) = normalize_valid_from(left, *op, right) {
                    apply_bound(normalized_op, lit, &mut lower, &mut upper);
                }
            }
            Expr::Between(between) if is_valid_from_col(&between.expr) => {
                if let (Some(lo), Some(hi)) = (
                    super::extract_u64_literal(&between.low),
                    super::extract_u64_literal(&between.high),
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

fn is_valid_from_col(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name() == "valid_from_block")
}

fn normalize_valid_from(
    left: &Expr,
    op: datafusion::logical_expr::Operator,
    right: &Expr,
) -> Option<(datafusion::logical_expr::Operator, u64)> {
    use datafusion::logical_expr::Operator;

    if is_valid_from_col(left) {
        super::extract_u64_literal(right).map(|v| (op, v))
    } else if is_valid_from_col(right) {
        let flipped = match op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => other,
        };
        super::extract_u64_literal(left).map(|v| (flipped, v))
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

/// Extract `valid_from_block` bounds, error if missing.
pub fn require_valid_from_range(filters: &[Expr]) -> datafusion::error::Result<(u64, u64)> {
    let range = extract_valid_from_range(filters).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(
            "history annotation queries require both lower and upper valid_from_block bounds \
             (e.g., WHERE valid_from_block >= 100 AND valid_from_block <= 500)"
                .to_owned(),
        )
    })?;

    if range.0 > range.1 {
        return Err(datafusion::error::DataFusionError::Plan(format!(
            "invalid valid_from_block range: lower ({}) > upper ({})",
            range.0, range.1
        )));
    }

    Ok(range)
}

/// SCD2 query bounds derived from filters.
struct Scd2Bounds {
    /// The query only returns rows where `valid_from_block <= upper_bound`
    valid_from_upper: u64,
    /// The query only returns rows where `valid_to_block IS NULL OR valid_to_block > lower_bound`
    valid_to_lower: u64,
}

fn derive_scd2_bounds(filters: &[Expr]) -> datafusion::error::Result<Scd2Bounds> {
    // Prefer block_number (virtual SCD2 column) filters first
    if let Some((lo, hi)) = extract_block_range(filters) {
        return Ok(Scd2Bounds {
            valid_from_upper: hi,
            valid_to_lower: lo,
        });
    }

    // Fallback: direct valid_from_block range
    if let Some((lo, hi)) = extract_valid_from_range(filters) {
        return Ok(Scd2Bounds {
            valid_from_upper: hi,
            valid_to_lower: lo,
        });
    }

    Err(datafusion::error::DataFusionError::Plan(
        "entities_history queries require block_number or valid_from_block bounds \
         (e.g., WHERE block_number BETWEEN 100 AND 500)"
            .to_owned(),
    ))
}

#[async_trait]
impl TableProvider for EntitiesHistoryProvider {
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
                if references_block_number(f) || references_column(f, "valid_from_block") {
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

        let bounds = derive_scd2_bounds(&filters)?;

        let batch = tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            query_entities_history(&conn, &bounds, &schema)
        })
        .await
        .df_err()??;

        let source = MemorySourceConfig::try_new(&[vec![batch]], out_schema, projection)?;
        let exec = DataSourceExec::new(Arc::new(source));
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}

fn query_entities_history(
    conn: &Connection,
    bounds: &Scd2Bounds,
    schema: &SchemaRef,
) -> datafusion::error::Result<RecordBatch> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT entity_key, valid_from_block, valid_to_block, owner, expires_at_block,
                    content_type, payload, created_at_block, tx_hash, extend_policy, operator
             FROM entities_history
             WHERE valid_from_block <= ?1
               AND (valid_to_block IS NULL OR valid_to_block > ?2)
             ORDER BY entity_key, valid_from_block",
        )
        .df_err()?;

    let upper_i64 = i64::try_from(bounds.valid_from_upper).df_err()?;
    let lower_i64 = i64::try_from(bounds.valid_to_lower).df_err()?;
    let mut rows = stmt.query([upper_i64, lower_i64]).df_err()?;

    let mut entity_key = FixedSizeBinaryBuilder::new(32);
    let mut valid_from = UInt64Builder::new();
    let mut valid_to = UInt64Builder::new();
    let mut owner = FixedSizeBinaryBuilder::new(20);
    let mut expires_at = UInt64Builder::new();
    let mut content_type = StringBuilder::new();
    let mut payload = BinaryBuilder::new();
    let mut created_at = UInt64Builder::new();
    let mut tx_hash = FixedSizeBinaryBuilder::new(32);
    let mut extend_policy = UInt8Builder::new();
    let mut operator = FixedSizeBinaryBuilder::new(20);
    let mut block_number = UInt64Builder::new();

    while let Some(row) = rows.next().df_err()? {
        entity_key
            .append_value(row.get_ref(0).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;

        let vf = u64::try_from(row.get::<_, i64>(1).df_err()?).df_err()?;
        valid_from.append_value(vf);

        match row.get::<_, Option<i64>>(2).df_err()? {
            Some(v) => valid_to.append_value(u64::try_from(v).df_err()?),
            None => valid_to.append_null(),
        }

        owner
            .append_value(row.get_ref(3).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;
        expires_at.append_value(u64::try_from(row.get::<_, i64>(4).df_err()?).df_err()?);
        content_type.append_value(row.get_ref(5).df_err()?.as_str().df_err()?);
        payload.append_value(row.get_ref(6).df_err()?.as_blob().df_err()?);
        created_at.append_value(u64::try_from(row.get::<_, i64>(7).df_err()?).df_err()?);
        tx_hash
            .append_value(row.get_ref(8).df_err()?.as_blob().df_err()?)
            .map_err(arrow_err)?;
        extend_policy.append_value(u8::try_from(row.get::<_, i64>(9).df_err()?).df_err()?);

        match row.get_ref(10).df_err()?.as_blob_or_null() {
            Ok(Some(v)) => operator.append_value(v).map_err(arrow_err)?,
            _ => operator.append_null(),
        }

        // Virtual block_number column populated from valid_from_block
        block_number.append_value(vf);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(entity_key.finish()),
        Arc::new(valid_from.finish()),
        Arc::new(valid_to.finish()),
        Arc::new(owner.finish()),
        Arc::new(expires_at.finish()),
        Arc::new(content_type.finish()),
        Arc::new(payload.finish()),
        Arc::new(created_at.finish()),
        Arc::new(tx_hash.finish()),
        Arc::new(extend_policy.finish()),
        Arc::new(operator.finish()),
        Arc::new(block_number.finish()),
    ];

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
}

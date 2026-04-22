use crate::error::IntoDataFusionError as _;
use core::fmt;
use std::{any::Any, sync::Arc};

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
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::{provider::block_range_filter::extract_block_range, sql_queries::HistoryDb};

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

struct Scd2Bounds {
    valid_from_upper: u64,
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
            HistoryDb::new(&conn).query_entities_history(
                bounds.valid_from_upper,
                bounds.valid_to_lower,
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn extract_valid_from_range_between() {
        let filter = col("valid_from_block")
            .gt_eq(lit(100u64))
            .and(col("valid_from_block").lt_eq(lit(500u64)));
        let result = extract_valid_from_range(&[filter]);
        assert_eq!(result, Some((100, 500)));
    }

    #[test]
    fn extract_valid_from_range_equality() {
        let filter = col("valid_from_block").eq(lit(42u64));
        let result = extract_valid_from_range(&[filter]);
        assert_eq!(result, Some((42, 42)));
    }

    #[test]
    fn extract_valid_from_range_missing_bound() {
        let filter = col("valid_from_block").gt_eq(lit(100u64));
        let result = extract_valid_from_range(&[filter]);
        assert_eq!(result, None);
    }

    #[test]
    fn extract_valid_from_range_gt_lt() {
        let filter = col("valid_from_block")
            .gt(lit(99u64))
            .and(col("valid_from_block").lt(lit(501u64)));
        let result = extract_valid_from_range(&[filter]);
        assert_eq!(result, Some((100, 500)));
    }

    #[test]
    fn require_valid_from_range_errors_on_missing() {
        let result = require_valid_from_range(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn require_valid_from_range_errors_on_inverted() {
        let filter = col("valid_from_block")
            .gt_eq(lit(500u64))
            .and(col("valid_from_block").lt_eq(lit(100u64)));
        let result = require_valid_from_range(&[filter]);
        assert!(result.is_err());
    }
}

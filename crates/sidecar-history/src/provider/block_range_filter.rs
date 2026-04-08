use datafusion::prelude::Expr;
use glint_primitives::columns;

use super::extract_u64_literal;

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

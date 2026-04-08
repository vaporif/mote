use crate::provider::extract_u64_literal;
use datafusion::prelude::Expr;

pub fn extract_string_eq(filters: &[Expr], col_name: &str) -> Option<String> {
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
pub fn extract_u64_eq(filters: &[Expr], col_name: &str) -> Option<u64> {
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

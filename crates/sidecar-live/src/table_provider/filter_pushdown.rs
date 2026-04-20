use std::collections::HashSet;

use alloy_primitives::Address;
use arrow::array::{Array, BooleanArray, FixedSizeBinaryArray, UInt32Array};
use datafusion::{
    common::ScalarValue,
    logical_expr::{BinaryExpr, Operator},
    prelude::Expr,
};
use roaring::RoaringBitmap;

use crate::entity_store::IndexSnapshot;

/// Matches `owner = <20-byte literal>`.
pub fn extract_owner_eq(expr: &Expr) -> Option<Address> {
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
        && let Expr::Column(col) = left.as_ref()
        && col.name == glint_primitives::columns::OWNER
        && let Expr::Literal(
            ScalarValue::FixedSizeBinary(20, Some(bytes)) | ScalarValue::Binary(Some(bytes)),
            _,
        ) = right.as_ref()
        && bytes.len() == 20
    {
        Some(Address::from_slice(bytes))
    } else {
        None
    }
}

/// Matches paired `ann_key = <literal>` and `ann_value = <literal>` filters.
pub fn extract_string_ann_eq(filters: &[&Expr]) -> Option<(String, String)> {
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;

    for expr in filters {
        if let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) = expr
            && let Expr::Column(col) = left.as_ref()
            && let Expr::Literal(ScalarValue::Utf8(Some(s)), _) = right.as_ref()
        {
            if col.name == glint_primitives::exex_schema::ann_columns::ANN_KEY {
                key = Some(s.clone());
            } else if col.name == glint_primitives::exex_schema::ann_columns::ANN_VALUE {
                value = Some(s.clone());
            }
        }
    }

    key.zip(value)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericOp {
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

/// Matches paired `ann_key = <literal>` and `ann_value {=,>,>=,<,<=} <u64>` filters.
pub fn extract_numeric_ann_pred(filters: &[&Expr]) -> Option<(String, NumericOp, u64)> {
    let mut key: Option<String> = None;
    let mut pred: Option<(NumericOp, u64)> = None;

    for expr in filters {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr
            && let Expr::Column(col) = left.as_ref()
        {
            if col.name == glint_primitives::exex_schema::ann_columns::ANN_KEY
                && *op == Operator::Eq
                && let Expr::Literal(ScalarValue::Utf8(Some(s)), _) = right.as_ref()
            {
                key = Some(s.clone());
            } else if col.name == glint_primitives::exex_schema::ann_columns::ANN_VALUE {
                let v = match right.as_ref() {
                    Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v),
                    Expr::Literal(ScalarValue::Int64(Some(v)), _) => u64::try_from(*v).ok(),
                    _ => None,
                };
                if let Some(v) = v {
                    let numeric_op = match op {
                        Operator::Eq => NumericOp::Eq,
                        Operator::Gt => NumericOp::Gt,
                        Operator::GtEq => NumericOp::GtEq,
                        Operator::Lt => NumericOp::Lt,
                        Operator::LtEq => NumericOp::LtEq,
                        _ => continue,
                    };
                    pred = Some((numeric_op, v));
                }
            }
        }
    }

    key.zip(pred).map(|(k, (op, v))| (k, op, v))
}

/// Resolves bitmap slots to sorted row indices via `slot_to_row`.
#[allow(clippy::cast_possible_truncation)]
pub fn bitmap_to_row_indices(bitmap: &RoaringBitmap, indexes: &IndexSnapshot) -> UInt32Array {
    let mut indices: Vec<u32> = bitmap
        .iter()
        .filter_map(|slot| indexes.slot_to_row.get(&slot).map(|&row| row as u32))
        .collect();
    indices.sort_unstable();
    UInt32Array::from(indices)
}

/// Keeps only annotation rows whose `entity_key` matches a bitmap slot.
pub fn filter_annotation_batch_by_bitmap(
    entities_batch: &arrow::record_batch::RecordBatch,
    annotation_batch: &arrow::record_batch::RecordBatch,
    bitmap: &RoaringBitmap,
    indexes: &IndexSnapshot,
) -> datafusion::common::Result<arrow::record_batch::RecordBatch> {
    let row_indices = bitmap_to_row_indices(bitmap, indexes);

    let ek_col_idx = entities_batch
        .schema()
        .index_of(glint_primitives::columns::ENTITY_KEY)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    let entities_ek = entities_batch
        .column(ek_col_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("entity_key must be FixedSizeBinary(32)");

    let matching_keys: HashSet<&[u8]> = row_indices
        .values()
        .iter()
        .filter(|&&idx| (idx as usize) < entities_ek.len())
        .map(|&idx| entities_ek.value(idx as usize))
        .collect();

    let ann_ek_col_idx = annotation_batch
        .schema()
        .index_of(glint_primitives::columns::ENTITY_KEY)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    let ann_ek = annotation_batch
        .column(ann_ek_col_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("annotation entity_key must be FixedSizeBinary(32)");

    let mask: BooleanArray = (0..annotation_batch.num_rows())
        .map(|i| Some(matching_keys.contains(ann_ek.value(i))))
        .collect();

    arrow::compute::filter_record_batch(annotation_batch, &mask)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// True if the expression references `ann_key` or `ann_value`.
pub fn is_ann_column(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => is_ann_column(&binary.left) || is_ann_column(&binary.right),
        Expr::Column(col) => {
            col.name == glint_primitives::exex_schema::ann_columns::ANN_KEY
                || col.name == glint_primitives::exex_schema::ann_columns::ANN_VALUE
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::Column;

    fn owner_eq_expr(addr: Address) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("owner"))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::FixedSizeBinary(20, Some(addr.as_slice().to_vec())),
                None,
            )),
        ))
    }

    #[test]
    fn extracts_owner_address() {
        let addr = Address::repeat_byte(0x42);
        assert_eq!(extract_owner_eq(&owner_eq_expr(addr)), Some(addr));
    }

    #[test]
    fn rejects_non_owner_column() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("entity_key"))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::FixedSizeBinary(20, Some(vec![0; 20])),
                None,
            )),
        ));
        assert_eq!(extract_owner_eq(&expr), None);
    }

    #[test]
    fn rejects_non_eq_operator() {
        let addr = Address::repeat_byte(0x42);
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("owner"))),
            Operator::NotEq,
            Box::new(Expr::Literal(
                ScalarValue::FixedSizeBinary(20, Some(addr.as_slice().to_vec())),
                None,
            )),
        ));
        assert_eq!(extract_owner_eq(&expr), None);
    }

    #[test]
    fn rejects_wrong_size_binary() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("owner"))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::FixedSizeBinary(32, Some(vec![0; 32])),
                None,
            )),
        ));
        assert_eq!(extract_owner_eq(&expr), None);
    }

    fn ann_eq_expr(col_name: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(col_name))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(value.to_owned())),
                None,
            )),
        ))
    }

    #[test]
    fn extracts_string_ann_key_and_value() {
        let filters = [
            ann_eq_expr("ann_key", "category"),
            ann_eq_expr("ann_value", "nft"),
        ];
        let refs: Vec<&Expr> = filters.iter().collect();
        let result = extract_string_ann_eq(&refs);
        assert_eq!(result, Some(("category".to_owned(), "nft".to_owned())));
    }

    #[test]
    fn string_ann_missing_value_returns_none() {
        let filters = [ann_eq_expr("ann_key", "category")];
        let refs: Vec<&Expr> = filters.iter().collect();
        assert_eq!(extract_string_ann_eq(&refs), None);
    }

    #[test]
    fn string_ann_missing_key_returns_none() {
        let filters = [ann_eq_expr("ann_value", "nft")];
        let refs: Vec<&Expr> = filters.iter().collect();
        assert_eq!(extract_string_ann_eq(&refs), None);
    }

    fn num_expr(col_name: &str, op: Operator, val: u64) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(col_name))),
            op,
            Box::new(Expr::Literal(ScalarValue::UInt64(Some(val)), None)),
        ))
    }

    #[test]
    fn extracts_numeric_eq() {
        let filters = [
            ann_eq_expr("ann_key", "price"),
            num_expr("ann_value", Operator::Eq, 1000),
        ];
        let refs: Vec<&Expr> = filters.iter().collect();
        let result = extract_numeric_ann_pred(&refs);
        assert_eq!(result, Some(("price".to_owned(), NumericOp::Eq, 1000)));
    }

    #[test]
    fn extracts_numeric_gt() {
        let filters = [
            ann_eq_expr("ann_key", "price"),
            num_expr("ann_value", Operator::Gt, 500),
        ];
        let refs: Vec<&Expr> = filters.iter().collect();
        let result = extract_numeric_ann_pred(&refs);
        assert_eq!(result, Some(("price".to_owned(), NumericOp::Gt, 500)));
    }

    #[test]
    fn numeric_missing_key_returns_none() {
        let filters = [num_expr("ann_value", Operator::Eq, 1000)];
        let refs: Vec<&Expr> = filters.iter().collect();
        assert_eq!(extract_numeric_ann_pred(&refs), None);
    }

    #[test]
    fn numeric_missing_value_returns_none() {
        let filters = [ann_eq_expr("ann_key", "price")];
        let refs: Vec<&Expr> = filters.iter().collect();
        assert_eq!(extract_numeric_ann_pred(&refs), None);
    }

    #[test]
    fn bitmap_to_row_indices_sorted() {
        use std::collections::HashMap;

        let mut slot_to_row = HashMap::new();
        slot_to_row.insert(5, 2usize);
        slot_to_row.insert(3, 0);
        slot_to_row.insert(10, 1);

        let indexes = IndexSnapshot {
            string_ann_index: HashMap::new(),
            numeric_ann_index: HashMap::new(),
            numeric_ann_range: HashMap::new(),
            owner_index: HashMap::new(),
            all_live_slots: RoaringBitmap::new(),
            slot_to_row,
        };

        let mut bm = RoaringBitmap::new();
        bm.insert(5);
        bm.insert(3);
        bm.insert(10);

        let result = bitmap_to_row_indices(&bm, &indexes);
        assert_eq!(result.values(), &[0, 1, 2]);
    }

    #[test]
    fn bitmap_to_row_indices_missing_slots_skipped() {
        use std::collections::HashMap;

        let mut slot_to_row = HashMap::new();
        slot_to_row.insert(3, 0usize);

        let indexes = IndexSnapshot {
            string_ann_index: HashMap::new(),
            numeric_ann_index: HashMap::new(),
            numeric_ann_range: HashMap::new(),
            owner_index: HashMap::new(),
            all_live_slots: RoaringBitmap::new(),
            slot_to_row,
        };

        let mut bm = RoaringBitmap::new();
        bm.insert(3);
        bm.insert(99);

        let result = bitmap_to_row_indices(&bm, &indexes);
        assert_eq!(result.values(), &[0]);
    }
}

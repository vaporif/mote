use std::{any::Any, future::Future, ops::Bound, pin::Pin, sync::Arc};

use alloy_primitives::Address;
use arrow::{
    array::{Array, ArrayRef, AsArray, StringBuilder, UInt64Builder},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::Session,
    common::Result as DfResult,
    datasource::{MemTable, TableProvider, TableType},
    execution::context::SessionContext,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        TableProviderFilterPushDown, Volatility, expr::InList,
    },
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use roaring::RoaringBitmap;
use tokio::sync::watch;

use crate::entity_store::{IndexSnapshot, Snapshot, entity_schema};

enum FilterMatch {
    Resolved(RoaringBitmap),
    Unresolved,
}

fn match_filter(expr: &Expr, indexes: &IndexSnapshot) -> FilterMatch {
    match expr {
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr { left, op, right }) => {
            match_binary_expr(left, *op, right, indexes)
        }
        Expr::InList(in_list) => match_in_list(in_list, indexes),
        _ => FilterMatch::Unresolved,
    }
}

fn extract_owner_literal(expr: &Expr) -> Option<Address> {
    match expr {
        Expr::Literal(
            datafusion::common::ScalarValue::FixedSizeBinary(20, Some(bytes))
            | datafusion::common::ScalarValue::Binary(Some(bytes)),
            _,
        ) => {
            if bytes.len() == 20 {
                Some(Address::from_slice(bytes))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_owner_col(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name() == "owner")
}

fn match_binary_expr(
    left: &Expr,
    op: datafusion::logical_expr::Operator,
    right: &Expr,
    indexes: &IndexSnapshot,
) -> FilterMatch {
    use datafusion::logical_expr::Operator;

    // owner = x'...' or x'...' = owner
    let owner_pair = match (is_owner_col(left), is_owner_col(right)) {
        (true, false) => extract_owner_literal(right),
        (false, true) => extract_owner_literal(left),
        _ => None,
    };
    if let Some(addr) = owner_pair {
        let bm = indexes.owner_index.get(&addr).cloned().unwrap_or_default();
        return match op {
            Operator::Eq => FilterMatch::Resolved(bm),
            Operator::NotEq => FilterMatch::Resolved(&indexes.all_live_slots - &bm),
            _ => FilterMatch::Unresolved,
        };
    }

    if op == Operator::And {
        let left_result = match_filter(left, indexes);
        let right_result = match_filter(right, indexes);
        return match (left_result, right_result) {
            (FilterMatch::Resolved(a), FilterMatch::Resolved(b)) => FilterMatch::Resolved(&a & &b),
            (FilterMatch::Resolved(a), FilterMatch::Unresolved) => FilterMatch::Resolved(a),
            (FilterMatch::Unresolved, FilterMatch::Resolved(b)) => FilterMatch::Resolved(b),
            (FilterMatch::Unresolved, FilterMatch::Unresolved) => FilterMatch::Unresolved,
        };
    }

    if op == Operator::Or {
        let left_result = match_filter(left, indexes);
        let right_result = match_filter(right, indexes);
        return match (left_result, right_result) {
            (FilterMatch::Resolved(a), FilterMatch::Resolved(b)) => FilterMatch::Resolved(&a | &b),
            _ => FilterMatch::Unresolved,
        };
    }

    match_udf_binary_expr(left, op, right, indexes)
}

fn extract_udf_call(expr: &Expr) -> Option<(&str, String)> {
    if let Expr::ScalarFunction(func) = expr {
        let name = func.func.name();
        if (name == "str_ann" || name == "num_ann")
            && func.args.len() == 2
            && let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(key)), _) =
                &func.args[1]
        {
            return Some((name, key.clone()));
        }
    }
    None
}

fn extract_string_literal(expr: &Expr) -> Option<String> {
    if let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(s)), _) = expr {
        Some(s.clone())
    } else {
        None
    }
}

enum NumericLiteral {
    Value(u64),
    Negative,
}

const fn extract_u64_literal(expr: &Expr) -> Option<NumericLiteral> {
    match expr {
        Expr::Literal(datafusion::common::ScalarValue::Int64(Some(v)), _) => {
            if *v >= 0 {
                Some(NumericLiteral::Value((*v).unsigned_abs()))
            } else {
                Some(NumericLiteral::Negative)
            }
        }
        Expr::Literal(datafusion::common::ScalarValue::UInt64(Some(v)), _) => {
            Some(NumericLiteral::Value(*v))
        }
        _ => None,
    }
}

fn match_udf_binary_expr(
    left: &Expr,
    op: datafusion::logical_expr::Operator,
    right: &Expr,
    indexes: &IndexSnapshot,
) -> FilterMatch {
    use datafusion::logical_expr::Operator;

    let Some((udf_name, ann_key)) = extract_udf_call(left) else {
        return FilterMatch::Unresolved;
    };

    match udf_name {
        "str_ann" => {
            let Some(value) = extract_string_literal(right) else {
                return FilterMatch::Unresolved;
            };
            let bm = indexes
                .string_ann_index
                .get(&(ann_key, value))
                .cloned()
                .unwrap_or_default();
            match op {
                Operator::Eq => FilterMatch::Resolved(bm),
                Operator::NotEq => FilterMatch::Resolved(&indexes.all_live_slots - &bm),
                _ => FilterMatch::Unresolved,
            }
        }
        "num_ann" => {
            let Some(lit) = extract_u64_literal(right) else {
                return FilterMatch::Unresolved;
            };
            match lit {
                NumericLiteral::Negative => match op {
                    Operator::Eq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq => FilterMatch::Resolved(RoaringBitmap::new()),
                    Operator::NotEq => FilterMatch::Resolved(indexes.all_live_slots.clone()),
                    _ => FilterMatch::Unresolved,
                },
                NumericLiteral::Value(val) => match_numeric_ann(&ann_key, op, val, indexes),
            }
        }
        _ => FilterMatch::Unresolved,
    }
}

fn match_numeric_ann(
    ann_key: &str,
    op: datafusion::logical_expr::Operator,
    val: u64,
    indexes: &IndexSnapshot,
) -> FilterMatch {
    use datafusion::logical_expr::Operator;

    match op {
        Operator::Eq | Operator::NotEq => {
            let bm = indexes
                .numeric_ann_index
                .get(&(ann_key.to_owned(), val))
                .cloned()
                .unwrap_or_default();
            if op == Operator::Eq {
                FilterMatch::Resolved(bm)
            } else {
                FilterMatch::Resolved(&indexes.all_live_slots - &bm)
            }
        }
        Operator::Gt => range_union(ann_key, (Bound::Excluded(val), Bound::Unbounded), indexes),
        Operator::GtEq => range_union(ann_key, (Bound::Included(val), Bound::Unbounded), indexes),
        Operator::Lt => range_union(ann_key, (Bound::Unbounded, Bound::Excluded(val)), indexes),
        Operator::LtEq => range_union(ann_key, (Bound::Unbounded, Bound::Included(val)), indexes),
        _ => FilterMatch::Unresolved,
    }
}

fn range_union(
    ann_key: &str,
    range: (Bound<u64>, Bound<u64>),
    indexes: &IndexSnapshot,
) -> FilterMatch {
    let Some(btree) = indexes.numeric_ann_range.get(ann_key) else {
        return FilterMatch::Resolved(RoaringBitmap::new());
    };
    let mut result = RoaringBitmap::new();
    for (_, bm) in btree.range(range) {
        result |= bm;
    }
    FilterMatch::Resolved(result)
}

fn resolve_in_list<'a, T>(
    items: &[Expr],
    extract: impl Fn(&Expr) -> Option<T>,
    lookup: impl Fn(&T) -> Option<&'a RoaringBitmap>,
    all_live: &RoaringBitmap,
    negated: bool,
) -> FilterMatch {
    let mut result = RoaringBitmap::new();
    for item in items {
        let Some(val) = extract(item) else {
            return FilterMatch::Unresolved;
        };
        if let Some(bm) = lookup(&val) {
            result |= bm;
        }
    }
    if negated {
        FilterMatch::Resolved(all_live - &result)
    } else {
        FilterMatch::Resolved(result)
    }
}

fn match_in_list(in_list: &InList, indexes: &IndexSnapshot) -> FilterMatch {
    if is_owner_col(&in_list.expr) {
        return resolve_in_list(
            &in_list.list,
            extract_owner_literal,
            |addr| indexes.owner_index.get(addr),
            &indexes.all_live_slots,
            in_list.negated,
        );
    }

    if let Some(("str_ann", ann_key)) = extract_udf_call(&in_list.expr) {
        return resolve_in_list(
            &in_list.list,
            extract_string_literal,
            |val| {
                indexes
                    .string_ann_index
                    .get(&(ann_key.clone(), val.clone()))
            },
            &indexes.all_live_slots,
            in_list.negated,
        );
    }

    if let Some(("num_ann", ann_key)) = extract_udf_call(&in_list.expr) {
        return resolve_in_list(
            &in_list.list,
            |e| match extract_u64_literal(e) {
                Some(NumericLiteral::Value(v)) => Some(v),
                _ => None,
            },
            |val| indexes.numeric_ann_index.get(&(ann_key.clone(), *val)),
            &indexes.all_live_slots,
            in_list.negated,
        );
    }

    FilterMatch::Unresolved
}

#[derive(Debug)]
pub struct IndexedTableProvider {
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
}

impl IndexedTableProvider {
    pub const fn new(snapshot_rx: watch::Receiver<Arc<Snapshot>>) -> Self {
        Self { snapshot_rx }
    }
}

impl TableProvider for IndexedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        entity_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        let indexes = Arc::clone(&self.snapshot_rx.borrow().indexes);
        Ok(filters
            .iter()
            .map(|f| match match_filter(f, &indexes) {
                FilterMatch::Resolved(_) => TableProviderFilterPushDown::Inexact,
                FilterMatch::Unresolved => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = DfResult<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let snapshot: Arc<Snapshot> = Arc::clone(&*self.snapshot_rx.borrow());
        let schema = entity_schema();
        let filters = filters.to_vec();

        Box::pin(async move {
            let indexes = &snapshot.indexes;

            // Intersect all resolved filter bitmaps; unresolved filters are skipped
            // here because DataFusion applies them post-scan (they're marked Unsupported
            // in supports_filters_pushdown).
            let mut result_bitmap: Option<RoaringBitmap> = None;
            for filter in &filters {
                if let FilterMatch::Resolved(bm) = match_filter(filter, indexes) {
                    result_bitmap = Some(match result_bitmap {
                        Some(existing) => existing & &bm,
                        None => bm,
                    });
                }
            }

            let batch = if let Some(bm) = result_bitmap {
                let row_indices: Vec<usize> = bm
                    .iter()
                    .filter_map(|slot| indexes.slot_to_row.get(&slot).copied())
                    .collect();

                if row_indices.len() == snapshot.batch.num_rows() {
                    (*snapshot.batch).clone()
                } else {
                    let indices = arrow::array::UInt32Array::from(
                        row_indices
                            .iter()
                            .map(|&i| {
                                u32::try_from(i).map_err(|_| {
                                    datafusion::common::DataFusionError::Execution(format!(
                                        "row index {i} exceeds u32::MAX"
                                    ))
                                })
                            })
                            .collect::<DfResult<Vec<u32>>>()?,
                    );
                    let columns: Vec<ArrayRef> = snapshot
                        .batch
                        .columns()
                        .iter()
                        .map(|col| arrow::compute::take(col, &indices, None))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| {
                            datafusion::common::DataFusionError::Execution(format!(
                                "index take failed: {e}"
                            ))
                        })?;
                    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "filtered batch construction failed: {e}"
                        ))
                    })?
                }
            } else {
                (*snapshot.batch).clone()
            };

            let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
            mem_table.scan(state, projection, &[], limit).await
        })
    }
}

pub fn create_session_context(
    snapshot_rx: watch::Receiver<Arc<Snapshot>>,
) -> DfResult<SessionContext> {
    // TODO: configure FairSpillPool memory limit to prevent OOM from large intermediate results
    let ctx = SessionContext::new();

    let table = IndexedTableProvider::new(snapshot_rx);
    ctx.register_table("entities", Arc::new(table))?;

    ctx.register_udf(ScalarUDF::from(StrAnnUdf::new()));
    ctx.register_udf(ScalarUDF::from(NumAnnUdf::new()));

    Ok(ctx)
}

/// `str_ann(string_annotations, 'key')` -- look up a string annotation by key, NULL if absent.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StrAnnUdf {
    signature: Signature,
}

impl StrAnnUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Map(
                        Arc::new(arrow::datatypes::Field::new(
                            "entries",
                            DataType::Struct(arrow::datatypes::Fields::from(vec![
                                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                                arrow::datatypes::Field::new("value", DataType::Utf8, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StrAnnUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "str_ann"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        let map_array = arrays[0]
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "str_ann: first argument must be a MapArray".into(),
                )
            })?;

        let key_array = arrays[1].as_string::<i32>();
        let n = map_array.len();
        let mut builder = StringBuilder::with_capacity(n, n * 8);

        let map_keys = map_array.keys().as_string::<i32>();
        let map_values = map_array.values().as_string::<i32>();
        let offsets = map_array.offsets();

        for row in 0..n {
            let lookup_key = key_array.value(row);
            let start = usize::try_from(offsets[row])
                .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;
            let end = usize::try_from(offsets[row + 1])
                .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;

            let found = (start..end).find_map(|i| {
                (map_keys.value(i) == lookup_key && !map_values.is_null(i))
                    .then(|| map_values.value(i))
            });

            match found {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// `num_ann(numeric_annotations, 'key')` -- look up a numeric annotation by key, NULL if absent.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NumAnnUdf {
    signature: Signature,
}

impl NumAnnUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Map(
                        Arc::new(arrow::datatypes::Field::new(
                            "entries",
                            DataType::Struct(arrow::datatypes::Fields::from(vec![
                                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                                arrow::datatypes::Field::new("value", DataType::UInt64, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NumAnnUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "num_ann"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        let map_array = arrays[0]
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "num_ann: first argument must be a MapArray".into(),
                )
            })?;

        let key_array = arrays[1].as_string::<i32>();
        let n = map_array.len();
        let mut builder = UInt64Builder::with_capacity(n);

        let map_keys = map_array.keys().as_string::<i32>();
        let map_values = map_array
            .values()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "num_ann: map values must be UInt64".into(),
                )
            })?;
        let offsets = map_array.offsets();

        for row in 0..n {
            let lookup_key = key_array.value(row);
            let start = usize::try_from(offsets[row])
                .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;
            let end = usize::try_from(offsets[row + 1])
                .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;

            let found = (start..end).find_map(|i| {
                (map_keys.value(i) == lookup_key && !map_values.is_null(i))
                    .then(|| map_values.value(i))
            });

            match found {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_store::{EntityRow, EntityStore, IndexSnapshot};
    use alloy_primitives::{Address, B256, Bytes};
    use datafusion::common::ScalarValue;
    use roaring::RoaringBitmap;
    use std::collections::{BTreeMap, HashMap};

    fn sample_store() -> EntityStore {
        let mut store = EntityStore::new();
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x01),
            owner: Address::repeat_byte(0x02),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_annotations: vec![("category".into(), "nft".into())],
            numeric_annotations: vec![("price".into(), 1000)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: Some(Address::ZERO),
        });
        store
    }

    #[tokio::test]
    async fn query_entities_table() {
        let store = sample_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = tokio::sync::watch::channel(snapshot);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx).unwrap();
        let df = ctx.sql("SELECT content_type FROM entities").await.unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn str_ann_udf() {
        let store = sample_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = tokio::sync::watch::channel(snapshot);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx).unwrap();
        let df = ctx
            .sql("SELECT * FROM entities WHERE str_ann(string_annotations, 'category') = 'nft'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn num_ann_udf() {
        let store = sample_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = tokio::sync::watch::channel(snapshot);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx).unwrap();
        let df = ctx
            .sql("SELECT * FROM entities WHERE num_ann(numeric_annotations, 'price') = 1000")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn empty_store_returns_zero_rows() {
        let store = EntityStore::new();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = tokio::sync::watch::channel(snapshot);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx).unwrap();
        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM entities")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    fn sample_index_snapshot() -> IndexSnapshot {
        let mut string_ann_index = HashMap::new();
        let mut numeric_ann_index = HashMap::new();
        let mut numeric_ann_range: HashMap<String, BTreeMap<u64, RoaringBitmap>> = HashMap::new();
        let mut owner_index = HashMap::new();
        let mut all_live_slots = RoaringBitmap::new();
        all_live_slots.insert(0);
        all_live_slots.insert(1);
        all_live_slots.insert(2);

        let mut owner1_bm = RoaringBitmap::new();
        owner1_bm.insert(0);
        owner1_bm.insert(2);
        owner_index.insert(Address::repeat_byte(0x01), owner1_bm);
        let mut owner2_bm = RoaringBitmap::new();
        owner2_bm.insert(1);
        owner_index.insert(Address::repeat_byte(0x02), owner2_bm);

        let mut nft_bm = RoaringBitmap::new();
        nft_bm.insert(0);
        nft_bm.insert(2);
        string_ann_index.insert(("category".to_owned(), "nft".to_owned()), nft_bm);
        let mut defi_bm = RoaringBitmap::new();
        defi_bm.insert(1);
        string_ann_index.insert(("category".to_owned(), "defi".to_owned()), defi_bm);

        let mut p1000 = RoaringBitmap::new();
        p1000.insert(0);
        numeric_ann_index.insert(("price".to_owned(), 1000), p1000.clone());
        let mut p2000 = RoaringBitmap::new();
        p2000.insert(1);
        numeric_ann_index.insert(("price".to_owned(), 2000), p2000.clone());
        let mut p3000 = RoaringBitmap::new();
        p3000.insert(2);
        numeric_ann_index.insert(("price".to_owned(), 3000), p3000.clone());

        let mut price_btree = BTreeMap::new();
        price_btree.insert(1000, p1000);
        price_btree.insert(2000, p2000);
        price_btree.insert(3000, p3000);
        numeric_ann_range.insert("price".to_owned(), price_btree);

        IndexSnapshot {
            string_ann_index,
            numeric_ann_index,
            numeric_ann_range,
            owner_index,
            all_live_slots,
            slot_to_row: HashMap::from([(0, 0), (1, 1), (2, 2)]),
        }
    }

    fn owner_eq_expr(byte: u8) -> Expr {
        use datafusion::prelude::*;
        col("owner").eq(lit(ScalarValue::FixedSizeBinary(
            20,
            Some(Address::repeat_byte(byte).to_vec()),
        )))
    }

    #[test]
    fn filter_match_owner_eq() {
        let idx = sample_index_snapshot();
        let expr = owner_eq_expr(0x01);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
                assert!(bm.contains(0));
                assert!(bm.contains(2));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_owner_neq() {
        use datafusion::prelude::*;
        let idx = sample_index_snapshot();
        let expr = col("owner").not_eq(lit(ScalarValue::FixedSizeBinary(
            20,
            Some(Address::repeat_byte(0x01).to_vec()),
        )));
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 1);
                assert!(bm.contains(1));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    fn str_ann_eq_expr(key: &str, value: &str) -> Expr {
        use datafusion::prelude::*;
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(StrAnnUdf::new())),
                    args: vec![col("string_annotations"), lit(key)],
                },
            )),
            op: datafusion::logical_expr::Operator::Eq,
            right: Box::new(lit(value)),
        })
    }

    #[test]
    fn filter_match_str_ann_eq() {
        let idx = sample_index_snapshot();
        let expr = str_ann_eq_expr("category", "nft");
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
                assert!(bm.contains(0));
                assert!(bm.contains(2));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_str_ann_not_found() {
        let idx = sample_index_snapshot();
        let expr = str_ann_eq_expr("category", "gaming");
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => assert!(bm.is_empty()),
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    fn num_ann_expr(key: &str, op: datafusion::logical_expr::Operator, value: i64) -> Expr {
        use datafusion::prelude::*;
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(NumAnnUdf::new())),
                    args: vec![col("numeric_annotations"), lit(key)],
                },
            )),
            op,
            right: Box::new(lit(ScalarValue::Int64(Some(value)))),
        })
    }

    #[test]
    fn filter_match_num_ann_eq() {
        let idx = sample_index_snapshot();
        let expr = num_ann_expr("price", datafusion::logical_expr::Operator::Eq, 1000);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 1);
                assert!(bm.contains(0));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_num_ann_gt() {
        let idx = sample_index_snapshot();
        let expr = num_ann_expr("price", datafusion::logical_expr::Operator::Gt, 1000);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
                assert!(bm.contains(1));
                assert!(bm.contains(2));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_num_ann_gte() {
        let idx = sample_index_snapshot();
        let expr = num_ann_expr("price", datafusion::logical_expr::Operator::GtEq, 2000);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
                assert!(bm.contains(1));
                assert!(bm.contains(2));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_num_ann_lt() {
        let idx = sample_index_snapshot();
        let expr = num_ann_expr("price", datafusion::logical_expr::Operator::Lt, 3000);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
                assert!(bm.contains(0));
                assert!(bm.contains(1));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_num_ann_lte() {
        let idx = sample_index_snapshot();
        let expr = num_ann_expr("price", datafusion::logical_expr::Operator::LtEq, 2000);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
                assert!(bm.contains(0));
                assert!(bm.contains(1));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_num_ann_negative_literal() {
        let idx = sample_index_snapshot();
        let expr = num_ann_expr("price", datafusion::logical_expr::Operator::Eq, -1);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => assert!(bm.is_empty()),
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_num_ann_negative_neq() {
        let idx = sample_index_snapshot();
        let expr = num_ann_expr("price", datafusion::logical_expr::Operator::NotEq, -1);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => assert_eq!(bm.len(), 3),
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_and_both_resolved() {
        let idx = sample_index_snapshot();
        let expr = owner_eq_expr(0x01).and(str_ann_eq_expr("category", "nft"));
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_and_mixed() {
        let idx = sample_index_snapshot();
        let unresolvable =
            Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(true)), None);
        let expr = owner_eq_expr(0x01).and(unresolvable);
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_or_both_resolved() {
        let idx = sample_index_snapshot();
        let expr = owner_eq_expr(0x01).or(owner_eq_expr(0x02));
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 3);
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_or_mixed_is_unresolved() {
        let idx = sample_index_snapshot();
        let unresolvable =
            Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(true)), None);
        let expr = owner_eq_expr(0x01).or(unresolvable);
        assert!(matches!(
            super::match_filter(&expr, &idx),
            super::FilterMatch::Unresolved
        ));
    }

    #[test]
    fn filter_match_owner_in_list() {
        use datafusion::prelude::*;
        let idx = sample_index_snapshot();
        let expr = Expr::InList(InList {
            expr: Box::new(col("owner")),
            list: vec![
                lit(ScalarValue::FixedSizeBinary(
                    20,
                    Some(Address::repeat_byte(0x01).to_vec()),
                )),
                lit(ScalarValue::FixedSizeBinary(
                    20,
                    Some(Address::repeat_byte(0x02).to_vec()),
                )),
            ],
            negated: false,
        });
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => assert_eq!(bm.len(), 3),
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[tokio::test]
    async fn supports_filters_pushdown_exact_for_owner() {
        use TableProviderFilterPushDown;
        let mut store = EntityStore::new();
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x01),
            owner: Address::repeat_byte(0x02),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_annotations: vec![("category".into(), "nft".into())],
            numeric_annotations: vec![("price".into(), 1000)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: Some(Address::ZERO),
        });
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let rx = tx.subscribe();
        let table = IndexedTableProvider::new(rx);

        let owner_filter = owner_eq_expr(0x02);
        let unknown_filter =
            Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(true)), None);
        let result = table
            .supports_filters_pushdown(&[&owner_filter, &unknown_filter])
            .unwrap();
        assert_eq!(result[0], TableProviderFilterPushDown::Inexact);
        assert_eq!(result[1], TableProviderFilterPushDown::Unsupported);
    }

    fn multi_entity_store() -> EntityStore {
        let mut store = EntityStore::new();
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x01),
            owner: Address::repeat_byte(0xAA),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"one"),
            string_annotations: vec![("pair".into(), "USDC/WETH".into())],
            numeric_annotations: vec![("price".into(), 1000)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0x01),
            extend_policy: 0,
            operator: None,
        });
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x02),
            owner: Address::repeat_byte(0xBB),
            expires_at_block: 200,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"two"),
            string_annotations: vec![("pair".into(), "DAI/WETH".into())],
            numeric_annotations: vec![("price".into(), 2000)],
            created_at_block: 2,
            tx_hash: B256::repeat_byte(0x02),
            extend_policy: 0,
            operator: None,
        });
        store.insert(EntityRow {
            entity_key: B256::repeat_byte(0x03),
            owner: Address::repeat_byte(0xAA),
            expires_at_block: 300,
            content_type: "application/json".into(),
            payload: Bytes::from_static(b"three"),
            string_annotations: vec![("pair".into(), "USDC/WETH".into())],
            numeric_annotations: vec![("price".into(), 3000)],
            created_at_block: 3,
            tx_hash: B256::repeat_byte(0x03),
            extend_policy: 0,
            operator: None,
        });
        store
    }

    #[tokio::test]
    async fn integration_str_ann_filter() {
        let store = multi_entity_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();
        let df = ctx
            .sql("SELECT content_type FROM entities WHERE str_ann(string_annotations, 'pair') = 'USDC/WETH'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn integration_num_ann_range_filter() {
        let store = multi_entity_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();
        let df = ctx
            .sql("SELECT content_type FROM entities WHERE num_ann(numeric_annotations, 'price') > 1000")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn integration_combined_filters() {
        let store = multi_entity_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();
        let df = ctx
            .sql("SELECT content_type FROM entities WHERE str_ann(string_annotations, 'pair') = 'USDC/WETH' AND num_ann(numeric_annotations, 'price') >= 3000")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn integration_owner_filter() {
        let store = multi_entity_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();
        let addr = Address::repeat_byte(0xAA);
        let owner_hex = addr.to_string();
        let owner_hex = &owner_hex[2..];
        let sql = format!("SELECT COUNT(*) AS cnt FROM entities WHERE owner = x'{owner_hex}'");
        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let cnt_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 2);
    }

    #[tokio::test]
    async fn integration_update_clears_stale_index() {
        let mut store = multi_entity_store();

        let updated = EntityRow {
            entity_key: B256::repeat_byte(0x01),
            owner: Address::repeat_byte(0xAA),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"one"),
            string_annotations: vec![("pair".into(), "DAI/WETH".into())],
            numeric_annotations: vec![("price".into(), 1000)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0x01),
            extend_policy: 0,
            operator: None,
        };
        store.insert(updated);

        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();

        let df = ctx
            .sql("SELECT content_type FROM entities WHERE str_ann(string_annotations, 'pair') = 'USDC/WETH'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        let df = ctx
            .sql("SELECT content_type FROM entities WHERE str_ann(string_annotations, 'pair') = 'DAI/WETH'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn integration_removal_excludes_from_query() {
        let mut store = multi_entity_store();
        store.remove(&B256::repeat_byte(0x01));

        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();

        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM entities WHERE str_ann(string_annotations, 'pair') = 'USDC/WETH'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let cnt_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 1);
    }

    #[tokio::test]
    async fn integration_snapshot_isolation() {
        let mut store = multi_entity_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();

        store.remove(&B256::repeat_byte(0x01));
        store.remove(&B256::repeat_byte(0x02));
        store.remove(&B256::repeat_byte(0x03));

        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM entities")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let cnt_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 3);

        let _ = tx.send(Arc::new(store.snapshot().expect("snapshot should succeed")));

        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM entities")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let cnt_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0);
    }

    #[tokio::test]
    async fn integration_filter_matches_zero_rows() {
        let store = multi_entity_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();
        // Non-existent owner
        let addr = Address::repeat_byte(0xFF);
        let owner_hex = &addr.to_string()[2..];
        let sql = format!("SELECT COUNT(*) AS cnt FROM entities WHERE owner = x'{owner_hex}'");
        let df = ctx.sql(&sql).await.unwrap();
        let results = df.collect().await.unwrap();
        let cnt_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0);
    }

    #[tokio::test]
    async fn integration_filter_matches_all_rows() {
        let store = multi_entity_store();
        let snapshot = Arc::new(store.snapshot().expect("snapshot should succeed"));
        let (tx, _rx) = watch::channel(snapshot);
        let ctx = create_session_context(tx.subscribe()).unwrap();
        // All entities have price >= 1000
        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM entities WHERE num_ann(numeric_annotations, 'price') >= 1000")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let cnt_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 3);
    }

    #[test]
    fn filter_match_str_ann_in_list() {
        use datafusion::prelude::*;
        let idx = sample_index_snapshot();
        let expr = Expr::InList(InList {
            expr: Box::new(Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(StrAnnUdf::new())),
                    args: vec![col("string_annotations"), lit("category")],
                },
            )),
            list: vec![lit("nft"), lit("defi")],
            negated: false,
        });
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => assert_eq!(bm.len(), 3),
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_num_ann_in_list() {
        use datafusion::prelude::*;
        let idx = sample_index_snapshot();
        let expr = Expr::InList(InList {
            expr: Box::new(Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(NumAnnUdf::new())),
                    args: vec![col("numeric_annotations"), lit("price")],
                },
            )),
            list: vec![
                lit(ScalarValue::Int64(Some(1000))),
                lit(ScalarValue::Int64(Some(3000))),
            ],
            negated: false,
        });
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 2);
                assert!(bm.contains(0));
                assert!(bm.contains(2));
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }

    #[test]
    fn filter_match_owner_not_in_list() {
        use datafusion::prelude::*;
        let idx = sample_index_snapshot();
        let expr = Expr::InList(InList {
            expr: Box::new(col("owner")),
            list: vec![lit(ScalarValue::FixedSizeBinary(
                20,
                Some(Address::repeat_byte(0x01).to_vec()),
            ))],
            negated: true,
        });
        match super::match_filter(&expr, &idx) {
            super::FilterMatch::Resolved(bm) => {
                assert_eq!(bm.len(), 1);
                assert!(bm.contains(1)); // only owner 0x02
            }
            super::FilterMatch::Unresolved => panic!("expected Resolved"),
        }
    }
}

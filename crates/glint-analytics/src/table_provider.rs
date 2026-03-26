use std::{any::Any, future::Future, pin::Pin, sync::Arc};

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
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use tokio::sync::watch;

use crate::entity_store::entity_schema;

#[derive(Debug)]
pub struct EntitiesTable {
    snapshot_rx: watch::Receiver<Arc<RecordBatch>>,
}

impl EntitiesTable {
    pub const fn new(snapshot_rx: watch::Receiver<Arc<RecordBatch>>) -> Self {
        Self { snapshot_rx }
    }
}

impl TableProvider for EntitiesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        entity_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
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
        let batch: Arc<RecordBatch> = Arc::clone(&*self.snapshot_rx.borrow());
        let schema = entity_schema();
        Box::pin(async move {
            let mem_table = MemTable::try_new(schema, vec![vec![(*batch).clone()]])?;
            mem_table.scan(state, projection, filters, limit).await
        })
    }
}

pub fn create_session_context(snapshot_rx: watch::Receiver<Arc<RecordBatch>>) -> SessionContext {
    // TODO: configure FairSpillPool memory limit to prevent OOM from large intermediate results
    let ctx = SessionContext::new();

    let table = EntitiesTable::new(snapshot_rx);
    ctx.register_table("entities", Arc::new(table))
        .expect("failed to register entities table");

    ctx.register_udf(ScalarUDF::from(StrAnnUdf::new()));
    ctx.register_udf(ScalarUDF::from(NumAnnUdf::new()));

    ctx
}

/// `str_ann(string_annotations, 'key')` — look up a string annotation by key, NULL if absent.
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
                if map_keys.value(i) == lookup_key {
                    if map_values.is_null(i) {
                        None
                    } else {
                        Some(map_values.value(i))
                    }
                } else {
                    None
                }
            });

            match found {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// `num_ann(numeric_annotations, 'key')` — look up a numeric annotation by key, NULL if absent.
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
                if map_keys.value(i) == lookup_key {
                    if map_values.is_null(i) {
                        None
                    } else {
                        Some(map_values.value(i))
                    }
                } else {
                    None
                }
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
    use crate::entity_store::{EntityRow, EntityStore};
    use alloy_primitives::{Address, B256, Bytes};

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
        let batch = Arc::new(store.to_record_batch());
        let (tx, _rx) = tokio::sync::watch::channel(batch);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx);
        let df = ctx.sql("SELECT content_type FROM entities").await.unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn str_ann_udf() {
        let store = sample_store();
        let batch = Arc::new(store.to_record_batch());
        let (tx, _rx) = tokio::sync::watch::channel(batch);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx);
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
        let batch = Arc::new(store.to_record_batch());
        let (tx, _rx) = tokio::sync::watch::channel(batch);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx);
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
        let batch = Arc::new(store.to_record_batch());
        let (tx, _rx) = tokio::sync::watch::channel(batch);
        let rx = tx.subscribe();
        let ctx = create_session_context(rx);
        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM entities")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }
}

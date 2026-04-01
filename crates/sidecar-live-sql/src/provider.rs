use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::{
    array::{
        BinaryBuilder, FixedSizeBinaryBuilder, MapBuilder, RecordBatch, StringBuilder,
        UInt8Builder, UInt64Builder,
    },
    datatypes::SchemaRef,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Result as DfResult,
    datasource::{MemTable, TableProvider, TableType},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use glint_primitives::{columns, exex_schema::map_field_names};
use parking_lot::Mutex;
use rusqlite::Connection;

use glint_analytics::entity_store::entity_schema;

#[derive(Debug)]
pub struct SqliteLatestTableProvider {
    read_conn: Arc<Mutex<Connection>>,
    current_block: Arc<AtomicU64>,
}

impl SqliteLatestTableProvider {
    pub const fn new(read_conn: Arc<Mutex<Connection>>, current_block: Arc<AtomicU64>) -> Self {
        Self {
            read_conn,
            current_block,
        }
    }
}

#[async_trait]
impl TableProvider for SqliteLatestTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        entity_schema()
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
                if references_column(f, columns::OWNER) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let current_block = self.current_block.load(Ordering::Relaxed);
        let owner_filter = extract_owner_filter(filters);

        let conn = Arc::clone(&self.read_conn);
        let batch = tokio::task::spawn_blocking(move || {
            query_entities_latest(&conn, current_block, owner_filter.as_deref())
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e:#}")))?;

        let mem = MemTable::try_new(entity_schema(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

#[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
fn query_entities_latest(
    conn: &Arc<Mutex<Connection>>,
    current_block: u64,
    owner_filter: Option<&[u8]>,
) -> eyre::Result<RecordBatch> {
    let guard = conn.lock();

    let current_block_i64 =
        i64::try_from(current_block).map_err(|e| eyre::eyre!("block number overflow: {e}"))?;

    let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match owner_filter {
        Some(owner) => (
            "SELECT entity_key, owner, expires_at_block, content_type, payload, \
                    string_annotations, numeric_annotations, created_at_block, \
                    tx_hash, extend_policy, operator \
             FROM entities_latest \
             WHERE expires_at_block > ?1 AND owner = ?2"
                .to_owned(),
            vec![Box::new(current_block_i64), Box::new(owner.to_vec())],
        ),
        None => (
            "SELECT entity_key, owner, expires_at_block, content_type, payload, \
                    string_annotations, numeric_annotations, created_at_block, \
                    tx_hash, extend_policy, operator \
             FROM entities_latest \
             WHERE expires_at_block > ?1"
                .to_owned(),
            vec![Box::new(current_block_i64)],
        ),
    };

    let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(AsRef::as_ref).collect();
    let mut stmt = guard.prepare_cached(&sql)?;
    let mut rows = stmt.query(params_refs.as_slice())?;

    let names = map_field_names();
    let mut entity_key_builder = FixedSizeBinaryBuilder::new(32);
    let mut owner_builder = FixedSizeBinaryBuilder::new(20);
    let mut expires_builder = UInt64Builder::new();
    let mut content_type_builder = StringBuilder::new();
    let mut payload_builder = BinaryBuilder::new();
    let mut str_ann_builder = MapBuilder::new(
        Some(names.clone()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut num_ann_builder =
        MapBuilder::new(Some(names), StringBuilder::new(), UInt64Builder::new());
    let mut created_at_builder = UInt64Builder::new();
    let mut tx_hash_builder = FixedSizeBinaryBuilder::new(32);
    let mut extend_policy_builder = UInt8Builder::new();
    let mut operator_builder = FixedSizeBinaryBuilder::new(20);

    while let Some(row) = rows.next()? {
        let entity_key: Vec<u8> = row.get(0)?;
        let owner: Vec<u8> = row.get(1)?;
        let expires: i64 = row.get(2)?;
        let content_type: String = row.get(3)?;
        let payload: Vec<u8> = row.get(4)?;
        let str_ann_json: String = row.get(5)?;
        let num_ann_json: String = row.get(6)?;
        let created_at: i64 = row.get(7)?;
        let tx_hash: Vec<u8> = row.get(8)?;
        let extend_policy: i64 = row.get(9)?;
        let operator: Option<Vec<u8>> = row.get(10)?;

        entity_key_builder.append_value(&entity_key)?;
        owner_builder.append_value(&owner)?;
        expires_builder.append_value(u64::try_from(expires)?);
        content_type_builder.append_value(&content_type);
        payload_builder.append_value(&payload);

        let str_pairs: Vec<Vec<String>> = serde_json::from_str(&str_ann_json).unwrap_or_default();
        for pair in &str_pairs {
            if pair.len() == 2 {
                str_ann_builder.keys().append_value(&pair[0]);
                str_ann_builder.values().append_value(&pair[1]);
            }
        }
        str_ann_builder.append(true)?;

        let num_pairs: Vec<serde_json::Value> =
            serde_json::from_str(&num_ann_json).unwrap_or_default();
        for pair in &num_pairs {
            if let Some(arr) = pair.as_array()
                && arr.len() == 2
                && let (Some(k), Some(v)) = (arr[0].as_str(), arr[1].as_u64())
            {
                num_ann_builder.keys().append_value(k);
                num_ann_builder.values().append_value(v);
            }
        }
        num_ann_builder.append(true)?;

        created_at_builder.append_value(u64::try_from(created_at)?);
        tx_hash_builder.append_value(&tx_hash)?;
        extend_policy_builder.append_value(u8::try_from(extend_policy)?);

        match operator {
            Some(op) => operator_builder.append_value(&op)?,
            None => operator_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        entity_schema(),
        vec![
            Arc::new(entity_key_builder.finish()),
            Arc::new(owner_builder.finish()),
            Arc::new(expires_builder.finish()),
            Arc::new(content_type_builder.finish()),
            Arc::new(payload_builder.finish()),
            Arc::new(str_ann_builder.finish()),
            Arc::new(num_ann_builder.finish()),
            Arc::new(created_at_builder.finish()),
            Arc::new(tx_hash_builder.finish()),
            Arc::new(extend_policy_builder.finish()),
            Arc::new(operator_builder.finish()),
        ],
    )?;

    Ok(batch)
}

fn references_column(expr: &Expr, col_name: &str) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => {
            references_column(&binary.left, col_name) || references_column(&binary.right, col_name)
        }
        Expr::Column(col) => col.name == col_name,
        _ => false,
    }
}

fn extract_owner_filter(filters: &[Expr]) -> Option<Vec<u8>> {
    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter
            && let (
                Expr::Column(col),
                datafusion::logical_expr::Operator::Eq,
                Expr::Literal(
                    datafusion::common::ScalarValue::FixedSizeBinary(20, Some(bytes))
                    | datafusion::common::ScalarValue::Binary(Some(bytes)),
                    _,
                ),
            ) = (binary.left.as_ref(), binary.op, binary.right.as_ref())
            && col.name == columns::OWNER
            && bytes.len() == 20
        {
            return Some(bytes.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;
    use crate::schema;
    use datafusion::prelude::SessionContext;
    use rusqlite::Connection;

    fn setup() -> (Arc<Mutex<Connection>>, Arc<AtomicU64>) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sidecar_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;",
        )
        .unwrap();
        schema::create_table(&conn).unwrap();
        (Arc::new(Mutex::new(conn)), Arc::new(AtomicU64::new(50)))
    }

    fn insert_entity(conn: &Connection, key: &[u8], owner: &[u8], expires: i64) {
        conn.execute(
            "INSERT INTO entities_latest (entity_key, owner, expires_at_block, content_type, payload, string_annotations, numeric_annotations, created_at_block, tx_hash, extend_policy)
             VALUES (?1, ?2, ?3, 'text/plain', X'00', '[]', '[]', 1, ?4, 0)",
            rusqlite::params![
                key,
                owner,
                expires,
                vec![0x33u8; 32],
            ],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn schema_matches_in_memory() {
        let (read_conn, block) = setup();
        let provider = SqliteLatestTableProvider::new(read_conn, block);
        let schema = provider.schema();

        assert_eq!(schema, entity_schema());
    }

    #[tokio::test]
    async fn scan_filters_expired_entities() {
        let (conn, block) = setup();
        // current_block=50, so expires_at=40 is expired, expires_at=100 is live
        {
            let guard = conn.lock();
            insert_entity(&guard, &[0x01; 32], &[0x02; 20], 40);
            insert_entity(&guard, &[0x03; 32], &[0x04; 20], 100);
        }

        let provider = Arc::new(SqliteLatestTableProvider::new(
            Arc::clone(&conn),
            Arc::clone(&block),
        ));
        let ctx = SessionContext::new();
        ctx.register_table("entities", provider).unwrap();

        let df = ctx.sql("SELECT entity_key FROM entities").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn scan_returns_correct_columns() {
        let (conn, block) = setup();
        {
            let guard = conn.lock();
            insert_entity(&guard, &[0x01; 32], &[0x02; 20], 100);
        }

        let provider = Arc::new(SqliteLatestTableProvider::new(
            Arc::clone(&conn),
            Arc::clone(&block),
        ));
        let ctx = SessionContext::new();
        ctx.register_table("entities", provider).unwrap();

        let df = ctx.sql("SELECT * FROM entities").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 11);
    }
}

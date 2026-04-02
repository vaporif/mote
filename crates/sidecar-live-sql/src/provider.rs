use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::{
    array::{
        BinaryBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt8Builder,
        UInt64Builder,
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
use glint_primitives::{
    columns,
    exex_schema::{
        ann_columns, entities_latest_schema, numeric_annotations_schema, string_annotations_schema,
    },
};
use parking_lot::Mutex;
use rusqlite::Connection;

// ---------------------------------------------------------------------------
// SqliteEntitiesProvider
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct SqliteEntitiesProvider {
    read_conn: Arc<Mutex<Connection>>,
    current_block: Arc<AtomicU64>,
}

impl SqliteEntitiesProvider {
    pub const fn new(read_conn: Arc<Mutex<Connection>>, current_block: Arc<AtomicU64>) -> Self {
        Self {
            read_conn,
            current_block,
        }
    }
}

#[async_trait]
impl TableProvider for SqliteEntitiesProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        entities_latest_schema()
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

        let mem = MemTable::try_new(entities_latest_schema(), vec![vec![batch]])?;
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
                    created_at_block, tx_hash, extend_policy, operator \
             FROM entities_latest \
             WHERE expires_at_block > ?1 AND owner = ?2"
                .to_owned(),
            vec![Box::new(current_block_i64), Box::new(owner.to_vec())],
        ),
        None => (
            "SELECT entity_key, owner, expires_at_block, content_type, payload, \
                    created_at_block, tx_hash, extend_policy, operator \
             FROM entities_latest \
             WHERE expires_at_block > ?1"
                .to_owned(),
            vec![Box::new(current_block_i64)],
        ),
    };

    let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(AsRef::as_ref).collect();
    let mut stmt = guard.prepare_cached(&sql)?;
    let mut rows = stmt.query(params_refs.as_slice())?;

    let mut entity_key_builder = FixedSizeBinaryBuilder::new(32);
    let mut owner_builder = FixedSizeBinaryBuilder::new(20);
    let mut expires_builder = UInt64Builder::new();
    let mut content_type_builder = StringBuilder::new();
    let mut payload_builder = BinaryBuilder::new();
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
        let created_at: i64 = row.get(5)?;
        let tx_hash: Vec<u8> = row.get(6)?;
        let extend_policy: i64 = row.get(7)?;
        let operator: Option<Vec<u8>> = row.get(8)?;

        entity_key_builder.append_value(&entity_key)?;
        owner_builder.append_value(&owner)?;
        expires_builder.append_value(u64::try_from(expires)?);
        content_type_builder.append_value(&content_type);
        payload_builder.append_value(&payload);
        created_at_builder.append_value(u64::try_from(created_at)?);
        tx_hash_builder.append_value(&tx_hash)?;
        extend_policy_builder.append_value(u8::try_from(extend_policy)?);

        match operator {
            Some(op) => operator_builder.append_value(&op)?,
            None => operator_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        entities_latest_schema(),
        vec![
            Arc::new(entity_key_builder.finish()),
            Arc::new(owner_builder.finish()),
            Arc::new(expires_builder.finish()),
            Arc::new(content_type_builder.finish()),
            Arc::new(payload_builder.finish()),
            Arc::new(created_at_builder.finish()),
            Arc::new(tx_hash_builder.finish()),
            Arc::new(extend_policy_builder.finish()),
            Arc::new(operator_builder.finish()),
        ],
    )?;

    Ok(batch)
}

// ---------------------------------------------------------------------------
// SqliteStringAnnotationsProvider
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct SqliteStringAnnotationsProvider {
    read_conn: Arc<Mutex<Connection>>,
}

impl SqliteStringAnnotationsProvider {
    pub const fn new(read_conn: Arc<Mutex<Connection>>) -> Self {
        Self { read_conn }
    }
}

#[async_trait]
impl TableProvider for SqliteStringAnnotationsProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        string_annotations_schema()
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
                if references_column(f, ann_columns::ANN_KEY)
                    || references_column(f, ann_columns::ANN_VALUE)
                {
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
        let ann_filters = extract_annotation_filters(filters);
        let conn = Arc::clone(&self.read_conn);

        let batch =
            tokio::task::spawn_blocking(move || query_string_annotations(&conn, &ann_filters))
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e:#}")))?;

        let mem = MemTable::try_new(string_annotations_schema(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

#[allow(clippy::significant_drop_tightening)]
fn query_string_annotations(
    conn: &Arc<Mutex<Connection>>,
    ann_filters: &AnnotationFilters,
) -> eyre::Result<RecordBatch> {
    let guard = conn.lock();

    let mut sql =
        String::from("SELECT entity_key, ann_key, ann_value FROM entity_string_annotations");
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    let mut conditions: Vec<String> = Vec::new();

    if let Some(ref key) = ann_filters.key {
        params.push(Box::new(key.clone()));
        conditions.push(format!("ann_key = ?{}", params.len()));
    }
    if let Some(ref value) = ann_filters.string_value {
        params.push(Box::new(value.clone()));
        conditions.push(format!("ann_value = ?{}", params.len()));
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(AsRef::as_ref).collect();
    let mut stmt = guard.prepare_cached(&sql)?;
    let mut rows = stmt.query(params_refs.as_slice())?;

    let mut entity_key_builder = FixedSizeBinaryBuilder::new(32);
    let mut key_builder = StringBuilder::new();
    let mut value_builder = StringBuilder::new();

    while let Some(row) = rows.next()? {
        let entity_key: Vec<u8> = row.get(0)?;
        let ann_key: String = row.get(1)?;
        let ann_value: String = row.get(2)?;

        entity_key_builder.append_value(&entity_key)?;
        key_builder.append_value(&ann_key);
        value_builder.append_value(&ann_value);
    }

    let batch = RecordBatch::try_new(
        string_annotations_schema(),
        vec![
            Arc::new(entity_key_builder.finish()),
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ],
    )?;

    Ok(batch)
}

// ---------------------------------------------------------------------------
// SqliteNumericAnnotationsProvider
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct SqliteNumericAnnotationsProvider {
    read_conn: Arc<Mutex<Connection>>,
}

impl SqliteNumericAnnotationsProvider {
    pub const fn new(read_conn: Arc<Mutex<Connection>>) -> Self {
        Self { read_conn }
    }
}

#[async_trait]
impl TableProvider for SqliteNumericAnnotationsProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        numeric_annotations_schema()
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
                if references_column(f, ann_columns::ANN_KEY)
                    || references_column(f, ann_columns::ANN_VALUE)
                {
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
        let ann_filters = extract_annotation_filters(filters);
        let conn = Arc::clone(&self.read_conn);

        let batch =
            tokio::task::spawn_blocking(move || query_numeric_annotations(&conn, &ann_filters))
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e:#}")))?;

        let mem = MemTable::try_new(numeric_annotations_schema(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

#[allow(clippy::significant_drop_tightening)]
fn query_numeric_annotations(
    conn: &Arc<Mutex<Connection>>,
    ann_filters: &AnnotationFilters,
) -> eyre::Result<RecordBatch> {
    let guard = conn.lock();

    let mut sql =
        String::from("SELECT entity_key, ann_key, ann_value FROM entity_numeric_annotations");
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    let mut conditions: Vec<String> = Vec::new();

    if let Some(ref key) = ann_filters.key {
        params.push(Box::new(key.clone()));
        conditions.push(format!("ann_key = ?{}", params.len()));
    }
    if let Some(ref value) = ann_filters.uint_value {
        params.push(Box::new(i64::try_from(*value)?));
        conditions.push(format!("ann_value = ?{}", params.len()));
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(AsRef::as_ref).collect();
    let mut stmt = guard.prepare_cached(&sql)?;
    let mut rows = stmt.query(params_refs.as_slice())?;

    let mut entity_key_builder = FixedSizeBinaryBuilder::new(32);
    let mut key_builder = StringBuilder::new();
    let mut value_builder = UInt64Builder::new();

    while let Some(row) = rows.next()? {
        let entity_key: Vec<u8> = row.get(0)?;
        let ann_key: String = row.get(1)?;
        let ann_value: i64 = row.get(2)?;

        entity_key_builder.append_value(&entity_key)?;
        key_builder.append_value(&ann_key);
        value_builder.append_value(u64::try_from(ann_value)?);
    }

    let batch = RecordBatch::try_new(
        numeric_annotations_schema(),
        vec![
            Arc::new(entity_key_builder.finish()),
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ],
    )?;

    Ok(batch)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

#[derive(Debug, Default)]
struct AnnotationFilters {
    key: Option<String>,
    string_value: Option<String>,
    uint_value: Option<u64>,
}

fn extract_annotation_filters(filters: &[Expr]) -> AnnotationFilters {
    let mut result = AnnotationFilters::default();

    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter
            && binary.op == datafusion::logical_expr::Operator::Eq
            && let Expr::Column(col) = binary.left.as_ref()
        {
            if col.name == ann_columns::ANN_KEY {
                if let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(v)), _) =
                    binary.right.as_ref()
                {
                    result.key = Some(v.clone());
                }
            } else if col.name == ann_columns::ANN_VALUE {
                if let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(v)), _) =
                    binary.right.as_ref()
                {
                    result.string_value = Some(v.clone());
                } else if let Expr::Literal(datafusion::common::ScalarValue::UInt64(Some(v)), _) =
                    binary.right.as_ref()
                {
                    result.uint_value = Some(*v);
                }
            }
        }
    }

    result
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

    fn insert_entity_with_annotations(conn: &Connection, key: &[u8], owner: &[u8], expires: i64) {
        conn.execute(
            "INSERT INTO entities_latest (entity_key, owner, expires_at_block, content_type, \
             payload, created_at_block, tx_hash, extend_policy) \
             VALUES (?1, ?2, ?3, 'text/plain', X'00', 1, ?4, 0)",
            rusqlite::params![key, owner, expires, vec![0x33u8; 32]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO entity_string_annotations (entity_key, ann_key, ann_value) \
             VALUES (?1, 'token', 'USDC')",
            [key],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO entity_numeric_annotations (entity_key, ann_key, ann_value) \
             VALUES (?1, 'price', 3000)",
            [key],
        )
        .unwrap();
    }

    fn register_all(ctx: &SessionContext, conn: Arc<Mutex<Connection>>, block: Arc<AtomicU64>) {
        ctx.register_table(
            "entities_latest",
            Arc::new(SqliteEntitiesProvider::new(
                Arc::clone(&conn),
                Arc::clone(&block),
            )),
        )
        .unwrap();
        ctx.register_table(
            "entity_string_annotations",
            Arc::new(SqliteStringAnnotationsProvider::new(Arc::clone(&conn))),
        )
        .unwrap();
        ctx.register_table(
            "entity_numeric_annotations",
            Arc::new(SqliteNumericAnnotationsProvider::new(conn)),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn entities_has_9_columns() {
        let (conn, block) = setup();
        {
            let guard = conn.lock();
            insert_entity_with_annotations(&guard, &[0x01; 32], &[0x02; 20], 100);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn, block);

        let df = ctx.sql("SELECT * FROM entities_latest").await.unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_columns(), 9);
    }

    #[tokio::test]
    async fn scan_filters_expired_entities() {
        let (conn, block) = setup();
        {
            let guard = conn.lock();
            insert_entity_with_annotations(&guard, &[0x01; 32], &[0x02; 20], 40);
            insert_entity_with_annotations(&guard, &[0x03; 32], &[0x04; 20], 100);
        }

        let ctx = SessionContext::new();
        register_all(&ctx, conn, block);

        let df = ctx
            .sql("SELECT entity_key FROM entities_latest")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn join_string_annotation() {
        let (conn, block) = setup();
        {
            let guard = conn.lock();
            insert_entity_with_annotations(&guard, &[0x01; 32], &[0x02; 20], 100);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn, block);

        let df = ctx
            .sql(
                "SELECT e.content_type FROM entities_latest e \
                 JOIN entity_string_annotations s USING (entity_key) \
                 WHERE s.ann_key = 'token' AND s.ann_value = 'USDC'",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn join_numeric_annotation() {
        let (conn, block) = setup();
        {
            let guard = conn.lock();
            insert_entity_with_annotations(&guard, &[0x01; 32], &[0x02; 20], 100);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn, block);

        let df = ctx
            .sql(
                "SELECT e.content_type FROM entities_latest e \
                 JOIN entity_numeric_annotations n USING (entity_key) \
                 WHERE n.ann_key = 'price' AND n.ann_value = 3000",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn string_annotations_standalone() {
        let (conn, block) = setup();
        {
            let guard = conn.lock();
            insert_entity_with_annotations(&guard, &[0x01; 32], &[0x02; 20], 100);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn, block);

        let df = ctx
            .sql("SELECT * FROM entity_string_annotations")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_columns(), 3);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn numeric_annotations_standalone() {
        let (conn, block) = setup();
        {
            let guard = conn.lock();
            insert_entity_with_annotations(&guard, &[0x01; 32], &[0x02; 20], 100);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn, block);

        let df = ctx
            .sql("SELECT * FROM entity_numeric_annotations")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_columns(), 3);
        assert_eq!(results[0].num_rows(), 1);
    }
}

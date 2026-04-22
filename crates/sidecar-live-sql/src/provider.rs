use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::datatypes::SchemaRef;
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

use crate::sql_queries::{AnnotationFilters, LiveDb};

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
            let guard = conn.lock();
            LiveDb::new(&guard).query_entities_latest(current_block, owner_filter.as_deref())
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e:#}")))?;

        let mem = MemTable::try_new(entities_latest_schema(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

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

        let batch = tokio::task::spawn_blocking(move || {
            let guard = conn.lock();
            LiveDb::new(&guard).query_string_annotations(&ann_filters)
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e:#}")))?;

        let mem = MemTable::try_new(string_annotations_schema(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

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

        let batch = tokio::task::spawn_blocking(move || {
            let guard = conn.lock();
            LiveDb::new(&guard).query_numeric_annotations(&ann_filters)
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{e:#}")))?;

        let mem = MemTable::try_new(numeric_annotations_schema(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
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

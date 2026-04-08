use datafusion::prelude::Expr;

pub mod annotation_extractors;
pub mod block_range_filter;
pub mod event_numeric_annotations;
pub mod event_string_annotations;
pub mod historical_events;

use glint_primitives::exex_schema::columns;

use crate::provider::block_range_filter::extract_block_range;

const fn extract_u64_literal(expr: &Expr) -> Option<u64> {
    match expr {
        #[allow(clippy::cast_sign_loss)] // guarded by *v >= 0
        Expr::Literal(datafusion::common::ScalarValue::Int64(Some(v)), _) if *v >= 0 => {
            Some(*v as u64)
        }
        Expr::Literal(datafusion::common::ScalarValue::UInt64(Some(v)), _) => Some(*v),
        _ => None,
    }
}

fn references_column(expr: &Expr, col: &str) -> bool {
    match expr {
        Expr::Column(c) => c.name() == col,
        Expr::BinaryExpr(be) => {
            references_column(&be.left, col) || references_column(&be.right, col)
        }
        Expr::Between(b) => references_column(&b.expr, col),
        _ => false,
    }
}

fn references_block_number(expr: &Expr) -> bool {
    references_column(expr, columns::BLOCK_NUMBER)
}

/// Require `block_number` bounds, return error if missing or inverted.
fn require_block_range(filters: &[Expr]) -> datafusion::error::Result<(u64, u64)> {
    let range = extract_block_range(filters).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(
            "historical queries require both lower and upper block_number bounds \
             (e.g., WHERE block_number BETWEEN 100 AND 500)"
                .to_owned(),
        )
    })?;

    if range.0 > range.1 {
        return Err(datafusion::error::DataFusionError::Plan(format!(
            "invalid block range: lower ({}) > upper ({})",
            range.0, range.1
        )));
    }

    Ok(range)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        provider::{
            event_numeric_annotions::EventNumericAnnotationsProvider,
            event_string_annnotations::EventStringAnnotationsProvider,
            historical_events::HistoricalEventsProvider,
        },
        schema,
    };
    use datafusion::prelude::SessionContext;
    use parking_lot::Mutex;
    use rusqlite::Connection;

    fn setup() -> Arc<Mutex<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        schema::create_tables(&conn).unwrap();
        Arc::new(Mutex::new(conn))
    }

    fn insert_event_with_annotations(conn: &Connection) {
        conn.execute(
            "INSERT INTO entity_events (block_number, block_hash, tx_index, tx_hash, log_index, event_type, entity_key, owner, expires_at_block, content_type, payload, extend_policy, gas_cost)
             VALUES (10, X'AA', 0, X'BB', 0, 0, X'0101010101010101010101010101010101010101010101010101010101010101', X'0202020202020202020202020202020202020202', 100, 'text/plain', X'00', 0, 21000)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO event_string_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', 10, 0, 'token', 'USDC')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO event_numeric_annotations (entity_key, block_number, log_index, ann_key, ann_value)
             VALUES (X'0101010101010101010101010101010101010101010101010101010101010101', 10, 0, 'price', 3000)",
            [],
        ).unwrap();
    }

    fn register_all(ctx: &SessionContext, conn: Arc<Mutex<Connection>>) {
        ctx.register_table(
            "entity_events",
            Arc::new(HistoricalEventsProvider::new(Arc::clone(&conn))),
        )
        .unwrap();
        ctx.register_table(
            "event_string_annotations",
            Arc::new(EventStringAnnotationsProvider::new(Arc::clone(&conn))),
        )
        .unwrap();
        ctx.register_table(
            "event_numeric_annotations",
            Arc::new(EventNumericAnnotationsProvider::new(conn)),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn events_table_no_annotation_columns() {
        let conn = setup();
        {
            let guard = conn.lock();
            insert_event_with_annotations(&guard);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn);
        let df = ctx
            .sql("SELECT * FROM entity_events WHERE block_number >= 0 AND block_number <= 100")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
        assert!(
            results[0]
                .schema()
                .field_with_name("string_annotations")
                .is_err()
        );
    }

    #[tokio::test]
    async fn join_event_annotations() {
        let conn = setup();
        {
            let guard = conn.lock();
            insert_event_with_annotations(&guard);
        }
        let ctx = SessionContext::new();
        register_all(&ctx, conn);
        let df = ctx
            .sql(
                "SELECT e.entity_key FROM entity_events e \
                 JOIN event_string_annotations s ON e.entity_key = s.entity_key AND e.block_number = s.block_number \
                 WHERE e.block_number >= 0 AND e.block_number <= 100 AND s.block_number >= 0 AND s.block_number <= 100 \
                 AND s.ann_key = 'token' AND s.ann_value = 'USDC'",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn extract_block_range_between() {
        use datafusion::prelude::*;
        let expr = col("block_number")
            .gt_eq(lit(100u64))
            .and(col("block_number").lt_eq(lit(500u64)));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, Some((100, 500)));
    }

    #[test]
    fn extract_block_range_equality() {
        use datafusion::prelude::*;
        let expr = col("block_number").eq(lit(42u64));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, Some((42, 42)));
    }

    #[test]
    fn extract_block_range_missing_upper() {
        use datafusion::prelude::*;
        let expr = col("block_number").gt_eq(lit(100u64));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, None);
    }

    fn has_block_range_filter(filters: &[Expr]) -> bool {
        filters.iter().any(|f| references_block_number(f))
    }

    #[test]
    fn has_block_range_filter_detects_column() {
        use datafusion::prelude::*;
        let expr = col("block_number").gt_eq(lit(100u64));
        assert!(has_block_range_filter(&[expr]));
    }

    #[test]
    fn has_block_range_filter_false_for_other_columns() {
        use datafusion::prelude::*;
        let expr = col("owner").eq(lit("foo"));
        assert!(!has_block_range_filter(&[expr]));
    }

    #[test]
    fn extract_block_range_inverted_returns_valid_tuple() {
        use datafusion::prelude::*;
        // lower > upper produces a valid tuple (validation happens at scan time)
        let expr = col("block_number")
            .gt_eq(lit(500u64))
            .and(col("block_number").lt_eq(lit(100u64)));
        let range = extract_block_range(&[expr]);
        assert_eq!(range, Some((500, 100)));
    }
}

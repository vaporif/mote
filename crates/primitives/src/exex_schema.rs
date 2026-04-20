use std::sync::{Arc, LazyLock};

use arrow::array::builder::MapFieldNames;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

pub use crate::columns;

pub mod ann_columns {
    pub const ANN_KEY: &str = "ann_key";
    pub const ANN_VALUE: &str = "ann_value";
}

static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_schema()));

#[must_use]
pub fn entity_events_schema() -> Arc<Schema> {
    Arc::clone(&SCHEMA)
}

#[must_use]
pub fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".into(),
        key: "key".into(),
        value: "value".into(),
    }
}

static HISTORICAL_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(build_historical_schema()));

/// Subset of [`entity_events_schema`] exposed by the historical table provider.
#[must_use]
pub fn historical_output_schema() -> Arc<Schema> {
    Arc::clone(&HISTORICAL_SCHEMA)
}

const HISTORICAL_COLUMNS: &[&str] = &[
    columns::BLOCK_NUMBER,
    columns::EVENT_TYPE,
    columns::ENTITY_KEY,
    columns::OWNER,
    columns::EXPIRES_AT_BLOCK,
    columns::CONTENT_TYPE,
    columns::PAYLOAD,
    columns::STRING_ANNOTATIONS,
    columns::NUMERIC_ANNOTATIONS,
    columns::EXTEND_POLICY,
    columns::OPERATOR,
    columns::GAS_COST,
];

fn build_historical_schema() -> Schema {
    let full = build_schema();
    let fields: Vec<_> = HISTORICAL_COLUMNS
        .iter()
        .map(|name| {
            full.field_with_name(name)
                .unwrap_or_else(|_| panic!("exex schema missing column {name}"))
                .clone()
        })
        .collect();
    Schema::new(fields)
}

fn build_schema() -> Schema {
    #[allow(clippy::wildcard_imports)]
    use columns::*;
    Schema::new(vec![
        Field::new(BLOCK_NUMBER, DataType::UInt64, false),
        Field::new(BLOCK_HASH, DataType::FixedSizeBinary(32), false),
        Field::new(TX_INDEX, DataType::UInt32, false),
        Field::new(TX_HASH, DataType::FixedSizeBinary(32), false),
        Field::new(LOG_INDEX, DataType::UInt32, false),
        Field::new(EVENT_TYPE, DataType::UInt8, false),
        Field::new(ENTITY_KEY, DataType::FixedSizeBinary(32), false),
        Field::new(OWNER, DataType::FixedSizeBinary(20), true),
        Field::new(EXPIRES_AT_BLOCK, DataType::UInt64, true),
        Field::new(OLD_EXPIRES_AT_BLOCK, DataType::UInt64, true),
        Field::new(CONTENT_TYPE, DataType::Utf8, true),
        Field::new(PAYLOAD, DataType::Binary, true),
        Field::new(
            STRING_ANNOTATIONS,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
        Field::new(
            NUMERIC_ANNOTATIONS,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::UInt64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
        Field::new(EXTEND_POLICY, DataType::UInt8, true),
        Field::new(OPERATOR, DataType::FixedSizeBinary(20), true),
        Field::new(GAS_COST, DataType::UInt64, true),
        Field::new(TIP_BLOCK, DataType::UInt64, false),
        Field::new(OP, DataType::UInt8, false),
    ])
}

/// Schema for `entities_latest` `DataFusion` table (no annotation columns).
#[must_use]
pub fn entities_latest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(columns::ENTITY_KEY, DataType::FixedSizeBinary(32), false),
        Field::new(columns::OWNER, DataType::FixedSizeBinary(20), false),
        Field::new(columns::EXPIRES_AT_BLOCK, DataType::UInt64, false),
        Field::new(columns::CONTENT_TYPE, DataType::Utf8, false),
        Field::new(columns::PAYLOAD, DataType::Binary, false),
        Field::new("created_at_block", DataType::UInt64, false),
        Field::new(columns::TX_HASH, DataType::FixedSizeBinary(32), false),
        Field::new(columns::EXTEND_POLICY, DataType::UInt8, false),
        Field::new(columns::OPERATOR, DataType::FixedSizeBinary(20), true),
    ]))
}

/// Schema for `entity_string_annotations` table.
#[must_use]
pub fn string_annotations_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(columns::ENTITY_KEY, DataType::FixedSizeBinary(32), false),
        Field::new(ann_columns::ANN_KEY, DataType::Utf8, false),
        Field::new(ann_columns::ANN_VALUE, DataType::Utf8, false),
    ]))
}

/// Schema for `entity_numeric_annotations` table.
#[must_use]
pub fn numeric_annotations_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(columns::ENTITY_KEY, DataType::FixedSizeBinary(32), false),
        Field::new(ann_columns::ANN_KEY, DataType::Utf8, false),
        Field::new(ann_columns::ANN_VALUE, DataType::UInt64, false),
    ]))
}

/// Schema for `entity_events` `DataFusion` table (no annotation columns).
#[must_use]
pub fn entity_events_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(columns::BLOCK_NUMBER, DataType::UInt64, false),
        Field::new(columns::LOG_INDEX, DataType::UInt32, false),
        Field::new(columns::EVENT_TYPE, DataType::UInt8, false),
        Field::new(columns::ENTITY_KEY, DataType::FixedSizeBinary(32), false),
        Field::new(columns::OWNER, DataType::FixedSizeBinary(20), true),
        Field::new(columns::EXPIRES_AT_BLOCK, DataType::UInt64, true),
        Field::new(columns::CONTENT_TYPE, DataType::Utf8, true),
        Field::new(columns::PAYLOAD, DataType::Binary, true),
        Field::new(columns::EXTEND_POLICY, DataType::UInt8, true),
        Field::new(columns::OPERATOR, DataType::FixedSizeBinary(20), true),
        Field::new(columns::GAS_COST, DataType::UInt64, true),
    ]))
}

/// Schema for `event_string_annotations` table.
#[must_use]
pub fn event_string_annotations_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(columns::ENTITY_KEY, DataType::FixedSizeBinary(32), false),
        Field::new(columns::BLOCK_NUMBER, DataType::UInt64, false),
        Field::new(columns::LOG_INDEX, DataType::UInt32, false),
        Field::new(ann_columns::ANN_KEY, DataType::Utf8, false),
        Field::new(ann_columns::ANN_VALUE, DataType::Utf8, false),
    ]))
}

/// Schema for `event_numeric_annotations` table.
#[must_use]
pub fn event_numeric_annotations_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(columns::ENTITY_KEY, DataType::FixedSizeBinary(32), false),
        Field::new(columns::BLOCK_NUMBER, DataType::UInt64, false),
        Field::new(columns::LOG_INDEX, DataType::UInt32, false),
        Field::new(ann_columns::ANN_KEY, DataType::Utf8, false),
        Field::new(ann_columns::ANN_VALUE, DataType::UInt64, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entities_latest_schema_has_no_annotation_columns() {
        let schema = entities_latest_schema();
        assert!(schema.field_with_name("string_annotations").is_err());
        assert!(schema.field_with_name("numeric_annotations").is_err());
        assert_eq!(schema.fields().len(), 9);
        assert!(schema.field_with_name("entity_key").is_ok());
        assert!(schema.field_with_name("operator").is_ok());
    }

    #[test]
    fn string_annotations_schema_shape() {
        let schema = string_annotations_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(
            schema.field_with_name("entity_key").unwrap().data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert_eq!(
            schema.field_with_name("ann_key").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("ann_value").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn numeric_annotations_schema_shape() {
        let schema = numeric_annotations_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(
            schema.field_with_name("ann_value").unwrap().data_type(),
            &DataType::UInt64
        );
    }

    #[test]
    fn entity_events_output_schema_has_no_annotation_columns() {
        let schema = entity_events_output_schema();
        assert!(schema.field_with_name("string_annotations").is_err());
        assert!(schema.field_with_name("numeric_annotations").is_err());
        assert!(schema.field_with_name("block_number").is_ok());
        assert!(schema.field_with_name("log_index").is_ok());
        assert!(schema.field_with_name("entity_key").is_ok());
    }

    #[test]
    fn event_string_annotations_schema_has_composite_key() {
        let schema = event_string_annotations_schema();
        assert_eq!(schema.fields().len(), 5);
        assert!(schema.field_with_name("entity_key").is_ok());
        assert!(schema.field_with_name("block_number").is_ok());
        assert!(schema.field_with_name("log_index").is_ok());
        assert!(schema.field_with_name("ann_key").is_ok());
        assert!(schema.field_with_name("ann_value").is_ok());
    }

    #[test]
    fn historical_columns_exist_in_full_schema() {
        let full = build_schema();
        for name in HISTORICAL_COLUMNS {
            assert!(
                full.field_with_name(name).is_ok(),
                "HISTORICAL_COLUMNS entry {name:?} missing from full schema"
            );
        }
    }
}

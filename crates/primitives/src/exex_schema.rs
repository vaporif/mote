use std::sync::{Arc, LazyLock};

use arrow::array::builder::MapFieldNames;
use arrow::datatypes::{DataType, Field, Schema};

pub use crate::columns;

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
        Field::new(TIP_BLOCK, DataType::UInt64, false),
        Field::new(OP, DataType::UInt8, false),
    ])
}

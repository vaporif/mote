use alloy_primitives::{Address, B256};
use arrow::{
    array::{
        Array, AsArray, BinaryArray, FixedSizeBinaryArray, MapArray, StringArray, UInt8Array,
        UInt64Array,
    },
    record_batch::RecordBatch,
};
use eyre::WrapErr;

macro_rules! col {
    ($batch:expr, $name:expr, $ty:ty) => {
        $batch
            .column_by_name($name)
            .ok_or_else(|| eyre::eyre!("missing column: {}", $name))?
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| eyre::eyre!("column {} is not {}", $name, stringify!($ty)))
    };
}

pub fn col_u8<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt8Array> {
    col!(batch, name, UInt8Array)
}

pub fn col_u64<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt64Array> {
    col!(batch, name, UInt64Array)
}

pub fn col_fsb<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a FixedSizeBinaryArray> {
    col!(batch, name, FixedSizeBinaryArray)
}

pub fn col_string<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a StringArray> {
    col!(batch, name, StringArray)
}

pub fn col_binary<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a BinaryArray> {
    col!(batch, name, BinaryArray)
}

pub fn col_map<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a MapArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| eyre::eyre!("missing column: {name}"))?
        .as_map_opt()
        .ok_or_else(|| eyre::eyre!("column {name} is not MapArray"))
}

pub fn b256_from_fsb(col: &FixedSizeBinaryArray, i: usize) -> B256 {
    B256::from_slice(col.value(i))
}

pub fn addr_from_fsb(col: &FixedSizeBinaryArray, i: usize) -> Address {
    Address::from_slice(col.value(i))
}

pub fn decode_string_map(col: &MapArray, i: usize) -> eyre::Result<Vec<(String, String)>> {
    if col.is_null(i) {
        return Ok(Vec::new());
    }

    let offsets = col.value_offsets();
    let start = usize::try_from(offsets[i]).wrap_err("negative string_annotations offset")?;
    let end = usize::try_from(offsets[i + 1]).wrap_err("negative string_annotations offset")?;

    if start == end {
        return Ok(Vec::new());
    }

    let keys = col.keys().as_string::<i32>();
    let values = col.values().as_string::<i32>();

    Ok((start..end)
        .map(|j| (keys.value(j).to_owned(), values.value(j).to_owned()))
        .collect())
}

pub fn decode_numeric_map(col: &MapArray, i: usize) -> eyre::Result<Vec<(String, u64)>> {
    if col.is_null(i) {
        return Ok(Vec::new());
    }

    let offsets = col.value_offsets();
    let start = usize::try_from(offsets[i]).wrap_err("negative numeric_annotations offset")?;
    let end = usize::try_from(offsets[i + 1]).wrap_err("negative numeric_annotations offset")?;

    if start == end {
        return Ok(Vec::new());
    }

    let keys = col.keys().as_string::<i32>();
    let values = col.values().as_primitive::<arrow::datatypes::UInt64Type>();

    Ok((start..end)
        .map(|j| (keys.value(j).to_owned(), values.value(j)))
        .collect())
}

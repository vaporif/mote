use std::{
    cmp::Reverse,
    collections::HashMap,
    path::{Path, PathBuf},
};

use arrow::{
    array::{Array, BinaryArray, FixedSizeBinaryArray, StringArray, UInt64Array, UInt8Array},
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::entity_store::{EntityRow, EntityStore, Snapshot};

const MANIFEST_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "manifest.json";

const ARROW_FILES: [&str; 3] = [
    "entities.arrow",
    "string_annotations.arrow",
    "numeric_annotations.arrow",
];

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    block_number: u64,
    entity_count: usize,
    files: HashMap<String, String>,
}

fn blake3_hex(path: &Path) -> Result<String> {
    let data = std::fs::read(path)?;
    Ok(blake3::hash(&data).to_hex().to_string())
}

fn write_manifest(dir: &Path, block: u64, entity_count: usize) -> Result<()> {
    let mut files = HashMap::new();
    for name in &ARROW_FILES {
        files.insert((*name).to_owned(), blake3_hex(&dir.join(name))?);
    }
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        block_number: block,
        entity_count,
        files,
    };
    let json = serde_json::to_string_pretty(&manifest)?;
    std::fs::write(dir.join(MANIFEST_FILE), json)?;
    Ok(())
}

fn verify_manifest(dir: &Path) -> Result<Manifest> {
    let manifest_path = dir.join(MANIFEST_FILE);
    let data = std::fs::read_to_string(&manifest_path)
        .map_err(|e| eyre::eyre!("missing manifest: {e}"))?;
    let manifest: Manifest =
        serde_json::from_str(&data).map_err(|e| eyre::eyre!("invalid manifest: {e}"))?;

    eyre::ensure!(
        manifest.version == MANIFEST_VERSION,
        "unsupported manifest version {} (expected {MANIFEST_VERSION})",
        manifest.version,
    );

    for name in &ARROW_FILES {
        let expected = manifest
            .files
            .get(*name)
            .ok_or_else(|| eyre::eyre!("manifest missing entry for {name}"))?;
        let actual = blake3_hex(&dir.join(name))?;
        eyre::ensure!(
            *expected == actual,
            "checksum mismatch for {name}: expected {expected}, got {actual}",
        );
    }

    Ok(manifest)
}

fn snapshot_dir_name(block: u64) -> String {
    format!("block-{block:08}")
}

/// Parse block number from a snapshot directory name like `block-00001000`.
fn parse_block_from_dir(name: &str) -> Option<u64> {
    name.strip_prefix("block-")?.parse().ok()
}

fn write_ipc_file(path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = FileWriter::try_new(file, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}

fn read_ipc_file(path: &Path) -> Result<RecordBatch> {
    let file = std::fs::File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    eyre::ensure!(
        batches.len() <= 1,
        "expected at most 1 batch in {}, got {}",
        path.display(),
        batches.len()
    );
    Ok(batches
        .into_iter()
        .next()
        .unwrap_or_else(|| RecordBatch::new_empty(schema)))
}

/// Write a snapshot (3 Arrow IPC files) for the given block.
///
/// Uses a temp directory + atomic rename so a crash mid-write never leaves a
/// partially-written snapshot visible to readers.
pub fn write_snapshot(snapshots_dir: &Path, block: u64, snapshot: &Snapshot) -> Result<()> {
    std::fs::create_dir_all(snapshots_dir)?;

    let final_dir = snapshots_dir.join(snapshot_dir_name(block));
    let tmp_dir = snapshots_dir.join(format!(".tmp-block-{block:08}"));

    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir)?;
    }

    std::fs::create_dir_all(&tmp_dir)?;

    write_ipc_file(&tmp_dir.join("entities.arrow"), &snapshot.entities)?;
    write_ipc_file(
        &tmp_dir.join("string_annotations.arrow"),
        &snapshot.string_annotations,
    )?;
    write_ipc_file(
        &tmp_dir.join("numeric_annotations.arrow"),
        &snapshot.numeric_annotations,
    )?;

    write_manifest(&tmp_dir, block, snapshot.entities.num_rows())?;

    std::fs::rename(&tmp_dir, &final_dir)?;

    Ok(())
}

/// Scan the snapshots directory and return the highest-numbered snapshot.
///
/// Returns `None` if the directory does not exist or contains no valid
/// snapshot subdirs. Corrupted snapshots are removed and skipped.
pub fn load_latest_snapshot(snapshots_dir: &Path) -> Result<Option<(u64, EntityStore)>> {
    if !snapshots_dir.exists() {
        return Ok(None);
    }

    cleanup_temp_dirs(snapshots_dir);

    let mut best: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(snapshots_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(block) = parse_block_from_dir(&name.to_string_lossy()) else {
            continue;
        };
        if best.as_ref().is_none_or(|(b, _)| block > *b) {
            best = Some((block, entry.path()));
        }
    }

    let Some((block, dir)) = best else {
        return Ok(None);
    };

    match load_snapshot_from_dir(&dir) {
        Ok(store) => Ok(Some((block, store))),
        Err(e) => {
            tracing::warn!(?e, dir = %dir.display(), "corrupted snapshot, removing");
            let _ = std::fs::remove_dir_all(&dir);
            Ok(None)
        }
    }
}

fn cleanup_temp_dirs(snapshots_dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(snapshots_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".tmp-") {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
}

fn col<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| eyre::eyre!("snapshot missing column `{name}`"))?;
    col.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| eyre::eyre!("snapshot column `{name}` has unexpected type"))
}

fn load_snapshot_from_dir(dir: &Path) -> Result<EntityStore> {
    use alloy_primitives::{Address, Bytes, B256};

    verify_manifest(dir)?;

    let entities_batch = read_ipc_file(&dir.join("entities.arrow"))?;
    let str_ann_batch = read_ipc_file(&dir.join("string_annotations.arrow"))?;
    let num_ann_batch = read_ipc_file(&dir.join("numeric_annotations.arrow"))?;

    let nrows = entities_batch.num_rows();

    let entity_key_col = col::<FixedSizeBinaryArray>(&entities_batch, "entity_key")?;
    let owner_col = col::<FixedSizeBinaryArray>(&entities_batch, "owner")?;
    let expires_col = col::<UInt64Array>(&entities_batch, "expires_at_block")?;
    let content_type_col = col::<StringArray>(&entities_batch, "content_type")?;
    let payload_col = col::<BinaryArray>(&entities_batch, "payload")?;
    let created_col = col::<UInt64Array>(&entities_batch, "created_at_block")?;
    let tx_hash_col = col::<FixedSizeBinaryArray>(&entities_batch, "tx_hash")?;
    let extend_policy_col = col::<UInt8Array>(&entities_batch, "extend_policy")?;
    let operator_col = col::<FixedSizeBinaryArray>(&entities_batch, "operator")?;

    let str_ann_ek = col::<FixedSizeBinaryArray>(&str_ann_batch, "entity_key")?;
    let str_ann_key = col::<StringArray>(&str_ann_batch, "ann_key")?;
    let str_ann_val = col::<StringArray>(&str_ann_batch, "ann_value")?;

    let num_ann_ek = col::<FixedSizeBinaryArray>(&num_ann_batch, "entity_key")?;
    let num_ann_key = col::<StringArray>(&num_ann_batch, "ann_key")?;
    let num_ann_val = col::<UInt64Array>(&num_ann_batch, "ann_value")?;

    // Group annotations by entity_key so we can attach them when building rows.
    let mut str_anns: HashMap<B256, Vec<(String, String)>> = HashMap::new();
    for i in 0..str_ann_batch.num_rows() {
        let ek = B256::from_slice(str_ann_ek.value(i));
        str_anns.entry(ek).or_default().push((
            str_ann_key.value(i).to_owned(),
            str_ann_val.value(i).to_owned(),
        ));
    }

    let mut num_anns: HashMap<B256, Vec<(String, u64)>> = HashMap::new();
    for i in 0..num_ann_batch.num_rows() {
        let ek = B256::from_slice(num_ann_ek.value(i));
        num_anns
            .entry(ek)
            .or_default()
            .push((num_ann_key.value(i).to_owned(), num_ann_val.value(i)));
    }

    let mut store = EntityStore::new();
    for i in 0..nrows {
        let entity_key = B256::from_slice(entity_key_col.value(i));
        let owner = Address::from_slice(owner_col.value(i));
        let operator =
            (!operator_col.is_null(i)).then(|| Address::from_slice(operator_col.value(i)));

        let row = EntityRow {
            entity_key,
            owner,
            expires_at_block: expires_col.value(i),
            content_type: content_type_col.value(i).to_owned(),
            payload: Bytes::copy_from_slice(payload_col.value(i)),
            string_annotations: str_anns.remove(&entity_key).unwrap_or_default(),
            numeric_annotations: num_anns.remove(&entity_key).unwrap_or_default(),
            created_at_block: created_col.value(i),
            tx_hash: B256::from_slice(tx_hash_col.value(i)),
            extend_policy: extend_policy_col.value(i),
            operator,
        };
        store.insert(row);
    }

    Ok(store)
}

pub fn prune_snapshots(snapshots_dir: &Path, keep: usize) -> Result<()> {
    if !snapshots_dir.exists() {
        return Ok(());
    }

    let mut dirs: Vec<(u64, PathBuf)> = std::fs::read_dir(snapshots_dir)?
        .flatten()
        .filter_map(|entry| {
            let block = parse_block_from_dir(&entry.file_name().to_string_lossy())?;
            Some((block, entry.path()))
        })
        .collect();

    dirs.sort_by_key(|(block, _)| Reverse(*block));

    for (_, path) in dirs.into_iter().skip(keep) {
        tracing::info!(path = %path.display(), "pruning old snapshot");
        let _ = std::fs::remove_dir_all(&path);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use alloy_primitives::{Address, Bytes, B256};

    use super::*;
    use crate::entity_store::EntityRow;

    fn sample_row(byte: u8) -> EntityRow {
        EntityRow {
            entity_key: B256::repeat_byte(byte),
            owner: Address::repeat_byte(byte),
            expires_at_block: 100,
            content_type: "text/plain".into(),
            payload: Bytes::from_static(b"hello"),
            string_annotations: vec![("color".into(), "red".into())],
            numeric_annotations: vec![("size".into(), 42)],
            created_at_block: 1,
            tx_hash: B256::repeat_byte(0xAA),
            extend_policy: 0,
            operator: Some(Address::ZERO),
        }
    }

    #[test]
    fn write_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();

        let mut store = EntityStore::new();
        store.set_current_block(50);
        store.insert(sample_row(0x01));
        store.insert(sample_row(0x02));
        let snap = store.snapshot().unwrap();

        write_snapshot(dir.path(), 1000, &snap).unwrap();

        let result = load_latest_snapshot(dir.path()).unwrap();
        assert!(result.is_some());
        let (block, loaded_store) = result.unwrap();
        assert_eq!(block, 1000);
        assert_eq!(loaded_store.len(), 2);
        let row = loaded_store.get(&B256::repeat_byte(0x01)).unwrap();
        assert_eq!(row.owner, Address::repeat_byte(0x01));
        assert_eq!(row.content_type, "text/plain");
        assert_eq!(row.string_annotations, vec![("color".into(), "red".into())]);
        assert_eq!(row.numeric_annotations, vec![("size".into(), 42)]);
        assert_eq!(row.extend_policy, 0);
        assert_eq!(row.operator, Some(Address::ZERO));
    }

    #[test]
    fn prune_keeps_n_most_recent() {
        let dir = tempfile::tempdir().unwrap();

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let snap = store.snapshot().unwrap();

        write_snapshot(dir.path(), 1000, &snap).unwrap();
        write_snapshot(dir.path(), 2000, &snap).unwrap();
        write_snapshot(dir.path(), 3000, &snap).unwrap();

        prune_snapshots(dir.path(), 2).unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
            .collect();
        assert_eq!(entries.len(), 2);
        assert!(!dir.path().join("block-00001000").exists());
        assert!(dir.path().join("block-00002000").exists());
        assert!(dir.path().join("block-00003000").exists());
    }

    #[test]
    fn load_from_empty_dir_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let result = load_latest_snapshot(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn load_from_nonexistent_dir_returns_none() {
        let result = load_latest_snapshot(Path::new("/tmp/does-not-exist-glint-test")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn null_operator_round_trips() {
        let dir = tempfile::tempdir().unwrap();

        let mut store = EntityStore::new();
        let mut row = sample_row(0x01);
        row.operator = None;
        store.insert(row);
        let snap = store.snapshot().unwrap();

        write_snapshot(dir.path(), 500, &snap).unwrap();
        let (_, loaded) = load_latest_snapshot(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.get(&B256::repeat_byte(0x01)).unwrap().operator, None);
    }

    #[test]
    fn empty_store_round_trips() {
        let dir = tempfile::tempdir().unwrap();

        let store = EntityStore::new();
        let snap = store.snapshot().unwrap();

        write_snapshot(dir.path(), 100, &snap).unwrap();
        let (block, loaded) = load_latest_snapshot(dir.path()).unwrap().unwrap();
        assert_eq!(block, 100);
        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn corrupted_arrow_file_is_detected_and_removed() {
        let dir = tempfile::tempdir().unwrap();

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let snap = store.snapshot().unwrap();

        write_snapshot(dir.path(), 1000, &snap).unwrap();

        let entities_path = dir.path().join("block-00001000/entities.arrow");
        std::fs::write(&entities_path, b"corrupted data").unwrap();

        let result = load_latest_snapshot(dir.path()).unwrap();
        assert!(result.is_none());
        assert!(!dir.path().join("block-00001000").exists());
    }

    #[test]
    fn missing_manifest_is_treated_as_corrupted() {
        let dir = tempfile::tempdir().unwrap();

        let mut store = EntityStore::new();
        store.insert(sample_row(0x01));
        let snap = store.snapshot().unwrap();

        write_snapshot(dir.path(), 1000, &snap).unwrap();

        std::fs::remove_file(dir.path().join("block-00001000/manifest.json")).unwrap();

        let result = load_latest_snapshot(dir.path()).unwrap();
        assert!(result.is_none());
        assert!(!dir.path().join("block-00001000").exists());
    }
}

use alloy_network::EthereumWallet;
use alloy_signer_local::PrivateKeySigner;
use arrow::array::RecordBatch;
use rstest::rstest;

use glint_e2e::Transport;
use glint_e2e::eth_node_handle::EthNodeHandle;
use glint_e2e::sidecar_handle::SidecarHandle;
use glint_primitives::transaction::{Create, GlintTransaction};
use glint_sdk::Glint;

fn col<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column: {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} has wrong type"))
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

const DEV_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn dev_wallet() -> EthereumWallet {
    let signer: PrivateKeySigner = DEV_KEY.parse().unwrap();
    EthereumWallet::from(signer)
}

#[tokio::test]
#[ignore = "requires eth-glint Docker image; run with `just e2e`"]
async fn test_create_entity() -> eyre::Result<()> {
    let node = EthNodeHandle::spawn(Transport::Ipc).await?;

    let wallet = dev_wallet();
    let expected_owner = wallet.default_signer().address();
    let client = Glint::connect_with_wallet(node.rpc_url(), wallet)?;

    let payload = b"hello glint";
    let btl: u64 = 100;
    let tx = GlintTransaction::new()
        .create(Create::new("text/plain", payload, btl).string_annotation("app", "e2e-test"));

    let result = client.send(&tx).await?;
    assert!(result.receipt.status(), "glint tx should succeed");

    let block_number = result
        .block_number()
        .expect("receipt should have block number");

    let entity_key = result.created_entity_keys[0];

    let entity = client
        .get_entity(entity_key)
        .await?
        .expect("entity should exist after create tx");

    assert_eq!(entity.owner, expected_owner);
    assert_eq!(entity.expires_at_block, block_number + btl);

    let count = client.get_entity_count().await?;
    assert_eq!(count, 1);

    let slots = client.get_used_slots().await?;
    assert!(slots >= 2, "expected at least 2 used slots, got {slots}");

    let timing = client.get_block_timing().await?;
    assert!(
        timing.block_number >= block_number,
        "block timing should be at or after entity block"
    );
    assert!(timing.timestamp > 0, "timestamp should be nonzero");

    Ok(())
}

#[rstest]
#[case::ipc(Transport::Ipc)]
#[case::grpc(Transport::Grpc)]
#[tokio::test]
#[ignore = "requires eth-glint + glint-sidecar Docker images; run with `just e2e`"]
async fn test_flight_sql_query(#[case] transport: Transport) -> eyre::Result<()> {
    let node = EthNodeHandle::spawn(transport).await?;
    let sidecar = SidecarHandle::spawn(&node, transport).await?;

    let client = Glint::builder(node.rpc_url())
        .wallet(dev_wallet())
        .flight_url(sidecar.flight_url())
        .build()
        .await?;

    let tx = GlintTransaction::new()
        .create(Create::new("text/plain", b"flight-sql-test", 100).string_annotation("app", "e2e"));

    let result = client.send(&tx).await?;
    assert!(result.receipt.status(), "glint tx should succeed");

    wait_for_sidecar_ready(&sidecar).await?;

    let batches = client
        .query("SELECT entity_key, content_type FROM entities_latest")
        .await?;

    assert!(
        row_count(&batches) >= 1,
        "expected at least 1 entity row from Flight SQL"
    );

    Ok(())
}

#[rstest]
#[case::ipc(Transport::Ipc)]
#[case::grpc(Transport::Grpc)]
#[tokio::test]
#[ignore = "requires eth-glint + glint-sidecar Docker images; run with `just e2e`"]
async fn test_flight_sql_complex_query(#[case] transport: Transport) -> eyre::Result<()> {
    let node = EthNodeHandle::spawn(transport).await?;
    let sidecar = SidecarHandle::spawn(&node, transport).await?;

    let wallet = dev_wallet();
    let expected_owner = wallet.default_signer().address();

    let client = Glint::builder(node.rpc_url())
        .wallet(wallet)
        .flight_url(sidecar.flight_url())
        .build()
        .await?;

    let payload = b"complex-query-test";
    let btl: u64 = 200;
    let tx = GlintTransaction::new().create(
        Create::new("application/json", payload, btl)
            .string_annotation("app", "e2e-complex")
            .string_annotation("env", "test")
            .numeric_annotation("priority", 42)
            .numeric_annotation("version", 1),
    );

    let result = client.send(&tx).await?;
    assert!(result.receipt.status(), "glint tx should succeed");

    let entity_key = result.created_entity_keys[0];

    wait_for_sidecar_ready(&sidecar).await?;

    let owner_hex = format!("{:x}", expected_owner);

    let sql = format!(
        "SELECT e.entity_key, e.owner, e.expires_at_block, e.content_type, e.payload, \
                sa.ann_value AS app_ann, se.ann_value AS env_ann \
         FROM entities_latest e \
         JOIN entity_string_annotations sa ON e.entity_key = sa.entity_key AND sa.ann_key = 'app' \
         JOIN entity_string_annotations se ON e.entity_key = se.entity_key AND se.ann_key = 'env' \
         WHERE e.owner = x'{owner_hex}'"
    );

    let batches = client.query(&sql).await?;
    assert_eq!(row_count(&batches), 1, "expected exactly 1 entity row");

    let batch = &batches[0];

    let ek_col: &arrow::array::FixedSizeBinaryArray = col(batch, "entity_key");
    assert_eq!(ek_col.value(0), entity_key.as_slice());

    let owner_col: &arrow::array::FixedSizeBinaryArray = col(batch, "owner");
    assert_eq!(owner_col.value(0), expected_owner.as_slice());

    let ct_col: &arrow::array::StringArray = col(batch, "content_type");
    assert_eq!(ct_col.value(0), "application/json");

    let payload_col: &arrow::array::BinaryArray = col(batch, "payload");
    assert_eq!(payload_col.value(0), payload);

    let app_col: &arrow::array::StringArray = col(batch, "app_ann");
    assert_eq!(app_col.value(0), "e2e-complex");

    let env_col: &arrow::array::StringArray = col(batch, "env_ann");
    assert_eq!(env_col.value(0), "test");

    // numeric annotations via JOIN
    let sql = format!(
        "SELECT na.ann_key, na.ann_value \
         FROM entities_latest e \
         JOIN entity_numeric_annotations na USING (entity_key) \
         WHERE e.owner = x'{owner_hex}' \
         ORDER BY na.ann_key"
    );
    let batches = client.query(&sql).await?;
    assert_eq!(row_count(&batches), 2, "expected 2 numeric annotations");

    // check expiration
    let sql = format!("SELECT expires_at_block FROM entities_latest WHERE owner = x'{owner_hex}'");
    let batches = client.query(&sql).await?;
    let expires_col: &arrow::array::UInt64Array = col(&batches[0], "expires_at_block");
    let block_number = result
        .block_number()
        .expect("receipt should have block number");
    assert_eq!(expires_col.value(0), block_number + btl);

    Ok(())
}

#[rstest]
#[case::ipc(Transport::Ipc)]
#[case::grpc(Transport::Grpc)]
#[tokio::test]
#[ignore = "requires eth-glint + glint-sidecar Docker images; run with `just e2e`"]
async fn test_flight_sql_multi_entity_filters(#[case] transport: Transport) -> eyre::Result<()> {
    let node = EthNodeHandle::spawn(transport).await?;
    let sidecar = SidecarHandle::spawn(&node, transport).await?;

    let wallet = dev_wallet();
    let expected_owner = wallet.default_signer().address();

    let client = Glint::builder(node.rpc_url())
        .wallet(wallet)
        .flight_url(sidecar.flight_url())
        .build()
        .await?;

    // 3 entities with varying annotations
    let payloads: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let mut entity_keys = Vec::new();

    for (i, payload) in payloads.iter().enumerate() {
        let category = if i < 2 { "group-a" } else { "group-b" };
        let tx = GlintTransaction::new().create(
            Create::new("text/plain", *payload, 200)
                .string_annotation("category", category)
                .numeric_annotation("rank", (i + 1) as u64),
        );
        let result = client.send(&tx).await?;
        assert!(result.receipt.status(), "tx {i} should succeed");
        entity_keys.push(result.created_entity_keys[0]);
    }

    wait_for_sidecar_ready(&sidecar).await?;

    // string filter: group-a -> alpha, beta
    let batches = client
        .query(
            "SELECT e.entity_key FROM entities_latest e \
             JOIN entity_string_annotations s USING (entity_key) \
             WHERE s.ann_key = 'category' AND s.ann_value = 'group-a'",
        )
        .await?;
    assert_eq!(row_count(&batches), 2, "expected 2 entities in group-a");

    // range filter: rank >= 2 -> beta, gamma
    let batches = client
        .query(
            "SELECT e.entity_key FROM entities_latest e \
             JOIN entity_numeric_annotations n USING (entity_key) \
             WHERE n.ann_key = 'rank' AND n.ann_value >= 2",
        )
        .await?;
    assert_eq!(row_count(&batches), 2, "expected 2 entities with rank >= 2");

    // exact filter: rank = 1 -> alpha
    let batches = client
        .query(
            "SELECT e.entity_key FROM entities_latest e \
             JOIN entity_numeric_annotations n USING (entity_key) \
             WHERE n.ann_key = 'rank' AND n.ann_value = 1",
        )
        .await?;
    assert_eq!(row_count(&batches), 1, "expected 1 entity with rank = 1");

    let ek_col: &arrow::array::FixedSizeBinaryArray = col(&batches[0], "entity_key");
    assert_eq!(ek_col.value(0), entity_keys[0].as_slice());

    // combined: group-a AND rank=2 -> beta
    let batches = client
        .query(
            "SELECT e.entity_key FROM entities_latest e \
             JOIN entity_string_annotations s USING (entity_key) \
             JOIN entity_numeric_annotations n USING (entity_key) \
             WHERE s.ann_key = 'category' AND s.ann_value = 'group-a' \
               AND n.ann_key = 'rank' AND n.ann_value = 2",
        )
        .await?;
    assert_eq!(
        row_count(&batches),
        1,
        "expected 1 entity matching group-a + rank=2"
    );

    let ek_col: &arrow::array::FixedSizeBinaryArray = col(&batches[0], "entity_key");
    assert_eq!(ek_col.value(0), entity_keys[1].as_slice());

    // owner filter -> all 3
    let owner_hex = format!("{:x}", expected_owner);
    let batches = client
        .query(&format!(
            "SELECT entity_key FROM entities_latest WHERE owner = x'{owner_hex}'"
        ))
        .await?;
    assert_eq!(
        row_count(&batches),
        3,
        "expected 3 entities owned by dev wallet"
    );

    // IN filter: group-b or group-c
    let batches = client
        .query(
            "SELECT e.entity_key FROM entities_latest e \
             JOIN entity_string_annotations s USING (entity_key) \
             WHERE s.ann_key = 'category' AND s.ann_value IN ('group-b', 'group-c')",
        )
        .await?;
    assert_eq!(
        row_count(&batches),
        1,
        "expected 1 entity in group-b/group-c"
    );

    Ok(())
}

#[rstest]
#[case::ipc(Transport::Ipc)]
#[case::grpc(Transport::Grpc)]
#[tokio::test]
#[ignore = "requires eth-glint + glint-sidecar Docker images; run with `just e2e`"]
async fn test_historical_query(#[case] transport: Transport) -> eyre::Result<()> {
    let node = EthNodeHandle::spawn(transport).await?;
    let sidecar = SidecarHandle::spawn(&node, transport).await?;

    let client = Glint::builder(node.rpc_url())
        .wallet(dev_wallet())
        .flight_url(sidecar.flight_url())
        .build()
        .await?;

    let tx1 = GlintTransaction::new().create(
        Create::new("text/plain", b"entity-one", 200).string_annotation("app", "hist-test"),
    );
    let result1 = client.send(&tx1).await?;
    assert!(result1.receipt.status());
    let block1 = result1.block_number().expect("should have block number");

    let tx2 = GlintTransaction::new().create(
        Create::new("text/plain", b"entity-two", 200).string_annotation("app", "hist-test"),
    );
    let result2 = client.send(&tx2).await?;
    assert!(result2.receipt.status());
    let block2 = result2.block_number().expect("should have block number");

    wait_for_sidecar_ready(&sidecar).await?;

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let sql = format!(
        "SELECT block_number, event_type FROM entity_events WHERE block_number BETWEEN {block1} AND {block2}"
    );
    let batches = client.query(&sql).await?;
    let total: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert!(
        total >= 2,
        "expected at least 2 historical events, got {total}"
    );

    let sql = format!(
        "SELECT block_number FROM entity_events WHERE block_number BETWEEN {block1} AND {block1}"
    );
    let batches = client.query(&sql).await?;
    let total: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert!(
        total >= 1,
        "expected at least 1 event in block {block1}, got {total}"
    );

    Ok(())
}

async fn wait_for_sidecar_ready(sidecar: &SidecarHandle) -> eyre::Result<()> {
    let ready_url = format!("{}/ready", sidecar.health_url());
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);

    while tokio::time::Instant::now() < deadline {
        if client
            .get(&ready_url)
            .send()
            .await
            .ok()
            .is_some_and(|r| r.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    eyre::bail!("sidecar /ready not 200 within 60s")
}

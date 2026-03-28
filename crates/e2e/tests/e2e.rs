use alloy_network::EthereumWallet;
use alloy_signer_local::PrivateKeySigner;

use glint_e2e::eth_node_handle::EthNodeHandle;
use glint_e2e::sidecar_handle::SidecarHandle;
use glint_primitives::entity::derive_entity_key;
use glint_primitives::transaction::{Create, GlintTransaction};
use glint_sdk::Glint;

const DEV_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn dev_wallet() -> EthereumWallet {
    let signer: PrivateKeySigner = DEV_KEY.parse().unwrap();
    EthereumWallet::from(signer)
}

#[tokio::test]
#[ignore = "requires eth-glint Docker image; run with `just e2e`"]
async fn test_create_entity() -> eyre::Result<()> {
    let node = EthNodeHandle::spawn().await?;

    let wallet = dev_wallet();
    let expected_owner = wallet.default_signer().address();
    let client = Glint::connect_with_wallet(node.rpc_url(), wallet)?;

    let payload = b"hello glint";
    let btl: u64 = 100;
    let tx = GlintTransaction::new()
        .create(Create::new("text/plain", payload, btl).string_annotation("app", "e2e-test"));

    let receipt = client.send(&tx).await?;
    assert!(receipt.status(), "glint tx should succeed");

    let tx_hash = receipt.transaction_hash;
    let block_number = receipt
        .block_number
        .expect("receipt should have block number");

    let entity_key = derive_entity_key(&tx_hash, payload, 0);

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

#[tokio::test]
#[ignore = "requires eth-glint + glint-db-sidecar Docker images; run with `just e2e`"]
async fn test_flight_sql_query() -> eyre::Result<()> {
    let node = EthNodeHandle::spawn().await?;
    let sidecar = SidecarHandle::spawn(node.exex_volume_path()).await?;

    let client = Glint::builder(node.rpc_url())
        .wallet(dev_wallet())
        .flight_url(sidecar.flight_url())
        .build()
        .await?;

    let tx = GlintTransaction::new()
        .create(Create::new("text/plain", b"flight-sql-test", 100).string_annotation("app", "e2e"));

    let receipt = client.send(&tx).await?;
    assert!(receipt.status(), "glint tx should succeed");

    wait_for_sidecar_ready(&sidecar).await?;

    let batches = client
        .query("SELECT entity_key, content_type FROM entities")
        .await?;

    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert!(
        total_rows >= 1,
        "expected at least 1 entity row from Flight SQL, got {total_rows}"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires eth-glint + glint-db-sidecar Docker images; run with `just e2e`"]
async fn test_historical_query() -> eyre::Result<()> {
    let node = EthNodeHandle::spawn().await?;
    let sidecar = SidecarHandle::spawn(node.exex_volume_path()).await?;

    let client = Glint::builder(node.rpc_url())
        .wallet(dev_wallet())
        .flight_url(sidecar.flight_url())
        .build()
        .await?;

    let tx1 = GlintTransaction::new().create(
        Create::new("text/plain", b"entity-one", 200).string_annotation("app", "hist-test"),
    );
    let receipt1 = client.send(&tx1).await?;
    assert!(receipt1.status());
    let block1 = receipt1.block_number.expect("should have block number");

    let tx2 = GlintTransaction::new().create(
        Create::new("text/plain", b"entity-two", 200).string_annotation("app", "hist-test"),
    );
    let receipt2 = client.send(&tx2).await?;
    assert!(receipt2.status());
    let block2 = receipt2.block_number.expect("should have block number");

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

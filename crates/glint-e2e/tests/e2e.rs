use alloy_network::EthereumWallet;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;

use glint_e2e::analytics_handle::AnalyticsHandle;
use glint_e2e::eth_node_handle::EthNodeHandle;
use glint_primitives::entity::derive_entity_key;
use glint_sdk::flight_sql::GlintFlightClient;
use glint_sdk::{CreateEntity, GlintClient};

#[tokio::test]
#[ignore = "requires built eth-glint binary; run with `just e2e`"]
async fn test_create_entity() -> eyre::Result<()> {
    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

    let client = make_glint_client(&node);

    let payload = b"hello glint";
    let btl: u64 = 100;
    let create = CreateEntity::new("text/plain", payload, btl).string_annotation("app", "e2e-test");

    let receipt = client
        .send_glint_transaction(&[create], &[], &[], &[], &[])
        .await?;
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

    assert_eq!(entity.owner, dev_address());
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
#[ignore = "requires built eth-glint + glint-analytics binaries; run with `just e2e`"]
async fn test_flight_sql_query() -> eyre::Result<()> {
    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

    let exex_socket = node.exex_socket().to_path_buf();
    let analytics = tokio::task::spawn_blocking(move || AnalyticsHandle::spawn(&exex_socket))
        .await
        .expect("spawn_blocking panicked")?;

    let client = make_glint_client(&node);

    let create =
        CreateEntity::new("text/plain", b"flight-sql-test", 100).string_annotation("app", "e2e");

    let receipt = client
        .send_glint_transaction(&[create], &[], &[], &[], &[])
        .await?;
    assert!(receipt.status(), "glint tx should succeed");

    wait_for_analytics_ready(&analytics).await?;

    let mut flight = GlintFlightClient::connect(analytics.flight_url()).await?;

    let batches = flight
        .query("SELECT entity_key, content_type FROM entities")
        .await?;

    let total_rows: usize = batches.iter().map(arrow::array::RecordBatch::num_rows).sum();
    assert!(
        total_rows >= 1,
        "expected at least 1 entity row from Flight SQL, got {total_rows}"
    );

    Ok(())
}

const DEV_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn dev_address() -> alloy_primitives::Address {
    DEV_KEY.parse::<PrivateKeySigner>().unwrap().address()
}

fn make_glint_client(node: &EthNodeHandle) -> GlintClient {
    let signer: PrivateKeySigner = DEV_KEY.parse().unwrap();
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(node.rpc_url().parse().unwrap());
    GlintClient::new(provider)
}

async fn wait_for_analytics_ready(analytics: &AnalyticsHandle) -> eyre::Result<()> {
    let ready_url = format!("{}/ready", analytics.health_url());
    tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let client = reqwest::blocking::Client::new();
        while std::time::Instant::now() < deadline {
            if client
                .get(&ready_url)
                .send()
                .ok()
                .is_some_and(|r| r.status().is_success())
            {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        eyre::bail!("analytics /ready not 200 within 60s")
    })
    .await
    .expect("spawn_blocking panicked")
}

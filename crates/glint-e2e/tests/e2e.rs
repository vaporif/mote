use alloy_network::EthereumWallet;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;

use glint_e2e::analytics_handle::AnalyticsHandle;
use glint_e2e::eth_node_handle::EthNodeHandle;
use glint_primitives::entity::derive_entity_key;
use glint_sdk::{CreateEntity, GlintClient};

#[tokio::test]
#[ignore = "requires built eth-glint binary; run with `just e2e`"]
async fn test_create_entity() -> eyre::Result<()> {
    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

    // Standard dev account (same as Hardhat/Anvil account 0, prefunded in --dev mode)
    let signer: PrivateKeySigner =
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".parse()?;
    let expected_owner = signer.address();
    let wallet = EthereumWallet::from(signer);

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(node.rpc_url().parse()?);

    let client = GlintClient::new(provider);

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

    assert_eq!(entity.owner, expected_owner);
    assert_eq!(entity.expires_at_block, block_number + btl);

    let count = client.get_entity_count().await?;
    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test]
#[ignore = "requires built eth-glint + glint-analytics binaries; run with `just e2e`"]
async fn test_flight_sql_query() -> eyre::Result<()> {
    use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
    use arrow_flight::{FlightDescriptor, flight_service_client::FlightServiceClient};
    use futures::TryStreamExt;
    use prost::Message;

    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

    let exex_socket = node.exex_socket().to_path_buf();
    let analytics = tokio::task::spawn_blocking(move || AnalyticsHandle::spawn(&exex_socket))
        .await
        .expect("spawn_blocking panicked")?;

    let signer: alloy_signer_local::PrivateKeySigner =
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".parse()?;
    let wallet = EthereumWallet::from(signer);

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(node.rpc_url().parse()?);

    let glint = GlintClient::new(provider);

    let create =
        CreateEntity::new("text/plain", b"flight-sql-test", 100).string_annotation("app", "e2e");

    let receipt = glint
        .send_glint_transaction(&[create], &[], &[], &[], &[])
        .await?;
    assert!(receipt.status(), "glint tx should succeed");

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
    .expect("spawn_blocking panicked")?;

    let channel = tonic::transport::Channel::from_shared(analytics.flight_url())?
        .connect()
        .await?;
    let mut flight_client = FlightServiceClient::new(channel);

    let query = CommandStatementQuery {
        query: "SELECT entity_key, content_type FROM entities".to_string(),
        ..Default::default()
    };
    let descriptor = FlightDescriptor::new_cmd(query.as_any().encode_to_vec());
    let flight_info = flight_client
        .get_flight_info(descriptor)
        .await?
        .into_inner();

    assert!(
        !flight_info.endpoint.is_empty(),
        "flight info should have endpoints"
    );

    let ticket = flight_info.endpoint[0]
        .ticket
        .as_ref()
        .expect("endpoint should have ticket")
        .clone();

    let stream = flight_client.do_get(ticket).await?.into_inner();
    let flight_data: Vec<_> = stream.try_collect().await?;
    let batches = arrow_flight::utils::flight_data_to_batches(&flight_data)?;

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

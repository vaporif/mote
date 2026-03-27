use alloy_network::EthereumWallet;
use alloy_signer_local::PrivateKeySigner;
use arrow::array::RecordBatch;

use glint_e2e::analytics_handle::AnalyticsHandle;
use glint_e2e::eth_node_handle::EthNodeHandle;
use glint_primitives::entity::derive_entity_key;
use glint_primitives::transaction::{Create, GlintTransaction};
use glint_sdk::Glint;

fn col<T: 'static>(batch: &RecordBatch, name: &str) -> &T {
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
#[ignore = "requires built eth-glint binary; run with `just e2e`"]
async fn test_create_entity() -> eyre::Result<()> {
    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

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
#[ignore = "requires built eth-glint + glint-analytics binaries; run with `just e2e`"]
async fn test_flight_sql_query() -> eyre::Result<()> {
    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

    let exex_socket = node.exex_socket().to_path_buf();
    let analytics = tokio::task::spawn_blocking(move || AnalyticsHandle::spawn(&exex_socket))
        .await
        .expect("spawn_blocking panicked")?;

    let client = Glint::builder(node.rpc_url())
        .wallet(dev_wallet())
        .flight_url(&analytics.flight_url())
        .build()
        .await?;

    let tx = GlintTransaction::new()
        .create(Create::new("text/plain", b"flight-sql-test", 100).string_annotation("app", "e2e"));

    let receipt = client.send(&tx).await?;
    assert!(receipt.status(), "glint tx should succeed");

    wait_for_analytics_ready(&analytics).await?;

    let batches = client
        .query("SELECT entity_key, content_type FROM entities")
        .await?;

    assert!(
        row_count(&batches) >= 1,
        "expected at least 1 entity row from Flight SQL"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires built eth-glint + glint-analytics binaries; run with `just e2e`"]
async fn test_flight_sql_complex_query() -> eyre::Result<()> {
    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

    let exex_socket = node.exex_socket().to_path_buf();
    let analytics = tokio::task::spawn_blocking(move || AnalyticsHandle::spawn(&exex_socket))
        .await
        .expect("spawn_blocking panicked")?;

    let wallet = dev_wallet();
    let expected_owner = wallet.default_signer().address();

    let client = Glint::builder(node.rpc_url())
        .wallet(wallet)
        .flight_url(&analytics.flight_url())
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

    let receipt = client.send(&tx).await?;
    assert!(receipt.status(), "glint tx should succeed");

    let tx_hash = receipt.transaction_hash;
    let entity_key = derive_entity_key(&tx_hash, payload, 0);

    wait_for_analytics_ready(&analytics).await?;

    let owner_hex = format!("{:x}", expected_owner);
    let sql = format!(
        "SELECT \
            entity_key, \
            owner, \
            expires_at_block, \
            content_type, \
            payload, \
            str_ann(string_annotations, 'app') AS app_ann, \
            str_ann(string_annotations, 'env') AS env_ann, \
            num_ann(numeric_annotations, 'priority') AS priority_ann, \
            num_ann(numeric_annotations, 'version') AS version_ann \
         FROM entities \
         WHERE owner = x'{owner_hex}'"
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

    let priority_col: &arrow::array::UInt64Array = col(batch, "priority_ann");
    assert_eq!(priority_col.value(0), 42);

    let version_col: &arrow::array::UInt64Array = col(batch, "version_ann");
    assert_eq!(version_col.value(0), 1);

    let expires_col: &arrow::array::UInt64Array = col(batch, "expires_at_block");
    let block_number = receipt
        .block_number
        .expect("receipt should have block number");
    assert_eq!(expires_col.value(0), block_number + btl);

    Ok(())
}

#[tokio::test]
#[ignore = "requires built eth-glint + glint-analytics binaries; run with `just e2e`"]
async fn test_flight_sql_multi_entity_filters() -> eyre::Result<()> {
    let node = tokio::task::spawn_blocking(EthNodeHandle::spawn)
        .await
        .expect("spawn_blocking panicked")?;

    let exex_socket = node.exex_socket().to_path_buf();
    let analytics = tokio::task::spawn_blocking(move || AnalyticsHandle::spawn(&exex_socket))
        .await
        .expect("spawn_blocking panicked")?;

    let wallet = dev_wallet();
    let expected_owner = wallet.default_signer().address();

    let client = Glint::builder(node.rpc_url())
        .wallet(wallet)
        .flight_url(&analytics.flight_url())
        .build()
        .await?;

    // Create 3 entities with different annotations
    let payloads: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let mut entity_keys = Vec::new();

    for (i, payload) in payloads.iter().enumerate() {
        let category = if i < 2 { "group-a" } else { "group-b" };
        let tx = GlintTransaction::new().create(
            Create::new("text/plain", *payload, 200)
                .string_annotation("category", category)
                .numeric_annotation("rank", (i + 1) as u64),
        );
        let receipt = client.send(&tx).await?;
        assert!(receipt.status(), "tx {i} should succeed");
        entity_keys.push(derive_entity_key(&receipt.transaction_hash, *payload, 0));
    }

    wait_for_analytics_ready(&analytics).await?;

    // Filter by string annotation — should return 2 entities (alpha, beta)
    let batches = client
        .query("SELECT entity_key FROM entities WHERE str_ann(string_annotations, 'category') = 'group-a'")
        .await?;
    assert_eq!(row_count(&batches), 2, "expected 2 entities in group-a");

    // Filter by numeric annotation range — rank >= 2 should return 2 entities (beta, gamma)
    let batches = client
        .query("SELECT entity_key FROM entities WHERE num_ann(numeric_annotations, 'rank') >= 2")
        .await?;
    assert_eq!(row_count(&batches), 2, "expected 2 entities with rank >= 2");

    // Filter by exact numeric annotation — rank = 1 should return 1 entity (alpha)
    let batches = client
        .query("SELECT entity_key FROM entities WHERE num_ann(numeric_annotations, 'rank') = 1")
        .await?;
    assert_eq!(row_count(&batches), 1, "expected 1 entity with rank = 1");

    let ek_col: &arrow::array::FixedSizeBinaryArray = col(&batches[0], "entity_key");
    assert_eq!(ek_col.value(0), entity_keys[0].as_slice());

    // Combined filter — group-a AND rank = 2 should return 1 entity (beta)
    let batches = client
        .query(
            "SELECT entity_key FROM entities \
             WHERE str_ann(string_annotations, 'category') = 'group-a' \
               AND num_ann(numeric_annotations, 'rank') = 2",
        )
        .await?;
    assert_eq!(row_count(&batches), 1, "expected 1 entity matching group-a + rank=2");

    let ek_col: &arrow::array::FixedSizeBinaryArray = col(&batches[0], "entity_key");
    assert_eq!(ek_col.value(0), entity_keys[1].as_slice());

    // Owner filter — all 3 should belong to our wallet
    let owner_hex = format!("{:x}", expected_owner);
    let batches = client
        .query(&format!(
            "SELECT entity_key FROM entities WHERE owner = x'{owner_hex}'"
        ))
        .await?;
    assert_eq!(row_count(&batches), 3, "expected 3 entities owned by dev wallet");

    // IN list filter — group-b OR non-existent category
    let batches = client
        .query(
            "SELECT entity_key FROM entities \
             WHERE str_ann(string_annotations, 'category') IN ('group-b', 'group-c')",
        )
        .await?;
    assert_eq!(row_count(&batches), 1, "expected 1 entity in group-b/group-c");

    Ok(())
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

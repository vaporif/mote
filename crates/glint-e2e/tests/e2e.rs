use alloy_network::EthereumWallet;
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;

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
        .send_glint_transaction(&[create], &[], &[], &[])
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

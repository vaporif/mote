use mote_primitives::config::MoteChainConfig;

pub fn extract_mote_config(genesis: &serde_json::Value) -> eyre::Result<MoteChainConfig> {
    let config = if let Some(mote_section) = genesis.get("config").and_then(|c| c.get("mote")) {
        serde_json::from_value::<MoteChainConfig>(mote_section.clone())?
    } else {
        tracing::info!("no config.mote in genesis, using defaults");
        MoteChainConfig::default()
    };
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mote_primitives::constants::{MAX_BTL, MAX_OPS_PER_TX};

    #[test]
    fn extract_mote_config_from_genesis_json() {
        let genesis_json = serde_json::json!({
            "config": {
                "chainId": 901,
                "mote": {
                    "max_btl": 100_000,
                    "processor_address": "0x000000000000000000000000000000006d6f7465"
                }
            },
            "alloc": {}
        });
        let config = extract_mote_config(&genesis_json).expect("valid config");
        assert_eq!(config.max_btl, 100_000);
        assert_eq!(config.max_ops_per_tx, MAX_OPS_PER_TX);
    }

    #[test]
    fn missing_mote_section_uses_defaults() {
        let genesis_json = serde_json::json!({
            "config": { "chainId": 901 },
            "alloc": {}
        });
        let config = extract_mote_config(&genesis_json).expect("defaults valid");
        assert_eq!(config.max_btl, MAX_BTL);
    }

    #[test]
    fn invalid_config_rejected() {
        let genesis_json = serde_json::json!({
            "config": {
                "chainId": 901,
                "mote": { "max_btl": 0 }
            },
            "alloc": {}
        });
        assert!(extract_mote_config(&genesis_json).is_err());
    }

    #[test]
    fn extract_from_sample_genesis_file() {
        let genesis_str = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../etc/genesis.json"),
        )
        .unwrap();
        let genesis: serde_json::Value = serde_json::from_str(&genesis_str).unwrap();
        let config = extract_mote_config(&genesis).unwrap();
        assert_eq!(config.max_btl, 302_400);
        assert_eq!(
            config.processor_address,
            mote_primitives::constants::PROCESSOR_ADDRESS
        );
    }
}

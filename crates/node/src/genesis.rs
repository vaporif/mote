use glint_primitives::config::GlintChainConfig;

pub fn extract_glint_config(genesis: &serde_json::Value) -> eyre::Result<GlintChainConfig> {
    let config = if let Some(glint_section) = genesis.get("config").and_then(|c| c.get("glint")) {
        serde_json::from_value::<GlintChainConfig>(glint_section.clone())?
    } else {
        tracing::info!("no config.glint in genesis, using defaults");
        GlintChainConfig::default()
    };
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use glint_primitives::constants::{MAX_BTL, MAX_OPS_PER_TX};

    #[test]
    fn extract_glint_config_from_genesis_json() {
        let genesis_json = serde_json::json!({
            "config": {
                "chainId": 901,
                "glint": {
                    "max_btl": 100_000,
                    "processor_address": "0x000000000000000000000000000000676c696e74"
                }
            },
            "alloc": {}
        });
        let config = extract_glint_config(&genesis_json).expect("valid config");
        assert_eq!(config.max_btl, 100_000);
        assert_eq!(config.max_ops_per_tx, MAX_OPS_PER_TX);
    }

    #[test]
    fn missing_glint_section_uses_defaults() {
        let genesis_json = serde_json::json!({
            "config": { "chainId": 901 },
            "alloc": {}
        });
        let config = extract_glint_config(&genesis_json).expect("defaults valid");
        assert_eq!(config.max_btl, MAX_BTL);
    }

    #[test]
    fn invalid_config_rejected() {
        let genesis_json = serde_json::json!({
            "config": {
                "chainId": 901,
                "glint": { "max_btl": 0 }
            },
            "alloc": {}
        });
        assert!(extract_glint_config(&genesis_json).is_err());
    }

    #[test]
    fn extract_from_sample_genesis_file() {
        let genesis_str = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../etc/genesis.json"),
        )
        .unwrap();
        let genesis: serde_json::Value = serde_json::from_str(&genesis_str).unwrap();
        let config = extract_glint_config(&genesis).unwrap();
        assert_eq!(config.max_btl, 302_400);
        assert_eq!(
            config.processor_address,
            glint_primitives::constants::PROCESSOR_ADDRESS
        );
    }
}

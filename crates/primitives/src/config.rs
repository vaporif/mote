use alloy_primitives::Address;
use serde::Deserialize;

use crate::constants::{
    MAX_ANNOTATION_KEY_SIZE, MAX_ANNOTATION_VALUE_SIZE, MAX_ANNOTATIONS_PER_ENTITY, MAX_BTL,
    MAX_CONTENT_TYPE_SIZE, MAX_OPS_PER_TX, MAX_PAYLOAD_SIZE, PROCESSOR_ADDRESS,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct GlintChainConfig {
    pub max_btl: u64,
    pub max_ops_per_tx: usize,
    pub max_payload_size: usize,
    pub max_annotations_per_entity: usize,
    pub max_annotation_key_size: usize,
    pub max_annotation_value_size: usize,
    pub max_content_type_size: usize,
    pub processor_address: Address,
}

impl Default for GlintChainConfig {
    fn default() -> Self {
        Self {
            max_btl: MAX_BTL,
            max_ops_per_tx: MAX_OPS_PER_TX,
            max_payload_size: MAX_PAYLOAD_SIZE,
            max_annotations_per_entity: MAX_ANNOTATIONS_PER_ENTITY,
            max_annotation_key_size: MAX_ANNOTATION_KEY_SIZE,
            max_annotation_value_size: MAX_ANNOTATION_VALUE_SIZE,
            max_content_type_size: MAX_CONTENT_TYPE_SIZE,
            processor_address: PROCESSOR_ADDRESS,
        }
    }
}

impl GlintChainConfig {
    pub fn validate(&self) -> eyre::Result<()> {
        eyre::ensure!(self.max_btl > 0, "max_btl must be > 0");
        eyre::ensure!(self.max_ops_per_tx > 0, "max_ops_per_tx must be > 0");
        eyre::ensure!(self.max_payload_size > 0, "max_payload_size must be > 0");
        eyre::ensure!(
            self.max_annotations_per_entity > 0,
            "max_annotations_per_entity must be > 0"
        );
        eyre::ensure!(
            self.max_annotation_key_size > 0,
            "max_annotation_key_size must be > 0"
        );
        eyre::ensure!(
            self.max_annotation_value_size > 0,
            "max_annotation_value_size must be > 0"
        );
        eyre::ensure!(
            self.max_content_type_size > 0,
            "max_content_type_size must be > 0"
        );
        eyre::ensure!(
            self.processor_address != Address::ZERO,
            "processor_address must not be zero"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_constants() {
        let config = GlintChainConfig::default();
        assert_eq!(config.max_btl, MAX_BTL);
        assert_eq!(config.max_ops_per_tx, MAX_OPS_PER_TX);
        assert_eq!(config.max_payload_size, MAX_PAYLOAD_SIZE);
        assert_eq!(
            config.max_annotations_per_entity,
            MAX_ANNOTATIONS_PER_ENTITY
        );
        assert_eq!(config.max_annotation_key_size, MAX_ANNOTATION_KEY_SIZE);
        assert_eq!(config.max_annotation_value_size, MAX_ANNOTATION_VALUE_SIZE);
        assert_eq!(config.max_content_type_size, MAX_CONTENT_TYPE_SIZE);
        assert_eq!(config.processor_address, PROCESSOR_ADDRESS);
    }

    #[test]
    fn validate_valid_config() {
        let config = GlintChainConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_zero_max_btl_rejected() {
        let config = GlintChainConfig {
            max_btl: 0,
            ..GlintChainConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_zero_address_rejected() {
        let config = GlintChainConfig {
            processor_address: Address::ZERO,
            ..GlintChainConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_zero_sizes_rejected() {
        let config = GlintChainConfig {
            max_payload_size: 0,
            ..GlintChainConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn deserialize_partial_config_uses_defaults() {
        let json = r#"{"max_btl": 100000}"#;
        let config: GlintChainConfig = serde_json::from_str(json).expect("valid json");
        assert_eq!(config.max_btl, 100_000);
        assert_eq!(config.max_ops_per_tx, MAX_OPS_PER_TX);
    }

    #[test]
    fn deserialize_empty_object_uses_all_defaults() {
        let json = "{}";
        let config: GlintChainConfig = serde_json::from_str(json).expect("valid json");
        assert_eq!(config.max_btl, MAX_BTL);
    }
}

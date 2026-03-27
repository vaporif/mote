use alloy_primitives::{Address, B256};
use glint_primitives::transaction::ExtendPolicy;

#[derive(Debug, Clone)]
#[must_use]
pub struct CreateEntity {
    pub content_type: String,
    pub payload: Vec<u8>,
    pub btl: u64,
    pub extend_policy: ExtendPolicy,
    pub operator: Option<Address>,
    pub string_annotations: Vec<(String, String)>,
    pub numeric_annotations: Vec<(String, u64)>,
}

impl CreateEntity {
    pub fn new(content_type: impl Into<String>, payload: &[u8], btl: u64) -> Self {
        Self {
            content_type: content_type.into(),
            payload: payload.to_vec(),
            btl,
            extend_policy: ExtendPolicy::OwnerOnly,
            operator: None,
            string_annotations: Vec::new(),
            numeric_annotations: Vec::new(),
        }
    }

    pub const fn operator(mut self, addr: Address) -> Self {
        self.operator = Some(addr);
        self
    }

    pub const fn anyone_can_extend(mut self, yes: bool) -> Self {
        self.extend_policy = if yes {
            ExtendPolicy::AnyoneCanExtend
        } else {
            ExtendPolicy::OwnerOnly
        };
        self
    }

    pub fn string_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.string_annotations.push((key.into(), value.into()));
        self
    }

    pub fn numeric_annotation(mut self, key: impl Into<String>, value: u64) -> Self {
        self.numeric_annotations.push((key.into(), value));
        self
    }
}

#[derive(Debug, Clone)]
#[must_use]
pub struct UpdateEntity {
    pub entity_key: B256,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub btl: u64,
    pub extend_policy: Option<ExtendPolicy>,
    pub operator: Option<Option<Address>>,
    pub string_annotations: Vec<(String, String)>,
    pub numeric_annotations: Vec<(String, u64)>,
}

impl UpdateEntity {
    pub fn new(
        entity_key: B256,
        content_type: impl Into<String>,
        payload: &[u8],
        btl: u64,
    ) -> Self {
        Self {
            entity_key,
            content_type: content_type.into(),
            payload: payload.to_vec(),
            btl,
            extend_policy: None,
            operator: None,
            string_annotations: Vec::new(),
            numeric_annotations: Vec::new(),
        }
    }

    pub const fn extend_policy(mut self, policy: ExtendPolicy) -> Self {
        self.extend_policy = Some(policy);
        self
    }

    /// `Some(addr)` to set, `None` to remove.
    pub const fn operator(mut self, addr: Option<Address>) -> Self {
        self.operator = Some(addr);
        self
    }

    pub fn string_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.string_annotations.push((key.into(), value.into()));
        self
    }

    pub fn numeric_annotation(mut self, key: impl Into<String>, value: u64) -> Self {
        self.numeric_annotations.push((key.into(), value));
        self
    }
}

#[derive(Debug, Clone)]
#[must_use]
pub struct ExtendEntity {
    pub entity_key: B256,
    pub additional_blocks: u64,
}

impl ExtendEntity {
    pub const fn new(entity_key: B256, additional_blocks: u64) -> Self {
        Self {
            entity_key,
            additional_blocks,
        }
    }
}

#[derive(Debug, Clone)]
#[must_use]
pub struct DeleteEntity {
    pub entity_key: B256,
}

impl DeleteEntity {
    pub const fn new(entity_key: B256) -> Self {
        Self { entity_key }
    }
}

#[derive(Debug, Clone)]
#[must_use]
pub struct ChangeOwnerEntity {
    pub entity_key: B256,
    pub new_owner: Option<Address>,
    pub extend_policy: Option<ExtendPolicy>,
    pub operator: Option<Option<Address>>,
}

impl ChangeOwnerEntity {
    pub const fn new(entity_key: B256) -> Self {
        Self {
            entity_key,
            new_owner: None,
            extend_policy: None,
            operator: None,
        }
    }

    pub const fn new_owner(mut self, addr: Address) -> Self {
        self.new_owner = Some(addr);
        self
    }

    pub const fn extend_policy(mut self, policy: ExtendPolicy) -> Self {
        self.extend_policy = Some(policy);
        self
    }

    /// `Some(addr)` to set, `None` to remove.
    pub const fn operator(mut self, addr: Option<Address>) -> Self {
        self.operator = Some(addr);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_builder_chaining() {
        let operator = Address::repeat_byte(0x42);
        let e = CreateEntity::new("application/json", b"{}", 200)
            .operator(operator)
            .anyone_can_extend(true)
            .string_annotation("app", "test")
            .numeric_annotation("priority", 1);

        assert_eq!(e.extend_policy, ExtendPolicy::AnyoneCanExtend);
        assert_eq!(e.operator, Some(operator));
        assert_eq!(e.string_annotations, vec![("app".into(), "test".into())]);
        assert_eq!(e.numeric_annotations, vec![("priority".into(), 1)]);
    }

    #[test]
    fn update_operator_set_and_remove() {
        let key = B256::repeat_byte(0x01);
        let addr = Address::repeat_byte(0xAB);

        // Some(Some(addr)) = set operator
        let u = UpdateEntity::new(key, "text/plain", b"data", 100).operator(Some(addr));
        assert_eq!(u.operator, Some(Some(addr)));

        // Some(None) = remove operator
        let u2 = UpdateEntity::new(key, "text/plain", b"data", 100).operator(None);
        assert_eq!(u2.operator, Some(None));
    }
}

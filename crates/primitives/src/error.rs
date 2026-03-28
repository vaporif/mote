use alloy_primitives::B256;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GlintError {
    #[error("transaction contains no operations")]
    EmptyTransaction,

    #[error("BTL must be > 0 and <= {max}", max = crate::constants::MAX_BTL)]
    InvalidBtl,

    #[error("content_type must be non-empty and <= {max} bytes", max = crate::constants::MAX_CONTENT_TYPE_SIZE)]
    InvalidContentType,

    #[error("payload exceeds {max} bytes", max = crate::constants::MAX_PAYLOAD_SIZE)]
    PayloadTooLarge,

    #[error("invalid annotation key: {0}")]
    InvalidAnnotationKey(String),

    #[error("annotation key uses reserved prefix: {0}")]
    ReservedAnnotationKey(String),

    #[error("too many annotations: {0}, max is {max}", max = crate::constants::MAX_ANNOTATIONS_PER_ENTITY)]
    TooManyAnnotations(usize),

    #[error("annotation key too large: {0} bytes, max is {max}", max = crate::constants::MAX_ANNOTATION_KEY_SIZE)]
    AnnotationKeyTooLarge(usize),

    #[error("annotation value too large: {0} bytes, max is {max}", max = crate::constants::MAX_ANNOTATION_VALUE_SIZE)]
    AnnotationValueTooLarge(usize),

    #[error("duplicate annotation key: {0}")]
    DuplicateAnnotationKey(String),

    #[error("extend additional_blocks must be > 0")]
    InvalidExtend,

    #[error("transaction contains {0} operations, max is {max}", max = crate::constants::MAX_OPS_PER_TX)]
    TooManyOperations(usize),

    #[error("entity not found: {0}")]
    EntityNotFound(B256),

    #[error("sender is not the entity owner")]
    NotOwner,

    #[error("extend would exceed MAX_BTL from current block")]
    ExceedsMaxBtl,

    #[error("RLP decode error: {0}")]
    RlpDecode(String),

    #[error("sender is not authorized to extend this entity")]
    NotAuthorizedToExtend,

    #[error("sender is not authorized to update this entity")]
    NotAuthorizedToUpdate,

    #[error("operator cannot change entity permissions")]
    OperatorCannotChangePermissions,

    #[error("operator address cannot be zero")]
    InvalidOperatorAddress,

    #[error("duplicate entity key in transaction: {0}")]
    DuplicateEntityKey(B256),

    #[error("change_owner must specify at least one change")]
    EmptyChangeOwner,

    #[error("new owner address cannot be zero")]
    InvalidOwnerAddress,
}

#[derive(Debug, Clone, Copy)]
pub struct HandshakeInfo {
    pub oldest_block: u64,
    pub tip_block: u64,
}

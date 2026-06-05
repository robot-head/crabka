//! Single-owner exactly-once dedup engine.

pub mod topic;

/// Deterministic FNV-1a-64 over the key, modulo partition count. Stable
/// across processes/restarts (unlike `DefaultHasher`'s per-run state), so a
/// given key always maps to the same dedup partition.
#[must_use]
pub fn partition_for(key: &str, partitions: u32) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // `hash % partitions` is < `partitions` (a u32), so it always fits in
    // u32; the `unwrap_or` fallback is unreachable.
    u32::try_from(hash % u64::from(partitions.max(1))).unwrap_or(0)
}

/// Minimal placeholder; replaced with the real engine in Commit 4.
pub struct DedupEngine;

impl DedupEngine {
    #[allow(clippy::unused_async)]
    pub async fn dedup_produce(
        &self,
        _rec: &crate::types::GatewayRecord,
        _value: bytes::Bytes,
    ) -> Result<crate::types::RecordOutcome, crate::error::GatewayError> {
        Err(crate::error::GatewayError::Other(
            "dedup not wired yet".into(),
        ))
    }
}

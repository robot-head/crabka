//! Retention applied by `Log::tick`. Free functions so the policy is
//! testable in isolation from `Log`'s mutable state.

use std::{
    path::Path,
    time::{Duration, SystemTime},
};

use crabka_ids::Offset;
use crabka_units::prelude::{ByteSize, ByteSizeExt as _, TimeExt as _};
use tracing::instrument;

use crate::{config::LogConfig, error::LogError, name, segment::Segment};

pub fn now_ms(now: SystemTime) -> i64 {
    let millis = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[instrument(
    level = "debug",
    skip_all,
    fields(sealed = sealed.len(), evicted = tracing::field::Empty),
)]
pub fn time_based_evict(sealed: &[&Segment], config: &LogConfig, now: SystemTime) -> Vec<Offset> {
    let Some(retention) = config.retention else {
        return Vec::new();
    };
    // Truncating, not rounding: the cutoff is compared against on-disk batch
    // timestamps, so a sub-millisecond retention window must not round up into
    // evicting a segment a millisecond early.
    let cutoff_ms = now_ms(now).saturating_sub(retention.millis_i64_trunc());
    let out: Vec<Offset> = sealed
        .iter()
        .take_while(|s| s.max_timestamp() < cutoff_ms)
        .map(|s| s.base_offset())
        .collect();
    tracing::Span::current().record("evicted", out.len());
    out
}

#[instrument(
    level = "debug",
    skip_all,
    fields(
        sealed = sealed.len(),
        active_size = active_size.bytes_u64(),
        evicted = tracing::field::Empty,
    ),
)]
pub fn size_based_evict(
    sealed: &[&Segment],
    active_size: ByteSize,
    config: &LogConfig,
) -> Vec<Offset> {
    let Some(budget) = config.retention_size else {
        return Vec::new();
    };
    let total: ByteSize = sealed.iter().fold(active_size, |acc, s| acc + s.size());
    if total <= budget {
        return Vec::new();
    }
    let mut deletable = total - budget;
    let mut out = Vec::new();
    for seg in sealed {
        if deletable <= ByteSize::ZERO {
            break;
        }
        out.push(seg.base_offset());
        deletable -= seg.size();
    }
    tracing::Span::current().record("evicted", out.len());
    out
}

#[instrument(level = "debug", skip_all, fields(dir = %dir.display(), base_offset = base_offset.0), err)]
pub fn delete_segment_files(dir: &Path, base_offset: Offset) -> Result<(), LogError> {
    std::fs::remove_file(name::log_path(dir, base_offset.0))?;
    std::fs::remove_file(name::index_path(dir, base_offset.0))?;
    std::fs::remove_file(name::timeindex_path(dir, base_offset.0))?;
    Ok(())
}

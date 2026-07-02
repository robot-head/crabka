//! Retention applied by `Log::tick`. Free functions so the policy is
//! testable in isolation from `Log`'s mutable state.

use std::{
    path::Path,
    time::{Duration, SystemTime},
};

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
pub fn time_based_evict(sealed: &[&Segment], config: &LogConfig, now: SystemTime) -> Vec<i64> {
    let Some(retention) = config.retention_ms else {
        return Vec::new();
    };
    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
    let cutoff_ms = now_ms(now).saturating_sub(retention_ms);
    let out: Vec<i64> = sealed
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
    fields(sealed = sealed.len(), active_size, evicted = tracing::field::Empty),
)]
pub fn size_based_evict(sealed: &[&Segment], active_size: u64, config: &LogConfig) -> Vec<i64> {
    let Some(retention_bytes) = config.retention_bytes else {
        return Vec::new();
    };
    let total: u64 = sealed.iter().map(|s| s.size_bytes()).sum::<u64>() + active_size;
    if total <= retention_bytes {
        return Vec::new();
    }
    let mut deletable: u64 = total - retention_bytes;
    let mut out = Vec::new();
    for seg in sealed {
        if deletable == 0 {
            break;
        }
        let n = seg.size_bytes();
        out.push(seg.base_offset());
        deletable = deletable.saturating_sub(n);
    }
    tracing::Span::current().record("evicted", out.len());
    out
}

#[instrument(level = "debug", skip_all, fields(dir = %dir.display(), base_offset), err)]
pub fn delete_segment_files(dir: &Path, base_offset: i64) -> Result<(), LogError> {
    std::fs::remove_file(name::log_path(dir, base_offset))?;
    std::fs::remove_file(name::index_path(dir, base_offset))?;
    std::fs::remove_file(name::timeindex_path(dir, base_offset))?;
    Ok(())
}

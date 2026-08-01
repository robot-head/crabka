//! Registry snapshot and dry-run operation model.

use crabka_gres_control::RangeBoundary;
use crabka_gres_substrate::RangeStatsSnapshot;
use crabka_units::{ByteSize, Frequency};
use serde::{Deserialize, Serialize};

/// One compute endpoint capable of hosting Chapter Gres ranges.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComputeNode {
    pub compute_id: String,
}

/// Table-level policy visible to the balancer.
///
/// `PartialEq` but not `Eq`, and no `Hash`: the dimensioned thresholds are
/// `f64`-backed quantities, so equality is not reflexive across the whole
/// domain and no consistent hash exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TablePolicy {
    pub table_id: u64,
    pub table_name: String,
    pub is_sharded: bool,
    pub auto_shard_disabled: bool,
    /// Convert an unsharded table once its ranges together store this much.
    #[serde(with = "crabka_units::serde_units::human::byte_size")]
    pub convert_store_threshold: ByteSize,
    #[serde(with = "crabka_units::serde_units::human::frequency")]
    pub convert_commit_rate_threshold: Frequency,
    /// Bucket count for hash placement, present exactly for hash-sharded tables.
    pub hash_bucket_count: Option<u32>,
}

/// Per-range registry layout plus aggregated metrics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeMetrics {
    pub range_id: u32,
    pub table_id: u64,
    /// Inclusive lower bound of the keys this range owns.
    pub start_key: RangeBoundary,
    /// Exclusive upper bound, or open-ended for the final range.
    pub end_key: Option<RangeBoundary>,
    pub compute_id: String,
    /// Authoritative stored bytes. `None` means unknown, never zero.
    pub store_bytes: Option<u64>,
    /// Checkpoint size is diagnostic only and may be unknown.
    pub checkpoint_bytes: Option<u64>,
    /// Authoritative write rate over the stats sample interval.
    pub commit_rate: Option<u64>,
    /// Authoritative read rate over the stats sample interval.
    pub scan_bytes: Option<u64>,
    pub is_sharded: bool,
    pub co_location_group: Option<String>,
    pub co_location_bucket: Option<u32>,
    pub is_index_range: bool,
}

impl RangeMetrics {
    /// Return the scalar load used by balancing goals.
    #[must_use]
    pub const fn load_score(&self) -> Option<u64> {
        match (self.commit_rate, self.scan_bytes) {
            (Some(write_rate), Some(read_rate)) => {
                Some(write_rate.saturating_add(read_rate / 1024))
            }
            _ => None,
        }
    }
}

/// Metrics and policies for one tenant registry record.
///
/// `PartialEq` but not `Eq`, and no `Hash`, following [`TablePolicy`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantMetrics {
    pub tenant_name: String,
    pub computes: Vec<ComputeNode>,
    pub tables: Vec<TablePolicy>,
    pub ranges: Vec<RangeMetrics>,
}

impl TenantMetrics {
    /// Replace metric-dependent fields with one authoritative stats snapshot.
    ///
    /// A range omitted from the snapshot, or a metric omitted by its provider,
    /// remains unknown. The synthetic values on `self` are deliberately not used
    /// as fallbacks, so stale fixtures cannot turn missing live data into zero.
    #[must_use]
    pub fn with_stats_snapshot(&self, snapshot: &RangeStatsSnapshot) -> Self {
        let mut tenant = self.clone();
        for range in &mut tenant.ranges {
            let stats = snapshot.ranges.iter().find(|stats| {
                stats.tenant_name == tenant.tenant_name && stats.range_id == range.range_id
            });
            range.store_bytes = stats.and_then(|stats| stats.store_bytes);
            range.checkpoint_bytes = None;
            range.commit_rate = stats.and_then(|stats| stats.write_rate);
            range.scan_bytes = stats.and_then(|stats| stats.read_rate);
        }
        tenant
    }
}

/// Stable operation flavor names matching the range orchestration surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Split,
    Move,
    Merge,
    ConvertToSharded,
}

impl OperationKind {
    /// Return the executor-facing operation name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Split => "split",
            Self::Move => "move",
            Self::Merge => "merge",
            Self::ConvertToSharded => "convert_to_sharded",
        }
    }
}

/// Dry-run operation emitted by goals and eventually sent to range orchestrators.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum BalanceOperation {
    Split {
        tenant_name: String,
        source_range_id: u32,
        split_at: RangeBoundary,
    },
    Move {
        tenant_name: String,
        range_id: u32,
        from_compute_id: String,
        to_compute_id: String,
    },
    Merge {
        tenant_name: String,
        left_range_id: u32,
        right_range_id: u32,
    },
    ConvertToSharded {
        tenant_name: String,
        table_id: u64,
        table_name: String,
    },
}

impl BalanceOperation {
    /// Return the operation kind.
    #[must_use]
    pub const fn kind(&self) -> OperationKind {
        match self {
            Self::Split { .. } => OperationKind::Split,
            Self::Move { .. } => OperationKind::Move,
            Self::Merge { .. } => OperationKind::Merge,
            Self::ConvertToSharded { .. } => OperationKind::ConvertToSharded,
        }
    }

    /// Return the executor-facing operation name.
    #[must_use]
    pub const fn operation_name(&self) -> &'static str {
        self.kind().as_str()
    }

    /// Return true when this operation is a whole-range move.
    #[must_use]
    pub const fn is_move(&self) -> bool {
        matches!(self, Self::Move { .. })
    }

    /// Apply this dry-run operation to a mutable test fleet model.
    pub fn apply_to(&self, tenants: &mut [TenantMetrics]) {
        match self {
            Self::Move {
                tenant_name,
                range_id,
                to_compute_id,
                ..
            } => apply_move(tenants, tenant_name, *range_id, to_compute_id),
            Self::ConvertToSharded {
                tenant_name,
                table_id,
                ..
            } => {
                apply_conversion(tenants, tenant_name, *table_id);
            }
            Self::Split { .. } | Self::Merge { .. } => {}
        }
    }
}

fn apply_move(
    tenants: &mut [TenantMetrics],
    tenant_name: &str,
    range_id: u32,
    to_compute_id: &str,
) {
    let Some(range) = tenants
        .iter_mut()
        .find(|tenant| tenant.tenant_name == tenant_name)
        .and_then(|tenant| {
            tenant
                .ranges
                .iter_mut()
                .find(|range| range.range_id == range_id)
        })
    else {
        return;
    };

    range.compute_id = to_compute_id.to_string();
}

fn apply_conversion(tenants: &mut [TenantMetrics], tenant_name: &str, table_id: u64) {
    let Some(tenant) = tenants
        .iter_mut()
        .find(|tenant| tenant.tenant_name == tenant_name)
    else {
        return;
    };

    if let Some(table) = tenant
        .tables
        .iter_mut()
        .find(|table| table.table_id == table_id)
    {
        table.is_sharded = true;
    }
    for range in tenant
        .ranges
        .iter_mut()
        .filter(|range| range.table_id == table_id)
    {
        range.is_sharded = true;
    }
}

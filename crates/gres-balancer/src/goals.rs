//! Goal implementations for the dry-run balancer.

use std::collections::{BTreeMap, HashMap};

use crabka_units::{ByteSize, convert::ByteSizeExt as _, gibibytes, mebibytes};
use serde::{Deserialize, Serialize};

use crate::model::{BalanceOperation, OperationKind, RangeMetrics, TenantMetrics};

/// Hard goals protect invariants; soft goals improve placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalPriority {
    Hard,
    Soft,
}

/// Configuration and recent-operation memory used by goals.
///
/// `PartialEq` but not `Eq`: the two byte thresholds are `f64`-backed
/// quantities, so equality is not reflexive across the whole domain.
///
/// The `_bytes` suffixes stay on the two [`ByteSize`] fields because they are
/// the JSON keys operators write (`sizeCeilingBytes`, `mergeFloorBytes`) and the
/// names the `GresBalancerThresholds` CRD mirrors; renaming the Rust fields
/// would move the wire encoding with them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalContext {
    /// Split any range stored above this size.
    #[serde(with = "crabka_units::serde_units::numeric::bytes_u64")]
    pub size_ceiling_bytes: ByteSize,
    /// Merge adjacent ranges whose combined size stays below this floor.
    #[serde(with = "crabka_units::serde_units::numeric::bytes_u64")]
    pub merge_floor_bytes: ByteSize,
    pub split_stride_rows: u64,
    pub load_skew_hysteresis_pct: u32,
    pub max_ranges_per_compute: Option<usize>,
    pub max_operations: usize,
    pub cooldown_epochs: u64,
    pub current_epoch: u64,
    pub cooldowns: Vec<(u32, OperationKind, u64)>,
}

impl GoalContext {
    /// Return whether a range/kind pair is still inside its cooldown window.
    #[must_use]
    pub fn is_in_cooldown(&self, range_id: u32, kind: OperationKind) -> bool {
        self.cooldowns
            .iter()
            .any(|(cooled_range, cooled_kind, until)| {
                *cooled_range == range_id && *cooled_kind == kind && *until > self.current_epoch
            })
    }
}

impl Default for GoalContext {
    fn default() -> Self {
        Self {
            size_ceiling_bytes: gibibytes(1),
            merge_floor_bytes: mebibytes(64),
            split_stride_rows: 1_000_000,
            load_skew_hysteresis_pct: 25,
            max_ranges_per_compute: None,
            max_operations: 32,
            cooldown_epochs: 2,
            current_epoch: 0,
            cooldowns: Vec::new(),
        }
    }
}

/// Planner goal names accepted by dry-run integration surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalName {
    CoLocationIntegrity,
    RangeLimit,
    RangeSize,
    LoadSkew,
    AutoShardConversion,
}

/// Per-goal enablement knobs for dry-run planning.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalToggles {
    #[serde(default)]
    pub disabled_goals: Vec<GoalName>,
}

impl GoalToggles {
    /// Return whether a goal is enabled.
    #[must_use]
    pub fn is_enabled(&self, goal: GoalName) -> bool {
        !self.disabled_goals.contains(&goal)
    }
}

/// Complete dry-run balancer configuration.
///
/// `PartialEq` but not `Eq`, following [`GoalContext`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BalancerConfig {
    #[serde(default)]
    pub goals: GoalToggles,
    #[serde(default)]
    pub context: GoalContext,
}

/// A planner goal over registry and range-metric snapshots.
pub trait Goal: Send + Sync {
    /// Stable goal name reported in plans.
    fn name(&self) -> &'static str;
    /// Goal priority.
    fn priority(&self) -> GoalPriority;
    /// Propose operations against the current working snapshot.
    fn propose(&self, tenants: &[TenantMetrics], ctx: &GoalContext) -> Vec<BalanceOperation>;
}

/// Keeps co-located hash buckets on the same compute.
#[derive(Debug, Clone, Copy)]
pub struct CoLocationGoal;

impl Goal for CoLocationGoal {
    fn name(&self) -> &'static str {
        "co_location_integrity"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, tenants: &[TenantMetrics], _ctx: &GoalContext) -> Vec<BalanceOperation> {
        let mut operations = Vec::new();
        for tenant in tenants {
            let mut anchors: BTreeMap<(String, u32), &RangeMetrics> = BTreeMap::new();
            for range in tenant.ranges.iter().filter(|range| {
                range.co_location_group.is_some() && range.co_location_bucket.is_some()
            }) {
                let key = (
                    range.co_location_group.clone().unwrap_or_default(),
                    range.co_location_bucket.unwrap_or_default(),
                );
                if let Some(anchor) = anchors.get(&key) {
                    if anchor.compute_id != range.compute_id {
                        operations.push(BalanceOperation::Move {
                            tenant_name: tenant.tenant_name.clone(),
                            range_id: range.range_id,
                            from_compute_id: range.compute_id.clone(),
                            to_compute_id: anchor.compute_id.clone(),
                        });
                    }
                } else {
                    anchors.insert(key, range);
                }
            }
        }
        operations
    }
}

/// Keeps per-compute range counts below a configured limit.
#[derive(Debug, Clone, Copy)]
pub struct RangeLimitGoal;

impl Goal for RangeLimitGoal {
    fn name(&self) -> &'static str {
        "range_limit"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, tenants: &[TenantMetrics], ctx: &GoalContext) -> Vec<BalanceOperation> {
        let Some(limit) = ctx.max_ranges_per_compute else {
            return Vec::new();
        };

        let mut operations = Vec::new();
        for tenant in tenants {
            let mut counts = range_counts_by_compute(tenant);
            for overloaded in overloaded_compute_ids(&counts, limit) {
                let Some(target) = least_loaded_compute(&counts, Some(&overloaded)) else {
                    continue;
                };
                if counts.get(&target).copied().unwrap_or_default() >= limit {
                    continue;
                }
                let Some(range) = tenant
                    .ranges
                    .iter()
                    .filter(|range| range.compute_id == overloaded)
                    .filter_map(|range| range.load_score().map(|score| (range, score)))
                    .min_by_key(|(_, score)| *score)
                    .map(|(range, _)| range)
                else {
                    continue;
                };
                operations.push(BalanceOperation::Move {
                    tenant_name: tenant.tenant_name.clone(),
                    range_id: range.range_id,
                    from_compute_id: overloaded.clone(),
                    to_compute_id: target.clone(),
                });
                decrement_count(&mut counts, &overloaded);
                *counts.entry(target).or_default() += 1;
            }
        }
        operations
    }
}

/// Splits ranges above the ceiling and merges adjacent small ranges.
#[derive(Debug, Clone, Copy)]
pub struct SizeGoal;

impl Goal for SizeGoal {
    fn name(&self) -> &'static str {
        "range_size"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }

    fn propose(&self, tenants: &[TenantMetrics], ctx: &GoalContext) -> Vec<BalanceOperation> {
        let mut operations = Vec::new();
        for tenant in tenants {
            operations.extend(split_oversized_ranges(tenant, ctx));
            operations.extend(merge_tiny_adjacent_ranges(tenant, ctx));
        }
        operations
    }
}

/// Moves hot ranges from overloaded computes to colder computes.
#[derive(Debug, Clone, Copy)]
pub struct LoadSkewGoal;

impl Goal for LoadSkewGoal {
    fn name(&self) -> &'static str {
        "load_skew"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }

    fn propose(&self, tenants: &[TenantMetrics], ctx: &GoalContext) -> Vec<BalanceOperation> {
        let mut operations = Vec::new();
        for tenant in tenants {
            let totals = load_by_compute(tenant);
            if skew_pct(&totals) <= ctx.load_skew_hysteresis_pct {
                continue;
            }
            let Some((hot_compute, _)) = totals.iter().max_by_key(|(_, total)| *total) else {
                continue;
            };
            let Some((cold_compute, _)) = totals.iter().min_by_key(|(_, total)| *total) else {
                continue;
            };
            if hot_compute == cold_compute {
                continue;
            }
            let Some(range) = best_skew_reducing_range(tenant, &totals, hot_compute, cold_compute)
            else {
                continue;
            };
            if ctx.is_in_cooldown(range.range_id, OperationKind::Move) {
                continue;
            }
            operations.push(BalanceOperation::Move {
                tenant_name: tenant.tenant_name.clone(),
                range_id: range.range_id,
                from_compute_id: hot_compute.clone(),
                to_compute_id: cold_compute.clone(),
            });
        }
        operations
    }
}

/// Recommends online sharded conversion for unsharded hot or large tables.
#[derive(Debug, Clone, Copy)]
pub struct ConversionGoal;

impl Goal for ConversionGoal {
    fn name(&self) -> &'static str {
        "auto_shard_conversion"
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }

    fn propose(&self, tenants: &[TenantMetrics], _ctx: &GoalContext) -> Vec<BalanceOperation> {
        let mut operations = Vec::new();
        for tenant in tenants {
            for table in tenant
                .tables
                .iter()
                .filter(|table| !table.is_sharded && !table.auto_shard_disabled)
            {
                let totals = tenant
                    .ranges
                    .iter()
                    .filter(|range| range.table_id == table.table_id)
                    .try_fold((0_u64, 0_u64), |(bytes, commits), range| {
                        Some((
                            bytes.saturating_add(range.store_bytes?),
                            commits.saturating_add(range.commit_rate?),
                        ))
                    });
                let Some(totals) = totals else {
                    continue;
                };
                if ByteSize::from_bytes(totals.0) < table.convert_store_bytes_threshold
                    && totals.1 < table.convert_commit_rate_threshold
                {
                    continue;
                }
                operations.push(BalanceOperation::ConvertToSharded {
                    tenant_name: tenant.tenant_name.clone(),
                    table_id: table.table_id,
                    table_name: table.table_name.clone(),
                });
            }
        }
        operations
    }
}

fn split_oversized_ranges(tenant: &TenantMetrics, ctx: &GoalContext) -> Vec<BalanceOperation> {
    tenant
        .ranges
        .iter()
        .filter(|range| {
            range
                .store_bytes
                .is_some_and(|bytes| ByteSize::from_bytes(bytes) > ctx.size_ceiling_bytes)
        })
        .filter(|range| !ctx.is_in_cooldown(range.range_id, OperationKind::Split))
        .map(|range| BalanceOperation::Split {
            tenant_name: tenant.tenant_name.clone(),
            table_id: range.table_id,
            source_range_id: range.range_id,
            split_at_rowid: split_at_rowid(range, ctx.split_stride_rows),
        })
        .collect()
}

fn merge_tiny_adjacent_ranges(tenant: &TenantMetrics, ctx: &GoalContext) -> Vec<BalanceOperation> {
    tenant
        .ranges
        .windows(2)
        .filter_map(|pair| {
            let [left, right] = pair else {
                return None;
            };
            if left.table_id != right.table_id || left.compute_id != right.compute_id {
                return None;
            }
            let (Some(left_bytes), Some(right_bytes)) = (left.store_bytes, right.store_bytes)
            else {
                return None;
            };
            if ByteSize::from_bytes(left_bytes.saturating_add(right_bytes)) >= ctx.merge_floor_bytes
            {
                return None;
            }
            if ctx.is_in_cooldown(left.range_id, OperationKind::Merge)
                || ctx.is_in_cooldown(right.range_id, OperationKind::Merge)
            {
                return None;
            }
            Some(BalanceOperation::Merge {
                tenant_name: tenant.tenant_name.clone(),
                left_range_id: left.range_id,
                right_range_id: right.range_id,
            })
        })
        .collect()
}

fn split_at_rowid(range: &RangeMetrics, stride: u64) -> u64 {
    if let Some(end) = range.end_rowid {
        return range
            .start_rowid
            .saturating_add(end.saturating_sub(range.start_rowid) / 2);
    }
    range.start_rowid.saturating_add(stride.max(1))
}

fn range_counts_by_compute(tenant: &TenantMetrics) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = tenant
        .computes
        .iter()
        .map(|compute| (compute.compute_id.clone(), 0))
        .collect();
    for range in &tenant.ranges {
        *counts.entry(range.compute_id.clone()).or_default() += 1;
    }
    counts
}

fn load_by_compute(tenant: &TenantMetrics) -> HashMap<String, u64> {
    let mut totals: HashMap<String, u64> = tenant
        .computes
        .iter()
        .map(|compute| (compute.compute_id.clone(), 0))
        .collect();
    for range in &tenant.ranges {
        let Some(load_score) = range.load_score() else {
            return HashMap::new();
        };
        *totals.entry(range.compute_id.clone()).or_default() = totals
            .get(&range.compute_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(load_score);
    }
    totals
}

fn overloaded_compute_ids(counts: &HashMap<String, usize>, limit: usize) -> Vec<String> {
    let mut overloaded: Vec<String> = counts
        .iter()
        .filter(|(_, count)| **count > limit)
        .map(|(compute_id, _)| compute_id.clone())
        .collect();
    overloaded.sort();
    overloaded
}

fn least_loaded_compute(
    counts: &HashMap<String, usize>,
    excluded_compute: Option<&str>,
) -> Option<String> {
    counts
        .iter()
        .filter(|(compute_id, _)| Some(compute_id.as_str()) != excluded_compute)
        .min_by_key(|(compute_id, count)| (**count, compute_id.as_str()))
        .map(|(compute_id, _)| compute_id.clone())
}

fn decrement_count(counts: &mut HashMap<String, usize>, compute_id: &str) {
    if let Some(count) = counts.get_mut(compute_id) {
        *count = count.saturating_sub(1);
    }
}

fn skew_pct(totals: &HashMap<String, u64>) -> u32 {
    let total: u64 = totals.values().copied().sum();
    if total == 0 {
        return 0;
    }
    let max = totals.values().copied().max().unwrap_or_default();
    let min = totals.values().copied().min().unwrap_or_default();
    u32::try_from(max.saturating_sub(min).saturating_mul(100) / total).unwrap_or(u32::MAX)
}

fn best_skew_reducing_range<'a>(
    tenant: &'a TenantMetrics,
    totals: &HashMap<String, u64>,
    hot_compute: &str,
    cold_compute: &str,
) -> Option<&'a RangeMetrics> {
    let current_skew = skew_pct(totals);
    tenant
        .ranges
        .iter()
        .filter(|range| range.compute_id == hot_compute)
        .filter_map(|range| {
            let mut projected = totals.clone();
            let load = range.load_score()?;
            decrement_load(&mut projected, hot_compute, load);
            *projected.entry(cold_compute.to_string()).or_default() = projected
                .get(cold_compute)
                .copied()
                .unwrap_or_default()
                .saturating_add(load);
            let projected_skew = skew_pct(&projected);
            (projected_skew < current_skew).then_some((range, projected_skew))
        })
        .min_by_key(|(range, projected_skew)| {
            (*projected_skew, std::cmp::Reverse(range.load_score()))
        })
        .map(|(range, _)| range)
}

fn decrement_load(totals: &mut HashMap<String, u64>, compute_id: &str, load: u64) {
    if let Some(total) = totals.get_mut(compute_id) {
        *total = total.saturating_sub(load);
    }
}

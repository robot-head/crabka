//! Goal-ordered planner.

use std::time::SystemTime;

use crabka_gres_substrate::{RangeStatsProvider, RangeStatsSnapshot};
use crabka_units::{Time, convert::StdDurationExt as _};

use crate::{
    goals::{
        BalancerConfig, CoLocationGoal, ConversionGoal, Goal, GoalContext, GoalName, GoalPriority,
        LoadSkewGoal, RangeLimitGoal, SizeGoal,
    },
    model::{BalanceOperation, TenantMetrics},
};

/// Dry-run plan emitted by the balancer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub operations: Vec<BalanceOperation>,
}

/// Planner output plus diagnostic goal order.
///
/// `PartialEq` but not `Eq`: [`TenantMetrics`] carries `f64`-backed quantities,
/// so equality over `state_after` is not reflexive across the whole domain.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanOutput {
    pub plan: Plan,
    pub goals_applied: Vec<String>,
    pub state_after: Vec<TenantMetrics>,
    /// Reason snapshot-driven planning safely abstained, if any.
    pub diagnostic: Option<PlanningDiagnostic>,
}

/// Explicit freshness bound for authoritative range-statistics snapshots.
///
/// `PartialEq` but not `Eq`: the bound is an `f64`-backed quantity, so equality
/// is not reflexive across the whole domain.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StatsFreshness {
    max_age: Time,
}

impl StatsFreshness {
    /// Build a policy that accepts samples no older than `max_age`.
    #[must_use]
    pub const fn new(max_age: Time) -> Self {
        Self { max_age }
    }

    /// Return the maximum accepted sample age.
    #[must_use]
    pub const fn max_age(self) -> Time {
        self.max_age
    }
}

/// Version progression remembered across snapshot-driven planning attempts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatsVersionProgress {
    last_accepted_version: Option<u64>,
}

/// Reason planning declined to use a range-statistics snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanningDiagnostic {
    /// The sample predates the caller-selected freshness bound.
    StaleSnapshot,
    /// The sample timestamp is later than the caller's planning clock.
    FutureSnapshot,
    /// The sample version was repeated or regressed after a prior accepted sample.
    NonProgressingVersion,
}

/// Goal-based range planner.
pub struct Planner {
    goals: Vec<Box<dyn Goal>>,
}

impl Planner {
    /// Build a planner from registered goals.
    #[must_use]
    pub fn new(goals: Vec<Box<dyn Goal>>) -> Self {
        Self { goals }
    }

    /// Build a planner from goal enablement knobs.
    #[must_use]
    pub fn from_config(config: &BalancerConfig) -> Self {
        let mut goals: Vec<Box<dyn Goal>> = Vec::new();
        if config.goals.is_enabled(GoalName::CoLocationIntegrity) {
            goals.push(Box::new(CoLocationGoal));
        }
        if config.goals.is_enabled(GoalName::RangeLimit) {
            goals.push(Box::new(RangeLimitGoal));
        }
        if config.goals.is_enabled(GoalName::RangeSize) {
            goals.push(Box::new(SizeGoal));
        }
        if config.goals.is_enabled(GoalName::LoadSkew) {
            goals.push(Box::new(LoadSkewGoal));
        }
        if config.goals.is_enabled(GoalName::AutoShardConversion) {
            goals.push(Box::new(ConversionGoal));
        }
        Self { goals }
    }

    /// Compute a dry-run plan from registry/range metrics.
    #[must_use]
    pub fn plan(&self, tenants: &[TenantMetrics], ctx: &GoalContext) -> PlanOutput {
        self.plan_tenants(tenants, ctx)
    }

    /// Compute a dry-run plan using a fresh, version-progressing stats snapshot.
    ///
    /// `freshness` is explicit so callers choose the maximum acceptable metric age.
    /// `progress` rejects repeated and regressed versions after an accepted sample.
    /// Invalid snapshots produce no operations and a [`PlanningDiagnostic`]. Missing
    /// range values in an accepted snapshot stay unknown, causing metric-dependent
    /// goals to abstain.
    #[must_use]
    pub fn plan_with_snapshot(
        &self,
        tenants: &[TenantMetrics],
        snapshot: &RangeStatsSnapshot,
        ctx: &GoalContext,
        now: SystemTime,
        freshness: StatsFreshness,
        progress: &mut StatsVersionProgress,
    ) -> PlanOutput {
        if let Some(diagnostic) = validate_snapshot(snapshot, now, freshness, progress) {
            return Self::abstained_plan(tenants, diagnostic);
        }
        let observed: Vec<TenantMetrics> = tenants
            .iter()
            .map(|tenant| tenant.with_stats_snapshot(snapshot))
            .collect();
        self.plan_tenants(&observed, ctx)
    }

    /// Read a provider's latest fresh, version-progressing sample and plan from it.
    #[must_use]
    pub fn plan_with_provider(
        &self,
        tenants: &[TenantMetrics],
        provider: &dyn RangeStatsProvider,
        ctx: &GoalContext,
        now: SystemTime,
        freshness: StatsFreshness,
        progress: &mut StatsVersionProgress,
    ) -> PlanOutput {
        self.plan_with_snapshot(tenants, &provider.snapshot(), ctx, now, freshness, progress)
    }

    fn abstained_plan(tenants: &[TenantMetrics], diagnostic: PlanningDiagnostic) -> PlanOutput {
        PlanOutput {
            plan: Plan::default(),
            goals_applied: Vec::new(),
            state_after: tenants.to_vec(),
            diagnostic: Some(diagnostic),
        }
    }

    fn plan_tenants(&self, tenants: &[TenantMetrics], ctx: &GoalContext) -> PlanOutput {
        let mut ordered: Vec<(usize, &dyn Goal)> = self
            .goals
            .iter()
            .enumerate()
            .map(|(index, goal)| (index, goal.as_ref()))
            .collect();
        ordered.sort_by_key(|(index, goal)| (priority_rank(goal.priority()), *index));

        let mut working = tenants.to_vec();
        let mut operations = Vec::new();
        let mut goals_applied = Vec::with_capacity(ordered.len());

        for (_, goal) in ordered {
            goals_applied.push(goal.name().to_string());
            for operation in goal.propose(&working, ctx) {
                if operations.len() >= ctx.max_operations {
                    break;
                }
                operation.apply_to(&mut working);
                operations.push(operation);
            }
        }

        PlanOutput {
            plan: Plan { operations },
            goals_applied,
            state_after: working,
            diagnostic: None,
        }
    }
}

fn validate_snapshot(
    snapshot: &RangeStatsSnapshot,
    now: SystemTime,
    freshness: StatsFreshness,
    progress: &mut StatsVersionProgress,
) -> Option<PlanningDiagnostic> {
    let Ok(age) = now.duration_since(snapshot.sampled_at) else {
        return Some(PlanningDiagnostic::FutureSnapshot);
    };
    if age.as_time() > freshness.max_age() {
        return Some(PlanningDiagnostic::StaleSnapshot);
    }
    if progress
        .last_accepted_version
        .is_some_and(|last_version| snapshot.version <= last_version)
    {
        return Some(PlanningDiagnostic::NonProgressingVersion);
    }
    progress.last_accepted_version = Some(snapshot.version);
    None
}

fn priority_rank(priority: GoalPriority) -> u8 {
    match priority {
        GoalPriority::Hard => 0,
        GoalPriority::Soft => 1,
    }
}

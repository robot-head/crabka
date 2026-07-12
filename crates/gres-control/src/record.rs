//! Tenant registry record schema and byte encoders.

use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::ControlError;

/// The compacted Kafka topic that stores whole-tenant registry snapshots.
pub const TENANT_REGISTRY_TOPIC: &str = "__gres_tenants";

/// Ordered durable progress for one registry-owned split initiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SplitOperationPhase {
    /// Immutable split intent has been accepted by the registry.
    Initiated,
    /// At least one execution attempt has started.
    Running,
    /// Source checkpoint receipt was acknowledged.
    Checkpointed,
    /// Source writer pause receipt was acknowledged.
    Paused,
    /// Successor filtered restore and marker inheritance completed.
    Restored,
    /// Successor fence/prologue proved the replacements ready.
    Activated,
    /// Atomic successor layout publication completed.
    LayoutPublished,
    /// Predecessor retirement is in progress or acknowledged.
    Retiring,
    /// Serving successors are being resumed after retirement.
    Resuming,
    /// The latest attempt failed and may be retried.
    Failed,
    /// The split completed successfully.
    Completed,
}

/// Append-only receipts recorded by the operator after each acknowledged control step.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitOperationEvidence {
    pub manifest_key: Option<String>,
    pub covered_offset: Option<i64>,
    pub barrier_offset: Option<i64>,
    pub tail_sha256: Option<String>,
    pub marker_digest: Option<String>,
}

/// Immutable full-layout authority captured when the operation is initiated.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitOperationPlan {
    pub source_record_version: u64,
    pub source_map_epoch: u64,
    pub routing_table_id: u64,
    pub current_layout: Vec<RangeLayoutEntry>,
    pub target_layout: Vec<RangeLayoutEntry>,
}

/// Durable registry journal record for initiating a sealed split or move.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SplitOperationRecord {
    /// Tenant whose range layout is being split.
    pub tenant: TenantName,
    /// Caller-chosen idempotency identity.
    pub operation_id: String,
    /// CAS revision, beginning at zero.
    pub revision: u64,
    /// One authoritative immutable split-or-move intent.
    pub mutation: RangeMutationPlan,
    /// Full immutable source/target layout and catalog routing identity.
    #[serde(default)]
    pub plan: Option<SplitOperationPlan>,
    /// Monotone operation phase.
    pub phase: SplitOperationPhase,
    /// Monotone number of execution attempts begun.
    pub attempts: u32,
    /// Append-only failure history.
    pub errors: Vec<String>,
    /// Append-only, response-derived control evidence.
    #[serde(default)]
    pub evidence: SplitOperationEvidence,
}

impl<'de> Deserialize<'de> for SplitOperationRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRecord {
            tenant: TenantName,
            operation_id: String,
            revision: u64,
            #[serde(default)]
            mutation: Option<RangeMutationPlan>,
            #[serde(default)]
            split: Option<SplitState>,
            #[serde(default)]
            plan: Option<SplitOperationPlan>,
            phase: SplitOperationPhase,
            attempts: u32,
            errors: Vec<String>,
            #[serde(default)]
            evidence: SplitOperationEvidence,
        }

        let wire = WireRecord::deserialize(deserializer)?;
        let mutation = match (wire.mutation, wire.split) {
            (Some(mutation), None) => mutation,
            (None, Some(split)) => RangeMutationPlan::Split { split },
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "split operation must contain exactly one mutation representation",
                ));
            }
            (None, None) => {
                return Err(serde::de::Error::missing_field("mutation"));
            }
        };
        Ok(Self {
            tenant: wire.tenant,
            operation_id: wire.operation_id,
            revision: wire.revision,
            mutation,
            plan: wire.plan,
            phase: wire.phase,
            attempts: wire.attempts,
            errors: wire.errors,
            evidence: wire.evidence,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RangeMutationPlan {
    Split { split: SplitState },
    Move { move_range: MoveRangeState },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRangeState {
    pub source_range_id: u32,
    pub predecessor_generation: u64,
    pub replacement: RangeLayoutEntry,
}

impl SplitOperationRecord {
    /// Construct a revision-zero initiation record.
    pub fn new(
        tenant: TenantName,
        operation_id: impl Into<String>,
        split: SplitState,
    ) -> Result<Self, crate::ControlError> {
        let record = Self {
            tenant,
            operation_id: operation_id.into(),
            revision: 0,
            mutation: RangeMutationPlan::Split { split },
            plan: None,
            phase: SplitOperationPhase::Initiated,
            attempts: 0,
            errors: Vec::new(),
            evidence: SplitOperationEvidence::default(),
        };
        record.ensure_valid()?;
        Ok(record)
    }

    pub fn new_move(
        tenant: TenantName,
        operation_id: impl Into<String>,
        source_range_id: u32,
        predecessor_generation: u64,
        replacement: RangeLayoutEntry,
    ) -> Result<Self, crate::ControlError> {
        let record = Self {
            tenant,
            operation_id: operation_id.into(),
            revision: 0,
            mutation: RangeMutationPlan::Move {
                move_range: MoveRangeState {
                    source_range_id,
                    predecessor_generation,
                    replacement,
                },
            },
            plan: None,
            phase: SplitOperationPhase::Initiated,
            attempts: 0,
            errors: Vec::new(),
            evidence: SplitOperationEvidence::default(),
        };
        record.ensure_valid()?;
        Ok(record)
    }

    pub fn with_plan(mut self, plan: SplitOperationPlan) -> Result<Self, crate::ControlError> {
        if self.revision != 0 || self.phase != SplitOperationPhase::Initiated {
            return Err(crate::ControlError::invalid_field(
                "split_operation.plan",
                "may only be sealed at initiation",
            ));
        }
        plan.ensure_valid(&self)?;
        self.plan = Some(plan);
        self.ensure_valid()?;
        Ok(self)
    }

    /// Build the next revision while preserving monotone progress.
    pub fn advance(
        &self,
        phase: SplitOperationPhase,
        attempts: u32,
        error: Option<String>,
    ) -> Result<Self, crate::ControlError> {
        self.advance_with_evidence(phase, attempts, error, self.evidence.clone())
    }

    pub fn advance_with_evidence(
        &self,
        phase: SplitOperationPhase,
        attempts: u32,
        error: Option<String>,
        evidence: SplitOperationEvidence,
    ) -> Result<Self, crate::ControlError> {
        let mut next = self.clone();
        next.revision = next.revision.checked_add(1).ok_or_else(|| {
            crate::ControlError::invalid_field("split_operation.revision", "must not overflow")
        })?;
        next.phase = phase;
        next.attempts = attempts;
        next.evidence = evidence;
        if let Some(error) = error {
            next.errors.push(error);
        }
        next.ensure_monotone_extension(self)?;
        Ok(next)
    }

    pub(crate) fn ensure_valid(&self) -> Result<(), crate::ControlError> {
        if self.operation_id.is_empty() {
            return Err(crate::ControlError::invalid_field(
                "split_operation.operation_id",
                "must not be empty",
            ));
        }
        if self.errors.iter().any(String::is_empty) {
            return Err(crate::ControlError::invalid_field(
                "split_operation.errors",
                "entries must not be empty",
            ));
        }
        match &self.mutation {
            RangeMutationPlan::Split { split } => split.ensure_intent_valid()?,
            RangeMutationPlan::Move { move_range } => move_range.ensure_intent_valid()?,
        }
        if let Some(plan) = &self.plan {
            plan.ensure_valid(self)?;
        }
        self.evidence.ensure_valid()?;
        let phase_shape = match self.phase {
            SplitOperationPhase::Initiated => {
                self.revision == 0 && self.attempts == 0 && self.errors.is_empty()
            }
            SplitOperationPhase::Running
            | SplitOperationPhase::Checkpointed
            | SplitOperationPhase::Paused
            | SplitOperationPhase::LayoutPublished
            | SplitOperationPhase::Restored
            | SplitOperationPhase::Activated
            | SplitOperationPhase::Retiring
            | SplitOperationPhase::Resuming
            | SplitOperationPhase::Completed => self.revision > 0 && self.attempts > 0,
            SplitOperationPhase::Failed => {
                self.revision > 0 && self.attempts > 0 && !self.errors.is_empty()
            }
        };
        if !phase_shape || self.errors.len() > self.attempts as usize {
            return Err(crate::ControlError::invalid_field(
                "split_operation.progress",
                "phase, attempts, and errors are inconsistent",
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_monotone_extension(
        &self,
        prior: &Self,
    ) -> Result<(), crate::ControlError> {
        self.ensure_valid()?;
        let revision = prior.revision.checked_add(1);
        let immutable = self.tenant == prior.tenant
            && self.operation_id == prior.operation_id
            && self.mutation == prior.mutation;
        let immutable = immutable && self.plan == prior.plan;
        let errors_extend = self.errors.starts_with(&prior.errors);
        let evidence_extends = self.evidence.extends(&prior.evidence);
        let phase_extends = self.phase == prior.phase
            || (self.phase == SplitOperationPhase::Failed
                && prior.phase != SplitOperationPhase::Completed)
            || (prior.phase == SplitOperationPhase::Failed
                && self.phase == SplitOperationPhase::Running
                && self.attempts > prior.attempts)
            || progress_rank(self.phase).is_some_and(|next| {
                progress_rank(prior.phase).is_some_and(|previous| next > previous)
            });
        if immutable
            && revision == Some(self.revision)
            && phase_extends
            && prior.phase != SplitOperationPhase::Completed
            && self.attempts >= prior.attempts
            && errors_extend
            && evidence_extends
        {
            Ok(())
        } else {
            Err(crate::ControlError::SplitOperationConflict {
                tenant: prior.tenant.clone(),
                operation_id: prior.operation_id.clone(),
                reason: "update is not a monotone extension".to_string(),
            })
        }
    }
}

impl MoveRangeState {
    fn ensure_intent_valid(&self) -> Result<(), crate::ControlError> {
        let replacement = &self.replacement;
        if replacement.range_id == self.source_range_id
            || replacement.endpoint.is_empty()
            || replacement.wal_generation <= self.predecessor_generation
            || replacement.lifecycle != RangeLifecycle::Serving
            || replacement.retirement.is_some()
        {
            return Err(crate::ControlError::invalid_field(
                "split_operation.mutation.move_range",
                "move replacement must be fresh, serving, generation-fenced, and distinct",
            ));
        }
        Ok(())
    }
}

impl SplitOperationRecord {
    #[must_use]
    pub fn source_range_id(&self) -> u32 {
        match &self.mutation {
            RangeMutationPlan::Split { split } => split.source_range_id,
            RangeMutationPlan::Move { move_range } => move_range.source_range_id,
        }
    }

    #[must_use]
    pub fn predecessor_generation(&self) -> u64 {
        match &self.mutation {
            RangeMutationPlan::Split { split } => split.predecessor_generation,
            RangeMutationPlan::Move { move_range } => move_range.predecessor_generation,
        }
    }

    #[must_use]
    pub fn split_intent(&self) -> Option<&SplitState> {
        match &self.mutation {
            RangeMutationPlan::Split { split } => Some(split),
            RangeMutationPlan::Move { .. } => None,
        }
    }

    #[must_use]
    pub fn move_intent(&self) -> Option<&MoveRangeState> {
        match &self.mutation {
            RangeMutationPlan::Move { move_range } => Some(move_range),
            RangeMutationPlan::Split { .. } => None,
        }
    }
}

impl SplitOperationPhase {
    /// Whether registry authority must already expose the sealed target layout.
    #[must_use]
    pub const fn expects_target_registry_layout(self) -> bool {
        matches!(
            self,
            Self::LayoutPublished | Self::Retiring | Self::Resuming | Self::Completed
        )
    }

    /// Whether this phase lies in one explicit durable progress window.
    #[must_use]
    pub const fn is_between(self, first: Self, last: Self) -> bool {
        match (
            progress_rank(self),
            progress_rank(first),
            progress_rank(last),
        ) {
            (Some(value), Some(first), Some(last)) => value >= first && value <= last,
            _ => false,
        }
    }
}

impl SplitOperationPlan {
    fn ensure_valid(&self, operation: &SplitOperationRecord) -> Result<(), crate::ControlError> {
        let source_range_id = operation.source_range_id();
        let predecessor_generation = operation.predecessor_generation();
        if self.source_record_version == 0 || self.routing_table_id == 0 {
            return Err(crate::ControlError::invalid_field(
                "split_operation.plan",
                "record version and routing table id must be positive",
            ));
        }
        ensure_range_layout_valid(&self.current_layout)?;
        ensure_range_layout_valid(&self.target_layout)?;
        let source_index = self
            .current_layout
            .iter()
            .position(|range| range.range_id == source_range_id);
        let mut expected_target = self.current_layout.clone();
        let target_matches = source_index.is_some_and(|source_index| match &operation.mutation {
            RangeMutationPlan::Split { split } => {
                expected_target.splice(
                    source_index..=source_index,
                    [split.left.clone(), split.right.clone()],
                );
                self.target_layout == expected_target
            }
            RangeMutationPlan::Move { move_range } => {
                let replacement = &move_range.replacement;
                if replacement.end_key != self.current_layout[source_index].end_key {
                    return false;
                }
                expected_target[source_index] = replacement.clone();
                self.target_layout == expected_target
            }
        });
        if source_index
            .is_none_or(|index| self.current_layout[index].wal_generation != predecessor_generation)
            || !target_matches
        {
            return Err(crate::ControlError::invalid_field(
                "split_operation.plan",
                "source or successor layout differs from split intent",
            ));
        }
        Ok(())
    }
}

impl SplitOperationEvidence {
    fn ensure_valid(&self) -> Result<(), crate::ControlError> {
        if self.manifest_key.as_ref().is_some_and(String::is_empty)
            || self.tail_sha256.as_ref().is_some_and(String::is_empty)
            || self.marker_digest.as_ref().is_some_and(String::is_empty)
            || self.covered_offset.is_some_and(|offset| offset < 0)
            || self.barrier_offset.is_some_and(|offset| offset < 0)
            || self.barrier_offset.is_some() && self.covered_offset.is_none()
        {
            return Err(crate::ControlError::invalid_field(
                "split_operation.evidence",
                "receipt evidence is empty, negative, or incomplete",
            ));
        }
        Ok(())
    }

    fn extends(&self, prior: &Self) -> bool {
        fn field_extends<T: PartialEq>(next: &Option<T>, prior: &Option<T>) -> bool {
            prior
                .as_ref()
                .is_none_or(|prior| next.as_ref() == Some(prior))
        }
        field_extends(&self.manifest_key, &prior.manifest_key)
            && field_extends(&self.covered_offset, &prior.covered_offset)
            && field_extends(&self.barrier_offset, &prior.barrier_offset)
            && field_extends(&self.tail_sha256, &prior.tail_sha256)
            && field_extends(&self.marker_digest, &prior.marker_digest)
    }
}

const fn progress_rank(phase: SplitOperationPhase) -> Option<u8> {
    match phase {
        SplitOperationPhase::Initiated => Some(0),
        SplitOperationPhase::Running => Some(1),
        SplitOperationPhase::Checkpointed => Some(2),
        SplitOperationPhase::Paused => Some(3),
        SplitOperationPhase::Restored => Some(4),
        SplitOperationPhase::Activated => Some(5),
        SplitOperationPhase::LayoutPublished => Some(6),
        SplitOperationPhase::Retiring => Some(7),
        SplitOperationPhase::Resuming => Some(8),
        SplitOperationPhase::Completed => Some(9),
        SplitOperationPhase::Failed => None,
    }
}
/// Prefix for the ACL-scoped per-tenant config topic read by the compute.
pub const TENANT_CONFIG_TOPIC_PREFIX: &str = "__gres_cfg.";

const REGISTRY_FORMAT_VERSION: u16 = 1;
const REGISTRY_KEY_MAGIC: u8 = 1;
const TENANT_KEY_TYPE: &str = "TENANT";

/// Validated tenant identifier. Stable across renames if that surface is added.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(String);

/// Validated tenant name. Also serves as the registry compaction key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantName(String);

/// Validated `PostgreSQL` login role name for the tenant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SqlUser(String);

macro_rules! string_newtype_accessors {
    ($ty:ty) => {
        impl $ty {
            /// Return the parsed string value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the wrapper and return the owned string.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_newtype_accessors!(TenantId);
string_newtype_accessors!(TenantName);
string_newtype_accessors!(SqlUser);

impl FromStr for TenantId {
    type Err = ControlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_dns_label(value, "tenant id").map(Self)
    }
}

impl TryFrom<&str> for TenantId {
    type Error = ControlError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl FromStr for TenantName {
    type Err = ControlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_dns_label(value, "tenant name").map(Self)
    }
}

impl TryFrom<&str> for TenantName {
    type Error = ControlError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl FromStr for SqlUser {
    type Err = ControlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_sql_user(value).map(Self)
    }
}

impl TryFrom<&str> for SqlUser {
    type Error = ControlError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// Tenant lifecycle state recorded by the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantState {
    /// Tenant compute should be provisioned and routed.
    Active,
    /// WAL parking has fenced resumes while the old WAL topics are being deleted.
    Parking,
    /// Tenant compute exited after a final checkpoint and should route to the activator.
    Suspended,
    /// An activator observed first traffic and requested the controller to scale up compute.
    ResumeRequested,
}

impl TenantState {
    /// Return whether the registry state machine permits this transition.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        if matches!(
            (self, next),
            (Self::Active, Self::Active)
                | (Self::Parking, Self::Parking)
                | (Self::Suspended, Self::Suspended)
                | (Self::ResumeRequested, Self::ResumeRequested)
        ) {
            return true;
        }

        matches!(
            (self, next),
            (Self::Active, Self::Parking | Self::Suspended)
                | (Self::Parking, Self::Suspended)
                | (Self::Suspended, Self::Active | Self::ResumeRequested)
                | (Self::ResumeRequested, Self::Active | Self::Suspended)
        )
    }
}

impl fmt::Display for TenantState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Parking => f.write_str("parking"),
            Self::Suspended => f.write_str("suspended"),
            Self::ResumeRequested => f.write_str("resume_requested"),
        }
    }
}

/// Whole-tenant registry snapshot stored as the value for a [`RegistryKey`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantRecord {
    /// Monotonic per-tenant version. Higher versions win during compaction replay.
    pub record_version: u64,
    /// Stable tenant identifier.
    pub id: TenantId,
    /// Tenant route name and compaction key.
    pub name: TenantName,
    /// Lifecycle state.
    pub state: TenantState,
    /// `PostgreSQL` role used by end-user SQL clients.
    pub sql_user: SqlUser,
    /// `PostgreSQL` `pg_authid` SCRAM verifier; never a plaintext password.
    pub scram_verifier: String,
    /// WAL topic replication factor for the tenant.
    pub wal_replication: i32,
    /// Optional object-store prefix for tenant checkpoints.
    pub bucket_prefix: Option<String>,
    /// Compute endpoint used by activators once a tenant becomes active.
    pub endpoint: Option<String>,
    /// Optional frame threshold for checkpointing.
    pub checkpoint_frames: Option<u64>,
    /// Optional byte threshold for checkpointing.
    pub checkpoint_bytes: Option<u64>,
    /// Idle seconds before G-5 suspension. `None` or `0` means never.
    pub idle_seconds: Option<u64>,
    /// WAL topic generation. Controllers bump this after parking and recreating WAL topics.
    pub wal_generation: u64,
    /// Ordered row-range placement discovered by range computes.
    pub ranges: Vec<RangeLayoutEntry>,
    /// Split predecessors whose old WAL generations are being retired after cutover.
    #[serde(default)]
    pub range_retirements: Vec<RangeRetirementRecord>,
    /// Hash-sharding placement metadata and co-location constraints.
    pub hash_placements: Vec<HashPlacement>,
    /// Optional maximum checkpoint size that remains eligible for suspension.
    pub suspend_max_checkpoint_bytes: Option<u64>,
    /// Final checkpoint manifest that made a suspended tenant safe to park.
    pub final_checkpoint: Option<FinalCheckpoint>,
}

/// Registry placement for one row-key range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeLayoutEntry {
    /// Range identifier used in WAL topic and east-west protocol names.
    pub range_id: u32,
    /// Exclusive `(table_id, rowid)` upper bound. `None` marks the final open-ended range.
    pub end_key: Option<RangeBoundary>,
    /// Range-compute endpoint used for forwarding and transaction RPC.
    pub endpoint: String,
    /// Per-range WAL generation. Controllers bump this independently for parking.
    pub wal_generation: u64,
    /// Range-scoped serving lifecycle; absent legacy values decode as serving.
    #[serde(default)]
    pub lifecycle: RangeLifecycle,
    /// Durable predecessor-retirement intent while parking or parked.
    #[serde(default)]
    pub retirement: Option<RangeRetirement>,
}

/// Independent lifecycle for one range placement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeLifecycle {
    #[default]
    Serving,
    Parking,
    Parked,
}

/// Durable, generation-fenced retirement metadata for one predecessor range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeRetirement {
    pub operation_id: String,
    pub retiring_generation: u64,
    pub checkpoint: FinalCheckpoint,
}

/// Durable sidecar for retiring a predecessor without changing successor serving state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeRetirementRecord {
    pub operation_id: String,
    pub source_range_id: u32,
    pub source_generation: u64,
    pub checkpoint: RangeRetirementCheckpoint,
    pub successor_ranges: Vec<(u32, u64)>,
    pub phase: RangeRetirementPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeRetirementCheckpoint {
    pub manifest_key: String,
    pub covered_offset: i64,
    pub barrier_offset: i64,
    pub tail_sha256: String,
    pub marker_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeRetirementPhase {
    Parking,
    Parked,
}

impl RangeLayoutEntry {
    pub fn begin_parking(
        &self,
        operation_id: impl Into<String>,
        expected_generation: u64,
        checkpoint: FinalCheckpoint,
    ) -> Result<Self, ControlError> {
        let operation_id = operation_id.into();
        checkpoint.ensure_valid()?;
        if self.lifecycle != RangeLifecycle::Serving
            || self.wal_generation != expected_generation
            || checkpoint.wal_generation != expected_generation
            || operation_id.is_empty()
        {
            return Err(ControlError::InvalidField {
                field: "ranges.retirement",
                reason: "parking requires serving state, matching generation/checkpoint, and operation id".into(),
            });
        }
        let mut next = self.clone();
        next.lifecycle = RangeLifecycle::Parking;
        next.retirement = Some(RangeRetirement {
            operation_id,
            retiring_generation: expected_generation,
            checkpoint,
        });
        Ok(next)
    }

    pub fn confirm_parked(
        &self,
        operation_id: &str,
        expected_generation: u64,
    ) -> Result<Self, ControlError> {
        let Some(retirement) = &self.retirement else {
            return Err(ControlError::InvalidField {
                field: "ranges.retirement",
                reason: "parking intent is missing".into(),
            });
        };
        if self.lifecycle != RangeLifecycle::Parking
            || retirement.operation_id != operation_id
            || retirement.retiring_generation != expected_generation
        {
            return Err(ControlError::InvalidField {
                field: "ranges.retirement",
                reason: "park confirmation does not match durable intent".into(),
            });
        }
        let mut next = self.clone();
        next.lifecycle = RangeLifecycle::Parked;
        Ok(next)
    }
}

/// Registry-visible hash-sharded table placement metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashPlacement {
    /// Table identifier that owns these bucket intervals.
    pub table_id: u64,
    /// Hash column names in catalog order.
    pub hash_columns: Vec<String>,
    /// Fixed power-of-two bucket count.
    pub bucket_count: u32,
    /// Optional co-location group name.
    pub co_location_group: Option<String>,
}

/// A registry-visible row-key boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RangeBoundary {
    /// Table identifier component of the range boundary.
    pub table_id: u64,
    /// Row identifier component of the range boundary.
    pub rowid: u64,
}

/// Parsed request to split one registry range layout entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeLayoutSplit {
    /// Existing range id that owns `split_key`.
    pub source_range_id: u32,
    /// Generation expected on the predecessor being retired.
    pub predecessor_generation: u64,
    /// Fresh left replacement placement.
    pub left: RangeLayoutEntry,
    /// Fresh right replacement placement.
    pub right: RangeLayoutEntry,
}

impl RangeLayoutSplit {
    fn ensure_intent_valid(&self) -> Result<(), ControlError> {
        if self.left.range_id == self.right.range_id
            || (self.source_range_id == self.left.range_id && self.source_range_id != 0)
            || self.source_range_id == self.right.range_id
        {
            return Err(ControlError::invalid_field(
                "split_operation.split.range_id",
                "two distinct successors must differ from the source range id",
            ));
        }
        if self.left.endpoint.is_empty() || self.right.endpoint.is_empty() {
            return Err(ControlError::invalid_field(
                "split_operation.split.endpoint",
                "successor endpoints must not be empty",
            ));
        }
        if self.left.wal_generation <= self.predecessor_generation
            || self.right.wal_generation <= self.predecessor_generation
        {
            return Err(ControlError::invalid_field(
                "split_operation.split.wal_generation",
                "successor generations must fence the predecessor generation",
            ));
        }
        if self.left.end_key.is_none()
            || self.left.lifecycle != RangeLifecycle::Serving
            || self.right.lifecycle != RangeLifecycle::Serving
            || self.left.retirement.is_some()
            || self.right.retirement.is_some()
        {
            return Err(ControlError::invalid_field(
                "split_operation.split.successors",
                "successors must be fresh serving placements and the left must be bounded",
            ));
        }
        Ok(())
    }
}

/// Durable explicit two-successor split state used by the initiation journal.
pub type SplitState = RangeLayoutSplit;

/// Parsed request to merge two adjacent registry range layout entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeLayoutMerge {
    /// Left adjacent range. This range id survives the merge.
    pub left_range_id: u32,
    /// Right adjacent range. This entry is removed from the layout.
    pub right_range_id: u32,
    /// Merged range-compute endpoint.
    pub merged_endpoint: String,
    /// Merged WAL generation. The stored value is monotonic over both inputs.
    pub merged_wal_generation: u64,
}

/// Parsed range-layout mutation shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeLayoutMutation {
    /// Split one existing range into a source and successor.
    Split(RangeLayoutSplit),
    /// Merge two adjacent ranges into the left range id.
    Merge(RangeLayoutMerge),
}

impl RangeBoundary {
    const MIN: Self = Self {
        table_id: 0,
        rowid: 0,
    };

    /// Build a boundary from table and row identifiers.
    #[must_use]
    pub const fn new(table_id: u64, rowid: u64) -> Self {
        Self { table_id, rowid }
    }

    /// Build the first boundary for a table.
    #[must_use]
    pub const fn table_start(table_id: u64) -> Self {
        Self { table_id, rowid: 0 }
    }
}

/// Registry-visible final checkpoint marker written before a tenant is parked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalCheckpoint {
    /// WAL generation represented by the checkpoint.
    pub wal_generation: u64,
    /// WAL offset covered through by the manifest.
    pub covered_offset: i64,
    /// Object key of the durable manifest.
    pub manifest_key: String,
    /// Total manifest plus part bytes observed by the checkpointer.
    pub total_bytes: u64,
}

impl TenantRecord {
    /// Build a validated tenant record from already-parsed identity fields.
    pub fn new(
        record_version: u64,
        id: TenantId,
        name: TenantName,
        state: TenantState,
        sql_user: SqlUser,
        scram_verifier: String,
        wal_replication: i32,
    ) -> Result<Self, ControlError> {
        let record = Self {
            record_version,
            id,
            name,
            state,
            sql_user,
            scram_verifier,
            wal_replication,
            bucket_prefix: None,
            endpoint: None,
            checkpoint_frames: None,
            checkpoint_bytes: None,
            idle_seconds: None,
            wal_generation: 0,
            ranges: Vec::new(),
            range_retirements: Vec::new(),
            hash_placements: Vec::new(),
            suspend_max_checkpoint_bytes: None,
            final_checkpoint: None,
        };
        record.ensure_valid()?;
        Ok(record)
    }

    /// Parse and validate every cross-field invariant.
    pub fn ensure_valid(&self) -> Result<(), ControlError> {
        parse_dns_label(self.id.as_str(), "tenant id")?;
        parse_dns_label(self.name.as_str(), "tenant name")?;
        parse_sql_user(self.sql_user.as_str())?;
        if self.record_version == 0 {
            return Err(ControlError::invalid_field(
                "record_version",
                "must be greater than zero",
            ));
        }
        if self.wal_replication < 1 {
            return Err(ControlError::invalid_field(
                "wal_replication",
                "must be at least one",
            ));
        }
        if !self.scram_verifier.starts_with("SCRAM-SHA-256$") {
            return Err(ControlError::invalid_field(
                "scram_verifier",
                "must be a PostgreSQL SCRAM-SHA-256 verifier",
            ));
        }
        if self.bucket_prefix.as_deref().is_some_and(str::is_empty) {
            return Err(ControlError::invalid_field(
                "bucket_prefix",
                "must not be empty when present",
            ));
        }
        if self.endpoint.as_deref().is_some_and(str::is_empty) {
            return Err(ControlError::invalid_field(
                "endpoint",
                "must not be empty when present",
            ));
        }
        ensure_range_layout_valid(&self.ranges)?;
        let mut retirement_keys = std::collections::BTreeSet::new();
        for retirement in &self.range_retirements {
            if retirement.operation_id.is_empty()
                || retirement.checkpoint.manifest_key.is_empty()
                || retirement.checkpoint.covered_offset < 0
                || retirement.checkpoint.barrier_offset < retirement.checkpoint.covered_offset
                || retirement.checkpoint.tail_sha256.is_empty()
                || retirement.checkpoint.marker_digest.is_empty()
                || retirement.successor_ranges.is_empty()
                || !retirement_keys.insert((
                    retirement.operation_id.as_str(),
                    retirement.source_range_id,
                    retirement.source_generation,
                ))
                || retirement
                    .successor_ranges
                    .iter()
                    .any(|(range_id, generation)| {
                        !self.ranges.iter().any(|range| {
                            range.range_id == *range_id && range.wal_generation == *generation
                        })
                    })
            {
                return Err(ControlError::invalid_field(
                    "range_retirements",
                    "retirement identity, evidence, or successor topology is invalid",
                ));
            }
        }
        ensure_hash_placements_valid(&self.hash_placements)?;
        if let Some(checkpoint) = &self.final_checkpoint {
            checkpoint.ensure_valid()?;
        }
        Ok(())
    }

    /// Return a record advanced to `next` with a bumped record version.
    pub fn transition_to(mut self, next: TenantState) -> Result<Self, ControlError> {
        if !self.state.can_transition_to(next) {
            return Err(ControlError::InvalidLifecycleTransition {
                from: self.state,
                to: next,
            });
        }

        if self.state == next {
            return Ok(self);
        }

        self.record_version = self.next_record_version()?;
        self.state = next;
        Ok(self)
    }

    /// Return a record marked active, including the endpoint activators should dial.
    pub fn mark_active(mut self, endpoint: impl Into<String>) -> Result<Self, ControlError> {
        let endpoint = endpoint.into();
        if endpoint.is_empty() {
            return Err(ControlError::invalid_field(
                "endpoint",
                "must not be empty when present",
            ));
        }

        let was_active = self.state == TenantState::Active;
        self = self.transition_to(TenantState::Active)?;
        if was_active && self.endpoint.as_deref() != Some(endpoint.as_str()) {
            self.record_version = self.next_record_version()?;
        }
        self.endpoint = Some(endpoint);
        self.ensure_valid()?;
        Ok(self)
    }

    /// Return a record marked suspended.
    pub fn mark_suspended(mut self) -> Result<Self, ControlError> {
        let clearing_endpoint_without_state_change =
            self.state == TenantState::Suspended && self.endpoint.is_some();
        self = self.transition_to(TenantState::Suspended)?;
        if clearing_endpoint_without_state_change {
            self.record_version = self.next_record_version()?;
        }
        self.endpoint = None;
        Ok(self)
    }

    /// Return a record marked suspended with the durable checkpoint that permits parking.
    pub fn mark_suspended_after_checkpoint(
        mut self,
        checkpoint: FinalCheckpoint,
    ) -> Result<Self, ControlError> {
        checkpoint.ensure_valid()?;
        let was_suspended = self.state == TenantState::Suspended;
        self = self.mark_suspended()?;
        if was_suspended && self.final_checkpoint.as_ref() != Some(&checkpoint) {
            self.record_version = self.next_record_version()?;
        }
        self.final_checkpoint = Some(checkpoint);
        self.ensure_valid()?;
        Ok(self)
    }

    /// Return a record with an activator resume request, or unchanged unless parking is complete.
    pub fn request_resume(self) -> Result<Self, ControlError> {
        if self.state != TenantState::Suspended {
            return Ok(self);
        }

        self.transition_to(TenantState::ResumeRequested)
    }

    /// Return a record with WAL generation advanced monotonically.
    pub fn with_wal_generation(mut self, generation: u64) -> Result<Self, ControlError> {
        let next_generation = self.wal_generation.max(generation);
        if next_generation == self.wal_generation {
            return Ok(self);
        }
        self.wal_generation = next_generation;
        self.record_version = self.next_record_version()?;
        Ok(self)
    }

    /// Return a record with the full range layout replaced.
    pub fn with_range_layout(
        mut self,
        ranges: Vec<RangeLayoutEntry>,
    ) -> Result<Self, ControlError> {
        ensure_range_layout_valid(&ranges)?;
        if self.ranges == ranges {
            return Ok(self);
        }
        self.ranges = ranges;
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Atomically publish an exact successor layout and a predecessor-retirement intent.
    pub fn publish_split_target_with_retirement(
        mut self,
        operation_id: impl Into<String>,
        source_range_id: u32,
        source_generation: u64,
        checkpoint: RangeRetirementCheckpoint,
        target_layout: Vec<RangeLayoutEntry>,
    ) -> Result<Self, ControlError> {
        let operation_id = operation_id.into();
        ensure_range_layout_valid(&target_layout)?;
        if operation_id.is_empty()
            || checkpoint.manifest_key.is_empty()
            || checkpoint.covered_offset < 0
            || checkpoint.barrier_offset < checkpoint.covered_offset
            || checkpoint.tail_sha256.is_empty()
            || checkpoint.marker_digest.is_empty()
        {
            return Err(ControlError::invalid_field(
                "range_retirements",
                "operation id and complete receipt evidence must identify the predecessor",
            ));
        }
        if let Some(existing) = self.range_retirements.iter().find(|retirement| {
            retirement.operation_id == operation_id
                && retirement.source_range_id == source_range_id
                && retirement.source_generation == source_generation
        }) {
            if self.ranges == target_layout && existing.phase == RangeRetirementPhase::Parking {
                return Ok(self);
            }
            return Err(ControlError::invalid_field(
                "range_retirements",
                "operation identity was reused with different cutover state",
            ));
        }
        let successor_ranges = target_layout
            .iter()
            .map(|range| (range.range_id, range.wal_generation))
            .collect();
        self.ranges = target_layout;
        self.range_retirements.push(RangeRetirementRecord {
            operation_id,
            source_range_id,
            source_generation,
            checkpoint,
            successor_ranges,
            phase: RangeRetirementPhase::Parking,
        });
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Confirm that one exact predecessor WAL generation is absent.
    pub fn confirm_split_predecessor_parked(
        mut self,
        operation_id: &str,
        source_range_id: u32,
        source_generation: u64,
    ) -> Result<Self, ControlError> {
        let retirement = self
            .range_retirements
            .iter_mut()
            .find(|retirement| {
                retirement.operation_id == operation_id
                    && retirement.source_range_id == source_range_id
                    && retirement.source_generation == source_generation
            })
            .ok_or_else(|| {
                ControlError::invalid_field("range_retirements", "retirement is missing")
            })?;
        if retirement.phase == RangeRetirementPhase::Parked {
            return Ok(self);
        }
        retirement.phase = RangeRetirementPhase::Parked;
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Return a record with hash placement/co-location metadata replaced.
    pub fn with_hash_placements(
        mut self,
        placements: Vec<HashPlacement>,
    ) -> Result<Self, ControlError> {
        ensure_hash_placements_valid(&placements)?;
        if self.hash_placements == placements {
            return Ok(self);
        }
        self.hash_placements = placements;
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Return a record with one range WAL generation advanced monotonically.
    pub fn with_range_wal_generation(
        mut self,
        range_id: u32,
        generation: u64,
    ) -> Result<Self, ControlError> {
        let Some(index) = self
            .ranges
            .iter()
            .position(|range| range.range_id == range_id)
        else {
            return Err(ControlError::invalid_field(
                "ranges",
                "range id is not in layout",
            ));
        };
        if self.ranges[index].lifecycle != RangeLifecycle::Serving {
            return Err(ControlError::invalid_field(
                "ranges.lifecycle",
                "only a serving range may advance generation",
            ));
        }
        let next_generation = self.ranges[index].wal_generation.max(generation);
        if next_generation == self.ranges[index].wal_generation {
            return Ok(self);
        }
        self.ranges[index].wal_generation = next_generation;
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Persist a generation-fenced parking intent for one range.
    pub fn begin_range_parking(
        mut self,
        range_id: u32,
        operation_id: impl Into<String>,
        expected_generation: u64,
        checkpoint: FinalCheckpoint,
    ) -> Result<Self, ControlError> {
        let range = self
            .ranges
            .iter_mut()
            .find(|range| range.range_id == range_id)
            .ok_or_else(|| ControlError::invalid_field("ranges.range_id", "range is missing"))?;
        *range = range.begin_parking(operation_id, expected_generation, checkpoint)?;
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Confirm one range parked after its retiring WAL generation is absent.
    pub fn confirm_range_parked(
        mut self,
        range_id: u32,
        operation_id: &str,
        expected_generation: u64,
    ) -> Result<Self, ControlError> {
        let range = self
            .ranges
            .iter_mut()
            .find(|range| range.range_id == range_id)
            .ok_or_else(|| ControlError::invalid_field("ranges.range_id", "range is missing"))?;
        *range = range.confirm_parked(operation_id, expected_generation)?;
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Return a record whose layout splits one row-key interval into two ranges.
    pub fn split_range_layout(mut self, split: RangeLayoutSplit) -> Result<Self, ControlError> {
        if split.left.range_id == split.right.range_id
            || (split.source_range_id == split.left.range_id && split.source_range_id != 0)
            || split.source_range_id == split.right.range_id
        {
            return Err(ControlError::invalid_field(
                "ranges.range_id",
                "two distinct successors must differ from the source range id",
            ));
        }
        if self.ranges.iter().any(|range| {
            (range.range_id == split.left.range_id && range.range_id != split.source_range_id)
                || range.range_id == split.right.range_id
        }) {
            return Err(ControlError::invalid_field(
                "ranges.range_id",
                "successor range id is already in layout",
            ));
        }

        let Some(index) = self
            .ranges
            .iter()
            .position(|range| range.range_id == split.source_range_id)
        else {
            return Err(ControlError::invalid_field(
                "ranges.range_id",
                "source range id is not in layout",
            ));
        };
        if self.ranges[index].lifecycle != RangeLifecycle::Serving {
            return Err(ControlError::invalid_field(
                "ranges.lifecycle",
                "only a serving range may split",
            ));
        }
        if self.ranges[index].wal_generation != split.predecessor_generation {
            return Err(ControlError::invalid_field(
                "ranges.wal_generation",
                "predecessor generation does not match the serving layout",
            ));
        }
        if split.left.wal_generation <= split.predecessor_generation
            || split.right.wal_generation <= split.predecessor_generation
        {
            return Err(ControlError::invalid_field(
                "ranges.wal_generation",
                "successor generations must fence the predecessor generation",
            ));
        }
        if split.left.lifecycle != RangeLifecycle::Serving
            || split.right.lifecycle != RangeLifecycle::Serving
            || split.left.retirement.is_some()
            || split.right.retirement.is_some()
        {
            return Err(ControlError::invalid_field(
                "ranges.lifecycle",
                "successors must be fresh serving placements",
            ));
        }
        let previous_end = index
            .checked_sub(1)
            .and_then(|previous| self.ranges[previous].end_key)
            .unwrap_or(RangeBoundary::MIN);
        let split_key = split.left.end_key.ok_or_else(|| {
            ControlError::invalid_field("ranges.end_key", "left successor must be bounded")
        })?;
        ensure_split_key_inside_range(previous_end, self.ranges[index].end_key, split_key)?;

        let source_end = self.ranges[index].end_key;
        if split.right.end_key != source_end {
            return Err(ControlError::invalid_field(
                "ranges.end_key",
                "right successor must preserve the predecessor end boundary",
            ));
        }
        self.ranges.splice(index..=index, [split.left, split.right]);
        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Return a record whose layout merges adjacent ranges into the left id.
    pub fn merge_range_layout(mut self, merge: RangeLayoutMerge) -> Result<Self, ControlError> {
        if merge.merged_endpoint.is_empty() {
            return Err(ControlError::invalid_field(
                "ranges.endpoint",
                "must not be empty",
            ));
        }
        if merge.left_range_id == merge.right_range_id {
            return Err(ControlError::invalid_field(
                "ranges.range_id",
                "merged range ids must differ",
            ));
        }

        let Some(left_index) = self.ranges.windows(2).position(|pair| {
            pair[0].range_id == merge.left_range_id && pair[1].range_id == merge.right_range_id
        }) else {
            return Err(ControlError::invalid_field(
                "ranges.range_id",
                "ranges must be adjacent in left-to-right order",
            ));
        };

        let right_index = left_index + 1;
        if self.ranges[left_index].lifecycle != RangeLifecycle::Serving
            || self.ranges[right_index].lifecycle != RangeLifecycle::Serving
        {
            return Err(ControlError::invalid_field(
                "ranges.lifecycle",
                "only serving ranges may merge",
            ));
        }
        let right = self.ranges.remove(right_index);
        let left = &mut self.ranges[left_index];
        left.end_key = right.end_key;
        left.endpoint = merge.merged_endpoint;
        left.wal_generation = left
            .wal_generation
            .max(right.wal_generation)
            .max(merge.merged_wal_generation);

        self.record_version = self.next_record_version()?;
        self.ensure_valid()?;
        Ok(self)
    }

    /// Return a record with one range-layout mutation applied.
    pub fn mutate_range_layout(self, mutation: RangeLayoutMutation) -> Result<Self, ControlError> {
        match mutation {
            RangeLayoutMutation::Split(split) => self.split_range_layout(split),
            RangeLayoutMutation::Merge(merge) => self.merge_range_layout(merge),
        }
    }

    fn next_record_version(&self) -> Result<u64, ControlError> {
        self.record_version.checked_add(1).ok_or_else(|| {
            ControlError::invalid_field("record_version", "must not overflow when bumped")
        })
    }
}

fn ensure_split_key_inside_range(
    range_start: RangeBoundary,
    source_end: Option<RangeBoundary>,
    split_key: RangeBoundary,
) -> Result<(), ControlError> {
    if split_key <= range_start {
        return Err(ControlError::invalid_field(
            "ranges.end_key",
            "split key must be greater than the source range start",
        ));
    }
    if source_end.is_some_and(|source_end| split_key >= source_end) {
        return Err(ControlError::invalid_field(
            "ranges.end_key",
            "split key must be less than the source range end",
        ));
    }
    Ok(())
}

fn ensure_range_layout_valid(ranges: &[RangeLayoutEntry]) -> Result<(), ControlError> {
    let mut seen = std::collections::BTreeSet::new();
    let mut previous_end = RangeBoundary::MIN;
    for (index, range) in ranges.iter().enumerate() {
        if !seen.insert(range.range_id) {
            return Err(ControlError::invalid_field(
                "ranges.range_id",
                "must be unique",
            ));
        }
        if range.endpoint.is_empty() {
            return Err(ControlError::invalid_field(
                "ranges.endpoint",
                "must not be empty",
            ));
        }
        match (&range.lifecycle, &range.retirement) {
            (RangeLifecycle::Serving, None) => {}
            (RangeLifecycle::Parking | RangeLifecycle::Parked, Some(retirement))
                if !retirement.operation_id.is_empty()
                    && retirement.retiring_generation == range.wal_generation
                    && retirement.checkpoint.wal_generation == range.wal_generation =>
            {
                retirement.checkpoint.ensure_valid()?;
            }
            _ => {
                return Err(ControlError::invalid_field(
                    "ranges.retirement",
                    "must match lifecycle, operation id, and WAL generation",
                ));
            }
        }
        if range.end_key.is_none() && index + 1 != ranges.len() {
            return Err(ControlError::invalid_field(
                "ranges.end_key",
                "only the final range may be open-ended",
            ));
        }
        if let Some(current) = range.end_key {
            if current <= previous_end {
                return Err(ControlError::invalid_field(
                    "ranges.end_key",
                    "must increase monotonically",
                ));
            }
            previous_end = current;
            continue;
        }

        if index + 1 != ranges.len() {
            return Err(ControlError::invalid_field(
                "ranges.end_key",
                "only the final range may be open-ended",
            ));
        }
    }
    Ok(())
}

fn ensure_hash_placements_valid(placements: &[HashPlacement]) -> Result<(), ControlError> {
    let mut seen_tables = std::collections::BTreeSet::new();
    let mut co_location_buckets = BTreeMap::new();
    for placement in placements {
        if !seen_tables.insert(placement.table_id) {
            return Err(ControlError::invalid_field(
                "hash_placements.table_id",
                "must be unique",
            ));
        }
        if placement.table_id == 0 {
            return Err(ControlError::invalid_field(
                "hash_placements.table_id",
                "must be greater than zero",
            ));
        }
        if placement.hash_columns.is_empty() {
            return Err(ControlError::invalid_field(
                "hash_placements.hash_columns",
                "must not be empty",
            ));
        }
        if placement.hash_columns.iter().any(String::is_empty) {
            return Err(ControlError::invalid_field(
                "hash_placements.hash_columns",
                "must not contain empty names",
            ));
        }
        if placement.bucket_count == 0 || !placement.bucket_count.is_power_of_two() {
            return Err(ControlError::invalid_field(
                "hash_placements.bucket_count",
                "must be a power of two",
            ));
        }
        if placement
            .co_location_group
            .as_deref()
            .is_some_and(str::is_empty)
        {
            return Err(ControlError::invalid_field(
                "hash_placements.co_location_group",
                "must not be empty when present",
            ));
        }
        if let Some(group) = &placement.co_location_group {
            let previous = co_location_buckets
                .entry(group.clone())
                .or_insert(placement.bucket_count);
            if *previous != placement.bucket_count {
                return Err(ControlError::invalid_field(
                    "hash_placements.co_location_group",
                    "co-located hash placements must use the same bucket count",
                ));
            }
        }
    }
    Ok(())
}

impl FinalCheckpoint {
    /// Validate checkpoint marker invariants.
    pub fn ensure_valid(&self) -> Result<(), ControlError> {
        if self.covered_offset < 0 {
            return Err(ControlError::invalid_field(
                "final_checkpoint.covered_offset",
                "must be non-negative",
            ));
        }
        if self.manifest_key.is_empty() {
            return Err(ControlError::invalid_field(
                "final_checkpoint.manifest_key",
                "must not be empty",
            ));
        }
        if self.total_bytes == 0 {
            return Err(ControlError::invalid_field(
                "final_checkpoint.total_bytes",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Compaction key for one tenant record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryKey {
    /// Always `TENANT`.
    pub keytype: String,
    /// Tenant name, used as the Kafka compacted key identity.
    pub name: TenantName,
    /// Greenfield key magic.
    pub magic: u8,
}

impl RegistryKey {
    /// Build a tenant registry key.
    #[must_use]
    pub fn new(name: TenantName) -> Self {
        Self {
            keytype: TENANT_KEY_TYPE.to_string(),
            name,
            magic: REGISTRY_KEY_MAGIC,
        }
    }

    /// Parse and validate raw key bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
        let key: Self =
            serde_json::from_slice(bytes).map_err(|e| ControlError::InvalidKey(e.to_string()))?;
        if key.keytype != TENANT_KEY_TYPE {
            return Err(ControlError::InvalidKey(format!(
                "unsupported keytype {}",
                key.keytype
            )));
        }
        if key.magic != REGISTRY_KEY_MAGIC {
            return Err(ControlError::InvalidKey(format!(
                "unsupported magic {}",
                key.magic
            )));
        }
        parse_dns_label(key.name.as_str(), "tenant name")
            .map_err(|e| ControlError::InvalidKey(e.to_string()))?;
        Ok(key)
    }

    /// Encode the key to bytes.
    pub fn encode(&self) -> Result<Vec<u8>, ControlError> {
        Ok(serde_json::to_vec(self)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TenantRecordEnvelope {
    format_version: u16,
    record: TenantRecord,
}

/// Build the registry key bytes for a tenant name.
pub fn tenant_registry_key(name: &TenantName) -> Result<Vec<u8>, ControlError> {
    RegistryKey::new(name.clone()).encode()
}

/// Return the ACL-scoped per-tenant config topic name for a compute.
#[must_use]
pub fn tenant_config_topic(name: &TenantName) -> String {
    format!("{TENANT_CONFIG_TOPIC_PREFIX}{name}")
}

/// Encode a whole tenant record as bytes for `__gres_cfg.<tenant>`.
pub fn encode_tenant_config_record(record: &TenantRecord) -> Result<Vec<u8>, ControlError> {
    record.ensure_valid()?;
    let envelope = TenantRecordEnvelope {
        format_version: REGISTRY_FORMAT_VERSION,
        record: record.clone(),
    };
    Ok(serde_json::to_vec(&envelope)?)
}

/// Decode a per-tenant config topic value.
pub fn decode_tenant_config_record(value: &[u8]) -> Result<TenantRecord, ControlError> {
    decode_registry_record(value)
}

/// Encode a whole tenant record as `(key, value)` bytes for `__gres_tenants`.
pub fn encode_registry_record(record: &TenantRecord) -> Result<(Vec<u8>, Vec<u8>), ControlError> {
    record.ensure_valid()?;
    let key = tenant_registry_key(&record.name)?;
    let envelope = TenantRecordEnvelope {
        format_version: REGISTRY_FORMAT_VERSION,
        record: record.clone(),
    };
    Ok((key, serde_json::to_vec(&envelope)?))
}

/// Decode a registry value. Tombstones are represented outside the value as `None`.
pub fn decode_registry_record(value: &[u8]) -> Result<TenantRecord, ControlError> {
    let envelope: TenantRecordEnvelope =
        serde_json::from_slice(value).map_err(|e| ControlError::InvalidValue(e.to_string()))?;
    if envelope.format_version != REGISTRY_FORMAT_VERSION {
        return Err(ControlError::InvalidValue(format!(
            "unsupported format_version {}",
            envelope.format_version
        )));
    }
    envelope.record.ensure_valid()?;
    Ok(envelope.record)
}

fn parse_dns_label(value: &str, field: &'static str) -> Result<String, ControlError> {
    if value.is_empty() {
        return Err(ControlError::invalid_field(field, "must not be empty"));
    }
    if value.len() > 63 {
        return Err(ControlError::invalid_field(
            field,
            "must be at most 63 bytes",
        ));
    }
    if !value.bytes().all(is_dns_label_byte) {
        return Err(ControlError::invalid_field(
            field,
            "must contain only lowercase ASCII letters, digits, or hyphens",
        ));
    }
    if value.starts_with('-') || value.ends_with('-') {
        return Err(ControlError::invalid_field(
            field,
            "must start and end with a letter or digit",
        ));
    }
    Ok(value.to_string())
}

fn parse_sql_user(value: &str) -> Result<String, ControlError> {
    if value.is_empty() {
        return Err(ControlError::invalid_field("sql_user", "must not be empty"));
    }
    if value.len() > 63 {
        return Err(ControlError::invalid_field(
            "sql_user",
            "must be at most 63 bytes",
        ));
    }
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(ControlError::invalid_field("sql_user", "must not be empty"));
    };
    if !is_sql_user_first_byte(first) {
        return Err(ControlError::invalid_field(
            "sql_user",
            "must start with an ASCII letter or underscore",
        ));
    }
    if !bytes.all(is_sql_user_byte) {
        return Err(ControlError::invalid_field(
            "sql_user",
            "must contain only ASCII letters, digits, or underscores",
        ));
    }
    Ok(value.to_string())
}

fn is_dns_label_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
}

fn is_sql_user_first_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_sql_user_byte(byte: u8) -> bool {
    is_sql_user_first_byte(byte) || byte.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    static_assertions::assert_not_impl_any!(SplitOperationPhase: PartialOrd, Ord);

    fn record(version: u64) -> TenantRecord {
        TenantRecord::new(
            version,
            TenantId::try_from("tenant-a").unwrap(),
            TenantName::try_from("tenant-a").unwrap(),
            TenantState::Active,
            SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            3,
        )
        .unwrap()
    }

    fn split_intent() -> RangeLayoutSplit {
        RangeLayoutSplit {
            source_range_id: 1,
            predecessor_generation: 4,
            left: RangeLayoutEntry {
                range_id: 2,
                end_key: Some(RangeBoundary::new(7, 50)),
                endpoint: "left:7432".into(),
                wal_generation: 5,
                lifecycle: RangeLifecycle::Serving,
                retirement: None,
            },
            right: RangeLayoutEntry {
                range_id: 3,
                end_key: None,
                endpoint: "right:7432".into(),
                wal_generation: 5,
                lifecycle: RangeLifecycle::Serving,
                retirement: None,
            },
        }
    }

    #[test]
    fn split_operation_journal_tracks_control_phases_and_retry() {
        let initiated = SplitOperationRecord::new(
            TenantName::try_from("tenant-a").unwrap(),
            "split-phases",
            split_intent(),
        )
        .unwrap();
        let running = initiated
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        let checkpointed = running
            .advance(SplitOperationPhase::Checkpointed, 1, None)
            .unwrap();
        let paused = checkpointed
            .advance(SplitOperationPhase::Paused, 1, None)
            .unwrap();
        let failed = paused
            .advance(
                SplitOperationPhase::Failed,
                1,
                Some("operator restart".into()),
            )
            .unwrap();

        assert!(
            failed
                .advance(SplitOperationPhase::Running, 2, None)
                .is_ok()
        );
        assert!(
            paused
                .advance(SplitOperationPhase::Running, 1, None)
                .is_err()
        );
    }

    #[test]
    fn split_operation_progress_rank_is_total_and_contiguous() {
        let phases = [
            SplitOperationPhase::Initiated,
            SplitOperationPhase::Running,
            SplitOperationPhase::Checkpointed,
            SplitOperationPhase::Paused,
            SplitOperationPhase::Restored,
            SplitOperationPhase::Activated,
            SplitOperationPhase::LayoutPublished,
            SplitOperationPhase::Retiring,
            SplitOperationPhase::Resuming,
            SplitOperationPhase::Completed,
        ];
        assert!(
            phases
                .iter()
                .enumerate()
                .all(|(rank, phase)| { progress_rank(*phase) == u8::try_from(rank).ok() })
        );
    }

    #[test]
    fn move_operation_seals_one_exact_replacement_and_rejects_unknown_kind() {
        let replacement = RangeLayoutEntry {
            range_id: 9,
            end_key: None,
            endpoint: "replacement:7443".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let operation = SplitOperationRecord::new_move(
            TenantName::try_from("tenant-a").unwrap(),
            "move-1",
            0,
            4,
            replacement.clone(),
        )
        .unwrap()
        .with_plan(SplitOperationPlan {
            source_record_version: 9,
            source_map_epoch: 9,
            routing_table_id: 7,
            current_layout: vec![RangeLayoutEntry {
                range_id: 0,
                end_key: None,
                endpoint: "source:7443".into(),
                wal_generation: 4,
                lifecycle: RangeLifecycle::Serving,
                retirement: None,
            }],
            target_layout: vec![replacement],
        })
        .unwrap();
        assert!(matches!(operation.mutation, RangeMutationPlan::Move { .. }));
        let mut json = serde_json::to_value(&operation).unwrap();
        assert!(json.get("split").is_none());
        assert_eq!(json["mutation"]["move_range"]["replacement"]["range_id"], 9);
        json["mutation"]["kind"] = serde_json::json!("future_kind");
        assert!(serde_json::from_value::<SplitOperationRecord>(json).is_err());
    }

    #[test]
    fn legacy_split_json_decodes_to_authoritative_typed_payload() {
        let operation = SplitOperationRecord::new(
            TenantName::try_from("tenant-a").unwrap(),
            "legacy-split",
            split_intent(),
        )
        .unwrap();
        let mut legacy = serde_json::to_value(&operation).unwrap();
        let split = legacy["mutation"]["split"].take();
        legacy.as_object_mut().unwrap().remove("mutation");
        legacy["split"] = split;

        let decoded: SplitOperationRecord = serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded, operation);
        let encoded = serde_json::to_value(decoded).unwrap();
        assert!(encoded.get("split").is_none());
        assert_eq!(encoded["mutation"]["kind"], "split");
    }

    #[test]
    fn sealed_move_plan_rejects_unrelated_edits_boundary_changes_and_reordering() {
        let coordinator = RangeLayoutEntry {
            range_id: 0,
            end_key: Some(RangeBoundary::new(7, 0)),
            endpoint: "coordinator:7443".into(),
            wal_generation: 3,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let source = RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "source:7443".into(),
            wal_generation: 4,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let replacement = RangeLayoutEntry {
            range_id: 9,
            end_key: source.end_key,
            endpoint: "replacement:7443".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let operation = || {
            SplitOperationRecord::new_move(
                TenantName::try_from("tenant-a").unwrap(),
                "move-exact",
                source.range_id,
                source.wal_generation,
                replacement.clone(),
            )
            .unwrap()
        };
        let plan = SplitOperationPlan {
            source_record_version: 9,
            source_map_epoch: 9,
            routing_table_id: 7,
            current_layout: vec![coordinator.clone(), source.clone()],
            target_layout: vec![coordinator.clone(), replacement.clone()],
        };
        operation().with_plan(plan.clone()).unwrap();

        for mutate in [
            |target: &mut Vec<RangeLayoutEntry>| target[0].endpoint.push_str("-forged"),
            |target: &mut Vec<RangeLayoutEntry>| target[0].wal_generation += 1,
            |target: &mut Vec<RangeLayoutEntry>| target[0].lifecycle = RangeLifecycle::Parking,
            |target: &mut Vec<RangeLayoutEntry>| target.swap(0, 1),
            |target: &mut Vec<RangeLayoutEntry>| {
                target[1].end_key = Some(RangeBoundary::new(8, 0));
            },
        ] {
            let mut forged = plan.clone();
            mutate(&mut forged.target_layout);
            assert!(operation().with_plan(forged).is_err());
        }
    }

    #[test]
    fn sealed_split_plan_rejects_any_unrelated_layout_edit() {
        let coordinator = RangeLayoutEntry {
            range_id: 0,
            end_key: Some(RangeBoundary::new(7, 0)),
            endpoint: "coordinator:7443".into(),
            wal_generation: 3,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let source = RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "source:7443".into(),
            wal_generation: 4,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let left = RangeLayoutEntry {
            range_id: 2,
            end_key: Some(RangeBoundary::new(7, 50)),
            endpoint: "left:7443".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let right = RangeLayoutEntry {
            range_id: 3,
            end_key: None,
            endpoint: "right:7443".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let operation = || {
            SplitOperationRecord::new(
                TenantName::try_from("tenant-a").unwrap(),
                "split-exact",
                RangeLayoutSplit {
                    source_range_id: source.range_id,
                    predecessor_generation: source.wal_generation,
                    left: left.clone(),
                    right: right.clone(),
                },
            )
            .unwrap()
        };
        let plan = SplitOperationPlan {
            source_record_version: 9,
            source_map_epoch: 9,
            routing_table_id: 7,
            current_layout: vec![coordinator.clone(), source.clone()],
            target_layout: vec![coordinator, left.clone(), right.clone()],
        };
        operation().with_plan(plan.clone()).unwrap();
        for mutate in [
            |target: &mut Vec<RangeLayoutEntry>| target[0].endpoint.push_str("-forged"),
            |target: &mut Vec<RangeLayoutEntry>| target[0].wal_generation += 1,
            |target: &mut Vec<RangeLayoutEntry>| target[0].lifecycle = RangeLifecycle::Parking,
            |target: &mut Vec<RangeLayoutEntry>| target.swap(1, 2),
        ] {
            let mut forged = plan.clone();
            mutate(&mut forged.target_layout);
            assert!(operation().with_plan(forged).is_err());
        }
    }

    #[test]
    fn split_operation_evidence_is_append_only_and_exact() {
        let initiated = SplitOperationRecord::new(
            TenantName::try_from("tenant-a").unwrap(),
            "split-evidence",
            split_intent(),
        )
        .unwrap();
        let running = initiated
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        let evidence = SplitOperationEvidence {
            manifest_key: Some("tenant/r1/manifest".into()),
            covered_offset: Some(41),
            ..Default::default()
        };
        let checkpointed = running
            .advance_with_evidence(SplitOperationPhase::Checkpointed, 1, None, evidence.clone())
            .unwrap();
        let mut forged = evidence;
        forged.manifest_key = Some("attacker/manifest".into());

        assert!(
            checkpointed
                .advance_with_evidence(SplitOperationPhase::Checkpointed, 1, None, forged)
                .is_err()
        );
    }

    #[test]
    fn tenant_names_reject_invalid_dns_label_shapes() {
        assert!(TenantName::try_from("tenant-a").is_ok());
        assert!(TenantName::try_from("Tenant-A").is_err());
        assert!(TenantName::try_from("-tenant").is_err());
        assert!(TenantName::try_from("tenant-").is_err());
        assert!(TenantName::try_from("").is_err());
    }

    #[test]
    fn sql_user_rejects_non_identifier_shapes() {
        assert!(SqlUser::try_from("alice_1").is_ok());
        assert!(SqlUser::try_from("1alice").is_err());
        assert!(SqlUser::try_from("alice-one").is_err());
    }

    #[test]
    fn tenant_record_rejects_password_like_or_unreplicated_records() {
        let mut invalid = record(1);
        invalid.scram_verifier = "hunter2".to_string();
        assert!(invalid.ensure_valid().is_err());

        invalid.scram_verifier = "SCRAM-SHA-256$4096:salt$stored:server".to_string();
        invalid.wal_replication = 0;
        assert!(invalid.ensure_valid().is_err());
    }

    #[test]
    fn hash_placements_roundtrip_and_reject_invalid_shapes() {
        let placement = HashPlacement {
            table_id: 7,
            hash_columns: vec!["id".into()],
            bucket_count: 16,
            co_location_group: Some("orders".into()),
        };
        let with_hash = record(1)
            .with_hash_placements(vec![placement.clone()])
            .expect("valid hash placement");

        let (_key, value) = encode_registry_record(&with_hash).expect("encode");
        assert!(decode_registry_record(&value).unwrap().hash_placements == vec![placement]);

        let invalid = record(1).with_hash_placements(vec![HashPlacement {
            table_id: 7,
            hash_columns: vec!["id".into()],
            bucket_count: 3,
            co_location_group: None,
        }]);
        assert!(invalid.is_err());

        let invalid_group = record(1).with_hash_placements(vec![
            HashPlacement {
                table_id: 7,
                hash_columns: vec!["id".into()],
                bucket_count: 16,
                co_location_group: Some("orders".into()),
            },
            HashPlacement {
                table_id: 8,
                hash_columns: vec!["tenant_id".into()],
                bucket_count: 8,
                co_location_group: Some("orders".into()),
            },
        ]);
        assert!(invalid_group.is_err());
    }

    #[test]
    fn full_layout_replacements_bump_once_and_leave_exact_noops_unchanged() {
        let ranges = vec![RangeLayoutEntry {
            range_id: 0,
            end_key: None,
            endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
            wal_generation: 0,
            lifecycle: Default::default(),
            retirement: None,
        }];
        let placements = vec![HashPlacement {
            table_id: 7,
            hash_columns: vec!["id".to_string()],
            bucket_count: 16,
            co_location_group: None,
        }];

        let record_with_ranges = record(4).with_range_layout(ranges.clone()).unwrap();
        let hashed = record_with_ranges
            .clone()
            .with_hash_placements(placements.clone())
            .unwrap();

        assert!(record_with_ranges.record_version == 5);
        assert!(hashed.record_version == 6);
        assert!(
            record_with_ranges
                .clone()
                .with_range_layout(ranges)
                .unwrap()
                == record_with_ranges
        );
        assert!(hashed.clone().with_hash_placements(placements).unwrap() == hashed);
    }

    #[test]
    fn decoding_rejects_invalid_deserialized_newtype_values() {
        let value = br#"{"format_version":1,"record":{"record_version":1,"id":"Tenant-A","name":"tenant-a","state":"active","sql_user":"alice","scram_verifier":"SCRAM-SHA-256$4096:salt$stored:server","wal_replication":1,"bucket_prefix":null,"endpoint":null,"checkpoint_frames":null,"checkpoint_bytes":null,"idle_seconds":null,"wal_generation":0,"ranges":[],"suspend_max_checkpoint_bytes":null,"final_checkpoint":null}}"#;
        assert!(decode_registry_record(value).is_err());

        let key = br#"{"keytype":"TENANT","name":"Tenant-A","magic":1}"#;
        assert!(RegistryKey::decode(key).is_err());
    }

    #[test]
    fn registry_record_round_trips_through_versioned_envelope() {
        let mut input = record(7);
        input.state = TenantState::ResumeRequested;
        input.endpoint = Some("tenant-a.gres.svc:5432".to_string());
        input.wal_generation = 3;
        input.ranges = vec![RangeLayoutEntry {
            range_id: 0,
            end_key: None,
            endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
            wal_generation: 5,
            lifecycle: Default::default(),
            retirement: None,
        }];
        input.suspend_max_checkpoint_bytes = Some(1_048_576);
        let (key, value) = encode_registry_record(&input).unwrap();

        assert!(RegistryKey::decode(&key).unwrap().name == input.name);
        assert!(decode_registry_record(&value).unwrap() == input);
    }

    #[test]
    fn lifecycle_state_machine_accepts_only_expected_transitions() {
        let cases = [
            (TenantState::Active, TenantState::Suspended, true),
            (TenantState::Active, TenantState::ResumeRequested, false),
            (TenantState::Suspended, TenantState::ResumeRequested, true),
            (TenantState::ResumeRequested, TenantState::Active, true),
            (TenantState::Suspended, TenantState::Active, true),
            (TenantState::ResumeRequested, TenantState::Suspended, true),
        ];

        for (from, to, allowed) in cases {
            assert!(from.can_transition_to(to) == allowed);
        }
    }

    #[test]
    fn invalid_resume_request_transition_fails_fast() {
        let active = record(1);

        let error = active
            .transition_to(TenantState::ResumeRequested)
            .expect_err("active to resume-requested must be rejected");

        assert!(matches!(
            error,
            ControlError::InvalidLifecycleTransition {
                from: TenantState::Active,
                to: TenantState::ResumeRequested
            }
        ));
    }

    #[test]
    fn lifecycle_helpers_bump_version_and_handle_idempotent_resume_requests() {
        let active = record(1);
        assert!(active.clone().request_resume().unwrap() == active);

        let suspended = active.mark_suspended().unwrap();
        assert!(suspended.record_version == 2);
        assert!(suspended.state == TenantState::Suspended);

        let requested = suspended.request_resume().unwrap();
        assert!(requested.record_version == 3);
        assert!(requested.state == TenantState::ResumeRequested);

        let duplicate = requested.clone().request_resume().unwrap();
        assert!(duplicate == requested);
    }

    #[test]
    fn parking_fences_resume_requests_until_wal_deletion_completes() {
        let parking = record(1).transition_to(TenantState::Parking).unwrap();

        assert!(parking.clone().request_resume().unwrap() == parking);
        assert!(parking.state.can_transition_to(TenantState::Suspended));
        assert!(
            !parking
                .state
                .can_transition_to(TenantState::ResumeRequested)
        );
    }

    #[test]
    fn tenant_config_topic_uses_acl_scoped_shape() {
        let name = TenantName::try_from("tenant-a").unwrap();

        assert!(tenant_config_topic(&name) == "__gres_cfg.tenant-a");
    }

    #[test]
    fn tenant_config_record_uses_whole_tenant_snapshot() {
        let input = record(9);
        let value = encode_tenant_config_record(&input).unwrap();

        assert!(decode_tenant_config_record(&value).unwrap() == input);
    }

    #[test]
    fn range_layout_round_trips_and_rejects_invalid_shapes() {
        let record = record(1)
            .with_range_layout(vec![
                RangeLayoutEntry {
                    range_id: 0,
                    end_key: Some(RangeBoundary::new(10, 55)),
                    endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                    wal_generation: 1,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                RangeLayoutEntry {
                    range_id: 1,
                    end_key: None,
                    endpoint: "tenant-a-r1.gres.svc:7432".to_string(),
                    wal_generation: 2,
                    lifecycle: Default::default(),
                    retirement: None,
                },
            ])
            .unwrap();
        let value = encode_tenant_config_record(&record).unwrap();

        assert!(decode_tenant_config_record(&value).unwrap().ranges == record.ranges);
        assert!(
            decode_tenant_config_record(&value).unwrap().ranges[0].end_key
                == Some(RangeBoundary::new(10, 55))
        );
        assert!(
            record
                .clone()
                .with_range_layout(vec![RangeLayoutEntry {
                    range_id: 0,
                    end_key: None,
                    endpoint: String::new(),
                    wal_generation: 0,
                    lifecycle: Default::default(),
                    retirement: None,
                }])
                .is_err()
        );
    }

    #[test]
    fn range_wal_generation_advances_monotonically() {
        let record = record(1)
            .with_range_layout(vec![RangeLayoutEntry {
                range_id: 0,
                end_key: None,
                endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                wal_generation: 7,
                lifecycle: Default::default(),
                retirement: None,
            }])
            .unwrap();

        let record = record.with_range_wal_generation(0, 2).unwrap();

        assert!(record.ranges[0].wal_generation == 7);
        assert!(record.record_version == 2);
    }

    #[test]
    fn wal_generation_mutations_bump_record_version() {
        let tenant = record(4).with_wal_generation(8).unwrap();
        let ranged = record(4)
            .with_range_layout(vec![RangeLayoutEntry {
                range_id: 0,
                end_key: None,
                endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                wal_generation: 7,
                lifecycle: Default::default(),
                retirement: None,
            }])
            .unwrap()
            .with_range_wal_generation(0, 9)
            .unwrap();

        assert!(tenant.record_version == 5);
        assert!(ranged.record_version == 6);
    }

    #[test]
    fn split_range_layout_atomically_replaces_predecessor_with_two_successors() {
        let record = record(4)
            .with_range_layout(vec![
                RangeLayoutEntry {
                    range_id: 0,
                    end_key: Some(RangeBoundary::table_start(10)),
                    endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                    wal_generation: 1,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                RangeLayoutEntry {
                    range_id: 1,
                    end_key: None,
                    endpoint: "tenant-a-r1.gres.svc:7432".to_string(),
                    wal_generation: 2,
                    lifecycle: Default::default(),
                    retirement: None,
                },
            ])
            .unwrap();

        let split = record
            .split_range_layout(RangeLayoutSplit {
                source_range_id: 1,
                predecessor_generation: 2,
                left: RangeLayoutEntry {
                    range_id: 2,
                    end_key: Some(RangeBoundary::table_start(20)),
                    endpoint: "tenant-a-r2.gres.svc:7432".to_string(),
                    wal_generation: 5,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                right: RangeLayoutEntry {
                    range_id: 3,
                    end_key: None,
                    endpoint: "tenant-a-r3.gres.svc:7432".to_string(),
                    wal_generation: 6,
                    lifecycle: Default::default(),
                    retirement: None,
                },
            })
            .unwrap();

        assert!(split.record_version == 6);
        assert!(
            split.ranges
                == vec![
                    RangeLayoutEntry {
                        range_id: 0,
                        end_key: Some(RangeBoundary::table_start(10)),
                        endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                        wal_generation: 1,
                        lifecycle: Default::default(),
                        retirement: None,
                    },
                    RangeLayoutEntry {
                        range_id: 2,
                        end_key: Some(RangeBoundary::table_start(20)),
                        endpoint: "tenant-a-r2.gres.svc:7432".to_string(),
                        wal_generation: 5,
                        lifecycle: Default::default(),
                        retirement: None,
                    },
                    RangeLayoutEntry {
                        range_id: 3,
                        end_key: None,
                        endpoint: "tenant-a-r3.gres.svc:7432".to_string(),
                        wal_generation: 6,
                        lifecycle: Default::default(),
                        retirement: None,
                    },
                ]
        );
    }

    #[test]
    fn split_range_layout_reuses_range_zero_only_as_the_left_successor() {
        let record = record(4)
            .with_range_layout(vec![RangeLayoutEntry {
                range_id: 0,
                end_key: None,
                endpoint: "tenant-a-r0.gres.svc:7432".into(),
                wal_generation: 2,
                lifecycle: Default::default(),
                retirement: None,
            }])
            .unwrap();

        let split = record
            .split_range_layout(RangeLayoutSplit {
                source_range_id: 0,
                predecessor_generation: 2,
                left: RangeLayoutEntry {
                    range_id: 0,
                    end_key: Some(RangeBoundary::table_start(20)),
                    endpoint: "tenant-a-r0.gres.svc:7432".into(),
                    wal_generation: 3,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                right: RangeLayoutEntry {
                    range_id: 1,
                    end_key: None,
                    endpoint: "tenant-a-r1.gres.svc:7432".into(),
                    wal_generation: 3,
                    lifecycle: Default::default(),
                    retirement: None,
                },
            })
            .expect("range-zero split");

        assert!(split.ranges.len() == 2);
        assert!(split.ranges[0].range_id == 0);
        assert!(split.ranges[0].wal_generation == 3);
        assert!(split.ranges[1].range_id == 1);
    }

    #[test]
    fn merge_range_layout_removes_right_and_keeps_generation_monotonic() {
        let record = record(4)
            .with_range_layout(vec![
                RangeLayoutEntry {
                    range_id: 0,
                    end_key: Some(RangeBoundary::table_start(10)),
                    endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                    wal_generation: 1,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                RangeLayoutEntry {
                    range_id: 1,
                    end_key: Some(RangeBoundary::table_start(20)),
                    endpoint: "tenant-a-r1.gres.svc:7432".to_string(),
                    wal_generation: 7,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                RangeLayoutEntry {
                    range_id: 2,
                    end_key: None,
                    endpoint: "tenant-a-r2.gres.svc:7432".to_string(),
                    wal_generation: 5,
                    lifecycle: Default::default(),
                    retirement: None,
                },
            ])
            .unwrap();

        let merged = record
            .merge_range_layout(RangeLayoutMerge {
                left_range_id: 1,
                right_range_id: 2,
                merged_endpoint: "tenant-a-r1-merged.gres.svc:7432".to_string(),
                merged_wal_generation: 3,
            })
            .unwrap();

        assert!(merged.record_version == 6);
        assert!(
            merged.ranges
                == vec![
                    RangeLayoutEntry {
                        range_id: 0,
                        end_key: Some(RangeBoundary::table_start(10)),
                        endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                        wal_generation: 1,
                        lifecycle: Default::default(),
                        retirement: None,
                    },
                    RangeLayoutEntry {
                        range_id: 1,
                        end_key: None,
                        endpoint: "tenant-a-r1-merged.gres.svc:7432".to_string(),
                        wal_generation: 7,
                        lifecycle: Default::default(),
                        retirement: None,
                    },
                ]
        );
    }

    #[test]
    fn merge_range_layout_rejects_non_adjacent_ranges() {
        let record = record(4)
            .with_range_layout(vec![
                RangeLayoutEntry {
                    range_id: 0,
                    end_key: Some(RangeBoundary::table_start(10)),
                    endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                    wal_generation: 1,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                RangeLayoutEntry {
                    range_id: 1,
                    end_key: Some(RangeBoundary::table_start(20)),
                    endpoint: "tenant-a-r1.gres.svc:7432".to_string(),
                    wal_generation: 2,
                    lifecycle: Default::default(),
                    retirement: None,
                },
                RangeLayoutEntry {
                    range_id: 2,
                    end_key: None,
                    endpoint: "tenant-a-r2.gres.svc:7432".to_string(),
                    wal_generation: 3,
                    lifecycle: Default::default(),
                    retirement: None,
                },
            ])
            .unwrap();

        let result = record.merge_range_layout(RangeLayoutMerge {
            left_range_id: 0,
            right_range_id: 2,
            merged_endpoint: "tenant-a-r0-merged.gres.svc:7432".to_string(),
            merged_wal_generation: 9,
        });

        assert!(result.is_err());
    }

    #[test]
    fn registry_key_layout_is_deterministic() {
        let key = tenant_registry_key(&TenantName::try_from("tenant-a").unwrap()).unwrap();
        assert!(key == br#"{"keytype":"TENANT","name":"tenant-a","magic":1}"#);
    }

    #[test]
    fn range_lifecycle_defaults_serving_and_parking_is_generation_fenced() {
        let legacy = r#"{"range_id":1,"end_key":null,"endpoint":"r1:7432","wal_generation":4}"#;
        let range: RangeLayoutEntry = serde_json::from_str(legacy).expect("legacy range layout");
        assert_eq!(range.lifecycle, RangeLifecycle::Serving);
        assert!(range.retirement.is_none());

        let checkpoint = FinalCheckpoint {
            wal_generation: 4,
            covered_offset: 12,
            manifest_key: "tenant/r1/g4/manifest".into(),
            total_bytes: 99,
        };
        let parking = range
            .begin_parking("split-7", 4, checkpoint.clone())
            .expect("matching generation parks");
        assert_eq!(parking.lifecycle, RangeLifecycle::Parking);
        assert_eq!(parking.retirement.as_ref().unwrap().operation_id, "split-7");
        assert!(range.begin_parking("split-7", 3, checkpoint).is_err());
        assert!(parking.confirm_parked("another-operation", 4).is_err());
        assert_eq!(
            parking.confirm_parked("split-7", 4).unwrap().lifecycle,
            RangeLifecycle::Parked
        );
        let mut malformed = parking;
        malformed
            .retirement
            .as_mut()
            .unwrap()
            .checkpoint
            .manifest_key
            .clear();
        let mut record = record(1);
        record.ranges = vec![malformed];
        assert!(record.ensure_valid().is_err());
    }

    #[test]
    fn split_cutover_keeps_reused_successor_serving_while_old_generation_parks() {
        let source = RangeLayoutEntry {
            range_id: 0,
            end_key: None,
            endpoint: "old-r0:7432".into(),
            wal_generation: 4,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let left = RangeLayoutEntry {
            range_id: 0,
            end_key: Some(RangeBoundary::new(7, 50)),
            endpoint: "new-r0:7432".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let right = RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "new-r1:7432".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let current = record(9).with_range_layout(vec![source]).unwrap();
        let checkpoint = RangeRetirementCheckpoint {
            covered_offset: 33,
            barrier_offset: 35,
            manifest_key: "tenant-a/r0/g4/manifest".into(),
            tail_sha256: "tail".into(),
            marker_digest: "markers".into(),
        };

        let cutover = current
            .publish_split_target_with_retirement(
                "split-1",
                0,
                4,
                checkpoint,
                vec![left.clone(), right.clone()],
            )
            .unwrap();

        assert_eq!(cutover.ranges, vec![left, right]);
        assert!(
            cutover
                .ranges
                .iter()
                .all(|range| range.lifecycle == RangeLifecycle::Serving)
        );
        assert_eq!(cutover.range_retirements.len(), 1);
        assert_eq!(
            cutover.range_retirements[0].phase,
            RangeRetirementPhase::Parking
        );
        let parked = cutover
            .confirm_split_predecessor_parked("split-1", 0, 4)
            .unwrap();
        assert_eq!(
            parked.range_retirements[0].phase,
            RangeRetirementPhase::Parked
        );
        assert!(
            parked
                .ranges
                .iter()
                .all(|range| range.lifecycle == RangeLifecycle::Serving)
        );
    }
}

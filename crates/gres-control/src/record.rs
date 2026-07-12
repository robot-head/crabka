//! Tenant registry record schema and byte encoders.

use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::ControlError;

/// The compacted Kafka topic that stores whole-tenant registry snapshots.
pub const TENANT_REGISTRY_TOPIC: &str = "__gres_tenants";
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
            || split.source_range_id == split.left.range_id
            || split.source_range_id == split.right.range_id
        {
            return Err(ControlError::invalid_field(
                "ranges.range_id",
                "two distinct successors must differ from the source range id",
            ));
        }
        if self.ranges.iter().any(|range| {
            range.range_id == split.left.range_id || range.range_id == split.right.range_id
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
}

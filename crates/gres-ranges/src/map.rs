use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{KeyHash, MapEpoch, RangeId, ShardId, TableId, TenantName};

const RANGE_MAP_FORMAT_VERSION: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeKey {
    pub table_id: TableId,
    pub bucket: u32,
    pub rowid: u64,
}

impl RangeKey {
    pub const MIN: Self = Self::table_start(TableId::ZERO);

    #[must_use]
    pub const fn new(table_id: TableId, rowid: u64) -> Self {
        Self {
            table_id,
            bucket: 0,
            rowid,
        }
    }

    #[must_use]
    pub const fn hash(table_id: TableId, bucket: u32, rowid: u64) -> Self {
        Self {
            table_id,
            bucket,
            rowid,
        }
    }

    #[must_use]
    pub const fn table_start(table_id: TableId) -> Self {
        Self::hash(table_id, 0, 0)
    }

    #[must_use]
    pub const fn hash_bucket_start(table_id: TableId, bucket: u32) -> Self {
        Self::hash(table_id, bucket, 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HashShardSpec {
    pub table_id: TableId,
    pub hash_columns: Vec<String>,
    pub bucket_count: u32,
    pub co_location_group: Option<String>,
}

impl HashShardSpec {
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn new(
        table_id: TableId,
        hash_columns: Vec<String>,
        bucket_count: u32,
        co_location_group: Option<String>,
    ) -> Result<Self, MapValidationError> {
        if hash_columns.is_empty() || hash_columns.iter().any(String::is_empty) {
            return Err(MapValidationError::InvalidHashShardSpec {
                reason: "hash sharding requires non-empty column names".into(),
            });
        }
        if bucket_count == 0 || !bucket_count.is_power_of_two() {
            return Err(MapValidationError::InvalidHashShardSpec {
                reason: "hash bucket count must be a power of two".into(),
            });
        }
        if co_location_group.as_deref().is_some_and(str::is_empty) {
            return Err(MapValidationError::InvalidHashShardSpec {
                reason: "co-location group name must not be empty".into(),
            });
        }
        Ok(Self {
            table_id,
            hash_columns,
            bucket_count,
            co_location_group,
        })
    }

    #[must_use]
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    pub fn bucket_for_value(&self, value: impl AsRef<[u8]>) -> u32 {
        crabka_pgkv::key::hash_bucket(value.as_ref(), self.bucket_count)
            .expect("validated hash spec has a power-of-two bucket count")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoLocationGroup {
    pub name: String,
    pub tables: Vec<TableId>,
    pub bucket_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeSpec {
    pub range_id: RangeId,
    pub start: RangeKey,
    pub end: Option<RangeKey>,
}

impl RangeSpec {
    #[must_use]
    pub const fn new(range_id: RangeId, table_start: TableId, table_end: Option<TableId>) -> Self {
        Self {
            range_id,
            start: RangeKey::table_start(table_start),
            end: match table_end {
                Some(table_end) => Some(RangeKey::table_start(table_end)),
                None => None,
            },
        }
    }

    #[must_use]
    pub const fn for_interval(range_id: RangeId, start: RangeKey, end: Option<RangeKey>) -> Self {
        Self {
            range_id,
            start,
            end,
        }
    }

    #[must_use]
    pub fn contains_table(&self, table_id: TableId) -> bool {
        if table_id < self.start.table_id {
            return false;
        }

        self.end.is_none_or(|end| table_id <= end.table_id)
    }

    #[must_use]
    pub fn contains_key(&self, key: RangeKey) -> bool {
        if key < self.start {
            return false;
        }

        self.end.is_none_or(|end| key < end)
    }

    #[must_use]
    pub const fn table_start(&self) -> TableId {
        self.start.table_id
    }

    #[must_use]
    pub fn table_end(&self) -> Option<TableId> {
        self.end.map(|end| end.table_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RangeMap {
    tenant: TenantName,
    epoch: MapEpoch,
    ranges: Vec<RangeSpec>,
    co_location_groups: Vec<CoLocationGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRangeMap {
    format_version: u16,
    tenant: TenantName,
    epoch: MapEpoch,
    ranges: Vec<RangeSpec>,
    co_location_groups: Vec<CoLocationGroup>,
}

impl Serialize for RangeMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        RawRangeMap {
            format_version: RANGE_MAP_FORMAT_VERSION,
            tenant: self.tenant.clone(),
            epoch: self.epoch,
            ranges: self.ranges.clone(),
            co_location_groups: self.co_location_groups.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RangeMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawRangeMap::deserialize(deserializer)?;
        if raw.format_version != RANGE_MAP_FORMAT_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported range map format_version {}",
                raw.format_version
            )));
        }

        Self::new_with_co_location(raw.tenant, raw.epoch, raw.ranges, raw.co_location_groups)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MapValidationError {
    #[error("range map must contain at least one range")]
    Empty,
    #[error("range map must contain coordinator range r0")]
    MissingCoordinatorRange,
    #[error("duplicate range id: r{range_id}")]
    DuplicateRangeId { range_id: RangeId },
    #[error("range r{range_id} is empty: start {start:?} end {end:?}")]
    EmptyRange {
        range_id: RangeId,
        start: RangeKey,
        end: RangeKey,
    },
    #[error("first range must start at (table 0, rowid 0), found {start:?}")]
    MissingCoverageStart { start: RangeKey },
    #[error("range r{range_id} starts at {start:?}, expected {expected_start:?}")]
    CoverageGap {
        range_id: RangeId,
        start: RangeKey,
        expected_start: RangeKey,
    },
    #[error("only the final range may have an open end")]
    OpenEndedRangeBeforeFinal,
    #[error("final range must be open-ended")]
    FinalRangeIsBounded,
    #[error("table {table_id} is not covered by the range map")]
    TableNotCovered { table_id: TableId },
    #[error("key {key:?} is not covered by the range map")]
    KeyNotCovered { key: RangeKey },
    #[error("cannot split range r{range_id} at key {split_at:?}")]
    InvalidSplitPoint {
        range_id: RangeId,
        split_at: RangeKey,
    },
    #[error("ranges r{left} and r{right} are not adjacent")]
    RangesAreNotAdjacent { left: RangeId, right: RangeId },
    #[error("invalid hash shard spec: {reason}")]
    InvalidHashShardSpec { reason: String },
    #[error("co-location group {group} is invalid: {reason}")]
    InvalidCoLocationGroup { group: String, reason: String },
}

impl RangeMap {
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn new(
        tenant: TenantName,
        epoch: MapEpoch,
        ranges: Vec<RangeSpec>,
    ) -> Result<Self, MapValidationError> {
        Self::new_with_co_location(tenant, epoch, ranges, Vec::new())
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn new_with_co_location(
        tenant: TenantName,
        epoch: MapEpoch,
        ranges: Vec<RangeSpec>,
        co_location_groups: Vec<CoLocationGroup>,
    ) -> Result<Self, MapValidationError> {
        validate_ranges(&ranges)?;
        validate_co_location_groups(&ranges, &co_location_groups)?;

        Ok(Self {
            tenant,
            epoch,
            ranges,
            co_location_groups,
        })
    }

    #[must_use]
    pub fn tenant(&self) -> &TenantName {
        &self.tenant
    }

    #[must_use]
    pub const fn epoch(&self) -> MapEpoch {
        self.epoch
    }

    #[must_use]
    pub fn ranges(&self) -> &[RangeSpec] {
        self.ranges.as_slice()
    }

    #[must_use]
    pub fn co_location_groups(&self) -> &[CoLocationGroup] {
        self.co_location_groups.as_slice()
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn route_table(&self, table_id: TableId) -> Result<TableRoute, MapValidationError> {
        let route = self
            .range_for_key(table_id, 0)
            .map_err(|_| MapValidationError::TableNotCovered { table_id })?;

        Ok(TableRoute {
            table_id,
            range_id: route.range_id,
        })
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn range_for_key(
        &self,
        table_id: TableId,
        rowid: u64,
    ) -> Result<KeyRoute, MapValidationError> {
        let key = RangeKey::new(table_id, rowid);
        let Some(range) = self.ranges.iter().find(|range| range.contains_key(key)) else {
            return Err(MapValidationError::KeyNotCovered { key });
        };

        Ok(KeyRoute {
            table_id,
            range_id: range.range_id,
            shard_id: ShardId::ZERO,
            key_hash: KeyHash::new(rowid),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn range_for_hash_bucket(
        &self,
        table_id: TableId,
        bucket: u32,
        rowid: u64,
    ) -> Result<KeyRoute, MapValidationError> {
        let key = RangeKey::hash(table_id, bucket, rowid);
        let Some(range) = self.ranges.iter().find(|range| range.contains_key(key)) else {
            return Err(MapValidationError::KeyNotCovered { key });
        };

        Ok(KeyRoute {
            table_id,
            range_id: range.range_id,
            shard_id: ShardId::new(bucket),
            key_hash: KeyHash::new(rowid),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn route_hash_equality(
        &self,
        spec: &HashShardSpec,
        value: impl AsRef<[u8]>,
    ) -> Result<KeyRoute, MapValidationError> {
        self.range_for_hash_bucket(spec.table_id, spec.bucket_for_value(value), 0)
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn route_key(
        &self,
        table_id: TableId,
        key: impl AsRef<[u8]>,
    ) -> Result<KeyRoute, MapValidationError> {
        let table_route = self.range_for_key(table_id, 0)?;

        Ok(KeyRoute {
            table_id,
            range_id: table_route.range_id,
            shard_id: ShardId::ZERO,
            key_hash: KeyHash::new(fnv1a64(key.as_ref())),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn plan_split(
        &self,
        range_id: RangeId,
        split_at: TableId,
        new_range_id: RangeId,
    ) -> Result<SplitPlan, MapValidationError> {
        self.plan_split_at_key(range_id, RangeKey::table_start(split_at), new_range_id)
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn plan_split_at_key(
        &self,
        range_id: RangeId,
        split_at: RangeKey,
        new_range_id: RangeId,
    ) -> Result<SplitPlan, MapValidationError> {
        let range = self
            .ranges
            .iter()
            .find(|range| range.range_id == range_id)
            .ok_or(MapValidationError::InvalidSplitPoint { range_id, split_at })?;

        if !range.contains_key(split_at) || split_at == range.start {
            return Err(MapValidationError::InvalidSplitPoint { range_id, split_at });
        }

        Ok(SplitPlan {
            source: range_id,
            split_at,
            left: RangeSpec::for_interval(range_id, range.start, Some(split_at)),
            right: RangeSpec::for_interval(new_range_id, split_at, range.end),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn plan_merge(
        &self,
        left: RangeId,
        right: RangeId,
    ) -> Result<MergePlan, MapValidationError> {
        let Some((left_spec, right_spec)) = adjacent_ranges(&self.ranges, left, right) else {
            return Err(MapValidationError::RangesAreNotAdjacent { left, right });
        };

        Ok(MergePlan {
            left,
            right,
            merged: RangeSpec::for_interval(left, left_spec.start, right_spec.end),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn merge_adjacent_ranges(
        &self,
        epoch: MapEpoch,
        left: RangeId,
        right: RangeId,
    ) -> Result<Self, MapValidationError> {
        let plan = self.plan_merge(left, right)?;
        let mut ranges = Vec::with_capacity(self.ranges.len().saturating_sub(1));
        let mut inserted_merge = false;

        for range in &self.ranges {
            if range.range_id == left {
                ranges.push(plan.merged.clone());
                inserted_merge = true;
                continue;
            }
            if range.range_id == right {
                continue;
            }
            ranges.push(range.clone());
        }

        if !inserted_merge {
            return Err(MapValidationError::RangesAreNotAdjacent { left, right });
        }

        Self::new_with_co_location(
            self.tenant.clone(),
            epoch,
            ranges,
            self.co_location_groups.clone(),
        )
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn scan_segments(
        &self,
        table_id: TableId,
        interval: RowInterval,
    ) -> Result<Vec<RangeScanSegment>, MapValidationError> {
        let scan_start = RangeKey::new(table_id, interval.start.unwrap_or(0));
        let scan_end = interval.end.map(|end| RangeKey::new(table_id, end));
        let mut segments = Vec::new();

        for range in &self.ranges {
            let start = scan_start.max(range.start);
            let end = min_optional_key(scan_end, range.end);
            if !segment_overlaps_table(table_id, start, end) {
                continue;
            }

            segments.push(RangeScanSegment {
                range_id: range.range_id,
                table_id,
                interval: RowInterval {
                    start: Some(start.rowid),
                    end: end
                        .filter(|key| key.table_id == table_id)
                        .map(|key| key.rowid),
                },
            });
        }

        if segments.is_empty() {
            return Err(MapValidationError::KeyNotCovered { key: scan_start });
        }

        Ok(segments)
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn scan_hash_bucket_segments(
        &self,
        table_id: TableId,
        bucket: u32,
        interval: RowInterval,
    ) -> Result<Vec<RangeScanSegment>, MapValidationError> {
        let scan_start = RangeKey::hash(table_id, bucket, interval.start.unwrap_or(0));
        let scan_end = interval
            .end
            .map(|end| RangeKey::hash(table_id, bucket, end));
        let mut segments = Vec::new();

        for range in &self.ranges {
            let start = scan_start.max(range.start);
            let end = min_optional_key(scan_end, range.end);
            if !segment_overlaps_hash_bucket(table_id, bucket, start, end) {
                continue;
            }

            segments.push(RangeScanSegment {
                range_id: range.range_id,
                table_id,
                interval: RowInterval {
                    start: Some(start.rowid),
                    end: end
                        .filter(|key| key.table_id == table_id && key.bucket == bucket)
                        .map(|key| key.rowid),
                },
            });
        }

        if segments.is_empty() {
            return Err(MapValidationError::KeyNotCovered { key: scan_start });
        }

        Ok(segments)
    }

    #[must_use]
    pub const fn route_intent(intent: RouteIntent) -> RouteIntent {
        intent
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteIntent {
    DataDefinition,
    DataManipulation { table_id: TableId },
}

impl RouteIntent {
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn route(self, range_map: &RangeMap) -> Result<TableRoute, MapValidationError> {
        match self {
            Self::DataDefinition => Ok(TableRoute {
                table_id: TableId::ZERO,
                range_id: RangeId::COORDINATOR,
            }),
            Self::DataManipulation { table_id } => range_map.route_table(table_id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRoute {
    pub table_id: TableId,
    pub range_id: RangeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRoute {
    pub table_id: TableId,
    pub range_id: RangeId,
    pub shard_id: ShardId,
    pub key_hash: KeyHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowInterval {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeScanSegment {
    pub range_id: RangeId,
    pub table_id: TableId,
    pub interval: RowInterval,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitPlan {
    pub source: RangeId,
    pub split_at: RangeKey,
    pub left: RangeSpec,
    pub right: RangeSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergePlan {
    pub left: RangeId,
    pub right: RangeId,
    pub merged: RangeSpec,
}

fn validate_ranges(ranges: &[RangeSpec]) -> Result<(), MapValidationError> {
    if ranges.is_empty() {
        return Err(MapValidationError::Empty);
    }

    if ranges[0].start != RangeKey::MIN {
        return Err(MapValidationError::MissingCoverageStart {
            start: ranges[0].start,
        });
    }

    if !ranges
        .iter()
        .any(|range| range.range_id == RangeId::COORDINATOR)
    {
        return Err(MapValidationError::MissingCoordinatorRange);
    }

    let mut seen_range_ids = BTreeSet::new();
    let mut expected_start = RangeKey::MIN;

    for (index, range) in ranges.iter().enumerate() {
        if !seen_range_ids.insert(range.range_id) {
            return Err(MapValidationError::DuplicateRangeId {
                range_id: range.range_id,
            });
        }

        if range.start != expected_start {
            return Err(MapValidationError::CoverageGap {
                range_id: range.range_id,
                start: range.start,
                expected_start,
            });
        }

        let is_final_range = index == ranges.len() - 1;
        let Some(end) = range.end else {
            if is_final_range {
                return Ok(());
            }

            return Err(MapValidationError::OpenEndedRangeBeforeFinal);
        };

        if end <= range.start {
            return Err(MapValidationError::EmptyRange {
                range_id: range.range_id,
                start: range.start,
                end,
            });
        }

        expected_start = end;
    }

    Err(MapValidationError::FinalRangeIsBounded)
}

fn validate_co_location_groups(
    ranges: &[RangeSpec],
    groups: &[CoLocationGroup],
) -> Result<(), MapValidationError> {
    let mut seen_groups = BTreeSet::new();
    for group in groups {
        if group.name.is_empty() {
            return Err(MapValidationError::InvalidCoLocationGroup {
                group: group.name.clone(),
                reason: "name must not be empty".into(),
            });
        }
        if !seen_groups.insert(group.name.clone()) {
            return Err(MapValidationError::InvalidCoLocationGroup {
                group: group.name.clone(),
                reason: "group name must be unique".into(),
            });
        }
        if group.tables.len() < 2 {
            return Err(MapValidationError::InvalidCoLocationGroup {
                group: group.name.clone(),
                reason: "at least two tables must share a co-location group".into(),
            });
        }
        if group.bucket_count == 0 || !group.bucket_count.is_power_of_two() {
            return Err(MapValidationError::InvalidCoLocationGroup {
                group: group.name.clone(),
                reason: "bucket count must be a power of two".into(),
            });
        }
        ensure_group_buckets_are_co_located(ranges, group)?;
    }
    Ok(())
}

fn ensure_group_buckets_are_co_located(
    ranges: &[RangeSpec],
    group: &CoLocationGroup,
) -> Result<(), MapValidationError> {
    for bucket in 0..group.bucket_count {
        let Some(first_table) = group.tables.first().copied() else {
            return Err(MapValidationError::InvalidCoLocationGroup {
                group: group.name.clone(),
                reason: "at least one table is required".into(),
            });
        };
        let expected =
            range_id_for_raw_key(ranges, RangeKey::hash_bucket_start(first_table, bucket))
                .ok_or_else(|| MapValidationError::InvalidCoLocationGroup {
                    group: group.name.clone(),
                    reason: format!("bucket {bucket} is not covered for table {first_table}"),
                })?;
        for table in group.tables.iter().copied().skip(1) {
            let actual = range_id_for_raw_key(ranges, RangeKey::hash_bucket_start(table, bucket))
                .ok_or_else(|| MapValidationError::InvalidCoLocationGroup {
                group: group.name.clone(),
                reason: format!("bucket {bucket} is not covered for table {table}"),
            })?;
            if actual != expected {
                return Err(MapValidationError::InvalidCoLocationGroup {
                    group: group.name.clone(),
                    reason: format!("bucket {bucket} is split between r{expected} and r{actual}"),
                });
            }
        }
    }
    Ok(())
}

fn range_id_for_raw_key(ranges: &[RangeSpec], key: RangeKey) -> Option<RangeId> {
    ranges
        .iter()
        .find(|range| range.contains_key(key))
        .map(|range| range.range_id)
}

fn adjacent_ranges(
    ranges: &[RangeSpec],
    left: RangeId,
    right: RangeId,
) -> Option<(&RangeSpec, &RangeSpec)> {
    ranges.windows(2).find_map(|pair| {
        let [left_spec, right_spec] = pair else {
            return None;
        };

        (left_spec.range_id == left && right_spec.range_id == right)
            .then_some((left_spec, right_spec))
    })
}

fn min_optional_key(left: Option<RangeKey>, right: Option<RangeKey>) -> Option<RangeKey> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn segment_overlaps_table(table_id: TableId, start: RangeKey, end: Option<RangeKey>) -> bool {
    if start.table_id > table_id {
        return false;
    }

    end.is_none_or(|end| RangeKey::table_start(table_id) < end)
}

fn segment_overlaps_hash_bucket(
    table_id: TableId,
    bucket: u32,
    start: RangeKey,
    end: Option<RangeKey>,
) -> bool {
    let bucket_start = RangeKey::hash_bucket_start(table_id, bucket);
    if start > bucket_start {
        return start.table_id == table_id && start.bucket == bucket;
    }

    end.is_none_or(|end| bucket_start < end)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use proptest::prelude::*;

    use super::*;

    #[derive(Clone, Debug)]
    struct GroupPlacementModel {
        owners: [[u8; 4]; 2],
        split_generations: [[u8; 4]; 2],
    }

    impl GroupPlacementModel {
        fn coordinated_move(&mut self, bucket: usize, owner: u8) {
            for table in &mut self.owners {
                table[bucket] = owner;
            }
        }

        fn coordinated_split(&mut self, bucket: usize) {
            for table in &mut self.split_generations {
                table[bucket] = table[bucket].wrapping_add(1);
            }
        }

        fn broken_move(&mut self, bucket: usize, owner: u8) {
            self.owners[0][bucket] = owner;
        }

        fn is_co_located(&self) -> bool {
            self.owners[0] == self.owners[1]
                && self.split_generations[0] == self.split_generations[1]
        }
    }
    use crate::TenantName;

    fn tenant() -> TenantName {
        TenantName::parse("tenant_a").unwrap()
    }

    fn map() -> RangeMap {
        RangeMap::new(
            tenant(),
            MapEpoch::new(3),
            vec![
                RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(10))),
                RangeSpec::new(RangeId::new(1), TableId::new(10), Some(TableId::new(20))),
                RangeSpec::new(RangeId::new(2), TableId::new(20), None),
            ],
        )
        .unwrap()
    }

    fn row_split_map() -> RangeMap {
        RangeMap::new(
            tenant(),
            MapEpoch::new(4),
            vec![
                RangeSpec::for_interval(
                    RangeId::COORDINATOR,
                    RangeKey::MIN,
                    Some(RangeKey::new(TableId::new(5), 100)),
                ),
                RangeSpec::for_interval(
                    RangeId::new(1),
                    RangeKey::new(TableId::new(5), 100),
                    Some(RangeKey::new(TableId::new(5), 200)),
                ),
                RangeSpec::for_interval(RangeId::new(2), RangeKey::new(TableId::new(5), 200), None),
            ],
        )
        .unwrap()
    }

    fn hash_bucket_map() -> RangeMap {
        RangeMap::new(
            tenant(),
            MapEpoch::new(5),
            vec![
                RangeSpec::for_interval(
                    RangeId::COORDINATOR,
                    RangeKey::MIN,
                    Some(RangeKey::hash_bucket_start(TableId::new(10), 1)),
                ),
                RangeSpec::for_interval(
                    RangeId::new(1),
                    RangeKey::hash_bucket_start(TableId::new(10), 1),
                    Some(RangeKey::hash_bucket_start(TableId::new(10), 2)),
                ),
                RangeSpec::for_interval(
                    RangeId::new(2),
                    RangeKey::hash_bucket_start(TableId::new(10), 2),
                    None,
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn route_table_uses_complete_half_open_coverage() {
        let range_map = map();

        assert!(range_map.route_table(TableId::new(0)).unwrap().range_id == RangeId::COORDINATOR);
        assert!(range_map.route_table(TableId::new(9)).unwrap().range_id == RangeId::COORDINATOR);
        assert!(range_map.route_table(TableId::new(10)).unwrap().range_id == RangeId::new(1));
        assert!(range_map.route_table(TableId::new(20)).unwrap().range_id == RangeId::new(2));
    }

    #[test]
    fn range_for_key_uses_table_and_rowid_boundaries() {
        let range_map = row_split_map();

        assert!(
            range_map
                .range_for_key(TableId::new(5), 99)
                .unwrap()
                .range_id
                == RangeId::COORDINATOR
        );
        assert!(
            range_map
                .range_for_key(TableId::new(5), 100)
                .unwrap()
                .range_id
                == RangeId::new(1)
        );
        assert!(
            range_map
                .range_for_key(TableId::new(5), 199)
                .unwrap()
                .range_id
                == RangeId::new(1)
        );
        assert!(
            range_map
                .range_for_key(TableId::new(5), 200)
                .unwrap()
                .range_id
                == RangeId::new(2)
        );
    }

    #[test]
    fn hash_bucket_is_leading_range_key_component() {
        let range_map = hash_bucket_map();

        assert_eq!(
            range_map
                .range_for_hash_bucket(TableId::new(10), 1, 999)
                .unwrap()
                .range_id,
            RangeId::new(1)
        );
        assert_eq!(
            range_map
                .range_for_hash_bucket(TableId::new(10), 2, 0)
                .unwrap()
                .range_id,
            RangeId::new(2)
        );
    }

    #[test]
    fn hash_equality_routes_deterministically_to_bucket_range() {
        let range_map = hash_bucket_map();
        let spec = HashShardSpec::new(TableId::new(10), vec!["id".into()], 4, None).unwrap();
        let route = range_map.route_hash_equality(&spec, b"alice").unwrap();
        let expected_bucket = spec.bucket_for_value(b"alice");

        assert_eq!(route.shard_id, ShardId::new(expected_bucket));
        assert_eq!(
            route.range_id,
            range_map
                .range_for_hash_bucket(TableId::new(10), expected_bucket, 0)
                .unwrap()
                .range_id
        );
    }

    #[test]
    fn hash_spec_rejects_empty_column_and_group_names() {
        for result in [
            HashShardSpec::new(TableId::new(10), vec![String::new()], 4, None),
            HashShardSpec::new(TableId::new(10), vec!["id".into()], 4, Some(String::new())),
        ] {
            assert!(matches!(
                result,
                Err(MapValidationError::InvalidHashShardSpec { .. })
            ));
        }
    }

    #[test]
    fn hash_bucket_corpus_matches_physical_key_encoding() {
        let spec = HashShardSpec::new(TableId::new(10), vec!["id".into()], 16, None).unwrap();
        let corpus = [
            (b"".as_slice(), 5),
            (b"a".as_slice(), 12),
            (b"alpha".as_slice(), 11),
            (b"alice".as_slice(), 7),
            (&[0_u8, 255, 1], 3),
        ];

        for (value, expected) in corpus {
            assert_eq!(spec.bucket_for_value(value), expected);
            assert_eq!(crabka_pgkv::key::hash_bucket(value, 16), Some(expected));
        }
    }

    #[test]
    fn co_location_group_rejects_bucket_placement_mismatch() {
        let err = RangeMap::new_with_co_location(
            tenant(),
            MapEpoch::new(6),
            vec![
                RangeSpec::for_interval(
                    RangeId::COORDINATOR,
                    RangeKey::MIN,
                    Some(RangeKey::hash_bucket_start(TableId::new(10), 1)),
                ),
                RangeSpec::for_interval(
                    RangeId::new(1),
                    RangeKey::hash_bucket_start(TableId::new(10), 1),
                    Some(RangeKey::hash_bucket_start(TableId::new(20), 1)),
                ),
                RangeSpec::for_interval(
                    RangeId::new(2),
                    RangeKey::hash_bucket_start(TableId::new(20), 1),
                    None,
                ),
            ],
            vec![CoLocationGroup {
                name: "g".into(),
                tables: vec![TableId::new(10), TableId::new(20)],
                bucket_count: 2,
            }],
        )
        .expect_err("mismatched co-location");

        assert!(matches!(
            err,
            MapValidationError::InvalidCoLocationGroup { .. }
        ));
    }

    proptest::proptest! {
        #[test]
        fn coordinated_group_moves_preserve_every_bucket_after_every_operation(
            operations in proptest::collection::vec((proptest::bool::ANY, 0_usize..4, 0_u8..4), 0..64)
        ) {
            let mut model = GroupPlacementModel {
                owners: [[0; 4]; 2],
                split_generations: [[0; 4]; 2],
            };
            for (is_split, bucket, owner) in operations {
                if is_split {
                    model.coordinated_split(bucket);
                } else {
                    model.coordinated_move(bucket, owner);
                }
                proptest::prop_assert!(model.is_co_located());
            }
        }
    }

    #[test]
    fn broken_uncoordinated_group_move_has_teeth() {
        let mut model = GroupPlacementModel {
            owners: [[0; 4]; 2],
            split_generations: [[0; 4]; 2],
        };
        model.broken_move(2, 1);
        assert!(!model.is_co_located());
    }

    #[test]
    fn table_only_ranges_are_rowid_unbounded_intervals() {
        let range_map = map();

        assert!(
            range_map
                .range_for_key(TableId::new(12), 0)
                .unwrap()
                .range_id
                == RangeId::new(1)
        );
        assert!(
            range_map
                .range_for_key(TableId::new(12), u64::MAX)
                .unwrap()
                .range_id
                == RangeId::new(1)
        );
    }

    #[test]
    fn ddl_routes_to_coordinator() {
        let route = RouteIntent::DataDefinition.route(&map()).unwrap();

        assert!(route.range_id == RangeId::COORDINATOR);
    }

    #[test]
    fn route_key_is_deterministic_and_table_granular() {
        let range_map = map();
        let first = range_map.route_key(TableId::new(12), b"alpha").unwrap();
        let second = range_map.route_key(TableId::new(12), b"alpha").unwrap();
        let other_key = range_map.route_key(TableId::new(12), b"beta").unwrap();

        assert!(first == second);
        assert!(first.range_id == other_key.range_id);
        assert!(first.key_hash != other_key.key_hash);
    }

    #[test]
    fn serde_roundtrip_preserves_validated_v2_map() {
        let range_map = row_split_map();
        let encoded = serde_json::to_string(&range_map).unwrap();
        let decoded: RangeMap = serde_json::from_str(&encoded).unwrap();

        assert!(encoded.contains("format_version"));
        assert!(decoded == range_map);
    }

    #[test]
    fn serde_rejects_v1_map() {
        let encoded = r#"{
            "format_version":1,
            "tenant":"tenant_a",
            "epoch":1,
            "ranges":[
                {"range_id":0,"start":{"table_id":0,"rowid":0},"end":null}
            ]
        }"#;

        assert!(serde_json::from_str::<RangeMap>(encoded).is_err());
    }

    #[test]
    fn serde_rejects_unversioned_maps() {
        let encoded = r#"{
            "tenant":"tenant_a",
            "epoch":1,
            "ranges":[
                {"range_id":0,"table_start":1,"table_end":null}
            ]
        }"#;

        assert!(serde_json::from_str::<RangeMap>(encoded).is_err());
    }

    #[test]
    fn validation_rejects_overlaps() {
        let result = RangeMap::new(
            tenant(),
            MapEpoch::ZERO,
            vec![
                RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(10))),
                RangeSpec::new(RangeId::new(1), TableId::new(9), None),
            ],
        );

        assert!(matches!(
            result,
            Err(MapValidationError::CoverageGap { .. })
        ));
    }

    #[test]
    fn validation_rejects_gaps() {
        let result = RangeMap::new(
            tenant(),
            MapEpoch::ZERO,
            vec![
                RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(10))),
                RangeSpec::new(RangeId::new(1), TableId::new(11), None),
            ],
        );

        assert!(matches!(
            result,
            Err(MapValidationError::CoverageGap { .. })
        ));
    }

    #[test]
    fn split_and_merge_plans_are_pure() {
        let range_map = map();
        let split = range_map
            .plan_split(RangeId::new(1), TableId::new(15), RangeId::new(3))
            .unwrap();
        let merge = range_map
            .plan_merge(RangeId::COORDINATOR, RangeId::new(1))
            .unwrap();

        assert!(split.left.end == Some(RangeKey::table_start(TableId::new(15))));
        assert!(split.right.start == RangeKey::table_start(TableId::new(15)));
        assert!(merge.merged.start == RangeKey::MIN);
        assert!(merge.merged.end == Some(RangeKey::table_start(TableId::new(20))));
    }

    #[test]
    fn scan_segments_decompose_rowid_interval_across_ranges() {
        let segments = row_split_map()
            .scan_segments(
                TableId::new(5),
                RowInterval {
                    start: Some(50),
                    end: Some(250),
                },
            )
            .unwrap();

        assert!(
            segments
                == vec![
                    RangeScanSegment {
                        range_id: RangeId::COORDINATOR,
                        table_id: TableId::new(5),
                        interval: RowInterval {
                            start: Some(50),
                            end: Some(100)
                        },
                    },
                    RangeScanSegment {
                        range_id: RangeId::new(1),
                        table_id: TableId::new(5),
                        interval: RowInterval {
                            start: Some(100),
                            end: Some(200)
                        },
                    },
                    RangeScanSegment {
                        range_id: RangeId::new(2),
                        table_id: TableId::new(5),
                        interval: RowInterval {
                            start: Some(200),
                            end: Some(250)
                        },
                    },
                ]
        );
    }

    proptest! {
        #[test]
        fn route_table_is_deterministic(table_id in 0_u64..1_000_000) {
            let range_map = map();
            let table_id = TableId::new(table_id);

            prop_assert_eq!(range_map.route_table(table_id), range_map.route_table(table_id));
        }

        #[test]
        fn valid_two_range_maps_cover_all_generated_tables(boundary in 1_u64..1_000, table_id in 0_u64..10_000) {
            let range_map = RangeMap::new(
                tenant(),
                MapEpoch::ZERO,
                vec![
                    RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(boundary))),
                    RangeSpec::new(RangeId::new(1), TableId::new(boundary), None),
                ],
            )
            .unwrap();

            prop_assert!(range_map.route_table(TableId::new(table_id)).is_ok());
        }
    }
}

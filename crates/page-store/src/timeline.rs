//! Tenant and timeline path identifiers plus branch ancestry metadata.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use bytes::Bytes;
use crabka_object_store::{ObjectOps, ObjectStoreError};
use crabka_postgres_wal::Lsn;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Tenant identifier segment in page-store paths.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TenantId(String);

impl TenantId {
    /// Parses a non-empty tenant id containing only safe path characters.
    pub fn parse(raw: impl Into<String>) -> Result<Self, TimelinePathError> {
        let raw = raw.into();
        if !is_safe_timeline_path_id(&raw) {
            return Err(TimelinePathError::InvalidTenantId(raw));
        }

        Ok(Self(raw))
    }

    #[cfg(test)]
    pub(crate) fn from_raw_for_test(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
}

impl Serialize for TenantId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TenantId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Timeline identifier segment in page-store paths.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimelineId(String);

impl TimelineId {
    /// Parses a non-empty timeline id containing only safe path characters.
    pub fn parse(raw: impl Into<String>) -> Result<Self, TimelinePathError> {
        let raw = raw.into();
        if !is_safe_timeline_path_id(&raw) {
            return Err(TimelinePathError::InvalidTimelineId(raw));
        }

        Ok(Self(raw))
    }

    #[cfg(test)]
    pub(crate) fn from_raw_for_test(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
}

impl Serialize for TimelineId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TimelineId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl fmt::Display for TimelineId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for TimelinePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.prefix())
    }
}

/// Prefix identifying one tenant/timeline layer namespace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimelinePath {
    /// Tenant id.
    pub tenant_id: TenantId,
    /// Timeline id.
    pub timeline_id: TimelineId,
}

impl TimelinePath {
    /// Builds a timeline path from parsed ids.
    #[must_use]
    pub const fn new(tenant_id: TenantId, timeline_id: TimelineId) -> Self {
        Self {
            tenant_id,
            timeline_id,
        }
    }

    /// Returns the object/directory prefix for this timeline.
    #[must_use]
    pub fn prefix(&self) -> String {
        format!("pg/{}/{}", self.tenant_id, self.timeline_id)
    }

    /// Returns the object name for this timeline's durable ancestry metadata.
    #[must_use]
    pub fn meta_object_name(&self) -> String {
        format!("{}/{}", self.prefix(), TIMELINE_META_FILE)
    }
}

const TIMELINE_META_FILE: &str = "timeline.meta";

/// Direct timeline ancestry plus durable head metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimelineMeta {
    /// Metadata schema version.
    pub version: u16,
    /// Timeline described by this metadata object.
    pub timeline_id: TimelineId,
    /// Direct ancestor timeline and branch boundary, if this is a branch.
    pub ancestor: Option<TimelineAncestor>,
    /// Highest durable LSN known for this timeline.
    #[serde(with = "lsn_serde")]
    pub high_watermark_lsn: Lsn,
}

impl TimelineMeta {
    /// Current `timeline.meta` schema version.
    pub const VERSION: u16 = 1;

    /// Builds metadata for a root timeline.
    #[must_use]
    pub const fn root(timeline_id: TimelineId, high_watermark_lsn: Lsn) -> Self {
        Self {
            version: Self::VERSION,
            timeline_id,
            ancestor: None,
            high_watermark_lsn,
        }
    }

    /// Builds metadata for a branch timeline.
    #[must_use]
    pub const fn branch(
        timeline_id: TimelineId,
        ancestor_id: TimelineId,
        branch_lsn: Lsn,
        high_watermark_lsn: Lsn,
    ) -> Self {
        Self {
            version: Self::VERSION,
            timeline_id,
            ancestor: Some(TimelineAncestor {
                timeline_id: ancestor_id,
                branch_lsn,
            }),
            high_watermark_lsn,
        }
    }

    /// Serializes this metadata as deterministic JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, TimelineMetaError> {
        let mut bytes = serde_json::to_vec_pretty(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Parses metadata and rejects unsupported versions.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TimelineMetaError> {
        let meta: Self = serde_json::from_slice(bytes)?;
        if meta.version != Self::VERSION {
            return Err(TimelineMetaError::UnsupportedVersion(meta.version));
        }
        Ok(meta)
    }
}

/// Direct ancestor relation encoded in `timeline.meta`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimelineAncestor {
    /// Direct ancestor timeline id.
    pub timeline_id: TimelineId,
    /// Boundary LSN; the ancestor owns all history at or below this value.
    #[serde(with = "lsn_serde")]
    pub branch_lsn: Lsn,
}

/// Validated ancestry graph reconstructed from `timeline.meta` objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineGraph {
    metas: BTreeMap<TimelineId, TimelineMeta>,
    children: BTreeMap<TimelineId, BTreeSet<TimelineId>>,
}

impl TimelineGraph {
    /// Builds and validates a graph from metadata objects.
    pub fn new(metas: impl IntoIterator<Item = TimelineMeta>) -> Result<Self, TimelineMetaError> {
        let mut by_id = BTreeMap::new();
        for meta in metas {
            if by_id.insert(meta.timeline_id.clone(), meta).is_some() {
                return Err(TimelineMetaError::DuplicateTimeline);
            }
        }

        let mut children: BTreeMap<TimelineId, BTreeSet<TimelineId>> = BTreeMap::new();
        for meta in by_id.values() {
            let Some(ancestor) = &meta.ancestor else {
                continue;
            };
            let parent = by_id.get(&ancestor.timeline_id).ok_or_else(|| {
                TimelineMetaError::MissingAncestor {
                    timeline_id: meta.timeline_id.clone(),
                    ancestor_id: ancestor.timeline_id.clone(),
                }
            })?;
            if ancestor.branch_lsn > parent.high_watermark_lsn {
                return Err(TimelineMetaError::BranchLsnBeyondHighWatermark {
                    timeline_id: meta.timeline_id.clone(),
                    branch_lsn: ancestor.branch_lsn,
                    high_watermark_lsn: parent.high_watermark_lsn,
                });
            }
            children
                .entry(ancestor.timeline_id.clone())
                .or_default()
                .insert(meta.timeline_id.clone());
        }

        let graph = Self {
            metas: by_id,
            children,
        };
        graph.ensure_acyclic()?;
        Ok(graph)
    }

    /// Loads all metadata objects below one tenant prefix.
    pub async fn load(
        ops: &dyn ObjectOps,
        tenant_id: &TenantId,
    ) -> Result<Self, TimelineMetaError> {
        let prefix = ObjectPath::from(format!("pg/{tenant_id}"));
        let mut metas = Vec::new();
        for meta in ops.list(Some(&prefix)).await? {
            if !meta.location.as_ref().ends_with(TIMELINE_META_FILE) {
                continue;
            }
            let bytes = ops.get(&meta.location).await?;
            metas.push(TimelineMeta::from_bytes(&bytes)?);
        }
        Self::new(metas)
    }

    /// Returns direct ancestry for `timeline_id`, if present.
    #[must_use]
    pub fn ancestor_of(&self, timeline_id: &TimelineId) -> Option<&TimelineAncestor> {
        self.metas.get(timeline_id)?.ancestor.as_ref()
    }

    /// Returns direct descendants of `timeline_id` in deterministic order.
    #[must_use]
    pub fn descendants_of(&self, timeline_id: &TimelineId) -> Vec<TimelineId> {
        self.children
            .get(timeline_id)
            .map(|children| children.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns all descendants with the branch point that ties them to `timeline_id` history.
    #[must_use]
    pub fn descendant_branch_points_of(&self, timeline_id: &TimelineId) -> Vec<(TimelineId, Lsn)> {
        let mut branch_points = Vec::new();
        self.collect_descendant_branch_points(timeline_id, None, &mut branch_points);
        branch_points
    }

    /// Returns true when the timeline has one or more direct descendants.
    #[must_use]
    pub fn has_descendants(&self, timeline_id: &TimelineId) -> bool {
        self.children
            .get(timeline_id)
            .is_some_and(|children| !children.is_empty())
    }

    fn collect_descendant_branch_points(
        &self,
        timeline_id: &TimelineId,
        inherited_branch_lsn: Option<Lsn>,
        branch_points: &mut Vec<(TimelineId, Lsn)>,
    ) {
        let Some(children) = self.children.get(timeline_id) else {
            return;
        };
        for child_id in children {
            let Some(ancestor) = self.ancestor_of(child_id) else {
                continue;
            };
            let visible_branch_lsn = inherited_branch_lsn.unwrap_or(ancestor.branch_lsn);
            branch_points.push((child_id.clone(), visible_branch_lsn));
            self.collect_descendant_branch_points(
                child_id,
                Some(visible_branch_lsn),
                branch_points,
            );
        }
    }

    fn ensure_acyclic(&self) -> Result<(), TimelineMetaError> {
        for timeline_id in self.metas.keys() {
            let mut seen = BTreeSet::new();
            let mut current = timeline_id;
            while let Some(ancestor) = self.ancestor_of(current) {
                if !seen.insert(current.clone()) {
                    return Err(TimelineMetaError::Cycle {
                        timeline_id: timeline_id.clone(),
                    });
                }
                current = &ancestor.timeline_id;
            }
        }
        Ok(())
    }
}

/// Stores write-once timeline metadata at `pg/<tenant>/<timeline>/timeline.meta`.
pub async fn store_timeline_meta(
    ops: &dyn ObjectOps,
    tenant_id: &TenantId,
    meta: &TimelineMeta,
) -> Result<(), TimelineMetaError> {
    let path = TimelinePath::new(tenant_id.clone(), meta.timeline_id.clone());
    let object_name = ObjectPath::from(path.meta_object_name());
    match ops.head(&object_name).await {
        Ok(_) => return Err(TimelineMetaError::AlreadyExists(path)),
        Err(ObjectStoreError::NotFound(_)) => {}
        Err(err) => return Err(err.into()),
    }
    ops.put(&object_name, Bytes::from(meta.to_bytes()?)).await?;
    Ok(())
}

/// Loads one timeline metadata object.
pub async fn load_timeline_meta(
    ops: &dyn ObjectOps,
    path: &TimelinePath,
) -> Result<TimelineMeta, TimelineMetaError> {
    let object_name = ObjectPath::from(path.meta_object_name());
    let bytes = ops.get(&object_name).await?;
    TimelineMeta::from_bytes(&bytes)
}

/// Errors returned while reading, writing, or validating timeline metadata.
#[derive(Debug, thiserror::Error)]
pub enum TimelineMetaError {
    /// JSON serialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Object-store access failed.
    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),
    /// The metadata version is unsupported.
    #[error("unsupported timeline.meta version {0}")]
    UnsupportedVersion(u16),
    /// A metadata object already exists.
    #[error("timeline metadata already exists for {0}")]
    AlreadyExists(TimelinePath),
    /// Two metadata objects described the same timeline id.
    #[error("duplicate timeline metadata object")]
    DuplicateTimeline,
    /// A branch references a timeline missing from the graph.
    #[error("timeline {timeline_id} references missing ancestor {ancestor_id}")]
    MissingAncestor {
        /// Timeline whose metadata was invalid.
        timeline_id: TimelineId,
        /// Referenced ancestor id.
        ancestor_id: TimelineId,
    },
    /// A branch point is newer than the ancestor high watermark.
    #[error(
        "timeline {timeline_id} branch point {branch_lsn} exceeds ancestor high watermark {high_watermark_lsn}"
    )]
    BranchLsnBeyondHighWatermark {
        /// Timeline whose metadata was invalid.
        timeline_id: TimelineId,
        /// Requested branch point.
        branch_lsn: Lsn,
        /// Ancestor high watermark.
        high_watermark_lsn: Lsn,
    },
    /// The ancestry relation contains a cycle.
    #[error("timeline ancestry cycle includes {timeline_id}")]
    Cycle {
        /// Timeline involved in the cycle.
        timeline_id: TimelineId,
    },
}

/// Errors returned while parsing timeline path components.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TimelinePathError {
    /// Tenant ids must be non-empty `[A-Za-z0-9_-]+` strings.
    #[error("tenant id must contain only ASCII letters, digits, underscores, or hyphens: {0:?}")]
    InvalidTenantId(String),
    /// Timeline ids must be non-empty `[A-Za-z0-9_-]+` strings.
    #[error("timeline id must contain only ASCII letters, digits, underscores, or hyphens: {0:?}")]
    InvalidTimelineId(String),
}

fn is_safe_timeline_path_id(raw: &str) -> bool {
    !raw.is_empty()
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

mod lsn_serde {
    use crabka_postgres_wal::Lsn;
    use serde::{Deserialize, Deserializer, Serializer};

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) fn serialize<S>(lsn: &Lsn, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(lsn.value())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Lsn, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Lsn(u64::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_object_store::ObjectStoreClient;
    use object_store::memory::InMemory;

    use super::*;

    fn tenant() -> TenantId {
        TenantId::parse("tenant").expect("test tenant id is valid")
    }

    fn timeline_id(raw: &str) -> TimelineId {
        TimelineId::parse(raw).expect("test timeline id is valid")
    }

    fn in_memory_ops() -> ObjectStoreClient {
        ObjectStoreClient::new(Arc::new(InMemory::new()))
    }

    #[test]
    fn tenant_and_timeline_ids_reject_unsafe_path_components() {
        for raw in ["", ".", "..", "a/b", "a\\b", "with space"] {
            assert!(TenantId::parse(raw).is_err());
            assert!(TimelineId::parse(raw).is_err());
        }
    }

    #[test]
    fn tenant_and_timeline_ids_allow_normal_ids() {
        for raw in ["tenant", "tenant_01", "tenant-01", "Tenant01"] {
            assert!(TenantId::parse(raw).is_ok());
            assert!(TimelineId::parse(raw).is_ok());
        }
    }

    #[test]
    fn tenant_and_timeline_ids_reject_unsafe_deserialized_values() {
        assert!(serde_json::from_str::<TenantId>(r#""..""#).is_err());
        assert!(serde_json::from_str::<TimelineId>(r#""a/b""#).is_err());
    }

    #[tokio::test]
    async fn timeline_meta_round_trips_and_rebuilds_graph_from_objects() {
        let ops = in_memory_ops();
        let root = TimelineMeta::root(timeline_id("root"), Lsn(500));
        let child = TimelineMeta::branch(
            timeline_id("child"),
            timeline_id("root"),
            Lsn(400),
            Lsn(400),
        );

        store_timeline_meta(&ops, &tenant(), &root).await.unwrap();
        store_timeline_meta(&ops, &tenant(), &child).await.unwrap();
        let graph = TimelineGraph::load(&ops, &tenant()).await.unwrap();

        let ancestor = graph.ancestor_of(&timeline_id("child")).unwrap();
        assert!(ancestor.timeline_id == timeline_id("root"));
        assert!(ancestor.branch_lsn == Lsn(400));
        assert!(graph.descendants_of(&timeline_id("root")) == vec![timeline_id("child")]);
        assert!(graph.has_descendants(&timeline_id("root")));
    }

    #[tokio::test]
    async fn timeline_meta_is_write_once() {
        let ops = in_memory_ops();
        let root = TimelineMeta::root(timeline_id("root"), Lsn(10));

        store_timeline_meta(&ops, &tenant(), &root).await.unwrap();
        let duplicate = store_timeline_meta(&ops, &tenant(), &root).await;

        assert!(matches!(
            duplicate,
            Err(TimelineMetaError::AlreadyExists(_))
        ));
    }

    #[test]
    fn branch_beyond_parent_high_watermark_is_rejected_by_graph() {
        let root = TimelineMeta::root(timeline_id("root"), Lsn(100));
        let child = TimelineMeta::branch(
            timeline_id("child"),
            timeline_id("root"),
            Lsn(101),
            Lsn(101),
        );

        let err = TimelineGraph::new([root, child]).unwrap_err();

        assert!(matches!(
            err,
            TimelineMetaError::BranchLsnBeyondHighWatermark {
                branch_lsn: Lsn(101),
                high_watermark_lsn: Lsn(100),
                ..
            }
        ));
    }

    #[test]
    fn metadata_graph_reconstructs_multi_hop_ancestry() {
        let root = TimelineMeta::root(timeline_id("root"), Lsn(500));
        let child = TimelineMeta::branch(
            timeline_id("child"),
            timeline_id("root"),
            Lsn(400),
            Lsn(450),
        );
        let grandchild = TimelineMeta::branch(
            timeline_id("grandchild"),
            timeline_id("child"),
            Lsn(425),
            Lsn(425),
        );
        let graph = TimelineGraph::new([root, child, grandchild]).unwrap();

        let child_ancestor = graph.ancestor_of(&timeline_id("child")).unwrap();
        let grandchild_ancestor = graph.ancestor_of(&timeline_id("grandchild")).unwrap();

        assert!(child_ancestor.timeline_id == timeline_id("root"));
        assert!(child_ancestor.branch_lsn == Lsn(400));
        assert!(grandchild_ancestor.timeline_id == timeline_id("child"));
        assert!(grandchild_ancestor.branch_lsn == Lsn(425));
    }
}

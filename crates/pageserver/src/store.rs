//! In-memory timeline store backed by page-store immutable layers.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fmt,
    sync::Arc,
};

use bytes::Bytes;
use crabka_object_store::ObjectStoreClient;
use crabka_page_store::{
    LayerKind, LayerMap, LayerMapError, PAGE_SIZE, PageKey, ReconstructData, RelMetaKey,
    RelMetaKind, RelTag, SlruPageKey, TenantId, TimelineId, TimelineMeta, TimelinePath, Value,
    write_layer,
};
use crabka_postgres_wal::Lsn;
use object_store::memory::InMemory;

use crate::{PageServiceError, error::wrong_timeline};

/// Logical branch identifier layered above page-store timelines.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchId(String);

impl BranchId {
    /// Parses a non-empty branch id containing only safe path characters.
    pub fn parse(raw: impl Into<String>) -> Result<Self, BranchIdError> {
        let raw = raw.into();
        if !is_safe_id(&raw) {
            return Err(BranchIdError::InvalidBranchId(raw));
        }

        Ok(Self(raw))
    }
}

impl fmt::Display for BranchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Errors returned while parsing branch identifiers.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BranchIdError {
    /// Branch ids must be non-empty `[A-Za-z0-9_-]+` strings.
    #[error("branch id must contain only ASCII letters, digits, underscores, or hyphens: {0:?}")]
    InvalidBranchId(String),
}

/// Identifies one branch/timeline namespace served by [`InMemoryTimelineStore`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimelineKey {
    /// Logical branch id.
    pub branch_id: BranchId,
    /// Page-store timeline path.
    pub path: TimelinePath,
}

impl TimelineKey {
    /// Builds a timeline key from parsed components.
    #[must_use]
    pub const fn new(branch_id: BranchId, path: TimelinePath) -> Self {
        Self { branch_id, path }
    }

    /// Parses a timeline key at the boundary.
    pub fn parse(
        branch_id: impl Into<String>,
        tenant_id: impl Into<String>,
        timeline_id: impl Into<String>,
    ) -> Result<Self, TimelineKeyParseError> {
        let tenant_id = tenant_id.into();
        let timeline_id = timeline_id.into();
        Ok(Self {
            branch_id: BranchId::parse(branch_id)?,
            path: TimelinePath::new(
                TenantId::parse(tenant_id.clone())
                    .map_err(|err| TimelineKeyParseError::TimelinePath(err.to_string()))?,
                TimelineId::parse(timeline_id.clone())
                    .map_err(|err| TimelineKeyParseError::TimelinePath(err.to_string()))?,
            ),
        })
    }
}

impl fmt::Display for TimelineKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.branch_id, self.path.prefix())
    }
}

/// Errors returned while parsing a complete timeline key.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TimelineKeyParseError {
    /// Branch id parsing failed.
    #[error(transparent)]
    Branch(#[from] BranchIdError),
    /// Page-store path id parsing failed.
    #[error("timeline path id is invalid: {0}")]
    TimelinePath(String),
}

/// Mutable in-memory registry of branch/timeline page-store layer maps.
#[derive(Default)]
pub struct InMemoryTimelineStore {
    timelines: BTreeMap<TimelineKey, TimelineLayers>,
}

/// Branch ancestry metadata retained by the pageserver namespace seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineAncestry {
    /// Direct ancestor branch/timeline namespace.
    pub source_timeline: TimelineKey,
    /// Boundary LSN; the ancestor owns all history at or below this value.
    pub branch_lsn: Lsn,
}

impl InMemoryTimelineStore {
    /// Creates an empty store.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            timelines: BTreeMap::new(),
        }
    }

    /// Creates an empty timeline if it does not already exist.
    pub fn create_timeline(&mut self, timeline: &TimelineKey) -> bool {
        match self.timelines.entry(timeline.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(TimelineLayers::new(timeline.path.clone()));
                true
            }
        }
    }

    /// Creates a branched timeline with ancestry metadata and boundary checks.
    pub fn create_branch(
        &mut self,
        timeline: &TimelineKey,
        source_timeline: &TimelineKey,
        branch_lsn: Lsn,
    ) -> Result<bool, PageServiceError> {
        if timeline == source_timeline {
            return Ok(self.create_timeline(timeline));
        }

        let source = self.timelines.get(source_timeline).ok_or_else(|| {
            PageServiceError::BranchSourceNotFound {
                timeline: source_timeline.clone(),
            }
        })?;
        if branch_lsn > source.timeline_meta.high_watermark_lsn {
            return Err(PageServiceError::BranchLsnBeyondHead {
                timeline: source_timeline.clone(),
                branch_lsn,
                head_lsn: source.timeline_meta.high_watermark_lsn,
            });
        }

        match self.timelines.entry(timeline.clone()) {
            Entry::Occupied(_) => Ok(false),
            Entry::Vacant(entry) => {
                let timeline_meta =
                    TimelineMeta::root(timeline.path.timeline_id.clone(), branch_lsn);
                let ancestry = TimelineAncestry {
                    source_timeline: source_timeline.clone(),
                    branch_lsn,
                };
                entry.insert(TimelineLayers::branch(
                    timeline.path.clone(),
                    timeline_meta,
                    ancestry,
                ));
                Ok(true)
            }
        }
    }

    /// Lists created timelines, ordered by their typed namespace key.
    #[must_use]
    pub fn list_timelines(&self) -> Vec<TimelineKey> {
        self.timelines.keys().cloned().collect()
    }

    /// Returns direct ancestry metadata for a timeline, when it is a branch.
    pub fn ancestry(
        &self,
        timeline: &TimelineKey,
    ) -> Result<Option<TimelineAncestry>, PageServiceError> {
        Ok(self.timeline_layers(timeline)?.ancestor.clone())
    }

    /// Deletes a timeline namespace if it exists.
    pub fn delete_timeline(&mut self, timeline: &TimelineKey) -> Result<bool, PageServiceError> {
        if self.timelines.values().any(|layers| {
            layers
                .ancestor
                .as_ref()
                .is_some_and(|ancestor| ancestor.source_timeline == *timeline)
        }) {
            return Err(PageServiceError::TimelineHasDescendants {
                timeline: timeline.clone(),
            });
        }

        Ok(self.timelines.remove(timeline).is_some())
    }

    /// Adds a full-image layer containing one page.
    pub async fn put_image(
        &mut self,
        timeline: &TimelineKey,
        key: PageKey,
        lsn: Lsn,
        page: Bytes,
    ) -> Result<(), PageServiceError> {
        if page.len() != PAGE_SIZE {
            return Err(PageServiceError::wrong_image_size(key, page.len()));
        }

        self.put_value(timeline, key, lsn, LayerKind::Image, Value::Image(page))
            .await
    }

    /// Adds a WAL delta layer containing one synthetic record.
    pub async fn put_wal(
        &mut self,
        timeline: &TimelineKey,
        key: PageKey,
        lsn: Lsn,
        will_init: bool,
        rec: Bytes,
    ) -> Result<(), PageServiceError> {
        self.put_value(
            timeline,
            key,
            lsn,
            LayerKind::Delta,
            Value::Wal { will_init, rec },
        )
        .await
    }

    /// Adds exact relation-size metadata visible at `lsn` and later LSNs.
    pub fn put_relation_size(
        &mut self,
        timeline: &TimelineKey,
        rel: RelTag,
        lsn: Lsn,
        blocks: u32,
    ) -> Result<(), PageServiceError> {
        let key = RelMetaKey::new(rel, RelMetaKind::Size);
        let layers = self.timeline_layers_mut(timeline)?;
        ensure_branch_write_is_above_boundary(timeline, layers, lsn)?;
        layers
            .metadata
            .put_relmeta(key, lsn, RelMetaValue::RelationSize(blocks));
        layers.max_lsn = layers.max_lsn.max(lsn);
        layers.timeline_meta.high_watermark_lsn = layers.timeline_meta.high_watermark_lsn.max(lsn);
        Ok(())
    }

    /// Adds opaque relation metadata visible at `lsn` and later LSNs.
    pub fn put_relmeta(
        &mut self,
        timeline: &TimelineKey,
        key: RelMetaKey,
        lsn: Lsn,
        metadata: Bytes,
    ) -> Result<(), PageServiceError> {
        let layers = self.timeline_layers_mut(timeline)?;
        ensure_branch_write_is_above_boundary(timeline, layers, lsn)?;
        layers
            .metadata
            .put_relmeta(key, lsn, RelMetaValue::Opaque(metadata));
        layers.max_lsn = layers.max_lsn.max(lsn);
        layers.timeline_meta.high_watermark_lsn = layers.timeline_meta.high_watermark_lsn.max(lsn);
        Ok(())
    }

    /// Adds one exact SLRU page visible at `lsn` and later LSNs.
    pub fn put_slru_page(
        &mut self,
        timeline: &TimelineKey,
        key: SlruPageKey,
        lsn: Lsn,
        page: Bytes,
    ) -> Result<(), PageServiceError> {
        if page.len() != PAGE_SIZE {
            return Err(PageServiceError::WrongSlruPageSize {
                key,
                expected: PAGE_SIZE,
                actual: page.len(),
            });
        }

        let layers = self.timeline_layers_mut(timeline)?;
        ensure_branch_write_is_above_boundary(timeline, layers, lsn)?;
        layers.metadata.put_slru_page(key, lsn, page);
        layers.max_lsn = layers.max_lsn.max(lsn);
        layers.timeline_meta.high_watermark_lsn = layers.timeline_meta.high_watermark_lsn.max(lsn);
        Ok(())
    }

    /// Returns reconstruction data for one page at `lsn`.
    pub async fn get_reconstruct_data(
        &self,
        timeline: &TimelineKey,
        key: PageKey,
        lsn: Lsn,
    ) -> Result<ReconstructData, PageServiceError> {
        let layers = self.timeline_layers(timeline)?;
        if let Some(ancestry) = &layers.ancestor
            && lsn <= ancestry.branch_lsn
        {
            self.ensure_ancestor_exists(&ancestry.source_timeline)?;
            return Box::pin(self.get_reconstruct_data(&ancestry.source_timeline, key, lsn)).await;
        }

        let data = layers
            .map
            .get_reconstruct_data(&layers.ops, key, lsn)
            .await
            .map_err(|err| PageServiceError::from_layer_map_error(timeline, err));
        if !matches!(data, Err(PageServiceError::PageNotFound { .. })) {
            return data;
        }

        let Some(ancestry) = &layers.ancestor else {
            return data;
        };
        self.ensure_ancestor_exists(&ancestry.source_timeline)?;
        Box::pin(self.get_reconstruct_data(&ancestry.source_timeline, key, ancestry.branch_lsn))
            .await
    }

    /// Returns page keys that have any page-store content visible at `lsn`.
    pub(crate) fn visible_page_keys(
        &self,
        timeline: &TimelineKey,
        lsn: Lsn,
    ) -> Result<Vec<PageKey>, PageServiceError> {
        let layers = self.timeline_layers(timeline)?;
        let mut keys: Vec<_> = layers
            .page_lsns
            .iter()
            .filter_map(|(key, lsns)| lsns.range(..=lsn).next_back().map(|_| *key))
            .collect();
        if let Some(ancestry) = &layers.ancestor {
            let ancestor_visibility_lsn = ancestry.branch_lsn.min(lsn);
            self.ensure_ancestor_exists(&ancestry.source_timeline)?;
            keys.extend(
                self.visible_page_keys(&ancestry.source_timeline, ancestor_visibility_lsn)?,
            );
            keys.sort_unstable();
            keys.dedup();
        }
        Ok(keys)
    }

    pub(crate) fn relation_size(
        &self,
        timeline: &TimelineKey,
        rel: RelTag,
        lsn: Lsn,
    ) -> Result<u32, PageServiceError> {
        let layers = self.timeline_layers(timeline)?;
        if let Some(blocks) = layers.metadata.relation_size(rel, lsn) {
            return Ok(blocks);
        }
        if let Some(ancestry) = &layers.ancestor {
            self.ensure_ancestor_exists(&ancestry.source_timeline)?;
            return self.relation_size(
                &ancestry.source_timeline,
                rel,
                ancestry.branch_lsn.min(lsn),
            );
        }

        Err(PageServiceError::RelationSizeMissing {
            timeline: timeline.clone(),
            rel,
            lsn,
        })
    }

    pub(crate) fn relmeta(
        &self,
        timeline: &TimelineKey,
        key: RelMetaKey,
        lsn: Lsn,
    ) -> Result<Bytes, PageServiceError> {
        let layers = self.timeline_layers(timeline)?;
        if let Some(metadata) = layers.metadata.relmeta(key, lsn) {
            return Ok(metadata.into_bytes());
        }
        if let Some(ancestry) = &layers.ancestor {
            self.ensure_ancestor_exists(&ancestry.source_timeline)?;
            return self.relmeta(&ancestry.source_timeline, key, ancestry.branch_lsn.min(lsn));
        }

        Err(PageServiceError::RelMetaMissing {
            timeline: timeline.clone(),
            key,
            lsn,
        })
    }

    pub(crate) fn slru_page(
        &self,
        timeline: &TimelineKey,
        key: SlruPageKey,
        lsn: Lsn,
    ) -> Result<Bytes, PageServiceError> {
        let layers = self.timeline_layers(timeline)?;
        if let Some(page) = layers.metadata.slru_page(key, lsn) {
            return Ok(page);
        }
        if let Some(ancestry) = &layers.ancestor {
            self.ensure_ancestor_exists(&ancestry.source_timeline)?;
            return self.slru_page(&ancestry.source_timeline, key, ancestry.branch_lsn.min(lsn));
        }

        Err(PageServiceError::SlruPageMissing {
            timeline: timeline.clone(),
            key,
            lsn,
        })
    }

    pub(crate) fn basebackup_metadata(
        &self,
        timeline: &TimelineKey,
        lsn: Lsn,
    ) -> Result<BasebackupMetadataSnapshot, PageServiceError> {
        let layers = self.timeline_layers(timeline)?;
        let mut snapshot = layers.metadata.basebackup_metadata(lsn);
        if let Some(ancestry) = &layers.ancestor {
            self.ensure_ancestor_exists(&ancestry.source_timeline)?;
            let inherited =
                self.basebackup_metadata(&ancestry.source_timeline, ancestry.branch_lsn.min(lsn))?;
            snapshot.merge_inherited(inherited);
        }
        Ok(snapshot)
    }

    async fn put_value(
        &mut self,
        timeline: &TimelineKey,
        key: PageKey,
        lsn: Lsn,
        kind: LayerKind,
        value: Value,
    ) -> Result<(), PageServiceError> {
        let layers = self.timeline_layers_mut(timeline)?;
        ensure_branch_write_is_above_boundary(timeline, layers, lsn)?;
        let desc = write_layer(&layers.ops, &timeline.path, kind, &[(key, lsn, value)])
            .await
            .map_err(LayerMapError::from)?;
        if desc.timeline != layers.map_timeline {
            return Err(wrong_timeline(&layers.map_timeline, &desc.timeline).into());
        }
        layers.map.insert(desc)?;
        layers.page_lsns.entry(key).or_default().insert(lsn);
        layers.max_lsn = layers.max_lsn.max(lsn);
        layers.timeline_meta.high_watermark_lsn = layers.timeline_meta.high_watermark_lsn.max(lsn);
        Ok(())
    }

    fn ensure_ancestor_exists(&self, ancestor: &TimelineKey) -> Result<(), PageServiceError> {
        if self.timelines.contains_key(ancestor) {
            return Ok(());
        }

        Err(PageServiceError::TimelineNotFound {
            timeline: ancestor.clone(),
        })
    }

    fn timeline_layers(&self, timeline: &TimelineKey) -> Result<&TimelineLayers, PageServiceError> {
        self.timelines
            .get(timeline)
            .ok_or_else(|| PageServiceError::TimelineNotFound {
                timeline: timeline.clone(),
            })
    }

    fn timeline_layers_mut(
        &mut self,
        timeline: &TimelineKey,
    ) -> Result<&mut TimelineLayers, PageServiceError> {
        self.timelines
            .get_mut(timeline)
            .ok_or_else(|| PageServiceError::TimelineNotFound {
                timeline: timeline.clone(),
            })
    }
}

pub(crate) struct BasebackupMetadataSnapshot {
    pub(crate) relmeta: Vec<(RelMetaKey, Bytes)>,
    pub(crate) slru_pages: Vec<(SlruPageKey, Bytes)>,
}

impl BasebackupMetadataSnapshot {
    fn merge_inherited(&mut self, inherited: Self) {
        let mut relmeta: BTreeMap<_, _> = inherited.relmeta.into_iter().collect();
        relmeta.extend(self.relmeta.drain(..));
        self.relmeta = relmeta.into_iter().collect();

        let mut slru_pages: BTreeMap<_, _> = inherited.slru_pages.into_iter().collect();
        slru_pages.extend(self.slru_pages.drain(..));
        self.slru_pages = slru_pages.into_iter().collect();
    }
}

struct TimelineLayers {
    ops: ObjectStoreClient,
    map: LayerMap,
    map_timeline: TimelinePath,
    metadata: TimelineMetadata,
    timeline_meta: TimelineMeta,
    ancestor: Option<TimelineAncestry>,
    page_lsns: BTreeMap<PageKey, BTreeSet<Lsn>>,
    max_lsn: Lsn,
}

impl TimelineLayers {
    fn new(path: TimelinePath) -> Self {
        let timeline_meta = TimelineMeta::root(path.timeline_id.clone(), Lsn(0));
        Self {
            ops: ObjectStoreClient::new(Arc::new(InMemory::new())),
            map: LayerMap::new(path.clone()),
            map_timeline: path,
            metadata: TimelineMetadata::new(),
            timeline_meta,
            ancestor: None,
            page_lsns: BTreeMap::new(),
            max_lsn: Lsn(0),
        }
    }

    fn branch(path: TimelinePath, timeline_meta: TimelineMeta, ancestry: TimelineAncestry) -> Self {
        Self {
            timeline_meta,
            ancestor: Some(ancestry),
            ..Self::new(path)
        }
    }
}

fn ensure_branch_write_is_above_boundary(
    timeline: &TimelineKey,
    layers: &TimelineLayers,
    lsn: Lsn,
) -> Result<(), PageServiceError> {
    let Some(ancestry) = &layers.ancestor else {
        return Ok(());
    };
    if lsn > ancestry.branch_lsn {
        return Ok(());
    }

    Err(PageServiceError::BranchBoundaryViolation {
        timeline: timeline.clone(),
        lsn,
        branch_lsn: ancestry.branch_lsn,
    })
}

struct TimelineMetadata {
    relmeta: BTreeMap<RelMetaKey, BTreeMap<Lsn, RelMetaValue>>,
    slru_pages: BTreeMap<SlruPageKey, BTreeMap<Lsn, Bytes>>,
}

impl TimelineMetadata {
    const fn new() -> Self {
        Self {
            relmeta: BTreeMap::new(),
            slru_pages: BTreeMap::new(),
        }
    }

    fn put_relmeta(&mut self, key: RelMetaKey, lsn: Lsn, metadata: RelMetaValue) {
        self.relmeta.entry(key).or_default().insert(lsn, metadata);
    }

    fn put_slru_page(&mut self, key: SlruPageKey, lsn: Lsn, page: Bytes) {
        self.slru_pages.entry(key).or_default().insert(lsn, page);
    }

    fn relation_size(&self, rel: RelTag, lsn: Lsn) -> Option<u32> {
        let key = RelMetaKey::new(rel, RelMetaKind::Size);
        let RelMetaValue::RelationSize(blocks) = self.relmeta(key, lsn)? else {
            return None;
        };
        Some(blocks)
    }

    fn relmeta(&self, key: RelMetaKey, lsn: Lsn) -> Option<RelMetaValue> {
        self.relmeta
            .get(&key)?
            .range(..=lsn)
            .next_back()
            .map(|(_, value)| value.clone())
    }

    fn slru_page(&self, key: SlruPageKey, lsn: Lsn) -> Option<Bytes> {
        self.slru_pages
            .get(&key)?
            .range(..=lsn)
            .next_back()
            .map(|(_, page)| page.clone())
    }

    fn basebackup_metadata(&self, lsn: Lsn) -> BasebackupMetadataSnapshot {
        let relmeta = self
            .relmeta
            .iter()
            .filter_map(|(key, values)| {
                values
                    .range(..=lsn)
                    .next_back()
                    .map(|(_, value)| (*key, value.clone().into_bytes()))
            })
            .collect();
        let slru_pages = self
            .slru_pages
            .iter()
            .filter_map(|(key, values)| {
                values
                    .range(..=lsn)
                    .next_back()
                    .map(|(_, page)| (*key, page.clone()))
            })
            .collect();

        BasebackupMetadataSnapshot {
            relmeta,
            slru_pages,
        }
    }
}

#[derive(Clone)]
enum RelMetaValue {
    RelationSize(u32),
    Opaque(Bytes),
}

impl RelMetaValue {
    fn into_bytes(self) -> Bytes {
        match self {
            Self::RelationSize(blocks) => Bytes::copy_from_slice(&blocks.to_le_bytes()),
            Self::Opaque(metadata) => metadata,
        }
    }
}

fn is_safe_id(raw: &str) -> bool {
    !raw.is_empty()
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn timeline(branch: &str, timeline: &str) -> TimelineKey {
        TimelineKey::new(
            BranchId::parse(branch).expect("test branch id is valid"),
            TimelinePath::new(
                TenantId::parse("tenant").expect("test tenant id is valid"),
                TimelineId::parse(timeline).expect("test timeline id is valid"),
            ),
        )
    }

    fn page_key() -> PageKey {
        PageKey::new(1663, 5, 16_384, 0, 7)
    }

    fn page(byte: u8) -> Bytes {
        Bytes::from(vec![byte; PAGE_SIZE])
    }

    #[tokio::test]
    async fn child_inherits_parent_history_below_branch_boundary() {
        let root = timeline("main", "root");
        let child = timeline("child", "child");
        let mut store = InMemoryTimelineStore::new();
        assert!(store.create_timeline(&root));
        store
            .put_image(&root, page_key(), Lsn(10), page(1))
            .await
            .unwrap();
        store.create_branch(&child, &root, Lsn(10)).unwrap();

        let inherited = store
            .get_reconstruct_data(&child, page_key(), Lsn(10))
            .await
            .unwrap();

        assert!(inherited.base == Some((Lsn(10), page(1))));
        assert!(inherited.deltas.is_empty());
    }

    #[tokio::test]
    async fn branch_boundary_rejects_child_writes_at_or_below_branch_lsn() {
        let root = timeline("main", "root");
        let child = timeline("child", "child");
        let mut store = InMemoryTimelineStore::new();
        assert!(store.create_timeline(&root));
        store
            .put_image(&root, page_key(), Lsn(20), page(1))
            .await
            .unwrap();
        store.create_branch(&child, &root, Lsn(20)).unwrap();

        let response = store.put_image(&child, page_key(), Lsn(20), page(2)).await;

        assert!(matches!(
            response,
            Err(PageServiceError::BranchBoundaryViolation {
                lsn: Lsn(20),
                branch_lsn: Lsn(20),
                ..
            })
        ));
    }

    #[test]
    fn deleting_timeline_with_descendant_is_rejected() {
        let root = timeline("main", "root");
        let child = timeline("child", "child");
        let mut store = InMemoryTimelineStore::new();
        assert!(store.create_timeline(&root));
        store.create_branch(&child, &root, Lsn(0)).unwrap();

        let response = store.delete_timeline(&root);

        assert!(matches!(
            response,
            Err(PageServiceError::TimelineHasDescendants { .. })
        ));
        assert!(store.delete_timeline(&child).unwrap());
        assert!(store.delete_timeline(&root).unwrap());
    }

    #[test]
    fn ancestry_uses_pageserver_namespace_mapping() {
        let root = timeline("main", "root");
        let child = timeline("child", "child");
        let mut store = InMemoryTimelineStore::new();
        assert!(store.create_timeline(&root));
        store.create_branch(&child, &root, Lsn(0)).unwrap();

        let ancestry = store.ancestry(&child).unwrap().unwrap();

        assert!(ancestry.source_timeline == root);
        assert!(ancestry.branch_lsn == Lsn(0));
    }

    #[tokio::test]
    async fn branch_inherits_from_matching_branch_namespace_when_timeline_ids_match() {
        let main_root = timeline("main", "shared");
        let alternate_root = timeline("alternate", "shared");
        let child = timeline("child", "child");
        let mut store = InMemoryTimelineStore::new();
        assert!(store.create_timeline(&main_root));
        assert!(store.create_timeline(&alternate_root));
        store
            .put_image(&main_root, page_key(), Lsn(10), page(1))
            .await
            .unwrap();
        store
            .put_image(&alternate_root, page_key(), Lsn(10), page(2))
            .await
            .unwrap();
        store
            .create_branch(&child, &alternate_root, Lsn(10))
            .unwrap();

        let inherited = store
            .get_reconstruct_data(&child, page_key(), Lsn(10))
            .await
            .unwrap();

        assert!(inherited.base == Some((Lsn(10), page(2))));
        assert!(store.delete_timeline(&main_root).unwrap());
        assert!(matches!(
            store.delete_timeline(&alternate_root),
            Err(PageServiceError::TimelineHasDescendants { .. })
        ));
    }
}

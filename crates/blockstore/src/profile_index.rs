//! Profiles block index.
//!
//! `ProfileIndex` embeds [`Index`] for label postings and matcher resolution,
//! then layers the profile-type lookup and stacktrace-partition map required by
//! Pyroscope-compatible profiles queries.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::block_index::BlockIndex;
use crate::error::{BlockStoreError, Result};
use crate::index::Index;
use crate::labels::{Labels, SeriesFingerprint};
use crate::matcher::LabelMatcher;

/// Reserved label carrying the 5-part profile type string.
pub const LABEL_PROFILE_TYPE: &str = "__profile_type__";

/// Maximum byte size of a profile-index snapshot object accepted by
/// [`ProfileIndex::load`].
///
/// Like [`crate::Index::load`], a profile-index snapshot is fully buffered in
/// memory before `serde_json` parsing; a corrupt or maliciously oversized
/// object from shared storage could otherwise OOM the process. The object is
/// `head()`ed first and rejected above this cap, mirroring the profiles gunzip
/// `max_decompressed` pattern. Defaults to 256 MiB.
pub const MAX_PROFILE_INDEX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;

#[derive(Default, Serialize, Deserialize)]
struct TenantProfileExtras {
    profile_types: BTreeMap<String, BTreeSet<SeriesFingerprint>>,
}

/// Profile-specific index state over the reusable series postings index.
#[derive(Default, Serialize, Deserialize)]
pub struct ProfileIndex {
    series: Index,
    extras: BTreeMap<String, TenantProfileExtras>,
    block_partitions: BTreeMap<String, Vec<u64>>,
}

impl ProfileIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels) {
        self.series.add_series(tenant, fp, labels);
        if let Some(profile_type) = labels.get(LABEL_PROFILE_TYPE) {
            self.extras
                .entry(tenant.to_string())
                .or_default()
                .profile_types
                .entry(profile_type.to_string())
                .or_default()
                .insert(fp);
        }
    }

    pub fn resolve(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        self.series.resolve(tenant, matchers)
    }

    pub fn matching_fingerprints(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        self.series.matching_fingerprints(tenant, matchers)
    }

    pub fn select_fingerprints(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let profile_fps = self.fingerprints_for_profile_type(tenant, profile_type);
        if matchers.is_empty() {
            return Ok(profile_fps);
        }
        let label_fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(profile_fps.intersection(&label_fps).copied().collect())
    }

    #[must_use]
    pub fn candidate_blocks_for_series(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        self.series
            .candidate_blocks_for_series(tenant, fps, min_ts, max_ts)
    }

    #[must_use]
    pub fn block_time_bounds(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
        self.series.block_time_bounds(tenant, min_ts, max_ts)
    }

    #[must_use]
    pub fn profile_types(&self, tenant: &str) -> Vec<String> {
        self.extras
            .get(tenant)
            .map(|extras| extras.profile_types.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn profile_types_for_time(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        self.extras
            .get(tenant)
            .map(|extras| {
                extras
                    .profile_types
                    .iter()
                    .filter(|(_, fps)| {
                        !self
                            .candidate_blocks_for_series(tenant, fps, min_ts, max_ts)
                            .is_empty()
                    })
                    .map(|(profile_type, _)| profile_type.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn label_values_for_time(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
    ) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let active = self.active_fingerprints_for_time(tenant, &fps, min_ts, max_ts);
        Ok(self
            .series
            .label_values_for_fingerprints(tenant, name, &active))
    }

    #[must_use]
    pub fn label_values_for_fingerprints(
        &self,
        tenant: &str,
        name: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        self.series.label_values_for_fingerprints(tenant, name, fps)
    }

    #[must_use]
    pub fn profile_types_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        self.extras
            .get(tenant)
            .map(|extras| {
                extras
                    .profile_types
                    .iter()
                    .filter(|(_, type_fps)| !type_fps.is_disjoint(fps))
                    .map(|(profile_type, _)| profile_type.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn label_names_for_time(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
    ) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let active = self.active_fingerprints_for_time(tenant, &fps, min_ts, max_ts);
        Ok(self.series.label_names_for_fingerprints(tenant, &active))
    }

    #[must_use]
    pub fn label_names_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        self.series.label_names_for_fingerprints(tenant, fps)
    }

    pub fn series_for_time(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        min_ts: i64,
        max_ts: i64,
    ) -> Result<Vec<Vec<(String, String)>>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let active = self.active_fingerprints_for_time(tenant, &fps, min_ts, max_ts);
        Ok(self
            .series
            .series_for_fingerprints(tenant, &active, label_names))
    }

    #[must_use]
    pub fn series_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        label_names: &[String],
    ) -> Vec<Vec<(String, String)>> {
        self.series
            .series_for_fingerprints(tenant, fps, label_names)
    }

    #[must_use]
    pub fn fingerprints_for_profile_type(
        &self,
        tenant: &str,
        profile_type: &str,
    ) -> BTreeSet<SeriesFingerprint> {
        self.extras
            .get(tenant)
            .and_then(|extras| extras.profile_types.get(profile_type))
            .cloned()
            .unwrap_or_default()
    }

    fn active_fingerprints_for_time(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> BTreeSet<SeriesFingerprint> {
        fps.iter()
            .copied()
            .filter(|fp| {
                !self
                    .candidate_blocks_for_series(tenant, &BTreeSet::from([*fp]), min_ts, max_ts)
                    .is_empty()
            })
            .collect()
    }

    pub fn add_profile_block(&mut self, _tenant: &str, object_key: &str, partitions: Vec<u64>) {
        self.block_partitions
            .insert(object_key.to_string(), partitions);
    }

    pub fn replace_profile_blocks(
        &mut self,
        tenant: &str,
        remove_keys: &[String],
        add: &[(BlockMeta, Vec<u64>)],
    ) {
        for key in remove_keys {
            self.block_partitions.remove(key);
        }
        let metas = add.iter().map(|(meta, _)| meta.clone()).collect::<Vec<_>>();
        self.series.replace_blocks(tenant, remove_keys, &metas);
        for (meta, partitions) in add {
            self.add_profile_block(tenant, &meta.object_key, partitions.clone());
        }
    }

    #[must_use]
    pub fn stacktrace_partitions(&self, object_key: &str) -> Vec<u64> {
        self.block_partitions
            .get(object_key)
            .cloned()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> Vec<String> {
        self.series.label_names(tenant)
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.series.label_values(tenant, name)
    }

    pub fn label_names_for(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<String>> {
        self.series.label_names_for(tenant, matchers)
    }

    pub fn label_values_for(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>> {
        self.series.label_values_for(tenant, name, matchers)
    }

    pub fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
    ) -> Result<Vec<Vec<(String, String)>>> {
        self.series.series_projected(tenant, matchers, label_names)
    }

    #[must_use]
    pub fn all_blocks(&self) -> Vec<BlockMeta> {
        self.series.all_blocks_unscoped()
    }

    pub async fn save(&self, store: &Arc<dyn ObjectStore>, key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        store.put(&Path::from(key), PutPayload::from(bytes)).await?;
        Ok(())
    }

    pub async fn load(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Self> {
        Self::load_with_cap(store, key, MAX_PROFILE_INDEX_SNAPSHOT_BYTES).await
    }

    async fn load_with_cap(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        max_bytes: usize,
    ) -> Result<Self> {
        let path = Path::from(key);
        let meta = store.head(&path).await?;
        if meta.size > max_bytes as u64 {
            return Err(BlockStoreError::InvalidBlock(format!(
                "profile index snapshot `{key}` is {} bytes, exceeds cap of {max_bytes} bytes",
                meta.size
            )));
        }
        let bytes = store.get(&path).await?.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

impl BlockIndex for ProfileIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        BlockIndex::add_block(&mut self.series, meta);
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        BlockIndex::candidate_blocks(&self.series, tenant, min_ts, max_ts)
    }

    fn block_count(&self, tenant: &str) -> usize {
        BlockIndex::block_count(&self.series, tenant)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::labels::Labels;
    use crate::matcher::{LabelMatcher, MatchOp};

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::from_pairs(pairs.iter().copied())
    }

    fn seed() -> ProfileIndex {
        let mut index = ProfileIndex::new();
        let cpu = labels(&[
            ("__name__", "process_cpu"),
            (
                "__profile_type__",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
            ),
            ("service_name", "checkout"),
        ]);
        let heap = labels(&[
            ("__name__", "memory"),
            ("__profile_type__", "memory:alloc_space:bytes:space:bytes"),
            ("service_name", "checkout"),
        ]);
        index.add_series("t", cpu.fingerprint(), &cpu);
        index.add_series("t", heap.fingerprint(), &heap);
        index
    }

    #[test]
    fn profile_types_lists_distinct_type_strings() {
        let index = seed();
        let mut types = index.profile_types("t");
        types.sort();
        assert!(
            types
                == vec![
                    "memory:alloc_space:bytes:space:bytes".to_string(),
                    "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
                ]
        );
        assert!(index.profile_types("nope").is_empty());
    }

    #[test]
    fn profile_type_index_maps_type_to_its_series() {
        let index = seed();
        let cpu_fps =
            index.fingerprints_for_profile_type("t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds");
        assert!(cpu_fps.len() == 1);
        let heap_fps =
            index.fingerprints_for_profile_type("t", "memory:alloc_space:bytes:space:bytes");
        assert!(cpu_fps.is_disjoint(&heap_fps));
    }

    #[test]
    fn resolve_reuses_series_postings() {
        let index = seed();
        let got = index
            .resolve(
                "t",
                &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")],
            )
            .unwrap();
        assert!(got.len() == 2);
    }

    #[test]
    fn stacktrace_partition_map_records_block_partitions() {
        let mut index = seed();
        index.add_profile_block("t", "blocks/p1.parquet", vec![0, 1, 2]);
        assert!(index.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1, 2]);
        assert!(
            index
                .stacktrace_partitions("blocks/absent.parquet")
                .is_empty()
        );
    }

    #[test]
    fn replace_profile_blocks_removes_old_partition_maps() {
        let mut index = seed();
        let labels = labels(&[
            ("__name__", "process_cpu"),
            (
                "__profile_type__",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
            ),
            ("service_name", "checkout"),
        ]);
        let fp = labels.fingerprint();
        index.add_block(&BlockMeta {
            tenant: "t".to_string(),
            object_key: "old.parquet".to_string(),
            min_ts: 0,
            max_ts: 10,
            row_count: 1,
            fingerprints: vec![fp],
        });
        index.add_profile_block("t", "old.parquet", vec![0]);

        index.replace_profile_blocks(
            "t",
            &["old.parquet".to_string()],
            &[(
                BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "new.parquet".to_string(),
                    min_ts: 0,
                    max_ts: 10,
                    row_count: 1,
                    fingerprints: vec![fp],
                },
                vec![99],
            )],
        );

        assert!(index.stacktrace_partitions("old.parquet").is_empty());
        assert!(index.stacktrace_partitions("new.parquet") == vec![99]);
        assert!(BlockIndex::candidate_blocks(&index, "t", 0, 10) == vec!["new.parquet"]);
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        use object_store::memory::InMemory;

        let mut index = seed();
        index.add_profile_block("t", "blocks/p1.parquet", vec![0, 1]);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        index.save(&store, "index/profiles.json").await.unwrap();
        let loaded = ProfileIndex::load(&store, "index/profiles.json")
            .await
            .unwrap();
        assert!(loaded.profile_types("t").len() == 2);
        assert!(loaded.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1]);
    }

    #[tokio::test]
    async fn load_rejects_over_cap_snapshot() {
        use object_store::memory::InMemory;

        let index = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        index.save(&store, "index/profiles.json").await.unwrap();

        // A tiny cap stands in for the production cap so the test need not
        // materialize an over-cap object; the real snapshot is well above 1 byte.
        let size = store
            .head(&Path::from("index/profiles.json"))
            .await
            .unwrap()
            .size;
        assert!(size > 1);

        let got = ProfileIndex::load_with_cap(&store, "index/profiles.json", 1).await;
        assert!(got.is_err());

        // A cap at/above the real size still loads.
        let loaded = ProfileIndex::load_with_cap(
            &store,
            "index/profiles.json",
            usize::try_from(size).unwrap(),
        )
        .await
        .unwrap();
        assert!(loaded.profile_types("t").len() == 2);
    }
}

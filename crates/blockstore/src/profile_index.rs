//! Profiles block index.
//!
//! `ProfileIndex` embeds [`Index`] for label postings and matcher resolution.
//! It then adds the profile-type lookup and the stacktrace-partition map that
//! Pyroscope-compatible profiles queries need.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    block::BlockMeta,
    block_index::BlockIndex,
    error::{BlockStoreError, Result},
    index::Index,
    index_snapshot::{
        DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain, latest_index_snapshot_path,
        put_index_snapshot,
    },
    labels::{Labels, SeriesFingerprint},
    matcher::LabelMatcher,
};

/// Reserved label carrying the 5-part profile type string.
pub const LABEL_PROFILE_TYPE: &str = "__profile_type__";

/// Maximum byte size of a profile-index snapshot object accepted by
/// [`ProfileIndex::load`].
///
/// As with [`crate::Index::load`], a load fully buffers a profile-index
/// snapshot in memory before the `serde_json` parse. A corrupt or maliciously
/// oversized object from shared storage could otherwise OOM the process. The
/// loader `head()`s the object first and rejects it above this cap. This
/// mirrors the profiles gunzip `max_decompressed` pattern. The default is
/// 256 MiB.
pub const MAX_PROFILE_INDEX_SNAPSHOT_BYTES: ByteSize = mebibytes(256);

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

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn resolve(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        self.series.resolve(tenant, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn matching_fingerprints(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        self.series.matching_fingerprints(tenant, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
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

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
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

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
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

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
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

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_names_for(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<String>> {
        self.series.label_names_for(tenant, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_values_for(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>> {
        self.series.label_values_for(tenant, name, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
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

    #[instrument(skip_all, fields(key = %key, len = tracing::field::Empty), err)]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save(&self, store: &Arc<dyn ObjectStore>, key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        tracing::Span::current().record("len", bytes.len());
        store.put(&Path::from(key), PutPayload::from(bytes)).await?;
        Ok(())
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save_latest_snapshot(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &str,
    ) -> Result<String> {
        self.save_latest_snapshot_with_retain(store, key, IndexSnapshotRetain::default())
            .await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save_latest_snapshot_with_retain(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &str,
        retain: IndexSnapshotRetain,
    ) -> Result<String> {
        put_index_snapshot(store, key, serde_json::to_vec(self)?, retain).await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Self> {
        Self::load_with_max_bytes(store, key, DEFAULT_INDEX_SNAPSHOT_MAX).await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        Self::load_path_with_max_bytes(store, &Path::from(key), max_bytes).await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load_latest_snapshot(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Self> {
        Self::load_latest_snapshot_with_max_bytes(store, key, DEFAULT_INDEX_SNAPSHOT_MAX).await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load_latest_snapshot_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        if let Some(path) = latest_index_snapshot_path(store, key).await? {
            return Self::load_path_with_max_bytes(store, &path, max_bytes).await;
        }
        Self::load_with_max_bytes(store, key, max_bytes).await
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path),
        err
    )]
    async fn load_path_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        let bytes = match crabka_object_store::read_capped(store, path, max_bytes.bytes_u64()).await
        {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(match error {
                    crabka_object_store::ObjectStoreError::TooLarge {
                        size, max_bytes, ..
                    } => BlockStoreError::InvalidBlock(format!(
                        "profile index snapshot `{path}` is {size} bytes, exceeds cap of {max_bytes} bytes"
                    )),
                    crabka_object_store::ObjectStoreError::Backend(message)
                    | crabka_object_store::ObjectStoreError::InvalidConfig(message) => {
                        BlockStoreError::ObjectStore(message)
                    }
                    crabka_object_store::ObjectStoreError::Io(error) => {
                        BlockStoreError::ObjectStore(error.to_string())
                    }
                    not_found @ crabka_object_store::ObjectStoreError::NotFound(_) => {
                        match store.head(path).await {
                            Ok(_) => BlockStoreError::ObjectStore(not_found.to_string()),
                            Err(missing) => BlockStoreError::ObjectStore(missing.to_string()),
                        }
                    }
                });
            }
        };
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
    use assert2::check;

    use super::*;
    use crate::{
        labels::Labels,
        matcher::{LabelMatcher, MatchOp},
    };

    const CPU_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    const HEAP_TYPE: &str = "memory:alloc_space:bytes:space:bytes";

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::from_pairs(pairs.iter().copied())
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn profile_labels(name: &str, profile_type: &str, service_name: &str) -> Labels {
        labels(&[
            ("__name__", name),
            ("__profile_type__", profile_type),
            ("service_name", service_name),
        ])
    }

    fn seed() -> ProfileIndex {
        let mut index = ProfileIndex::new();
        let cpu = labels(&[
            ("__name__", "process_cpu"),
            ("__profile_type__", CPU_TYPE),
            ("service_name", "checkout"),
        ]);
        let heap = labels(&[
            ("__name__", "memory"),
            ("__profile_type__", HEAP_TYPE),
            ("service_name", "checkout"),
        ]);
        index.add_series("t", cpu.fingerprint(), &cpu);
        index.add_series("t", heap.fingerprint(), &heap);
        index
    }

    fn seed_with_blocks() -> (
        ProfileIndex,
        SeriesFingerprint,
        SeriesFingerprint,
        SeriesFingerprint,
    ) {
        let mut index = ProfileIndex::new();
        let cpu_checkout = profile_labels("process_cpu", CPU_TYPE, "checkout");
        let heap_checkout = profile_labels("memory", HEAP_TYPE, "checkout");
        let cpu_payments = profile_labels("process_cpu", CPU_TYPE, "payments");
        let cpu_checkout_fp = cpu_checkout.fingerprint();
        let heap_checkout_fp = heap_checkout.fingerprint();
        let cpu_payments_fp = cpu_payments.fingerprint();

        index.add_series("t", cpu_checkout_fp, &cpu_checkout);
        index.add_series("t", heap_checkout_fp, &heap_checkout);
        index.add_series("t", cpu_payments_fp, &cpu_payments);

        for meta in [
            BlockMeta {
                tenant: "t".to_string(),
                object_key: "cpu-checkout.parquet".to_string(),
                min_ts: 100,
                max_ts: 199,
                row_count: 10,
                fingerprints: vec![cpu_checkout_fp],
            },
            BlockMeta {
                tenant: "t".to_string(),
                object_key: "heap-checkout.parquet".to_string(),
                min_ts: 300,
                max_ts: 399,
                row_count: 20,
                fingerprints: vec![heap_checkout_fp],
            },
            BlockMeta {
                tenant: "t".to_string(),
                object_key: "cpu-payments.parquet".to_string(),
                min_ts: 150,
                max_ts: 250,
                row_count: 30,
                fingerprints: vec![cpu_payments_fp],
            },
        ] {
            <ProfileIndex as BlockIndex>::add_block(&mut index, &meta);
        }

        (index, cpu_checkout_fp, heap_checkout_fp, cpu_payments_fp)
    }

    #[test]
    fn snapshot_size_cap_is_256_mib() {
        assert2::assert!(MAX_PROFILE_INDEX_SNAPSHOT_BYTES == mebibytes(256));
        assert2::assert!(MAX_PROFILE_INDEX_SNAPSHOT_BYTES.bytes_u64() == 256 * 1024 * 1024);
    }

    #[test]
    fn profile_types_lists_distinct_type_strings() {
        let index = seed();
        let mut types = index.profile_types("t");
        types.sort();
        assert2::assert!(types == strings(&[HEAP_TYPE, CPU_TYPE]));
        assert2::assert!(index.profile_types("nope").is_empty());
    }

    #[test]
    fn profile_type_index_maps_type_to_its_series() {
        let index = seed();
        let cpu_fps = index.fingerprints_for_profile_type("t", CPU_TYPE);
        let heap_fps = index.fingerprints_for_profile_type("t", HEAP_TYPE);
        assert2::assert!(
            cpu_fps
                == BTreeSet::from([
                    profile_labels("process_cpu", CPU_TYPE, "checkout").fingerprint()
                ])
        );
        assert2::assert!(
            heap_fps
                == BTreeSet::from([profile_labels("memory", HEAP_TYPE, "checkout").fingerprint()])
        );
    }

    #[test]
    fn profile_index_matching_and_block_helpers_return_pruned_metadata() {
        let (index, cpu_checkout, heap_checkout, cpu_payments) = seed_with_blocks();

        assert2::assert!(
            index
                .matching_fingerprints(
                    "t",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == BTreeSet::from([cpu_checkout, heap_checkout])
        );
        assert2::assert!(
            index
                .select_fingerprints(
                    "t",
                    CPU_TYPE,
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == BTreeSet::from([cpu_checkout])
        );
        assert2::assert!(
            index.select_fingerprints("t", CPU_TYPE, &[]).unwrap()
                == BTreeSet::from([cpu_checkout, cpu_payments])
        );
        assert2::assert!(
            index.candidate_blocks_for_series(
                "t",
                &BTreeSet::from([cpu_checkout, cpu_payments]),
                175,
                180,
            ) == strings(&["cpu-checkout.parquet", "cpu-payments.parquet"])
        );
        assert2::assert!(index.block_time_bounds("t", 175, 180) == Some((100, 250)));
        assert2::assert!(index.block_time_bounds("t", 450, 500) == None);
        assert2::assert!(
            BlockIndex::candidate_blocks(&index, "t", 175, 180)
                == strings(&["cpu-checkout.parquet", "cpu-payments.parquet"])
        );
        assert2::assert!(BlockIndex::block_count(&index, "t") == 3);
    }

    #[test]
    fn profile_index_profile_type_helpers_return_pruned_metadata() {
        let (index, cpu_checkout, heap_checkout, _) = seed_with_blocks();

        assert2::assert!(index.profile_types_for_time("t", 175, 180) == strings(&[CPU_TYPE]));
        assert2::assert!(index.profile_types_for_time("t", 320, 330) == strings(&[HEAP_TYPE]));
        assert2::assert!(
            index.profile_types_for_fingerprints("t", &BTreeSet::from([cpu_checkout]))
                == strings(&[CPU_TYPE])
        );
        assert2::assert!(
            index.profile_types_for_fingerprints("t", &BTreeSet::from([heap_checkout]))
                == strings(&[HEAP_TYPE])
        );
    }

    #[test]
    fn profile_index_label_helpers_return_pruned_metadata() {
        let (index, cpu_checkout, heap_checkout, _) = seed_with_blocks();

        assert2::assert!(
            index
                .label_values_for_time("t", "service_name", &[], 175, 180)
                .unwrap()
                == strings(&["checkout", "payments"])
        );
        assert2::assert!(
            index.label_values_for_fingerprints(
                "t",
                "service_name",
                &BTreeSet::from([heap_checkout])
            ) == strings(&["checkout"])
        );
        assert2::assert!(
            index.label_names_for_time("t", &[], 175, 180).unwrap()
                == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index.label_names_for_fingerprints("t", &BTreeSet::from([cpu_checkout]))
                == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index.label_names("t") == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index.label_values("t", "service_name") == strings(&["checkout", "payments"])
        );
        assert2::assert!(
            index
                .label_names_for(
                    "t",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == strings(&["__name__", "__profile_type__", "service_name"])
        );
        assert2::assert!(
            index
                .label_values_for(
                    "t",
                    "__name__",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")]
                )
                .unwrap()
                == strings(&["memory", "process_cpu"])
        );
    }

    #[test]
    fn profile_index_series_helpers_return_projected_metadata() {
        let (index, cpu_checkout, heap_checkout, cpu_payments) = seed_with_blocks();

        let mut blocks = index.all_blocks();
        blocks.sort_by(|left, right| left.object_key.cmp(&right.object_key));
        assert2::assert!(
            index
                .series_for_time("t", &[], &["service_name".to_string()], 175, 180)
                .unwrap()
                == vec![
                    vec![("service_name".to_string(), "checkout".to_string())],
                    vec![("service_name".to_string(), "payments".to_string())],
                ]
        );
        assert2::assert!(
            index.series_for_fingerprints("t", &BTreeSet::from([heap_checkout]), &[])
                == vec![vec![
                    ("__name__".to_string(), "memory".to_string()),
                    ("__profile_type__".to_string(), HEAP_TYPE.to_string()),
                    ("service_name".to_string(), "checkout".to_string()),
                ]]
        );
        assert2::assert!(
            index
                .series(
                    "t",
                    &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")],
                    &["__name__".to_string()],
                )
                .unwrap()
                == vec![
                    vec![("__name__".to_string(), "memory".to_string())],
                    vec![("__name__".to_string(), "process_cpu".to_string())],
                ]
        );
        assert2::assert!(
            blocks
                == vec![
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "cpu-checkout.parquet".to_string(),
                        min_ts: 100,
                        max_ts: 199,
                        row_count: 10,
                        fingerprints: vec![cpu_checkout],
                    },
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "cpu-payments.parquet".to_string(),
                        min_ts: 150,
                        max_ts: 250,
                        row_count: 30,
                        fingerprints: vec![cpu_payments],
                    },
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "heap-checkout.parquet".to_string(),
                        min_ts: 300,
                        max_ts: 399,
                        row_count: 20,
                        fingerprints: vec![heap_checkout],
                    },
                ]
        );
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
        assert2::assert!(
            got == BTreeSet::from([
                profile_labels("process_cpu", CPU_TYPE, "checkout").fingerprint(),
                profile_labels("memory", HEAP_TYPE, "checkout").fingerprint(),
            ])
        );
    }

    #[test]
    fn stacktrace_partition_map_records_block_partitions() {
        let mut index = seed();
        index.add_profile_block("t", "blocks/p1.parquet", vec![0, 1, 2]);
        assert2::assert!(index.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1, 2]);
        assert2::assert!(
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

        assert2::assert!(index.stacktrace_partitions("old.parquet").is_empty());
        assert2::assert!(index.stacktrace_partitions("new.parquet") == vec![99]);
        assert2::assert!(
            BlockIndex::candidate_blocks(&index, "t", 0, 10) == vec!["new.parquet".to_string()]
        );
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
        assert2::assert!(loaded.profile_types("t") == strings(&[HEAP_TYPE, CPU_TYPE]));
        assert2::assert!(loaded.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1]);
    }

    #[tokio::test]
    async fn latest_snapshot_round_trips_without_rewriting_legacy_key() {
        use object_store::memory::InMemory;

        let mut index = seed();
        index.add_profile_block("t", "blocks/p1.parquet", vec![0, 1]);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let snapshot_key = index
            .save_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();
        let loaded = ProfileIndex::load_latest_snapshot(&store, "index/profiles.json")
            .await
            .unwrap();

        check!(snapshot_key.starts_with("index/profiles/snapshots/"));
        check!(
            store
                .head(&Path::from("index/profiles.json"))
                .await
                .is_err()
        );
        assert2::assert!(loaded.profile_types("t") == strings(&[HEAP_TYPE, CPU_TYPE]));
        assert2::assert!(loaded.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1]);
    }

    #[tokio::test]
    async fn latest_snapshot_retains_bounded_snapshot_set() {
        use futures::StreamExt as _;
        use object_store::memory::InMemory;

        let index = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        for _ in 0..(crate::index_snapshot::DEFAULT_INDEX_SNAPSHOT_RETAIN + 3) {
            index
                .save_latest_snapshot(&store, "index/profiles.json")
                .await
                .unwrap();
        }

        let prefix = Path::from(crate::index_snapshot_prefix_for_key("index/profiles.json"));
        let mut stream = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(meta) = stream.next().await {
            meta.unwrap();
            count += 1;
        }

        assert2::assert!(count == crate::index_snapshot::DEFAULT_INDEX_SNAPSHOT_RETAIN);
    }

    #[tokio::test]
    async fn configurable_snapshot_policy_caps_loads_and_retention() {
        use futures::StreamExt as _;
        use object_store::memory::InMemory;

        let index = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let retain = crate::IndexSnapshotRetain::new(2).unwrap();

        for _ in 0..4 {
            index
                .save_latest_snapshot_with_retain(&store, "index/profiles.json", retain)
                .await
                .unwrap();
        }

        let prefix = Path::from(crate::index_snapshot_prefix_for_key("index/profiles.json"));
        let mut stream = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(meta) = stream.next().await {
            meta.unwrap();
            count += 1;
        }
        assert_eq!(count, 2);

        let cap = crabka_units::bytes(1);
        let got =
            ProfileIndex::load_latest_snapshot_with_max_bytes(&store, "index/profiles.json", cap)
                .await;
        assert2::assert!(matches!(got, Err(BlockStoreError::InvalidBlock(_))));
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
        assert2::assert!(size > 1);

        let got = ProfileIndex::load_with_max_bytes(
            &store,
            "index/profiles.json",
            crabka_units::bytes(1),
        )
        .await;
        let Err(BlockStoreError::InvalidBlock(msg)) = got else {
            panic!("expected InvalidBlock for oversized profile index snapshot");
        };
        assert2::assert!(
            msg == format!(
                "profile index snapshot `index/profiles.json` is {size} bytes, exceeds cap of 1 bytes"
            )
        );

        // A cap at/above the real size still loads.
        let loaded = ProfileIndex::load_with_max_bytes(
            &store,
            "index/profiles.json",
            ByteSize::from_bytes(size),
        )
        .await
        .unwrap();
        let mut profile_types = loaded.profile_types("t");
        profile_types.sort();
        assert2::assert!(profile_types == strings(&[HEAP_TYPE, CPU_TYPE]));
    }

    #[tokio::test]
    async fn load_missing_snapshot_preserves_object_store_error_text() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("index/missing-profiles.json");
        let expected = store.head(&path).await.unwrap_err().to_string();

        let got = ProfileIndex::load_with_max_bytes(
            &store,
            "index/missing-profiles.json",
            crate::DEFAULT_INDEX_SNAPSHOT_MAX,
        )
        .await;

        let Err(BlockStoreError::ObjectStore(msg)) = got else {
            panic!("expected ObjectStore error for missing profile index snapshot");
        };
        assert_eq!(msg, expected);
    }
}

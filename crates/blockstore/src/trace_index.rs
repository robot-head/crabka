//! Trace-specific block index: sharded trace-id blooms plus tag sets.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use crabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    block::BlockMeta,
    block_index::BlockIndex,
    bloom::ShardedTraceBloom,
    error::{BlockStoreError, Result},
    index_snapshot::{
        DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain, latest_index_snapshot_path,
        put_index_snapshot,
    },
};

/// The per-block trace footprint registered by a block builder.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceBlockStats {
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub bloom: ShardedTraceBloom,
    pub tag_names: BTreeSet<String>,
    pub tag_values: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Default, Serialize, Deserialize)]
struct TenantTraceIndex {
    blocks: Vec<TraceBlockStats>,
}

/// Trace block index.
#[derive(Default, Serialize, Deserialize)]
pub struct TraceIndex {
    tenants: HashMap<String, TenantTraceIndex>,
}

impl TraceIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_trace_block(&mut self, tenant: &str, stats: TraceBlockStats) {
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        tenant_index
            .blocks
            .retain(|block| block.object_key != stats.object_key);
        tenant_index.blocks.push(stats);
    }

    #[must_use]
    pub fn trace_blocks(&self, tenant: &str) -> &[TraceBlockStats] {
        self.tenants
            .get(tenant)
            .map_or(&[], |tenant_index| tenant_index.blocks.as_slice())
    }

    #[must_use]
    pub fn tenants(&self) -> Vec<String> {
        let mut tenants: Vec<String> = self.tenants.keys().cloned().collect();
        tenants.sort();
        tenants
    }

    pub fn replace_trace_blocks(
        &mut self,
        tenant: &str,
        old_keys: &[String],
        mut replacement: TraceBlockStats,
    ) {
        let old_keys: BTreeSet<&str> = old_keys.iter().map(String::as_str).collect();
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();

        let mut carried_tag_names = BTreeSet::new();
        let mut carried_tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        tenant_index.blocks.retain(|block| {
            if block.object_key == replacement.object_key {
                return false;
            }
            if !old_keys.contains(block.object_key.as_str()) {
                return true;
            }
            carried_tag_names.extend(block.tag_names.iter().cloned());
            for (tag, values) in &block.tag_values {
                carried_tag_values
                    .entry(tag.clone())
                    .or_default()
                    .extend(values.iter().cloned());
            }
            false
        });

        replacement.tag_names.extend(carried_tag_names);
        for (tag, values) in carried_tag_values {
            replacement
                .tag_values
                .entry(tag)
                .or_default()
                .extend(values);
        }
        tenant_index.blocks.push(replacement);
    }

    #[must_use]
    pub fn candidate_blocks_for_trace(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .filter(|b| b.bloom.maybe_contains(trace_id))
            .map(|b| b.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn prune_blocks_by_tag(
        &self,
        tenant: &str,
        tag: &str,
        value: Option<&str>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .filter(|b| {
                if !b.tag_names.contains(tag) {
                    return false;
                }
                match value {
                    None => true,
                    Some(v) => b
                        .tag_values
                        .get(tag)
                        .is_some_and(|values| values.contains(v)),
                }
            })
            .map(|b| b.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn tag_names(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for block in &t.blocks {
            if block.min_ts <= max_ts && block.max_ts >= min_ts {
                out.extend(block.tag_names.iter().cloned());
            }
        }
        out.into_iter().collect()
    }

    #[must_use]
    pub fn tag_values(&self, tenant: &str, tag: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for block in &t.blocks {
            if block.min_ts <= max_ts
                && block.max_ts >= min_ts
                && let Some(values) = block.tag_values.get(tag)
            {
                out.extend(values.iter().cloned());
            }
        }
        out.into_iter().collect()
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

    #[instrument(level = "debug", skip_all, fields(path = %path), err)]
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
                        "trace index snapshot `{path}` is {size} bytes, exceeds cap of {max_bytes} bytes"
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
        let index: Self = serde_json::from_slice(&bytes)?;
        // `Deserialize` bypasses the bloom constructors' invariant checks, so a
        // structurally-valid-but-corrupt snapshot would panic on the first
        // lookup. Validate here so it surfaces as an error instead.
        index.validate()?;
        Ok(index)
    }

    fn validate(&self) -> Result<()> {
        for tenant_index in self.tenants.values() {
            for block in &tenant_index.blocks {
                block.bloom.validate().map_err(|e| {
                    BlockStoreError::Serde(format!(
                        "corrupt trace bloom for block `{}`: {e}",
                        block.object_key
                    ))
                })?;
            }
        }
        Ok(())
    }
}

impl BlockIndex for TraceIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        self.add_trace_block(
            &meta.tenant,
            TraceBlockStats {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom: ShardedTraceBloom::match_all_with_tempo_defaults(),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .map(|b| b.object_key.clone())
            .collect()
    }

    fn block_count(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.blocks.len())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use assert2::check;

    use super::*;
    use crate::bloom::ShardedTraceBloom;

    fn tid(n: u8) -> [u8; 16] {
        let mut t = [0_u8; 16];
        t[0] = n;
        t
    }

    fn stats(
        key: &str,
        min: i64,
        max: i64,
        traces: &[u8],
        tags: &[(&str, &str)],
    ) -> TraceBlockStats {
        let mut bloom = ShardedTraceBloom::new(8, 64, 0.01);
        for &n in traces {
            bloom.insert(&tid(n));
        }
        let mut tag_names = BTreeSet::new();
        let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (key, value) in tags {
            tag_names.insert((*key).to_string());
            tag_values
                .entry((*key).to_string())
                .or_default()
                .insert((*value).to_string());
        }
        TraceBlockStats {
            object_key: key.to_string(),
            min_ts: min,
            max_ts: max,
            bloom,
            tag_names,
            tag_values,
        }
    }

    fn seed() -> TraceIndex {
        let mut idx = TraceIndex::new();
        idx.add_trace_block(
            "t",
            stats("b1", 0, 100, &[1, 2], &[("service.name", "api")]),
        );
        idx.add_trace_block("t", stats("b2", 200, 300, &[3], &[("service.name", "web")]));
        idx
    }

    #[test]
    fn by_id_locate_uses_bloom_and_time_no_global_map() {
        let idx = seed();
        for (_name, trace_id, min_ts, expected) in [
            ("first trace", tid(1), 0, vec!["b1".to_string()]),
            ("second trace", tid(3), 0, vec!["b2".to_string()]),
            ("outside time window", tid(1), 500, Vec::new()),
        ] {
            assert2::assert!(
                idx.candidate_blocks_for_trace("t", &trace_id, min_ts, 1_000) == expected
            );
        }
    }

    #[test]
    fn tag_pruning_keeps_only_blocks_that_can_contain_the_tag_value() {
        let idx = seed();
        for (_name, value, expected) in [
            ("api", "api", vec!["b1".to_string()]),
            ("web", "web", vec!["b2".to_string()]),
            ("absent", "nope", Vec::new()),
        ] {
            assert2::assert!(
                idx.prune_blocks_by_tag("t", "service.name", Some(value), 0, 1_000) == expected
            );
        }
    }

    #[test]
    fn tag_discovery_unions_blocks_in_window() {
        let idx = seed();
        let names = idx.tag_names("t", 0, 1_000);
        let mut vals = idx.tag_values("t", "service.name", 0, 1_000);
        vals.sort();
        assert2::assert!(names == vec!["service.name".to_string()]);
        assert2::assert!(vals == vec!["api".to_string(), "web".to_string()]);
    }

    #[test]
    fn tenants_lists_distinct_sorted_tenants() {
        let mut idx = TraceIndex::new();
        idx.add_trace_block("zeta", stats("b1", 0, 100, &[1], &[]));
        idx.add_trace_block("alpha", stats("b2", 0, 100, &[2], &[]));
        idx.add_trace_block("alpha", stats("b3", 0, 100, &[3], &[]));
        assert2::assert!(idx.tenants() == vec!["alpha".to_string(), "zeta".to_string()]);
        assert2::assert!(TraceIndex::new().tenants().is_empty());
    }

    #[test]
    fn prune_blocks_by_tag_time_filter_needs_both_ends() {
        let idx = seed();
        // b1 is [0,100], b2 is [200,300]. A window of [400,500] overlaps
        // neither. With `&&`→`||` the `min_ts <= max_ts` half stays true for
        // both blocks, so the filter would wrongly admit them.
        for (_name, min_ts, max_ts, expected) in [
            ("above both blocks", 400, 500, Vec::new()),
            ("overlaps first block", 50, 150, vec!["b1".to_string()]),
        ] {
            assert2::assert!(
                idx.prune_blocks_by_tag("t", "service.name", None, min_ts, max_ts) == expected
            );
        }
    }

    #[test]
    fn tag_names_time_filter_needs_both_ends() {
        let mut idx = TraceIndex::new();
        idx.add_trace_block("t", stats("b1", 0, 100, &[1], &[("a", "x")]));
        idx.add_trace_block("t", stats("b2", 200, 300, &[2], &[("b", "y")]));

        for (_name, min_ts, max_ts, expected) in [
            ("overlaps first block", 50, 150, vec!["a".to_string()]),
            ("above both blocks", 400, 500, Vec::new()),
        ] {
            assert2::assert!(idx.tag_names("t", min_ts, max_ts) == expected);
        }
    }

    #[test]
    fn block_index_candidate_blocks_time_filter_needs_both_ends() {
        use crate::block_index::BlockIndex;

        let idx = seed();
        for (_name, min_ts, max_ts, expected) in [
            ("above both blocks", 400, 500, Vec::new()),
            ("overlaps second block", 250, 350, vec!["b2".to_string()]),
        ] {
            assert2::assert!(BlockIndex::candidate_blocks(&idx, "t", min_ts, max_ts) == expected);
        }
    }

    #[test]
    fn block_index_trait_prefilter_is_time_only() {
        use crate::block_index::BlockIndex;

        let idx = seed();
        let mut got = BlockIndex::candidate_blocks(&idx, "t", 0, 1_000);
        got.sort();
        assert2::assert!(got == vec!["b1".to_string(), "b2".to_string()]);
        assert2::assert!(idx.block_count("t") == 2);
    }

    #[test]
    fn block_index_trait_add_block_is_idempotent_by_object_key() {
        use crate::{block::BlockMeta, block_index::BlockIndex};

        let mut idx = TraceIndex::new();
        let meta = BlockMeta {
            tenant: "t".into(),
            object_key: "traces/t/00000/00000000000000000001.parquet".into(),
            min_ts: 10,
            max_ts: 20,
            row_count: 1,
            fingerprints: Vec::new(),
        };

        BlockIndex::add_block(&mut idx, &meta);
        BlockIndex::add_block(&mut idx, &meta);

        assert2::assert!(idx.block_count("t") == 1);
        assert2::assert!(
            BlockIndex::candidate_blocks(&idx, "t", 0, 100)
                == vec!["traces/t/00000/00000000000000000001.parquet".to_string()]
        );
    }

    #[test]
    fn block_index_trait_add_block_does_not_false_negative_by_id_candidates() {
        use crate::{block::BlockMeta, block_index::BlockIndex};

        let mut idx = TraceIndex::new();
        let meta = BlockMeta {
            tenant: "t".into(),
            object_key: "traces/t/00000/00000000000000000001.parquet".into(),
            min_ts: 10,
            max_ts: 20,
            row_count: 1,
            fingerprints: Vec::new(),
        };

        BlockIndex::add_block(&mut idx, &meta);

        assert2::assert!(
            idx.candidate_blocks_for_trace("t", &tid(99), 0, 100)
                == vec!["traces/t/00000/00000000000000000001.parquet".to_string()]
        );
    }

    #[test]
    fn replace_trace_blocks_is_idempotent_by_replacement_object_key() {
        let mut idx = seed();
        let replacement = stats(
            "compacted-b1-b2",
            0,
            300,
            &[1, 2, 3],
            &[("service.name", "api")],
        );
        let old_keys = vec!["b1".to_string(), "b2".to_string()];

        idx.replace_trace_blocks("t", &old_keys, replacement.clone());
        idx.replace_trace_blocks("t", &old_keys, replacement.clone());

        let blocks = idx.trace_blocks("t");
        assert2::assert!(
            serde_json::to_value(blocks).unwrap() == serde_json::to_value([replacement]).unwrap()
        );
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        use object_store::{ObjectStore, memory::InMemory};

        let idx = seed();
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        idx.save(&store, "index/traces.json").await.unwrap();
        let loaded = TraceIndex::load(&store, "index/traces.json").await.unwrap();
        let got = loaded.candidate_blocks_for_trace("t", &tid(1), 0, 1_000);
        assert2::assert!(got == vec!["b1".to_string()]);
    }

    #[tokio::test]
    async fn latest_snapshot_round_trips_without_rewriting_legacy_key() {
        use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

        let idx = seed();
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());

        let snapshot_key = idx
            .save_latest_snapshot(&store, "index/traces.json")
            .await
            .unwrap();
        let loaded = TraceIndex::load_latest_snapshot(&store, "index/traces.json")
            .await
            .unwrap();

        check!(snapshot_key.starts_with("index/traces/snapshots/"));
        check!(
            store
                .head(&object_store::path::Path::from("index/traces.json"))
                .await
                .is_err()
        );
        check!(loaded.candidate_blocks_for_trace("t", &tid(1), 0, 1_000) == vec!["b1"]);
    }

    #[tokio::test]
    async fn latest_snapshot_retains_bounded_snapshot_set() {
        use futures::StreamExt as _;
        use object_store::{ObjectStore, memory::InMemory};

        let idx = seed();
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());

        for _ in 0..(crate::index_snapshot::DEFAULT_INDEX_SNAPSHOT_RETAIN + 3) {
            idx.save_latest_snapshot(&store, "index/traces.json")
                .await
                .unwrap();
        }

        let prefix = object_store::path::Path::from(crate::index_snapshot_prefix_for_key(
            "index/traces.json",
        ));
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
        use object_store::{ObjectStore, memory::InMemory};

        let idx = seed();
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let retain = crate::IndexSnapshotRetain::new(2).unwrap();

        for _ in 0..4 {
            idx.save_latest_snapshot_with_retain(&store, "index/traces.json", retain)
                .await
                .unwrap();
        }

        let prefix = object_store::path::Path::from(crate::index_snapshot_prefix_for_key(
            "index/traces.json",
        ));
        let mut stream = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(meta) = stream.next().await {
            meta.unwrap();
            count += 1;
        }
        assert_eq!(count, 2);

        let cap = crabka_units::bytes(1);
        let got =
            TraceIndex::load_latest_snapshot_with_max_bytes(&store, "index/traces.json", cap).await;
        assert2::assert!(matches!(got, Err(BlockStoreError::InvalidBlock(_))));
    }

    #[tokio::test]
    async fn load_rejects_corrupt_bloom_instead_of_panicking() {
        use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory};

        // A structurally-valid-but-corrupt snapshot: a shard with num_bits == 0
        // would divide-by-zero on the first `% num_bits` probe. `load` must
        // surface this as an error rather than letting a later lookup panic.
        let snapshot = serde_json::json!({
            "tenants": {
                "t": {
                    "blocks": [{
                        "object_key": "b1",
                        "min_ts": 0,
                        "max_ts": 100,
                        "bloom": { "shards": [{ "bits": [], "num_bits": 0, "k": 1 }] },
                        "tag_names": [],
                        "tag_values": {}
                    }]
                }
            }
        });
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        store
            .put(
                &object_store::path::Path::from("index/corrupt.json"),
                PutPayload::from(serde_json::to_vec(&snapshot).unwrap()),
            )
            .await
            .unwrap();

        let loaded = TraceIndex::load(&store, "index/corrupt.json").await;
        assert2::assert!(loaded.is_err());
    }

    #[tokio::test]
    async fn load_rejects_empty_shards_bloom() {
        use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory};

        // Empty `shards` would divide-by-zero on `% shards.len()` in shard_of.
        let snapshot = serde_json::json!({
            "tenants": {
                "t": {
                    "blocks": [{
                        "object_key": "b1",
                        "min_ts": 0,
                        "max_ts": 100,
                        "bloom": { "shards": [] },
                        "tag_names": [],
                        "tag_values": {}
                    }]
                }
            }
        });
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        store
            .put(
                &object_store::path::Path::from("index/empty-shards.json"),
                PutPayload::from(serde_json::to_vec(&snapshot).unwrap()),
            )
            .await
            .unwrap();

        let loaded = TraceIndex::load(&store, "index/empty-shards.json").await;
        assert2::assert!(loaded.is_err());
    }
}

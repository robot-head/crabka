//! Trace-specific block index: sharded trace-id blooms plus tag sets.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::block_index::BlockIndex;
use crate::bloom::ShardedTraceBloom;
use crate::error::Result;

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

    pub async fn save(&self, store: &Arc<dyn ObjectStore>, key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        store.put(&Path::from(key), PutPayload::from(bytes)).await?;
        Ok(())
    }

    pub async fn load(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Self> {
        let bytes = store.get(&Path::from(key)).await?.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

impl BlockIndex for TraceIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        self.tenants
            .entry(meta.tenant.clone())
            .or_default()
            .blocks
            .push(TraceBlockStats {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom: ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            });
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

    use assert2::assert;

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
        let got = idx.candidate_blocks_for_trace("t", &tid(1), 0, 1_000);
        assert!(got == vec!["b1".to_string()]);
        let got = idx.candidate_blocks_for_trace("t", &tid(3), 0, 1_000);
        assert!(got == vec!["b2".to_string()]);
        let got = idx.candidate_blocks_for_trace("t", &tid(1), 500, 1_000);
        assert!(got.is_empty());
    }

    #[test]
    fn tag_pruning_keeps_only_blocks_that_can_contain_the_tag_value() {
        let idx = seed();
        let got = idx.prune_blocks_by_tag("t", "service.name", Some("api"), 0, 1_000);
        assert!(got == vec!["b1".to_string()]);
        let got = idx.prune_blocks_by_tag("t", "service.name", Some("web"), 0, 1_000);
        assert!(got == vec!["b2".to_string()]);
        let got = idx.prune_blocks_by_tag("t", "service.name", Some("nope"), 0, 1_000);
        assert!(got.is_empty());
    }

    #[test]
    fn tag_discovery_unions_blocks_in_window() {
        let idx = seed();
        let names = idx.tag_names("t", 0, 1_000);
        assert!(names == vec!["service.name".to_string()]);
        let mut vals = idx.tag_values("t", "service.name", 0, 1_000);
        vals.sort();
        assert!(vals == vec!["api".to_string(), "web".to_string()]);
    }

    #[test]
    fn block_index_trait_prefilter_is_time_only() {
        use crate::block_index::BlockIndex;

        let idx = seed();
        let mut got = BlockIndex::candidate_blocks(&idx, "t", 0, 1_000);
        got.sort();
        assert!(got == vec!["b1".to_string(), "b2".to_string()]);
        assert!(idx.block_count("t") == 2);
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        use object_store::ObjectStore;
        use object_store::memory::InMemory;

        let idx = seed();
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        idx.save(&store, "index/traces.json").await.unwrap();
        let loaded = TraceIndex::load(&store, "index/traces.json").await.unwrap();
        let got = loaded.candidate_blocks_for_trace("t", &tid(1), 0, 1_000);
        assert!(got == vec!["b1".to_string()]);
    }
}

//! In-memory label/series/block index.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::block_index::BlockIndex;
use crate::error::{BlockStoreError, Result};
use crate::labels::{Labels, SeriesFingerprint};
use crate::matcher::{LabelMatcher, MatchOp};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockEntry {
    object_key: String,
    min_ts: i64,
    max_ts: i64,
    row_count: usize,
    fingerprints: BTreeSet<SeriesFingerprint>,
}

#[derive(Default, Serialize, Deserialize)]
struct TenantIndex {
    series: HashMap<SeriesFingerprint, Labels>,
    postings: HashMap<String, BTreeSet<SeriesFingerprint>>,
    values: HashMap<String, BTreeSet<String>>,
    blocks: Vec<BlockEntry>,
}

/// Multi-tenant in-memory index.
#[derive(Default, Serialize, Deserialize)]
pub struct SeriesIndex {
    tenants: HashMap<String, TenantIndex>,
}

impl SeriesIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels) {
        let t = self.tenants.entry(tenant.to_string()).or_default();
        if t.series.contains_key(&fp) {
            return;
        }
        t.series.insert(fp, labels.clone());
        for (name, value) in labels.iter() {
            t.postings
                .entry(posting_key(name, value))
                .or_default()
                .insert(fp);
            t.values
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    pub fn add_block(&mut self, meta: &BlockMeta) {
        let t = self.tenants.entry(meta.tenant.clone()).or_default();
        t.blocks.push(BlockEntry {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            row_count: meta.row_count,
            fingerprints: meta.fingerprints.iter().copied().collect(),
        });
    }

    pub fn resolve(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        if matchers.is_empty() {
            return Err(BlockStoreError::InvalidBlock(
                "at least one label matcher is required".into(),
            ));
        }
        let Some(t) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };

        let mut acc: Option<BTreeSet<SeriesFingerprint>> = None;
        for matcher in matchers {
            let matched = t.match_one(matcher)?;
            acc = Some(match acc {
                None => matched,
                Some(prev) => prev.intersection(&matched).copied().collect(),
            });
            if acc.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
        }
        Ok(acc.unwrap_or_default())
    }

    #[must_use]
    pub fn candidate_blocks(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .filter(|b| b.fingerprints.iter().any(|fp| fps.contains(fp)))
            .map(|b| b.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .map(|t| t.values.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .and_then(|t| t.values.get(name))
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn save(&self, store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        store
            .put(&Path::from(object_key), PutPayload::from(bytes))
            .await?;
        Ok(())
    }

    pub async fn load(store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<Self> {
        let bytes = store.get(&Path::from(object_key)).await?.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Fingerprints matching `matchers` (every series when `matchers` is empty).
    pub fn matching_fingerprints(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let Some(t) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };
        if matchers.is_empty() {
            return Ok(t.series.keys().copied().collect());
        }
        self.resolve(tenant, matchers)
    }

    /// Distinct label names across the series matching `matchers`.
    pub fn label_names_for(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.label_names_for_fingerprints(tenant, &fps))
    }

    /// Distinct label names across the given fingerprints.
    #[must_use]
    pub fn label_names_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut names = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = t.series.get(fp) {
                names.extend(labels.iter().map(|(name, _)| name.clone()));
            }
        }
        names.into_iter().collect()
    }

    /// Distinct values of `name` across the series matching `matchers`.
    pub fn label_values_for(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.label_values_for_fingerprints(tenant, name, &fps))
    }

    /// Distinct values of `name` across the given fingerprints.
    #[must_use]
    pub fn label_values_for_fingerprints(
        &self,
        tenant: &str,
        name: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut values = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = t.series.get(fp)
                && let Some(value) = labels.get(name)
            {
                values.insert(value.to_string());
            }
        }
        values.into_iter().collect()
    }

    /// Projected label sets for the series matching `matchers`.
    pub fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
    ) -> Result<Vec<Vec<(String, String)>>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.series_for_fingerprints(tenant, &fps, label_names))
    }

    /// Projected label sets for the given fingerprints.
    #[must_use]
    pub fn series_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        label_names: &[String],
    ) -> Vec<Vec<(String, String)>> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for fp in fps {
            let Some(labels) = t.series.get(fp) else {
                continue;
            };
            let projected = label_names
                .iter()
                .filter_map(|name| {
                    labels
                        .get(name)
                        .map(|value| (name.clone(), value.to_string()))
                })
                .collect::<Vec<_>>();
            if !projected.is_empty() || label_names.is_empty() {
                out.insert(projected);
            }
        }
        out.into_iter().collect()
    }

    /// Time + fingerprint pruned candidate block keys (alias of
    /// [`Self::candidate_blocks`], named for the profile index's call sites).
    #[must_use]
    pub fn candidate_blocks_for_series(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        self.candidate_blocks(tenant, fps, min_ts, max_ts)
    }

    /// Combined `[min_ts, max_ts]` across blocks overlapping the query window.
    #[must_use]
    pub fn block_time_bounds(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
        let t = self.tenants.get(tenant)?;
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .fold(None, |acc, b| match acc {
                Some((min, max)) => Some((min.min(b.min_ts), max.max(b.max_ts))),
                None => Some((b.min_ts, b.max_ts)),
            })
    }

    /// Replace the `remove_keys` blocks with `add` (compaction swap).
    pub fn replace_blocks(&mut self, tenant: &str, remove_keys: &[String], add: &[BlockMeta]) {
        let t = self.tenants.entry(tenant.to_string()).or_default();
        let remove_keys = remove_keys.iter().collect::<BTreeSet<_>>();
        t.blocks
            .retain(|block| !remove_keys.contains(&block.object_key));
        for meta in add {
            t.blocks.push(BlockEntry {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                row_count: meta.row_count,
                fingerprints: meta.fingerprints.iter().copied().collect(),
            });
        }
    }

    /// Every registered block across all tenants, as [`BlockMeta`].
    #[must_use]
    pub fn all_blocks(&self) -> Vec<BlockMeta> {
        self.tenants
            .iter()
            .flat_map(|(tenant, t)| {
                t.blocks.iter().map(|block| BlockMeta {
                    tenant: tenant.clone(),
                    object_key: block.object_key.clone(),
                    min_ts: block.min_ts,
                    max_ts: block.max_ts,
                    row_count: block.row_count,
                    fingerprints: block.fingerprints.iter().copied().collect(),
                })
            })
            .collect()
    }
}

impl BlockIndex for SeriesIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        Self::add_block(self, meta);
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
        self.tenants
            .get(tenant)
            .map_or(0, |tenant_index| tenant_index.blocks.len())
    }
}

impl TenantIndex {
    fn match_one(&self, matcher: &LabelMatcher) -> Result<BTreeSet<SeriesFingerprint>> {
        match matcher.op {
            MatchOp::Eq => Ok(self
                .postings
                .get(&posting_key(&matcher.name, &matcher.value))
                .cloned()
                .unwrap_or_default()),
            MatchOp::Neq => {
                let excluded = self
                    .postings
                    .get(&posting_key(&matcher.name, &matcher.value))
                    .cloned()
                    .unwrap_or_default();
                Ok(self
                    .series
                    .keys()
                    .copied()
                    .filter(|fp| !excluded.contains(fp))
                    .collect())
            }
            MatchOp::Re | MatchOp::Nre => {
                let re = regex::Regex::new(&anchored(&matcher.value))
                    .map_err(|e| BlockStoreError::InvalidBlock(format!("bad regex: {e}")))?;
                let mut matching_fps = BTreeSet::new();
                for (key, fps) in &self.postings {
                    let Some((name, value)) = split_posting_key(key) else {
                        continue;
                    };
                    if name == matcher.name && re.is_match(value) {
                        matching_fps.extend(fps.iter().copied());
                    }
                }
                if matcher.op == MatchOp::Re {
                    Ok(matching_fps)
                } else {
                    Ok(self
                        .series
                        .keys()
                        .copied()
                        .filter(|fp| !matching_fps.contains(fp))
                        .collect())
                }
            }
        }
    }
}

fn anchored(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

fn posting_key(name: &str, value: &str) -> String {
    format!("{name}\u{0}{value}")
}

fn split_posting_key(key: &str) -> Option<(String, &str)> {
    let (name, value) = key.split_once('\0')?;
    Some((name.to_string(), value))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;
    use crate::block::BlockMeta;
    use crate::labels::Labels;
    use crate::matcher::{LabelMatcher, MatchOp};

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        for (k, v) in pairs {
            l.insert(*k, *v);
        }
        l
    }

    fn seed() -> SeriesIndex {
        let mut idx = SeriesIndex::new();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]);
        let api_dev = labels(&[("app", "api"), ("env", "dev")]);
        let web_prod = labels(&[("app", "web"), ("env", "prod")]);
        idx.add_series("t", api_prod.fingerprint(), &api_prod);
        idx.add_series("t", api_dev.fingerprint(), &api_dev);
        idx.add_series("t", web_prod.fingerprint(), &web_prod);
        idx
    }

    #[test]
    fn resolve_eq_intersection() {
        let idx = seed();
        let want = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Eq, "prod"),
                ],
            )
            .unwrap();
        assert!(got == BTreeSet::from([want]));
    }

    #[test]
    fn resolve_neq_excludes() {
        let idx = seed();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "prod"),
                ],
            )
            .unwrap();
        let want = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        assert!(got == BTreeSet::from([want]));
    }

    #[test]
    fn resolve_regex_union() {
        let idx = seed();
        let got = idx
            .resolve("t", &[LabelMatcher::new("env", MatchOp::Re, "pro.*")])
            .unwrap();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        assert!(got == BTreeSet::from([api_prod, web_prod]));
    }

    #[test]
    fn resolve_unknown_tenant_is_empty() {
        let idx = seed();
        let got = idx
            .resolve("nope", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn candidate_blocks_prune_by_fp_and_time() {
        let mut idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        });
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 200,
            max_ts: 300,
            row_count: 1,
            fingerprints: vec![web_prod],
        });

        let got = idx.candidate_blocks("t", &BTreeSet::from([api_prod]), 0, 150);
        assert!(got == vec!["b1.parquet".to_string()]);

        let got = idx.candidate_blocks("t", &BTreeSet::from([api_prod]), 500, 600);
        assert!(got.is_empty());
    }

    #[test]
    fn label_names_and_values() {
        let idx = seed();
        let mut names = idx.label_names("t");
        names.sort();
        assert!(names == vec!["app".to_string(), "env".to_string()]);
        let mut envs = idx.label_values("t", "env");
        envs.sort();
        assert!(envs == vec!["dev".to_string(), "prod".to_string()]);
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        use std::sync::Arc;

        use object_store::ObjectStore;
        use object_store::memory::InMemory;

        let idx = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/snapshot.json").await.unwrap();

        let loaded = SeriesIndex::load(&store, "index/snapshot.json")
            .await
            .unwrap();
        let got = loaded
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert!(got.len() == 2);
    }

    #[test]
    fn index_implements_block_index_time_prefilter() {
        let mut idx = seed();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![],
        });
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 200,
            max_ts: 300,
            row_count: 1,
            fingerprints: vec![],
        });

        assert!(<SeriesIndex as BlockIndex>::block_count(&idx, "t") == 2);
        assert!(
            <SeriesIndex as BlockIndex>::candidate_blocks(&idx, "t", 50, 150)
                == vec!["b1.parquet".to_string()]
        );
    }
}

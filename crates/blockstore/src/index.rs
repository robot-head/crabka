//! In-memory label/series/block index.

use std::collections::{BTreeSet, HashMap};

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::block_index::SignalBlockIndex;
use crate::error::{BlockStoreError, Result};
use crate::labels::{Labels, SeriesFingerprint};
use crate::matcher::{LabelMatcher, MatchOp};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockEntry {
    object_key: String,
    min_ts: i64,
    max_ts: i64,
    #[serde(default)]
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

/// Multi-tenant label postings index for logs/metrics/profile series.
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
        let index = self.tenants.entry(tenant.to_string()).or_default();
        if index.series.contains_key(&fp) {
            return;
        }
        index.series.insert(fp, labels.clone());
        for (name, value) in labels.iter() {
            index
                .postings
                .entry(posting_key(name, value))
                .or_default()
                .insert(fp);
            index
                .values
                .entry(name.to_string())
                .or_default()
                .insert(value.to_string());
        }
    }

    pub fn add_block(&mut self, meta: &BlockMeta) {
        let index = self.tenants.entry(meta.tenant.clone()).or_default();
        index.blocks.push(BlockEntry {
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
                "at least one label matcher is required".to_string(),
            ));
        }
        let Some(index) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };

        let mut acc: Option<BTreeSet<SeriesFingerprint>> = None;
        for matcher in matchers {
            let matched = index.match_one(matcher)?;
            acc = Some(match acc {
                Some(current) => current.intersection(&matched).copied().collect(),
                None => matched,
            });
            if acc.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
        }
        Ok(acc.unwrap_or_default())
    }

    pub fn matching_fingerprints(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let Some(index) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };
        if matchers.is_empty() {
            return Ok(index.series.keys().copied().collect());
        }
        self.resolve(tenant, matchers)
    }

    pub fn label_names_for(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<String>> {
        let Some(index) = self.tenants.get(tenant) else {
            return Ok(Vec::new());
        };
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let mut names = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = index.series.get(&fp) {
                names.extend(labels.iter().map(|(name, _)| name.to_string()));
            }
        }
        Ok(names.into_iter().collect())
    }

    #[must_use]
    pub fn label_names_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut names = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = index.series.get(fp) {
                names.extend(labels.iter().map(|(name, _)| name.to_string()));
            }
        }
        names.into_iter().collect()
    }

    pub fn label_values_for(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>> {
        let Some(index) = self.tenants.get(tenant) else {
            return Ok(Vec::new());
        };
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let mut values = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = index.series.get(&fp)
                && let Some(value) = labels.get(name)
            {
                values.insert(value.to_string());
            }
        }
        Ok(values.into_iter().collect())
    }

    #[must_use]
    pub fn label_values_for_fingerprints(
        &self,
        tenant: &str,
        name: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut values = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = index.series.get(fp)
                && let Some(value) = labels.get(name)
            {
                values.insert(value.to_string());
            }
        }
        values.into_iter().collect()
    }

    pub fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
    ) -> Result<Vec<Vec<(String, String)>>> {
        let Some(index) = self.tenants.get(tenant) else {
            return Ok(Vec::new());
        };
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let mut out = BTreeSet::new();
        for fp in fps {
            let Some(labels) = index.series.get(&fp) else {
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
        Ok(out.into_iter().collect())
    }

    #[must_use]
    pub fn series_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        label_names: &[String],
    ) -> Vec<Vec<(String, String)>> {
        let Some(index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for fp in fps {
            let Some(labels) = index.series.get(fp) else {
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

    #[must_use]
    pub fn candidate_blocks_for_series(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        index
            .blocks
            .iter()
            .filter(|block| overlaps(block.min_ts, block.max_ts, min_ts, max_ts))
            .filter(|block| block.fingerprints.iter().any(|fp| fps.contains(fp)))
            .map(|block| block.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn block_time_bounds(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
        let index = self.tenants.get(tenant)?;
        index
            .blocks
            .iter()
            .filter(|block| overlaps(block.min_ts, block.max_ts, min_ts, max_ts))
            .fold(None, |acc, block| match acc {
                Some((min, max)) => Some((min.min(block.min_ts), max.max(block.max_ts))),
                None => Some((block.min_ts, block.max_ts)),
            })
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .map(|index| index.values.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .and_then(|index| index.values.get(name))
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn replace_blocks(&mut self, tenant: &str, remove_keys: &[String], add: &[BlockMeta]) {
        let index = self.tenants.entry(tenant.to_string()).or_default();
        let remove_keys = remove_keys.iter().collect::<BTreeSet<_>>();
        index
            .blocks
            .retain(|block| !remove_keys.contains(&block.object_key));
        for meta in add {
            index.blocks.push(BlockEntry {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                row_count: meta.row_count,
                fingerprints: meta.fingerprints.iter().copied().collect(),
            });
        }
    }

    #[must_use]
    pub fn all_blocks(&self) -> Vec<BlockMeta> {
        self.tenants
            .iter()
            .flat_map(|(tenant, index)| {
                index.blocks.iter().map(|block| BlockMeta {
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

impl SignalBlockIndex for SeriesIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        Self::add_block(self, meta);
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        index
            .blocks
            .iter()
            .filter(|block| overlaps(block.min_ts, block.max_ts, min_ts, max_ts))
            .map(|block| block.object_key.clone())
            .collect()
    }

    fn block_count(&self, tenant: &str) -> usize {
        self.tenants
            .get(tenant)
            .map_or(0, |index| index.blocks.len())
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
            MatchOp::Re | MatchOp::Nre => self.match_regex(matcher),
        }
    }

    fn match_regex(&self, matcher: &LabelMatcher) -> Result<BTreeSet<SeriesFingerprint>> {
        let regex = Regex::new(&format!("^(?:{})$", matcher.value))
            .map_err(|err| BlockStoreError::InvalidBlock(format!("bad regex: {err}")))?;
        let mut matching_fps = BTreeSet::new();
        for (key, fps) in &self.postings {
            let Some((name, value)) = split_posting_key(key) else {
                continue;
            };
            if name == matcher.name && regex.is_match(value) {
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

fn overlaps(left_min: i64, left_max: i64, right_min: i64, right_max: i64) -> bool {
    left_min <= right_max && left_max >= right_min
}

fn posting_key(name: &str, value: &str) -> String {
    format!("{name}\u{1f}{value}")
}

fn split_posting_key(key: &str) -> Option<(String, &str)> {
    let (name, value) = key.split_once('\u{1f}')?;
    Some((name.to_string(), value))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;
    use crate::BlockMeta;
    use crate::labels::Labels;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::from_pairs(pairs.iter().copied())
    }

    fn seed() -> SeriesIndex {
        let mut index = SeriesIndex::new();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]);
        let api_dev = labels(&[("app", "api"), ("env", "dev")]);
        let web_prod = labels(&[("app", "web"), ("env", "prod")]);
        index.add_series("t", api_prod.fingerprint(), &api_prod);
        index.add_series("t", api_dev.fingerprint(), &api_dev);
        index.add_series("t", web_prod.fingerprint(), &web_prod);
        index
    }

    #[test]
    fn resolve_eq_intersection() {
        let index = seed();
        let want = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let got = index
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
        let index = seed();
        let got = index
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
        let index = seed();
        let got = index
            .resolve("t", &[LabelMatcher::new("env", MatchOp::Re, "pro.*")])
            .unwrap();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        assert!(got == BTreeSet::from([api_prod, web_prod]));
    }

    #[test]
    fn resolve_unknown_tenant_is_empty() {
        let index = seed();
        let got = index
            .resolve("nope", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn candidate_blocks_prune_by_fp_and_time() {
        let mut index = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        index.add_block(&BlockMeta {
            tenant: "t".to_string(),
            object_key: "b1.parquet".to_string(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        });
        index.add_block(&BlockMeta {
            tenant: "t".to_string(),
            object_key: "b2.parquet".to_string(),
            min_ts: 200,
            max_ts: 300,
            row_count: 1,
            fingerprints: vec![web_prod],
        });

        let got = index.candidate_blocks_for_series("t", &BTreeSet::from([api_prod]), 0, 150);
        assert!(got == vec!["b1.parquet".to_string()]);

        let got = index.candidate_blocks_for_series("t", &BTreeSet::from([api_prod]), 500, 600);
        assert!(got.is_empty());
    }

    #[test]
    fn trait_candidate_blocks_are_time_only() {
        let mut index = seed();
        index.add_block(&BlockMeta {
            tenant: "t".to_string(),
            object_key: "b1.parquet".to_string(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![],
        });
        assert!(SignalBlockIndex::candidate_blocks(&index, "t", 50, 60) == vec!["b1.parquet"]);
        assert!(SignalBlockIndex::block_count(&index, "t") == 1);
    }

    #[test]
    fn label_names_and_values() {
        let index = seed();
        let mut names = index.label_names("t");
        names.sort();
        assert!(names == vec!["app".to_string(), "env".to_string()]);
        let mut envs = index.label_values("t", "env");
        envs.sort();
        assert!(envs == vec!["dev".to_string(), "prod".to_string()]);
    }

    #[test]
    fn replace_blocks_swaps_old_keys_for_new_metadata() {
        let mut index = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        index.add_block(&BlockMeta {
            tenant: "t".to_string(),
            object_key: "old-a.parquet".to_string(),
            min_ts: 0,
            max_ts: 10,
            row_count: 1,
            fingerprints: vec![api_prod],
        });
        index.add_block(&BlockMeta {
            tenant: "t".to_string(),
            object_key: "old-b.parquet".to_string(),
            min_ts: 20,
            max_ts: 30,
            row_count: 1,
            fingerprints: vec![api_prod],
        });

        index.replace_blocks(
            "t",
            &["old-a.parquet".to_string(), "old-b.parquet".to_string()],
            &[BlockMeta {
                tenant: "t".to_string(),
                object_key: "new.parquet".to_string(),
                min_ts: 0,
                max_ts: 30,
                row_count: 2,
                fingerprints: vec![api_prod],
            }],
        );

        assert!(SignalBlockIndex::candidate_blocks(&index, "t", 0, 30) == vec!["new.parquet"]);
    }
}

//! In-memory label/series/block index.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    block::BlockMeta,
    block_index::BlockIndex,
    error::{BlockStoreError, Result},
    labels::{Labels, SeriesFingerprint},
    matcher::{LabelMatcher, MatchOp, QUERY_SHARD_LABEL, parse_query_shard_selector},
};

/// Maximum byte size of an index snapshot object accepted by [`Index::load`].
///
/// Snapshots come from shared object storage and (per the threat model) may be
/// corrupted or maliciously oversized; loading one fully buffers it in memory
/// before `serde_json` parsing, so an unbounded read could OOM the process. The
/// object is `head()`ed first and rejected when larger than this cap, mirroring
/// the `max_decompressed` output cap used by the profiles gunzip path. Defaults
/// to 256 MiB, comfortably above a realistic single-tenant-fleet index.
pub const MAX_INDEX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockEntry {
    object_key: String,
    min_ts: i64,
    max_ts: i64,
    row_count: usize,
    fingerprints: BTreeSet<SeriesFingerprint>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TenantIndex {
    series: BTreeMap<SeriesFingerprint, Labels>,
    /// `name -> value -> fingerprints`. Structured (rather than an in-band
    /// `name\0value` key) so arbitrary label bytes — including NUL — can never
    /// collide distinct `(name, value)` pairs into one bucket.
    postings: BTreeMap<String, BTreeMap<String, BTreeSet<SeriesFingerprint>>>,
    values: BTreeMap<String, BTreeSet<String>>,
    blocks: Vec<BlockEntry>,
}

/// Multi-tenant in-memory index for label resolution and block pruning.
///
/// This is the metrics/logs (series) index; it is embedded by the profiles and
/// traces indexes for shared label posting and matcher resolution.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Index {
    tenants: BTreeMap<String, TenantIndex>,
}

impl Index {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels) {
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        if tenant_index.series.contains_key(&fp) {
            return;
        }
        tenant_index.series.insert(fp, labels.clone());

        for (name, value) in labels.iter() {
            tenant_index
                .postings
                .entry(name.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(fp);
            tenant_index
                .values
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    pub fn add_block(&mut self, meta: &BlockMeta) {
        let tenant_index = self.tenants.entry(meta.tenant.clone()).or_default();
        if let Some(entry) = tenant_index
            .blocks
            .iter_mut()
            .find(|entry| entry.object_key == meta.object_key)
        {
            entry.min_ts = meta.min_ts;
            entry.max_ts = meta.max_ts;
            entry.row_count = meta.row_count;
            entry.fingerprints = meta.fingerprints.iter().copied().collect();
            return;
        }

        tenant_index.blocks.push(BlockEntry {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            row_count: meta.row_count,
            fingerprints: meta.fingerprints.iter().copied().collect(),
        });
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
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

        // Prometheus rejects vector selectors in which every matcher matches the
        // empty string (e.g. `{foo!="bar"}`): such a selector restricts nothing
        // and forces an O(total-series) full tenant scan. Require at least one
        // matcher that cannot match the empty string. The synthetic
        // `__query_shard__` matcher is internal-only and never restricts the
        // candidate set to a posting, so it does not satisfy this requirement.
        let mut has_non_empty_matcher = false;
        for matcher in matchers {
            if matcher.name != QUERY_SHARD_LABEL && !matcher_matches_empty(matcher)? {
                has_non_empty_matcher = true;
                break;
            }
        }
        if !has_non_empty_matcher {
            return Err(BlockStoreError::InvalidBlock(
                "vector selector must contain at least one non-empty matcher".to_string(),
            ));
        }

        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };

        let mut resolved = tenant_index.resolve_one(&matchers[0])?;
        for matcher in &matchers[1..] {
            let matched = tenant_index.resolve_one(matcher)?;
            resolved = resolved.intersection(&matched).copied().collect();
            if resolved.is_empty() {
                break;
            }
        }

        Ok(resolved)
    }

    #[must_use]
    pub fn candidate_blocks(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };

        tenant_index
            .blocks
            .iter()
            .filter(|block| block.min_ts <= max_ts && block.max_ts >= min_ts)
            .filter(|block| block.fingerprints.iter().any(|fp| fps.contains(fp)))
            .map(|block| block.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn all_blocks(&self, tenant: &str) -> Vec<BlockMeta> {
        self.tenants
            .get(tenant)
            .map(|tenant_index| {
                tenant_index
                    .blocks
                    .iter()
                    .map(|block| BlockMeta {
                        tenant: tenant.to_string(),
                        object_key: block.object_key.clone(),
                        min_ts: block.min_ts,
                        max_ts: block.max_ts,
                        row_count: block.row_count,
                        fingerprints: block.fingerprints.iter().copied().collect(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .map(|tenant_index| tenant_index.values.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .and_then(|tenant_index| tenant_index.values.get(name))
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Full label sets for the series matching `matchers` (every series when
    /// `matchers` is empty).
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn series(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<Labels>> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Ok(Vec::new());
        };

        let fingerprints = if matchers.is_empty() {
            tenant_index.all_fingerprints()
        } else {
            self.resolve(tenant, matchers)?
        };
        Ok(fingerprints
            .into_iter()
            .filter_map(|fp| tenant_index.series.get(&fp).cloned())
            .collect())
    }

    /// Resolve matchers to fingerprints, treating an empty matcher set as
    /// "all fingerprints in the tenant" (unlike [`Index::resolve`], which
    /// rejects empty matchers).
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn matching_fingerprints(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };
        if matchers.is_empty() {
            return Ok(tenant_index.all_fingerprints());
        }
        self.resolve(tenant, matchers)
    }

    /// Distinct label names carried by the series matching `matchers`.
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_names_for(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.label_names_for_fingerprints(tenant, &fps))
    }

    /// Distinct label names carried by the given fingerprints.
    #[must_use]
    pub fn label_names_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut names = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = tenant_index.series.get(fp) {
                names.extend(labels.iter().map(|(name, _)| name.clone()));
            }
        }
        names.into_iter().collect()
    }

    /// Distinct values for `name` across the series matching `matchers`.
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_values_for(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.label_values_for_fingerprints(tenant, name, &fps))
    }

    /// Distinct values for `name` across the given fingerprints.
    #[must_use]
    pub fn label_values_for_fingerprints(
        &self,
        tenant: &str,
        name: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut values = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = tenant_index.series.get(fp)
                && let Some(value) = labels.get(name)
            {
                values.insert(value.to_string());
            }
        }
        values.into_iter().collect()
    }

    /// Project the series matching `matchers` onto `label_names`.
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn series_projected(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
    ) -> Result<Vec<Vec<(String, String)>>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.series_for_fingerprints(tenant, &fps, label_names))
    }

    /// Project the given fingerprints onto `label_names`.
    #[must_use]
    pub fn series_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        label_names: &[String],
    ) -> Vec<Vec<(String, String)>> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for fp in fps {
            let Some(labels) = tenant_index.series.get(fp) else {
                continue;
            };
            // An empty `label_names` means "return the full label set" (the
            // Prometheus/Loki/Pyroscope `/series` convention). Projecting onto an
            // empty name list previously yielded one empty label set (`[{}]`),
            // which broke Grafana's Pyroscope label autocomplete.
            let mut projected = if label_names.is_empty() {
                labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect::<Vec<_>>()
            } else {
                label_names
                    .iter()
                    .filter_map(|name| {
                        labels
                            .get(name)
                            .map(|value| (name.clone(), value.to_string()))
                    })
                    .collect::<Vec<_>>()
            };
            // Pyroscope's `/series` emits each set's labels SORTED by name. The
            // full-label-set form already iterates the `BTreeMap` in key order, but
            // the projected form follows the request's `label_names` order, so sort
            // unconditionally to keep the wire order identical to Pyroscope's.
            projected.sort();
            if !projected.is_empty() {
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

    /// Tightest `(min, max)` time bounds across blocks overlapping the range.
    #[must_use]
    pub fn block_time_bounds(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
        let tenant_index = self.tenants.get(tenant)?;
        tenant_index
            .blocks
            .iter()
            .filter(|block| block.min_ts <= max_ts && block.max_ts >= min_ts)
            .fold(None, |acc, block| match acc {
                Some((min, max)) => Some((min.min(block.min_ts), max.max(block.max_ts))),
                None => Some((block.min_ts, block.max_ts)),
            })
    }

    /// Replace the `remove_keys` blocks with `add` (compaction swap).
    pub fn replace_blocks(&mut self, tenant: &str, remove_keys: &[String], add: &[BlockMeta]) {
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        let remove_keys = remove_keys.iter().collect::<BTreeSet<_>>();
        tenant_index
            .blocks
            .retain(|block| !remove_keys.contains(&block.object_key));
        for meta in add {
            tenant_index.blocks.push(BlockEntry {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                row_count: meta.row_count,
                fingerprints: meta.fingerprints.iter().copied().collect(),
            });
        }
    }

    /// Every block across every tenant, as [`BlockMeta`]. Use
    /// [`Index::all_blocks`] when a tenant is known.
    #[must_use]
    pub fn all_blocks_unscoped(&self) -> Vec<BlockMeta> {
        self.tenants
            .iter()
            .flat_map(|(tenant, tenant_index)| {
                tenant_index.blocks.iter().map(move |block| BlockMeta {
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

    /// Number of blocks recorded for a tenant.
    #[must_use]
    pub fn block_count(&self, tenant: &str) -> usize {
        self.tenants
            .get(tenant)
            .map_or(0, |tenant_index| tenant_index.blocks.len())
    }

    /// Object keys of blocks overlapping `[min_ts, max_ts]`, ignoring
    /// fingerprints.
    #[must_use]
    pub fn blocks_in_range(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        tenant_index
            .blocks
            .iter()
            .filter(|block| block.min_ts <= max_ts && block.max_ts >= min_ts)
            .map(|block| block.object_key.clone())
            .collect()
    }

    /// Persist the index as a JSON snapshot to object storage.
    #[instrument(
        skip_all,
        fields(object_key = %object_key, len = tracing::field::Empty),
        err
    )]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save(&self, store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        tracing::Span::current().record("len", bytes.len());
        let path = Path::from(object_key);
        store.put(&path, PutPayload::from(bytes)).await?;
        Ok(())
    }

    /// Load an index JSON snapshot from object storage.
    ///
    /// The object is `head()`ed first and rejected when larger than
    /// [`MAX_INDEX_SNAPSHOT_BYTES`], so a corrupt or oversized snapshot from
    /// shared storage cannot OOM the process during the buffered read.
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load(store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<Self> {
        Self::load_with_cap(store, object_key, MAX_INDEX_SNAPSHOT_BYTES).await
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(object_key = %object_key),
        err
    )]
    async fn load_with_cap(
        store: &Arc<dyn ObjectStore>,
        object_key: &str,
        max_bytes: usize,
    ) -> Result<Self> {
        let path = Path::from(object_key);
        let bytes = crabka_object_store::read_capped(store, &path, max_bytes as u64)
            .await
            .map_err(|e| match e {
                crabka_object_store::ObjectStoreError::TooLarge { size, max_bytes, .. } => {
                    BlockStoreError::InvalidBlock(format!(
                        "index snapshot `{object_key}` is {size} bytes, exceeds cap of {max_bytes} bytes"
                    ))
                }
                other => BlockStoreError::ObjectStore(other.to_string()),
            })?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

impl BlockIndex for Index {
    fn add_block(&mut self, meta: &BlockMeta) {
        Self::add_block(self, meta);
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        self.blocks_in_range(tenant, min_ts, max_ts)
    }

    fn block_count(&self, tenant: &str) -> usize {
        Self::block_count(self, tenant)
    }
}

impl TenantIndex {
    fn all_fingerprints(&self) -> BTreeSet<SeriesFingerprint> {
        self.series.keys().copied().collect()
    }

    fn exact_posting(&self, name: &str, value: &str) -> BTreeSet<SeriesFingerprint> {
        self.postings
            .get(name)
            .and_then(|values| values.get(value))
            .cloned()
            .unwrap_or_default()
    }

    fn resolve_one(&self, label_matcher: &LabelMatcher) -> Result<BTreeSet<SeriesFingerprint>> {
        if label_matcher.name == QUERY_SHARD_LABEL {
            return self.resolve_query_shard(label_matcher);
        }

        match label_matcher.op {
            MatchOp::Eq => {
                if label_matcher.value.is_empty() {
                    let present = self.present_fingerprints(&label_matcher.name);
                    let mut matched: BTreeSet<SeriesFingerprint> = self
                        .series
                        .keys()
                        .copied()
                        .filter(|fp| !present.contains(fp))
                        .collect();
                    matched.extend(self.exact_posting(&label_matcher.name, ""));
                    Ok(matched)
                } else {
                    Ok(self.exact_posting(&label_matcher.name, &label_matcher.value))
                }
            }
            MatchOp::Neq => {
                let excluded = if label_matcher.value.is_empty() {
                    let present = self.present_fingerprints(&label_matcher.name);
                    let mut excluded: BTreeSet<SeriesFingerprint> = self
                        .series
                        .keys()
                        .copied()
                        .filter(|fp| !present.contains(fp))
                        .collect();
                    excluded.extend(self.exact_posting(&label_matcher.name, ""));
                    excluded
                } else {
                    self.exact_posting(&label_matcher.name, &label_matcher.value)
                };
                Ok(self
                    .series
                    .keys()
                    .copied()
                    .filter(|fp| !excluded.contains(fp))
                    .collect())
            }
            MatchOp::Re | MatchOp::Nre => self.resolve_regex(label_matcher),
        }
    }

    fn resolve_query_shard(
        &self,
        label_matcher: &LabelMatcher,
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let selector = parse_query_shard_selector(&label_matcher.value).map_err(|error| {
            BlockStoreError::InvalidBlock(format!("invalid query shard matcher: {error}"))
        })?;
        match label_matcher.op {
            MatchOp::Eq => Ok(self
                .series
                .keys()
                .copied()
                .filter(|fp| selector.matches(*fp))
                .collect()),
            MatchOp::Neq => Ok(self
                .series
                .keys()
                .copied()
                .filter(|fp| !selector.matches(*fp))
                .collect()),
            MatchOp::Re | MatchOp::Nre => Err(BlockStoreError::InvalidBlock(
                "query shard matcher must use equality or inequality".to_string(),
            )),
        }
    }

    fn resolve_regex(&self, label_matcher: &LabelMatcher) -> Result<BTreeSet<SeriesFingerprint>> {
        let regex = regex::Regex::new(&anchored_regex(&label_matcher.value)).map_err(|error| {
            BlockStoreError::InvalidBlock(format!("invalid label matcher regex: {error}"))
        })?;

        let mut matched_fps = BTreeSet::new();
        if regex.is_match("") {
            let present = self.present_fingerprints(&label_matcher.name);
            matched_fps.extend(
                self.series
                    .keys()
                    .copied()
                    .filter(|fp| !present.contains(fp)),
            );
        }
        if let Some(values) = self.postings.get(&label_matcher.name) {
            for (value, fps) in values {
                if regex.is_match(value) {
                    matched_fps.extend(fps.iter().copied());
                }
            }
        }

        if label_matcher.op == MatchOp::Re {
            Ok(matched_fps)
        } else {
            Ok(self
                .all_fingerprints()
                .difference(&matched_fps)
                .copied()
                .collect())
        }
    }

    fn present_fingerprints(&self, name: &str) -> BTreeSet<SeriesFingerprint> {
        self.postings
            .get(name)
            .map(|values| {
                values
                    .values()
                    .flat_map(|fps| fps.iter().copied())
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn anchored_regex(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

/// Whether `matcher` matches a series for which the label is absent (i.e. the
/// matcher matches the empty string), following Prometheus `Matcher.Matches("")`
/// semantics. A selector built entirely from such matchers restricts nothing.
fn matcher_matches_empty(matcher: &LabelMatcher) -> Result<bool> {
    match matcher.op {
        MatchOp::Eq => Ok(matcher.value.is_empty()),
        MatchOp::Neq => Ok(!matcher.value.is_empty()),
        MatchOp::Re | MatchOp::Nre => {
            let regex = regex::Regex::new(&anchored_regex(&matcher.value)).map_err(|error| {
                BlockStoreError::InvalidBlock(format!("invalid label matcher regex: {error}"))
            })?;
            let regex_matches_empty = regex.is_match("");
            Ok(match matcher.op {
                MatchOp::Re => regex_matches_empty,
                // `name!~"re"` selects series whose value does not match `re`;
                // the empty/absent value is selected exactly when `re` itself
                // does not match the empty string.
                _ => !regex_matches_empty,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{
        block::BlockMeta,
        labels::Labels,
        matcher::{LabelMatcher, MatchOp},
    };

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    fn seed() -> Index {
        let mut idx = Index::new();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]);
        let api_dev = labels(&[("app", "api"), ("env", "dev")]);
        let web_prod = labels(&[("app", "web"), ("env", "prod")]);
        idx.add_series("t", api_prod.fingerprint(), &api_prod);
        idx.add_series("t", api_dev.fingerprint(), &api_dev);
        idx.add_series("t", web_prod.fingerprint(), &web_prod);
        idx
    }

    #[test]
    fn snapshot_size_cap_is_256_mib() {
        assert2::assert!(MAX_INDEX_SNAPSHOT_BYTES == 256 * 1024 * 1024);
    }

    #[test]
    fn resolve_matcher_cases() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        for (_name, tenant, matchers, expected) in [
            (
                "equal intersection",
                "t",
                vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Eq, "prod"),
                ],
                BTreeSet::from([api_prod]),
            ),
            (
                "not equal exclusion",
                "t",
                vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "prod"),
                ],
                BTreeSet::from([api_dev]),
            ),
            (
                "regex union",
                "t",
                vec![LabelMatcher::new("env", MatchOp::Re, "pro.*")],
                BTreeSet::from([api_prod, web_prod]),
            ),
            (
                "unknown tenant",
                "nope",
                vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                BTreeSet::new(),
            ),
        ] {
            assert2::assert!(idx.resolve(tenant, &matchers).unwrap() == expected);
        }
    }

    #[test]
    fn eq_does_not_collide_across_nul_boundary() {
        // `("x", "a\0b")` and `("x\0a", "b")` share the same naive
        // `name\0value` byte string, so an in-band NUL delimiter would index
        // both under one bucket and contaminate Eq results across series.
        let mut idx = Index::new();
        let s1 = labels(&[("x", "a\u{0}b")]);
        let s2 = labels(&[("x\u{0}a", "b")]);
        idx.add_series("t", s1.fingerprint(), &s1);
        idx.add_series("t", s2.fingerprint(), &s2);

        for (_name, matcher, expected) in [
            (
                "NUL in label value",
                LabelMatcher::new("x", MatchOp::Eq, "a\u{0}b"),
                BTreeSet::from([s1.fingerprint()]),
            ),
            (
                "NUL in label name",
                LabelMatcher::new("x\u{0}a", MatchOp::Eq, "b"),
                BTreeSet::from([s2.fingerprint()]),
            ),
        ] {
            assert2::assert!(idx.resolve("t", &[matcher]).unwrap() == expected);
        }
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

        for (_name, min_ts, max_ts, expected) in [
            (
                "matching fingerprint and time",
                0,
                150,
                vec!["b1.parquet".to_string()],
            ),
            ("outside time range", 500, 600, Vec::new()),
        ] {
            assert2::assert!(
                idx.candidate_blocks("t", &BTreeSet::from([api_prod]), min_ts, max_ts) == expected
            );
        }
    }

    #[test]
    fn label_names_and_values() {
        let idx = seed();
        assert2::assert!(idx.label_names("t") == vec!["app".to_string(), "env".to_string()]);
        assert2::assert!(
            idx.label_values("t", "env") == vec!["dev".to_string(), "prod".to_string()]
        );
    }

    #[test]
    fn invalid_regex_returns_err() {
        let idx = seed();

        let got = idx.resolve("t", &[LabelMatcher::new("env", MatchOp::Re, "[")]);

        assert2::assert!(got.is_err());
    }

    #[test]
    fn empty_matchers_returns_err() {
        let idx = seed();

        let got = idx.resolve("t", &[]);

        assert2::assert!(got.is_err());
    }

    #[test]
    fn all_empty_matching_selector_returns_err() {
        let idx = seed();

        // Every matcher below matches the empty (absent) value, so the selector
        // restricts nothing and would force a full tenant scan; Prometheus
        // rejects it. Each is tested as the sole matcher in the selector.
        let cases = [
            (
                "not-equal matcher accepts empty",
                vec![LabelMatcher::new("foo", MatchOp::Neq, "bar")],
                false,
            ),
            (
                "equal-empty matcher",
                vec![LabelMatcher::new("foo", MatchOp::Eq, "")],
                false,
            ),
            (
                "match-all regex",
                vec![LabelMatcher::new("foo", MatchOp::Re, ".*")],
                false,
            ),
            (
                "negative regex accepts empty",
                vec![LabelMatcher::new("foo", MatchOp::Nre, "bar")],
                false,
            ),
            (
                "synthetic shard only",
                vec![LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2")],
                false,
            ),
            (
                "restricting regex",
                vec![LabelMatcher::new("foo", MatchOp::Re, ".*bar.*")],
                true,
            ),
            (
                "non-empty regex",
                vec![LabelMatcher::new("foo", MatchOp::Re, ".+")],
                true,
            ),
            (
                "empty matcher paired with restricting matcher",
                vec![
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "dev"),
                ],
                true,
            ),
        ];
        for (_name, matchers, expected_ok) in cases {
            assert2::assert!(idx.resolve("t", &matchers).is_ok() == expected_ok);
        }
    }

    #[test]
    fn tenant_isolation_for_same_labels() {
        let mut idx = Index::new();
        let tenant_a = labels(&[("app", "api"), ("env", "prod")]);
        let tenant_b = labels(&[("app", "api"), ("env", "prod")]);
        let other = labels(&[("app", "web"), ("env", "prod")]);
        idx.add_series("a", tenant_a.fingerprint(), &tenant_a);
        idx.add_series("b", tenant_b.fingerprint(), &tenant_b);
        idx.add_series("b", other.fingerprint(), &other);

        let got = idx
            .resolve("a", &[LabelMatcher::new("env", MatchOp::Eq, "prod")])
            .unwrap();

        assert2::assert!(got == BTreeSet::from([tenant_a.fingerprint()]));
    }

    #[test]
    fn add_series_is_idempotent_for_existing_fingerprint() {
        let mut idx = Index::new();
        let original = labels(&[("app", "api")]);
        let replacement = labels(&[("app", "web"), ("env", "prod")]);
        let fp = original.fingerprint();
        idx.add_series("t", fp, &original);
        idx.add_series("t", fp, &replacement);

        let snapshot = serde_json::to_string(&idx).unwrap();
        assert2::assert!(idx.label_names("t") == vec!["app".to_string()]);
        assert2::assert!(
            idx.resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
                .unwrap()
                == BTreeSet::from([fp])
        );
        assert2::assert!(!snapshot.contains("web"));
        assert2::assert!(!snapshot.contains("env"));
    }

    #[test]
    fn absent_labels_match_empty_string_semantics() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        let all = BTreeSet::from([api_prod, api_dev, web_prod]);

        // The empty-string matchers below all match the absent label, so each is
        // anchored with a non-empty `app=~".+"` matcher (which selects every
        // seeded series) to form a valid Prometheus vector selector; the anchor
        // does not change the empty-string posting result under test.
        let anchor = LabelMatcher::new("app", MatchOp::Re, ".+");

        for (_name, matcher, expected) in [
            (
                "equal empty",
                LabelMatcher::new("missing", MatchOp::Eq, ""),
                all.clone(),
            ),
            (
                "regex empty",
                LabelMatcher::new("missing", MatchOp::Re, ".*"),
                all,
            ),
            (
                "not equal empty",
                LabelMatcher::new("missing", MatchOp::Neq, ""),
                BTreeSet::new(),
            ),
            (
                "not regex empty",
                LabelMatcher::new("missing", MatchOp::Nre, ".*"),
                BTreeSet::new(),
            ),
        ] {
            assert2::assert!(idx.resolve("t", &[anchor.clone(), matcher]).unwrap() == expected);
        }
    }

    #[test]
    fn present_empty_labels_match_empty_string_semantics() {
        let mut idx = Index::new();
        let empty_zone = labels(&[("app", "api"), ("zone", "")]);
        let absent_zone = labels(&[("app", "web")]);
        let non_empty_zone = labels(&[("app", "db"), ("zone", "us")]);
        idx.add_series("t", empty_zone.fingerprint(), &empty_zone);
        idx.add_series("t", absent_zone.fingerprint(), &absent_zone);
        idx.add_series("t", non_empty_zone.fingerprint(), &non_empty_zone);
        let empty_equivalent =
            BTreeSet::from([empty_zone.fingerprint(), absent_zone.fingerprint()]);

        // `zone=""` matches the empty string, so anchor with a non-empty matcher
        // (`app=~".+"` selects all three series) to form a valid selector.
        let anchor = LabelMatcher::new("app", MatchOp::Re, ".+");

        for (_name, matchers, expected) in [
            (
                "equal empty",
                vec![anchor, LabelMatcher::new("zone", MatchOp::Eq, "")],
                empty_equivalent,
            ),
            (
                "not equal empty",
                vec![LabelMatcher::new("zone", MatchOp::Neq, "")],
                BTreeSet::from([non_empty_zone.fingerprint()]),
            ),
        ] {
            assert2::assert!(idx.resolve("t", &matchers).unwrap() == expected);
        }
    }

    #[test]
    fn resolve_query_shard_matcher_filters_by_series_fingerprint_modulo() {
        let mut idx = Index::new();
        let series = (0..12)
            .map(|id| labels(&[("app", "api"), ("series", &id.to_string())]))
            .collect::<Vec<_>>();
        for labels in &series {
            idx.add_series("t", labels.fingerprint(), labels);
        }

        let expected = series
            .iter()
            .map(Labels::fingerprint)
            .filter(|fp| fp % 2 == 0)
            .collect::<BTreeSet<_>>();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2"),
                ],
            )
            .unwrap();

        assert2::assert!(!expected.is_empty());
        assert2::assert!(expected.len() < series.len());
        assert2::assert!(got == expected);
    }

    #[test]
    fn resolve_query_shard_not_equal_returns_complement() {
        let mut idx = Index::new();
        let series = (0..12)
            .map(|id| labels(&[("app", "api"), ("series", &id.to_string())]))
            .collect::<Vec<_>>();
        for labels in &series {
            idx.add_series("t", labels.fingerprint(), labels);
        }

        let expected = series
            .iter()
            .map(Labels::fingerprint)
            .filter(|fp| fp % 2 != 0)
            .collect::<BTreeSet<_>>();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("__query_shard__", MatchOp::Neq, "1_of_2"),
                ],
            )
            .unwrap();

        assert2::assert!(!expected.is_empty());
        assert2::assert!(expected.len() < series.len());
        assert2::assert!(got == expected);
    }

    #[test]
    fn matching_fingerprints_returns_matched_set() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        for (_name, tenant, matchers, expected) in [
            (
                "specific matcher",
                "t",
                vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                BTreeSet::from([api_prod, api_dev]),
            ),
            (
                "all tenant series",
                "t",
                Vec::new(),
                BTreeSet::from([api_prod, api_dev, web_prod]),
            ),
            ("unknown tenant", "nope", Vec::new(), BTreeSet::new()),
        ] {
            assert2::assert!(idx.matching_fingerprints(tenant, &matchers).unwrap() == expected);
        }
    }

    #[test]
    fn label_names_for_returns_distinct_sorted_names() {
        let idx = seed();
        let names = idx
            .label_names_for("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert2::assert!(names == vec!["app".to_string(), "env".to_string()]);
        assert2::assert!(idx.label_names_for("nope", &[]).unwrap().is_empty());
    }

    #[test]
    fn label_names_for_fingerprints_returns_distinct_names() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let names = idx.label_names_for_fingerprints("t", &BTreeSet::from([api_prod]));
        assert2::assert!(names == vec!["app".to_string(), "env".to_string()]);
        assert2::assert!(
            idx.label_names_for_fingerprints("nope", &BTreeSet::from([api_prod]))
                .is_empty()
        );
    }

    #[test]
    fn label_values_for_returns_distinct_sorted_values() {
        let idx = seed();
        for (_name, tenant, matchers, expected) in [
            (
                "all api environments",
                "t",
                vec![LabelMatcher::new("app", MatchOp::Eq, "api")],
                vec!["dev".to_string(), "prod".to_string()],
            ),
            (
                "only web environment",
                "t",
                vec![LabelMatcher::new("app", MatchOp::Eq, "web")],
                vec!["prod".to_string()],
            ),
            ("unknown tenant", "nope", Vec::new(), Vec::new()),
        ] {
            assert2::assert!(idx.label_values_for(tenant, "env", &matchers).unwrap() == expected);
        }
    }

    #[test]
    fn label_values_for_fingerprints_returns_distinct_values() {
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let values =
            idx.label_values_for_fingerprints("t", "env", &BTreeSet::from([api_prod, api_dev]));
        assert2::assert!(values == vec!["dev".to_string(), "prod".to_string()]);
        assert2::assert!(
            idx.label_values_for_fingerprints("nope", "env", &BTreeSet::from([api_prod]))
                .is_empty()
        );
    }

    #[test]
    fn series_projects_requested_label_names() {
        let idx = seed();
        let got = idx
            .series_projected(
                "t",
                &[LabelMatcher::new("app", MatchOp::Eq, "api")],
                &["app".to_string(), "env".to_string()],
            )
            .unwrap();
        assert2::assert!(
            got == vec![
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "dev".to_string())
                ],
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "prod".to_string())
                ],
            ]
        );
        assert2::assert!(
            idx.series_projected("nope", &[], &["app".to_string()])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn series_returns_full_label_sets() {
        let idx = seed();
        let got = idx
            .series("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert2::assert!(
            got == vec![
                labels(&[("app", "api"), ("env", "prod")]),
                labels(&[("app", "api"), ("env", "dev")]),
            ]
        );

        let mut expected_all = vec![
            labels(&[("app", "api"), ("env", "prod")]),
            labels(&[("app", "api"), ("env", "dev")]),
            labels(&[("app", "web"), ("env", "prod")]),
        ];
        expected_all.sort_by_key(Labels::fingerprint);
        assert2::assert!(idx.series("t", &[]).unwrap() == expected_all);
        assert2::assert!(idx.series("nope", &[]).unwrap() == Vec::new());
    }

    #[test]
    fn series_for_fingerprints_projects_label_values() {
        let idx = seed();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        let got = idx.series_for_fingerprints(
            "t",
            &BTreeSet::from([web_prod]),
            &["app".to_string(), "env".to_string()],
        );
        assert2::assert!(
            got == vec![vec![
                ("app".to_string(), "web".to_string()),
                ("env".to_string(), "prod".to_string()),
            ]]
        );
        assert2::assert!(
            idx.series_for_fingerprints("nope", &BTreeSet::from([web_prod]), &["app".to_string()])
                .is_empty()
        );
    }

    #[test]
    fn series_for_fingerprints_projection_is_sorted_by_name() {
        // Pyroscope's `/series` emits each set's labels SORTED by name regardless
        // of the request's `labelNames` order. Request the projection in REVERSE
        // sorted order (`env` before `app`) and assert the response is still
        // `[app, env]` — the wire order the Grafana drilldown compares against.
        let idx = seed();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        let got = idx.series_for_fingerprints(
            "t",
            &BTreeSet::from([web_prod]),
            &["env".to_string(), "app".to_string()],
        );
        assert2::assert!(
            got == vec![vec![
                ("app".to_string(), "web".to_string()),
                ("env".to_string(), "prod".to_string()),
            ]]
        );
    }

    #[test]
    fn series_for_fingerprints_empty_names_returns_full_label_sets() {
        // Empty `label_names` means "return all labels" (the
        // Prometheus/Loki/Pyroscope `/series` convention). Previously this
        // returned a single empty label set (`[{}]`), breaking Grafana's
        // Pyroscope label autocomplete.
        let idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        let mut got = idx.series_for_fingerprints("t", &BTreeSet::from([api_prod, api_dev]), &[]);
        got.sort();
        assert2::assert!(
            got == vec![
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "dev".to_string()),
                ],
                vec![
                    ("app".to_string(), "api".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ],
            ]
        );
    }

    #[test]
    fn candidate_blocks_for_series_returns_pruned_keys() {
        let mut idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        });
        let got = idx.candidate_blocks_for_series("t", &BTreeSet::from([api_prod]), 0, 150);
        assert2::assert!(got == vec!["b1.parquet".to_string()]);
    }

    #[test]
    fn block_time_bounds_spans_overlapping_blocks() {
        let mut idx = seed();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 10,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![],
        });
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 200,
            max_ts: 350,
            row_count: 1,
            fingerprints: vec![],
        });

        for (_name, tenant, min_ts, max_ts, want) in [
            // Window covering both → combined min/max across them.
            ("both blocks", "t", 0, 1_000, Some((10, 350))),
            // Window covering only b1 → exactly b1's bounds (kills Some((x,y)) stubs).
            ("first block", "t", 0, 150, Some((10, 100))),
            // Window that overlaps nothing → None.
            ("no overlap", "t", 500, 600, None),
            // Unknown tenant → None.
            ("unknown tenant", "nope", 0, 1_000, None),
        ] {
            assert2::assert!(idx.block_time_bounds(tenant, min_ts, max_ts) == want);
        }
    }

    #[test]
    fn block_time_bounds_overlap_filter_is_inclusive_on_both_ends() {
        let mut idx = Index::new();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b.parquet".into(),
            min_ts: 100,
            max_ts: 200,
            row_count: 1,
            fingerprints: vec![],
        });

        for (_name, min_ts, max_ts, want) in [
            // Touch the block's max at the window's min: b.min_ts(100) <= max_ts(200)
            // && b.max_ts(200) >= min_ts(200). `<=`→`>` or `>=`→`<` would drop it.
            ("touches maximum", 200, 300, Some((100, 200))),
            // Touch the block's min at the window's max.
            ("touches minimum", 0, 100, Some((100, 200))),
            // A window entirely above the block: with `&&`→`||` this would wrongly
            // include the block (one side still true), so demand None here.
            ("entirely above", 300, 400, None),
            // A window entirely below the block: the other side is the true one.
            ("entirely below", 0, 50, None),
        ] {
            assert2::assert!(idx.block_time_bounds("t", min_ts, max_ts) == want);
        }
    }

    #[test]
    fn all_blocks_lists_every_registered_block() {
        let mut idx = seed();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 7,
            fingerprints: vec![],
        });
        idx.add_block(&BlockMeta {
            tenant: "u".into(),
            object_key: "b2.parquet".into(),
            min_ts: 5,
            max_ts: 9,
            row_count: 3,
            fingerprints: vec![],
        });

        let mut blocks = idx.all_blocks_unscoped();
        blocks.sort_by(|a, b| a.object_key.cmp(&b.object_key));
        assert2::assert!(
            blocks
                == vec![
                    BlockMeta {
                        tenant: "t".to_string(),
                        object_key: "b1.parquet".to_string(),
                        min_ts: 0,
                        max_ts: 100,
                        row_count: 7,
                        fingerprints: vec![],
                    },
                    BlockMeta {
                        tenant: "u".to_string(),
                        object_key: "b2.parquet".to_string(),
                        min_ts: 5,
                        max_ts: 9,
                        row_count: 3,
                        fingerprints: vec![],
                    },
                ]
        );

        // Tenant-scoped `all_blocks` returns only that tenant's blocks.
        assert2::assert!(
            idx.all_blocks("t")
                == vec![BlockMeta {
                    tenant: "t".to_string(),
                    object_key: "b1.parquet".to_string(),
                    min_ts: 0,
                    max_ts: 100,
                    row_count: 7,
                    fingerprints: vec![],
                }]
        );
    }

    #[test]
    fn resolve_nre_excludes_regex_matches() {
        // `Nre` negates the regex match set: the `all_fingerprints().difference`
        // against the matches. Deleting the negation would flip it to keep only
        // the matches. A bare `{env!~"pro.*"}` is rejected by the non-empty
        // matcher gate (it matches absent `env`, exactly Prometheus' rule), so
        // anchor it with `app=~".+"`, which matches all three seed series and
        // therefore leaves the negated `env` match as the sole discriminator.
        let idx = seed();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Re, ".+"),
                    LabelMatcher::new("env", MatchOp::Nre, "pro.*"),
                ],
            )
            .unwrap();
        // Only the `env=dev` series survives the negated `pro.*` match.
        let api_dev = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        assert2::assert!(got == BTreeSet::from([api_dev]));
    }

    #[test]
    fn index_implements_block_index_time_prefilter() {
        let mut idx = seed();
        <Index as BlockIndex>::add_block(
            &mut idx,
            &BlockMeta {
                tenant: "t".into(),
                object_key: "b1.parquet".into(),
                min_ts: 0,
                max_ts: 100,
                row_count: 1,
                fingerprints: vec![],
            },
        );
        <Index as BlockIndex>::add_block(
            &mut idx,
            &BlockMeta {
                tenant: "t".into(),
                object_key: "b2.parquet".into(),
                min_ts: 200,
                max_ts: 300,
                row_count: 1,
                fingerprints: vec![],
            },
        );

        assert2::assert!(<Index as BlockIndex>::block_count(&idx, "t") == 2);
        assert2::assert!(
            <Index as BlockIndex>::candidate_blocks(&idx, "t", 50, 150)
                == vec!["b1.parquet".to_string()]
        );
    }

    #[test]
    fn add_block_is_idempotent_by_object_key() {
        let mut idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let meta = BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        };

        idx.add_block(&meta);
        idx.add_block(&meta);

        let got = idx.candidate_blocks("t", &BTreeSet::from([api_prod]), 0, 100);
        assert2::assert!(got == vec!["b1.parquet".to_string()]);
    }

    #[tokio::test]
    async fn snapshot_round_trips() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory};

        let idx = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/snapshot.json").await.unwrap();

        let loaded = Index::load(&store, "index/snapshot.json").await.unwrap();
        let got = loaded
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();

        assert2::assert!(
            got == BTreeSet::from([
                labels(&[("app", "api"), ("env", "prod")]).fingerprint(),
                labels(&[("app", "api"), ("env", "dev")]).fingerprint(),
            ])
        );
    }

    #[tokio::test]
    async fn load_rejects_over_cap_snapshot() {
        use std::sync::Arc;

        use object_store::{ObjectStore, memory::InMemory, path::Path};

        let idx = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/snapshot.json").await.unwrap();

        // A tiny cap stands in for the production cap so the test need not
        // materialize an over-cap object; the real snapshot is well above 1 byte.
        let size = store
            .head(&Path::from("index/snapshot.json"))
            .await
            .unwrap()
            .size;
        assert2::assert!(size > 1);

        let got = Index::load_with_cap(&store, "index/snapshot.json", 1).await;
        assert2::assert!(got.is_err());

        // A cap at/above the real size still loads.
        let loaded = Index::load_with_cap(
            &store,
            "index/snapshot.json",
            usize::try_from(size).unwrap(),
        )
        .await
        .unwrap();
        assert2::assert!(loaded.block_count("t") == idx.block_count("t"));
    }
}

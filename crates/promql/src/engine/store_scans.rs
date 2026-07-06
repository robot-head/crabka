use std::{collections::BTreeMap, sync::Arc};

use crabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};

use super::{
    PromqlEngine,
    row_cache::{
        FloatRow, HistogramRow, RANGE_SCAN_CACHE, collect_float_rows, collect_histogram_rows,
        matchers_cache_key,
    },
};
use crate::{PromqlError, error::Result, store::MetricStore};

impl<S: MetricStore> PromqlEngine<S> {
    async fn labels_by_fingerprint(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Labels>> {
        // Range queries resolve the same selector's series at every step. Labels
        // are window-independent, so cache the union-window resolution once per
        // matcher set and reuse it across steps (see RANGE_SCAN_CACHE). Requests
        // outside the pre-scanned union fall back to a direct resolution.
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.labels.get(&key).cloned()
                };
                let resolved = if let Some(map) = cached {
                    map
                } else {
                    let map = Arc::new(
                        self.labels_by_fingerprint_uncached(
                            tenant,
                            matchers,
                            full_start_ms,
                            full_end_ms,
                        )
                        .await?,
                    );
                    cache
                        .lock()
                        .expect("range scan cache poisoned")
                        .labels
                        .insert(key, Arc::clone(&map));
                    map
                };
                return Ok((*resolved).clone());
            }
        }
        self.labels_by_fingerprint_uncached(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn labels_by_fingerprint_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Labels>> {
        Ok(self
            .store
            .series(tenant, matchers, start_ms, end_ms)
            .await?
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect())
    }

    pub(super) async fn labels_by_fingerprint_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Labels>> {
        let mut out = BTreeMap::new();
        for matchers in matcher_sets {
            out.extend(
                self.labels_by_fingerprint(tenant, matchers, start_ms, end_ms)
                    .await?,
            );
        }
        Ok(out)
    }

    async fn scan_float_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        // Inside a range query (see RANGE_SCAN_CACHE), serve overlapping per-step
        // scans from a single union-window scan per matcher set. A request that
        // falls outside the pre-scanned union (offset/@-modifier, or a `[range]`
        // longer than the lookback) bypasses the cache and scans directly, so
        // results are identical - only redundant re-scans are eliminated.
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.floats.get(&key).cloned()
                };
                let full_rows = if let Some(rows) = cached {
                    rows
                } else {
                    let rows = Arc::new(
                        self.scan_float_rows_uncached(tenant, matchers, full_start_ms, full_end_ms)
                            .await?,
                    );
                    cache
                        .lock()
                        .expect("range scan cache poisoned")
                        .floats
                        .insert(key, Arc::clone(&rows));
                    rows
                };
                return Ok(full_rows
                    .iter()
                    .filter(|row| row.ts_ms >= start_ms && row.ts_ms <= end_ms)
                    .cloned()
                    .collect());
            }
        }
        self.scan_float_rows_uncached(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn scan_float_rows_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        let scan = self.store.scan(tenant, matchers, start_ms, end_ms).await?;
        let Some(table) = scan.float_table.clone() else {
            return Ok(Vec::new());
        };
        collect_float_rows(scan, &table, self.opts.max_samples).await
    }

    pub(super) async fn scan_float_row_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        let mut out = Vec::new();
        for matchers in matcher_sets {
            out.extend(
                self.scan_float_rows(tenant, matchers, start_ms, end_ms)
                    .await?,
            );
            if out.len() > self.opts.max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={}",
                    self.opts.max_samples
                )));
            }
        }
        Ok(out)
    }

    async fn scan_histogram_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        // Mirror scan_float_rows: serve per-step histogram probes from one
        // union-window scan during a range query (see RANGE_SCAN_CACHE).
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.histograms.get(&key).cloned()
                };
                let full_rows = if let Some(rows) = cached {
                    rows
                } else {
                    let rows = Arc::new(
                        self.scan_histogram_rows_uncached(
                            tenant,
                            matchers,
                            full_start_ms,
                            full_end_ms,
                        )
                        .await?,
                    );
                    cache
                        .lock()
                        .expect("range scan cache poisoned")
                        .histograms
                        .insert(key, Arc::clone(&rows));
                    rows
                };
                return Ok(full_rows
                    .iter()
                    .filter(|row| row.ts_ms >= start_ms && row.ts_ms <= end_ms)
                    .cloned()
                    .collect());
            }
        }
        self.scan_histogram_rows_uncached(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn scan_histogram_rows_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        let scan = self.store.scan(tenant, matchers, start_ms, end_ms).await?;
        let Some(table) = scan.histogram_table.clone() else {
            return Ok(Vec::new());
        };
        collect_histogram_rows(scan, &table, self.opts.max_samples).await
    }

    pub(super) async fn scan_histogram_row_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        let mut out = Vec::new();
        for matchers in matcher_sets {
            out.extend(
                self.scan_histogram_rows(tenant, matchers, start_ms, end_ms)
                    .await?,
            );
            if out.len() > self.opts.max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={}",
                    self.opts.max_samples
                )));
            }
        }
        Ok(out)
    }
}

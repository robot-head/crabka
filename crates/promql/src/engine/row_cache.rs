use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use arrow::{
    array::AsArray,
    datatypes::{Float64Type, Int64Type, UInt64Type},
};
use crabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use crabka_metrics::{NativeHistogram, decode_native_histograms};

use crate::{PromqlError, ScanResult, error::Result};

#[derive(Clone)]
pub(super) struct FloatRow {
    pub(super) fp: SeriesFingerprint,
    pub(super) ts_ms: i64,
    pub(super) value: f64,
}

/// Per-range-query float-scan cache (see `PromqlEngine::scan_float_rows`).
///
/// A range query evaluates the same selector at every step, and each step's
/// instant scan covers `[step - lookback, step]`. Those windows overlap almost
/// completely, so a driver without a cache re-scans the store once per step
/// (240x for a 1h/15s query).
///
/// This cache scans the union window `[start - lookback, end]` one time per
/// matcher set and serves each step from the in-memory result. The store is a
/// pure time-range filter, so a filtered superset is byte-for-byte what a direct
/// sub-window scan returns. Both stores keep `[start, end]` inclusive.
///
/// Only requests inside the pre-scanned union use the cache. An
/// `offset`-modified, `@`-modified, or long-`[range]` scan outside the union
/// falls back to a direct scan. Results therefore never change, and only the
/// redundant re-scans are removed.
pub(super) struct RangeScanCacheInner {
    pub(super) full_start_ms: i64,
    pub(super) full_end_ms: i64,
    pub(super) floats: HashMap<String, Arc<Vec<FloatRow>>>,
    /// Per-matcher-set histogram rows over the union window. The instant-selector
    /// path probes for histogram series at every step
    /// (`selector_has_histogram_series`). This probe is a second per-step store
    /// scan next to the float scan, so the cache holds it the same way.
    pub(super) histograms: HashMap<String, Arc<Vec<HistogramRow>>>,
    /// Per-matcher-set fingerprint->labels resolution. A series label set is
    /// immutable, so the union-window result is a superset of the active series
    /// of any sub-window. Callers use it only as a `get(&fp)` lookup keyed by
    /// rows already filtered to the sub-window, so they never read the extra
    /// entries.
    pub(super) labels: HashMap<String, Arc<BTreeMap<SeriesFingerprint, Labels>>>,
}

pub(super) type RangeScanCache = Arc<Mutex<RangeScanCacheInner>>;

tokio::task_local! {
    /// Active only for the dynamic extent of the step loop in
    /// `PromqlEngine::eval_range_via_planner`. Nested range evaluations
    /// (subqueries) shadow it with their own cache and restore the outer cache
    /// on exit, so each range scans its own union.
    pub(super) static RANGE_SCAN_CACHE: RangeScanCache;
}

/// Deterministic cache key for a matcher set. `LabelMatcher` is not `Hash`, but
/// its `Debug` output is stable and uniquely identifies the (name, op, value)
/// triples in order. This is enough, because the same selector returns the same
/// matcher list at every step of a range query.
pub(super) fn matchers_cache_key(matchers: &[LabelMatcher]) -> String {
    format!("{matchers:?}")
}

#[derive(Clone)]
pub(super) struct HistogramRow {
    pub(super) fp: SeriesFingerprint,
    pub(super) ts_ms: i64,
    pub(super) hist: NativeHistogram,
}

pub(super) async fn collect_float_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<FloatRow>> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT series_fingerprint, timestamp, value FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let fps = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        let values = batch.column(2).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            if rows.len() >= max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={max_samples}"
                )));
            }
            rows.push(FloatRow {
                fp: fps.value(row),
                ts_ms: timestamps.value(row),
                value: values.value(row),
            });
        }
    }
    Ok(rows)
}

pub(super) async fn collect_histogram_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<HistogramRow>> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let decoded = decode_native_histograms(&batch)
            .map_err(|error| PromqlError::Store(error.to_string()))?;
        for (fp, ts_ms, hist) in decoded {
            if rows.len() >= max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={max_samples}"
                )));
            }
            rows.push(HistogramRow { fp, ts_ms, hist });
        }
    }
    Ok(rows)
}

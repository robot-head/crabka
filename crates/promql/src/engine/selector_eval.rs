use std::collections::BTreeMap;

use crabka_blockstore::SeriesFingerprint;
use promql_parser::parser::{Expr, MatrixSelector, SubqueryExpr, VectorSelector};

use super::{
    AtModifierBounds, PromqlEngine, RangeEval,
    planner_support::validate_extended_selector_modifier,
    range_functions::{align_subquery_start, instant_smoothed_boundary_value},
    selector::{apply_selector_time_modifier, duration_ms, label_matcher_sets},
};
use crate::{
    PromqlError,
    error::Result,
    extension::is_stale_nan,
    planner::{ExtendedSelectorExpr, ExtendedSelectorModifier},
    result::{InstantSample, QueryResult, RangeSeries, SampleValue},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    pub(super) async fn eval_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            None,
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;

        let mut latest_by_fp: BTreeMap<SeriesFingerprint, (i64, SampleValue)> = BTreeMap::new();
        for row in rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            latest_by_fp
                .entry(row.fp)
                .and_modify(|latest| {
                    if row.ts_ms > latest.0 {
                        *latest = (row.ts_ms, SampleValue::Float(row.value));
                    }
                })
                .or_insert((row.ts_ms, SampleValue::Float(row.value)));
        }
        for row in hist_rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            latest_by_fp
                .entry(row.fp)
                .and_modify(|latest| {
                    if row.ts_ms > latest.0 {
                        *latest = (row.ts_ms, SampleValue::Histogram(row.hist.clone()));
                    }
                })
                .or_insert((row.ts_ms, SampleValue::Histogram(row.hist)));
        }

        let samples = latest_by_fp
            .into_iter()
            .filter_map(|(fp, (ts_ms, value))| {
                if matches!(&value, SampleValue::Float(value) if is_stale_nan(*value)) {
                    return None;
                }
                labels_by_fp.get(&fp).cloned().map(|labels| InstantSample {
                    labels,
                    ts_ms,
                    value,
                })
            })
            .collect();
        Ok(QueryResult::InstantVector(samples))
    }

    pub(super) async fn eval_smoothed_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            None,
        )?;
        let scan_start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let scan_end_ms = eval_time_ms.saturating_add(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;

        let mut rows_by_fp: BTreeMap<SeriesFingerprint, Vec<(i64, f64)>> = BTreeMap::new();
        for row in rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms || is_stale_nan(row.value) {
                continue;
            }
            rows_by_fp
                .entry(row.fp)
                .or_default()
                .push((row.ts_ms, row.value));
        }

        let samples = rows_by_fp
            .into_iter()
            .filter_map(|(fp, mut rows)| {
                rows.sort_by_key(|(ts_ms, _)| *ts_ms);
                let timestamps = rows.iter().map(|(ts_ms, _)| *ts_ms).collect::<Vec<_>>();
                let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
                let value = instant_smoothed_boundary_value(&timestamps, &values, eval_time_ms)?;
                labels_by_fp.get(&fp).cloned().map(|labels| InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(value),
                })
            })
            .collect();
        Ok(QueryResult::InstantVector(samples))
    }

    pub(super) async fn eval_matrix_selector(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        start_ms: i64,
        end_ms: i64,
        modifier: Option<ExtendedSelectorModifier>,
    ) -> Result<Vec<RangeSeries>> {
        let range_ms = duration_ms(selector.range)?;
        let bounds = AtModifierBounds { start_ms, end_ms };
        let eval_start_ms = apply_selector_time_modifier(
            start_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            Some(bounds),
        )?;
        let eval_end_ms = apply_selector_time_modifier(
            end_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            Some(bounds),
        )?;
        let range_start_ms = eval_start_ms.saturating_sub(range_ms);
        let scan_start_ms = match modifier {
            Some(ExtendedSelectorModifier::Anchored | ExtendedSelectorModifier::Smoothed) => {
                range_start_ms.saturating_sub(self.opts.lookback_delta_ms)
            }
            None => range_start_ms,
        };
        let scan_end_ms = match modifier {
            Some(ExtendedSelectorModifier::Smoothed) => {
                eval_end_ms.saturating_add(self.opts.lookback_delta_ms)
            }
            Some(ExtendedSelectorModifier::Anchored) | None => eval_end_ms,
        };
        let matcher_sets = label_matcher_sets(&selector.vs);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;

        let mut samples_by_fp: BTreeMap<SeriesFingerprint, BTreeMap<i64, SampleValue>> =
            BTreeMap::new();
        for row in rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms {
                continue;
            }
            if is_stale_nan(row.value) {
                continue;
            }
            samples_by_fp
                .entry(row.fp)
                .or_default()
                .insert(row.ts_ms, SampleValue::Float(row.value));
        }
        for row in hist_rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms {
                continue;
            }
            samples_by_fp
                .entry(row.fp)
                .or_default()
                .insert(row.ts_ms, SampleValue::Histogram(row.hist));
        }

        let mut out = Vec::new();
        for (fp, samples) in samples_by_fp {
            let Some(labels) = labels_by_fp.get(&fp).cloned() else {
                continue;
            };
            out.push(RangeSeries {
                labels,
                samples: samples.into_iter().collect(),
            });
        }
        Ok(out)
    }

    pub(super) async fn eval_subquery(
        &self,
        tenant: &str,
        subquery: &SubqueryExpr,
        time_ms: i64,
    ) -> Result<Vec<RangeSeries>> {
        let range_ms = duration_ms(subquery.range)?;
        let step_ms = match subquery.step {
            Some(step) => duration_ms(step)?,
            None => self.opts.eval_interval_ms,
        };
        if step_ms <= 0 {
            return Err(PromqlError::Plan(
                "subquery step must be positive".to_string(),
            ));
        }
        let end_ms = apply_selector_time_modifier(
            time_ms,
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            None,
        )?;
        let start_ms = align_subquery_start(end_ms.saturating_sub(range_ms), step_ms);
        // Evaluate the subquery's inner instant expression over its sub-grid
        // through the operator planner (the sole evaluation engine). The planner
        // is total, so it produces a result for every plannable inner; an
        // `Ok(None)` would be a planner bug, surfaced as an internal error.
        self.eval_range_via_planner(tenant, &subquery.expr, start_ms, end_ms, step_ms)
            .await?
            .ok_or_else(|| {
                PromqlError::Plan("planner returned no result for a subquery inner".to_string())
            })
    }

    pub(super) async fn eval_range_arg(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
        function_name: &str,
    ) -> Result<RangeEval> {
        let mut expr = expr;
        let mut modifier = None;
        loop {
            match expr {
                Expr::Paren(paren) => expr = &paren.expr,
                Expr::Extension(extension) => {
                    let Some(extended) = extension
                        .expr
                        .as_any()
                        .downcast_ref::<ExtendedSelectorExpr>()
                    else {
                        return Err(PromqlError::Plan(format!(
                            "{function_name} expects a range-vector selector"
                        )));
                    };
                    validate_extended_selector_modifier(function_name, extended.modifier())?;
                    modifier = Some(extended.modifier());
                    let Some(child) = extended.child() else {
                        return Err(PromqlError::Plan(format!(
                            "{function_name} expects a range-vector selector"
                        )));
                    };
                    expr = child;
                }
                _ => break,
            }
        }

        match expr {
            Expr::MatrixSelector(selector) => {
                let range_ms = duration_ms(selector.range)?;
                let end_ms = apply_selector_time_modifier(
                    time_ms,
                    selector.vs.at.as_ref(),
                    selector.vs.offset.as_ref(),
                    None,
                )?;
                let series = self
                    .eval_matrix_selector(tenant, selector, time_ms, time_ms, modifier)
                    .await?;
                Ok(RangeEval {
                    series,
                    end_ms,
                    range_ms,
                    modifier,
                })
            }
            Expr::Subquery(subquery) => {
                let range_ms = duration_ms(subquery.range)?;
                let end_ms = apply_selector_time_modifier(
                    time_ms,
                    subquery.at.as_ref(),
                    subquery.offset.as_ref(),
                    None,
                )?;
                let series = self.eval_subquery(tenant, subquery, time_ms).await?;
                Ok(RangeEval {
                    series,
                    end_ms,
                    range_ms,
                    modifier,
                })
            }
            _ => Err(PromqlError::Plan(format!(
                "{function_name} expects a range-vector selector"
            ))),
        }
    }
}

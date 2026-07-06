use std::collections::BTreeMap;

use crabka_blockstore::LabelMatcher;
use promql_parser::parser::{Call, Expr, VectorSelector};

use super::{
    PromqlEngine,
    info::{InfoContext, apply_info, info_samples_by_identifying_key, parse_info_call},
    planned::PlannedInstant,
};
use crate::{
    PromqlError,
    error::Result,
    parse_promql,
    result::{InstantSample, QueryResult},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plan an `info(v [, data_label_selector])` call onto the operator path:
    /// parse the (store-independent) [`InfoContext`], recurse the input vector `v`
    /// through the histogram-aware [`Self::histogram_fold_inner_vector`] (so a
    /// histogram-valued input passes through unchanged, exactly as the interpreter
    /// does), select the `target_info` / custom-selector series through the SAME
    /// interpreter helper ([`Self::info_by_key`]), and apply the **shared**
    /// [`apply_info`] join — returning the finished vector as a
    /// [`PlannedInstant::Precomputed`]. Because the context parse, the info-series
    /// selection, and the join all come from the interpreter's own code, the
    /// operator path matches Prometheus by construction (incl. the latest-sample
    /// conflict resolution, the required-matcher drop, and the `target_info` /
    /// info-metric passthrough rules exercised by the conformance corpus).
    ///
    /// Returns `None` (interpreter fallback) only for an input vector the recursive
    /// planner cannot evaluate. Wrong arity or a non-vector-selector data-label
    /// argument surfaces here as `Err` (via [`parse_info_call`]), and a histogram
    /// info-series match surfaces as `Err` (via [`info_samples_by_identifying_key`])
    /// — both identical to the interpreter.
    pub(super) async fn plan_info_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let context = parse_info_call(call)?;
        let Some(samples) = self
            .histogram_fold_inner_vector(tenant, &call.args.args[0], time_ms)
            .await?
        else {
            return Ok(None);
        };
        let info_by_key = self.info_by_key(tenant, &context, time_ms).await?;
        Ok(Some(PlannedInstant::Precomputed(apply_info(
            samples,
            &info_by_key,
            &context,
        ))))
    }

    /// Select the `target_info` (or custom-selector) series and fold them into the
    /// `identifying-key -> info sample` map the [`apply_info`] join consumes. This
    /// is the store-touching half of [`Self::eval_info_call`], shared with the
    /// operator path's `info` dispatch.
    pub(super) async fn info_by_key(
        &self,
        tenant: &str,
        context: &InfoContext<'_>,
        time_ms: i64,
    ) -> Result<BTreeMap<String, InstantSample>> {
        let info_samples = self
            .eval_info_selector_samples(
                tenant,
                context.data_label_selector,
                &context.data_label_matchers,
                time_ms,
            )
            .await?;
        info_samples_by_identifying_key(info_samples, &context.data_label_matchers)
    }

    async fn eval_info_selector_samples(
        &self,
        tenant: &str,
        data_label_selector: Option<&VectorSelector>,
        data_label_matchers: &[LabelMatcher],
        time_ms: i64,
    ) -> Result<Vec<InstantSample>> {
        let selector = if data_label_matchers
            .iter()
            .any(|matcher| matcher.name == "__name__")
        {
            data_label_selector.cloned()
        } else {
            match parse_promql("target_info")? {
                Expr::VectorSelector(selector) => Some(selector),
                _ => {
                    return Err(PromqlError::Plan(
                        "target_info selector did not produce a vector selector".to_string(),
                    ));
                }
            }
        };
        let Some(selector) = selector else {
            return Ok(Vec::new());
        };
        let QueryResult::InstantVector(info_samples) = self
            .eval_instant_selector(tenant, &selector, time_ms)
            .await?
        else {
            return Err(PromqlError::Plan(
                "info selector did not produce an instant vector".to_string(),
            ));
        };
        Ok(info_samples)
    }
}

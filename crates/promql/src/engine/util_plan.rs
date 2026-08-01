use crabka_blockstore::Labels;
use promql_parser::parser::{Call, Expr};

use super::{
    PromqlEngine,
    labels::{absent_labels, labels_without_metric_name},
    planned::PlannedInstant,
    range_functions::range_has_samples,
    scalar::{CalendarFn, calendar_fn_from_function_name},
    selector::timestamp_seconds,
};
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, QueryResult, SampleValue},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plan the float UTILITY functions onto the operator path:
    /// `timestamp`/`scalar`/`vector`, the calendar family, `time`/`pi`, and
    /// `absent`/`absent_over_time`. See `Self::plan_call_expr`.
    ///
    /// Returns `Ok(Some(..))` for a supported, parity-exact shape and `Ok(None)`
    /// (interpreter fallback) for everything else (unknown function, wrong arity,
    /// non-plannable / histogram-bearing inner, non-scalar `vector` arg, …). The
    /// interpreter then raises any canonical arity/type error.
    pub(super) async fn plan_util_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        match call.func.name {
            // `time()` — the eval timestamp in seconds (no arguments).
            "time" => {
                if !call.args.args.is_empty() {
                    return Ok(None);
                }
                Ok(Some(PlannedInstant::PrecomputedScalar {
                    ts_ms: time_ms,
                    value: timestamp_seconds(time_ms),
                }))
            }
            // `pi()` — the constant π (no arguments).
            "pi" => {
                if !call.args.args.is_empty() {
                    return Ok(None);
                }
                Ok(Some(PlannedInstant::PrecomputedScalar {
                    ts_ms: time_ms,
                    value: std::f64::consts::PI,
                }))
            }
            // `scalar(v)` — the lone series' value, else NaN (incl. for a
            // histogram-valued single series, which yields NaN).
            "scalar" => self.plan_scalar_function_call(tenant, call, time_ms).await,
            // `vector(s)` — a single no-label series carrying the scalar `s`.
            "vector" => self.plan_vector_function_call(tenant, call, time_ms).await,
            // `timestamp(v)` — per-row: the sample's timestamp in seconds.
            "timestamp" => self.plan_timestamp_call(tenant, call, time_ms).await,
            // `absent(v)` / `absent_over_time(v[range])`.
            "absent" => self.plan_absent_call(tenant, call, time_ms).await,
            "absent_over_time" => self.plan_absent_over_time_call(tenant, call, time_ms).await,
            // The calendar family over a vector argument, or argless over `time()`.
            other => {
                let Some(kind) = calendar_fn_from_function_name(other) else {
                    return Ok(None);
                };
                self.plan_calendar_call(tenant, call, kind, time_ms).await
            }
        }
    }

    /// Plan `timestamp(v)`: recurse `v` through the planner, assemble it, and map
    /// each row to its own sample timestamp in seconds (dropping `__name__`,
    /// reattaching the eval timestamp), exactly mirroring
    /// `Self::eval_timestamp_call`. Wrong arity, a non-plannable inner, or a
    /// histogram-bearing inner fall back to the interpreter.
    async fn plan_timestamp_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let Some(samples) = self.label_ops_inner_vector(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        // The interpreter's `timestamp` keeps every sample (it does not filter
        // floats vs histograms — it uses only the timestamp). `label_ops_inner_vector`
        // already rejects histogram-bearing bare selectors, but a nested inner
        // could still surface a histogram sample; fall back wholesale so the
        // interpreter (which would keep it) stays the source of truth.
        if samples
            .iter()
            .any(|sample| matches!(sample.value, SampleValue::Histogram(_)))
        {
            return Ok(None);
        }
        let out = samples
            .into_iter()
            .map(|sample| InstantSample {
                labels: labels_without_metric_name(&sample.labels),
                ts_ms: time_ms,
                value: SampleValue::Float(timestamp_seconds(sample.ts_ms)),
            })
            .collect();
        Ok(Some(PlannedInstant::Precomputed(out)))
    }

    /// Plan a calendar function. With one argument it recurses the inner vector
    /// and applies [`CalendarFn::apply`] per float row (dropping non-floats and
    /// `__name__`, reattaching the eval timestamp), mirroring
    /// `Self::eval_calendar_call`. With zero arguments it operates on `time()`
    /// (the eval timestamp in seconds) and yields a `PrecomputedScalar`. Wrong
    /// arity or a non-plannable inner fall back.
    async fn plan_calendar_call(
        &self,
        tenant: &str,
        call: &Call,
        kind: CalendarFn,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            // The argless calendar form operates on `time()`.
            if call.args.args.is_empty() {
                return Ok(Some(PlannedInstant::PrecomputedScalar {
                    ts_ms: time_ms,
                    value: kind.apply(timestamp_seconds(time_ms)),
                }));
            }
            return Ok(None);
        };
        let Some(samples) = self.label_ops_inner_vector(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        let out = samples
            .into_iter()
            .filter_map(|sample| {
                let SampleValue::Float(value) = sample.value else {
                    return None;
                };
                Some(InstantSample {
                    labels: labels_without_metric_name(&sample.labels),
                    ts_ms: time_ms,
                    value: SampleValue::Float(kind.apply(value)),
                })
            })
            .collect();
        Ok(Some(PlannedInstant::Precomputed(out)))
    }

    /// Plan `scalar(v)`: recurse `v` through the planner, assemble it, and return
    /// the lone series' float value, or NaN when `v` is not exactly one series (or
    /// the single series is histogram-valued), mirroring
    /// `Self::eval_scalar_function_call`. Wrong arity or a non-plannable inner
    /// fall back.
    async fn plan_scalar_function_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let Some(planned) = self.plan_instant_expr(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        let QueryResult::InstantVector(samples) =
            self.assemble_planned_instant(planned, time_ms).await?
        else {
            return Ok(None);
        };
        let value = if samples.len() == 1 {
            match samples.into_iter().next().expect("single sample").value {
                SampleValue::Float(value) => value,
                SampleValue::Histogram(_) => f64::NAN,
            }
        } else {
            f64::NAN
        };
        Ok(Some(PlannedInstant::PrecomputedScalar {
            ts_ms: time_ms,
            value,
        }))
    }

    /// Plan `vector(s)`: fold the scalar argument `s` via the interpreter's pure
    /// scalar path and emit a single no-label series carrying that value,
    /// mirroring `Self::eval_vector_function_call`. Wrong arity or a non-scalar
    /// argument fall back.
    async fn plan_vector_function_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let QueryResult::Scalar { value, .. } = self.plan_and_resolve(tenant, arg, time_ms).await?
        else {
            return Ok(None);
        };
        Ok(Some(PlannedInstant::Precomputed(vec![InstantSample {
            labels: Labels::new(),
            ts_ms: time_ms,
            value: SampleValue::Float(value),
        }])))
    }

    /// Plan `absent(v)`: recurse `v` through the planner and assemble it; an
    /// empty result yields a single 1-valued series whose labels are derived from
    /// `v`'s matchers (`absent_labels`), and a non-empty result yields the empty
    /// vector, mirroring `Self::eval_absent_call`. Wrong arity or a non-plannable /
    /// histogram-bearing inner fall back.
    async fn plan_absent_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let Some(planned) = self.plan_instant_expr(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        let QueryResult::InstantVector(samples) =
            self.assemble_planned_instant(planned, time_ms).await?
        else {
            return Ok(None);
        };
        if !samples.is_empty() {
            return Ok(Some(PlannedInstant::Precomputed(Vec::new())));
        }
        Ok(Some(PlannedInstant::Precomputed(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }])))
    }

    /// Plan `absent_over_time(v[range])`: evaluate the range selector via the
    /// shared `Self::eval_range_arg` (parity-exact — the same code the interpreter
    /// runs) and, when no series carries an in-window sample, emit a single
    /// 1-valued series whose labels derive from `v`'s matchers, mirroring
    /// `Self::eval_absent_over_time_call`. A histogram-bearing matrix selector or
    /// any non-matrix-selector inner falls back to the interpreter.
    async fn plan_absent_over_time_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // Wrong arity is the interpreter's canonical error — raise it here too.
        if call.args.args.len() != 1 {
            return Ok(Some(PlannedInstant::Precomputed(
                self.absent_over_time_via_interpreter(tenant, call, time_ms)
                    .await?,
            )));
        }
        let arg = call.args.args[0].as_ref();
        // A bare float-only matrix selector rides the fast `eval_range_arg` path
        // (no operator lowering — the shared `eval_range_arg` is reused verbatim).
        // A HISTOGRAM-bearing matrix, a SUBQUERY range, an anchored/smoothed
        // selector, or any other inner shape delegates to the interpreter's
        // `eval_absent_over_time_call` (parity-exact, and the canonical source of
        // the per-shape error), wrapped as `Precomputed` — so the planner is TOTAL.
        let mut inner = arg;
        while let Expr::Paren(paren) = inner {
            inner = paren.expr.as_ref();
        }
        let needs_interpreter = match inner {
            Expr::MatrixSelector(selector) => {
                self.matrix_selector_has_histogram_series(tenant, selector, time_ms)
                    .await?
            }
            _ => true,
        };
        if needs_interpreter {
            return Ok(Some(PlannedInstant::Precomputed(
                self.absent_over_time_via_interpreter(tenant, call, time_ms)
                    .await?,
            )));
        }

        let range = self
            .eval_range_arg(tenant, &call.args.args[0], time_ms, call.func.name)
            .await?;
        if range
            .series
            .iter()
            .any(|series| range_has_samples(series, range.end_ms, range.range))
        {
            return Ok(Some(PlannedInstant::Precomputed(Vec::new())));
        }
        Ok(Some(PlannedInstant::Precomputed(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }])))
    }

    /// Evaluate `absent_over_time(v[range])` for the shapes the fast planner path
    /// declines (a histogram-bearing matrix, a subquery range, an anchored/smoothed
    /// selector) by reusing the shared `Self::eval_range_arg` leaf kernel — the
    /// SAME code the tree-walking oracle's `eval_absent_over_time_call` runs — so
    /// the result, and the per-shape / wrong-arity error, are byte-for-byte
    /// identical. This keeps the planner self-recursive (no re-entry into the
    /// interpreter dispatch) while still routing these shapes through `Precomputed`.
    async fn absent_over_time_via_interpreter(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Vec<InstantSample>> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let range = self
            .eval_range_arg(tenant, arg, time_ms, call.func.name)
            .await?;
        if range
            .series
            .iter()
            .any(|series| range_has_samples(series, range.end_ms, range.range))
        {
            return Ok(Vec::new());
        }
        Ok(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }])
    }
}

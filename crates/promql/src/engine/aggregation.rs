use std::{cmp::Ordering, collections::BTreeMap};

use crabka_blockstore::Labels;
use crabka_metrics::NativeHistogram;
#[cfg(feature = "experimental-functions")]
use num_traits::ToPrimitive as _;
#[cfg(test)]
use promql_parser::parser::token::{
    T_AVG, T_COUNT, T_GROUP, T_MAX, T_MIN, T_STDDEV, T_STDVAR, T_SUM,
};
use promql_parser::parser::{
    AggregateExpr, Expr, LabelModifier,
    token::{T_TOPK, TokenType},
};

use super::{
    annotations::{emit_warning, invalid_quantile_warning, is_valid_quantile},
    histogram::{add_compatible_native_histogram, scaled_native_histogram},
    labels::{aggregate_labels, float_sample_value, labels_key},
    range_functions::kahan_sum_inc,
    result_utils::quantile_value,
};
use crate::{
    error::{PromqlError, Result},
    result::{InstantSample, SampleValue},
};

pub(super) fn aggregate_k(aggregate: &AggregateExpr) -> Result<usize> {
    let Some(param) = &aggregate.param else {
        return Err(PromqlError::Plan(format!(
            "{} requires a numeric parameter",
            aggregate.op
        )));
    };
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return Err(PromqlError::Plan(format!(
            "{} parameter must be numeric",
            aggregate.op
        )));
    };
    if !number.val.is_finite() || number.val < 0.0 || number.val.fract() != 0.0 {
        return Err(PromqlError::Plan(format!(
            "{} parameter must be a non-negative integer",
            aggregate.op
        )));
    }
    number
        .val
        .to_string()
        .parse::<usize>()
        .map_err(|_| PromqlError::Plan(format!("{} parameter is too large", aggregate.op)))
}

pub(super) fn aggregate_quantile(aggregate: &AggregateExpr) -> Result<f64> {
    let Some(param) = &aggregate.param else {
        return Err(PromqlError::Plan(
            "quantile requires a numeric parameter".to_string(),
        ));
    };
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return Err(PromqlError::Plan(
            "quantile parameter must be numeric".to_string(),
        ));
    };
    // An out-of-range / NaN phi is NOT an error here: Prometheus returns signed
    // `+/-Inf` / `NaN` plus an `InvalidQuantileWarning` (emitted by
    // `apply_quantile_aggregate`), exactly like the `histogram_quantile` family.
    Ok(number.val)
}

fn count_values_label_value(value: &SampleValue) -> Result<String> {
    match value {
        // Render the float with the crate's canonical Prometheus formatter so
        // non-finite values match the wire form (`+Inf`/`-Inf`/`NaN`) rather than
        // `f64::to_string`'s `inf`/`-inf`/`NaN`.
        SampleValue::Float(value) => Ok(crate::http_api::format_sample_value(*value)),
        SampleValue::Histogram(histogram) => serde_json::to_string(histogram).map_err(|error| {
            PromqlError::Exec(format!(
                "failed to encode histogram sample for count_values: {error}"
            ))
        }),
    }
}

/// Shared simple-aggregation core over an already-evaluated instant vector.
///
/// The simple ops are `sum`, `avg`, `count`, `group`, `min`, `max`, `stddev`,
/// and `stdvar`.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_instant_aggregate`) and the operator path
/// (`PromqlEngine::plan_aggregate_with_grouping`), so the two are identical by
/// construction once their inputs match. It groups the samples by the
/// `by`/`without` label set, accumulates each group's [`AggregateState`], and
/// returns one reduced sample per surviving group.
///
/// [`AggregateState`] and [`AggregateOp`] hold all the native-histogram rules,
/// which match Prometheus exactly:
/// - `sum`/`avg` (`aggregates_histograms`): histogram samples are MERGED. `sum`
///   adds them, and `avg` scales the merged histogram by `1/count`. A group that
///   mixes a float and a histogram is marked invalid and DROPPED from the
///   output through the `invalid_mixed_sample_type` flag. This happens when a
///   float arrives after a histogram
///   ([`AggregateState::mark_invalid_mixed_sample_type`]) or when a histogram
///   arrives after a float ([`AggregateState::push_histogram`]).
/// - `count`/`group` (`counts_histograms`): every sample is counted, whatever
///   its type. Histograms go through [`AggregateState::push_observation`].
/// - `min`/`max`/`stddev`/`stdvar` (`ignores_histograms`): histogram samples are
///   dropped with no annotation, exactly as the interpreter ignores them. This
///   matches Prometheus.
///
/// This function returns `Err` only for the unreachable case of a histogram
/// sample under an op that does not aggregate, count, or ignore histograms.
/// Every [`AggregateOp`] is in one of those three groups, so this branch mirrors
/// the interpreter's identical defensive branch.
pub(super) fn apply_simple_aggregate(
    samples: Vec<InstantSample>,
    op: AggregateOp,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<String, AggregateState> = BTreeMap::new();
    for sample in samples {
        let labels = aggregate_labels(&sample.labels, modifier);
        let state = groups
            .entry(labels_key(&labels))
            .or_insert_with(|| AggregateState::new(labels));
        match sample.value {
            SampleValue::Float(value) => {
                if op.aggregates_histograms() && state.has_histogram() {
                    state.mark_invalid_mixed_sample_type();
                    continue;
                }
                state.push_float(value);
            }
            SampleValue::Histogram(histogram) if op.aggregates_histograms() => {
                state.push_histogram(histogram)?;
            }
            SampleValue::Histogram(_) if op.counts_histograms() => state.push_observation(),
            SampleValue::Histogram(_) if op.ignores_histograms() => {}
            SampleValue::Histogram(_) => {
                return Err(PromqlError::Plan(
                    "native histogram reached an invalid aggregate classification".to_string(),
                ));
            }
        }
    }

    Ok(groups
        .into_values()
        .filter_map(|state| {
            op.finish(&state).map(|value| InstantSample {
                labels: state.labels,
                ts_ms: time_ms,
                value,
            })
        })
        .collect())
}

/// Shared `topk`/`bottomk` core over an already-evaluated instant vector.
///
/// This function backs both the interpreter (`PromqlEngine::eval_k_aggregate`)
/// and the operator path (`PromqlEngine::plan_param_aggregate_expr`), so the two
/// are identical by construction once their inputs match. It groups the samples
/// by the `by`/`without` label set and sorts each group by value: highest first
/// for `topk`, lowest first for `bottomk`, with a `labels_key` tie-break. It
/// then clamps each group to `k` and returns the surviving original samples.
/// The labels, including `__name__`, the timestamp, and the value all stay
/// unchanged, because this is a selection and not a reduction. This function
/// skips histogram-typed samples, which carry no float to rank. A `k` of 0
/// returns the empty vector.
pub(super) fn apply_k_aggregate(
    samples: Vec<InstantSample>,
    op: TokenType,
    k: usize,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantSample> {
    if k == 0 {
        return Vec::new();
    }

    let mut groups = BTreeMap::<String, Vec<InstantSample>>::new();
    for sample in samples {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            continue;
        }
        let labels = aggregate_labels(&sample.labels, modifier);
        groups.entry(labels_key(&labels)).or_default().push(sample);
    }

    let mut out = Vec::new();
    for mut group in groups.into_values() {
        group.sort_by(|left, right| compare_k_aggregate_samples(op, left, right));
        group.truncate(k.min(group.len()));
        out.extend(group);
    }
    out
}

/// Shared experimental `limitk(k, v)` core over an already-evaluated instant
/// vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_limitk_aggregate`) and the operator path. It groups by
/// the `by`/`without` label set and keeps the first `k` members of each group in
/// a deterministic order: fingerprint first, then `labels_key`. This is exactly
/// what Prometheus' reproducible `limitk` does. The caller resolves `k` before
/// reaching here, and short-circuits `k==0` to the empty vector.
#[cfg(feature = "experimental-functions")]
pub(super) fn apply_limitk_aggregate(
    samples: Vec<InstantSample>,
    k: usize,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantSample> {
    let mut groups = BTreeMap::<String, Vec<InstantSample>>::new();
    for sample in samples {
        let labels = aggregate_labels(&sample.labels, modifier);
        groups.entry(labels_key(&labels)).or_default().push(sample);
    }

    let mut out = Vec::new();
    for mut samples in groups.into_values() {
        samples.sort_by(|left, right| {
            left.labels
                .fingerprint()
                .cmp(&right.labels.fingerprint())
                .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
        });
        samples.truncate(k.min(samples.len()));
        out.extend(samples);
    }
    out
}

#[cfg(feature = "experimental-functions")]
fn limit_ratio_includes_sample(ratio: f64, labels: &Labels) -> bool {
    let sample_offset = prometheus_labels_hash(labels).to_f64().unwrap_or(f64::MAX)
        / u64::MAX.to_f64().unwrap_or(f64::MAX);
    if ratio == 0.0 {
        false
    } else if ratio.is_sign_positive() {
        sample_offset < ratio
    } else {
        sample_offset >= 1.0 + ratio
    }
}

/// Hashes labels exactly like Prometheus' `labels.Labels.Hash`.
///
/// Crabka's persisted series fingerprint deliberately uses a different,
/// length-prefixed encoding. `PromQL`'s `limit_ratio`, however, is externally
/// observable and must use Prometheus' xxHash64 over sorted
/// `name\xffvalue\xff` pairs.
#[cfg(feature = "experimental-functions")]
fn prometheus_labels_hash(labels: &Labels) -> u64 {
    let capacity = labels
        .iter()
        .map(|(name, value)| name.len() + value.len() + 2)
        .sum();
    let mut bytes = Vec::with_capacity(capacity);
    for (name, value) in labels.iter() {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);
    }
    xxhash_rust::xxh64::xxh64(&bytes, 0)
}

/// Shared experimental `limit_ratio(ratio, v)` core over an already-evaluated
/// instant vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_limit_ratio_aggregate`) and the operator path. It keeps
/// each sample whose label-set hash falls in the ratio's deterministic selection
/// band, as [`limit_ratio_includes_sample`] defines. The caller resolves and
/// caps the ratio before reaching here, and raises the `InvalidRatioWarning`
/// when the ratio was out of range. The caller also short-circuits `ratio==0` to
/// the empty vector.
#[cfg(feature = "experimental-functions")]
pub(super) fn apply_limit_ratio_aggregate(
    samples: Vec<InstantSample>,
    ratio: f64,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter(|sample| limit_ratio_includes_sample(ratio, &sample.labels))
        .collect()
}

/// Orders two samples for `topk`/`bottomk` selection.
///
/// The first key is the float value. `topk` uses `right.total_cmp(left)` so that
/// the highest value sorts first, and `bottomk` uses the reverse. The tie-break
/// is `labels_key`. A non-float sample, which the caller already filters out, or
/// a NaN sorts through `total_cmp`. This matches Prometheus.
fn compare_k_aggregate_samples(
    op: TokenType,
    left: &InstantSample,
    right: &InstantSample,
) -> Ordering {
    let left_value = float_sample_value(left).unwrap_or(f64::NAN);
    let right_value = float_sample_value(right).unwrap_or(f64::NAN);
    let by_value = if op.id() == T_TOPK {
        right_value.total_cmp(&left_value)
    } else {
        left_value.total_cmp(&right_value)
    };
    by_value.then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
}

/// Shared `quantile(phi, v)` core over an already-evaluated instant vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_quantile_aggregate`) and the operator path. It groups
/// the float samples by the `by`/`without` label set and returns the
/// phi-quantile of each group's values. [`quantile_value`] does the linear
/// interpolation in rank space. An empty group returns no row. This function
/// skips histogram-typed samples.
///
/// A `phi` outside `[0, 1]`, or a NaN `phi`, is NOT an error. Each group returns
/// the signed `+/-Inf` or `NaN` value that [`quantile_value`] returns, and the
/// function raises one `InvalidQuantileWarning`. This matches Prometheus and the
/// `histogram_quantile` family.
pub(super) fn apply_quantile_aggregate(
    samples: Vec<InstantSample>,
    quantile: f64,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Vec<InstantSample> {
    if !is_valid_quantile(quantile) {
        emit_warning(invalid_quantile_warning(quantile));
    }
    let mut groups: BTreeMap<String, (Labels, Vec<f64>)> = BTreeMap::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            continue;
        };
        let labels = aggregate_labels(&sample.labels, modifier);
        groups
            .entry(labels_key(&labels))
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(value);
    }

    groups
        .into_values()
        .filter_map(|(labels, mut values)| {
            quantile_value(quantile, &mut values).map(|value| InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect()
}

/// Shared `count_values("label", v)` core over an already-evaluated instant
/// vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_count_values_aggregate`) and the operator path. It
/// groups by the `by`/`without` label set, extended with the named label, which
/// it sets to each sample's formatted value. Floats use `Display` and histograms
/// use JSON. The function returns one series per distinct value, and each series
/// carries the group's count. It returns `Err` only when it cannot encode a
/// histogram value.
pub(super) fn apply_count_values_aggregate(
    samples: Vec<InstantSample>,
    label_name: &str,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups = BTreeMap::<String, AggregateState>::new();
    for sample in samples {
        let mut labels = aggregate_labels(&sample.labels, modifier);
        labels.insert(label_name, count_values_label_value(&sample.value)?);
        groups
            .entry(labels_key(&labels))
            .or_insert_with(|| AggregateState::new(labels))
            .push_float(1.0);
    }

    Ok(groups
        .into_values()
        .map(|state| InstantSample {
            labels: state.labels,
            ts_ms: time_ms,
            value: SampleValue::Float(state.count_f64),
        })
        .collect())
}

/// Shared `stddev(v)` / `stdvar(v)` core over an already-evaluated float-only
/// instant vector.
///
/// This function backs both the interpreter and the operator path. The
/// interpreter reaches it through the general
/// `PromqlEngine::eval_instant_aggregate` loop, which builds the same
/// [`AggregateState`] and calls the same [`AggregateOp::finish`]. The function
/// groups the float samples by the `by`/`without` label set, accumulates each
/// group's running sum, sum of squares, and count, and returns the population
/// standard deviation (`Stddev`) or the variance (`Stdvar`) per group. `op` must
/// be [`AggregateOp::Stddev`] or [`AggregateOp::Stdvar`]. This function ignores
/// histogram samples exactly as the interpreter ignores them for these ops. The
/// operator path feeds only float-only inputs, so no histogram sample appears in
/// practice.
pub(super) fn apply_stddev_stdvar_aggregate(
    samples: Vec<InstantSample>,
    op: AggregateOp,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Vec<InstantSample> {
    debug_assert!(
        matches!(op, AggregateOp::Stddev | AggregateOp::Stdvar),
        "apply_stddev_stdvar_aggregate requires a stddev/stdvar op"
    );
    // `stddev`/`stdvar` are `ignores_histograms` ops, so the shared simple-
    // aggregate kernel skips histogram samples (its `op.ignores_histograms()`
    // no-op branch) exactly as this routine used to, and never hits the
    // unreachable error branch. Delegating keeps the interpreter and operator
    // param paths sharing one core.
    apply_simple_aggregate(samples, op, modifier, time_ms)
        .expect("stddev/stdvar ignore histograms, so the kernel is infallible here")
}

#[derive(Clone, Copy)]
pub(super) enum AggregateOp {
    Sum,
    Avg,
    Count,
    Group,
    Min,
    Max,
    Stddev,
    Stdvar,
}

impl AggregateOp {
    #[cfg(test)]
    pub(super) fn try_from_token(token: TokenType) -> Result<Self> {
        match token.id() {
            T_SUM => Ok(Self::Sum),
            T_AVG => Ok(Self::Avg),
            T_COUNT => Ok(Self::Count),
            T_GROUP => Ok(Self::Group),
            T_MIN => Ok(Self::Min),
            T_MAX => Ok(Self::Max),
            T_STDDEV => Ok(Self::Stddev),
            T_STDVAR => Ok(Self::Stdvar),
            _ => Err(PromqlError::Unsupported(format!(
                "unsupported simple aggregation `{token}`"
            ))),
        }
    }

    fn finish(self, state: &AggregateState) -> Option<SampleValue> {
        if state.count == 0 || state.invalid_mixed_sample_type {
            return None;
        }
        Some(match self {
            Self::Sum => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(histogram.clone()),
                None => SampleValue::Float(state.sum),
            },
            Self::Avg => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(scaled_native_histogram(
                    histogram,
                    1.0 / state.count_f64,
                )),
                None => SampleValue::Float(state.avg_mean + state.avg_comp),
            },
            Self::Count => SampleValue::Float(state.count_f64),
            Self::Group => SampleValue::Float(1.0),
            Self::Min => SampleValue::Float(state.min),
            Self::Max => SampleValue::Float(state.max),
            Self::Stddev => SampleValue::Float(state.population_variance().sqrt()),
            Self::Stdvar => SampleValue::Float(state.population_variance()),
        })
    }

    fn ignores_histograms(self) -> bool {
        matches!(self, Self::Min | Self::Max | Self::Stddev | Self::Stdvar)
    }

    fn counts_histograms(self) -> bool {
        matches!(self, Self::Count | Self::Group)
    }

    fn aggregates_histograms(self) -> bool {
        matches!(self, Self::Sum | Self::Avg)
    }
}

struct AggregateState {
    labels: Labels,
    count: usize,
    count_f64: f64,
    sum: f64,
    /// Incremental Kahan-compensated mean for `avg`, which is
    /// `avg_mean + avg_comp`. This matches Prometheus. The naive `sum / count`
    /// overflows to +/-Inf for groups with a very large magnitude. The
    /// incremental form stays finite, and once it does saturate it keeps the
    /// same-sign-infinity handling.
    avg_mean: f64,
    avg_comp: f64,
    /// Welford running mean and `M2` accumulators for `stddev`/`stdvar`, each
    /// Kahan-compensated. The naive `E[x^2] - E[x]^2` form has catastrophic
    /// cancellation for groups of large, close values, and it then gives a
    /// negative variance whose `sqrt` is NaN. Welford stays stable and matches
    /// Prometheus.
    var_mean: f64,
    var_mean_comp: f64,
    var_aux: f64,
    var_aux_comp: f64,
    /// Running `min`/`max` over the group's float samples. Prometheus' `min` and
    /// `max` ignore NaN: a group's extremum comes from its non-NaN values, and
    /// the result is NaN only when every sample is NaN. This code mirrors
    /// Prometheus' aggregation loop in `promql/engine.go` exactly. The first
    /// sample seeds the running value, NaN included, and each later sample `f`
    /// replaces the running value when `running {>,<} f` or when `running` is
    /// NaN. So a later non-NaN value always displaces an earlier NaN, and an
    /// all-NaN group keeps NaN. `seen_float` tracks whether the code has taken
    /// the seed.
    seen_float: bool,
    min: f64,
    max: f64,
    histogram: Option<NativeHistogram>,
    invalid_mixed_sample_type: bool,
}

impl AggregateState {
    fn new(labels: Labels) -> Self {
        Self {
            labels,
            count: 0,
            count_f64: 0.0,
            sum: 0.0,
            avg_mean: 0.0,
            avg_comp: 0.0,
            var_mean: 0.0,
            var_mean_comp: 0.0,
            var_aux: 0.0,
            var_aux_comp: 0.0,
            seen_float: false,
            min: f64::NAN,
            max: f64::NAN,
            histogram: None,
            invalid_mixed_sample_type: false,
        }
    }

    fn push_float(&mut self, value: f64) {
        self.push_observation();
        self.sum += value;

        // Incremental Kahan-compensated mean for `avg` (Prometheus' `avg_over`-
        // style fold), keeping the running mean finite past naive-sum overflow.
        // Once the mean is infinite, a same-sign infinity or any finite sample
        // leaves it unchanged (only a flip to the opposite infinity / a NaN moves
        // it), exactly as Prometheus' `avg` aggregation does.
        let keep_infinite_mean = self.avg_mean.is_infinite()
            && ((value.is_infinite() && (value > 0.0) == (self.avg_mean > 0.0))
                || (!value.is_infinite() && !value.is_nan()));
        if !keep_infinite_mean {
            let (mean, comp) = kahan_sum_inc(
                value / self.count_f64 - self.avg_mean / self.count_f64,
                self.avg_mean,
                self.avg_comp,
            );
            self.avg_mean = mean;
            self.avg_comp = comp;
        }

        // Welford + Kahan variance accumulation for `stddev`/`stdvar`.
        let delta = value - (self.var_mean + self.var_mean_comp);
        let (var_mean, var_mean_comp) =
            kahan_sum_inc(delta / self.count_f64, self.var_mean, self.var_mean_comp);
        self.var_mean = var_mean;
        self.var_mean_comp = var_mean_comp;
        let (var_aux, var_aux_comp) = kahan_sum_inc(
            delta * (value - (self.var_mean + self.var_mean_comp)),
            self.var_aux,
            self.var_aux_comp,
        );
        self.var_aux = var_aux;
        self.var_aux_comp = var_aux_comp;

        if self.seen_float {
            // Replace the running extremum when the new sample wins under the
            // float ordering, or when the running value is NaN (so a non-NaN
            // sample displaces a NaN seed). `NaN > _` / `NaN < _` are false, so
            // a NaN sample never displaces an existing non-NaN extremum.
            if self.min > value || self.min.is_nan() {
                self.min = value;
            }
            if self.max < value || self.max.is_nan() {
                self.max = value;
            }
        } else {
            // First sample seeds both extrema (NaN included).
            self.seen_float = true;
            self.min = value;
            self.max = value;
        }
    }

    fn push_observation(&mut self) {
        self.count += 1;
        self.count_f64 += 1.0;
    }

    fn push_histogram(&mut self, histogram: NativeHistogram) -> Result<()> {
        if self.invalid_mixed_sample_type {
            return Ok(());
        }
        if self.count != 0 && self.histogram.is_none() {
            self.mark_invalid_mixed_sample_type();
            return Ok(());
        }
        self.push_observation();
        match &mut self.histogram {
            Some(existing) => add_compatible_native_histogram(existing, &histogram)?,
            None => self.histogram = Some(histogram),
        }
        Ok(())
    }

    fn mark_invalid_mixed_sample_type(&mut self) {
        self.invalid_mixed_sample_type = true;
        self.histogram = None;
    }

    fn has_histogram(&self) -> bool {
        self.histogram.is_some()
    }

    fn population_variance(&self) -> f64 {
        // Welford `M2 / n` (the running `var_aux` already accumulates the sum of
        // squared deviations from the running mean), Kahan-corrected.
        (self.var_aux + self.var_aux_comp) / self.count_f64
    }
}

#[cfg(all(test, feature = "experimental-functions"))]
mod tests {
    use super::*;

    #[test]
    fn limit_ratio_uses_a_strict_positive_hash_threshold() {
        let mut labels = Labels::new();
        labels.insert("__name__", "requests_total");
        labels.insert("instance", "api-1");
        let offset = prometheus_labels_hash(&labels).to_f64().unwrap() / u64::MAX.to_f64().unwrap();

        assert!(!limit_ratio_includes_sample(offset, &labels));
        assert!(limit_ratio_includes_sample(offset.next_up(), &labels));
        assert!(!limit_ratio_includes_sample(0.0, &labels));
        assert!(!limit_ratio_includes_sample(-0.0, &labels));
    }
}

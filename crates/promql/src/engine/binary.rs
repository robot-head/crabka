use std::collections::{BTreeMap, BTreeSet};

use crabka_blockstore::Labels;
use crabka_metrics::{NativeHistogram, ResetHint};
use promql_parser::parser::{
    BinModifier, BinaryExpr, LabelModifier, VectorMatchCardinality,
    token::{
        T_ADD, T_ATAN2, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE, T_LUNLESS, T_MOD,
        T_MUL, T_NEQ, T_POW, T_SUB, TokenType,
    },
};

use super::{
    annotations::{emit_info, incompatible_types_in_binop_info},
    histogram::{
        add_compatible_native_histogram, scale_native_histogram_values, scaled_native_histogram,
    },
    labels::{
        float_sample_value, is_result_metadata_label, labels_key, labels_without_metric_name,
    },
};
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, QueryResult, SampleValue},
};

pub(super) enum InstantValue {
    Scalar(f64),
    Vector(Vec<InstantSample>),
}

#[cfg(test)]
impl InstantValue {
    pub(super) fn try_from_query(result: QueryResult) -> Result<Self> {
        match result {
            QueryResult::Scalar { value, .. } => Ok(Self::Scalar(value)),
            QueryResult::InstantVector(samples) => Ok(Self::Vector(samples)),
            QueryResult::RangeMatrix(_) => Err(PromqlError::Plan(
                "binary expression requires instant operands".to_string(),
            )),
            QueryResult::Str { .. } => Err(PromqlError::Plan(
                "binary expression does not support string operands".to_string(),
            )),
        }
    }
}

#[derive(Clone, Copy)]
enum ScalarSide {
    Left,
    Right,
}

#[derive(Clone, Copy)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

impl BinaryOp {
    fn try_from_token(token: TokenType) -> Result<Self> {
        match token.id() {
            T_ADD => Ok(Self::Add),
            T_SUB => Ok(Self::Sub),
            T_MUL => Ok(Self::Mul),
            T_DIV => Ok(Self::Div),
            T_MOD => Ok(Self::Mod),
            T_POW => Ok(Self::Pow),
            T_ATAN2 => Ok(Self::Atan2),
            T_EQLC => Ok(Self::Eq),
            T_NEQ => Ok(Self::Neq),
            T_GTR => Ok(Self::Gt),
            T_LSS => Ok(Self::Lt),
            T_GTE => Ok(Self::Gte),
            T_LTE => Ok(Self::Lte),
            T_LAND | T_LOR | T_LUNLESS => Err(PromqlError::Unsupported(format!(
                "set operator `{token}` is not implemented yet"
            ))),
            _ => Err(PromqlError::Unsupported(format!(
                "binary operator `{token}` is not implemented yet"
            ))),
        }
    }

    fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Gte | Self::Lte
        )
    }

    /// Returns the `PromQL` surface symbol for this operator.
    ///
    /// The symbol matches the Prometheus annotation text, for example `==`,
    /// `!=`, `>`, and `>=`.
    fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "^",
            Self::Atan2 => "atan2",
            Self::Eq => "==",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::Lt => "<",
            Self::Gte => ">=",
            Self::Lte => "<=",
        }
    }

    fn apply_scalar(self, left: f64, right: f64, modifier: Option<&BinModifier>) -> Option<f64> {
        if self.is_comparison() {
            let pass = self.compare(left, right);
            if binary_returns_bool(modifier) {
                Some(if pass { 1.0 } else { 0.0 })
            } else if pass {
                Some(left)
            } else {
                None
            }
        } else {
            Some(self.arithmetic(left, right))
        }
    }

    fn apply_vector_scalar(
        self,
        sample: InstantSample,
        scalar: f64,
        modifier: Option<&BinModifier>,
        scalar_side: ScalarSide,
    ) -> Option<InstantSample> {
        if let SampleValue::Histogram(histogram) = sample.value {
            return self.apply_histogram_scalar(
                &sample.labels,
                sample.ts_ms,
                &histogram,
                scalar,
                scalar_side,
            );
        }

        let sample_value = float_sample_value(&sample).ok()?;
        let (left, right) = match scalar_side {
            ScalarSide::Left => (scalar, sample_value),
            ScalarSide::Right => (sample_value, scalar),
        };
        let value = if self.is_comparison() && !binary_returns_bool(modifier) {
            self.compare(left, right).then_some(sample_value)?
        } else {
            self.apply_scalar(left, right, modifier)?
        };
        let labels = if self.is_comparison() && !binary_returns_bool(modifier) {
            sample.labels
        } else {
            labels_without_metric_name(&sample.labels)
        };
        Some(InstantSample {
            labels,
            ts_ms: sample.ts_ms,
            value: SampleValue::Float(value),
        })
    }

    fn apply_histogram_scalar(
        self,
        labels: &Labels,
        ts_ms: i64,
        histogram: &NativeHistogram,
        scalar: f64,
        scalar_side: ScalarSide,
    ) -> Option<InstantSample> {
        let factor = match (self, scalar_side) {
            (Self::Mul, ScalarSide::Left | ScalarSide::Right) => scalar,
            (Self::Div, ScalarSide::Right) => 1.0 / scalar,
            _ => {
                if self.is_comparison() {
                    // Prometheus ignores the histogram operand in a comparison
                    // against a float, dropping the sample and raising an info.
                    let (lhs, rhs) = match scalar_side {
                        ScalarSide::Left => ("float", "histogram"),
                        ScalarSide::Right => ("histogram", "float"),
                    };
                    emit_info(incompatible_types_in_binop_info(lhs, self.symbol(), rhs));
                }
                return None;
            }
        };
        Some(InstantSample {
            labels: labels_without_metric_name(labels),
            ts_ms,
            value: SampleValue::Histogram(scaled_native_histogram(histogram, factor)),
        })
    }

    fn arithmetic(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Add => left + right,
            Self::Sub => left - right,
            Self::Mul => left * right,
            Self::Div => left / right,
            Self::Mod => left % right,
            Self::Pow => left.powf(right),
            Self::Atan2 => left.atan2(right),
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Gte | Self::Lte => {
                unreachable!("comparison op used as arithmetic")
            }
        }
    }

    fn compare(self, left: f64, right: f64) -> bool {
        match self {
            Self::Eq => left
                .partial_cmp(&right)
                .is_some_and(std::cmp::Ordering::is_eq),
            Self::Neq => !left
                .partial_cmp(&right)
                .is_some_and(std::cmp::Ordering::is_eq),
            Self::Gt => left > right,
            Self::Lt => left < right,
            Self::Gte => left >= right,
            Self::Lte => left <= right,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod | Self::Pow | Self::Atan2 => {
                unreachable!("arithmetic op used as comparison")
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SetOp {
    And,
    Or,
    Unless,
}

impl SetOp {
    fn from_token(token: TokenType) -> Option<Self> {
        match token.id() {
            T_LAND => Some(Self::And),
            T_LOR => Some(Self::Or),
            T_LUNLESS => Some(Self::Unless),
            _ => None,
        }
    }
}

fn validate_binary_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if matches!(modifier.card, VectorMatchCardinality::ManyToMany) {
        return Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        ));
    }
    Ok(())
}

fn validate_set_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
        return Err(PromqlError::Unsupported(
            "binary fill modifiers are not implemented yet".to_string(),
        ));
    }
    Ok(())
}

/// Combines two already-evaluated instant operands into the binary result.
///
/// This function applies the operator and the modifier of `binary`. It is the
/// shared core of `PromQL` binary evaluation. The interpreter function
/// `PromqlEngine::eval_instant_binary` evaluates both operands through the
/// interpreter and then calls this function. The operator path
/// `PromqlEngine::plan_binary_expr` recurses both operands through the planner,
/// assembles each one to an [`InstantValue`], and then calls this same function.
///
/// Both callers send their operands through one combine routine, so the two
/// paths are byte-for-byte identical once their operand vectors match. This
/// function decides the set operations, the vector matching, the `__name__`
/// dropping, the `bool` modifier, and the `group_left` and `group_right`
/// copying. The call site decides none of them.
pub(super) fn combine_instant_binary(
    binary: &BinaryExpr,
    lhs: InstantValue,
    rhs: InstantValue,
    time_ms: i64,
) -> Result<QueryResult> {
    let modifier = binary.modifier.as_ref();

    if let Some(op) = SetOp::from_token(binary.op) {
        validate_set_modifier(modifier)?;
        let (InstantValue::Vector(left), InstantValue::Vector(right)) = (lhs, rhs) else {
            return Err(PromqlError::Plan(format!(
                "set operator `{}` requires instant-vector operands",
                binary.op
            )));
        };
        return Ok(QueryResult::InstantVector(eval_vector_set_binary(
            left, right, op, modifier,
        )));
    }

    validate_binary_modifier(modifier)?;
    let op = BinaryOp::try_from_token(binary.op)?;
    match (lhs, rhs) {
        (InstantValue::Scalar(left), InstantValue::Scalar(right)) => {
            let Some(value) = op.apply_scalar(left, right, modifier) else {
                return Err(PromqlError::Plan(
                    "scalar comparison without bool cannot filter a scalar".to_string(),
                ));
            };
            Ok(QueryResult::Scalar {
                ts_ms: time_ms,
                value,
            })
        }
        (InstantValue::Vector(vector), InstantValue::Scalar(scalar)) => {
            let samples = vector
                .into_iter()
                .filter_map(|sample| {
                    op.apply_vector_scalar(sample, scalar, modifier, ScalarSide::Right)
                })
                .collect();
            Ok(QueryResult::InstantVector(samples))
        }
        (InstantValue::Scalar(scalar), InstantValue::Vector(vector)) => {
            let samples = vector
                .into_iter()
                .filter_map(|sample| {
                    op.apply_vector_scalar(sample, scalar, modifier, ScalarSide::Left)
                })
                .collect();
            Ok(QueryResult::InstantVector(samples))
        }
        (InstantValue::Vector(left), InstantValue::Vector(right)) => {
            eval_vector_vector_binary(left, right, op, modifier).map(QueryResult::InstantVector)
        }
    }
}

fn eval_vector_set_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: SetOp,
    modifier: Option<&BinModifier>,
) -> Vec<InstantSample> {
    let mut left_keys = BTreeSet::new();
    let mut right_keys = BTreeSet::new();
    for sample in &left {
        left_keys.insert(binary_match_key(&sample.labels, modifier));
    }
    for sample in &right {
        right_keys.insert(binary_match_key(&sample.labels, modifier));
    }

    let mut out = Vec::new();
    match op {
        SetOp::And => {
            for sample in left {
                if right_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
        SetOp::Unless => {
            for sample in left {
                if !right_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
        SetOp::Or => {
            out.extend(left);
            for sample in right {
                if !left_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
    }
    out
}

fn eval_vector_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Vec<InstantSample>> {
    let card = modifier.map_or(VectorMatchCardinality::OneToOne, |modifier| {
        modifier.card.clone()
    });
    match card {
        VectorMatchCardinality::OneToOne => {
            eval_one_to_one_vector_binary(left, right, op, modifier)
        }
        VectorMatchCardinality::ManyToOne(group_labels) => {
            eval_many_to_one_vector_binary(left, right, op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::OneToMany(group_labels) => {
            eval_one_to_many_vector_binary(left, right, op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::ManyToMany => Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        )),
    }
}

fn eval_one_to_one_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Vec<InstantSample>> {
    let mut right_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in right {
        let key = binary_match_key(&sample.labels, modifier);
        if right_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "many-to-one matching for key `{key}` is not supported"
            )));
        }
    }

    let mut out = Vec::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.remove(&key) else {
            let Some(rhs_fill) = modifier.and_then(|modifier| modifier.fill_values.rhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&left_sample, rhs_fill, op, modifier, MissingSide::Right)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                left_sample.labels
            } else {
                one_to_one_binary_result_labels(&left_sample.labels, modifier)
            };
            out.push(InstantSample {
                labels,
                ts_ms: left_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(&left_sample, &right_sample, op, modifier)?
        else {
            continue;
        };
        let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            left_sample.labels
        } else {
            one_to_one_binary_result_labels(&left_sample.labels, modifier)
        };
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
        });
    }
    if let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) {
        for right_sample in right_by_key.into_values() {
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                right_sample.labels
            } else {
                one_to_one_binary_result_labels(&right_sample.labels, modifier)
            };
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
            });
        }
    }
    Ok(out)
}

fn eval_many_to_one_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let mut right_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in right {
        let key = binary_match_key(&sample.labels, modifier);
        if right_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "many-to-one matching requires the right side to be unique for key `{key}`"
            )));
        }
    }

    let mut out = Vec::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.get(&key) else {
            let Some(rhs_fill) = modifier.and_then(|modifier| modifier.fill_values.rhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&left_sample, rhs_fill, op, modifier, MissingSide::Right)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                left_sample.labels
            } else {
                labels_without_metric_name(&left_sample.labels)
            };
            out.push(InstantSample {
                labels,
                ts_ms: left_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(&left_sample, right_sample, op, modifier)?
        else {
            continue;
        };
        let mut labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            left_sample.labels.clone()
        } else {
            labels_without_metric_name(&left_sample.labels)
        };
        copy_group_labels(&mut labels, &right_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
        });
    }
    Ok(out)
}

fn eval_one_to_many_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let mut left_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in left {
        let key = binary_match_key(&sample.labels, modifier);
        if left_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "one-to-many matching requires the left side to be unique for key `{key}`"
            )));
        }
    }

    let mut out = Vec::new();
    for right_sample in right {
        let key = binary_match_key(&right_sample.labels, modifier);
        let Some(left_sample) = left_by_key.get(&key) else {
            let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                right_sample.labels
            } else {
                labels_without_metric_name(&right_sample.labels)
            };
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(left_sample, &right_sample, op, modifier)?
        else {
            continue;
        };
        let mut labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            right_sample.labels.clone()
        } else {
            labels_without_metric_name(&right_sample.labels)
        };
        copy_group_labels(&mut labels, &left_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: right_sample.ts_ms,
            value,
        });
    }
    Ok(out)
}

fn copy_group_labels(labels: &mut Labels, one_side: &Labels, group_labels: &[String]) {
    for name in group_labels {
        if is_result_metadata_label(name) {
            continue;
        }
        if let Some(value) = one_side.get(name) {
            labels.insert(name, value);
        }
    }
}

fn one_to_one_binary_result_labels(input: &Labels, modifier: Option<&BinModifier>) -> Labels {
    match modifier.and_then(|modifier| modifier.matching.as_ref()) {
        Some(LabelModifier::Include(include)) => {
            let mut labels = Labels::new();
            for name in &include.labels {
                if is_result_metadata_label(name) {
                    continue;
                }
                if let Some(value) = input.get(name) {
                    labels.insert(name, value);
                }
            }
            labels
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            let mut labels = Labels::new();
            for (name, value) in input.iter() {
                if is_result_metadata_label(name) || excluded.contains(name) {
                    continue;
                }
                labels.insert(name, value);
            }
            labels
        }
        None => labels_without_metric_name(input),
    }
}

fn binary_returns_bool(modifier: Option<&BinModifier>) -> bool {
    modifier.is_some_and(|modifier| modifier.return_bool)
}

#[derive(Clone, Copy)]
enum MissingSide {
    Left,
    Right,
}

fn apply_binary_fill_value(
    present: &InstantSample,
    fill_value: f64,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    missing_side: MissingSide,
) -> Result<Option<SampleValue>> {
    let filled = InstantSample {
        labels: Labels::new(),
        ts_ms: present.ts_ms,
        value: SampleValue::Float(fill_value),
    };
    match missing_side {
        MissingSide::Left => apply_binary_sample_value(&filled, present, op, modifier),
        MissingSide::Right => apply_binary_sample_value(present, &filled, op, modifier),
    }
}

fn apply_binary_sample_value(
    left: &InstantSample,
    right: &InstantSample,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    match (&left.value, &right.value) {
        (SampleValue::Float(left), SampleValue::Float(right)) => Ok(op
            .apply_scalar(*left, *right, modifier)
            .map(SampleValue::Float)),
        (SampleValue::Histogram(left), SampleValue::Histogram(right)) => {
            apply_histogram_histogram_binary(left, right, op, modifier)
        }
        (SampleValue::Float(left), SampleValue::Histogram(right)) => {
            if op.is_comparison() {
                emit_info(incompatible_types_in_binop_info(
                    "float",
                    op.symbol(),
                    "histogram",
                ));
                return Ok(None);
            }
            Ok(apply_histogram_float_binary(
                right,
                *left,
                op,
                ScalarSide::Left,
            ))
        }
        (SampleValue::Histogram(left), SampleValue::Float(right)) => {
            if op.is_comparison() {
                emit_info(incompatible_types_in_binop_info(
                    "histogram",
                    op.symbol(),
                    "float",
                ));
                return Ok(None);
            }
            Ok(apply_histogram_float_binary(
                left,
                *right,
                op,
                ScalarSide::Right,
            ))
        }
    }
}

fn apply_histogram_float_binary(
    histogram: &NativeHistogram,
    scalar: f64,
    op: BinaryOp,
    scalar_side: ScalarSide,
) -> Option<SampleValue> {
    let factor = match (op, scalar_side) {
        (BinaryOp::Mul, ScalarSide::Left | ScalarSide::Right) => scalar,
        (BinaryOp::Div, ScalarSide::Right) => 1.0 / scalar,
        _ => return None,
    };
    Some(SampleValue::Histogram(scaled_native_histogram(
        histogram, factor,
    )))
}

fn apply_histogram_histogram_binary(
    left: &NativeHistogram,
    right: &NativeHistogram,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    let mut out = left.clone();
    match op {
        BinaryOp::Add => add_compatible_native_histogram(&mut out, right)?,
        BinaryOp::Sub => {
            let mut right = right.clone();
            scale_native_histogram_values(&mut right, -1.0);
            add_compatible_native_histogram(&mut out, &right)?;
            out.reset_hint = ResetHint::Gauge;
        }
        BinaryOp::Eq | BinaryOp::Neq => {
            let pass = match op {
                BinaryOp::Eq => left == right,
                BinaryOp::Neq => left != right,
                _ => unreachable!("non-comparison histogram op"),
            };
            return Ok(if binary_returns_bool(modifier) {
                Some(SampleValue::Float(if pass { 1.0 } else { 0.0 }))
            } else if pass {
                Some(SampleValue::Histogram(left.clone()))
            } else {
                None
            });
        }
        BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte => {
            // Ordered comparisons are undefined between two histograms:
            // Prometheus drops the pair and raises an info annotation.
            emit_info(incompatible_types_in_binop_info(
                "histogram",
                op.symbol(),
                "histogram",
            ));
            return Ok(None);
        }
        _ => return Ok(None),
    }
    Ok(Some(SampleValue::Histogram(out)))
}

fn binary_match_key(labels: &Labels, modifier: Option<&BinModifier>) -> String {
    let mut key_labels = Labels::new();
    match modifier.and_then(|modifier| modifier.matching.as_ref()) {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if let Some(value) = labels.get(name) {
                    key_labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for (name, value) in labels.iter() {
                if name == "__name__" || excluded.contains(name) {
                    continue;
                }
                key_labels.insert(name, value);
            }
        }
        None => {
            for (name, value) in labels.iter() {
                if is_result_metadata_label(name) {
                    continue;
                }
                key_labels.insert(name, value);
            }
        }
    }
    labels_key(&key_labels)
}

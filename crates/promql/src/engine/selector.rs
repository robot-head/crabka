use std::{collections::BTreeSet, time::SystemTime};

use crabka_blockstore::{LabelMatcher, Labels, MatchOp};
use num_traits::ToPrimitive;
use promql_parser::{
    label as prom_label,
    parser::{AtModifier, Offset, VectorSelector},
};
use regex::Regex;

use crate::{PromqlError, error::Result};

pub(crate) fn label_matcher_sets(selector: &VectorSelector) -> Vec<Vec<LabelMatcher>> {
    if selector.matchers.or_matchers.is_empty() {
        return vec![build_label_matchers(
            selector.name.as_deref(),
            &selector.matchers.matchers,
        )];
    }

    let mut out = Vec::new();
    for matchers in &selector.matchers.or_matchers {
        out.push(build_label_matchers(selector.name.as_deref(), matchers));
    }
    out
}

pub(super) fn info_data_label_matchers(selector: &VectorSelector) -> Result<Vec<LabelMatcher>> {
    let matcher_sets = label_matcher_sets(selector);
    let [matchers] = matcher_sets.as_slice() else {
        return Err(PromqlError::Plan(
            "info data label selector does not support or matchers".to_string(),
        ));
    };
    Ok(matchers.clone())
}

fn build_label_matchers(
    metric_name: Option<&str>,
    matchers: &[prom_label::Matcher],
) -> Vec<LabelMatcher> {
    let mut out = Vec::new();
    if let Some(name) = metric_name {
        out.push(LabelMatcher::new("__name__", MatchOp::Eq, name));
    }
    let mut seen = out
        .iter()
        .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
        .collect::<BTreeSet<_>>();
    for matcher in matchers {
        let op = match matcher.op {
            prom_label::MatchOp::Equal => MatchOp::Eq,
            prom_label::MatchOp::NotEqual => MatchOp::Neq,
            prom_label::MatchOp::Re(_) => MatchOp::Re,
            prom_label::MatchOp::NotRe(_) => MatchOp::Nre,
        };
        let next = LabelMatcher::new(&matcher.name, op, &matcher.value);
        if seen.insert((next.name.clone(), next.value.clone())) {
            out.push(next);
        }
    }
    out
}

/// One label matcher with its `=~`/`!~` regex compiled ahead of time, anchored
/// `^(?:...)$` exactly as `labels_match` anchors it.
struct CompiledLabelMatcher {
    name: String,
    op: MatchOp,
    /// The literal comparand for `Eq`/`Neq`; the precompiled, anchored regex's
    /// source is `value` too, but the compiled form lives in `regex`.
    value: String,
    regex: Option<Regex>,
}

/// A set of [`CompiledLabelMatcher`]s, so a hot loop can match many labelsets
/// without recompiling each `=~`/`!~` regex per call (the bug `labels_match` has
/// when invoked per sample).
pub(super) struct CompiledLabelMatchers {
    matchers: Vec<CompiledLabelMatcher>,
}

impl CompiledLabelMatchers {
    /// Whether `labels` satisfies every compiled matcher, the precompiled
    /// equivalent of `labels_match`.
    pub(super) fn matches(&self, labels: &Labels) -> bool {
        for matcher in &self.matchers {
            let value = labels.get(&matcher.name).unwrap_or("");
            let is_match = match matcher.op {
                MatchOp::Eq => value == matcher.value,
                MatchOp::Neq => value != matcher.value,
                MatchOp::Re | MatchOp::Nre => {
                    let regex_matches = matcher
                        .regex
                        .as_ref()
                        .is_some_and(|regex| regex.is_match(value));
                    if matcher.op == MatchOp::Re {
                        regex_matches
                    } else {
                        !regex_matches
                    }
                }
            };
            if !is_match {
                return false;
            }
        }
        true
    }
}

/// Compile a matcher set once, precompiling each `=~`/`!~` regex (anchored
/// `^(?:...)$`). Returns the same regex-compile error `labels_match` would.
pub(super) fn compile_label_matchers(matchers: &[LabelMatcher]) -> Result<CompiledLabelMatchers> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        let regex = match matcher.op {
            MatchOp::Re | MatchOp::Nre => Some(
                Regex::new(&format!("^(?:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?,
            ),
            MatchOp::Eq | MatchOp::Neq => None,
        };
        compiled.push(CompiledLabelMatcher {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
        });
    }
    Ok(CompiledLabelMatchers { matchers: compiled })
}

pub(super) fn labels_match(labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool> {
    for matcher in matchers {
        let value = labels.get(&matcher.name).unwrap_or("");
        let is_match = match matcher.op {
            MatchOp::Eq => value == matcher.value,
            MatchOp::Neq => value != matcher.value,
            MatchOp::Re | MatchOp::Nre => {
                let regex = Regex::new(&format!("^(?:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?;
                let regex_matches = regex.is_match(value);
                if matcher.op == MatchOp::Re {
                    regex_matches
                } else {
                    !regex_matches
                }
            }
        };
        if !is_match {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn apply_selector_time_modifier(
    time_ms: i64,
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let base_time_ms = selector_at_ms(time_ms, at, bounds)?;
    apply_offset_delta(base_time_ms, selector_offset_ms(offset)?)
}

#[derive(Clone, Copy)]
pub(super) struct AtModifierBounds {
    pub(super) start_ms: i64,
    pub(super) end_ms: i64,
}

fn selector_at_ms(
    time_ms: i64,
    at: Option<&AtModifier>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let Some(at) = at else {
        return Ok(time_ms);
    };
    match at {
        AtModifier::At(time) => system_time_ms(*time),
        AtModifier::Start => bounds.map(|bounds| bounds.start_ms).ok_or_else(|| {
            PromqlError::Unsupported(
                "@ start()/end() modifiers require range-query bounds".to_string(),
            )
        }),
        AtModifier::End => bounds.map(|bounds| bounds.end_ms).ok_or_else(|| {
            PromqlError::Unsupported(
                "@ start()/end() modifiers require range-query bounds".to_string(),
            )
        }),
    }
}

fn system_time_ms(time: SystemTime) -> Result<i64> {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration_to_i64_ms(duration),
        Err(error) => duration_to_i64_ms(error.duration()).and_then(|duration_ms| {
            duration_ms
                .checked_neg()
                .ok_or_else(|| PromqlError::Plan("@ modifier timestamp is too small".to_string()))
        }),
    }
}

fn duration_to_i64_ms(duration: std::time::Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PromqlError::Plan("@ modifier timestamp is too large".to_string()))
}

fn selector_offset_ms(offset: Option<&Offset>) -> Result<i64> {
    let Some(offset) = offset else {
        return Ok(0);
    };
    let (duration, sign) = match offset {
        Offset::Pos(duration) => (*duration, -1_i64),
        Offset::Neg(duration) => (*duration, 1_i64),
    };
    let duration_ms = duration_ms(duration)?;
    duration_ms
        .checked_mul(sign)
        .ok_or_else(|| PromqlError::Plan("offset duration is too large".to_string()))
}

fn apply_offset_delta(time_ms: i64, offset_ms: i64) -> Result<i64> {
    time_ms
        .checked_add(offset_ms)
        .ok_or_else(|| PromqlError::Plan("offset evaluation time overflow".to_string()))
}

pub(super) fn timestamp_seconds(timestamp_ms: i64) -> f64 {
    timestamp_ms.to_f64().unwrap_or(f64::MAX) / 1000.0
}

pub(super) fn duration_ms(duration: std::time::Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PromqlError::Plan("range selector duration is too large".to_string()))
}

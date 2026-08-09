use std::collections::{BTreeMap, BTreeSet};

use crabka_blockstore::{LabelMatcher, Labels, MatchOp};
use promql_parser::parser::{Expr, LabelModifier, VectorSelector};

use super::selector::label_matcher_sets;
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, SampleValue},
};

/// Records the `__name__` of the first sample for a histogram group key.
///
/// A later mixed-histogram warning names the metric with this value, as
/// Prometheus does.
pub(super) fn record_metric_name(names: &mut BTreeMap<String, String>, key: &str, labels: &Labels) {
    if let Some(name) = labels.get("__name__") {
        names
            .entry(key.to_string())
            .or_insert_with(|| name.to_string());
    }
}

pub(super) fn float_sample_value(sample: &InstantSample) -> Result<f64> {
    match sample.value {
        SampleValue::Float(value) => Ok(value),
        SampleValue::Histogram(_) => Err(PromqlError::Unsupported(
            "binary operations over histograms are not implemented yet".to_string(),
        )),
    }
}

pub(super) fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let mut labels = Labels::new();
    match modifier {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if name == "__name__" {
                    continue;
                }
                if let Some(value) = input.get(name) {
                    labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for (name, value) in input.iter() {
                if name == "__name__" || excluded.contains(name) {
                    continue;
                }
                labels.insert(name, value);
            }
        }
        None => {}
    }
    labels
}

pub(super) fn labels_without_metric_name(input: &Labels) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        if !is_result_metadata_label(name) {
            labels.insert(name, value);
        }
    }
    labels
}

pub(super) fn is_result_metadata_label(name: &str) -> bool {
    matches!(name, "__name__" | "__type__" | "__unit__")
}

pub(super) fn labels_without_metric_and_label(input: &Labels, drop_label: &str) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        if !is_result_metadata_label(name) && name != drop_label {
            labels.insert(name, value);
        }
    }
    labels
}

pub(super) fn absent_labels(expr: &Expr) -> Result<Labels> {
    match expr {
        Expr::VectorSelector(selector) => Ok(absent_labels_from_selector(selector)),
        Expr::MatrixSelector(selector) => Ok(absent_labels_from_selector(&selector.vs)),
        Expr::Paren(paren) => absent_labels(&paren.expr),
        _ => Ok(Labels::new()),
    }
}

fn absent_labels_from_selector(selector: &VectorSelector) -> Labels {
    let matcher_sets = label_matcher_sets(selector);
    if matcher_sets.len() == 1 {
        return absent_labels_from_matchers(&matcher_sets[0]);
    }
    Labels::new()
}

fn absent_labels_from_matchers(matchers: &[LabelMatcher]) -> Labels {
    let mut labels = Labels::new();
    for matcher in matchers {
        if matcher.name != "__name__" && matcher.op == MatchOp::Eq {
            labels.insert(&matcher.name, &matcher.value);
        }
    }
    labels
}

pub(super) fn labels_key(labels: &Labels) -> String {
    let mut key = String::new();
    for (name, value) in labels.iter() {
        key.push_str(name);
        key.push('=');
        key.push_str(value);
        key.push('\n');
    }
    key
}

//! Pure label-rewrite and ordering transforms for the `label_replace`,
//! `label_join`, `sort`, and `sort_desc` operator paths.
//!
//! Unlike the selector / rate / `*_over_time` / scalar-math paths, these four
//! functions do not lower to a `DataFusion` projection over a leaf table. They
//! operate on the *already-assembled* inner instant vector — one
//! [`InstantSample`] per matched series — and rewrite its label columns
//! (`label_replace`/`label_join`) or reorder its rows (`sort`/`sort_desc`).
//! Because the transform is a pure function over the assembled vector, the
//! engine recurses into the inner expression, assembles it (applying that
//! shape's own NaN/staleness semantics), and applies one of these transforms at
//! assembly time rather than re-emitting through the operator chain.
//!
//! The exact same functions back the interpreter's `eval_label_replace_call` /
//! `eval_label_join_call` / `eval_sort_call`, so the operator path matches the
//! interpreter by construction — including `$1`/`${name}` capture-group
//! expansion, empty-replacement label writes, no-match passthrough, the
//! `separator`-join semantics, and the `total_cmp`-with-`labels_key`-tiebreak
//! ordering (which places `NaN` last for `sort` and first for `sort_desc`).

use std::cmp::Ordering;

use crabka_blockstore::Labels;
use regex::Regex;

use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, SampleValue},
};

/// Sort order for the `sort` / `sort_desc` functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    /// Compare two sample values in this order using `total_cmp`, matching the
    /// interpreter's `SortDirection::compare`. `total_cmp` places a (positive)
    /// `NaN` above every finite value, so ascending sends `NaN` to the end and
    /// descending (the reverse) sends it to the front.
    fn compare(self, left: f64, right: f64) -> Ordering {
        match self {
            Self::Ascending => left.total_cmp(&right),
            Self::Descending => right.total_cmp(&left),
        }
    }
}

/// Canonical `name=value\n…` rendering of a label set, used as the sort
/// tiebreak and the collision key. Matches the interpreter's `labels_key`.
fn labels_key(labels: &Labels) -> String {
    let mut key = String::new();
    for (name, value) in labels.iter() {
        key.push_str(name);
        key.push('=');
        key.push_str(value);
        key.push('\n');
    }
    key
}

/// The float value of a sample, or `NaN` for a histogram sample (matching the
/// interpreter's `float_sample_value(...).unwrap_or(f64::NAN)` in the sort
/// comparator).
fn sort_value(sample: &InstantSample) -> f64 {
    match sample.value {
        SampleValue::Float(value) => value,
        SampleValue::Histogram(_) => f64::NAN,
    }
}

/// Apply `label_replace(v, dst_label, replacement, src_label, regex)` to an
/// already-assembled instant vector.
///
/// For each series, if `regex` — fully anchored as `^(?:<regex>)$`, matching
/// Prometheus — matches the *entire* value of `src_label`, the destination label
/// is set to `replacement` with `$1` /
/// `${name}` capture-group expansion. A non-matching series is passed through
/// unchanged. `__name__` is preserved unless `dst_label == "__name__"` (these
/// functions never drop the metric name themselves). Writing an empty expansion
/// stores `dst_label=""` (the interpreter's `Labels::insert` keeps empty-valued
/// labels), so the empty entry participates in later collision checks exactly as
/// the interpreter sees it.
///
/// # Errors
///
/// Returns [`PromqlError::Plan`] when `regex` is not a valid regular expression,
/// matching the interpreter's error text.
pub fn apply_label_replace(
    samples: Vec<InstantSample>,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex: &str,
) -> Result<Vec<InstantSample>> {
    // Prometheus FULLY anchors `label_replace`'s regex (`^(?:<regex>)$`), so it
    // must match the *entire* source-label value — `regexp.MatchString` on a
    // `^(?:...)$`-wrapped pattern, the same anchoring `crabka-blockstore`'s
    // `anchored_regex` applies to label matchers. A raw unanchored `Regex` would
    // wrongly match a substring (e.g. `foo` inside `xfooy`).
    let regex = Regex::new(&format!("^(?:{regex})$"))
        .map_err(|err| PromqlError::Plan(format!("invalid label_replace regex: {err}")))?;
    Ok(samples
        .into_iter()
        .map(|mut sample| {
            if let Some(captures) = regex.captures(sample.labels.get(src_label).unwrap_or("")) {
                let mut value = String::new();
                captures.expand(replacement, &mut value);
                sample.labels.insert(dst_label, value);
            }
            sample
        })
        .collect())
}

/// Apply `label_join(v, dst_label, separator, src_label_1, …)` to an
/// already-assembled instant vector: set `dst_label` to the `separator`-joined
/// values of the listed source labels (missing labels contribute the empty
/// string), for every series. Mirrors the interpreter's `eval_label_join_call`.
#[must_use]
pub fn apply_label_join(
    samples: Vec<InstantSample>,
    dst_label: &str,
    separator: &str,
    src_labels: &[String],
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .map(|mut sample| {
            let value = src_labels
                .iter()
                .map(|label| sample.labels.get(label).unwrap_or(""))
                .collect::<Vec<_>>()
                .join(separator);
            sample.labels.insert(dst_label, value);
            sample
        })
        .collect()
}

/// Sort an already-assembled instant vector by sample value in `order`, breaking
/// ties by canonical label key. Mirrors the interpreter's `eval_sort_call`.
#[must_use]
pub fn apply_sort(mut samples: Vec<InstantSample>, order: SortOrder) -> Vec<InstantSample> {
    samples.sort_by(|left, right| {
        order
            .compare(sort_value(left), sort_value(right))
            .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
    });
    samples
}

/// Compare two label sets by the listed `label_names` in `order`, returning the
/// first non-equal label-value comparison (missing labels compare as the empty
/// string), or [`Ordering::Equal`] when every listed label is equal. Mirrors the
/// interpreter's `SortDirection::compare_label_values`.
fn compare_label_values(
    left: &Labels,
    right: &Labels,
    label_names: &[String],
    order: SortOrder,
) -> Ordering {
    for label_name in label_names {
        let ordering = left
            .get(label_name.as_str())
            .unwrap_or("")
            .cmp(right.get(label_name.as_str()).unwrap_or(""));
        let ordering = match order {
            SortOrder::Ascending => ordering,
            SortOrder::Descending => ordering.reverse(),
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Sort an already-assembled instant vector by the values of the named labels in
/// `order`, breaking ties by canonical label key. Mirrors the interpreter's
/// `eval_sort_by_label_call`: the sort is over the listed labels first (in order),
/// then the full canonical label key (so a `_desc` sort still tiebreaks by the
/// *ascending* label key, exactly as the interpreter's `labels_key` tiebreak does).
#[must_use]
pub fn apply_sort_by_label(
    mut samples: Vec<InstantSample>,
    label_names: &[String],
    order: SortOrder,
) -> Vec<InstantSample> {
    samples.sort_by(|left, right| {
        compare_label_values(&left.labels, &right.labels, label_names, order)
            .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
    });
    samples
}

#[cfg(test)]
mod tests {

    use super::*;

    fn sample(pairs: &[(&str, &str)], value: f64) -> InstantSample {
        let mut labels = Labels::new();
        for (name, val) in pairs {
            labels.insert(*name, *val);
        }
        InstantSample {
            labels,
            ts_ms: 1_000,
            value: SampleValue::Float(value),
        }
    }

    #[test]
    fn label_replace_capture_group_expands() {
        let out = apply_label_replace(
            vec![sample(&[("__name__", "m"), ("src", "a-b")], 1.0)],
            "dst",
            "$1",
            "src",
            "(.*)-.*",
        )
        .unwrap();
        // `dst` gets the capture-group expansion; `__name__` is preserved
        // (label_replace does not drop it).
        assert2::assert!(
            out == vec![sample(
                &[("__name__", "m"), ("src", "a-b"), ("dst", "a")],
                1.0
            )]
        );
    }

    #[test]
    fn label_replace_no_match_passthrough() {
        let input = vec![sample(&[("__name__", "m"), ("src", "zzz")], 1.0)];
        let out = apply_label_replace(input.clone(), "dst", "$1", "src", "(\\d+)").unwrap();
        // No match: series unchanged, no `dst` label added.
        assert2::assert!(out == input);
    }

    #[test]
    fn label_replace_empty_replacement_writes_empty_label() {
        let out = apply_label_replace(
            vec![sample(&[("__name__", "m"), ("src", "abc")], 1.0)],
            "dst",
            "",
            "src",
            ".*",
        )
        .unwrap();
        // The interpreter's `Labels::insert` keeps an empty-valued label.
        assert2::assert!(out[0].labels.get("dst") == Some(""));
    }

    #[test]
    fn label_replace_anchors_regex_fully() {
        // Prometheus fully anchors the regex (`^(?:foo)$`), so a `foo` pattern
        // must NOT match the substring inside `xfooy`. A raw unanchored `Regex`
        // would wrongly match and write `dst`.
        let input = vec![sample(&[("__name__", "m"), ("src", "xfooy")], 1.0)];
        let out = apply_label_replace(input.clone(), "dst", "$0", "src", "foo").unwrap();
        assert2::assert!(out == input);
        assert2::assert!(out[0].labels.get("dst").is_none());

        // The same pattern matches when it spans the entire value.
        let out = apply_label_replace(
            vec![sample(&[("__name__", "m"), ("src", "foo")], 1.0)],
            "dst",
            "$0",
            "src",
            "foo",
        )
        .unwrap();
        assert2::assert!(out[0].labels.get("dst") == Some("foo"));
    }

    #[test]
    fn label_replace_invalid_regex_errors() {
        let err = apply_label_replace(vec![sample(&[("src", "x")], 1.0)], "dst", "$1", "src", "(")
            .unwrap_err();
        assert2::assert!(matches!(err, PromqlError::Plan(_)));
    }

    #[test]
    fn label_join_joins_sources_with_separator() {
        let out = apply_label_join(
            vec![sample(&[("__name__", "m"), ("a", "1"), ("b", "2")], 1.0)],
            "dst",
            "-",
            &["a".to_string(), "b".to_string()],
        );
        assert2::assert!(out[0].labels.get("dst") == Some("1-2"));
    }

    #[test]
    fn label_join_missing_source_is_empty() {
        let out = apply_label_join(
            vec![sample(&[("a", "1")], 1.0)],
            "dst",
            ",",
            &["a".to_string(), "missing".to_string()],
        );
        assert2::assert!(out[0].labels.get("dst") == Some("1,"));
    }

    /// The series whose `l` label spells the post-sort order, by label.
    fn order(out: &[InstantSample]) -> Vec<&str> {
        out.iter()
            .map(|s| s.labels.get("l").unwrap_or(""))
            .collect()
    }

    #[test]
    fn sort_ascending_places_nan_last() {
        let out = apply_sort(
            vec![
                sample(&[("l", "b")], 2.0),
                sample(&[("l", "n")], f64::NAN),
                sample(&[("l", "a")], 1.0),
            ],
            SortOrder::Ascending,
        );
        // 1.0 < 2.0 < NaN (total_cmp puts NaN last for ascending).
        assert2::assert!(order(&out) == vec!["a", "b", "n"]);
        assert2::assert!(matches!(out[2].value, SampleValue::Float(v) if v.is_nan()));
    }

    #[test]
    fn sort_desc_orders_high_to_low() {
        let out = apply_sort(
            vec![sample(&[("l", "a")], 1.0), sample(&[("l", "b")], 2.0)],
            SortOrder::Descending,
        );
        assert2::assert!(order(&out) == vec!["b", "a"]);
    }

    #[test]
    fn sort_breaks_ties_by_label_key() {
        let out = apply_sort(
            vec![sample(&[("l", "z")], 1.0), sample(&[("l", "a")], 1.0)],
            SortOrder::Ascending,
        );
        assert2::assert!(out[0].labels.get("l") == Some("a"));
        assert2::assert!(out[1].labels.get("l") == Some("z"));
    }
}

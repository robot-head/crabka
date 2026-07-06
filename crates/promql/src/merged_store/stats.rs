//! Cardinality and TSDB-stat merge helpers for [`super::MergedMetricStore`].

use std::collections::{BTreeMap, BTreeSet};

use crabka_blockstore::SeriesFingerprint;

use crate::{LabelNameCardinality, LabelValueCardinality, NamedTsdbStat};

pub(super) fn label_name_cardinality(
    by_name: BTreeMap<String, BTreeSet<SeriesFingerprint>>,
) -> Vec<LabelNameCardinality> {
    let mut out = by_name
        .into_iter()
        .map(|(name, fingerprints)| LabelNameCardinality {
            name,
            series_count: fingerprints.len(),
        })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    out
}

pub(super) fn label_value_cardinality(
    by_value: BTreeMap<(String, String), BTreeSet<SeriesFingerprint>>,
) -> Vec<LabelValueCardinality> {
    let mut out = by_value
        .into_iter()
        .map(
            |((label_name, label_value), fingerprints)| LabelValueCardinality {
                label_name,
                label_value,
                series_count: fingerprints.len(),
            },
        )
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.label_name.cmp(&right.label_name))
            .then_with(|| left.label_value.cmp(&right.label_value))
    });
    out
}

pub(super) fn merge_named_stats(
    left: Vec<NamedTsdbStat>,
    right: Vec<NamedTsdbStat>,
) -> Vec<NamedTsdbStat> {
    let mut values = BTreeMap::<String, usize>::new();
    for stat in left.into_iter().chain(right) {
        *values.entry(stat.name).or_default() += stat.value;
    }
    let mut out = values
        .into_iter()
        .map(|(name, value)| NamedTsdbStat { name, value })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.name.cmp(&right.name))
    });
    out
}

/// Combine the head min-time of two stores using explicit presence flags.
///
/// Emptiness is reported via `None` (the caller threads a has-data flag) rather
/// than overloading `0`, so a legitimate `min_time == 0` from a store that does
/// hold samples is preserved instead of being mistaken for "empty".
pub(super) fn min_present_time(left: Option<i64>, right: Option<i64>) -> i64 {
    match (left, right) {
        (Some(left), Some(right)) => left.min(right),
        (Some(value), None) | (None, Some(value)) => value,
        (None, None) => 0,
    }
}

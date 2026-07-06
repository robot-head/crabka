use std::collections::BTreeSet;

use super::labels::labels_key;
use crate::{PromqlError, error::Result, result::QueryResult};

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "PromQL quantile interpolation works in f64 rank space, then indexes a sorted in-memory vector after bounding the rank."
)]
pub(super) fn quantile_value(quantile: f64, values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // Prometheus' `quantile()` does NOT error on an out-of-range/NaN phi: a NaN
    // phi yields NaN, phi < 0 yields -Inf, and phi > 1 yields +Inf (the caller
    // raises an `InvalidQuantileWarning` alongside). This mirrors the
    // `histogram_quantile` family's leading guards.
    if quantile.is_nan() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }
    values.sort_by(f64::total_cmp);
    if values.len() == 1 {
        return Some(values[0]);
    }

    let rank = quantile * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return Some(values[lower]);
    }
    let weight = rank - lower as f64;
    Some(values[lower] * (1.0 - weight) + values[upper] * weight)
}

pub(super) fn validate_unique_instant_labelsets(result: &QueryResult) -> Result<()> {
    let QueryResult::InstantVector(samples) = result else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for sample in samples {
        let key = labels_key(&sample.labels);
        if !seen.insert(key.clone()) {
            return Err(PromqlError::Exec(format!(
                "vector cannot contain metrics with the same labelset: {key}"
            )));
        }
    }
    Ok(())
}

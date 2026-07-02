//! Select-series result types and step-bucketing helpers.

use crate::ProfileError;

/// One time series returned by `select_series` in the next slice.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub labels: Vec<(String, String)>,
    pub points: Vec<(i64, f64)>,
}

/// Select-series aggregation mode. Bodies land in the next slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeriesAgg {
    Sum,
    Average,
}

#[must_use]
pub fn step_bucket_ms(ts_ms: i64, step_ms: i64) -> i64 {
    ts_ms.div_euclid(step_ms) * step_ms
}

pub fn step_ms_from_secs(step_secs: f64) -> Result<i64, ProfileError> {
    if !(step_secs.is_finite() && step_secs > 0.0) {
        return Err(ProfileError::Plan(format!(
            "step must be a positive finite number of seconds, got {step_secs}"
        )));
    }
    if step_secs * 1000.0 < 1.0 {
        return Err(ProfileError::Plan("step must be >= 1ms".to_string()));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok((step_secs * 1000.0).round() as i64)
}

#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn fold_bucket(agg: SeriesAgg, values: &[i64]) -> f64 {
    let sum: i64 = values.iter().sum();
    match agg {
        SeriesAgg::Sum => sum as f64,
        SeriesAgg::Average => sum as f64 / values.len() as f64,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn takes_copy(_agg: SeriesAgg) {}

    #[test]
    fn series_holds_points_and_agg_is_copy() {
        let series = Series {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            points: vec![(1000, 1.5), (2000, 2.0)],
        };
        assert!(series.points[1] == (2000, 2.0));
        let agg = SeriesAgg::Sum;
        takes_copy(agg);
        takes_copy(agg);
    }

    #[test]
    fn step_secs_to_ms_rounds_and_rejects_nonpositive() {
        assert!(step_ms_from_secs(15.0).unwrap() == 15_000);
        assert!(step_ms_from_secs(0.5).unwrap() == 500);
        let zero = step_ms_from_secs(0.0).unwrap_err();
        assert!(matches!(zero, ProfileError::Plan(message) if message.contains("positive finite")));
        assert!(step_ms_from_secs(-1.0).is_err());
        let infinity = step_ms_from_secs(f64::INFINITY).unwrap_err();
        assert!(
            matches!(infinity, ProfileError::Plan(message) if message.contains("positive finite"))
        );
    }

    #[test]
    fn step_secs_rejects_sub_millisecond_values() {
        for (step_secs, want) in [
            (0.0001, None),
            (0.0005, None),
            (0.000_999_9, None),
            (0.001, Some(1)),
        ] {
            assert!(step_ms_from_secs(step_secs).ok() == want, "{step_secs}");
        }
    }

    #[test]
    fn bucket_start_is_step_floored() {
        for (timestamp_ms, want) in [
            (17_000, 15_000),
            (15_000, 15_000),
            (14_999, 0),
            (-1, -15_000),
        ] {
            assert!(
                step_bucket_ms(timestamp_ms, 15_000) == want,
                "{timestamp_ms}"
            );
        }
    }

    #[test]
    fn fold_sum_vs_average() {
        assert!((fold_bucket(SeriesAgg::Sum, &[2, 3, 5]) - 10.0).abs() < f64::EPSILON);
        assert!((fold_bucket(SeriesAgg::Average, &[2, 3, 5]) - 10.0 / 3.0).abs() < 1e-12);
    }
}

//! Profile heatmap binning.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heatmap {
    pub start_ms: i64,
    pub end_ms: i64,
    pub time_buckets: usize,
    pub value_buckets: usize,
    pub min_value: i64,
    pub max_value: i64,
    pub counts: Vec<Vec<u64>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledHeatmap {
    pub labels: Vec<(String, String)>,
    pub heatmap: Heatmap,
}

#[must_use]
pub fn bin_heatmap(
    points: &[(i64, i64)],
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    value_buckets: usize,
) -> Heatmap {
    let (min_value, max_value) = value_bounds(points);
    let mut counts = vec![vec![0; value_buckets]; time_buckets];
    if start_ms >= end_ms || time_buckets == 0 || value_buckets == 0 {
        return Heatmap {
            start_ms,
            end_ms,
            time_buckets,
            value_buckets,
            min_value,
            max_value,
            counts,
        };
    }

    let time_span = i128::from(end_ms - start_ms);
    let value_span = i128::from(max_value - min_value);
    for (timestamp, value) in points {
        if *timestamp < start_ms || *timestamp >= end_ms {
            continue;
        }
        let time_idx = bucket_index(i128::from(*timestamp - start_ms), time_span, time_buckets);
        let value_idx = if value_span == 0 {
            0
        } else {
            bucket_index(i128::from(*value - min_value), value_span, value_buckets)
        };
        counts[time_idx][value_idx] += 1;
    }

    Heatmap {
        start_ms,
        end_ms,
        time_buckets,
        value_buckets,
        min_value,
        max_value,
        counts,
    }
}

fn value_bounds(points: &[(i64, i64)]) -> (i64, i64) {
    let Some((_, first)) = points.first() else {
        return (0, 0);
    };
    points
        .iter()
        .fold((*first, *first), |(min, max), (_, value)| {
            (min.min(*value), max.max(*value))
        })
}

fn bucket_index(offset: i128, span: i128, buckets: usize) -> usize {
    let raw = offset * i128::try_from(buckets).expect("bucket count fits i128") / span;
    let clamped = raw.clamp(
        0,
        i128::try_from(buckets - 1).expect("bucket count fits i128"),
    );
    usize::try_from(clamped).expect("bucket index fits usize")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn bin_counts_profiles_per_time_value_cell() {
        let points = vec![(0, 0), (10, 5), (60, 30), (90, 35)];

        let heatmap = bin_heatmap(&points, 0, 100, 2, 2);

        assert!(
            heatmap
                == Heatmap {
                    start_ms: 0,
                    end_ms: 100,
                    time_buckets: 2,
                    value_buckets: 2,
                    min_value: 0,
                    max_value: 35,
                    counts: vec![vec![2, 0], vec![0, 2]],
                }
        );
    }

    #[test]
    fn bin_returns_empty_counts_for_invalid_ranges_or_zero_buckets() {
        let invalid_range = bin_heatmap(&[(10, 1)], 20, 10, 2, 2);
        assert!(invalid_range.counts == vec![vec![0, 0], vec![0, 0]]);

        let zero_time_buckets = bin_heatmap(&[(10, 1)], 0, 20, 0, 2);
        assert!(zero_time_buckets.counts.is_empty());

        let zero_value_buckets = bin_heatmap(&[(10, 1)], 0, 20, 2, 0);
        assert!(zero_value_buckets.counts == vec![Vec::<u64>::new(), Vec::new()]);
    }

    #[test]
    fn bin_uses_offsets_and_excludes_points_outside_time_range() {
        let points = vec![
            (99, 30),
            (100, 10),
            (149, 20),
            (150, 20),
            (199, 30),
            (200, 10),
        ];

        let heatmap = bin_heatmap(&points, 100, 200, 2, 2);

        assert!(heatmap.min_value == 10 && heatmap.max_value == 30);
        assert!(heatmap.counts == vec![vec![1, 1], vec![0, 2]]);
    }
}

//! Selectivity and cardinality estimates derived from `ANALYZE` statistics.
//!
//! This is deliberately independent from the parser and catalog.  The planner
//! supplies decoded MCV and histogram values; keeping the arithmetic here makes
//! it usable for ordinary columns and expression statistics alike.

#![allow(
    dead_code,
    reason = "P2 exposes the complete estimator surface before P4 attaches every path type"
)]

use crabka_pgtypes::{ColumnType, Datum};

/// PostgreSQL's fallback for equality predicates without usable statistics.
pub(crate) const DEFAULT_EQ_SEL: f64 = 0.005;
/// PostgreSQL's fallback for scalar inequality predicates.
pub(crate) const DEFAULT_INEQ_SEL: f64 = 1.0 / 3.0;
/// PostgreSQL's fallback for pattern predicates without a usable prefix.
pub(crate) const DEFAULT_MATCH_SEL: f64 = 0.005;
const DEFAULT_NUM_DISTINCT: f64 = 1.0 / DEFAULT_EQ_SEL;

/// Decoded single-column statistics used by the scalar estimators.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ColumnStats<'a, T> {
    /// Relation cardinality before this clause is applied.
    pub(crate) rows: f64,
    /// Fraction of rows whose value is SQL NULL.
    pub(crate) null_frac: f64,
    /// PostgreSQL's `stadistinct` convention: a negative value is a fraction
    /// of `rows`, while a non-negative value is an absolute count.
    pub(crate) n_distinct: Option<f64>,
    /// Values and fractions from the MCV slot.
    pub(crate) mcv: &'a [(T, f64)],
    /// Sorted scalar histogram bounds, when the type has an ordering.
    pub(crate) histogram: &'a [T],
}

/// Decoded catalog statistics. `pg_statistic` stores its value slots as array
/// text, while selectivity code must compare real datums; this bridge performs
/// the same element input step as PostgreSQL's statistic slot reader.
#[derive(Debug, Clone)]
pub(crate) struct DecodedColumnStats {
    rows: f64,
    null_frac: f64,
    n_distinct: Option<f64>,
    mcv: Vec<(Datum, f64)>,
    histogram: Vec<Datum>,
}

impl DecodedColumnStats {
    /// Borrow the decoded values through the generic equality/join estimator.
    pub(crate) fn as_stats(&self) -> ColumnStats<'_, Datum> {
        ColumnStats {
            rows: self.rows,
            null_frac: self.null_frac,
            n_distinct: self.n_distinct,
            mcv: &self.mcv,
            histogram: &self.histogram,
        }
    }

    /// Estimate an inequality from typed scalar slots.
    ///
    /// Numeric slots retain their interpolated histogram estimate. Other
    /// orderable types still use their exact MCV mass, which is especially
    /// important when the MCV list covers the relation.
    pub(crate) fn scalar_inequality(
        &self,
        constant: &Datum,
        inequality: Inequality,
    ) -> Option<f64> {
        if let Some(constant) = numeric_datum(constant)
            && let Some(mcv) = self
                .mcv
                .iter()
                .map(|(value, frequency)| Some((numeric_datum(value)?, *frequency)))
                .collect::<Option<Vec<_>>>()
            && let Some(histogram) = self
                .histogram
                .iter()
                .map(numeric_datum)
                .collect::<Option<Vec<_>>>()
        {
            return Some(scalarineqsel(
                ColumnStats {
                    rows: self.rows,
                    null_frac: self.null_frac,
                    n_distinct: self.n_distinct,
                    mcv: &mcv,
                    histogram: &histogram,
                },
                Some(constant),
                inequality,
            ));
        }

        let mcv = self
            .mcv
            .iter()
            .try_fold(0.0, |selectivity, (value, frequency)| {
                let ordering = crabka_pgtypes::ops::compare(value, constant).ok()??;
                let selected = matches!(
                    (inequality, ordering),
                    (Inequality::Less, std::cmp::Ordering::Less)
                        | (
                            Inequality::LessEqual,
                            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                        )
                        | (Inequality::Greater, std::cmp::Ordering::Greater)
                        | (
                            Inequality::GreaterEqual,
                            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                        )
                );
                Some(selectivity + if selected { *frequency } else { 0.0 })
            })?;
        let common = self.mcv.iter().map(|(_, frequency)| frequency).sum::<f64>();
        Some(probability(
            mcv + (1.0 - probability(self.null_frac) - probability(common)).max(0.0) * 0.5,
        ))
    }
}

fn numeric_datum(value: &Datum) -> Option<f64> {
    let value = match value {
        Datum::Int2(value) => f64::from(*value),
        Datum::Int4(value) => f64::from(*value),
        Datum::Int8(value) => *value as f64,
        Datum::Float4(value) => f64::from(*value),
        Datum::Float8(value) => *value,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

/// Decode the statistic slots belonging to one typed attribute. Corrupt or
/// obsolete slots are ignored by returning `None`: planning must fall back to
/// default selectivity rather than make `EXPLAIN` fail on old metadata.
pub(crate) fn decode_catalog_stats(
    stats: &crate::attrstats::AttributeStats,
    ty: ColumnType,
    rows: f64,
    ctx: &crate::clock::EvalCtx,
) -> Option<DecodedColumnStats> {
    let mcv_values = match stats.most_common_vals.as_deref() {
        Some(text) => decode_stat_array(text, ty, ctx)?,
        None => Vec::new(),
    };
    let mcv_frequencies = match stats.most_common_freqs.as_deref() {
        Some(text) => decode_frequency_array(text)?,
        None => Vec::new(),
    };
    if mcv_values.len() != mcv_frequencies.len() {
        return None;
    }
    let histogram = match stats.histogram_bounds.as_deref() {
        Some(text) => decode_stat_array(text, ty, ctx)?,
        None => Vec::new(),
    };
    Some(DecodedColumnStats {
        rows,
        null_frac: f64::from(stats.null_frac.unwrap_or_default()),
        n_distinct: stats.n_distinct.map(f64::from),
        mcv: mcv_values.into_iter().zip(mcv_frequencies).collect(),
        histogram,
    })
}

/// Estimate `constant = ANY(array_column)` from PostgreSQL's MCELEM slot.
pub(crate) fn array_element_selectivity(
    stats: &crate::attrstats::AttributeStats,
    element_type: ColumnType,
    constant: &Datum,
    ctx: &crate::clock::EvalCtx,
) -> Option<f64> {
    let elements = decode_stat_array(stats.most_common_elems.as_deref()?, element_type, ctx)?;
    let frequencies = decode_frequency_array(stats.most_common_elem_freqs.as_deref()?)?;
    if frequencies.len() != elements.len() + 3 {
        return None;
    }
    let selectivity = elements
        .iter()
        .zip(&frequencies)
        .find_map(|(element, frequency)| {
            (crabka_pgtypes::ops::compare(element, constant).ok()?
                == Some(std::cmp::Ordering::Equal))
            .then_some(*frequency)
        })
        .unwrap_or_else(|| DEFAULT_MATCH_SEL.min(frequencies[elements.len()] / 2.0));
    Some(probability(
        selectivity * (1.0 - f64::from(stats.null_frac.unwrap_or_default())),
    ))
}

impl<T> ColumnStats<'_, T> {
    fn null_frac(&self) -> f64 {
        probability(self.null_frac)
    }

    fn distinct_count(&self) -> f64 {
        match self.n_distinct {
            Some(count) if count >= 0.0 => count.max(1.0),
            Some(count) if self.rows > 0.0 => (-count * self.rows).max(1.0),
            _ => DEFAULT_NUM_DISTINCT,
        }
    }

    fn common_fraction(&self) -> f64 {
        probability(self.mcv.iter().map(|(_, frequency)| frequency).sum())
    }
}

/// A scalar comparison direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Inequality {
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

/// A boolean test expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoolTest {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    IsUnknown,
    IsNotUnknown,
}

/// Estimate `var = constant`, including PostgreSQL's MCV and `stadistinct`
/// rules. A null constant is always filtered by a strict equality operator.
pub(crate) fn eqsel<T: PartialEq>(stats: ColumnStats<'_, T>, constant: Option<&T>) -> f64 {
    let Some(constant) = constant else {
        return 0.0;
    };
    if let Some((_, frequency)) = stats.mcv.iter().find(|(value, _)| value == constant) {
        return probability(*frequency);
    }

    let common = stats.common_fraction();
    let other_distinct = (stats.distinct_count() - stats.mcv.len() as f64).max(1.0);
    let mut estimate = (1.0 - stats.null_frac() - common).max(0.0) / other_distinct;
    if let Some(least_common) = stats
        .mcv
        .iter()
        .map(|(_, frequency)| *frequency)
        .reduce(f64::min)
    {
        estimate = estimate.min(least_common);
    }
    probability(estimate)
}

/// Estimate `var <> constant`. Nulls never satisfy a strict inequality.
pub(crate) fn neqsel<T: PartialEq>(stats: ColumnStats<'_, T>, constant: Option<&T>) -> f64 {
    probability(1.0 - stats.null_frac() - eqsel(stats, constant))
}

/// Estimate a scalar inequality from MCVs and an evenly distributed histogram.
///
/// Histogram values are represented on their ordering scalar; catalog adapters
/// may use this directly for numeric, date, and timestamp columns.
pub(crate) fn scalarineqsel(
    stats: ColumnStats<'_, f64>,
    constant: Option<f64>,
    inequality: Inequality,
) -> f64 {
    let Some(constant) = constant else {
        return 0.0;
    };
    let common = stats.common_fraction();
    let mcv: f64 = stats
        .mcv
        .iter()
        .filter(|(value, _)| matches_inequality(*value, constant, inequality))
        .map(|(_, frequency)| *frequency)
        .sum();
    let other_distinct = stats.distinct_count() - stats.mcv.len() as f64;
    let equality = (other_distinct > 1.0)
        .then(|| other_distinct.recip())
        .unwrap_or(0.0);
    let histogram =
        histogram_selectivity(stats.histogram, constant, inequality, equality).unwrap_or(0.5);
    probability(mcv + (1.0 - stats.null_frac() - common).max(0.0) * histogram)
}

/// Estimate `IS [NOT] NULL` using `stanullfrac`.
pub(crate) fn nulltestsel<T>(stats: ColumnStats<'_, T>, is_null: bool) -> f64 {
    let null = stats.null_frac();
    if is_null { null } else { 1.0 - null }
}

/// Estimate a boolean test. Values omitted from an MCV list share its
/// remaining non-null population equally, the same neutral assumption used by
/// PostgreSQL's boolean estimator.
pub(crate) fn booltestsel(stats: ColumnStats<'_, bool>, test: BoolTest) -> f64 {
    let null = stats.null_frac();
    let true_mcv: f64 = stats
        .mcv
        .iter()
        .filter(|(value, _)| *value)
        .map(|(_, frequency)| *frequency)
        .sum();
    let false_mcv: f64 = stats
        .mcv
        .iter()
        .filter(|(value, _)| !*value)
        .map(|(_, frequency)| *frequency)
        .sum();
    let remainder = (1.0 - null - true_mcv - false_mcv).max(0.0) / 2.0;
    let truth = probability(true_mcv + remainder);
    let falsehood = probability(false_mcv + remainder);
    match test {
        BoolTest::IsTrue => truth,
        BoolTest::IsNotTrue => probability(falsehood + null),
        BoolTest::IsFalse => falsehood,
        BoolTest::IsNotFalse => probability(truth + null),
        BoolTest::IsUnknown => null,
        BoolTest::IsNotUnknown => 1.0 - null,
    }
}

/// Estimate an equality join, preserving the known joint mass of matching
/// MCVs and distributing the remaining values over the larger distinct set.
pub(crate) fn eqjoinsel<T: PartialEq>(left: ColumnStats<'_, T>, right: ColumnStats<'_, T>) -> f64 {
    let mcv: f64 = left
        .mcv
        .iter()
        .filter_map(|(left_value, left_frequency)| {
            right
                .mcv
                .iter()
                .find(|(right_value, _)| right_value == left_value)
                .map(|(_, right_frequency)| left_frequency * right_frequency)
        })
        .sum();
    let left_other = (1.0 - left.null_frac() - left.common_fraction()).max(0.0);
    let right_other = (1.0 - right.null_frac() - right.common_fraction()).max(0.0);
    let distinct = left.distinct_count().max(right.distinct_count()).max(1.0);
    probability(mcv + left_other * right_other / distinct)
}

/// Estimate groups produced by independent grouping expressions. PostgreSQL
/// treats multiple keys as likely correlated, capping their product at ten
/// percent of the relation cardinality but never below the largest key.
pub(crate) fn estimate_num_groups(input_rows: f64, relation_rows: f64, distincts: &[f64]) -> f64 {
    if distincts.is_empty() {
        return 1.0;
    }
    let groups = distincts
        .iter()
        .fold(1.0, |groups, distinct| groups * distinct.max(1.0));
    let groups = if distincts.len() > 1 {
        groups.min((relation_rows * 0.1).max(distincts.iter().copied().fold(1.0_f64, f64::max)))
    } else {
        groups
    };
    groups.ceil().clamp(1.0, input_rows.max(1.0))
}

/// Hash-table load estimates for a single hash key.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct HashBucketStats {
    /// Expected tuples in a bucket not occupied by an MCV.
    pub(crate) average_bucket_rows: f64,
    /// Largest known MCV bucket as a fraction of the relation.
    pub(crate) mcv_frequency: f64,
}

/// Estimate the ordinary and most-common hash bucket sizes.
pub(crate) fn estimate_hash_bucket_stats<T>(
    stats: ColumnStats<'_, T>,
    buckets: f64,
) -> HashBucketStats {
    let buckets = buckets.max(1.0);
    HashBucketStats {
        average_bucket_rows: ((1.0 - stats.null_frac()) * stats.rows.max(0.0) / buckets).max(0.0),
        mcv_frequency: stats
            .mcv
            .iter()
            .map(|(_, frequency)| probability(*frequency))
            .fold(0.0, f64::max),
    }
}

/// The fallback for LIKE/regex without a prefix that can use a histogram.
pub(crate) const fn patternsel() -> f64 {
    DEFAULT_MATCH_SEL
}

fn histogram_selectivity(
    bounds: &[f64],
    constant: f64,
    inequality: Inequality,
    equality: f64,
) -> Option<f64> {
    let (first, last) = (*bounds.first()?, *bounds.last()?);
    if bounds.len() < 2 || !constant.is_finite() {
        return None;
    }
    let less = if constant <= first {
        0.0
    } else if constant >= last {
        1.0
    } else {
        let upper = bounds.partition_point(|bound| *bound <= constant);
        let lower = upper - 1;
        let span = bounds[upper] - bounds[lower];
        let fraction = if span == 0.0 {
            0.5
        } else {
            (constant - bounds[lower]) / span
        };
        (lower as f64 + fraction) / (bounds.len() - 1) as f64
    };
    let equality = (constant <= last).then_some(equality).unwrap_or(0.0);
    Some(match inequality {
        Inequality::Less => probability(less - equality),
        Inequality::LessEqual => less,
        Inequality::Greater => 1.0 - less,
        Inequality::GreaterEqual => probability(1.0 - less + equality),
    })
}

fn matches_inequality(value: f64, constant: f64, inequality: Inequality) -> bool {
    match inequality {
        Inequality::Less => value < constant,
        Inequality::LessEqual => value <= constant,
        Inequality::Greater => value > constant,
        Inequality::GreaterEqual => value >= constant,
    }
}

fn probability(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

fn decode_stat_array(
    text: &str,
    ty: ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Option<Vec<Datum>> {
    crabka_pgtypes::array::parse_literal(text)
        .ok()?
        .elements
        .into_iter()
        .map(|element| {
            let text = element?;
            crate::eval::cast_value_in(&Datum::Text(text), ty, ctx.output_style()).ok()
        })
        .collect()
}

fn decode_frequency_array(text: &str) -> Option<Vec<f64>> {
    crabka_pgtypes::array::parse_literal(text)
        .ok()?
        .elements
        .into_iter()
        .map(|element| {
            element?
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer_stats<'a>(mcv: &'a [(i32, f64)]) -> ColumnStats<'a, i32> {
        ColumnStats {
            rows: 100.0,
            null_frac: 0.1,
            n_distinct: Some(10.0),
            mcv,
            histogram: &[],
        }
    }

    #[test]
    fn equality_uses_mcv_then_non_common_population() {
        let stats = integer_stats(&[(1, 0.4), (2, 0.2)]);
        assert!((eqsel(stats, Some(&1)) - 0.4).abs() < f64::EPSILON);
        // (1 - null - MCV mass) / remaining distinct values, capped by the
        // least common MCV: 0.3 / 8.
        assert!((eqsel(stats, Some(&9)) - 0.0375).abs() < f64::EPSILON);
        assert!((neqsel(stats, Some(&1)) - 0.5).abs() < f64::EPSILON);
        assert_eq!(eqsel(stats, None), 0.0);
    }

    #[test]
    fn scalar_inequality_combines_mcv_and_histogram_populations() {
        let stats = ColumnStats {
            rows: 100.0,
            null_frac: 0.1,
            n_distinct: Some(10.0),
            mcv: &[(1.0, 0.4), (9.0, 0.1)],
            histogram: &[2.0, 6.0, 10.0],
        };
        // MCV 1 plus the strict histogram fraction after excluding equality.
        assert!((scalarineqsel(stats, Some(6.0), Inequality::Less) - 0.55).abs() < f64::EPSILON);
        for inequality in [
            Inequality::LessEqual,
            Inequality::Greater,
            Inequality::GreaterEqual,
        ] {
            assert!((0.0..=1.0).contains(&scalarineqsel(stats, Some(6.0), inequality)));
        }
        assert!((DEFAULT_INEQ_SEL - 1.0 / 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn boolean_and_join_estimators_keep_nulls_out_of_strict_predicates() {
        let bools = ColumnStats {
            rows: 20.0,
            null_frac: 0.2,
            n_distinct: Some(2.0),
            mcv: &[(true, 0.5), (false, 0.3)],
            histogram: &[],
        };
        assert!((booltestsel(bools, BoolTest::IsTrue) - 0.5).abs() < f64::EPSILON);
        assert!((booltestsel(bools, BoolTest::IsNotTrue) - 0.5).abs() < f64::EPSILON);
        assert!((nulltestsel(bools, true) - 0.2).abs() < f64::EPSILON);
        for test in [
            BoolTest::IsFalse,
            BoolTest::IsNotFalse,
            BoolTest::IsUnknown,
            BoolTest::IsNotUnknown,
        ] {
            assert!((0.0..=1.0).contains(&booltestsel(bools, test)));
        }

        let left = integer_stats(&[(1, 0.5)]);
        let right = integer_stats(&[(1, 0.2)]);
        assert!((eqjoinsel(left, right) - 0.128).abs() < f64::EPSILON);
    }

    #[test]
    fn cardinality_helpers_bound_their_results() {
        assert!((estimate_num_groups(100.0, 100.0, &[3.0, 40.0]) - 40.0).abs() < f64::EPSILON);
        assert!(
            (estimate_num_groups(1_000.0, 1_000.0, &[11.0, 11.0]) - 100.0).abs() < f64::EPSILON
        );
        assert!((estimate_num_groups(0.0, 0.0, &[]) - 1.0).abs() < f64::EPSILON);
        let buckets = estimate_hash_bucket_stats(integer_stats(&[(1, 0.6)]), 10.0);
        assert!((buckets.average_bucket_rows - 9.0).abs() < f64::EPSILON);
        assert!((buckets.mcv_frequency - 0.6).abs() < f64::EPSILON);
        assert!((patternsel() - DEFAULT_MATCH_SEL).abs() < f64::EPSILON);
    }

    #[test]
    fn catalog_slots_decode_through_the_attribute_type() {
        let stats = crate::attrstats::AttributeStats {
            null_frac: Some(0.1),
            n_distinct: Some(4.0),
            most_common_vals: Some("{1,2}".into()),
            most_common_freqs: Some("{0.4,0.2}".into()),
            histogram_bounds: Some("{1,5,9}".into()),
            ..crate::attrstats::AttributeStats::default()
        };
        let decoded = decode_catalog_stats(
            &stats,
            ColumnType::Int4,
            10.0,
            &crate::clock::EvalCtx::test_default(),
        )
        .expect("valid statistics decode");
        assert!((eqsel(decoded.as_stats(), Some(&Datum::Int4(1))) - 0.4).abs() < f64::EPSILON);
        assert!(decoded.as_stats().histogram == [Datum::Int4(1), Datum::Int4(5), Datum::Int4(9)]);
    }

    #[test]
    fn malformed_or_mismatched_slots_fall_back_cleanly() {
        let mismatched = crate::attrstats::AttributeStats {
            most_common_vals: Some("{1,2}".into()),
            most_common_freqs: Some("{0.5}".into()),
            ..crate::attrstats::AttributeStats::default()
        };
        assert!(
            decode_catalog_stats(
                &mismatched,
                ColumnType::Int4,
                1.0,
                &crate::clock::EvalCtx::test_default(),
            )
            .is_none()
        );
    }
}

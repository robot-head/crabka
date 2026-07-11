//! `PromQL` `min`/`max` aggregations as `DataFusion` [`AggregateUDF`]s.
//!
//! Arrow/`DataFusion`'s built-in `min`/`max` order floats with `total_cmp`,
//! which places NaN at the extremes and therefore *propagates* NaN into the
//! result (a NaN sample can become the reported `max`, and a group's `min`/`max`
//! is NaN whenever any sample is NaN). Prometheus does the opposite: `min`/`max`
//! **ignore** NaN — a group's extremum is taken over its non-NaN samples, and
//! the result is NaN only when **every** sample in the group is NaN.
//!
//! These UDAFs (`prom_min`/`prom_max`) reproduce Prometheus' aggregation loop
//! exactly (`promql/engine.go`), so the operator path agrees bit-for-bit with the
//! tree-walking interpreter's [`crate::engine`] `AggregateState`:
//!
//! - The running extremum is seeded with the first observed sample (NaN
//!   included).
//! - Each later sample `f` replaces the running value `r` when `r {>,<} f`
//!   (the float comparison the new sample wins) **or** when `r` is NaN. Because
//!   `NaN > _` and `NaN < _` are both false, a non-NaN sample always displaces a
//!   NaN seed, while a NaN sample never displaces an existing non-NaN extremum.
//! - An empty group produces no accumulator output here (the planner's grouping
//!   guarantees every emitted group has at least one row); an all-NaN group keeps
//!   NaN.
//!
//! Signed zero matches Prometheus: `0.0 {>,<} -0.0` is false, so the
//! first-observed zero is kept (neither sign displaces the other).

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray},
    datatypes::{DataType, Float64Type},
};
use datafusion::{
    common::{Result as DfResult, ScalarValue},
    logical_expr::{Accumulator, AggregateUDF, Volatility, create_udaf, function::AccumulatorArgs},
    prelude::SessionContext,
};

/// Registered name of the NaN-ignoring `min` aggregate UDAF.
pub const PROM_MIN_UDAF_NAME: &str = "prom_min";
/// Registered name of the NaN-ignoring `max` aggregate UDAF.
pub const PROM_MAX_UDAF_NAME: &str = "prom_max";

/// Which extremum a [`PromExtremumAccumulator`] tracks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Extremum {
    Min,
    Max,
}

impl Extremum {
    /// Whether the running value `running` should be replaced by `candidate`
    /// under Prometheus' NaN-ignoring float ordering. A NaN running value is
    /// always replaced; a NaN candidate (with a non-NaN running value) never is.
    fn should_replace(self, running: f64, candidate: f64) -> bool {
        if running.is_nan() {
            return true;
        }
        match self {
            Self::Min => running > candidate,
            Self::Max => running < candidate,
        }
    }
}

/// Prometheus-faithful NaN-ignoring `min`/`max` accumulator over `Float64`
/// samples. `running` holds the seeded extremum once `seen` is set.
#[derive(Debug)]
struct PromExtremumAccumulator {
    extremum: Extremum,
    running: f64,
    seen: bool,
}

impl PromExtremumAccumulator {
    fn new(extremum: Extremum) -> Self {
        Self {
            extremum,
            running: f64::NAN,
            seen: false,
        }
    }

    /// Fold a single float sample into the running extremum, seeding on the
    /// first observation (NaN included) and otherwise applying the
    /// NaN-ignoring replacement rule.
    fn observe(&mut self, value: f64) {
        if self.seen {
            if self.extremum.should_replace(self.running, value) {
                self.running = value;
            }
        } else {
            self.seen = true;
            self.running = value;
        }
    }
}

impl Accumulator for PromExtremumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        // Single `Float64` input column. Arrow nulls cannot appear on the
        // operator path's `value` column (the leaf always emits a non-null
        // float), but a null is skipped defensively rather than seeding the
        // accumulator with a spurious sample.
        let array = values[0].as_primitive::<Float64Type>();
        for value in array.iter().flatten() {
            self.observe(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        // An unseen accumulator (empty group) reports NULL; the planner never
        // emits an empty group, so this only guards against misuse.
        let value = if self.seen { Some(self.running) } else { None };
        Ok(ScalarValue::Float64(value))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        // Serialize the running extremum plus the seen flag so partial
        // aggregates merge correctly. An unseen partition emits NULL/false and
        // contributes nothing on merge.
        let running = if self.seen { Some(self.running) } else { None };
        Ok(vec![
            ScalarValue::Float64(running),
            ScalarValue::Boolean(Some(self.seen)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        // `states[0]` = each partition's running extremum, `states[1]` = its
        // seen flag. Only seen partitions contribute; their running value folds
        // through the same NaN-ignoring rule, so the merge of partial states
        // matches a single-pass scan exactly (including all-NaN -> NaN).
        let running = states[0].as_primitive::<Float64Type>();
        let seen = states[1].as_boolean();
        for (running, seen) in running.iter().zip(seen.iter()) {
            if seen == Some(true) {
                // A seen partition always carries a (possibly NaN) running value.
                self.observe(running.unwrap_or(f64::NAN));
            }
        }
        Ok(())
    }
}

/// Build the NaN-ignoring `prom_min` / `prom_max` aggregate UDAF.
#[must_use]
fn extremum_udaf(extremum: Extremum) -> AggregateUDF {
    let name = match extremum {
        Extremum::Min => PROM_MIN_UDAF_NAME,
        Extremum::Max => PROM_MAX_UDAF_NAME,
    };
    create_udaf(
        name,
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(move |_args: AccumulatorArgs| {
            Ok(Box::new(PromExtremumAccumulator::new(extremum)) as Box<dyn Accumulator>)
        }),
        // (running extremum, seen flag) intermediate state.
        Arc::new(vec![DataType::Float64, DataType::Boolean]),
    )
}

/// The NaN-ignoring `min` aggregate UDAF.
#[must_use]
pub fn prom_min_udaf() -> AggregateUDF {
    extremum_udaf(Extremum::Min)
}

/// The NaN-ignoring `max` aggregate UDAF.
#[must_use]
pub fn prom_max_udaf() -> AggregateUDF {
    extremum_udaf(Extremum::Max)
}

/// Register `prom_min`/`prom_max` on `ctx` so the aggregation planner can lower
/// `min`/`max` onto NaN-ignoring UDAFs that match the interpreter.
pub fn register_aggregate_udafs(ctx: &SessionContext) {
    ctx.register_udaf(prom_min_udaf());
    ctx.register_udaf(prom_max_udaf());
}

#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, Float64Array};
    use datafusion::execution::FunctionRegistry;

    use super::*;

    fn run(extremum: Extremum, samples: &[f64]) -> ScalarValue {
        let mut acc = PromExtremumAccumulator::new(extremum);
        let array: ArrayRef = Arc::new(Float64Array::from(samples.to_vec()));
        acc.update_batch(&[array]).unwrap();
        acc.evaluate().unwrap()
    }

    fn float(value: ScalarValue) -> f64 {
        match value {
            ScalarValue::Float64(Some(value)) => value,
            other => panic!("expected non-null Float64, got {other:?}"),
        }
    }

    /// Bit-exact float equality (so a result of exactly `expected` is required,
    /// avoiding clippy's `float_cmp` lint while remaining precise for the
    /// integer-valued and signed-zero cases under test).
    fn bits_eq(value: f64, expected: f64) -> bool {
        value.to_bits() == expected.to_bits()
    }

    #[test]
    fn ignores_nan_in_mixed_group() {
        // min/max are taken over the non-NaN values; NaN appearing first then
        // non-NaN still yields the non-NaN extremum.
        for (extremum, samples, want) in [
            (Extremum::Min, &[f64::NAN, 3.0, 1.0, f64::NAN][..], 1.0),
            (Extremum::Max, &[f64::NAN, 3.0, 1.0, f64::NAN][..], 3.0),
            (Extremum::Min, &[f64::NAN, 5.0][..], 5.0),
            (Extremum::Max, &[f64::NAN, 5.0][..], 5.0),
        ] {
            assert2::assert!(bits_eq(float(run(extremum, samples)), want));
        }
    }

    #[test]
    fn all_nan_group_yields_nan() {
        // A single NaN sample is still NaN (seen, never displaced).
        for (extremum, samples) in [
            (Extremum::Min, &[f64::NAN, f64::NAN][..]),
            (Extremum::Max, &[f64::NAN, f64::NAN][..]),
            (Extremum::Min, &[f64::NAN][..]),
            (Extremum::Max, &[f64::NAN][..]),
        ] {
            assert2::assert!(float(run(extremum, samples)).is_nan());
        }
    }

    #[test]
    fn handles_infinities() {
        assert2::assert!(
            float(run(Extremum::Min, &[f64::INFINITY, 1.0, f64::NEG_INFINITY])).is_infinite()
        );
        for (extremum, samples, want) in [
            (
                Extremum::Min,
                &[1.0, f64::NEG_INFINITY][..],
                f64::NEG_INFINITY,
            ),
            (Extremum::Max, &[1.0, f64::INFINITY][..], f64::INFINITY),
            (
                Extremum::Max,
                &[f64::NEG_INFINITY, f64::NAN][..],
                f64::NEG_INFINITY,
            ),
        ] {
            assert2::assert!(bits_eq(float(run(extremum, samples)), want));
        }
    }

    #[test]
    fn signed_zero_keeps_first_seen() {
        // `0.0 {>,<} -0.0` is false, so the first-observed zero is retained,
        // matching Prometheus and the interpreter.
        for (extremum, samples, want) in [
            (Extremum::Min, [0.0, -0.0], 0.0),
            (Extremum::Min, [-0.0, 0.0], -0.0),
            (Extremum::Max, [0.0, -0.0], 0.0),
            (Extremum::Max, [-0.0, 0.0], -0.0),
        ] {
            assert2::assert!(bits_eq(float(run(extremum, &samples)), want));
        }
    }

    #[test]
    fn empty_group_evaluates_null() {
        let mut acc = PromExtremumAccumulator::new(Extremum::Min);
        let empty: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        acc.update_batch(&[empty]).unwrap();
        assert2::assert!(acc.evaluate().unwrap() == ScalarValue::Float64(None));
    }

    #[test]
    fn accumulator_size_reports_struct_size() {
        let acc = PromExtremumAccumulator::new(Extremum::Min);
        assert2::assert!(acc.size() == std::mem::size_of_val(&acc));
        assert2::assert!(acc.size() > 1);
    }

    #[test]
    fn merge_of_partial_states_matches_single_pass() {
        // Two partitions: one all-NaN, one with finite values. The merged result
        // is the min over the finite values (the all-NaN partition contributes a
        // NaN running value that is displaced).
        let mut left = PromExtremumAccumulator::new(Extremum::Min);
        left.update_batch(&[Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN])) as ArrayRef])
            .unwrap();
        let mut right = PromExtremumAccumulator::new(Extremum::Min);
        right
            .update_batch(&[Arc::new(Float64Array::from(vec![4.0, 2.0])) as ArrayRef])
            .unwrap();

        let left_state = left.state().unwrap();
        let right_state = right.state().unwrap();
        let running = Arc::new(Float64Array::from(vec![
            match left_state[0] {
                ScalarValue::Float64(value) => value,
                _ => unreachable!(),
            },
            match right_state[0] {
                ScalarValue::Float64(value) => value,
                _ => unreachable!(),
            },
        ])) as ArrayRef;
        let seen = Arc::new(BooleanArray::from(vec![
            match left_state[1] {
                ScalarValue::Boolean(value) => value,
                _ => unreachable!(),
            },
            match right_state[1] {
                ScalarValue::Boolean(value) => value,
                _ => unreachable!(),
            },
        ])) as ArrayRef;

        let mut merged = PromExtremumAccumulator::new(Extremum::Min);
        merged.merge_batch(&[running, seen]).unwrap();
        assert2::assert!(bits_eq(float(merged.evaluate().unwrap()), 2.0));
    }

    #[test]
    fn merge_ignores_unseen_partition_even_with_running_value() {
        let running = Arc::new(Float64Array::from(vec![7.0])) as ArrayRef;
        let seen = Arc::new(BooleanArray::from(vec![false])) as ArrayRef;

        let mut merged = PromExtremumAccumulator::new(Extremum::Min);
        merged.merge_batch(&[running, seen]).unwrap();
        assert2::assert!(merged.evaluate().unwrap() == ScalarValue::Float64(None));
    }

    #[test]
    fn merge_of_all_nan_partitions_stays_nan() {
        let mut a = PromExtremumAccumulator::new(Extremum::Max);
        a.update_batch(&[Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef])
            .unwrap();
        let a_state = a.state().unwrap();
        let running = Arc::new(Float64Array::from(vec![match a_state[0] {
            ScalarValue::Float64(value) => value,
            _ => unreachable!(),
        }])) as ArrayRef;
        let seen = Arc::new(BooleanArray::from(vec![match a_state[1] {
            ScalarValue::Boolean(value) => value,
            _ => unreachable!(),
        }])) as ArrayRef;
        let mut merged = PromExtremumAccumulator::new(Extremum::Max);
        merged.merge_batch(&[running, seen]).unwrap();
        assert2::assert!(float(merged.evaluate().unwrap()).is_nan());
    }

    #[test]
    fn register_installs_min_and_max_udafs() {
        let ctx = SessionContext::new();
        register_aggregate_udafs(&ctx);

        assert2::assert!(ctx.udaf(PROM_MIN_UDAF_NAME).is_ok());
        assert2::assert!(ctx.udaf(PROM_MAX_UDAF_NAME).is_ok());
    }
}

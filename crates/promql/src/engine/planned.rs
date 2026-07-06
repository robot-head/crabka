use std::collections::BTreeMap;

use crabka_blockstore::{Labels, SeriesFingerprint};
use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

use crate::result::{InstantSample, RangeSeries};

/// How a planner-path output batch carries its result value and labels, so the
/// shared assembler (`PromqlEngine::assemble_planned_instant`) knows how to read
/// each shape's columns into an `InstantVector`.
pub(super) enum InstantShape {
    /// `SeriesDivide -> SeriesNormalize -> InstantManipulate`. Output carries
    /// label columns plus `timestamp`/`value`/`sample_timestamp`; the selected
    /// sample's true timestamp survives in `sample_timestamp`. Result labels are
    /// recovered from `labels_by_fp` keyed by the row's reconstructed fingerprint.
    Selector,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>(...) AS value)`.
    /// Output carries label columns plus a single `value` column; the eval
    /// timestamp is reattached at assembly and the metric name is dropped. NaN
    /// rows are suppressed (the UDF's "no value" sentinel).
    RateProjection,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>_over_time(...)
    /// AS value)`. Output carries label columns plus a single `value` column; the
    /// eval timestamp is reattached at assembly and NaN rows (the UDF's "no
    /// value" sentinel) are suppressed. `preserve_metric_name` keeps `__name__`
    /// only for `last_over_time`; every other family drops it, matching the
    /// interpreter's `eval_over_time_call`.
    OverTimeProjection { preserve_metric_name: bool },
    /// `<inner> -> Aggregate -> Projection(group_labels..., agg AS value)`.
    /// Output carries exactly the grouping label columns plus `value`. The
    /// result labelset is the grouping labels read directly from the batch (no
    /// fingerprint lookup), and the eval timestamp is reattached at assembly.
    Aggregate,
    /// `<leaf over already-evaluated inner vector> -> Projection(labels...,
    /// prom_<fn>([bounds...,] value) AS value)`. Output carries the
    /// metadata-free label columns plus a single `value` column; the metric name
    /// is already dropped at the leaf. The labelset is read directly from the
    /// batch and the eval timestamp is reattached at assembly. Unlike the
    /// rate/`*_over_time` shapes, **every** row is kept (no NaN suppression):
    /// `f(NaN)` / `sqrt(-1)` render as `NaN`, matching the interpreter, which
    /// keeps every float sample.
    ScalarMath,
}

/// A planned instant-query result. Produced by the recursive
/// `PromqlEngine::plan_instant_expr` and consumed by
/// `PromqlEngine::assemble_planned_instant`.
///
/// Most shapes lower to a `DataFusion` `LogicalPlan` over the custom operators
/// (`PlannedInstant::Operator`). The label-rewrite / ordering functions
/// (`label_replace`/`label_join`/`sort`/`sort_desc`) instead transform their
/// already-assembled inner instant vector in pure Rust, so they carry the
/// finished samples directly (`PlannedInstant::Precomputed`); no operator plan
/// is executed for them.
pub(super) enum PlannedInstant {
    /// An executable operator plan plus the metadata its shape's assembler needs.
    /// Boxed to keep the enum small (the operator payload carries a
    /// `SessionContext` and a `LogicalPlan`).
    Operator(Box<OperatorInstant>),
    /// A fully-assembled instant vector produced by a label-rewrite / ordering
    /// transform over a recursively-planned inner vector. Returned to the caller
    /// verbatim - there is no operator plan to execute.
    Precomputed(Vec<InstantSample>),
    /// A fully-computed **scalar** result. Carried by the scalar-returning utility
    /// functions (`time`/`pi`/`scalar`, the argless calendar forms) and any
    /// scalar∘scalar binary fold that the planner resolves in pure Rust. Assembled
    /// into a `QueryResult::Scalar` verbatim - there is no operator plan to
    /// execute. The `ts_ms`/`value` mirror exactly what the interpreter would
    /// return for the same expression, so the two paths are parity-exact.
    PrecomputedScalar { ts_ms: i64, value: f64 },
    /// A fully-computed **string** result. Carried by a top-level string literal.
    /// Assembled into a `QueryResult::Str` verbatim - there is no operator plan to
    /// execute. Mirrors exactly what the interpreter returns for the same literal.
    PrecomputedString { ts_ms: i64, value: String },
    /// A fully-materialized **range vector** (range matrix). Carried by a
    /// top-level raw matrix selector / subquery, whose `query_instant` result is a
    /// `QueryResult::RangeMatrix`. Built via the interpreter's own
    /// `eval_matrix_selector` / `eval_subquery`, so the two paths are parity-exact
    /// by construction.
    PrecomputedMatrix(Vec<RangeSeries>),
}

/// The executable payload of `PlannedInstant::Operator`.
pub(super) struct OperatorInstant {
    /// Session context whose physical planner understands the custom operators
    /// (and holds the rate UDFs), with the inner leaf table registered.
    pub(super) ctx: SessionContext,
    /// The fully-lowered logical plan to execute.
    pub(super) plan: LogicalPlan,
    /// Series labels keyed by fingerprint, for the selector/rate shapes' result
    /// assembly. The aggregate/scalar-math shapes read labels straight from the
    /// batch and leave this empty.
    pub(super) labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
    /// How to read the output batches into an instant vector.
    pub(super) shape: InstantShape,
}

impl PlannedInstant {
    /// Wrap an executable operator plan, boxing the payload.
    pub(super) fn operator(
        ctx: SessionContext,
        plan: LogicalPlan,
        labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
        shape: InstantShape,
    ) -> Self {
        Self::Operator(Box::new(OperatorInstant {
            ctx,
            plan,
            labels_by_fp,
            shape,
        }))
    }
}

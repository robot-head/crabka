use std::collections::BTreeMap;

use crabka_blockstore::{Labels, SeriesFingerprint};
use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

use crate::result::{InstantSample, RangeSeries};

/// How a planner-path output batch carries its result value and labels.
///
/// The shared assembler `PromqlEngine::assemble_planned_instant` uses the shape
/// to read each variant's columns into an `InstantVector`.
pub(super) enum InstantShape {
    /// `SeriesDivide -> SeriesNormalize -> InstantManipulate`. The output carries
    /// label columns plus `timestamp`/`value`/`sample_timestamp`. The selected
    /// sample's true timestamp stays in `sample_timestamp`. The assembler
    /// recovers the result labels from `labels_by_fp`, keyed by the row's
    /// reconstructed fingerprint.
    Selector,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>(...) AS value)`.
    /// The output carries label columns plus a single `value` column. The
    /// assembler reattaches the eval timestamp and drops the metric name. It also
    /// suppresses NaN rows, which are the UDF's "no value" sentinel.
    RateProjection,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>_over_time(...)
    /// AS value)`. The output carries label columns plus a single `value` column.
    /// The assembler reattaches the eval timestamp and suppresses NaN rows, which
    /// are the UDF's "no value" sentinel. `preserve_metric_name` keeps `__name__`
    /// only for `last_over_time`. Every other family drops it, which matches the
    /// interpreter's `eval_over_time_call`.
    OverTimeProjection { preserve_metric_name: bool },
    /// `<inner> -> Aggregate -> Projection(group_labels..., agg AS value)`. The
    /// output carries exactly the grouping label columns plus `value`. The
    /// result label set is the grouping labels, which the assembler reads
    /// straight from the batch without a fingerprint lookup. The assembler also
    /// reattaches the eval timestamp.
    Aggregate,
    /// `<leaf over already-evaluated inner vector> -> Projection(labels...,
    /// prom_<fn>([bounds...,] value) AS value)`. The output carries the
    /// metadata-free label columns plus a single `value` column, because the leaf
    /// already dropped the metric name. The assembler reads the label set
    /// straight from the batch and reattaches the eval timestamp. This shape
    /// keeps every row and does not suppress NaN rows, unlike the rate and
    /// `*_over_time` shapes. `f(NaN)` and `sqrt(-1)` render as `NaN`, which
    /// matches the interpreter, because the interpreter keeps every float sample.
    ScalarMath,
}

/// A planned instant-query result.
///
/// The recursive `PromqlEngine::plan_instant_expr` produces this type, and
/// `PromqlEngine::assemble_planned_instant` consumes it.
///
/// Most shapes lower to a `DataFusion` `LogicalPlan` over the custom operators
/// (`PlannedInstant::Operator`). The label-rewrite and ordering functions
/// `label_replace`/`label_join`/`sort`/`sort_desc` instead transform their
/// already-assembled inner instant vector in pure Rust, so they carry the
/// finished samples directly as `PlannedInstant::Precomputed`. No operator plan
/// runs for them.
pub(super) enum PlannedInstant {
    /// An executable operator plan plus the metadata its shape's assembler needs.
    /// The box keeps the enum small, because the operator payload carries a
    /// `SessionContext` and a `LogicalPlan`.
    Operator(Box<OperatorInstant>),
    /// A fully-assembled instant vector from a label-rewrite or ordering
    /// transform over a recursively-planned inner vector. The assembler returns
    /// it to the caller verbatim. There is no operator plan to execute.
    Precomputed(Vec<InstantSample>),
    /// A fully-computed scalar result. The scalar-returning utility functions
    /// `time`/`pi`/`scalar` and the argless calendar forms carry this variant, as
    /// does any scalar∘scalar binary fold that the planner resolves in pure Rust.
    /// The assembler turns it into a `QueryResult::Scalar` verbatim, and there is
    /// no operator plan to execute. The `ts_ms`/`value` mirror exactly what the
    /// interpreter returns for the same expression, so the two paths are
    /// parity-exact.
    PrecomputedScalar { ts_ms: i64, value: f64 },
    /// A fully-computed string result. A top-level string literal carries this
    /// variant. The assembler turns it into a `QueryResult::Str` verbatim, and
    /// there is no operator plan to execute. The value mirrors exactly what the
    /// interpreter returns for the same literal.
    PrecomputedString { ts_ms: i64, value: String },
    /// A fully-materialized range vector, also called a range matrix. A top-level
    /// raw matrix selector or subquery carries this variant, and its
    /// `query_instant` result is a `QueryResult::RangeMatrix`. The interpreter's
    /// own `eval_matrix_selector`/`eval_subquery` builds it, so the two paths are
    /// parity-exact by construction.
    PrecomputedMatrix(Vec<RangeSeries>),
}

/// The executable payload of `PlannedInstant::Operator`.
pub(super) struct OperatorInstant {
    /// Session context whose physical planner understands the custom operators.
    /// It also holds the rate UDFs and the registered inner leaf table.
    pub(super) ctx: SessionContext,
    /// The fully-lowered logical plan to execute.
    pub(super) plan: LogicalPlan,
    /// Series labels keyed by fingerprint, used when the selector and rate shapes
    /// assemble their result. The aggregate and scalar-math shapes read labels
    /// straight from the batch and leave this map empty.
    pub(super) labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
    /// How to read the output batches into an instant vector.
    pub(super) shape: InstantShape,
}

impl PlannedInstant {
    /// Wraps an executable operator plan and boxes the payload.
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

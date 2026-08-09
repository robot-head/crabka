use futures::{FutureExt, future::BoxFuture};
use promql_parser::parser::{BinaryExpr, Expr, value::ValueType};

use super::{
    PromqlEngine,
    binary::{InstantValue, combine_instant_binary},
    planned::PlannedInstant,
};
use crate::{error::Result, result::QueryResult, store::MetricStore};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plans an `Expr::Binary` onto the operator path.
    ///
    /// The binary expression is an arithmetic, comparison, or set operator. This
    /// method recurses both operands through the planner, assembles each one to
    /// an [`InstantValue`], then applies the shared combine routine
    /// [`combine_instant_binary`] in pure Rust. This method returns the result as
    /// a [`PlannedInstant::Precomputed`] vector. For a scalar∘scalar fold it
    /// returns a precomputed single-element scalar inside the vector shape, as
    /// described below.
    ///
    /// The same combine routine backs the interpreter
    /// [`Self::eval_instant_binary`], so the operator path matches Prometheus by
    /// construction for every supported form: vector∘scalar, scalar∘vector,
    /// one-to-one vector∘vector with `on`/`ignoring` and `bool`, `group_left` and
    /// `group_right` with copied labels, and the `and`/`or`/`unless` set ops.
    ///
    /// Both operands must be planner-supported. A scalar operand, one whose
    /// `value_type() == Scalar`, folds through the interpreter's pure scalar
    /// evaluation. Scalars carry no NaN-staleness subtlety, so that fold is
    /// parity-exact. A vector operand recurses [`Self::plan_instant_expr`] and is
    /// assembled with that shape's own drop semantics. The whole binary returns
    /// `None`, and the interpreter takes over, if either operand is not
    /// planner-supported, is histogram-bearing, or has a non-instant type: matrix
    /// or string. A scalar∘scalar fold returns a `Scalar` result through
    /// [`PlannedInstant::PrecomputedScalar`]. Both operands fold through the
    /// interpreter's pure scalar path, so that fold is parity-exact.
    pub(super) async fn plan_binary_expr(
        &self,
        tenant: &str,
        binary: &BinaryExpr,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let Some(lhs) = self
            .plan_binary_operand(tenant, &binary.lhs, time_ms)
            .await?
        else {
            return Ok(None);
        };
        let Some(rhs) = self
            .plan_binary_operand(tenant, &binary.rhs, time_ms)
            .await?
        else {
            return Ok(None);
        };

        match combine_instant_binary(binary, lhs, rhs, time_ms)? {
            QueryResult::InstantVector(samples) => Ok(Some(PlannedInstant::Precomputed(samples))),
            // A scalar∘scalar fold: carry the constant through the scalar planned
            // result. Both operands were folded via the interpreter's pure scalar
            // path, so this matches the interpreter exactly.
            QueryResult::Scalar { ts_ms, value } => {
                Ok(Some(PlannedInstant::PrecomputedScalar { ts_ms, value }))
            }
            // A string / matrix combine result cannot be produced by a binary op;
            // fall back defensively.
            _ => Ok(None),
        }
    }

    /// Evaluates one binary operand into an [`InstantValue`] with the planner.
    ///
    /// A scalar-typed operand folds through the interpreter's pure scalar path.
    /// That path is parity-exact, because scalars have no staleness or NaN-window
    /// subtlety. A vector-typed operand recurses [`Self::plan_instant_expr`] and
    /// is assembled to an instant vector. This method returns `None`, and the
    /// caller falls back, for a vector operand the planner does not support and
    /// for a non-instant operand type: matrix or string.
    fn plan_binary_operand<'a>(
        &'a self,
        tenant: &'a str,
        operand: &'a Expr,
        time_ms: i64,
    ) -> BoxFuture<'a, Result<Option<InstantValue>>> {
        async move {
            match operand.value_type() {
                ValueType::Scalar => {
                    let QueryResult::Scalar { value, .. } =
                        self.plan_and_resolve(tenant, operand, time_ms).await?
                    else {
                        return Ok(None);
                    };
                    Ok(Some(InstantValue::Scalar(value)))
                }
                ValueType::Vector => {
                    let Some(planned) = self.plan_instant_expr(tenant, operand, time_ms).await?
                    else {
                        return Ok(None);
                    };
                    let QueryResult::InstantVector(samples) =
                        self.assemble_planned_instant(planned, time_ms).await?
                    else {
                        return Ok(None);
                    };
                    Ok(Some(InstantValue::Vector(samples)))
                }
                ValueType::Matrix | ValueType::String => Ok(None),
            }
        }
        .boxed()
    }
}

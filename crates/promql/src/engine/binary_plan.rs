use futures::{FutureExt, future::BoxFuture};
use promql_parser::parser::{BinaryExpr, Expr, value::ValueType};

use super::{
    PromqlEngine,
    binary::{InstantValue, combine_instant_binary},
    planned::PlannedInstant,
};
use crate::{error::Result, result::QueryResult, store::MetricStore};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plan an `Expr::Binary` (arithmetic / comparison / set operator) onto the
    /// operator path: recurse both operands through the planner, assemble each to
    /// an [`InstantValue`], then apply the **shared** combine routine
    /// ([`combine_instant_binary`]) in pure Rust. The result is returned as a
    /// [`PlannedInstant::Precomputed`] vector (or, for a scalar∘scalar fold, a
    /// precomputed single-element scalar carried inside the vector shape — see
    /// below). Because the same combine routine backs the interpreter
    /// ([`Self::eval_instant_binary`]), the operator path matches Prometheus by
    /// construction for every supported form: vector∘scalar, scalar∘vector,
    /// one-to-one vector∘vector (with `on`/`ignoring` and `bool`), `group_left` /
    /// `group_right` (with copied labels), and the `and`/`or`/`unless` set ops.
    ///
    /// Both operands must be planner-supported. A scalar operand
    /// (`value_type() == Scalar`) folds via the interpreter's pure scalar
    /// evaluation — scalars carry no NaN-staleness subtlety, so this is
    /// parity-exact. A vector operand recurses [`Self::plan_instant_expr`] and is
    /// assembled (applying that shape's own drop semantics). If either operand is
    /// not planner-supported (the recurse returns `None`), histogram-bearing, or a
    /// non-instant type (matrix / string), the whole binary returns `None`
    /// (interpreter fallback). A scalar∘scalar fold yields a `Scalar` result,
    /// carried through [`PlannedInstant::PrecomputedScalar`] — both operands are
    /// folded via the interpreter's pure scalar path, so it is parity-exact.
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

    /// Evaluate one binary operand into an [`InstantValue`] via the planner.
    ///
    /// A scalar-typed operand is folded via the interpreter's pure scalar path
    /// (parity-exact — scalars have no staleness/NaN-window subtlety). A
    /// vector-typed operand recurses [`Self::plan_instant_expr`] and is assembled
    /// to an instant vector. Returns `None` (caller falls back) for a
    /// non-planner-supported vector operand or a non-instant operand type
    /// (matrix / string).
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

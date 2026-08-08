//! `ColumnarProcessor`: a batch operator that maps `DataFrame` to `0..n`
//! `DataFrame`.
//!
//! This module also holds the built-in stateless operators, which are expressed
//! as polars exprs, and the within-batch `group_by` and `agg`. The reserved
//! metadata columns, such as `__key`, survive the stateless operators. An
//! aggregation drops them or recomputes them. The items below document each
//! case.

use ::polars::prelude::*;

use super::codec::{BatchError, RESERVED_COLUMNS};

/// Sink for batches forwarded by a `ColumnarProcessor`.
pub struct ColumnarContext {
    forwarded: Vec<DataFrame>,
}

impl ColumnarContext {
    pub(crate) fn new() -> Self {
        Self {
            forwarded: Vec::new(),
        }
    }
    /// Forward a batch to this node's children.
    pub fn forward(&mut self, df: DataFrame) {
        self.forwarded.push(df);
    }
    pub(crate) fn take(&mut self) -> Vec<DataFrame> {
        std::mem::take(&mut self.forwarded)
    }
}

/// A batch operator. In v1 it is stateless across calls: one batch in, `0..n`
/// batches out.
pub trait ColumnarProcessor: Send + 'static {
    /// Process one batch and forward `0..n` output batches through `ctx`.
    ///
    /// # Errors
    /// Returns `BatchError` if the underlying polars computation fails.
    fn process(&mut self, ctx: &mut ColumnarContext, batch: DataFrame) -> Result<(), BatchError>;
}

/// The built-in operator kinds. The DSL and the graph build these.
///
/// This enum is `Clone`, so a topology can build a fresh operator instance for
/// each `run_batch`. The exprs are cheap, refcounted polars `Expr`s.
#[derive(Clone)]
pub enum BuiltinOp {
    /// `filter(predicate)`. It keeps the reserved columns.
    Filter(Expr),
    /// `select(exprs)`, a projection. The operator appends the reserved columns
    /// again, so downstream sinks can still rebuild the records.
    Select(Vec<Expr>),
    /// `with_columns(exprs)`. It adds or replaces columns and keeps the reserved
    /// columns.
    WithColumns(Vec<Expr>),
    /// The within-batch `group_by(keys).agg(aggs)`. It drops the reserved
    /// metadata columns, because the grouped frame has a new cardinality. The
    /// sink writes the grouped rows with null keys and the current timestamp,
    /// unless an agg recreates `__key`.
    GroupByAgg { keys: Vec<Expr>, aggs: Vec<Expr> },
}

impl BuiltinOp {
    /// A cheap static label for tracing. It is the operator variant name.
    fn kind_label(&self) -> &'static str {
        match self {
            BuiltinOp::Filter(_) => "filter",
            BuiltinOp::Select(_) => "select",
            BuiltinOp::WithColumns(_) => "with_columns",
            BuiltinOp::GroupByAgg { .. } => "group_by_agg",
        }
    }
}

impl ColumnarProcessor for BuiltinOp {
    #[tracing::instrument(
        name = "streams.columnar.operator",
        level = "debug",
        skip_all,
        fields(op = self.kind_label(), rows = batch.height()),
        err,
    )]
    fn process(&mut self, ctx: &mut ColumnarContext, batch: DataFrame) -> Result<(), BatchError> {
        let lf = batch.lazy();
        let out = match self {
            BuiltinOp::Filter(p) => lf.filter(p.clone()),
            BuiltinOp::Select(exprs) => {
                let mut all = exprs.clone();
                for c in RESERVED_COLUMNS {
                    all.push(col(c));
                }
                lf.select(all)
            }
            BuiltinOp::WithColumns(exprs) => lf.with_columns(exprs.clone()),
            BuiltinOp::GroupByAgg { keys, aggs } => lf.group_by(keys.clone()).agg(aggs.clone()),
        };
        let df = out.collect().map_err(|e| BatchError(e.to_string()))?;
        ctx.forward(df);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn batch() -> DataFrame {
        df!(
            "user" => ["a", "a", "b"],
            "amount" => [5_i64, 3, 9],
            "__key" => [Some(b"a".to_vec()), Some(b"a".to_vec()), Some(b"b".to_vec())],
            "__timestamp" => [1_i64, 2, 3],
            "__partition" => [0_i32, 0, 0],
            "__offset" => [0_i64, 1, 2],
        )
        .unwrap()
    }

    fn run(mut op: BuiltinOp, df: DataFrame) -> Vec<DataFrame> {
        let mut ctx = ColumnarContext::new();
        op.process(&mut ctx, df).unwrap();
        ctx.take()
    }

    #[test]
    fn filter_keeps_reserved_columns() {
        let out = run(BuiltinOp::Filter(col("amount").gt(lit(4))), batch());
        check!(out[0].height() == 2);
        check!(out[0].column("__key").is_ok());
    }

    #[test]
    fn select_reappends_reserved_columns() {
        let out = run(BuiltinOp::Select(vec![col("user")]), batch());
        check!(out[0].column("user").is_ok());
        check!(out[0].column("__timestamp").is_ok());
    }

    #[test]
    fn group_by_agg_within_batch() {
        let out = run(
            BuiltinOp::GroupByAgg {
                keys: vec![col("user")],
                aggs: vec![col("amount").sum().alias("total")],
            },
            batch(),
        );
        let df = out[0]
            .sort(["user"], SortMultipleOptions::default())
            .unwrap();
        check!(df.height() == 2);
        let totals = df.column("total").unwrap().i64().unwrap();
        check!(totals.get(0) == Some(8)); // a: 5+3
        check!(totals.get(1) == Some(9)); // b: 9
    }
}

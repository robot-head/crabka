//! `ColumnarProcessor`: a batch operator (`DataFrame -> 0..n DataFrame`), plus
//! built-in stateless operators expressed as polars exprs and within-batch
//! `group_by`/`agg`. Reserved metadata columns (`__key`, …) are preserved across
//! stateless operators and dropped/recomputed by aggregations (documented below).

use super::codec::{BatchError, RESERVED_COLUMNS};
use ::polars::prelude::*;

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

/// A batch operator. Stateless across calls in v1 (one batch in, 0..n out).
pub trait ColumnarProcessor: Send + 'static {
    /// Process one batch, forwarding `0..n` output batches via `ctx`.
    ///
    /// # Errors
    /// Returns `BatchError` if the underlying polars computation fails.
    fn process(&mut self, ctx: &mut ColumnarContext, batch: DataFrame) -> Result<(), BatchError>;
}

/// Built-in operator kinds (the DSL/graph builds these).
///
/// `Clone` so a topology can build a fresh operator instance per `run_batch`
/// (the exprs are cheap, refcounted polars `Expr`s).
#[derive(Clone)]
pub enum BuiltinOp {
    /// `filter(predicate)` — keeps reserved columns.
    Filter(Expr),
    /// `select(exprs)` — projection; reserved columns are auto-appended so
    /// downstream sinks can still reconstruct records.
    Select(Vec<Expr>),
    /// `with_columns(exprs)` — add/replace columns; keeps reserved columns.
    WithColumns(Vec<Expr>),
    /// within-batch `group_by(keys).agg(aggs)`. Drops reserved metadata columns
    /// (the grouped frame has new cardinality); the sink writes the grouped rows
    /// with null keys / current timestamp unless an agg recreates `__key`.
    GroupByAgg { keys: Vec<Expr>, aggs: Vec<Expr> },
}

impl BuiltinOp {
    /// A cheap static label for tracing (the operator variant name).
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
    use super::*;
    use assert2::check;

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

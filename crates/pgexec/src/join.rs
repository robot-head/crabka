//! SP33: joins over `Relation`s. A `Relation` is a `Scope` (ordered schema) plus
//! its materialized rows. Base tables, joins, and derived subqueries all produce
//! one. This module is pure relational algebra over already-fetched rows, with
//! no kv or catalog access, so hand-built relations can unit-test it. See the
//! SP33 design doc for why this single-range pure fold warrants no model.
//!
//! An equality-constrained join probes a hash index over the right relation
//! instead of a walk of it per left row, so a 10k-row self-join costs 10k
//! predicate evaluations and not 100M. Such a join is `USING`/`NATURAL`, or an
//! `ON` whose top-level conjuncts include `left.col = right.col`. Everything
//! else still folds as a nested loop, as does any key whose values are not
//! exactly hash-comparable.

use std::collections::HashMap;

use crabka_pgparser::ast::{BinaryOp, Expr, JoinConstraint, JoinKind};
use crabka_pgtypes::Datum;

use crate::{
    error::ExecError,
    scope::{ColumnBinding, Scope},
};

/// A materialized relation: an ordered `Scope` (the schema) plus its rows, each
/// row positionally aligned to `scope.columns`. Base tables, joins, and, later,
/// derived subqueries all produce one.
#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub scope: Scope,
    pub rows: Vec<Vec<Datum>>,
}

/// Join two relations under `kind` and `constraint`, and return the combined
/// relation. `ctx` carries the session zone and clock that evaluate an `ON`
/// predicate with temporal expressions in it. A USING/NATURAL/CROSS join, or a
/// rows-free schema join, never touches `ctx`.
pub(crate) fn join_relations(
    left: Relation,
    right: Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    blocking_query_memory: crabka_units::ByteSize,
) -> Result<Relation, ExecError> {
    use std::cmp::Ordering;

    // Self-join / duplicate alias: a qualifier may not appear on both sides.
    for c in &right.scope.columns {
        if let Some(q) = &c.qualifier
            && left
                .scope
                .columns
                .iter()
                .any(|lc| lc.qualifier.as_ref() == Some(q))
        {
            return Err(ExecError::DuplicateAlias(q.clone()));
        }
    }

    // Combined schema (left ++ right): the ON-predicate evaluation scope and the
    // pre-reshape output schema.
    let mut combined_scope = left.scope.clone();
    combined_scope
        .columns
        .extend(right.scope.columns.iter().cloned());

    // USING/NATURAL -> the join columns and their (left_idx, right_idx) pairs; a
    // column must exist on BOTH sides (else 42703/42702 via `resolve`). NATURAL
    // with no common column has empty pairs and degenerates to a cross join.
    let join_cols: Vec<String> = match constraint {
        JoinConstraint::Using(cols) => cols.clone(),
        JoinConstraint::Natural => natural_common_columns(&left.scope, &right.scope),
        JoinConstraint::On(_) | JoinConstraint::None => Vec::new(),
    };
    let mut pairs: Vec<(usize, usize)> = Vec::with_capacity(join_cols.len());
    for jc in &join_cols {
        let li = left.scope.resolve(None, jc)?;
        let ri = right.scope.resolve(None, jc)?;
        pairs.push((li, ri));
    }
    let on_pred: Option<&Expr> = match constraint {
        JoinConstraint::On(e) => Some(e),
        _ => None,
    };

    let lw = left.scope.width();
    let matches = |lrow: &[Datum], rrow: &[Datum]| -> Result<bool, ExecError> {
        // USING/NATURAL: every join-column pair must compare Equal (NULL never matches).
        if !pairs.is_empty() {
            for (li, ri) in &pairs {
                if crabka_pgtypes::ops::compare(&lrow[*li], &rrow[*ri])? != Some(Ordering::Equal) {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        // ON(expr) over the combined row; CROSS/comma (no predicate) always matches.
        let Some(e) = on_pred else { return Ok(true) };
        let mut combined = lrow.to_vec();
        combined.extend_from_slice(rrow);
        match crate::eval::eval(e, &combined_scope, &combined, ctx)? {
            Datum::Bool(b) => Ok(b),
            Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "JOIN/ON condition must be boolean".into(),
            )),
        }
    };

    // The equality columns the ON predicate (or USING/NATURAL) requires, used to
    // skip right rows that cannot match. `matches` still decides every candidate,
    // so extra conjuncts and the NULL rules are unaffected.
    let equi_keys = if pairs.is_empty() {
        on_pred.map_or_else(Vec::new, |e| equi_key_columns(e, &combined_scope, lw))
    } else {
        pairs.clone()
    };
    let index = EquiIndex::build(&left, &right, &equi_keys);
    // Every right row, for the rows the index cannot narrow.
    let all_right: Vec<usize> = if index.is_some() {
        Vec::new()
    } else {
        (0..right.rows.len()).collect()
    };
    let mut key_buf: Vec<Datum> = Vec::with_capacity(equi_keys.len());

    let mut rows = Vec::new();
    let mut result_bytes = 0usize;
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            for l in &left.rows {
                for &ri in candidate_rows(index.as_ref(), &all_right, l, &mut key_buf) {
                    let r = &right.rows[ri];
                    if matches(l, r)? {
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        push_bounded_join_row(
                            &mut rows,
                            &mut result_bytes,
                            row,
                            blocking_query_memory,
                        )?;
                    }
                }
            }
        }
        JoinKind::Left | JoinKind::Right | JoinKind::Full => {
            let rw = right.scope.width();
            let want_left = matches!(kind, JoinKind::Left | JoinKind::Full);
            let want_right = matches!(kind, JoinKind::Right | JoinKind::Full);
            let mut right_matched = vec![false; right.rows.len()];
            for l in &left.rows {
                let mut any = false;
                for &ri in candidate_rows(index.as_ref(), &all_right, l, &mut key_buf) {
                    let r = &right.rows[ri];
                    if matches(l, r)? {
                        any = true;
                        right_matched[ri] = true;
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        push_bounded_join_row(
                            &mut rows,
                            &mut result_bytes,
                            row,
                            blocking_query_memory,
                        )?;
                    }
                }
                if !any && want_left {
                    let mut row = l.clone();
                    row.extend(vec![Datum::Null; rw]);
                    push_bounded_join_row(
                        &mut rows,
                        &mut result_bytes,
                        row,
                        blocking_query_memory,
                    )?;
                }
            }
            if want_right {
                for (ri, r) in right.rows.iter().enumerate() {
                    if !right_matched[ri] {
                        let mut row = vec![Datum::Null; lw];
                        row.extend(r.iter().cloned());
                        push_bounded_join_row(
                            &mut rows,
                            &mut result_bytes,
                            row,
                            blocking_query_memory,
                        )?;
                    }
                }
            }
        }
    }

    // USING/NATURAL: coalesce + reorder the join columns. Otherwise the combined
    // left ++ right schema is the result.
    if pairs.is_empty() {
        Ok(Relation {
            scope: combined_scope,
            rows,
        })
    } else {
        Ok(coalesce_join_columns(
            &left.scope,
            &right.scope,
            &pairs,
            &join_cols,
            rows,
        ))
    }
}

/// The right rows a left row could possibly join with: the index's bucket for
/// its key, or every right row when no usable index exists.
fn candidate_rows<'a>(
    index: Option<&'a EquiIndex>,
    all_right: &'a [usize],
    lrow: &[Datum],
    key_buf: &mut Vec<Datum>,
) -> &'a [usize] {
    match index {
        Some(index) => index.candidates(lrow, key_buf),
        None => all_right,
    }
}

/// Right-relation row indices grouped by their equality key, ascending within
/// each bucket so a probe visits candidates in the order the nested loop would.
///
/// `Datum`'s hash equality agrees with `ops::compare` only for values of the
/// SAME variant. `Int2(1)` and `Int4(1)` compare Equal but hash apart, and so do
/// `Int4(1)` and `Numeric(1)`. So the index exists only when every non-NULL
/// value of a key column, on BOTH sides, carries one variant. Anything
/// else leaves the join as a nested-loop fold, which is always correct.
struct EquiIndex {
    /// Key columns, as indices into a left row.
    left_key: Vec<usize>,
    buckets: HashMap<Vec<Datum>, Vec<usize>>,
}

/// What a key column's values look like on one side of the join.
enum KeyVariant<'a> {
    /// Every non-NULL value carries the variant of this sample.
    Uniform(&'a Datum),
    /// No non-NULL value at all. Nothing can match through this column, and the
    /// index represents that faithfully, because a NULL key is never bucketed.
    AllNull,
    /// Values of differing variants, whose hash equality would not agree with
    /// `ops::compare`.
    Mixed,
}

impl EquiIndex {
    /// Below this many left×right pairs the nested loop is already cheap enough
    /// that the buckets would cost more to build than the probes save.
    const MIN_PAIRS: usize = 4096;

    /// `keys` are `(left_column, right_column)` pairs the predicate requires to
    /// compare Equal. Returns `None` when no index applies.
    fn build(left: &Relation, right: &Relation, keys: &[(usize, usize)]) -> Option<Self> {
        if keys.is_empty() || left.rows.len().saturating_mul(right.rows.len()) < Self::MIN_PAIRS {
            return None;
        }
        for (li, ri) in keys {
            match (key_variant(&left.rows, *li), key_variant(&right.rows, *ri)) {
                (KeyVariant::Mixed, _) | (_, KeyVariant::Mixed) => return None,
                (KeyVariant::Uniform(l), KeyVariant::Uniform(r))
                    if std::mem::discriminant(l) != std::mem::discriminant(r)
                        || !hashes_like_it_compares(l) =>
                {
                    return None;
                }
                (KeyVariant::Uniform(_), KeyVariant::Uniform(_)) => {}
                // With no non-NULL value on one side, no probe can hit a bucket
                // whatever the other side holds — which is the right answer.
                _ => {}
            }
        }
        let mut buckets: HashMap<Vec<Datum>, Vec<usize>> = HashMap::new();
        for (ri, row) in right.rows.iter().enumerate() {
            // A NULL in the key never compares Equal, so the row is not indexed
            // and simply falls out as unmatched.
            if keys.iter().any(|(_, rc)| row[*rc].is_null()) {
                continue;
            }
            let key: Vec<Datum> = keys.iter().map(|(_, rc)| row[*rc].clone()).collect();
            buckets.entry(key).or_default().push(ri);
        }
        Some(Self {
            left_key: keys.iter().map(|(lc, _)| *lc).collect(),
            buckets,
        })
    }

    fn candidates<'a>(&'a self, lrow: &[Datum], key_buf: &mut Vec<Datum>) -> &'a [usize] {
        key_buf.clear();
        for &lc in &self.left_key {
            if lrow[lc].is_null() {
                return &[];
            }
            key_buf.push(lrow[lc].clone());
        }
        match self.buckets.get(key_buf.as_slice()) {
            Some(rows) => rows,
            None => &[],
        }
    }
}

fn key_variant(rows: &[Vec<Datum>], column: usize) -> KeyVariant<'_> {
    let mut seen: Option<&Datum> = None;
    for row in rows {
        let value = &row[column];
        if value.is_null() {
            continue;
        }
        match seen {
            None => seen = Some(value),
            Some(sample) if std::mem::discriminant(sample) == std::mem::discriminant(value) => {}
            Some(_) => return KeyVariant::Mixed,
        }
    }
    seen.map_or(KeyVariant::AllNull, KeyVariant::Uniform)
}

/// Whether `Datum`'s `Eq`/`Hash` decide this variant exactly as `ops::compare`
/// does, which is what lets a hash bucket stand in for the comparison.
///
/// The scalar types agree by construction, because `Eq` and `Hash` both
/// canonicalize NaN, signed zero, and numeric scale the way `compare` orders
/// them. The composite types do not agree. `array_cmp` ignores the element type,
/// so `int4[]` `{1}` and `int8[]` `{1}` compare Equal while `Eq` calls them
/// different, and `interval` compares by a canonical estimate. Those keys keep
/// the nested loop.
fn hashes_like_it_compares(sample: &Datum) -> bool {
    matches!(
        sample,
        Datum::Bool(_)
            | Datum::Int2(_)
            | Datum::Int4(_)
            | Datum::Int8(_)
            | Datum::Float4(_)
            | Datum::Float8(_)
            | Datum::Numeric(_)
            | Datum::Text(_)
            | Datum::Bytea(_)
            | Datum::Date(_)
            | Datum::Time(_)
            | Datum::Timestamp(_)
            | Datum::Timestamptz(_)
    )
}

/// The `(left_column, right_column)` pairs an `ON` predicate requires to compare
/// Equal: its top-level `AND` conjuncts of the form `l.col = r.col`, with one
/// side resolving into the left relation and the other into the right.
///
/// These are necessary conditions for the predicate to hold, which is what makes
/// them safe as a pre-filter. The full predicate still decides every candidate.
/// Conjuncts of any other shape contribute no key, including `OR`, which is not
/// a necessary condition.
fn equi_key_columns(pred: &Expr, combined: &Scope, lw: usize) -> Vec<(usize, usize)> {
    let mut keys = Vec::new();
    collect_equi_key_columns(pred, combined, lw, &mut keys);
    keys
}

fn collect_equi_key_columns(
    pred: &Expr,
    combined: &Scope,
    lw: usize,
    out: &mut Vec<(usize, usize)>,
) {
    match pred {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            collect_equi_key_columns(left, combined, lw, out);
            collect_equi_key_columns(right, combined, lw, out);
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            let (Some(a), Some(b)) = (
                combined_column_index(left, combined),
                combined_column_index(right, combined),
            ) else {
                return;
            };
            match (a < lw, b < lw) {
                (true, false) => out.push((a, b - lw)),
                (false, true) => out.push((b, a - lw)),
                _ => {}
            }
        }
        _ => {}
    }
}

/// The combined-scope position of a bare column reference, or `None` for any
/// other expression. That includes an expression that does not resolve, or that
/// resolves ambiguously. `matches` reports that error when it evaluates the
/// predicate.
fn combined_column_index(expr: &Expr, combined: &Scope) -> Option<usize> {
    let Expr::Column { table, name } = expr else {
        return None;
    };
    combined.resolve(table.as_deref(), name).ok()
}

fn push_bounded_join_row(
    rows: &mut Vec<Vec<Datum>>,
    used: &mut usize,
    row: Vec<Datum>,
    blocking_query_memory: crabka_units::ByteSize,
) -> Result<(), ExecError> {
    let bytes = crate::scanner::datum_row_bytes(&row);
    if crate::scanner::exceeds_query_memory(used.saturating_add(bytes), blocking_query_memory) {
        return Err(crate::scanner::memory_budget_exceeded());
    }
    *used += bytes;
    rows.push(row);
    Ok(())
}

/// The column names common to both scopes, matched by name, in left order, and
/// deduplicated. This drives `NATURAL JOIN`'s join-column set. An empty set
/// degenerates to a cross join, as PostgreSQL does.
fn natural_common_columns(left: &Scope, right: &Scope) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in &left.columns {
        if right.columns.iter().any(|rc| rc.name == c.name) && !out.contains(&c.name) {
            out.push(c.name.clone());
        }
    }
    out
}

/// Reshape a `left ++ right` combined relation into PostgreSQL's USING/NATURAL
/// output. Each join column appears ONCE, coalesced so that the present side
/// wins, which matters for outer joins. Each join column is unqualified and sits
/// FIRST in `join` order. The remaining left columns follow, and then the
/// remaining right columns.
fn coalesce_join_columns(
    left_scope: &Scope,
    right_scope: &Scope,
    pairs: &[(usize, usize)], // (left_idx, right_idx) per join column, in join order
    join_names: &[String],
    rows: Vec<Vec<Datum>>, // combined left ++ right rows
) -> Relation {
    let lw = left_scope.width();
    let left_join: Vec<usize> = pairs.iter().map(|(li, _)| *li).collect();
    let right_join: Vec<usize> = pairs.iter().map(|(_, ri)| *ri).collect();

    // New schema: merged join cols (unqualified), then non-join left, then non-join right.
    // The merged column takes the LEFT side's type. USING/NATURAL keys are the same
    // type on both sides in this slice's tested surface; PG unifies left/right types
    // for a mixed-width key (e.g. `int4` USING `int8`) — that unification is deferred.
    let mut columns: Vec<ColumnBinding> = Vec::new();
    for ((li, _ri), name) in pairs.iter().zip(join_names) {
        columns.push(ColumnBinding {
            qualifier: None,
            name: name.clone(),
            ty: left_scope.ty_at(*li),
        });
    }
    for (i, c) in left_scope.columns.iter().enumerate() {
        if !left_join.contains(&i) {
            columns.push(c.clone());
        }
    }
    for (i, c) in right_scope.columns.iter().enumerate() {
        if !right_join.contains(&i) {
            columns.push(c.clone());
        }
    }
    let scope = Scope { columns };

    let new_rows = rows
        .into_iter()
        .map(|row| {
            let mut out: Vec<Datum> = Vec::with_capacity(scope.width());
            // Coalesced join columns (left value unless NULL, else right value).
            for (li, ri) in pairs {
                let lv = &row[*li];
                out.push(if lv.is_null() {
                    row[lw + *ri].clone()
                } else {
                    lv.clone()
                });
            }
            // Remaining left columns.
            for (i, val) in row[..lw].iter().enumerate() {
                if !left_join.contains(&i) {
                    out.push(val.clone());
                }
            }
            // Remaining right columns.
            for (i, val) in row[lw..].iter().enumerate() {
                if !right_join.contains(&i) {
                    out.push(val.clone());
                }
            }
            out
        })
        .collect();
    Relation {
        scope,
        rows: new_rows,
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::ColumnType;

    use super::*;

    /// A default (UTC/epoch) eval context. These pure relational-algebra tests
    /// use no temporal ON predicate, so the zone never affects the result.
    fn tctx() -> crate::clock::EvalCtx {
        crate::clock::EvalCtx::test_default()
    }

    fn join_relations(
        left: Relation,
        right: Relation,
        kind: JoinKind,
        constraint: &JoinConstraint,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<Relation, ExecError> {
        super::join_relations(
            left,
            right,
            kind,
            constraint,
            ctx,
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
    }

    fn rel(qual: &str, cols: &[&str], rows: Vec<Vec<i32>>) -> Relation {
        let scope = Scope {
            columns: cols
                .iter()
                .map(|n| crate::scope::ColumnBinding {
                    qualifier: Some(qual.into()),
                    name: (*n).into(),
                    ty: ColumnType::Int4,
                })
                .collect(),
        };
        Relation {
            scope,
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(Datum::Int4).collect())
                .collect(),
        }
    }

    fn on_eq(lq: &str, lc: &str, rq: &str, rc: &str) -> JoinConstraint {
        JoinConstraint::On(Expr::Binary {
            op: crabka_pgparser::ast::BinaryOp::Eq,
            left: Box::new(Expr::Column {
                table: Some(lq.into()),
                name: lc.into(),
            }),
            right: Box::new(Expr::Column {
                table: Some(rq.into()),
                name: rc.into(),
            }),
        })
    }

    #[test]
    fn inner_join_keeps_only_matches() {
        let a = rel("a", &["id"], vec![vec![1], vec![2], vec![3]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3], vec![4]]);
        let j = join_relations(a, b, JoinKind::Inner, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("join");
        assert_eq!(
            j.rows,
            vec![
                vec![Datum::Int4(2), Datum::Int4(2)],
                vec![Datum::Int4(3), Datum::Int4(3)]
            ]
        );
    }

    #[test]
    fn cross_join_is_the_product() {
        let a = rel("a", &["x"], vec![vec![1], vec![2]]);
        let b = rel("b", &["y"], vec![vec![9]]);
        let j = join_relations(a, b, JoinKind::Cross, &JoinConstraint::None, &tctx())
            .expect("cross join");
        assert_eq!(j.rows.len(), 2);
        assert_eq!(j.scope.width(), 2);
    }

    #[test]
    fn cross_join_rejects_result_before_memory_budget_is_crossed() {
        let scope = |qualifier: &str| Scope {
            columns: vec![crate::scope::ColumnBinding {
                qualifier: Some(qualifier.into()),
                name: "value".into(),
                ty: ColumnType::Text,
            }],
        };
        let wide = "x".repeat(5 * 1024 * 1024);
        let left = Relation {
            scope: scope("l"),
            rows: vec![vec![Datum::Text(wide.clone())], vec![Datum::Text(wide)]],
        };
        let right = Relation {
            scope: scope("r"),
            rows: vec![vec![Datum::Text("a".into())], vec![Datum::Text("b".into())]],
        };

        let error = join_relations(left, right, JoinKind::Cross, &JoinConstraint::None, &tctx())
            .expect_err("join output must respect blocking memory budget");

        assert_eq!(error.into_pg().code, "53200");
    }

    #[test]
    fn left_join_null_extends_unmatched_left_rows() {
        let a = rel("a", &["id"], vec![vec![1], vec![2], vec![3]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Left, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("left join");
        // id=1 has no match -> (1, NULL); 2,3 match.
        assert!(j.rows.contains(&vec![Datum::Int4(1), Datum::Null]));
        assert_eq!(j.rows.len(), 3);
    }

    #[test]
    fn right_join_null_extends_unmatched_right_rows() {
        let a = rel("a", &["id"], vec![vec![2]]);
        let b = rel("b", &["id"], vec![vec![1], vec![2]]);
        let j = join_relations(a, b, JoinKind::Right, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("right join");
        assert!(j.rows.contains(&vec![Datum::Null, Datum::Int4(1)]));
        assert_eq!(j.rows.len(), 2);
    }

    #[test]
    fn full_join_keeps_unmatched_from_both_sides() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Full, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("full join");
        assert!(j.rows.contains(&vec![Datum::Int4(1), Datum::Null])); // unmatched left
        assert!(j.rows.contains(&vec![Datum::Null, Datum::Int4(3)])); // unmatched right
        assert!(j.rows.contains(&vec![Datum::Int4(2), Datum::Int4(2)])); // matched
        assert_eq!(j.rows.len(), 3);
    }

    #[test]
    fn using_join_coalesces_the_column_first_and_unqualified() {
        let a = rel("a", &["id", "av"], vec![vec![1, 10], vec![2, 20]]);
        let b = rel("b", &["id", "bv"], vec![vec![2, 200], vec![3, 300]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Inner,
            &JoinConstraint::Using(vec!["id".into()]),
            &tctx(),
        )
        .expect("using");
        // Output schema: merged unqualified `id` first, then a.av, then b.bv.
        assert_eq!(j.scope.columns[0].qualifier, None);
        assert_eq!(j.scope.columns[0].name, "id");
        assert_eq!(
            j.scope
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
            vec!["id", "av", "bv"]
        );
        assert_eq!(
            j.rows,
            vec![vec![Datum::Int4(2), Datum::Int4(20), Datum::Int4(200)]]
        );
    }

    #[test]
    fn natural_join_uses_all_common_columns() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Inner, &JoinConstraint::Natural, &tctx())
            .expect("natural");
        assert_eq!(j.scope.columns.len(), 1); // single merged `id`
        assert_eq!(j.rows, vec![vec![Datum::Int4(2)]]);
    }

    #[test]
    fn left_join_using_coalesces_unmatched_to_left_value() {
        // LEFT JOIN USING: an unmatched left row keeps its own join-key value (the
        // right side is NULL, so COALESCE picks the left).
        let a = rel("a", &["id", "av"], vec![vec![1, 10], vec![2, 20]]);
        let b = rel("b", &["id", "bv"], vec![vec![2, 200]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Left,
            &JoinConstraint::Using(vec!["id".into()]),
            &tctx(),
        )
        .expect("left using");
        // rows: id=1 unmatched -> (1, 10, NULL); id=2 matched -> (2, 20, 200).
        assert!(
            j.rows
                .contains(&vec![Datum::Int4(1), Datum::Int4(10), Datum::Null])
        );
        assert!(
            j.rows
                .contains(&vec![Datum::Int4(2), Datum::Int4(20), Datum::Int4(200)])
        );
        assert_eq!(j.rows.len(), 2);
    }

    /// A relation whose single key column holds `keys`, with `None` for NULL.
    fn keyed(qualifier: &str, keys: &[Option<i32>]) -> Relation {
        Relation {
            scope: Scope {
                columns: vec![crate::scope::ColumnBinding {
                    qualifier: Some(qualifier.into()),
                    name: "k".into(),
                    ty: ColumnType::Int4,
                }],
            },
            rows: keys
                .iter()
                .map(|k| vec![k.map_or(Datum::Null, Datum::Int4)])
                .collect(),
        }
    }

    /// An independent double loop over the same inputs. This is the answer the
    /// indexed probe has to reproduce exactly, in rows and in order.
    fn reference_join(left: &Relation, right: &Relation, kind: JoinKind) -> Vec<Vec<Datum>> {
        let matches = |l: &Datum, r: &Datum| !l.is_null() && !r.is_null() && l == r;
        let mut rows: Vec<Vec<Datum>> = Vec::new();
        let mut right_matched = vec![false; right.rows.len()];
        for l in &left.rows {
            let mut any = false;
            for (ri, r) in right.rows.iter().enumerate() {
                if matches(&l[0], &r[0]) {
                    any = true;
                    right_matched[ri] = true;
                    rows.push(vec![l[0].clone(), r[0].clone()]);
                }
            }
            if !any && matches!(kind, JoinKind::Left | JoinKind::Full) {
                rows.push(vec![l[0].clone(), Datum::Null]);
            }
        }
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            for (ri, r) in right.rows.iter().enumerate() {
                if !right_matched[ri] {
                    rows.push(vec![Datum::Null, r[0].clone()]);
                }
            }
        }
        rows
    }

    /// The indexed probe is an optimization, not a semantic change. Over a
    /// relation pair big enough to take it, with duplicate keys, NULLs, and
    /// unmatched rows on both sides, every join kind returns exactly what the
    /// double loop returns, in the same order.
    #[test]
    fn indexed_equi_join_agrees_with_the_nested_loop() {
        let left_keys: Vec<Option<i32>> =
            (0..120i32).map(|i| (i % 7 != 0).then_some(i % 5)).collect();
        let right_keys: Vec<Option<i32>> = (0..120i32)
            .map(|i| (i % 11 != 0).then_some(i % 6))
            .collect();
        // Big enough that `EquiIndex::build` engages rather than declining.
        assert2::assert!(left_keys.len() * right_keys.len() >= EquiIndex::MIN_PAIRS);

        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
        ] {
            let left = keyed("a", &left_keys);
            let right = keyed("b", &right_keys);
            let expected = reference_join(&left, &right, kind);
            let actual = join_relations(left, right, kind, &on_eq("a", "k", "b", "k"), &tctx())
                .expect("join")
                .rows;
            assert2::assert!(actual == expected, "{kind:?}");
        }
    }

    /// Key columns of different `Datum` variants must NOT be indexed: `Int4(1)`
    /// and `Int8(1)` compare Equal but hash apart, so bucketing them would lose
    /// matches. The nested loop still finds them.
    #[test]
    fn mixed_key_variants_still_join_through_the_nested_loop() {
        let left = keyed("a", &(0..80i32).map(Some).collect::<Vec<_>>());
        let right = Relation {
            scope: Scope {
                columns: vec![crate::scope::ColumnBinding {
                    qualifier: Some("b".into()),
                    name: "k".into(),
                    ty: ColumnType::Int8,
                }],
            },
            rows: (0..80i64).map(|i| vec![Datum::Int8(i)]).collect(),
        };
        assert2::assert!(EquiIndex::build(&left, &right, &[(0, 0)]).is_none());

        let joined = join_relations(
            left,
            right,
            JoinKind::Inner,
            &on_eq("a", "k", "b", "k"),
            &tctx(),
        )
        .expect("join")
        .rows;
        assert2::assert!(joined.len() == 80);
    }

    /// The point of the index: an equi-join on a unique key visits ONE right row
    /// per left row instead of the whole right relation. Without it a 10k-row
    /// self-join evaluates the ON predicate 100 million times and never answers.
    /// `pg_regress`'s `join` corpus does exactly that self-join.
    #[test]
    fn indexed_equi_join_visits_one_candidate_per_left_row() {
        let keys: Vec<Option<i32>> = (0..10_000i32).map(Some).collect();
        let left = keyed("a", &keys);
        let right = keyed("b", &keys);
        let index = EquiIndex::build(&left, &right, &[(0, 0)]).expect("unique int4 key is indexed");

        let mut key_buf = Vec::new();
        let visited: usize = left
            .rows
            .iter()
            .map(|row| index.candidates(row, &mut key_buf).len())
            .sum();
        assert2::assert!(visited == left.rows.len());
    }
}

//! SP33: nested-loop joins over `Relation`s. A `Relation` is a `Scope` (ordered
//! schema) plus its materialized rows; base tables, joins, and derived subqueries
//! all produce one. This module is pure relational algebra over already-fetched
//! rows — no kv/catalog access — so it is unit-testable with hand-built relations.
//! (See the SP33 design doc for why this single-range pure fold warrants no model.)

use crabka_pgparser::ast::{Expr, JoinConstraint, JoinKind};
use crabka_pgtypes::Datum;

use crate::{
    error::ExecError,
    scope::{ColumnBinding, Scope},
};

/// A materialized relation: an ordered `Scope` (the schema) plus its rows, each
/// row positionally aligned to `scope.columns`. Base tables, joins, and (later)
/// derived subqueries all produce one.
#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub scope: Scope,
    pub rows: Vec<Vec<Datum>>,
}

/// Join two relations under `kind` + `constraint`, returning the combined
/// relation. `ctx` carries the session zone + clock used to evaluate an `ON`
/// predicate that contains temporal expressions (a USING/NATURAL/CROSS join, or a
/// rows-free schema join, never touches it).
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

    let mut rows = Vec::new();
    let mut result_bytes = 0usize;
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            for l in &left.rows {
                for r in &right.rows {
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
                for (ri, r) in right.rows.iter().enumerate() {
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

/// The column names common to both scopes (matched by name), in left order,
/// deduplicated. Drives `NATURAL JOIN`'s join-column set (empty => degenerates to
/// a cross join, per PostgreSQL).
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
/// output: each join column appears ONCE (coalesced — the present side wins, which
/// matters for outer joins), unqualified, positioned FIRST in `join` order; then
/// the remaining left columns, then the remaining right columns.
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

    /// A default (UTC/epoch) eval context — these pure relational-algebra tests use
    /// no temporal ON predicate, so the zone never affects the result.
    fn tctx() -> crate::clock::EvalCtx {
        crate::clock::EvalCtx::test_default()
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
        let j = join_relations(
            a,
            b,
            JoinKind::Inner,
            &on_eq("a", "id", "b", "id"),
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
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
        let j = join_relations(
            a,
            b,
            JoinKind::Cross,
            &JoinConstraint::None,
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
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

        let error = join_relations(
            left,
            right,
            JoinKind::Cross,
            &JoinConstraint::None,
            &tctx(),
            crabka_units::bytes(1),
        )
        .expect_err("join output must respect blocking memory budget");

        assert_eq!(error.into_pg().code, "53200");
    }

    #[test]
    fn left_join_null_extends_unmatched_left_rows() {
        let a = rel("a", &["id"], vec![vec![1], vec![2], vec![3]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Left,
            &on_eq("a", "id", "b", "id"),
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("left join");
        // id=1 has no match -> (1, NULL); 2,3 match.
        assert!(j.rows.contains(&vec![Datum::Int4(1), Datum::Null]));
        assert_eq!(j.rows.len(), 3);
    }

    #[test]
    fn right_join_null_extends_unmatched_right_rows() {
        let a = rel("a", &["id"], vec![vec![2]]);
        let b = rel("b", &["id"], vec![vec![1], vec![2]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Right,
            &on_eq("a", "id", "b", "id"),
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("right join");
        assert!(j.rows.contains(&vec![Datum::Null, Datum::Int4(1)]));
        assert_eq!(j.rows.len(), 2);
    }

    #[test]
    fn full_join_keeps_unmatched_from_both_sides() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Full,
            &on_eq("a", "id", "b", "id"),
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
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
            crate::scanner::BLOCKING_QUERY_MEMORY,
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
        let j = join_relations(
            a,
            b,
            JoinKind::Inner,
            &JoinConstraint::Natural,
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
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
            crate::scanner::BLOCKING_QUERY_MEMORY,
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
}

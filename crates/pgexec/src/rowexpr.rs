//! Row constructors: `ROW(a, b)` and the bare parenthesised `(a, b)`.
//!
//! A row reaching an ordinary value position becomes a [`Datum::Record`] of the
//! anonymous `record` type, OID 2249, with `PostgreSQL`'s `f1`…`fn` field names.
//! This module still takes the *row-wise* operations field by field, before it
//! packages the row. Those operations are comparison, `IN`, `IS [NOT] NULL`, and
//! `IS [NOT] DISTINCT FROM`. Their three-valued semantics differ from the
//! composite-value operators of the same spelling: `ROW(1,NULL) < ROW(1,2)` is
//! NULL, while the same comparison between two composite *values* is `false`.

use std::cmp::Ordering;

use crabka_pgparser::ast::{BinaryOp, Expr};
use crabka_pgtypes::{ColumnType, Datum, RecordValue, ops};

use crate::{error::ExecError, scope::Scope};

/// Type-check every field operator selected by a row comparison.
///
/// The row itself has the anonymous `record` type, so scalar operator analysis
/// cannot see an unsupported field type unless each pair is checked here.
pub(crate) fn validate_comparison(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &Scope,
) -> Result<(), ExecError> {
    let (Expr::Row(left), Expr::Row(right)) = (left, right) else {
        return Ok(());
    };
    if left.len() != right.len() {
        return Err(ExecError::Syntax(
            "unequal number of entries in row expressions".into(),
        ));
    }
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::IsDistinctFrom
            | BinaryOp::IsNotDistinctFrom
            | BinaryOp::Overlaps
    ) {
        return Ok(());
    }
    for (left, right) in left.iter().zip(right) {
        validate_comparison(op, left, right, scope)?;
        let field_op = if matches!(op, BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) {
            BinaryOp::Eq
        } else {
            op
        };
        if let Some(error) = crate::eval::comparison_mismatch(
            &Expr::Binary {
                op: field_op,
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
            },
            scope,
        ) {
            return Err(error);
        }
        let (left_type, right_type) = (
            crate::eval::infer_type(left, scope)?,
            crate::eval::infer_type(right, scope)?,
        );
        if left_type.storage_type() != ColumnType::JsonPath
            && right_type.storage_type() != ColumnType::JsonPath
        {
            continue;
        }
        crate::eval::infer_type(
            &Expr::Binary {
                op,
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
            },
            scope,
        )?;
    }
    Ok(())
}

/// Evaluate a row constructor's fields with the caller's child evaluator.
fn fields(
    items: &[Expr],
    eval_child: &mut impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Vec<Datum>, ExecError> {
    items.iter().map(eval_child).collect()
}

/// A row value in an ordinary value position: the anonymous `record`.
pub(crate) fn eval_row(
    items: &[Expr],
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    Ok(Datum::Record(RecordValue::anonymous(fields(
        items,
        &mut eval_child,
    )?)))
}

/// `PostgreSQL`'s `record_out`, over already-evaluated field values.
/// Row-wise comparison: this function compares fields left to right, and the
/// first pair that differs decides the whole row. A NULL pair reached before any
/// decision makes the comparison NULL. That is why `(1,NULL) < (2,2)` is true,
/// because the first pair already decided, while `(1,2) < (1,NULL)` is NULL.
///
/// `ROW()` has nothing to decide with, and `PostgreSQL` refuses to order or
/// equate two of them (0A000) rather than calling them equal — even though
/// `IS [NOT] DISTINCT FROM` and `IS [NOT] NULL`, which do not go through here,
/// answer for it happily.
fn compare(left: &[Datum], right: &[Datum], equality: bool) -> Result<Option<Ordering>, ExecError> {
    if left.len() != right.len() {
        return Err(ExecError::Syntax(
            "unequal number of entries in row expressions".into(),
        ));
    }
    if left.is_empty() {
        return Err(ExecError::Unsupported(
            "cannot compare rows of zero length".into(),
        ));
    }
    for (l, r) in left.iter().zip(right) {
        if equality {
            if crate::eval::runtime_equality_short_circuit(l, r) == Some(false) {
                return Ok(Some(Ordering::Less));
            }
            crate::eval::require_runtime_equality(l, r)?;
        } else {
            crate::eval::require_runtime_comparison(l, r)?;
        }
        match ops::compare(l, r)? {
            Some(Ordering::Equal) => {}
            Some(order) => return Ok(Some(order)),
            None => return Ok(None),
        }
    }
    Ok(Some(Ordering::Equal))
}

/// Row-wise `IS DISTINCT FROM`: two rows are distinct if and only if some field
/// pair is distinct. A field pair of two NULLs is not distinct, and a NULL
/// against a non-NULL is distinct. The result is never NULL.
fn distinct(left: &[Datum], right: &[Datum]) -> Result<bool, ExecError> {
    if left.len() != right.len() {
        return Err(ExecError::Syntax(
            "unequal number of entries in row expressions".into(),
        ));
    }
    for (l, r) in left.iter().zip(right) {
        if is_distinct(l, r)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Scalar `IS DISTINCT FROM`: null-safe inequality. The result is never NULL.
pub(crate) fn is_distinct(l: &Datum, r: &Datum) -> Result<bool, ExecError> {
    match (l.is_null(), r.is_null()) {
        (true, true) => Ok(false),
        (true, false) | (false, true) => Ok(true),
        (false, false) => {
            if let Some(equal) = crate::eval::runtime_equality_short_circuit(l, r) {
                return Ok(!equal);
            }
            crate::eval::require_runtime_equality(l, r)?;
            Ok(ops::compare(l, r)? != Some(Ordering::Equal))
        }
    }
}

/// `left <op> right` when BOTH operands are row constructors, for the operators
/// defined over rows. Returns `Ok(None)` when this is not a row-wise operation,
/// which leaves the caller's ordinary path in charge.
pub(crate) fn eval_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Option<Datum>, ExecError> {
    let (Expr::Row(l), Expr::Row(r)) = (left, right) else {
        return Ok(None);
    };
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::IsDistinctFrom
            | BinaryOp::IsNotDistinctFrom
            | BinaryOp::Overlaps
    ) {
        return Ok(None);
    }
    let (lv, rv) = (fields(l, &mut eval_child)?, fields(r, &mut eval_child)?);
    if op == BinaryOp::Overlaps {
        if lv.len() != 2 || rv.len() != 2 {
            return Err(ExecError::UndefinedFunction(
                "function overlaps(record, record) does not exist".into(),
            ));
        }
        if lv.iter().chain(&rv).any(Datum::is_null) {
            return Ok(Some(Datum::Null));
        }
        let before = |a: &Datum, b: &Datum| -> Result<bool, ExecError> {
            crate::eval::require_runtime_comparison(a, b)?;
            Ok(ops::compare(a, b)? == Some(Ordering::Less))
        };
        let (ls, le) = if before(&lv[1], &lv[0])? {
            (&lv[1], &lv[0])
        } else {
            (&lv[0], &lv[1])
        };
        let (rs, re) = if before(&rv[1], &rv[0])? {
            (&rv[1], &rv[0])
        } else {
            (&rv[0], &rv[1])
        };
        return Ok(Some(Datum::Bool(before(ls, re)? && before(rs, le)?)));
    }
    if matches!(op, BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) {
        let distinct = distinct(&lv, &rv)?;
        return Ok(Some(Datum::Bool(
            distinct ^ (op == BinaryOp::IsNotDistinctFrom),
        )));
    }
    Ok(Some(crate::eval::cmp_result(
        op,
        compare(&lv, &rv, matches!(op, BinaryOp::Eq | BinaryOp::Ne))?,
    )))
}

/// `row IS [NOT] NULL`, which is field-wise and so not a pair of negations.
/// `IS NULL` holds only when EVERY field is NULL, and `IS NOT NULL` only when
/// every field is non-NULL, so `ROW(1, NULL)` satisfies neither. Returns
/// `Ok(None)` when the operand is not a row constructor.
pub(crate) fn eval_is_null(
    expr: &Expr,
    negated: bool,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Option<Datum>, ExecError> {
    let Expr::Row(items) = expr else {
        return Ok(None);
    };
    let values = fields(items, &mut eval_child)?;
    Ok(Some(field_wise_is_null(&values, negated)))
}

/// `IS [NOT] NULL` over an already-evaluated operand, when that operand turned
/// out to be a composite value.
///
/// `PostgreSQL`'s `NullTest` carries an `argisrow` flag, set for every operand
/// whose *type* is composite, and the field-wise rule
/// [`eval_is_null`] applies to a row constructor is the same rule that flag
/// selects. So it is the value's shape, not the expression's, that decides:
/// a whole-row reference, a column of a composite type, and a composite the
/// correlated walker already folded to a constant are all tested field by
/// field, and only `ROW(1, NULL)`-shaped operands can satisfy neither
/// `IS NULL` nor `IS NOT NULL`.
///
/// A composite that is itself NULL arrives as [`Datum::Null`], not as a record
/// of NULLs, so it never reaches here and keeps the scalar answer — which is
/// the same answer `PostgreSQL` gives it.
///
/// `None` when the operand is not composite, leaving the scalar test in place.
pub(crate) fn composite_is_null(value: &Datum, negated: bool) -> Option<Datum> {
    let Datum::Record(record) = value else {
        return None;
    };
    Some(field_wise_is_null(&record.values, negated))
}

/// The field-wise `IS [NOT] NULL` answer shared by a row constructor and a
/// composite value.
fn field_wise_is_null(values: &[Datum], negated: bool) -> Datum {
    Datum::Bool(if negated {
        values.iter().all(|value| !value.is_null())
    } else {
        values.iter().all(Datum::is_null)
    })
}

/// `row [NOT] IN (row, …)`, compared row-wise with the same three-valued logic
/// as the scalar list. An equal row wins. If no row is equal, a NULL comparison
/// anywhere makes the result NULL. Returns `Ok(None)` when the left operand is
/// not a row.
pub(crate) fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Option<Datum>, ExecError> {
    let Expr::Row(items) = expr else {
        return Ok(None);
    };
    let probe = fields(items, &mut eval_child)?;
    let mut saw_null = false;
    for candidate in list {
        let Expr::Row(candidate) = candidate else {
            return Err(ExecError::TypeMismatch(
                "IN list of a row expression must contain row expressions".into(),
            ));
        };
        let values = fields(candidate, &mut eval_child)?;
        match compare(&probe, &values, true)? {
            Some(Ordering::Equal) => return Ok(Some(Datum::Bool(!negated))),
            Some(_) => {}
            None => saw_null = true,
        }
    }
    if saw_null {
        return Ok(Some(Datum::Null));
    }
    Ok(Some(Datum::Bool(negated)))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgtypes::{ArrayDim, ArrayValue, ElemType};

    use super::*;

    fn int(n: i32) -> Datum {
        Datum::Int4(n)
    }

    /// `record_out`'s quoting, which is what a composite value's text form goes
    /// through on the wire. A field is quoted only when leaving it bare would be
    /// ambiguous, a NULL field is empty, and `"`/`\\` are doubled/escaped.
    #[test]
    fn text_output_quotes_the_fields_postgres_quotes() {
        let f = |s: &str| Some(s.to_string());
        let cases: &[(&[Option<String>], &str)] = &[
            (&[f("1"), f("2")], "(1,2)"),
            (&[f("1"), None, f("t")], "(1,,t)"),
            (&[f("a b")], "(\"a b\")"),
            (&[f("a,b")], "(\"a,b\")"),
            (&[f("c\"d")], "(\"c\"\"d\")"),
            (&[f("a\\b")], "(\"a\\\\b\")"),
            (&[f("a(b")], "(\"a(b\")"),
            (&[f("")], "(\"\")"),
            (&[], "()"),
        ];
        for (values, expected) in cases {
            let got = crabka_pgtypes::composite::record_out(values);
            assert!(got == *expected, "{values:?}: {got} != {expected}");
        }
    }

    #[test]
    fn comparison_stops_at_the_first_field_that_decides() {
        let cases: &[(&[Datum], &[Datum], Option<Ordering>)] = &[
            (&[int(1), int(2)], &[int(1), int(2)], Some(Ordering::Equal)),
            (&[int(1), int(2)], &[int(1), int(3)], Some(Ordering::Less)),
            (
                &[int(2), int(1)],
                &[int(1), int(9)],
                Some(Ordering::Greater),
            ),
            // A field that already decided wins over a later NULL.
            (
                &[int(1), Datum::Null],
                &[int(2), int(2)],
                Some(Ordering::Less),
            ),
            // A NULL reached before any decision makes the whole row NULL.
            (&[int(1), Datum::Null], &[int(1), int(2)], None),
            (
                &[Datum::Null, Datum::Null],
                &[Datum::Null, Datum::Null],
                None,
            ),
        ];
        for (left, right, expected) in cases {
            assert!(compare(left, right, false).expect("comparable") == *expected);
        }
    }

    #[test]
    fn two_zero_length_rows_cannot_be_compared_but_can_be_tested() {
        assert!(
            compare(&[], &[], false)
                .expect_err("no fields to decide with")
                .into_pg()
                .code
                == "0A000"
        );
        // The tests that do not order fields still answer.
        assert!(distinct(&[], &[]).expect("no field is distinct") == false);
    }

    #[test]
    fn comparison_rejects_unequal_row_lengths() {
        assert!(
            compare(&[int(1)], &[int(1), int(2)], false)
                .expect_err("row arity mismatch")
                .into_pg()
                .code
                == "42601"
        );
    }

    #[test]
    fn distinct_is_null_safe_and_field_wise() {
        assert!(distinct(&[int(1), Datum::Null], &[int(1), Datum::Null]).expect("ok") == false);
        assert!(distinct(&[int(1), Datum::Null], &[int(1), int(2)]).expect("ok") == true);
        assert!(distinct(&[int(1), int(2)], &[int(1), int(2)]).expect("ok") == false);
    }

    #[test]
    fn distinct_uses_array_shape_before_jsonpath_element_equality() {
        let value = Datum::JsonPath("$".into());
        let one_dimensional = Datum::Array(ArrayValue::with_dims(
            ElemType::JsonPath,
            vec![value.clone()],
            vec![ArrayDim::from_len(1)],
        ));
        let two_dimensional = Datum::Array(ArrayValue::with_dims(
            ElemType::JsonPath,
            vec![value],
            vec![ArrayDim::from_len(1), ArrayDim::from_len(1)],
        ));
        assert!(is_distinct(&one_dimensional, &two_dimensional).expect("shape differs"));
    }

    #[test]
    fn row_null_tests_are_field_wise_in_both_directions() {
        for (values, is_null, is_not_null) in [
            (vec![Datum::Null, Datum::Null], true, false),
            (vec![int(1), int(2)], false, true),
            (vec![int(1), Datum::Null], false, false),
        ] {
            assert!(field_wise_is_null(&values, false) == Datum::Bool(is_null));
            assert!(field_wise_is_null(&values, true) == Datum::Bool(is_not_null));
        }
    }

    #[test]
    fn temporal_row_pairs_overlap_only_when_their_half_open_periods_intersect() {
        let ctx = crate::clock::EvalCtx::test_default();
        for (sql, expected) in [
            (
                "(date '2024-01-01', date '2024-01-03') OVERLAPS (date '2024-01-02', date '2024-01-04')",
                true,
            ),
            (
                "(date '2024-01-01', date '2024-01-02') OVERLAPS (date '2024-01-02', date '2024-01-03')",
                false,
            ),
            (
                "(date '2024-01-03', date '2024-01-01') OVERLAPS (date '2024-01-02', date '2024-01-04')",
                true,
            ),
        ] {
            let expr = crabka_pgparser::parser::parse_expr_for_test(sql).expect("parse");
            assert!(
                crate::eval::eval(&expr, &Scope::empty(), &[], &ctx).expect("eval")
                    == Datum::Bool(expected),
                "{sql}"
            );
            assert!(
                crate::eval::infer_type(&expr, &Scope::empty()).expect("type") == ColumnType::Bool
            );
        }
        let left = crabka_pgparser::parser::parse_expr_for_test(
            "ROW(date '2024-01-01', date '2024-01-02')",
        )
        .expect("left row");
        let right = crabka_pgparser::parser::parse_expr_for_test("ROW(date '2024-01-01')")
            .expect("right row");
        assert!(
            eval_binary(BinaryOp::Overlaps, &left, &right, |expr| {
                crate::eval::eval(expr, &Scope::empty(), &[], &ctx)
            })
            .expect_err("row arity")
            .into_pg()
            .code
                == "42883"
        );
    }
}

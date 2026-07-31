//! `PostgreSQL`'s SQL92 "a bare constant names an output column" rule, shared by
//! `ORDER BY`, `GROUP BY` and `DISTINCT ON`.
//!
//! `findTargetlistEntrySQL92` decides this in the parser, before any expression
//! typing happens, so the test is purely syntactic: *is the item an `A_Const`?*
//! A `A_Const` holding an integer is an output position; any other constant —
//! decimal, string, boolean, NULL, or an integer too wide for `int4` — is
//! rejected with `42601`, and everything else is an ordinary expression.
//!
//! Two details are easy to get wrong and are what this module exists to pin
//! down. The `-` of a negative literal folds *into* the constant (`doNegate`),
//! so `ORDER BY -1` is position `-1` and reports `42P10`, not an expression that
//! silently sorts every row equal. A unary `+`, by contrast, folds into nothing:
//! `ORDER BY +1` is the operator expression `+1` and sorts by a constant.

use crabka_pgparser::ast::{Expr, UnaryOp};

use crate::error::ExecError;

/// The clause a position reference appears in. Only affects error text, which
/// `PostgreSQL` spells with the clause name (`non-integer constant in ORDER BY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sql92Clause {
    OrderBy,
    GroupBy,
    DistinctOn,
}

impl Sql92Clause {
    /// The clause name as `PostgreSQL` spells it in `ParseExprKindName`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Sql92Clause::OrderBy => "ORDER BY",
            Sql92Clause::GroupBy => "GROUP BY",
            Sql92Clause::DistinctOn => "DISTINCT ON",
        }
    }
}

/// A bare constant, classified the way `findTargetlistEntrySQL92` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Constant {
    /// An `int4`-ranged integer constant — the only kind that names a position.
    /// May be zero or negative; the caller reports that as `42P10`.
    Position(i64),
    /// Any other constant. `PostgreSQL` rejects it outright with `42601`.
    NonInteger,
}

/// The output-column position `expr` names, if it is a bare constant.
///
/// `Ok(None)` means `expr` is an ordinary expression and must be resolved as
/// one. `Ok(Some(pos))` is an unchecked 1-based position — the caller compares
/// it against its own output width and reports `42P10` with this exact value, so
/// `ORDER BY -1` says `position -1`. `Err` is `PostgreSQL`'s `42601` for a
/// constant that is not an integer.
pub(crate) fn position(expr: &Expr, clause: Sql92Clause) -> Result<Option<i64>, ExecError> {
    match constant(expr) {
        Some(Constant::Position(pos)) => Ok(Some(pos)),
        Some(Constant::NonInteger) => Err(ExecError::Syntax(format!(
            "non-integer constant in {}",
            clause.name()
        ))),
        None => Ok(None),
    }
}

/// Range-check a position against an output list `width` columns wide, yielding
/// the 0-based index.
///
/// `PostgreSQL` echoes the position exactly as the query wrote it, so a negative
/// one reports `position -1` rather than being clamped or silently ignored.
pub(crate) fn output_index(
    position: i64,
    width: usize,
    clause: Sql92Clause,
) -> Result<usize, ExecError> {
    usize::try_from(position)
        .ok()
        .filter(|index| *index >= 1 && *index <= width)
        .map(|index| index - 1)
        .ok_or_else(|| {
            ExecError::InvalidColumnReference(format!(
                "{} position {position} is not in select list",
                clause.name()
            ))
        })
}

/// [`position`] followed by [`output_index`]: the whole SQL92 rule for a clause
/// whose output list is `width` columns wide. `Ok(None)` means "not a constant".
pub(crate) fn output_position(
    expr: &Expr,
    width: usize,
    clause: Sql92Clause,
) -> Result<Option<usize>, ExecError> {
    position(expr, clause)?
        .map(|pos| output_index(pos, width, clause))
        .transpose()
}

/// Classify `expr` as a bare constant, or `None` if it is an expression.
fn constant(expr: &Expr) -> Option<Constant> {
    match expr {
        // `makeStringConst`/`makeBoolAConst`/`makeNullAConst` all build `A_Const`
        // nodes, so these reach the "non-integer constant" branch rather than
        // being treated as expressions.
        Expr::StringLiteral(_) | Expr::BoolLiteral(_) | Expr::NullLiteral => {
            Some(Constant::NonInteger)
        }
        other => negatable(other),
    }
}

/// The numeric constants, with `-` folded in exactly where `doNegate` folds it:
/// into an integer or float constant, and nowhere else.
fn negatable(expr: &Expr) -> Option<Constant> {
    match expr {
        // `PostgreSQL`'s `T_Integer` is 32-bit. A literal that overflows it is
        // scanned as a float constant, which is why `ORDER BY 3000000000` is
        // "non-integer constant" rather than an out-of-range position.
        Expr::IntLiteral(text) => Some(text.parse::<i32>().map_or(Constant::NonInteger, |value| {
            Constant::Position(i64::from(value))
        })),
        Expr::NumericLiteral(_) => Some(Constant::NonInteger),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match negatable(expr)? {
            Constant::Position(value) => Some(Constant::Position(-value)),
            Constant::NonInteger => Some(Constant::NonInteger),
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::{BinaryOp, Expr, UnaryOp};

    use super::*;

    fn int(text: &str) -> Expr {
        Expr::IntLiteral(text.into())
    }

    fn neg(expr: Expr) -> Expr {
        Expr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(expr),
        }
    }

    #[test]
    fn bare_constants_classify_as_postgresql_does() {
        let cases: Vec<(Expr, Result<Option<i64>, ExecError>)> = vec![
            (int("1"), Ok(Some(1))),
            (int("0"), Ok(Some(0))),
            (neg(int("1")), Ok(Some(-1))),
            (neg(int("0")), Ok(Some(0))),
            (neg(neg(int("2"))), Ok(Some(2))),
            // Wider than int4: a float constant in PostgreSQL, so 42601.
            (
                int("3000000000"),
                Err(ExecError::Syntax("non-integer constant in ORDER BY".into())),
            ),
            (
                neg(int("3000000000")),
                Err(ExecError::Syntax("non-integer constant in ORDER BY".into())),
            ),
            (
                Expr::NumericLiteral("1.0".into()),
                Err(ExecError::Syntax("non-integer constant in ORDER BY".into())),
            ),
            (
                Expr::StringLiteral("x".into()),
                Err(ExecError::Syntax("non-integer constant in ORDER BY".into())),
            ),
            (
                Expr::BoolLiteral(true),
                Err(ExecError::Syntax("non-integer constant in ORDER BY".into())),
            ),
            (
                Expr::NullLiteral,
                Err(ExecError::Syntax("non-integer constant in ORDER BY".into())),
            ),
            // Not constants: ordinary expressions the caller resolves normally.
            (
                Expr::Column {
                    table: None,
                    name: "a".into(),
                },
                Ok(None),
            ),
            (
                Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(int("1")),
                    right: Box::new(int("1")),
                },
                Ok(None),
            ),
            // `doNegate` only folds numeric constants, so `- 'x'` stays an
            // operator expression rather than becoming a rejected constant.
            (neg(Expr::StringLiteral("x".into())), Ok(None)),
            (neg(Expr::NullLiteral), Ok(None)),
        ];

        for (expr, expected) in cases {
            assert!(
                position(&expr, Sql92Clause::OrderBy) == expected,
                "{expr:?}"
            );
        }
    }

    #[test]
    fn clause_name_appears_in_the_rejection() {
        let cases = [
            (Sql92Clause::OrderBy, "non-integer constant in ORDER BY"),
            (Sql92Clause::GroupBy, "non-integer constant in GROUP BY"),
            (
                Sql92Clause::DistinctOn,
                "non-integer constant in DISTINCT ON",
            ),
        ];
        for (clause, message) in cases {
            let got = position(&Expr::NumericLiteral("1.0".into()), clause);
            assert!(got == Err(ExecError::Syntax(message.into())));
        }
    }
}

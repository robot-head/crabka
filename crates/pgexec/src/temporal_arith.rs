//! `PostgreSQL`'s operator resolution for `+ - * /` when a date/time value is
//! an operand.
//!
//! The four arithmetic spellings are each carried by many `pg_operator` rows,
//! and which row a call selects is *derived*, not looked up: `oper()` matches
//! the operand types against every row by implicit coercion
//! (`func_match_argtypes`) and then narrows the survivors
//! (`func_select_candidate`). The outcome has three shapes, and a corpus that
//! only ever checks the message would not tell them apart:
//!
//! * no row is reachable — 42883 `operator does not exist`;
//! * exactly one row survives — that row is the operator, and it may be one
//!   neither operand's type spells (`date - time` selects `date - interval`,
//!   because `time → interval` is an implicit cast);
//! * several rows survive and none is better — 42725 `operator is not unique`.
//!
//! This module answers *which* row, and the result type of the expression is
//! that row's. Converting each operand to the row's declared parameter type —
//! PostgreSQL's second step — is not done yet, so a row reached through a
//! coercion resolves here and then fails in `crabka_pgtypes::ops`, which knows
//! only the pairs it implements. That is the same refusal those pairs already
//! got, with a better-founded plan behind it.
//!
//! `time + time` and `timetz + timetz` are the pair that shows why this cannot
//! be a table of answers. They are written identically, and `PostgreSQL`
//! answers them differently: `time` casts implicitly to both `interval` and
//! `timetz`, so `time + time` reaches `time + interval`, `interval + time`,
//! `interval + interval`, `timetz + interval` and `interval + timetz` and can
//! choose between the first two; `timetz` casts implicitly to nothing at all,
//! so `timetz + timetz` reaches no row whatsoever.
//!
//! Both inputs of this module are catalog data rather than judgement: the rows
//! are `pg_operator`'s, and the coercions come from
//! [`crate::builtin_casts::BUILTIN_CASTS`], which is `pg_cast` itself.

use crabka_pgparser::ast::BinaryOp;
use crabka_pgtypes::ColumnType;

use crate::error::ExecError;

/// One `pg_operator` row: `oprleft <oprname> oprright → oprresult`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Signature {
    pub(crate) left: ColumnType,
    pub(crate) right: ColumnType,
    pub(crate) result: ColumnType,
}

/// Every `pg_operator` row for `+ - * /` with a date/time operand, in the
/// catalog's own order.
///
/// Rows *without* such an operand are deliberately absent, and that omission is
/// safe rather than convenient: no date/time type has an implicit cast to any
/// other category, and no other category has one into date/time, so a pair with
/// a date/time operand can never match a purely numeric row. See
/// [`is_temporal`] for the gate that keeps a numeric pair out of here entirely.
const SIGNATURES: &[(BinaryOp, Signature)] = {
    use ColumnType::{Date, Float8, Int4, Interval, Time, Timestamp, Timestamptz, Timetz};
    const fn row(
        op: BinaryOp,
        left: ColumnType,
        right: ColumnType,
        result: ColumnType,
    ) -> (BinaryOp, Signature) {
        (
            op,
            Signature {
                left,
                right,
                result,
            },
        )
    }
    &[
        row(BinaryOp::Add, Date, Interval, Timestamp),
        row(BinaryOp::Sub, Date, Interval, Timestamp),
        row(BinaryOp::Sub, Date, Date, Int4),
        row(BinaryOp::Add, Date, Int4, Date),
        row(BinaryOp::Sub, Date, Int4, Date),
        row(BinaryOp::Add, Timestamptz, Interval, Timestamptz),
        row(BinaryOp::Sub, Timestamptz, Timestamptz, Interval),
        row(BinaryOp::Sub, Timestamptz, Interval, Timestamptz),
        row(BinaryOp::Add, Interval, Interval, Interval),
        row(BinaryOp::Sub, Interval, Interval, Interval),
        row(BinaryOp::Add, Date, Time, Timestamp),
        row(BinaryOp::Add, Date, Timetz, Timestamptz),
        row(BinaryOp::Add, Time, Date, Timestamp),
        row(BinaryOp::Add, Timetz, Date, Timestamptz),
        row(BinaryOp::Sub, Time, Time, Interval),
        row(BinaryOp::Mul, Interval, Float8, Interval),
        row(BinaryOp::Mul, Float8, Interval, Interval),
        row(BinaryOp::Div, Interval, Float8, Interval),
        row(BinaryOp::Add, Time, Interval, Time),
        row(BinaryOp::Sub, Time, Interval, Time),
        row(BinaryOp::Add, Timetz, Interval, Timetz),
        row(BinaryOp::Sub, Timetz, Interval, Timetz),
        row(BinaryOp::Add, Interval, Time, Time),
        row(BinaryOp::Add, Timestamp, Interval, Timestamp),
        row(BinaryOp::Sub, Timestamp, Timestamp, Interval),
        row(BinaryOp::Sub, Timestamp, Interval, Timestamp),
        row(BinaryOp::Add, Interval, Date, Timestamp),
        row(BinaryOp::Add, Interval, Timetz, Timetz),
        row(BinaryOp::Add, Interval, Timestamp, Timestamp),
        row(BinaryOp::Add, Interval, Timestamptz, Timestamptz),
        row(BinaryOp::Add, Int4, Date, Date),
    ]
};

/// The `pg_type.typcategory` values these rows use. `DateTime` is `'D'`,
/// `Timespan` is `'T'` and `Numeric` is `'N'`; every other type is `Other`,
/// which no row declares and so can never be preferred.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Category {
    DateTime,
    Timespan,
    Numeric,
    Other,
}

/// `TypeCategory()` restricted to the types these operators reach.
fn category(ty: ColumnType) -> Category {
    match ty.storage_type() {
        ColumnType::Date
        | ColumnType::Time
        | ColumnType::Timetz
        | ColumnType::Timestamp
        | ColumnType::Timestamptz => Category::DateTime,
        ColumnType::Interval => Category::Timespan,
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float4 => {
            Category::Numeric
        }
        ColumnType::Float8 => Category::Numeric,
        other if other.is_numeric() => Category::Numeric,
        _ => Category::Other,
    }
}

/// `IsPreferredType()`: is `candidate` its category's preferred type, *and* is
/// that the category the input argument belongs to?
///
/// `pg_type.typispreferred` is set on exactly one member of each category here
/// — `timestamptz` for `'D'`, `interval` for `'T'`, `float8` for `'N'` — and
/// the two-sided test is what stops `interval` from counting as a preferred
/// answer for a `time` input: `interval` is category `'T'` and `time` is `'D'`,
/// so the two never meet. That is the whole reason `time + time` stays
/// ambiguous instead of settling on one of `time + interval` /
/// `interval + time`.
fn is_preferred_for(input: ColumnType, candidate: ColumnType) -> bool {
    let preferred = matches!(
        candidate.storage_type(),
        ColumnType::Timestamptz | ColumnType::Interval | ColumnType::Float8
    );
    preferred && category(input) == category(candidate)
}

/// Is `source` implicitly coercible to `target`?
///
/// Read straight out of `pg_cast`: a row with `castcontext = 'i'`. Nothing else
/// counts — `timetz → time` and `interval → time` both exist at *assignment*
/// context, and admitting either would make `timetz + timetz` resolve where
/// `PostgreSQL` says no operator exists.
fn implicitly_coercible(source: ColumnType, target: ColumnType) -> bool {
    let (source, target) = (source.storage_type().oid(), target.storage_type().oid());
    source == target
        || crate::builtin_casts::BUILTIN_CASTS
            .iter()
            .any(|&(_, from, to, _, context, _)| {
                context == "i"
                    && u32::try_from(from) == Ok(source)
                    && u32::try_from(to) == Ok(target)
            })
}

/// Does this type belong to the date/time or timespan categories — the gate
/// that decides whether an expression is this module's business at all?
pub(crate) fn is_temporal(ty: ColumnType) -> bool {
    matches!(category(ty), Category::DateTime | Category::Timespan)
}

/// `oper()` for `+ - * /` when at least one operand is a date/time value.
///
/// `None` means "not this module's business": neither operand is temporal, so
/// the caller's numeric rules apply unchanged.
///
/// # Errors
///
/// 42883 when no row is reachable, 42725 when several are and none is better.
pub(crate) fn resolve(
    op: BinaryOp,
    lt: ColumnType,
    rt: ColumnType,
) -> Option<Result<Signature, ExecError>> {
    if !is_temporal(lt) && !is_temporal(rt) {
        return None;
    }
    if !matches!(
        op,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
    ) {
        return None;
    }
    Some(select(op, lt, rt))
}

fn select(op: BinaryOp, lt: ColumnType, rt: ColumnType) -> Result<Signature, ExecError> {
    let rows = || SIGNATURES.iter().filter(move |(name, _)| *name == op);
    // `binary_oper_exact`: an operator declared over exactly these two types
    // wins outright, without enumerating anything.
    if let Some((_, exact)) = rows().find(|(_, sig)| {
        sig.left.oid() == lt.storage_type().oid() && sig.right.oid() == rt.storage_type().oid()
    }) {
        return Ok(*exact);
    }
    // `func_match_argtypes`: keep the rows both operands reach by implicit cast.
    let mut candidates: Vec<Signature> = rows()
        .filter(|(_, sig)| {
            implicitly_coercible(lt, sig.left) && implicitly_coercible(rt, sig.right)
        })
        .map(|(_, sig)| *sig)
        .collect();
    if candidates.is_empty() {
        return Err(crate::eval::undefined_operator(
            crate::eval::op_spelling(op),
            lt,
            rt,
        ));
    }
    // `func_select_candidate` pass 1: most arguments matched on the exact type.
    let exact =
        |input: ColumnType, declared: ColumnType| input.storage_type().oid() == declared.oid();
    narrow(&mut candidates, (lt, rt), exact);
    if let [only] = candidates.as_slice() {
        return Ok(*only);
    }
    // Pass 2: most arguments matched exactly *or* by their category's preferred
    // type.
    narrow(&mut candidates, (lt, rt), |input, declared| {
        exact(input, declared) || is_preferred_for(input, declared)
    });
    match candidates.as_slice() {
        [only] => Ok(*only),
        // Both operands are known types, so `PostgreSQL` has no unknown-argument
        // heuristic left to try and gives up here.
        _ => Err(ambiguous_operator(crate::eval::op_spelling(op), lt, rt)),
    }
}

/// Keep only the candidates that score highest under `matches`, counting one
/// point per argument position. `PostgreSQL` keeps every candidate when none
/// scores at all, which falls out of this: they then all tie at zero.
fn narrow(
    candidates: &mut Vec<Signature>,
    (lt, rt): (ColumnType, ColumnType),
    matches: impl Fn(ColumnType, ColumnType) -> bool,
) {
    let score =
        |sig: &Signature| usize::from(matches(lt, sig.left)) + usize::from(matches(rt, sig.right));
    let Some(best) = candidates.iter().map(score).max() else {
        return;
    };
    candidates.retain(|sig| score(sig) == best);
}

/// 42725 with `PostgreSQL`'s own wording.
fn ambiguous_operator(op: &str, lt: ColumnType, rt: ColumnType) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42725",
        message: format!("operator is not unique: {} {op} {}", lt.name(), rt.name()),
    }
}

/// The six spellings `ColumnType::name` gives the date/time types, which is how
/// an already-rendered message is recognised as one of this module's.
const TEMPORAL_TYPE_NAMES: [&str; 6] = [
    "date",
    "time without time zone",
    "time with time zone",
    "timestamp without time zone",
    "timestamp with time zone",
    "interval",
];

/// Point `PostgreSQL`'s caret at the arithmetic operator whose resolution
/// failed, for the two diagnostics [`resolve`] raises.
///
/// # Why only the date/time family
///
/// `PostgreSQL` carries a location on every operator it fails to resolve, so in
/// principle every 42883 of this shape deserves a caret. In practice a caret
/// costs two lines wherever crabka raises an error `PostgreSQL` does not, and
/// crabka's other 42883 operator reports are largely of that kind: measured on
/// the pinned 18.4 corpus, thirty of its `operator does not exist:` reports have
/// no counterpart upstream against fourteen that do. Restricted to a date/time
/// operand the population is four upstream sites, all four carrying a caret,
/// and none crabka invents — so this family is the whole gain and none of the
/// cost.
pub(crate) fn attach_operator_position(
    sql: &str,
    error: crabka_pgwire::error::PgError,
) -> crabka_pgwire::error::PgError {
    use crabka_pgparser::token::Token;

    if !matches!(error.code.as_str(), "42883" | "42725")
        || error
            .diagnostics
            .as_ref()
            .is_some_and(|diagnostics| diagnostics.position.is_some())
    {
        return error;
    }
    let Some(operands) = error
        .message
        .strip_prefix("operator does not exist: ")
        .or_else(|| error.message.strip_prefix("operator is not unique: "))
    else {
        return error;
    };
    // The rendered message is `<left type> <spelling> <right type>`, and no type
    // name holds an arithmetic spelling, so the one word that is one of the four
    // is the operator.
    let words: Vec<&str> = operands.split(' ').collect();
    let spelled: Vec<(usize, &str)> = words
        .iter()
        .enumerate()
        .filter(|(_, word)| matches!(**word, "+" | "-" | "*" | "/"))
        .map(|(index, word)| (index, *word))
        .collect();
    let [(index, spelling)] = spelled.as_slice() else {
        return error;
    };
    let names = [words[..*index].join(" "), words[index + 1..].join(" ")];
    if !names
        .iter()
        .any(|name| TEMPORAL_TYPE_NAMES.contains(&name.as_str()))
    {
        return error;
    }
    let wanted = match *spelling {
        "+" => Token::Plus,
        "-" => Token::Minus,
        "*" => Token::Star,
        _ => Token::Slash,
    };
    let Ok(tokens) = crabka_pgparser::lexer::lex(sql) else {
        return error;
    };
    let positions: Vec<usize> = tokens
        .iter()
        .filter(|(token, _)| *token == wanted)
        .map(|(_, offset)| sql[..*offset].chars().count() + 1)
        .collect();
    match positions.as_slice() {
        [position] => error.with_position(*position),
        _ => error,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::BinaryOp;
    use crabka_pgtypes::ColumnType::{Date, Float8, Int4, Int8, Interval, Time, Timestamp, Timetz};

    use super::resolve;
    use crate::error::ExecError;

    /// What `resolve` decided, flattened to something a table can state.
    fn outcome(
        op: BinaryOp,
        lt: crabka_pgtypes::ColumnType,
        rt: crabka_pgtypes::ColumnType,
    ) -> String {
        match resolve(op, lt, rt) {
            None => "not temporal".to_string(),
            Some(Ok(signature)) => format!(
                "{} {} {} -> {}",
                signature.left.name(),
                crate::eval::op_spelling(op),
                signature.right.name(),
                signature.result.name()
            ),
            Some(Err(ExecError::UndefinedFunction(message))) => format!("42883 {message}"),
            Some(Err(ExecError::FunctionError { sqlstate, message })) => {
                format!("{sqlstate} {message}")
            }
            Some(Err(other)) => format!("unexpected {other:?}"),
        }
    }

    /// `time + time` and `timetz + timetz` are written the same way and
    /// resolve differently, and every step of the difference is catalog data:
    /// `time` casts implicitly to `interval` and to `timetz`, `timetz` casts
    /// implicitly to nothing.
    #[test]
    fn the_same_shape_over_time_and_timetz_resolves_differently() {
        assert!(
            outcome(BinaryOp::Add, Time, Time)
                == "42725 operator is not unique: time without time zone + time without time zone"
        );
        assert!(
            outcome(BinaryOp::Add, Timetz, Timetz)
                == "42883 operator does not exist: time with time zone + time with time zone"
        );
    }

    /// The three outcomes over the pairs `PostgreSQL` 18.4 decides for itself.
    #[test]
    fn resolution_matches_postgres_over_the_datetime_operators() {
        let cases = [
            // An exact `pg_operator` row wins without enumerating anything.
            (
                BinaryOp::Sub,
                Time,
                Time,
                "time without time zone - time without time zone -> interval",
            ),
            (
                BinaryOp::Add,
                Date,
                Timetz,
                "date + time with time zone -> timestamp with time zone",
            ),
            (BinaryOp::Add, Date, Int4, "date + integer -> date"),
            // Reached only through an implicit cast: `time → interval` makes
            // `date - time` the `date - interval` operator, which is why it
            // answers a timestamp rather than refusing.
            (
                BinaryOp::Sub,
                Date,
                Time,
                "date - interval -> timestamp without time zone",
            ),
            (
                BinaryOp::Mul,
                Interval,
                Int4,
                "interval * double precision -> interval",
            ),
            (
                BinaryOp::Div,
                Interval,
                Float8,
                "interval / double precision -> interval",
            ),
            // One side coerces, and the exact-match pass picks the row that
            // needed the fewer coercions.
            (
                BinaryOp::Sub,
                Timestamp,
                Date,
                "timestamp without time zone - timestamp without time zone -> interval",
            ),
            // No row is reachable. `int8 → int4` is an *assignment* cast, so
            // `date + bigint` has no candidate even though `date + integer`
            // does.
            (
                BinaryOp::Add,
                Date,
                Int8,
                "42883 operator does not exist: date + bigint",
            ),
            (
                BinaryOp::Sub,
                Date,
                Timetz,
                "42883 operator does not exist: date - time with time zone",
            ),
            (
                BinaryOp::Add,
                Date,
                Date,
                "42883 operator does not exist: date + date",
            ),
            (
                BinaryOp::Mul,
                Interval,
                Interval,
                "42883 operator does not exist: interval * interval",
            ),
            (
                BinaryOp::Add,
                Int4,
                Interval,
                "42883 operator does not exist: integer + interval",
            ),
            // Neither operand is temporal, so the numeric tower owns the pair.
            (BinaryOp::Add, Int4, Int4, "not temporal"),
        ];
        for (op, lt, rt, expected) in cases {
            assert!(
                outcome(op, lt, rt) == expected,
                "{} {} {}",
                lt.name(),
                crate::eval::op_spelling(op),
                rt.name()
            );
        }
    }

    /// The preferred-type pass may only ever break a tie; it must not promote a
    /// candidate that lost the exact-match pass. `timestamptz` is category
    /// `D`'s preferred type, so `timestamp - date` would resolve to
    /// `timestamptz - timestamptz` if the passes ran in the wrong order.
    #[test]
    fn the_preferred_type_never_overrules_an_exact_match() {
        assert!(
            outcome(BinaryOp::Sub, Timestamp, Date)
                == "timestamp without time zone - timestamp without time zone -> interval"
        );
        assert!(
            outcome(BinaryOp::Add, Timestamp, Time)
                == "timestamp without time zone + interval -> timestamp without time zone"
        );
    }
}

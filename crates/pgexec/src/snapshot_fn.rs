//! The transaction-id surface: `pg_current_xact_id`, `pg_current_snapshot`,
//! the `pg_snapshot` accessors, `pg_xact_status`, and the deprecated `txid_*`
//! spelling of every one of them.
//!
//! `PostgreSQL` declares each function twice. The modern pair works in `xid8`
//! and `pg_snapshot`; the older `txid_*` pair works in `bigint` and
//! `txid_snapshot` and runs the *same* C function under a second `pg_proc`
//! name. Gres follows that shape: one implementation, two declared signatures,
//! and a [`Family`] that decides only which types the signature names and which
//! datum the answer comes back in.
//!
//! # Where the answers come from
//!
//! Nothing here is modelled. `pg_current_snapshot()` is the running set the
//! [`ProcArray`](crate::procarray::ProcArray) already keeps, exported through
//! [`crabka_pgtypes::snapshot::PgSnapshot::from_running`], which is
//! `sort_snapshot`. `pg_current_xact_id()` is the same xid the write path
//! allocates. `pg_xact_status` reads the clog the visibility rules read.
//!
//! # Assigning an xid from inside an expression
//!
//! `pg_current_xact_id()` must return a *valid* xid, so a transaction that has
//! not written yet gets one here. Expression evaluation holds the session's
//! context by shared reference and cannot write the transaction's own xid
//! slot, so the allocation is staged on
//! [`TxnRuntime::assigned`](crate::clock::TxnRuntime::assigned) and the session
//! adopts it. That is the seam `nextval` already uses for a sequence advance,
//! and it is here for the same reason.
//!
//! # What `pg_xact_status` cannot know
//!
//! `PostgreSQL` returns NULL for an id older than `oldestClogXid`, whose entry
//! has been truncated away. Gres has the same floor and the same reason for it:
//! `SqlEngine::truncate_clog_below` deletes every entry under the garbage
//! horizon once vacuum has frozen or pruned the versions that referenced them,
//! and records the floor durably. [`oldest_recorded_xid`] reads that record,
//! so an id below it is answered NULL here too — not "aborted", which is what
//! the bare absent key would otherwise read as and which would be a wrong
//! answer for every transaction that committed and was then forgotten.
//!
//! One line of `xid.sql` and `txid.sql` still cannot match. Upstream numbers
//! `FrozenTransactionId` 2 and reports it committed without consulting
//! anything, where Gres numbers its first *ordinary* transaction 2 — so once
//! that transaction's entry is truncated, Gres answers NULL where upstream
//! answers `committed`. Reporting a real transaction as committed on the
//! strength of its number alone would be a lie about a transaction that
//! existed.

use std::borrow::Cow;

use crabka_pgmvcc::clog::XidStatus;
use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, snapshot::PgSnapshot};

use crate::{
    clock::{EvalCtx, TxnRuntime},
    error::ExecError,
    eval::infer_type,
    func::{checked_args, require_arity, undefined_function},
    scope::Scope,
};

/// Which of the two declared spellings a name belongs to.
///
/// The pair differs only in the types the signature names: `xid8` and
/// `pg_snapshot` against `bigint` and `txid_snapshot`. `bigint` is signed and
/// `xid8` is not, so the same 64 bits print differently past 2^63 — which is
/// `txid_*`'s own long-standing limitation and the reason it was replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    /// `pg_*`, over `xid8` and `pg_snapshot`.
    Modern,
    /// `txid_*`, over `bigint` and `txid_snapshot`.
    Legacy,
}

impl Family {
    /// The type this family calls a transaction id.
    fn xid_type(self) -> ColumnType {
        match self {
            Family::Modern => ColumnType::Xid8,
            Family::Legacy => ColumnType::Int8,
        }
    }

    /// The type this family calls a snapshot.
    fn snapshot_type(self) -> ColumnType {
        match self {
            Family::Modern => ColumnType::PgSnapshot,
            Family::Legacy => ColumnType::TxidSnapshot,
        }
    }

    /// A transaction id as a value of this family's id type.
    fn xid_datum(self, xid: u64) -> Datum {
        match self {
            Family::Modern => Datum::Xid8(xid),
            // `txid_*` reinterprets the bits rather than range-checking them,
            // which is what makes an id past 2^63 print negative there.
            Family::Legacy => Datum::Int8(xid.cast_signed()),
        }
    }
}

/// The functions this module answers, each in both families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotFunc {
    /// `pg_current_xact_id()` / `txid_current()`, which assigns an xid when the
    /// transaction has none.
    CurrentXactId,
    /// `pg_current_xact_id_if_assigned()` / `txid_current_if_assigned()`, which
    /// never assigns one.
    CurrentXactIdIfAssigned,
    /// `pg_current_snapshot()` / `txid_current_snapshot()`.
    CurrentSnapshot,
    /// `pg_snapshot_xmin()` / `txid_snapshot_xmin()`.
    SnapshotXmin,
    /// `pg_snapshot_xmax()` / `txid_snapshot_xmax()`.
    SnapshotXmax,
    /// `pg_visible_in_snapshot()` / `txid_visible_in_snapshot()`.
    VisibleInSnapshot,
    /// `pg_xact_status()` / `txid_status()`.
    XactStatus,
}

/// Resolve a name to its function and family.
fn snapshot_func(name: &str) -> Option<(SnapshotFunc, Family)> {
    use Family::{Legacy, Modern};
    Some(match name {
        "pg_current_xact_id" => (SnapshotFunc::CurrentXactId, Modern),
        "txid_current" => (SnapshotFunc::CurrentXactId, Legacy),
        "pg_current_xact_id_if_assigned" => (SnapshotFunc::CurrentXactIdIfAssigned, Modern),
        "txid_current_if_assigned" => (SnapshotFunc::CurrentXactIdIfAssigned, Legacy),
        "pg_current_snapshot" => (SnapshotFunc::CurrentSnapshot, Modern),
        "txid_current_snapshot" => (SnapshotFunc::CurrentSnapshot, Legacy),
        "pg_snapshot_xmin" => (SnapshotFunc::SnapshotXmin, Modern),
        "txid_snapshot_xmin" => (SnapshotFunc::SnapshotXmin, Legacy),
        "pg_snapshot_xmax" => (SnapshotFunc::SnapshotXmax, Modern),
        "txid_snapshot_xmax" => (SnapshotFunc::SnapshotXmax, Legacy),
        "pg_visible_in_snapshot" => (SnapshotFunc::VisibleInSnapshot, Modern),
        "txid_visible_in_snapshot" => (SnapshotFunc::VisibleInSnapshot, Legacy),
        "pg_xact_status" => (SnapshotFunc::XactStatus, Modern),
        "txid_status" => (SnapshotFunc::XactStatus, Legacy),
        _ => return None,
    })
}

/// Is `name` one of this module's functions? (`func::is_scalar` folds this in.)
pub(crate) fn is_snapshot_func(name: &str) -> bool {
    snapshot_func(name).is_some()
}

/// Each function's declared parameter list and result type, as `pg_proc`
/// records them for its family.
fn signature(f: SnapshotFunc, family: Family) -> (Vec<ColumnType>, ColumnType) {
    match f {
        SnapshotFunc::CurrentXactId | SnapshotFunc::CurrentXactIdIfAssigned => {
            (Vec::new(), family.xid_type())
        }
        SnapshotFunc::CurrentSnapshot => (Vec::new(), family.snapshot_type()),
        SnapshotFunc::SnapshotXmin | SnapshotFunc::SnapshotXmax => {
            (vec![family.snapshot_type()], family.xid_type())
        }
        SnapshotFunc::VisibleInSnapshot => (
            vec![family.xid_type(), family.snapshot_type()],
            ColumnType::Bool,
        ),
        SnapshotFunc::XactStatus => (vec![family.xid_type()], ColumnType::Text),
    }
}

/// Statically infer the call's result type, validating its arity and argument
/// types.
///
/// # Errors
///
/// 42883 for an unknown name, a bad arity, or an argument of the wrong type.
pub(crate) fn snapshot_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let (f, family) = snapshot_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let (params, result) = signature(f, family);
    if args.len() != params.len() {
        return Err(wrong_signature(fc, args, scope));
    }
    for (arg, want) in args.iter().zip(&params) {
        if !accepts(arg, *want, scope)? {
            return Err(wrong_signature(fc, args, scope));
        }
    }
    Ok(result)
}

/// Does `arg` satisfy a parameter declared `want`?
///
/// An `unknown` literal always does — the parameter type is what its input
/// function will be. A narrower integer does too where `bigint` is wanted,
/// because `pg_cast` makes `int2 → int8` and `int4 → int8` implicit and
/// `txid_visible_in_snapshot(id, …)` over a `generate_series` column depends on
/// it. Nothing widens into `xid8`: it has no implicit cast from anything.
fn accepts(arg: &Expr, want: ColumnType, scope: &Scope) -> Result<bool, ExecError> {
    if crate::eval::is_unknown_literal(arg) {
        return Ok(true);
    }
    let ty = infer_type(arg, scope)?.storage_type();
    if ty == want.storage_type() {
        return Ok(true);
    }
    Ok(want == ColumnType::Int8
        && matches!(ty, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8))
}

/// 42883 naming the argument types the call actually carried, which is what
/// `PostgreSQL` prints — never a literal `(...)`.
fn wrong_signature(fc: &FuncCall, args: &[Expr], scope: &Scope) -> ExecError {
    let spelled: Vec<&str> = args
        .iter()
        .map(|arg| {
            if crate::eval::is_unknown_literal(arg) {
                "unknown"
            } else {
                infer_type(arg, scope).map_or("unknown", ColumnType::name)
            }
        })
        .collect();
    ExecError::UndefinedFunction(format!(
        "function {}({}) does not exist",
        fc.name,
        spelled.join(", ")
    ))
}

/// Evaluate a transaction-id function call.
///
/// # Errors
///
/// 42883 for a bad arity, 0A000 where no SQL session backs the evaluation,
/// 22P02 for an argument whose text form is not a value of its type, and 22023
/// from `pg_xact_status` for an id the engine has never handed out.
pub(crate) fn eval_snapshot(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let (f, family) = snapshot_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let (params, _) = signature(f, family);
    require_arity(fc, args.len() == params.len())?;
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_child(arg)?);
    }
    // Every one of these is strict, so a NULL argument answers NULL without
    // the transaction state being touched at all.
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let txn = ctx.txn(&fc.name)?;
    match f {
        SnapshotFunc::CurrentXactId => Ok(family.xid_datum(assign_xact_id(txn)?)),
        SnapshotFunc::CurrentXactIdIfAssigned => {
            Ok(assigned_xact_id(txn).map_or(Datum::Null, |xid| family.xid_datum(xid)))
        }
        SnapshotFunc::CurrentSnapshot => {
            // A session holding no transaction took no snapshot, so read the
            // registry now rather than report one it never held.
            let taken = txn
                .snapshot
                .clone()
                .unwrap_or_else(|| txn.procarray.snapshot());
            Ok(Datum::PgSnapshot(Box::new(PgSnapshot::from_running(
                taken.xmin, taken.xmax, &taken.xip,
            ))))
        }
        SnapshotFunc::SnapshotXmin => {
            Ok(family.xid_datum(snapshot_arg(fc, &values[0], ctx)?.xmin()))
        }
        SnapshotFunc::SnapshotXmax => {
            Ok(family.xid_datum(snapshot_arg(fc, &values[0], ctx)?.xmax()))
        }
        SnapshotFunc::VisibleInSnapshot => {
            let xid = xid_arg(fc, &values[0], family, ctx)?;
            let snapshot = snapshot_arg(fc, &values[1], ctx)?;
            Ok(Datum::Bool(snapshot.is_visible(xid)))
        }
        SnapshotFunc::XactStatus => xact_status(ctx, txn, xid_arg(fc, &values[0], family, ctx)?),
    }
}

/// `GetTopFullTransactionId` — the transaction's xid, assigning one when it has
/// none.
///
/// The allocation is staged rather than stored; see the module documentation.
fn assign_xact_id(txn: &TxnRuntime) -> Result<u64, ExecError> {
    if let Some(xid) = txn.own_xid {
        return Ok(xid);
    }
    let mut staged = txn.assigned.lock().expect("assigned xact id mutex");
    if let Some(xid) = *staged {
        return Ok(xid);
    }
    let xid = txn.procarray.begin_write()?;
    *staged = Some(xid);
    Ok(xid)
}

/// `GetTopFullTransactionIdIfAny` — the transaction's xid, or `None` while it
/// has not needed one.
fn assigned_xact_id(txn: &TxnRuntime) -> Option<u64> {
    txn.own_xid
        .or_else(|| *txn.assigned.lock().expect("assigned xact id mutex"))
}

/// `pg_xact_status` over the clog and the running set.
fn xact_status(ctx: &EvalCtx, txn: &TxnRuntime, xid: u64) -> Result<Datum, ExecError> {
    if xid == crabka_pgmvcc::xid::INVALID_XID {
        // `TransactionIdInRecentPast` refuses an invalid id before it looks at
        // anything else, and the caller turns that refusal into NULL.
        return Ok(Datum::Null);
    }
    if xid < crabka_pgmvcc::xid::FIRST_NORMAL_XID {
        // `TransactionIdInRecentPast` accepts a non-normal id straight away —
        // "for non-normal transaction IDs, we can ignore the epoch" — so this
        // precedes the future test rather than following it.
        // `TransactionLogFetch` then answers without reading the clog, and
        // both reserved ids that name a transaction are committed by
        // definition: `BootstrapTransactionId` wrote the initial catalog and
        // `FrozenTransactionId` is every frozen row's always-visible creator.
        return Ok(status_text("committed"));
    }
    // The live registry rather than the statement's snapshot:
    // `TransactionIdInRecentPast` reads `ReadNextFullTransactionId()`, so an id
    // handed out since the statement began is in the past, not the future.
    let live = txn.procarray.snapshot();
    if xid >= live.xmax {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: format!("transaction ID {xid} is in the future"),
        });
    }
    // Running is checked before the clog, as it is for a row: a transaction
    // that has written its commit record and not yet left the running set is
    // still in progress, and reporting it committed would let a reader see it
    // before its own snapshot does.
    if live.xip.binary_search(&xid).is_ok() {
        return Ok(status_text("in progress"));
    }
    let kv = ctx
        .data()
        .ok_or_else(|| ExecError::Unsupported("pg_xact_status requires a SQL session".into()))?;
    if xid < oldest_recorded_xid(kv)? {
        // The entry that would have answered was deleted, so the engine no
        // longer knows and says so. This is `TransactionIdInRecentPast`'s
        // `oldestClogXid` test, and the reason it must not be answered
        // "aborted": an absent entry below the floor is a transaction that
        // committed long ago just as often as one that never did.
        return Ok(Datum::Null);
    }
    Ok(status_text(match crabka_pgmvcc::clog::get(kv, xid)? {
        XidStatus::Committed => "committed",
        // A prepared transaction has not been decided, so from here it is
        // neither of the two outcomes — which is what "in progress" reports.
        XidStatus::Prepared(_) => "in progress",
        // An absent clog entry is a transaction that was handed an id and
        // never recorded an outcome. It is in no snapshot and never will be,
        // so it is aborted in every way a reader can observe.
        XidStatus::Aborted | XidStatus::InProgress => "aborted",
    }))
}

fn status_text(status: &str) -> Datum {
    Datum::Text(status.to_string())
}

/// The lowest id whose outcome the engine still holds — `oldestClogXid`.
///
/// Vacuum deletes every clog entry below the garbage horizon once it has
/// frozen or pruned the versions that referenced them, and it records the
/// floor it truncated to durably. Reading that record is what lets this
/// function distinguish "never committed" from "no longer known", which are
/// the same absent key.
fn oldest_recorded_xid(kv: &dyn crabka_pgkv::Kv) -> Result<u64, ExecError> {
    match kv.get(&crabka_pgkv::key::clog_scan_lo_key())? {
        Some(recorded) if recorded.len() == 8 => Ok(u64::from_be_bytes(
            recorded[..8].try_into().expect("eight bytes make a u64"),
        )),
        // Nothing was ever truncated, so every entry the engine wrote is still
        // there and the floor is the first id it could have written one for.
        _ => Ok(crabka_pgmvcc::xid::FIRST_NORMAL_XID),
    }
}

/// A transaction-id argument, in whichever type the family declares.
///
/// An `unknown` literal arrives as text — `txid_visible_in_snapshot('123', …)`
/// writes no cast — so the parameter type's own input function runs here, and
/// reports the same 22P02 a written cast would.
fn xid_arg(fc: &FuncCall, value: &Datum, family: Family, ctx: &EvalCtx) -> Result<u64, ExecError> {
    match (family, value) {
        (Family::Modern, Datum::Xid8(xid)) => Ok(*xid),
        (Family::Modern, Datum::Text(text)) => Ok(crabka_pgtypes::sysid::uint64_in(text, "xid8")?),
        // `bigint` is signed, and the id is the same 64 bits under either
        // reading, so a negative one names an id past 2^63 rather than failing.
        (Family::Legacy, Datum::Int8(value)) => Ok(value.cast_unsigned()),
        (Family::Legacy, Datum::Int4(value)) => Ok(i64::from(*value).cast_unsigned()),
        (Family::Legacy, Datum::Int2(value)) => Ok(i64::from(*value).cast_unsigned()),
        (Family::Legacy, Datum::Text(_)) => {
            match crabka_pgtypes::cast::cast_in(value, ColumnType::Int8, ctx.output_style())? {
                Datum::Int8(value) => Ok(value.cast_unsigned()),
                other => Err(wrong_arg(fc, &other)),
            }
        }
        (_, other) => Err(wrong_arg(fc, other)),
    }
}

/// A snapshot argument, running `pg_snapshot_in` over an `unknown` literal for
/// the same reason [`xid_arg`] runs `xid8in`.
fn snapshot_arg<'a>(
    fc: &FuncCall,
    value: &'a Datum,
    ctx: &EvalCtx,
) -> Result<Cow<'a, PgSnapshot>, ExecError> {
    match value {
        Datum::PgSnapshot(snapshot) => Ok(Cow::Borrowed(snapshot)),
        Datum::Text(_) => {
            match crabka_pgtypes::cast::cast_in(value, ColumnType::PgSnapshot, ctx.output_style())?
            {
                Datum::PgSnapshot(snapshot) => Ok(Cow::Owned(*snapshot)),
                other => Err(wrong_arg(fc, &other)),
            }
        }
        other => Err(wrong_arg(fc, other)),
    }
}

fn wrong_arg(fc: &FuncCall, value: &Datum) -> ExecError {
    ExecError::UndefinedFunction(format!(
        "function {}({}) does not exist",
        fc.name,
        value.column_type().map_or("unknown", ColumnType::name)
    ))
}

//! `currtid2(relation_name, tid)` — the latest version of the row a `tid`
//! names.
//!
//! `PostgreSQL`'s `currtid_byrelname` opens the named relation, walks the
//! update chain that starts at `tid`, and returns the chain's last link. It is
//! how a client that cached a `ctid` finds where the row moved to, and it is
//! the one built-in that asks a question about *storage identity* rather than
//! about a value.
//!
//! **In Gres the chain never moves.** A row's rowid is the key every version of
//! it hangs under, so an `UPDATE` writes the new version under the same rowid
//! and the row keeps its `ctid` — see [`crate::scope::row_ctid`], which
//! [`crate::exec`] pins on the write path. The last link of the chain that
//! starts at a live `tid` is therefore that same `tid`, and answering with the
//! argument is the correct answer for this storage model rather than a stub.
//! `CLUSTER` is the one statement that moves a row's `ctid`, and it moves it in
//! `PostgreSQL` too: both engines document a `ctid` as valid only until the row
//! is updated or the relation is rewritten, so a caller that asks across a
//! rewrite was already outside the contract in both.
//!
//! What is *not* trivial is everything around that answer, and it is what
//! `tid.sql` tests: the name resolves like any written relation name, an index
//! is refused for its kind, a relation with no storage is refused for having
//! none, a view is resolved to the relation its `ctid` column comes from, and a
//! `tid` outside the relation's address space is refused rather than echoed.
//!
//! # Which `tid`s a relation has
//!
//! `PostgreSQL` answers that from the heap file: `heapam_tuple_tid_valid` takes
//! any `tid` whose block is below the relation's current length, whatever is or
//! is not stored in that block. Gres has no file whose length outlives its
//! rows, so the analogue is the address space its rows actually occupy: a `tid`
//! is valid exactly while some live row of the relation sits at or above it.
//! The two agree on the cases that matter — an empty relation has no valid
//! `tid`, a populated one does, and a `TRUNCATE` takes them all away — and
//! diverge only where `PostgreSQL` keeps a page a row no longer needs, after a
//! `DELETE` or an aborted `INSERT`. There Gres reports the tid as invalid where
//! `PostgreSQL` reports the tid back. That is the safe direction: a visible
//! error instead of a `tid` naming storage that holds nothing.
//!
//! One divergence is worth naming separately. The probe reads committed state,
//! as `PostgreSQL`'s does (`GetLatestSnapshot`), but it has no read of the
//! caller's own transaction: a row this transaction inserted and has not
//! committed does not extend the address space here, and does in `PostgreSQL`.
//! Expression evaluation carries no xid to check against — see
//! [`EvalCtx`](crate::clock::EvalCtx) — and inventing one to guess with would
//! trade a visible refusal for a wrong `tid`.

use crabka_pgcatalog::{RelationName, Table};
use crabka_pgkv::Kv;
use crabka_pgparser::ast::{Expr, FuncCall, QueryBody, SelectItem, SetExpr, Statement, TableExpr};
use crabka_pgtypes::{ColumnType, Datum, Tid};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    func::{checked_args, require_arity, undefined_function},
    privilege::{Privilege, PrivilegeCtx, RelationKind},
    relname::SchemaDisposition,
    scope::Scope,
};

/// How deep a chain of views the `ctid` column may be followed through.
///
/// A view's body cannot name itself — [`crate::viewdeps`] refuses the cycle at
/// `CREATE` — so this bound is never reached by a well-formed catalog. It is
/// here so a catalog that is not well-formed reports a refusal rather than
/// recursing until the stack ends.
const MAX_VIEW_DEPTH: usize = 32;

/// Is `name` a member of this family?
pub(crate) fn is_tid_func(name: &str) -> bool {
    name.eq_ignore_ascii_case("currtid2")
}

/// Statically infer the call's result type, and validate the arity.
///
/// # Errors
///
/// 42883 for an unknown name or a bad arity.
pub(crate) fn tid_func_result_type(fc: &FuncCall, _scope: &Scope) -> Result<ColumnType, ExecError> {
    if !is_tid_func(&fc.name) {
        return Err(undefined_function(&fc.name));
    }
    require_arity(fc, checked_args(fc)?.len() == 2)?;
    Ok(ColumnType::Tid)
}

/// Evaluate `currtid2(relation_name, tid)`.
///
/// # Errors
///
/// 42883 for a bad arity, 42P01 for a name nothing answers to, 42809 for an
/// index, 0A000 for a relation with no storage and for a view whose `ctid`
/// cannot be traced, and 22023 for a `tid` the relation's address space does
/// not reach.
pub(crate) fn eval_tid(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let args = checked_args(fc)?;
    require_arity(fc, args.len() == 2)?;
    let relation = eval_child(&args[0])?;
    let tid = eval_child(&args[1])?;
    // `pg_proc` leaves `proisstrict` at its default, so a NULL argument answers
    // NULL without opening anything.
    if relation == Datum::Null || tid == Datum::Null {
        return Ok(Datum::Null);
    }
    let written = text_arg(&relation)?;
    let tid = tid_arg(&tid, ctx)?;
    let catalog = ctx
        .catalog()
        .ok_or_else(|| unsupported("currtid2 requires a catalog"))?;
    let data = ctx.data().unwrap_or(catalog);
    latest_tid(catalog, data, ctx, written, tid, 0).map(Datum::Tid)
}

/// The relation `written` names, and the latest `tid` of the row `tid` names in
/// it.
///
/// The order of the refusals is `PostgreSQL`'s `currtid_internal`: the kind
/// check belongs to opening the relation and comes first, then the caller's
/// `SELECT` privilege on it, then the view rewrite, then storage.
fn latest_tid(
    catalog: &dyn Kv,
    data: &dyn Kv,
    ctx: &EvalCtx,
    written: &str,
    tid: Tid,
    depth: usize,
) -> Result<Tid, ExecError> {
    let scope = ctx.resolution();
    let parsed = crate::relname::parse_written_relation(scope, written)?;
    let name = crate::relname::resolve_relation(
        catalog,
        scope,
        &parsed.reference,
        SchemaDisposition::Utility,
    )?;
    if let Some(error) = crate::exec::open_wrong_kind(catalog, &name) {
        return Err(error);
    }
    let kind =
        crate::exec::relation_kind(catalog, &name).ok_or_else(|| parsed.undefined_table())?;
    match kind {
        "view" => {
            let view = crabka_pgcatalog::get_view(catalog, &name)?;
            require_select(catalog, ctx, &view.name, &view.owner, RelationKind::View)?;
            view_latest_tid(catalog, data, ctx, &view, tid, depth)
        }
        // A sequence is one relation-sized row of state, which `PostgreSQL`
        // stores as a single tuple on a single page. Only that page exists, and
        // the row on it never moves. Gres keeps a sequence's state in the
        // catalog rather than in a row space, so the answer is read off the
        // shape rather than probed for — and there is no ACL to consult,
        // because Gres records no grants on a sequence.
        "sequence" => {
            if tid.offset == 0 || tid.block > 0 {
                return Err(invalid_tid(tid, &name));
            }
            Ok(tid)
        }
        _ => {
            let table = crabka_pgcatalog::get_table(catalog, &name).map_err(|_| {
                // Every other kind reaching here is a relation the catalog
                // synthesises, which has rows to read and no storage they are
                // read from.
                no_storage(&name)
            })?;
            require_select(catalog, ctx, &table.name, &table.owner, RelationKind::Table)?;
            table_latest_tid(catalog, data, &table, tid)
        }
    }
}

/// The latest `tid` in a relation that stores its own rows.
fn table_latest_tid(
    catalog: &dyn Kv,
    data: &dyn Kv,
    table: &Table,
    tid: Tid,
) -> Result<Tid, ExecError> {
    if table.foreign.is_some() || crate::partition::is_partitioned(catalog, &table.name)? {
        // Neither holds a row of its own: a foreign table's rows are the remote
        // side's, and a partitioned table's are its leaves'. `PostgreSQL`
        // refuses both for the same reason, in the same words.
        return Err(no_storage(&table.name));
    }
    if crate::exec::table_uses_global_visibility(table) {
        // A sharded relation's hidden rowids are timestamps drawn from a lease
        // rather than a per-table counter, so they do not lay out over a block
        // and an offset the way [`crate::scope::row_ctid`] lays a rowid out.
        // `TRUNCATE` refuses them for the neighbouring reason.
        return Err(ExecError::Unsupported(
            "currtid2 on sharded tables is not supported".into(),
        ));
    }
    valid_in_relation(data, table, tid)?;
    Ok(tid)
}

/// Follow a view's `ctid` column to the relation it is a column of, and answer
/// there.
///
/// `PostgreSQL`'s `currtid_for_view` reads the view's rewrite rule and requires
/// the target entry at the `ctid` column's position to be a `Var` naming a
/// range-table entry's own `ctid`. Gres stores a view's body as text, so the
/// same requirement is read off the parsed body: the entry at that position has
/// to be a bare `ctid` column reference, and the body's `FROM` has to be the one
/// relation it can be a column of.
fn view_latest_tid(
    catalog: &dyn Kv,
    data: &dyn Kv,
    ctx: &EvalCtx,
    view: &crabka_pgcatalog::View,
    tid: Tid,
    depth: usize,
) -> Result<Tid, ExecError> {
    let ordinal = view
        .columns
        .iter()
        .position(|column| column.name == crate::scope::CTID_COLUMN)
        .ok_or_else(|| unsupported("currtid cannot handle views with no CTID"))?;
    // The type is checked before the body is, and before the column is traced
    // anywhere: `CREATE VIEW v AS SELECT 1 AS ctid` declares a column of that
    // name that is not one.
    if view.columns[ordinal].ty != ColumnType::Tid {
        return Err(unsupported("ctid isn't of type TID"));
    }
    if depth >= MAX_VIEW_DEPTH {
        return Err(unsupported("currtid cannot handle this view"));
    }
    let source =
        ctid_source(view, ordinal).ok_or_else(|| unsupported("currtid cannot handle this view"))?;
    latest_tid(catalog, data, ctx, &source, tid, depth + 1)
}

/// The relation a view's `ctid` output column is the `ctid` *of*, written as a
/// name [`latest_tid`] can resolve, or `None` when the body does not select one
/// relation's own `ctid` into that position.
fn ctid_source(view: &crabka_pgcatalog::View, ordinal: usize) -> Option<String> {
    let statements = crabka_pgparser::parse(&view.definition).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return None;
    };
    let [
        TableExpr::Table {
            name: source,
            alias,
            ..
        },
    ] = select.from.as_slice()
    else {
        return None;
    };
    // A wildcard stands for a run of columns rather than one, so it breaks the
    // position-for-position pairing this walk depends on. It also never
    // *produces* a `ctid`: a system column is in no expansion of a relation's
    // columns, so a view with a `ctid` column always wrote that column out.
    if select
        .projection
        .iter()
        .any(|item| !matches!(item, SelectItem::Expr { .. }))
    {
        return None;
    }
    let SelectItem::Expr { expr, .. } = select.projection.get(ordinal)? else {
        return None;
    };
    let qualifier = alias.as_deref().unwrap_or(&source.name);
    match expr {
        Expr::Column { table, name }
            if name == crate::scope::CTID_COLUMN
                && table.as_deref().is_none_or(|written| written == qualifier) =>
        {
            Some(match &source.schema {
                Some(schema) => format!(
                    "{}.{}",
                    crate::catalog_fn::quote_identifier(schema),
                    crate::catalog_fn::quote_identifier(&source.name)
                ),
                None => crate::catalog_fn::quote_identifier(&source.name),
            })
        }
        _ => None,
    }
}

/// `Ok(())` when `tid` names storage this relation reaches, and the 22023
/// `PostgreSQL` raises when it does not.
fn valid_in_relation(data: &dyn Kv, table: &Table, tid: Tid) -> Result<(), ExecError> {
    if tid.offset == 0 {
        // `(n,0)` is `PostgreSQL`'s invalid item pointer, which no relation ever
        // holds a row at.
        return Err(invalid_tid(tid, &table.name));
    }
    let first = crate::scope::first_identity_in_block(tid.block);
    let snapshot = crabka_pgmvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    let global = crate::NO_GLOBAL_SNAPSHOT();
    let reached = crate::exec::scan_live_interval(
        data,
        data,
        &global,
        &snapshot,
        None,
        table,
        crate::scanner::RowInterval::from(first..),
    )?;
    if reached.is_empty() {
        return Err(invalid_tid(tid, &table.name));
    }
    Ok(())
}

/// `tid (b, o) is not valid for relation "x"` (22023).
fn invalid_tid(tid: Tid, name: &RelationName) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22023",
        message: format!(
            "tid ({}, {}) is not valid for relation \"{}\"",
            tid.block, tid.offset, name.name
        ),
    }
}

/// `cannot look at latest visible tid for relation "s.x"` (0A000), which
/// `PostgreSQL` writes schema-qualified where every neighbouring refusal writes
/// the bare name.
fn no_storage(name: &RelationName) -> ExecError {
    ExecError::Unsupported(format!(
        "cannot look at latest visible tid for relation \"{}.{}\"",
        crabka_pgcatalog::displayed_schema(&name.schema),
        name.name
    ))
}

fn unsupported(message: &str) -> ExecError {
    ExecError::Unsupported(message.to_string())
}

/// The `SELECT` privilege `PostgreSQL` checks on every relation this function
/// opens, including each view of a chain and the relation under it.
fn require_select(
    catalog: &dyn Kv,
    ctx: &EvalCtx,
    name: &RelationName,
    owner: &str,
    kind: RelationKind,
) -> Result<(), ExecError> {
    // Through the same mapping the enforcement path reads `current_user`
    // with: a session that authenticated as nobody acts as the bootstrap
    // superuser, and a refusal written against the literal `public` would deny
    // it a relation every other statement lets it read.
    let role = crate::catalog_fn::effective_privilege_role(&ctx.current_user);
    crate::privilege::require(
        &PrivilegeCtx::new(catalog, &role),
        name,
        owner,
        kind,
        Privilege::Select,
    )
}

/// The relation-name argument, which the type pass has already accepted as
/// `text`.
fn text_arg(value: &Datum) -> Result<&str, ExecError> {
    match value {
        Datum::Text(text) => Ok(text),
        other => Err(ExecError::TypeMismatch(format!(
            "currtid2 relation name must be text, not {}",
            other.column_type().map_or("unknown", |ty| ty.name())
        ))),
    }
}

/// The `tid` argument.
///
/// An `unknown` literal — `currtid2('t', '(0,1)')`, with no cast written —
/// arrives here as text, because this family's result type is decided without
/// looking at the arguments. `PostgreSQL` coerces it to the declared `tid`
/// parameter during analysis, so the cast is run here instead and reports the
/// same `22P02` a written `'(0,1)'::tid` would.
fn tid_arg(value: &Datum, ctx: &EvalCtx) -> Result<Tid, ExecError> {
    match value {
        Datum::Tid(tid) => Ok(*tid),
        Datum::Text(_) => {
            match crabka_pgtypes::cast::cast_in(value, ColumnType::Tid, ctx.output_style())? {
                Datum::Tid(tid) => Ok(tid),
                other => Err(ExecError::TypeMismatch(format!(
                    "currtid2 argument must be tid, not {}",
                    other.column_type().map_or("unknown", |ty| ty.name())
                ))),
            }
        }
        other => Err(ExecError::TypeMismatch(format!(
            "currtid2 argument must be tid, not {}",
            other.column_type().map_or("unknown", |ty| ty.name())
        ))),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

    use crate::SqlEngine;

    async fn run(engine: &SqlEngine, sql: &str) -> QueryResult {
        engine
            .connect()
            .simple_query(sql)
            .await
            .expect("query ok")
            .pop()
            .expect("one result")
    }

    async fn refuse(engine: &SqlEngine, sql: &str) -> (String, String) {
        let error = engine
            .connect()
            .simple_query(sql)
            .await
            .expect_err("refused");
        (error.code.clone(), error.message.clone())
    }

    fn cell0(result: &QueryResult) -> Option<String> {
        match result {
            QueryResult::Rows { rows, .. } => rows
                .first()
                .and_then(|row| row[0].as_ref())
                .map(|cell: &Cell| String::from_utf8(cell.text.to_vec()).expect("utf8")),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// The claim the whole module rests on: the row's `ctid` is the answer, and
    /// it stays the answer across the `UPDATE` that would move it in
    /// `PostgreSQL`.
    #[tokio::test]
    async fn answers_the_rows_own_tid_across_an_update() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int, b text)").await;
        run(&engine, "INSERT INTO t VALUES (1, 'one'), (2, 'two')").await;
        let before = cell0(&run(&engine, "SELECT ctid FROM t WHERE a = 2").await);
        assert!(before == Some("(0,2)".to_string()));

        assert!(
            cell0(&run(&engine, "SELECT currtid2('t', '(0,2)'::tid)").await)
                == Some("(0,2)".to_string())
        );
        run(&engine, "UPDATE t SET b = 'changed' WHERE a = 2").await;
        assert!(cell0(&run(&engine, "SELECT ctid FROM t WHERE a = 2").await) == before);
        assert!(
            cell0(&run(&engine, "SELECT currtid2('t', '(0,2)'::tid)").await)
                == Some("(0,2)".to_string())
        );
    }

    /// A `tid` the relation's rows do not reach is refused rather than echoed,
    /// and the address space follows the rows rather than a high-water mark: a
    /// relation emptied of its rows reaches nothing.
    #[tokio::test]
    async fn refuses_a_tid_the_relation_does_not_reach() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;

        // Block 0 exists, so every offset in it is answered — `PostgreSQL`
        // checks the block alone.
        assert!(
            cell0(&run(&engine, "SELECT currtid2('t', '(0,9)'::tid)").await)
                == Some("(0,9)".to_string())
        );
        for tid in ["(1,1)", "(0,0)"] {
            let (code, message) =
                refuse(&engine, &format!("SELECT currtid2('t', '{tid}'::tid)")).await;
            assert!(code == "22023");
            assert!(message.contains("is not valid for relation \"t\""));
        }
        run(&engine, "DELETE FROM t").await;
        let (code, _) = refuse(&engine, "SELECT currtid2('t', '(0,1)'::tid)").await;
        assert!(code == "22023");
    }

    /// The name is resolved and dispatched on the relation's kind, each with
    /// the refusal `PostgreSQL` writes for it.
    #[tokio::test]
    async fn dispatches_on_the_relations_kind() {
        let engine = SqlEngine::new();
        run(&engine, "CREATE TABLE t (a int)").await;
        run(&engine, "INSERT INTO t VALUES (1)").await;
        run(&engine, "CREATE INDEX t_a ON t (a)").await;
        run(&engine, "CREATE TABLE p (a int) PARTITION BY RANGE (a)").await;
        run(&engine, "CREATE SEQUENCE s").await;
        run(&engine, "CREATE VIEW v_no_ctid AS SELECT a FROM t").await;
        run(&engine, "CREATE VIEW v_ctid AS SELECT ctid, a FROM t").await;
        run(
            &engine,
            "CREATE VIEW v_fake_ctid AS SELECT 1 AS ctid, 2 AS a",
        )
        .await;

        // A sequence is one row on one page in `PostgreSQL`, and nothing here
        // probes for it.
        assert!(
            cell0(&run(&engine, "SELECT currtid2('s', '(0,1)'::tid)").await)
                == Some("(0,1)".to_string())
        );
        // A view is answered by the relation its `ctid` column comes from,
        // which is also the relation a refusal from underneath names.
        assert!(
            cell0(&run(&engine, "SELECT currtid2('v_ctid', '(0,1)'::tid)").await)
                == Some("(0,1)".to_string())
        );

        let cases = [
            ("t_a", "42809", "cannot open relation \"t_a\""),
            (
                "p",
                "0A000",
                "cannot look at latest visible tid for relation \"public.p\"",
            ),
            (
                "v_no_ctid",
                "0A000",
                "currtid cannot handle views with no CTID",
            ),
            ("v_fake_ctid", "0A000", "ctid isn't of type TID"),
            ("nosuch", "42P01", "relation \"nosuch\" does not exist"),
        ];
        for (relation, code, message) in cases {
            let refusal = refuse(
                &engine,
                &format!("SELECT currtid2('{relation}', '(0,1)'::tid)"),
            )
            .await;
            assert!(refusal == (code.to_string(), message.to_string()));
        }
    }

    /// `PostgreSQL` leaves `proisstrict` at its default, so neither argument is
    /// looked at when the other is NULL.
    #[tokio::test]
    async fn a_null_argument_answers_null() {
        let engine = SqlEngine::new();
        for call in [
            "currtid2(NULL, '(0,1)'::tid)",
            "currtid2('nosuchrelation', NULL)",
        ] {
            assert!(cell0(&run(&engine, &format!("SELECT {call}")).await).is_none());
        }
    }
}

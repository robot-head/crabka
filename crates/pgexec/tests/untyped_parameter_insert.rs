//! An undeclared parameter in an `INSERT` takes the target column's type.
//!
//! `transformInsertStmt` accepts a target entry that is either a `Const` **or**
//! a `Param` whose type is `unknown`:
//!
//! ```c
//! if (tle->expr && (IsA(tle->expr, Const) || IsA(tle->expr, Param)) &&
//!     exprType((Node *) tle->expr) == UNKNOWNOID)
//! ```
//!
//! So `INSERT INTO point_tbl SELECT $1`, from a client that declared no type
//! for `$1`, stores a point -- exactly as the written literal `'(0,0)'` does,
//! and as the `VALUES` spelling of the same row does.
//!
//! # A parameter's type is declared in Parse, not Bind
//!
//! The wire protocol's `Bind` message carries *formats*, not type oids; the
//! types travel in `Parse`. `parse(name, sql, &[TEXT])` is therefore what a
//! client that means `text` actually sends, and it is what these tests use.
//! Attaching an oid to the bound value instead does not declare anything: it
//! exercises the undeclared path while looking like the declared one, and
//! measuring that way reports a defect that is not there.
//!
//! That distinction is the whole rule. A client that declared `text` has said
//! what it means, so `text` into a `point` column stays 42804 through both
//! spellings; only an undeclared parameter takes the column's type. Every
//! expectation below was measured against a live `PostgreSQL` 18.4.

use assert2::assert;
use bytes::Bytes;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{BoundParam, Engine, QueryResult, Session};

fn text_format(value: &str) -> BoundParam {
    BoundParam {
        type_oid: None,
        format: 0,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
    }
}

async fn run(s: &mut SqlSession, sql: &str) {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

/// Parse `sql` declaring `declared` for its parameters, bind one text-format
/// value, and execute. A fresh engine per call, because a parameter type
/// inferred for one prepared statement is session state that would otherwise
/// reach the next and make a later case measure the earlier one.
async fn insert_one(declared: &[u32], sql: &str, value: &str) -> Result<(), String> {
    let (_engine, mut s) = engine_with(&["CREATE TABLE pt (id int4, p point)"]).await;
    s.parse("s", sql, declared)
        .await
        .map_err(|e| e.code.clone())?;
    s.bind("p", "s", &[text_format(value)], &[])
        .await
        .map_err(|e| e.code.clone())?;
    s.execute("p", 0).await.map_err(|e| e.code.clone())?;
    Ok(())
}

/// The value that reached storage, rendered by the column type's output
/// function.
async fn stored_point(sql: &str, value: &str) -> String {
    let (_engine, mut s) = engine_with(&["CREATE TABLE pt (id int4, p point)"]).await;
    s.parse("s", sql, &[]).await.expect("parse");
    s.bind("p", "s", &[text_format(value)], &[])
        .await
        .expect("bind");
    s.execute("p", 0).await.expect("execute");
    match s
        .simple_query("SELECT p FROM pt")
        .await
        .expect("select")
        .remove(0)
    {
        QueryResult::Rows { rows, .. } => rows
            .first()
            .and_then(|row| row.first().cloned())
            .flatten()
            .map_or_else(
                || "NULL".to_string(),
                |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
            ),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Both spellings take the column's type when the client declared none.
#[tokio::test]
async fn an_undeclared_parameter_takes_the_target_columns_type() {
    assert!(insert_one(&[], "INSERT INTO pt VALUES (1, $1)", "(1,2)").await == Ok(()));
    assert!(insert_one(&[], "INSERT INTO pt SELECT 2, $1", "(3,4)").await == Ok(()));
}

/// And the column type's input function parses the value, rather than the text
/// being stored as it arrived: the canonical rendering is what comes back.
#[tokio::test]
async fn the_column_types_input_function_parses_the_value() {
    assert!(stored_point("INSERT INTO pt VALUES (1, $1)", "(1, 2)").await == "(1,2)");
    assert!(stored_point("INSERT INTO pt SELECT 2, $1", "(3, 4)").await == "(3,4)");
}

/// A parameter the client declared `text` is not an `unknown`, and must not be
/// coerced into a `point` through either spelling.
#[tokio::test]
async fn a_parameter_declared_text_is_refused() {
    let text = crabka_pgtypes::oids::TEXT;
    assert!(
        insert_one(&[text], "INSERT INTO pt VALUES (1, $1)", "(1,2)").await
            == Err("42804".to_string())
    );
    assert!(
        insert_one(&[text], "INSERT INTO pt SELECT 2, $1", "(3,4)").await
            == Err("42804".to_string())
    );
}

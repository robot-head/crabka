//! An untyped parameter in an `INSERT` takes the target column's type.
//!
//! `transformInsertStmt` accepts a target entry that is either a `Const` **or**
//! a `Param` whose type is `unknown`:
//!
//! ```c
//! if (tle->expr && (IsA(tle->expr, Const) || IsA(tle->expr, Param)) &&
//!     exprType((Node *) tle->expr) == UNKNOWNOID)
//! ```
//!
//! So a client that sends `$1` with no type oid, and a text-format value, is
//! sending an `unknown` — and `INSERT INTO point_tbl SELECT $1` stores a point,
//! exactly as the written literal `'(0,0)'` does.
//!
//! The distinction that matters is between *no type* and *type text*. A client
//! that declares `$1` as `text` has said what it means, and a `text` value must
//! not be coerced into a `point` column: that stays 42804. A fix that coerces
//! every parameter would satisfy the first half of this file and be worse than
//! the defect.

use assert2::assert;
use bytes::Bytes;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{BoundParam, Engine, QueryResult, Session};

/// A parameter the client sent with no type oid, in the text format — an
/// `unknown` in `PostgreSQL`'s terms.
fn untyped(value: &str) -> BoundParam {
    BoundParam {
        type_oid: None,
        format: 0,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
    }
}

/// The same value, declared `text` by the client.
fn typed_text(value: &str) -> BoundParam {
    BoundParam {
        type_oid: Some(crabka_pgtypes::oids::TEXT),
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

/// Bind `sql` with one parameter and execute it, returning the SQLSTATE if it
/// was refused at either step.
async fn insert_one(
    s: &mut SqlSession,
    name: &str,
    sql: &str,
    param: BoundParam,
) -> Result<(), String> {
    // A distinct statement and portal name per call: reusing one is 42P05
    // duplicate prepared statement, which would mask the answer being measured.
    let portal = format!("{name}_p");
    s.parse(name, sql, &[]).await.map_err(|e| e.code.clone())?;
    s.bind(&portal, name, &[param], &[])
        .await
        .map_err(|e| e.code.clone())?;
    s.execute(&portal, 0).await.map_err(|e| e.code.clone())?;
    Ok(())
}

async fn stored(s: &mut SqlSession, sql: &str) -> Vec<String> {
    match s.simple_query(sql).await.expect(sql).remove(0) {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|c| {
                        c.as_ref().map_or_else(
                            || "NULL".to_string(),
                            |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// The two cases gres already answers as `PostgreSQL` 18.4 does. These guard
/// against a regression while the other two are open.
#[tokio::test]
async fn the_cases_that_already_match_postgresql() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE pt (id int4, p point)"]).await;

    // An untyped parameter through VALUES takes the column's type.
    assert!(
        insert_one(
            &mut s,
            "v",
            "INSERT INTO pt VALUES (1, $1)",
            untyped("(1,2)")
        )
        .await
            == Ok(())
    );
    assert!(stored(&mut s, "SELECT p FROM pt WHERE id = 1").await == ["(1,2)"]);

    // A parameter the client declared `text` is not coerced into a point.
    assert!(
        insert_one(
            &mut s,
            "ts",
            "INSERT INTO pt SELECT 6, $1",
            typed_text("(7,8)")
        )
        .await
            == Err("42804".to_string())
    );
}

/// Measured against a live `PostgreSQL` 18.4 on 2026-08-12:
///
/// | case               | PostgreSQL | gres  |
/// |--------------------|------------|-------|
/// | untyped + `VALUES` | accepted   | accepted |
/// | untyped + `SELECT` | accepted   | **42804** |
/// | `text` + `VALUES`  | **42804**  | accepted |
/// | `text` + `SELECT`  | 42804      | 42804 |
///
/// The two disagreements point in opposite directions, and the `VALUES` path is
/// not the one to copy: it accepts an untyped parameter by ignoring the
/// declared type altogether, so reusing its shape for `SELECT` would spread the
/// over-coercion rather than fix the under-coercion.
#[ignore = "two open defects: SELECT under-coerces an untyped param, VALUES over-coerces a typed one"]
#[tokio::test]
async fn an_untyped_parameter_takes_the_target_columns_type() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE pt (id int4, p point)"]).await;

    assert!(
        insert_one(
            &mut s,
            "v",
            "INSERT INTO pt VALUES (1, $1)",
            untyped("(1,2)")
        )
        .await
            == Ok(())
    );
    assert!(
        insert_one(
            &mut s,
            "sel",
            "INSERT INTO pt SELECT 2, $1",
            untyped("(3,4)")
        )
        .await
            == Ok(())
    );

    // Canonical point rendering, so a value merely stored as its own text
    // would not match.
    assert!(stored(&mut s, "SELECT id,p FROM pt ORDER BY id").await == ["1,(1,2)", "2,(3,4)"]);
}

/// A parameter the client declared `text` is not an `unknown`, and must not be
/// coerced into a `point`. The `VALUES` half of this is the open defect.
#[ignore = "VALUES accepts a text-declared parameter into a point column; PostgreSQL refuses it"]
#[tokio::test]
async fn a_parameter_declared_text_is_still_refused() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE pt (id int4, p point)"]).await;

    assert!(
        insert_one(
            &mut s,
            "tv",
            "INSERT INTO pt VALUES (1, $1)",
            typed_text("(1,2)")
        )
        .await
            == Err("42804".to_string())
    );
    assert!(
        insert_one(
            &mut s,
            "ts",
            "INSERT INTO pt SELECT 2, $1",
            typed_text("(3,4)")
        )
        .await
            == Err("42804".to_string())
    );
}

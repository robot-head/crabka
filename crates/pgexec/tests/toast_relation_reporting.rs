//! `pg_class.reltoastrelid`: which relations `PostgreSQL` gives an out-of-line
//! store, and therefore which ones report a nonzero oid here.
//!
//! `heapam_relation_needs_toast_table` asks three questions in order. A
//! relation with no column that can go out of line never gets a store. One with
//! a column of unbounded width always does. Otherwise the widest tuple the
//! bounded columns can build decides it, against a quarter of a block.
//!
//! crabka stores wide values inline, so no relation this oid names exists. The
//! column still has to answer what `PostgreSQL` answers, because
//! `reltoastrelid <> 0` is how the regression suite asks whether a column can
//! be stored out of line at all.
//!
//! Every expectation here was captured from `postgres:18.4` on a `UTF8`
//! cluster, which is the only encoding crabka has.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut SqlSession, sql: &str) {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

/// Every relation this file creates, as `(relname, has_toast)`. The `t_` prefix
/// keeps `information_schema.tables` and its neighbours out.
async fn toast_flags(s: &mut SqlSession) -> Vec<(String, String)> {
    let result = s
        .simple_query(
            "SELECT relname, reltoastrelid <> 0 FROM pg_class \
             WHERE relname LIKE 't\\_%' ORDER BY relname",
        )
        .await
        .expect("pg_class is readable")
        .pop()
        .expect("one result");
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}");
    };
    let cell = |c: &Option<Cell>| {
        String::from_utf8(c.as_ref().expect("not null").text.to_vec()).expect("utf-8 cell")
    };
    rows.iter()
        .map(|row| (cell(&row[0]), cell(&row[1])))
        .collect()
}

/// The `reltoastrelid` of `t_one` and `t_two`, in that order.
async fn toast_oids(s: &mut SqlSession) -> Vec<String> {
    let result = s
        .simple_query(
            "SELECT reltoastrelid::text FROM pg_class \
             WHERE relname IN ('t_one', 't_two') ORDER BY relname",
        )
        .await
        .expect("pg_class is readable")
        .pop()
        .expect("one result");
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}");
    };
    rows.iter()
        .map(|row| {
            String::from_utf8(row[0].as_ref().expect("not null").text.to_vec()).expect("utf-8 cell")
        })
        .collect()
}

fn expected(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, flag)| ((*name).to_string(), (*flag).to_string()))
        .collect()
}

/// The whole rule in one table.
///
/// The boundary pair is what pins the arithmetic: `varchar(501)` builds a tuple
/// of exactly `TOAST_TUPLE_THRESHOLD` and `varchar(502)` builds one byte-group
/// more. That only lands where it does because `type_maximum_size` counts a
/// `varchar(n)` in *characters* — four bytes each under `UTF8` — rather than in
/// bytes.
#[tokio::test]
async fn a_relation_reports_a_store_exactly_when_postgresql_would_build_one() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for ddl in [
        // No column that can go out of line.
        "create table t_fixed (c point, d int)",
        // Bounded, and far under the threshold.
        "create table t_char1 (c char(1))",
        "create table t_numeric (c numeric(1000,0))",
        "create table t_bit (a bit(16000))",
        // Bounded, and over it.
        "create table t_varchar4000 (c varchar(4000))",
        "create table t_two_varchar (a varchar(500), b varchar(500))",
        // The two sides of the threshold itself.
        "create table t_under (c varchar(501))",
        "create table t_over (c varchar(502))",
        // Unbounded, which settles it without any arithmetic.
        "create table t_text (c text)",
    ] {
        run(&mut s, ddl).await;
    }

    assert!(
        toast_flags(&mut s).await
            == expected(&[
                ("t_bit", "f"),
                ("t_char1", "f"),
                ("t_fixed", "f"),
                ("t_numeric", "f"),
                ("t_over", "t"),
                ("t_text", "t"),
                ("t_two_varchar", "t"),
                ("t_under", "f"),
                ("t_varchar4000", "t"),
            ])
    );
}

/// Which relation *kinds* can carry a store. A partitioned table holds no rows
/// of its own; its partitions hold them all and are measured like any other
/// relation. A materialized view holds its rows in a heap exactly as a table
/// does, so it is measured too.
#[tokio::test]
async fn only_the_kinds_that_hold_rows_report_a_store() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for ddl in [
        "create table t_parted (a int, b text) partition by range (a)",
        "create table t_part partition of t_parted for values from (1) to (10)",
        "create materialized view t_matview as select 'x'::text as c",
        "create view t_view as select 'x'::text as c",
        "create sequence t_seq",
    ] {
        run(&mut s, ddl).await;
    }

    assert!(
        toast_flags(&mut s).await
            == expected(&[
                ("t_matview", "t"),
                ("t_part", "t"),
                ("t_parted", "f"),
                ("t_seq", "f"),
                ("t_view", "f"),
            ])
    );
}

/// A virtual generated column occupies no tuple space, so it neither counts
/// towards the width nor makes a relation need a store on its own. A stored one
/// is an ordinary column and does both.
#[tokio::test]
async fn a_virtual_generated_column_is_not_measured() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for ddl in [
        "create table t_virtual (a int, b text generated always as (a::text) virtual)",
        "create table t_stored (a int, b text generated always as (a::text) stored)",
    ] {
        run(&mut s, ddl).await;
    }

    assert!(toast_flags(&mut s).await == expected(&[("t_stored", "t"), ("t_virtual", "f")]));
}

/// `ADD COLUMN` is what gives a relation a store it did not have, which is the
/// property `create_misc` checks: the tree starts with only bounded columns and
/// gains an unbounded one.
#[tokio::test]
async fn adding_an_unbounded_column_gives_the_relation_a_store() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "create table t_grow (class char, aa int4)").await;
    assert!(toast_flags(&mut s).await == expected(&[("t_grow", "f")]));

    run(&mut s, "alter table t_grow add column a text").await;
    assert!(toast_flags(&mut s).await == expected(&[("t_grow", "t")]));
}

/// Distinct relations get distinct oids, and the oid does not move when
/// something unrelated changes. A join back to `pg_class` finds nothing,
/// because crabka builds no relation for it.
#[tokio::test]
async fn the_oid_is_stable_per_relation_and_names_no_relation() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for ddl in ["create table t_one (c text)", "create table t_two (c text)"] {
        run(&mut s, ddl).await;
    }

    let before = toast_oids(&mut s).await;
    assert!(before.len() == 2);
    assert!(before[0] != before[1]);
    assert!(before.iter().all(|oid| oid != "0"));

    run(&mut s, "create table t_three (c text)").await;
    assert!(toast_oids(&mut s).await == before);

    // Nothing in `pg_class` carries one of these oids.
    let result = s
        .simple_query(
            "SELECT count(*)::text FROM pg_class c \
             JOIN pg_class t ON t.oid = c.reltoastrelid",
        )
        .await
        .expect("the join is readable")
        .pop()
        .expect("one result");
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}");
    };
    let count = String::from_utf8(rows[0][0].as_ref().expect("not null").text.to_vec())
        .expect("utf-8 cell");
    assert!(count == "0");
}

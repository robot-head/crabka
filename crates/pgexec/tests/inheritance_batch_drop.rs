//! Dropping several relations in one statement, when inheritance links run
//! between them.
//!
//! The links are rewritten from the *committed* store, so a rewrite that sees
//! one departing relation at a time cannot see the rest of its own batch. Each
//! case here drops a set and then reads the catalog the way psql's `\d` and
//! `pg_inherits` read it, because a parent list naming a relation that is gone
//! does not fail at the statement that wrote it — it fails at every later read
//! of `pg_inherits`, for every relation in the database.
//!
//! # A divergence these tests pin rather than assert
//!
//! `PostgreSQL` 18.4 refuses to drop an inheritance parent whose child is not
//! going too: `DROP TABLE a` over a child `c` is
//! `cannot drop table a because other objects depend on it`, and `CASCADE`
//! takes `c` with it. This engine instead keeps `c` and takes the parent out of
//! its list. That is a separate divergence, out of scope here; these tests pin
//! the current behaviour so that the *metadata* is coherent either way.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// Everything one statement can produce, as a single comparable value, so a
/// case states its whole expected script instead of a chain of field
/// assertions.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Tag(String),
    Rows(Vec<Vec<Option<String>>>),
    Error { code: String, message: String },
}

fn tag(text: &str) -> Outcome {
    Outcome::Tag(text.to_string())
}

fn rows(values: &[&[&str]]) -> Outcome {
    Outcome::Rows(
        values
            .iter()
            .map(|row| row.iter().map(|v| Some((*v).to_string())).collect())
            .collect(),
    )
}

fn empty() -> Outcome {
    Outcome::Rows(Vec::new())
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn outcome(session: &mut SqlSession, sql: &str) -> Outcome {
    match session.simple_query(sql).await {
        Err(err) => Outcome::Error {
            code: err.code,
            message: err.message,
        },
        Ok(results) => match results.into_iter().next() {
            Some(QueryResult::Command { tag }) => Outcome::Tag(tag),
            Some(QueryResult::Rows { rows, .. }) => Outcome::Rows(
                rows.iter()
                    .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
                    .collect(),
            ),
            other => panic!("unexpected result for {sql}: {other:?}"),
        },
    }
}

struct Case {
    why: &'static str,
    setup: &'static [&'static str],
    script: &'static [&'static str],
    expect: Vec<Outcome>,
}

async fn run_cases(cases: Vec<Case>) {
    for case in cases {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        for sql in case.setup {
            session
                .simple_query(sql)
                .await
                .unwrap_or_else(|err| panic!("setup {sql} failed: {err:?} ({})", case.why));
        }
        let mut actual = Vec::with_capacity(case.script.len());
        for sql in case.script {
            actual.push(outcome(&mut session, sql).await);
        }
        assert!(actual == case.expect, "{}", case.why);
    }
}

/// The whole of `pg_inherits` by name, which is what psql's `\d` reads and what
/// a parent list naming a departed relation makes fail.
const LINKS: &str = "SELECT ch.relname, pa.relname FROM pg_inherits i \
                     JOIN pg_class ch ON ch.oid = i.inhrelid \
                     JOIN pg_class pa ON pa.oid = i.inhparent \
                     ORDER BY 1, 2";

/// One child of two parents, both of which one statement can take away.
const TWO_PARENTS: &[&str] = &[
    "CREATE TABLE par_a (a int4)",
    "CREATE TABLE par_b (b int4)",
    "CREATE TABLE kid () INHERITS (par_a, par_b)",
];

#[tokio::test]
async fn dropping_both_parents_at_once_leaves_the_child_naming_neither() {
    run_cases(vec![Case {
        why: "one Put per departing parent let the last one stand, so the child \
              was left naming whichever parent the batch happened to write first",
        setup: TWO_PARENTS,
        script: &[
            "DROP TABLE par_a, par_b",
            LINKS,
            "SELECT a, b FROM kid",
            "INSERT INTO kid VALUES (1, 2)",
            "SELECT a, b FROM kid",
        ],
        expect: vec![
            tag("DROP TABLE"),
            empty(),
            empty(),
            tag("INSERT 0 1"),
            rows(&[&["1", "2"]]),
        ],
    }])
    .await;
}

#[tokio::test]
async fn dropping_one_of_two_parents_leaves_the_other_link_intact() {
    run_cases(vec![Case {
        why: "the parent that stays keeps its child, and the child keeps only it",
        setup: TWO_PARENTS,
        script: &["DROP TABLE par_a", LINKS, "SELECT b FROM par_b"],
        expect: vec![tag("DROP TABLE"), rows(&[&["kid", "par_b"]]), empty()],
    }])
    .await;
}

#[tokio::test]
async fn a_child_dropped_with_its_parents_leaves_nothing_a_new_relation_can_adopt() {
    // `DROP TABLE kid, par_a, par_b` names every relation involved, which is
    // what `PostgreSQL` requires and what upstream `inherit.sql` writes. The
    // per-parent rewrite still put `kid`'s parent list back *after* `kid`'s own
    // delete. No read reaches that key while `kid` is gone, so the damage only
    // surfaces when some later statement takes the name — and then every read
    // of `pg_inherits` fails, not just one.
    run_cases(vec![Case {
        why: "a departing child must not have its parent list rewritten by a departing parent",
        setup: TWO_PARENTS,
        script: &[
            "DROP TABLE kid, par_a, par_b",
            LINKS,
            "CREATE TABLE kid (unrelated int4)",
            LINKS,
            "SELECT unrelated FROM kid",
        ],
        expect: vec![
            tag("DROP TABLE"),
            empty(),
            tag("CREATE TABLE"),
            empty(),
            empty(),
        ],
    }])
    .await;
}

#[tokio::test]
async fn dropping_a_schema_leaves_nothing_a_recreated_schema_can_adopt() {
    // `DROP SCHEMA … CASCADE` reaches the same rewrite by another road, and it
    // is the road a test suite takes at teardown. The schema's contents come out
    // in name order, so the child is handled before its parents whatever the
    // author wrote.
    run_cases(vec![Case {
        why: "a recreated schema and table name must not inherit the dropped schema's links",
        setup: &[
            "CREATE SCHEMA s",
            "CREATE TABLE s.mmm_par (m int4)",
            "CREATE TABLE s.nnn_par (n int4)",
            "CREATE TABLE s.aaa_kid_first () INHERITS (s.mmm_par, s.nnn_par)",
        ],
        script: &[
            "DROP SCHEMA s CASCADE",
            LINKS,
            "CREATE SCHEMA s",
            "CREATE TABLE s.aaa_kid_first (unrelated int4)",
            LINKS,
        ],
        expect: vec![
            tag("DROP SCHEMA"),
            empty(),
            tag("CREATE SCHEMA"),
            tag("CREATE TABLE"),
            empty(),
        ],
    }])
    .await;
}

#[tokio::test]
async fn a_three_level_tree_dropped_from_the_top_keeps_the_levels_below_it() {
    run_cases(vec![Case {
        why: "removing the top two levels must leave the leaf parentless, not \
              pointing at either of them",
        setup: &[
            "CREATE TABLE lvl_top (t int4)",
            "CREATE TABLE lvl_mid () INHERITS (lvl_top)",
            "CREATE TABLE lvl_leaf () INHERITS (lvl_mid, lvl_top)",
        ],
        script: &[
            LINKS,
            "DROP TABLE lvl_top, lvl_mid",
            LINKS,
            "SELECT t FROM lvl_leaf",
        ],
        expect: vec![
            rows(&[
                &["lvl_leaf", "lvl_mid"],
                &["lvl_leaf", "lvl_top"],
                &["lvl_mid", "lvl_top"],
            ]),
            tag("DROP TABLE"),
            empty(),
            empty(),
        ],
    }])
    .await;
}

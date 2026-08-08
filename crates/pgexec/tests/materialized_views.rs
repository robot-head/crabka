//! Materialized views against a real in-process engine: the lifecycle, the
//! catalog projections, and — the part that is easy to get wrong — every place a
//! relation kind that is neither a table nor a view has to be told apart from
//! both.
//!
//! A materialized view is stored like a table and defined like a view, so each
//! of those two resemblances is a trap. It has heap contents, so the write paths
//! would happily accept a row into one; it has a query, so the view rewriter
//! would happily treat one as auto-updatable. `PostgreSQL` refuses both, in
//! wordings that differ per command, and those wordings are what this file
//! pins.
//!
//! Every SQLSTATE, message and hint asserted here was captured from a live
//! `PostgreSQL` 18.4 server rather than from documentation. Where this engine
//! knowingly diverges the case says so at the assertion.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// Everything one statement can produce, as a single comparable value, so a case
/// states its whole expected script rather than a chain of field assertions.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Tag(String),
    Rows(Vec<Vec<Option<String>>>),
    Error {
        code: String,
        message: String,
        hint: Option<String>,
    },
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

fn error(code: &str, message: &str) -> Outcome {
    Outcome::Error {
        code: code.to_string(),
        message: message.to_string(),
        hint: None,
    }
}

fn hinted(code: &str, message: &str, hint: &str) -> Outcome {
    Outcome::Error {
        code: code.to_string(),
        message: message.to_string(),
        hint: Some(hint.to_string()),
    }
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn outcome(session: &mut SqlSession, sql: &str) -> Outcome {
    match session.simple_query(sql).await {
        Err(err) => Outcome::Error {
            code: err.code,
            message: err.message,
            hint: err.diagnostics.and_then(|fields| fields.hint),
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

/// One scenario: the relations it starts from, and the script whose every
/// outcome is compared as one value.
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

/// A table, a view over it, and nothing else — the starting point almost every
/// case shares.
const BASE: &[&str] = &[
    "CREATE TABLE base (id int4, kind text, amt numeric)",
    "INSERT INTO base VALUES (1,'x',2),(2,'x',3),(3,'y',5)",
    "CREATE VIEW basev AS SELECT kind, sum(amt) AS totamt FROM base GROUP BY kind",
];

#[tokio::test]
async fn a_materialized_view_holds_a_snapshot_that_only_refresh_moves() {
    run_cases(vec![
        Case {
            why: "WITH DATA populates at CREATE and reports the query's row count",
            setup: BASE,
            script: &[
                "CREATE MATERIALIZED VIEW mv AS SELECT kind, sum(amt) AS totamt \
                 FROM base GROUP BY kind",
                "SELECT kind, totamt FROM mv ORDER BY kind",
                "SELECT relkind, relispopulated FROM pg_class WHERE relname = 'mv'",
            ],
            expect: vec![
                tag("SELECT 2"),
                rows(&[&["x", "5"], &["y", "5"]]),
                rows(&[&["m", "t"]]),
            ],
        },
        Case {
            why: "the contents are a snapshot: the base moving does not move them, \
                  and a view over the same query does move",
            setup: BASE,
            script: &[
                "CREATE MATERIALIZED VIEW mv AS SELECT kind, sum(amt) AS totamt \
                 FROM base GROUP BY kind",
                "INSERT INTO base VALUES (4,'y',7)",
                "SELECT kind, totamt FROM mv ORDER BY kind",
                "SELECT kind, totamt FROM basev ORDER BY kind",
                "REFRESH MATERIALIZED VIEW mv",
                "SELECT kind, totamt FROM mv ORDER BY kind",
            ],
            expect: vec![
                tag("SELECT 2"),
                tag("INSERT 0 1"),
                rows(&[&["x", "5"], &["y", "5"]]),
                rows(&[&["x", "5"], &["y", "12"]]),
                tag("REFRESH MATERIALIZED VIEW"),
                rows(&[&["x", "5"], &["y", "12"]]),
            ],
        },
        Case {
            why: "a REFRESH replaces the contents rather than appending to them",
            setup: BASE,
            script: &[
                "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
                "REFRESH MATERIALIZED VIEW mv",
                "REFRESH MATERIALIZED VIEW mv",
                "SELECT id FROM mv ORDER BY id",
            ],
            expect: vec![
                tag("SELECT 3"),
                tag("REFRESH MATERIALIZED VIEW"),
                tag("REFRESH MATERIALIZED VIEW"),
                rows(&[&["1"], &["2"], &["3"]]),
            ],
        },
        Case {
            why: "a materialized view may read a view, and another materialized view",
            setup: BASE,
            script: &[
                "CREATE MATERIALIZED VIEW mv1 AS SELECT kind, totamt FROM basev",
                "CREATE MATERIALIZED VIEW mv2 AS SELECT sum(totamt) AS grand FROM mv1",
                "SELECT grand FROM mv2",
            ],
            expect: vec![tag("SELECT 2"), tag("SELECT 1"), rows(&[&["10"]])],
        },
    ])
    .await;
}

/// The unpopulated state is the one thing a materialized view has that no other
/// relation does, and the refusal has to survive every read path — including the
/// aggregate pushdowns, where a fold over the row space would otherwise answer
/// zero for a relation the general scan refuses.
#[tokio::test]
async fn an_unpopulated_materialized_view_is_an_error_to_read_on_every_path() {
    let unpopulated = || {
        hinted(
            "55000",
            "materialized view \"mv\" has not been populated",
            "Use the REFRESH MATERIALIZED VIEW command.",
        )
    };
    run_cases(vec![
        Case {
            why: "WITH NO DATA leaves it unpopulated, and every shape of read is 55000",
            setup: BASE,
            script: &[
                "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base WITH NO DATA",
                "SELECT relispopulated FROM pg_class WHERE relname = 'mv'",
                "SELECT * FROM mv",
                "SELECT id FROM mv",
                "SELECT count(*) FROM mv",
                "SELECT max(id) FROM mv",
                "SELECT * FROM ONLY mv",
                "SELECT 1 FROM mv WHERE id > 0",
                "SELECT id FROM mv UNION SELECT id FROM base",
                "SELECT (SELECT count(*) FROM mv)",
            ],
            expect: vec![
                tag("CREATE MATERIALIZED VIEW"),
                rows(&[&["f"]]),
                unpopulated(),
                unpopulated(),
                unpopulated(),
                unpopulated(),
                unpopulated(),
                unpopulated(),
                unpopulated(),
                unpopulated(),
            ],
        },
        Case {
            why: "REFRESH populates it; REFRESH WITH NO DATA empties it and makes it \
                  an error to read again — the one way back",
            setup: BASE,
            script: &[
                "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base WITH NO DATA",
                "REFRESH MATERIALIZED VIEW mv",
                "SELECT count(*) FROM mv",
                "REFRESH MATERIALIZED VIEW mv WITH NO DATA",
                "SELECT relispopulated FROM pg_class WHERE relname = 'mv'",
                "SELECT count(*) FROM mv",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
                "SELECT count(*) FROM mv",
            ],
            expect: vec![
                tag("CREATE MATERIALIZED VIEW"),
                tag("REFRESH MATERIALIZED VIEW"),
                rows(&[&["3"]]),
                tag("REFRESH MATERIALIZED VIEW"),
                rows(&[&["f"]]),
                unpopulated(),
                tag("REFRESH MATERIALIZED VIEW"),
                rows(&[&["3"]]),
            ],
        },
    ])
    .await;
}

/// A materialized view is not auto-updatable and has no rewrite rules, so every
/// write against one is refused — in the wording the command that made it uses.
#[tokio::test]
async fn writes_against_a_materialized_view_are_refused() {
    let cannot_change = || error("42809", "cannot change materialized view \"mv\"");
    run_cases(vec![Case {
        why: "the DML family, MERGE, TRUNCATE, COPY FROM and a data-modifying CTE \
              all refuse; reading out of one is still fine",
        setup: &[
            "CREATE TABLE base (id int4, kind text, amt numeric)",
            "INSERT INTO base VALUES (1,'x',2)",
            "CREATE MATERIALIZED VIEW mv AS SELECT id, kind FROM base",
        ],
        script: &[
            "INSERT INTO mv VALUES (9, 'q')",
            "INSERT INTO mv SELECT id, kind FROM base",
            "UPDATE mv SET kind = 'q'",
            "DELETE FROM mv",
            "TRUNCATE mv",
            "MERGE INTO mv t USING base b ON t.id = b.id WHEN MATCHED THEN DELETE",
            "COPY mv FROM STDIN",
            "WITH w AS (INSERT INTO mv VALUES (9,'q') RETURNING id) SELECT id FROM w",
            "SELECT id, kind FROM mv ORDER BY id",
        ],
        expect: vec![
            cannot_change(),
            cannot_change(),
            cannot_change(),
            cannot_change(),
            // TRUNCATE words its refusal like DROP TABLE's but emits no hint.
            error("42809", "\"mv\" is not a table"),
            error("42809", "cannot execute MERGE on relation \"mv\""),
            error("42809", "cannot copy to materialized view \"mv\""),
            cannot_change(),
            rows(&[&["1", "x"]]),
        ],
    }])
    .await;
}

/// The relation-kind refusals, in both directions: a command aimed at a
/// materialized view that is not one, and a command aimed at something else that
/// finds one.
#[tokio::test]
async fn commands_that_name_the_wrong_relation_kind_are_refused() {
    run_cases(vec![
        Case {
            why: "DROP names the kind it was asked for and hints at the kind it found",
            setup: &[
                "CREATE TABLE base (id int4)",
                "CREATE VIEW basev AS SELECT id FROM base",
                "CREATE SEQUENCE baseq",
                "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
            ],
            script: &[
                "DROP VIEW mv",
                "DROP TABLE mv",
                "DROP SEQUENCE mv",
                "DROP MATERIALIZED VIEW base",
                "DROP MATERIALIZED VIEW basev",
                "DROP MATERIALIZED VIEW baseq",
            ],
            expect: vec![
                hinted(
                    "42809",
                    "\"mv\" is not a view",
                    "Use DROP MATERIALIZED VIEW to remove a materialized view.",
                ),
                hinted(
                    "42809",
                    "\"mv\" is not a table",
                    "Use DROP MATERIALIZED VIEW to remove a materialized view.",
                ),
                hinted(
                    "42809",
                    "\"mv\" is not a sequence",
                    "Use DROP MATERIALIZED VIEW to remove a materialized view.",
                ),
                hinted(
                    "42809",
                    "\"base\" is not a materialized view",
                    "Use DROP TABLE to remove a table.",
                ),
                hinted(
                    "42809",
                    "\"basev\" is not a materialized view",
                    "Use DROP VIEW to remove a view.",
                ),
                hinted(
                    "42809",
                    "\"baseq\" is not a materialized view",
                    "Use DROP SEQUENCE to remove a sequence.",
                ),
            ],
        },
        Case {
            why: "REFRESH refuses in two wordings and neither carries a hint: 0A000 for a \
                  relation with a heap, 42809 for one without",
            setup: &[
                "CREATE TABLE base (id int4)",
                "CREATE VIEW basev AS SELECT id FROM base",
                "CREATE SEQUENCE baseq",
            ],
            script: &[
                "REFRESH MATERIALIZED VIEW base",
                "REFRESH MATERIALIZED VIEW basev",
                "REFRESH MATERIALIZED VIEW baseq",
                "REFRESH MATERIALIZED VIEW nope",
            ],
            expect: vec![
                error("0A000", "\"base\" is not a materialized view"),
                error("42809", "\"basev\" is not a table or materialized view"),
                error("42809", "\"baseq\" is not a table or materialized view"),
                error("42P01", "relation \"nope\" does not exist"),
            ],
        },
        Case {
            why: "a refused REFRESH empties nothing: the check runs before the heap is \
                  touched",
            setup: &[
                "CREATE TABLE base (id int4)",
                "INSERT INTO base VALUES (1),(2)",
            ],
            script: &[
                "REFRESH MATERIALIZED VIEW base",
                "SELECT id FROM base ORDER BY id",
            ],
            expect: vec![
                error("0A000", "\"base\" is not a materialized view"),
                rows(&[&["1"], &["2"]]),
            ],
        },
        Case {
            why: "IF EXISTS waives a missing name but never a wrong kind",
            setup: &["CREATE TABLE base (id int4)"],
            script: &[
                "DROP MATERIALIZED VIEW IF EXISTS nope",
                "DROP MATERIALIZED VIEW nope",
                "DROP MATERIALIZED VIEW IF EXISTS base",
            ],
            expect: vec![
                tag("DROP MATERIALIZED VIEW"),
                error("42P01", "materialized view \"nope\" does not exist"),
                hinted(
                    "42809",
                    "\"base\" is not a materialized view",
                    "Use DROP TABLE to remove a table.",
                ),
            ],
        },
    ])
    .await;
}

/// A materialized view depends on what its query reads, and is depended on by
/// whatever reads it — both directions have to reach the same `CASCADE` machinery
/// a view uses, and each object has to be named by its own kind.
#[tokio::test]
async fn a_materialized_view_participates_in_the_dependency_chain() {
    run_cases(vec![
        Case {
            why: "a materialized view over a table blocks DROP TABLE and goes with a \
                  CASCADE",
            setup: &[
                "CREATE TABLE base (id int4)",
                "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
            ],
            script: &[
                "DROP TABLE base",
                "DROP TABLE base CASCADE",
                "SELECT relname FROM pg_class WHERE relname IN ('base','mv')",
            ],
            expect: vec![
                hinted(
                    "2BP01",
                    "cannot drop table base because other objects depend on it",
                    "Use DROP ... CASCADE to drop the dependent objects too.",
                ),
                tag("DROP TABLE"),
                empty(),
            ],
        },
        Case {
            why: "a view and a materialized view over a materialized view both block its \
                  drop, and both go with a CASCADE",
            setup: &[
                "CREATE TABLE base (id int4)",
                "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
                "CREATE VIEW v_on_mv AS SELECT id FROM mv",
                "CREATE MATERIALIZED VIEW mv_on_mv AS SELECT id FROM mv",
            ],
            script: &[
                "DROP MATERIALIZED VIEW mv",
                "DROP MATERIALIZED VIEW mv CASCADE",
                "SELECT relname FROM pg_class WHERE relname IN ('mv','v_on_mv','mv_on_mv')",
            ],
            expect: vec![
                hinted(
                    "2BP01",
                    "cannot drop materialized view mv because other objects depend on it",
                    "Use DROP ... CASCADE to drop the dependent objects too.",
                ),
                tag("DROP MATERIALIZED VIEW"),
                empty(),
            ],
        },
        Case {
            why: "DROP SCHEMA CASCADE takes a materialized view with the schema",
            setup: &[
                "CREATE SCHEMA s",
                "CREATE TABLE s.base (id int4)",
                "CREATE MATERIALIZED VIEW s.mv AS SELECT id FROM s.base",
            ],
            script: &[
                "DROP SCHEMA s CASCADE",
                "SELECT relname FROM pg_class WHERE relname IN ('base','mv')",
            ],
            expect: vec![tag("DROP SCHEMA"), empty()],
        },
    ])
    .await;
}

/// Indexes are ordinary catalog objects over the relation, so `REFRESH` — which
/// empties and refills through the write path that maintains them — must leave
/// every one both present and correct.
#[tokio::test]
async fn refresh_does_not_orphan_an_index_on_a_materialized_view() {
    run_cases(vec![Case {
        why: "a unique index survives a refresh, still enforces, and still answers reads",
        setup: &[
            "CREATE TABLE base (id int4, kind text)",
            "INSERT INTO base VALUES (1,'x'),(2,'y')",
            "CREATE MATERIALIZED VIEW mv AS SELECT id, kind FROM base",
            "CREATE UNIQUE INDEX mv_id ON mv (id)",
        ],
        script: &[
            "SELECT relhasindex FROM pg_class WHERE relname = 'mv'",
            "SELECT hasindexes FROM pg_matviews WHERE matviewname = 'mv'",
            "REFRESH MATERIALIZED VIEW mv",
            "SELECT indexname FROM pg_indexes WHERE tablename = 'mv'",
            "SELECT kind FROM mv WHERE id = 2",
            "REFRESH MATERIALIZED VIEW mv WITH NO DATA",
            "SELECT indexname FROM pg_indexes WHERE tablename = 'mv'",
            "REFRESH MATERIALIZED VIEW mv",
            "SELECT kind FROM mv WHERE id = 1",
        ],
        expect: vec![
            rows(&[&["t"]]),
            rows(&[&["t"]]),
            tag("REFRESH MATERIALIZED VIEW"),
            rows(&[&["mv_id"]]),
            rows(&[&["y"]]),
            tag("REFRESH MATERIALIZED VIEW"),
            rows(&[&["mv_id"]]),
            tag("REFRESH MATERIALIZED VIEW"),
            rows(&[&["x"]]),
        ],
    }])
    .await;
}

/// Which catalog projections a materialized view appears in, and which it must
/// stay out of. `pg_tables`, `pg_views` and the whole of `information_schema`
/// filter on `relkind`, and a new kind that leaks into any of them is a wrong
/// answer for clients that have nothing to do with materialized views.
#[tokio::test]
async fn a_materialized_view_appears_in_exactly_the_right_catalog_projections() {
    run_cases(vec![Case {
        why: "pg_class and pg_matviews report it; pg_tables, pg_views and \
              information_schema do not",
        setup: &[
            "CREATE TABLE base (id int4)",
            "CREATE VIEW basev AS SELECT id FROM base",
            "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
        ],
        script: &[
            "SELECT relname, relkind, relam, relhasrules, relispopulated FROM pg_class \
             WHERE relname IN ('base','basev','mv') ORDER BY relname",
            "SELECT matviewname, matviewowner, hasindexes, ispopulated FROM pg_matviews \
             ORDER BY matviewname",
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
            "SELECT viewname FROM pg_views WHERE schemaname = 'public' ORDER BY viewname",
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = 'public' ORDER BY table_name",
            "SELECT DISTINCT table_name FROM information_schema.columns \
             WHERE table_schema = 'public' ORDER BY table_name",
            "SELECT attname FROM pg_attribute WHERE attrelid = 'mv'::regclass AND attnum > 0 \
             ORDER BY attnum",
        ],
        expect: vec![
            rows(&[
                &["base", "r", "2", "f", "t"],
                &["basev", "v", "0", "t", "t"],
                &["mv", "m", "2", "t", "t"],
            ]),
            rows(&[&["mv", "postgres", "f", "t"]]),
            rows(&[&["base"]]),
            rows(&[&["basev"]]),
            rows(&[&["base", "BASE TABLE"], &["basev", "VIEW"]]),
            rows(&[&["base"], &["basev"]]),
            rows(&[&["id"]]),
        ],
    }])
    .await;
}

/// `pg_matviews.definition` and `pg_get_viewdef` answer for a materialized view
/// exactly as they do for a view, and both name output columns from the
/// relation's own catalog column list rather than from the stored text — which is
/// what makes a renamed column show up in the definition.
#[tokio::test]
async fn a_materialized_view_definition_is_deparsed_like_a_view_definition() {
    run_cases(vec![Case {
        why: "the same query stored as a view and as a materialized view deparses \
              identically, and follows a RENAME COLUMN",
        setup: &[
            "CREATE TABLE base (id int4, kind text)",
            "CREATE VIEW v AS SELECT id, kind FROM base",
            "CREATE MATERIALIZED VIEW mv AS SELECT id, kind FROM base",
        ],
        script: &[
            "SELECT pg_get_viewdef('v'::regclass) = pg_get_viewdef('mv'::regclass)",
            "SELECT definition FROM pg_views WHERE viewname = 'v'",
            "SELECT definition FROM pg_matviews WHERE matviewname = 'mv'",
            "ALTER MATERIALIZED VIEW mv RENAME COLUMN kind TO sort",
            "SELECT definition FROM pg_matviews WHERE matviewname = 'mv'",
        ],
        expect: vec![
            rows(&[&["t"]]),
            rows(&[&[" SELECT id,\n    kind\n   FROM base;"]]),
            rows(&[&[" SELECT id,\n    kind\n   FROM base;"]]),
            // The engine reports ALTER TABLE's tag here; PostgreSQL reports
            // ALTER MATERIALIZED VIEW. The parser routes the statement onto
            // ALTER TABLE deliberately, so every subcommand works, and the tag
            // follows the statement rather than the spelling.
            tag("ALTER TABLE"),
            rows(&[&[" SELECT id,\n    kind AS sort\n   FROM base;"]]),
        ],
    }])
    .await;
}

/// A materialized view is grantable and commentable like any other relation, and
/// its comment has to come back out — which it only does if the lookup asks for
/// the kind the relation actually is.
#[tokio::test]
async fn a_materialized_view_carries_a_comment_and_an_owner() {
    run_cases(vec![Case {
        why: "COMMENT ON MATERIALIZED VIEW round-trips, and the wrong kind word is refused",
        setup: &[
            "CREATE TABLE base (id int4)",
            "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
        ],
        script: &[
            "COMMENT ON MATERIALIZED VIEW mv IS 'hello'",
            "SELECT obj_description('mv'::regclass)",
            "COMMENT ON TABLE mv IS 'wrong kind'",
            "COMMENT ON MATERIALIZED VIEW base IS 'wrong kind'",
            "SELECT obj_description('mv'::regclass)",
        ],
        expect: vec![
            tag("COMMENT"),
            rows(&[&["hello"]]),
            error("42809", "\"mv\" is not a table"),
            error("42809", "\"base\" is not a materialized view"),
            rows(&[&["hello"]]),
        ],
    }])
    .await;
}

/// The failure modes at creation time, including the one that has to leave no
/// trace: a `WITH DATA` create whose query fails at runtime.
#[tokio::test]
async fn a_failed_create_materialized_view_leaves_nothing_behind() {
    run_cases(vec![Case {
        why: "a runtime failure in the query undoes the relation; the ordinary \
              fix-and-retry then works",
        setup: &["CREATE TABLE base (id int4)", "INSERT INTO base VALUES (0)"],
        script: &[
            "CREATE MATERIALIZED VIEW mv AS SELECT 1/id AS q FROM base",
            "SELECT relname FROM pg_class WHERE relname = 'mv'",
            "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
            "SELECT id FROM mv",
        ],
        expect: vec![
            error("22012", "division by zero"),
            empty(),
            tag("SELECT 1"),
            rows(&[&["0"]]),
        ],
    }])
    .await;
}

/// The shape checks `CREATE MATERIALIZED VIEW` shares with `CREATE TABLE AS`
/// and `CREATE VIEW`, which have to fire before anything is created.
#[tokio::test]
async fn create_materialized_view_checks_its_output_shape() {
    run_cases(vec![Case {
        why: "too many column names, duplicate output names, and a name already taken",
        setup: &["CREATE TABLE base (id int4, kind text)"],
        script: &[
            "CREATE MATERIALIZED VIEW mv (a, b, c) AS SELECT id, kind FROM base",
            "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a, 2 AS a",
            "CREATE MATERIALIZED VIEW mv (a, b) AS SELECT id, kind FROM base",
            "SELECT a, b FROM mv ORDER BY a",
            "CREATE MATERIALIZED VIEW mv AS SELECT id FROM base",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT id FROM base",
            "SELECT relkind FROM pg_class WHERE relname = 'mv'",
        ],
        expect: vec![
            error("42601", "too many column names were specified"),
            error("42701", "column \"a\" specified more than once"),
            tag("SELECT 0"),
            empty(),
            error("42P07", "relation \"mv\" already exists"),
            tag("CREATE MATERIALIZED VIEW"),
            rows(&[&["m"]]),
        ],
    }])
    .await;
}

/// Adding a relation kind reaches every projection and drop path in the engine,
/// so the kinds that were already there have to keep answering exactly as they
/// did. Nothing in this test is a materialized view.
#[tokio::test]
async fn the_other_relation_kinds_are_unchanged() {
    run_cases(vec![
        Case {
            why: "relkind, relispopulated and relam for every kind that is not 'm'",
            setup: &[
                "CREATE TABLE t (a int4 PRIMARY KEY, b text)",
                "CREATE VIEW v AS SELECT a FROM t",
                "CREATE SEQUENCE s",
                "CREATE INDEX t_b ON t (b)",
                "CREATE TABLE p (a int4) PARTITION BY RANGE (a)",
            ],
            script: &[
                "SELECT relname, relkind, relispopulated FROM pg_class \
                 WHERE relname IN ('t','v','s','t_b','p') ORDER BY relname",
                "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
                "SELECT viewname FROM pg_views WHERE schemaname='public' ORDER BY viewname",
                "SELECT matviewname FROM pg_matviews",
                "SELECT table_name, table_type FROM information_schema.tables \
                 WHERE table_schema='public' ORDER BY table_name",
            ],
            expect: vec![
                rows(&[
                    &["p", "p", "t"],
                    &["s", "S", "t"],
                    &["t", "r", "t"],
                    &["t_b", "i", "t"],
                    &["v", "v", "t"],
                ]),
                rows(&[&["p"], &["t"]]),
                rows(&[&["v"]]),
                empty(),
                rows(&[&["p", "BASE TABLE"], &["t", "BASE TABLE"], &["v", "VIEW"]]),
            ],
        },
        Case {
            why: "the wrong-kind DROP refusals for the pre-existing kinds name the kind \
                  asked for and hint at the kind found",
            setup: &[
                "CREATE TABLE t (a int4)",
                "CREATE VIEW v AS SELECT a FROM t",
                "CREATE SEQUENCE s",
                "CREATE INDEX t_a ON t (a)",
            ],
            script: &[
                "DROP VIEW t",
                "DROP TABLE v",
                "DROP SEQUENCE t",
                "DROP INDEX t",
                "DROP TABLE s",
                "DROP INDEX v",
            ],
            expect: vec![
                hinted(
                    "42809",
                    "\"t\" is not a view",
                    "Use DROP TABLE to remove a table.",
                ),
                hinted(
                    "42809",
                    "\"v\" is not a table",
                    "Use DROP VIEW to remove a view.",
                ),
                hinted(
                    "42809",
                    "\"t\" is not a sequence",
                    "Use DROP TABLE to remove a table.",
                ),
                hinted(
                    "42809",
                    "\"t\" is not an index",
                    "Use DROP TABLE to remove a table.",
                ),
                hinted(
                    "42809",
                    "\"s\" is not a table",
                    "Use DROP SEQUENCE to remove a sequence.",
                ),
                hinted(
                    "42809",
                    "\"v\" is not an index",
                    "Use DROP VIEW to remove a view.",
                ),
            ],
        },
        Case {
            why: "CREATE TABLE AS keeps both spellings, and WITH NO DATA leaves an \
                  ordinary table that reads as empty rather than erroring",
            setup: &["CREATE TABLE t (a int4)", "INSERT INTO t VALUES (1),(2)"],
            script: &[
                "CREATE TABLE ctas AS SELECT a FROM t",
                "SELECT a FROM ctas ORDER BY a",
                "CREATE TABLE nodata AS SELECT a FROM t WITH NO DATA",
                "SELECT a FROM nodata",
                "SELECT relkind, relispopulated FROM pg_class WHERE relname = 'nodata'",
                "SELECT a INTO into_t FROM t",
                "SELECT a FROM into_t ORDER BY a",
            ],
            expect: vec![
                tag("SELECT 2"),
                rows(&[&["1"], &["2"]]),
                tag("CREATE TABLE AS"),
                empty(),
                rows(&[&["r", "t"]]),
                tag("SELECT 2"),
                rows(&[&["1"], &["2"]]),
            ],
        },
        Case {
            why: "writes through an auto-updatable view still reach the table underneath",
            setup: &[
                "CREATE TABLE t (a int4 PRIMARY KEY, b text)",
                "CREATE VIEW v AS SELECT a, b FROM t WHERE a > 0",
            ],
            script: &[
                "INSERT INTO v VALUES (1, 'one')",
                "UPDATE v SET b = 'ONE' WHERE a = 1",
                "SELECT a, b FROM t ORDER BY a",
                "DELETE FROM v WHERE a = 1",
                "SELECT a, b FROM t",
                "TRUNCATE t",
            ],
            expect: vec![
                tag("INSERT 0 1"),
                tag("UPDATE 1"),
                rows(&[&["1", "ONE"]]),
                tag("DELETE 1"),
                empty(),
                tag("TRUNCATE TABLE"),
            ],
        },
        Case {
            why: "a view over a table still blocks DROP TABLE and still goes with a CASCADE",
            setup: &[
                "CREATE TABLE t (a int4)",
                "CREATE VIEW v AS SELECT a FROM t",
                "CREATE VIEW v2 AS SELECT a FROM v",
            ],
            script: &[
                "DROP TABLE t",
                "DROP TABLE t CASCADE",
                "SELECT relname FROM pg_class WHERE relname IN ('t','v','v2')",
            ],
            expect: vec![
                hinted(
                    "2BP01",
                    "cannot drop table t because other objects depend on it",
                    "Use DROP ... CASCADE to drop the dependent objects too.",
                ),
                tag("DROP TABLE"),
                empty(),
            ],
        },
    ])
    .await;
}

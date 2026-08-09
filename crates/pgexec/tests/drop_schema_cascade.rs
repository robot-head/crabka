//! `DROP SCHEMA … CASCADE` against a real in-process engine, for the
//! dependencies that reach *out* of the dropped schema.
//!
//! Those dependencies are a foreign key declared in another schema, a partition
//! stored in another schema, and a view defined in another schema.
//!
//! These are the cases where a dependency left behind is worse than stale
//! metadata. A surviving foreign key resolves its parent by name, while the
//! parent side of the same constraint is keyed by id. A recreation of the
//! schema and of a same-named table therefore rebinds one half and not the
//! other, so the checks fire but the referential actions do not. A surviving
//! partition keeps a parent link to a deleted relation, and a recreated parent
//! adopts it, so rows are routed into a relation that was never attached to it.
//! Each case therefore drops, recreates, and asserts that the dependency is
//! either wholly gone or wholly live.
//!
//! Every SQLSTATE, message and outcome asserted here was captured from a live
//! `PostgreSQL` 18.4 server, and not from documentation. Where this engine
//! knowingly diverges, the test pins the *current* behaviour and says so at the
//! assertion.

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

fn error(code: &str, message: &str) -> Outcome {
    Outcome::Error {
        code: code.to_string(),
        message: message.to_string(),
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

/// One drop scenario. It holds the schemas and relations the scenario starts
/// from, and the script whose every outcome is compared as one value.
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

/// A parent in the dropped schema and a child outside it.
const CROSS_SCHEMA_FOREIGN_KEY: &[&str] = &[
    "CREATE SCHEMA s",
    "CREATE SCHEMA o",
    "CREATE TABLE s.parent (id int4 PRIMARY KEY)",
    "CREATE TABLE o.child (id int4 PRIMARY KEY, p int4 REFERENCES s.parent (id))",
    "INSERT INTO s.parent VALUES (1)",
    "INSERT INTO o.child VALUES (1, 1)",
];

/// A partitioned parent in the dropped schema and its partition outside it.
const CROSS_SCHEMA_PARTITION: &[&str] = &[
    "CREATE SCHEMA s",
    "CREATE SCHEMA o",
    "CREATE TABLE s.p (id int4) PARTITION BY RANGE (id)",
    "CREATE TABLE o.p1 PARTITION OF s.p FOR VALUES FROM (0) TO (10)",
    "INSERT INTO s.p VALUES (1)",
];

/// A table in the dropped schema and a view over it outside.
const CROSS_SCHEMA_VIEW: &[&str] = &[
    "CREATE SCHEMA s",
    "CREATE SCHEMA o",
    "CREATE TABLE s.t (id int4 PRIMARY KEY, v int4)",
    "CREATE VIEW o.v AS SELECT * FROM s.t",
];

// ---------------------------------------------------------------------------
// Foreign keys reaching in from another schema
// ---------------------------------------------------------------------------

/// `CASCADE` drops the referencing *constraint* and keeps the child table and
/// its rows.
///
/// This is the split `PostgreSQL` makes between a foreign key, which is a
/// dependency of the parent, and the relation that declares it.
#[tokio::test]
async fn cascade_drops_a_foreign_key_declared_in_another_schema() {
    run_cases(vec![
        Case {
            why: "the constraint goes, the child table stays, and the child is writable again",
            setup: CROSS_SCHEMA_FOREIGN_KEY,
            script: &[
                "DROP SCHEMA s CASCADE",
                "SELECT conname FROM pg_constraint WHERE contype = 'f' ORDER BY 1",
                "SELECT id, p FROM o.child ORDER BY id",
                "INSERT INTO o.child VALUES (2, 99)",
                "SELECT id, p FROM o.child ORDER BY id",
            ],
            expect: vec![
                tag("DROP SCHEMA"),
                empty(),
                rows(&[&["1", "1"]]),
                tag("INSERT 0 1"),
                rows(&[&["1", "1"], &["2", "99"]]),
            ],
        },
        Case {
            why: "a child inside the dropped schema goes with the schema, constraint and all",
            setup: &[
                "CREATE SCHEMA s",
                "CREATE TABLE s.parent (id int4 PRIMARY KEY)",
                "CREATE TABLE s.child (id int4 PRIMARY KEY, p int4 REFERENCES s.parent (id))",
            ],
            script: &[
                "DROP SCHEMA s CASCADE",
                "SELECT conname FROM pg_constraint WHERE contype = 'f' ORDER BY 1",
                "SELECT tablename FROM pg_tables WHERE schemaname = 's' ORDER BY 1",
                "SELECT nspname FROM pg_namespace WHERE nspname = 's'",
            ],
            expect: vec![tag("DROP SCHEMA"), empty(), empty(), empty()],
        },
    ])
    .await;
}

/// A recreation of the schema and of a same-named parent must not bring back
/// half a constraint.
///
/// The child side resolves its parent by name and the parent side resolves by
/// id. A leftover record therefore binds its checks to the new table, and the
/// new table has no referencing entry at all. `INSERT` is then policed,
/// `DELETE` is not, and orphan rows appear with nothing to report a
/// violation.
#[tokio::test]
async fn a_recreated_parent_does_not_adopt_a_leftover_foreign_key() {
    run_cases(vec![Case {
        why: "the constraint is wholly gone: no catalog record, no insert check, \
              and a delete that leaves no orphan because nothing references the row",
        setup: CROSS_SCHEMA_FOREIGN_KEY,
        script: &[
            "DROP SCHEMA s CASCADE",
            "CREATE SCHEMA s",
            "CREATE TABLE s.parent (id int4 PRIMARY KEY)",
            "SELECT conname FROM pg_constraint WHERE contype = 'f' ORDER BY 1",
            // A value absent from the recreated parent: a half-live constraint
            // would raise 23503 here.
            "INSERT INTO o.child VALUES (2, 99)",
            "INSERT INTO s.parent VALUES (7)",
            "INSERT INTO o.child VALUES (3, 7)",
            // A half-live constraint would leave this unpoliced too, because the
            // recreated parent has no reverse index entry.
            "DELETE FROM s.parent WHERE id = 7",
            "SELECT id, p FROM o.child ORDER BY id",
        ],
        expect: vec![
            tag("DROP SCHEMA"),
            tag("CREATE SCHEMA"),
            tag("CREATE TABLE"),
            empty(),
            tag("INSERT 0 1"),
            tag("INSERT 0 1"),
            tag("INSERT 0 1"),
            tag("DELETE 1"),
            rows(&[&["1", "1"], &["2", "99"], &["3", "7"]]),
        ],
    }])
    .await;
}

// ---------------------------------------------------------------------------
// Partitions stored in another schema
// ---------------------------------------------------------------------------

/// A partition has no independent existence, so it goes with its parent even
/// when it is stored elsewhere. `PostgreSQL` drops it. A partition left behind
/// would leak a relation whose parent link names a deleted table.
#[tokio::test]
async fn cascade_drops_a_partition_stored_in_another_schema() {
    run_cases(vec![Case {
        why: "the partition and its rows go with the parent, not just the parent's own metadata",
        setup: CROSS_SCHEMA_PARTITION,
        script: &[
            "DROP SCHEMA s CASCADE",
            "SELECT tablename FROM pg_tables WHERE schemaname = 'o' ORDER BY 1",
            "SELECT id FROM o.p1 ORDER BY id",
        ],
        expect: vec![
            tag("DROP SCHEMA"),
            empty(),
            error("42P01", "relation \"o.p1\" does not exist"),
        ],
    }])
    .await;
}

/// A leftover partition is worse than a leak.
///
/// The dead parent's children index still lists it, so a recreated parent of
/// the same name adopts it. Its bound then blocks a new partition that covers
/// the same range, and rows inserted into the new parent land in a relation
/// that was never attached to it.
#[tokio::test]
async fn a_recreated_parent_does_not_adopt_a_leftover_partition() {
    run_cases(vec![Case {
        why: "the recreated parent starts with no partitions: the same bound is free \
              again, and every row routes into the partition actually attached to it",
        setup: CROSS_SCHEMA_PARTITION,
        script: &[
            "DROP SCHEMA s CASCADE",
            "CREATE SCHEMA s",
            "CREATE TABLE s.p (id int4) PARTITION BY RANGE (id)",
            // A leftover `o.p1` still bound to 0..10 would collide here.
            "CREATE TABLE s.p2 PARTITION OF s.p FOR VALUES FROM (0) TO (10)",
            "INSERT INTO s.p VALUES (5)",
            "SELECT id FROM s.p2 ORDER BY id",
            // Only the new row: a leftover partition would contribute its own.
            "SELECT id FROM s.p ORDER BY id",
        ],
        expect: vec![
            tag("DROP SCHEMA"),
            tag("CREATE SCHEMA"),
            tag("CREATE TABLE"),
            tag("CREATE TABLE"),
            tag("INSERT 0 1"),
            rows(&[&["5"]]),
            rows(&[&["5"]]),
        ],
    }])
    .await;
}

/// The other direction. A partition *in* the dropped schema whose parent is
/// outside it is dropped and detached, and the surviving parent stays
/// consistent.
#[tokio::test]
async fn cascade_detaches_a_partition_from_a_parent_in_another_schema() {
    run_cases(vec![Case {
        why: "the surviving parent loses the partition and the bound it held",
        setup: &[
            "CREATE SCHEMA s",
            "CREATE SCHEMA o",
            "CREATE TABLE o.p (id int4) PARTITION BY RANGE (id)",
            "CREATE TABLE s.p1 PARTITION OF o.p FOR VALUES FROM (0) TO (10)",
            "INSERT INTO o.p VALUES (1)",
        ],
        script: &[
            "DROP SCHEMA s CASCADE",
            "SELECT id FROM o.p ORDER BY id",
            "CREATE TABLE o.p2 PARTITION OF o.p FOR VALUES FROM (0) TO (10)",
            "INSERT INTO o.p VALUES (5)",
            "SELECT id FROM o.p ORDER BY id",
        ],
        expect: vec![
            tag("DROP SCHEMA"),
            empty(),
            tag("CREATE TABLE"),
            tag("INSERT 0 1"),
            rows(&[&["5"]]),
        ],
    }])
    .await;
}

// ---------------------------------------------------------------------------
// Views defined in another schema
// ---------------------------------------------------------------------------

/// A view is a dependency of what it reads, so `CASCADE` drops it outright.
/// This is unlike a foreign key, whose relation survives without the
/// constraint.
#[tokio::test]
async fn cascade_drops_a_view_defined_in_another_schema() {
    run_cases(vec![Case {
        why: "the view goes with the table it reads, rather than surviving as a \
              definition over a relation that no longer exists",
        setup: CROSS_SCHEMA_VIEW,
        script: &[
            "DROP SCHEMA s CASCADE",
            "SELECT viewname FROM pg_views WHERE schemaname = 'o' ORDER BY 1",
            "SELECT id FROM o.v",
        ],
        expect: vec![
            tag("DROP SCHEMA"),
            empty(),
            error("42P01", "relation \"o.v\" does not exist"),
        ],
    }])
    .await;
}

/// A recreation of the schema and of a same-named table must not give the
/// leftover view a new binding. The view is gone, so the name is free for a
/// definition of its own shape.
#[tokio::test]
async fn a_recreated_table_does_not_adopt_a_leftover_view() {
    run_cases(vec![Case {
        why: "the view name is free after the cascade, so a fresh definition takes it",
        setup: CROSS_SCHEMA_VIEW,
        script: &[
            "DROP SCHEMA s CASCADE",
            "CREATE SCHEMA s",
            "CREATE TABLE s.t (id int4 PRIMARY KEY, w int4)",
            "INSERT INTO s.t VALUES (1, 2)",
            "CREATE VIEW o.v AS SELECT * FROM s.t",
            "SELECT id, w FROM o.v ORDER BY id",
        ],
        expect: vec![
            tag("DROP SCHEMA"),
            tag("CREATE SCHEMA"),
            tag("CREATE TABLE"),
            tag("INSERT 0 1"),
            tag("CREATE VIEW"),
            rows(&[&["1", "2"]]),
        ],
    }])
    .await;
}

// ---------------------------------------------------------------------------
// The refusal without CASCADE
// ---------------------------------------------------------------------------

/// Without `CASCADE` a non-empty schema is refused whatever depends on it, and
/// nothing is dropped.
///
/// `PostgreSQL` adds a `DETAIL` that names each dependency, and a `HINT`. This
/// engine reports the message and the SQLSTATE only, which is what the
/// assertion pins.
#[tokio::test]
async fn drop_schema_without_cascade_refuses_and_drops_nothing() {
    const REFUSAL: &str = "cannot drop schema s because other objects depend on it";
    run_cases(vec![
        Case {
            why: "a foreign key from another schema does not make the schema droppable",
            setup: CROSS_SCHEMA_FOREIGN_KEY,
            script: &[
                "DROP SCHEMA s",
                "SELECT id FROM s.parent ORDER BY id",
                "SELECT conname FROM pg_constraint WHERE contype = 'f' ORDER BY 1",
            ],
            expect: vec![
                error("2BP01", REFUSAL),
                rows(&[&["1"]]),
                rows(&[&["child_p_fkey"]]),
            ],
        },
        Case {
            why: "a partition in another schema does not make the schema droppable",
            setup: CROSS_SCHEMA_PARTITION,
            script: &["DROP SCHEMA s", "SELECT id FROM o.p1 ORDER BY id"],
            expect: vec![error("2BP01", REFUSAL), rows(&[&["1"]])],
        },
        Case {
            why: "a view in another schema does not make the schema droppable",
            setup: CROSS_SCHEMA_VIEW,
            script: &[
                "DROP SCHEMA s",
                "SELECT viewname FROM pg_views WHERE schemaname = 'o' ORDER BY 1",
            ],
            expect: vec![error("2BP01", REFUSAL), rows(&[&["v"]])],
        },
    ])
    .await;
}

// ---------------------------------------------------------------------------
// A session's own temporary namespace
// ---------------------------------------------------------------------------

/// `DISCARD TEMP` empties the session's namespace through the same batch, and a
/// permanent view is not collateral.
///
/// Nothing outside a temporary namespace may depend on what is inside it,
/// because a view over a temporary relation is itself temporary.
#[tokio::test]
async fn discarding_temporary_relations_leaves_permanent_views_alone() {
    run_cases(vec![
        Case {
            why: "a permanent view over a same-named permanent table survives a \
                  temporary table shadowing that name",
            setup: &[
                "CREATE TABLE orders (id int4)",
                "INSERT INTO orders VALUES (1)",
                "CREATE VIEW v AS SELECT * FROM orders",
                "CREATE TEMP TABLE orders (id int4)",
            ],
            script: &[
                "DISCARD TEMP",
                "SELECT viewname FROM pg_views WHERE schemaname = 'public' ORDER BY 1",
                "SELECT id FROM v ORDER BY id",
            ],
            expect: vec![tag("DISCARD TEMP"), rows(&[&["v"]]), rows(&[&["1"]])],
        },
        Case {
            why: "a temporary table with a name of its own is no threat either",
            setup: &[
                "CREATE TABLE orders (id int4)",
                "INSERT INTO orders VALUES (1)",
                "CREATE VIEW v AS SELECT * FROM orders",
                "CREATE TEMP TABLE scratch (id int4)",
            ],
            script: &[
                "DISCARD TEMP",
                "SELECT viewname FROM pg_views WHERE schemaname = 'public' ORDER BY 1",
                "SELECT id FROM v ORDER BY id",
            ],
            expect: vec![tag("DISCARD TEMP"), rows(&[&["v"]]), rows(&[&["1"]])],
        },
    ])
    .await;
}

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

/// User types occupy a schema even though they are stored outside the relation
/// key families. A range's generated multirange may occupy a different schema
/// from its primary row, and user types in still-live schemas may depend on
/// either identity. The same cleanup batch also owns temporary namespace type
/// removal, so that path is exercised sequentially here: user-type parser state
/// is process-wide, while each test engine has an independent OID counter.
#[tokio::test]
async fn schema_user_types_require_cascade_and_drop_dependents() {
    const SCHEMA_TYPES: &str = "SELECT n.nspname, t.typname FROM pg_type t \
                                JOIN pg_namespace n ON n.oid = t.typnamespace \
                                WHERE t.typname IN \
                                ('root_e', 'local_range', 'local_multirange', \
                                 'dep_pair', 'external_range', 'generated_mr', \
                                 'range_dep', 'multirange_dep') \
                                ORDER BY 1, 2";
    const TEMP_TYPES: &str = "SELECT typname FROM pg_type WHERE typname IN \
                              ('discard_temp_e', 'discard_temp_dep', \
                               'discard_temp_range', 'discard_temp_multirange') \
                              ORDER BY 1";
    run_cases(vec![
        Case {
            why: "RESTRICT sees type-only contents; CASCADE drops primary and generated type \
                  rows, including dependents in another schema",
            setup: &[
                "CREATE SCHEMA type_drop_s",
                "CREATE SCHEMA type_drop_o",
                "CREATE TYPE type_drop_s.root_e AS ENUM ('x')",
                "CREATE TYPE type_drop_s.local_range AS RANGE (SUBTYPE = int4)",
                "CREATE TYPE type_drop_o.dep_pair AS (value type_drop_s.root_e)",
                "CREATE TYPE type_drop_o.external_range AS RANGE \
                 (SUBTYPE = int4, MULTIRANGE_TYPE_NAME = type_drop_s.generated_mr)",
                "CREATE TYPE type_drop_o.range_dep AS (value type_drop_o.external_range)",
                "CREATE TYPE type_drop_o.multirange_dep AS \
                 (value type_drop_s.local_multirange)",
            ],
            script: &[
                "DROP SCHEMA type_drop_s",
                SCHEMA_TYPES,
                "DROP SCHEMA type_drop_s CASCADE",
                SCHEMA_TYPES,
                "SELECT NULL::type_drop_s.root_e",
                "SELECT NULL::type_drop_s.generated_mr",
                "SELECT NULL::type_drop_o.dep_pair",
            ],
            expect: vec![
                error(
                    "2BP01",
                    "cannot drop schema type_drop_s because other objects depend on it",
                ),
                rows(&[
                    &["type_drop_o", "dep_pair"],
                    &["type_drop_o", "external_range"],
                    &["type_drop_o", "multirange_dep"],
                    &["type_drop_o", "range_dep"],
                    &["type_drop_s", "generated_mr"],
                    &["type_drop_s", "local_multirange"],
                    &["type_drop_s", "local_range"],
                    &["type_drop_s", "root_e"],
                ]),
                tag("DROP SCHEMA"),
                empty(),
                error("42704", "type \"type_drop_s.root_e\" does not exist"),
                error("42704", "type \"type_drop_s.generated_mr\" does not exist"),
                error("42704", "type \"type_drop_o.dep_pair\" does not exist"),
            ],
        },
        Case {
            why: "DISCARD TEMP removes durable rows and parser registry identities for all temp \
                  types",
            setup: &[
                "CREATE TEMP TABLE temp_type_seed (id int4)",
                "CREATE TYPE pg_temp.discard_temp_e AS ENUM ('x')",
                "CREATE TYPE pg_temp.discard_temp_dep AS (value discard_temp_e)",
                "CREATE TYPE pg_temp.discard_temp_range AS RANGE (SUBTYPE = int4)",
            ],
            script: &[
                TEMP_TYPES,
                "DISCARD TEMP",
                TEMP_TYPES,
                "SELECT NULL::discard_temp_e",
                "SELECT NULL::discard_temp_multirange",
            ],
            expect: vec![
                rows(&[
                    &["discard_temp_dep"],
                    &["discard_temp_e"],
                    &["discard_temp_multirange"],
                    &["discard_temp_range"],
                ]),
                tag("DISCARD TEMP"),
                empty(),
                error("42704", "type \"discard_temp_e\" does not exist"),
                error("42704", "type \"discard_temp_multirange\" does not exist"),
            ],
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

/// A `CASCADE` drop reports what it took with it.
///
/// `PostgreSQL` names a single dependent inline and several as a count plus one
/// `DETAIL` line each, ordered depth-first: a view reading another view is
/// reported directly after it, before the next sibling. Every string here was
/// captured from `PostgreSQL` 18.4.
#[tokio::test]
async fn a_cascade_drop_reports_the_objects_it_removes() {
    async fn go(session: &mut SqlSession, sql: &str) {
        session
            .simple_query(sql)
            .await
            .unwrap_or_else(|err| panic!("{sql} failed: {err:?}"));
    }

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let mut notices = session.take_notices().expect("notice receiver");

    // m3 reads m1, so it is reported between the two direct dependents.
    go(&mut session, "CREATE TABLE m(a int, b int)").await;
    go(&mut session, "CREATE VIEW m1 AS SELECT a FROM m").await;
    go(&mut session, "CREATE VIEW m2 AS SELECT b FROM m").await;
    go(&mut session, "CREATE VIEW m3 AS SELECT a FROM m1").await;
    go(&mut session, "DROP TABLE m CASCADE").await;

    let notice = notices.try_recv().expect("a cascade notice");
    assert!(
        notice.message == "drop cascades to 3 other objects",
        "{notice:?}"
    );
    let fields = notice.diagnostics.expect("structured fields");
    assert!(
        fields.detail.as_deref()
            == Some("drop cascades to view m1\ndrop cascades to view m3\ndrop cascades to view m2"),
        "{fields:?}"
    );

    // One dependent is named inline, with no DETAIL at all.
    go(&mut session, "CREATE TABLE s(a int)").await;
    go(&mut session, "CREATE VIEW s1 AS SELECT a FROM s").await;
    go(&mut session, "DROP TABLE s CASCADE").await;
    let notice = notices.try_recv().expect("a cascade notice");
    assert!(notice.message == "drop cascades to view s1", "{notice:?}");
    assert!(notice.diagnostics.is_none(), "{notice:?}");

    // DROP VIEW cascades the same way.
    go(&mut session, "CREATE TABLE v(a int)").await;
    go(&mut session, "CREATE VIEW v1 AS SELECT a FROM v").await;
    go(&mut session, "CREATE VIEW v2 AS SELECT a FROM v1").await;
    go(&mut session, "DROP VIEW v1 CASCADE").await;
    let notice = notices.try_recv().expect("a cascade notice");
    assert!(notice.message == "drop cascades to view v2", "{notice:?}");

    // A drop that cascades to nothing says nothing.
    go(&mut session, "CREATE TABLE q(a int)").await;
    go(&mut session, "DROP TABLE q CASCADE").await;
    assert!(notices.try_recv().is_err());
}

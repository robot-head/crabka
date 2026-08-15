//! `ALTER TABLE`'s column-shape subcommands reach every descendant.
//!
//! A partition or inheritance child that keeps the shape its parent had before
//! an `ADD COLUMN` is not merely stale: every later read of the tree fails with
//! *child table is missing column*, because the parent's row shape can no
//! longer be built from the child's. These tests drive each subcommand from the
//! root of five differently-shaped trees and observe the leaf.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

fn rows_text(r: &QueryResult) -> Vec<Vec<Option<String>>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        o => panic!("expected Rows, got {o:?}"),
    }
}

async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&run(s, sql).await[0])
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

fn rows(values: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
    values
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.map(std::string::ToString::to_string))
                .collect()
        })
        .collect()
}

/// A tree whose root is `root (a int, b int)` and whose deepest relation,
/// `leaf`, already holds the row `(1, 2)` before the `ALTER TABLE` runs.
struct Tree {
    shape: &'static str,
    setup: &'static [&'static str],
}

const TREES: &[Tree] = &[
    Tree {
        // Attached rather than declared `PARTITION OF`, and in the opposite
        // column order, so the recursion cannot be relying on ordinals.
        shape: "partition",
        setup: &[
            "create table root (a int, b int) partition by range (a)",
            "create table leaf (b int, a int)",
            "alter table root attach partition leaf for values from (1) to (10)",
            "insert into root values (1, 2)",
        ],
    },
    Tree {
        // The child carries a local column of its own, which must survive every
        // subcommand aimed at the inherited ones.
        shape: "inheritance",
        setup: &[
            "create table root (a int, b int)",
            "create table leaf (z int) inherits (root)",
            "insert into leaf values (1, 2, 0)",
        ],
    },
    Tree {
        shape: "multi-level partition",
        setup: &[
            "create table root (a int, b int) partition by range (a)",
            "create table mid (a int, b int) partition by range (a)",
            "alter table root attach partition mid for values from (1) to (10)",
            "create table leaf (a int, b int)",
            "alter table mid attach partition leaf for values from (1) to (5)",
            "insert into root values (1, 2)",
        ],
    },
    Tree {
        shape: "multi-level inheritance",
        setup: &[
            "create table root (a int, b int)",
            "create table mid () inherits (root)",
            "create table leaf () inherits (mid)",
            "insert into leaf values (1, 2)",
        ],
    },
    Tree {
        // Multiple inheritance makes the tree a DAG: `leaf` is reachable from
        // `root` by two paths and must be altered exactly once.
        shape: "inheritance diamond",
        setup: &[
            "create table root (a int, b int)",
            "create table dia_left () inherits (root)",
            "create table dia_right () inherits (root)",
            "create table leaf () inherits (dia_left, dia_right)",
            "insert into leaf values (1, 2)",
        ],
    },
];

/// What a case's probe must produce once the subcommand has recursed.
enum Expect {
    Rows(Vec<Vec<Option<String>>>),
    /// The `SQLSTATE` the probe must fail with — how the absence of a column,
    /// or the presence of a constraint, is observed.
    Error(&'static str),
}

struct Case {
    subcommand: &'static str,
    /// Statements run against `root` (and, where a default has to be observed,
    /// an insert into `leaf`).
    steps: &'static [&'static str],
    probe: &'static str,
    expect: Expect,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            subcommand: "ADD COLUMN",
            steps: &["alter table root add column c text"],
            probe: "select a, b, c from leaf",
            expect: Expect::Rows(rows(&[&[Some("1"), Some("2"), None]])),
        },
        Case {
            subcommand: "ADD COLUMN … DEFAULT",
            steps: &["alter table root add column c int default 7"],
            probe: "select c from leaf",
            expect: Expect::Rows(rows(&[&[Some("7")]])),
        },
        Case {
            subcommand: "DROP COLUMN",
            steps: &["alter table root drop column b"],
            probe: "select b from leaf",
            expect: Expect::Error("42703"),
        },
        Case {
            subcommand: "RENAME COLUMN",
            steps: &["alter table root rename column b to renamed"],
            probe: "select renamed from leaf",
            expect: Expect::Rows(rows(&[&[Some("2")]])),
        },
        Case {
            subcommand: "ALTER COLUMN … TYPE",
            // Concatenation only type-checks once the leaf's own column is
            // text, so this fails to parse-analyse if the type stayed integer.
            steps: &["alter table root alter column b type text"],
            probe: "select b || '!' from leaf",
            expect: Expect::Rows(rows(&[&[Some("2!")]])),
        },
        Case {
            subcommand: "ALTER COLUMN … SET DEFAULT",
            steps: &[
                "alter table root alter column b set default 9",
                "insert into leaf (a) values (3)",
            ],
            probe: "select b from leaf where a = 3",
            expect: Expect::Rows(rows(&[&[Some("9")]])),
        },
        Case {
            subcommand: "ALTER COLUMN … DROP DEFAULT",
            steps: &[
                "alter table root alter column b set default 9",
                "alter table root alter column b drop default",
                "insert into leaf (a) values (3)",
            ],
            probe: "select b from leaf where a = 3",
            expect: Expect::Rows(rows(&[&[None]])),
        },
        Case {
            subcommand: "ALTER COLUMN … SET NOT NULL",
            steps: &["alter table root alter column b set not null"],
            probe: "insert into leaf (a) values (3)",
            expect: Expect::Error("23502"),
        },
        Case {
            subcommand: "ALTER COLUMN … DROP NOT NULL",
            steps: &[
                "alter table root alter column b set not null",
                "alter table root alter column b drop not null",
                "insert into leaf (a) values (3)",
            ],
            probe: "select b from leaf where a = 3",
            expect: Expect::Rows(rows(&[&[None]])),
        },
    ]
}

#[tokio::test]
async fn every_column_subcommand_reaches_the_leaf_of_every_tree() {
    for tree in TREES {
        for case in cases() {
            let (_engine, mut s) = engine_with(tree.setup).await;
            for step in case.steps {
                run(&mut s, step).await;
            }
            let label = format!("{} on a {} tree", case.subcommand, tree.shape);
            match case.expect {
                Expect::Rows(expected) => {
                    assert!(query(&mut s, case.probe).await == expected, "{label}");
                }
                Expect::Error(code) => {
                    let error = s
                        .simple_query(case.probe)
                        .await
                        .expect_err(case.subcommand)
                        .code;
                    assert!(error == code, "{label}");
                }
            }
        }
    }
}

/// The regression the recursion exists for: a read of the *tree* — not of the
/// leaf — is what failed, because the parent's row shape could not be built
/// from a child that lacked the column.
#[tokio::test]
async fn the_tree_reads_back_with_the_added_column_null() {
    for tree in TREES {
        let (_engine, mut s) = engine_with(tree.setup).await;
        run(&mut s, "alter table root add c text").await;
        run(&mut s, "alter table root add d int, add e int").await;
        run(&mut s, "alter table root drop e").await;
        assert!(
            query(&mut s, "select a, b, c, d from root order by a").await
                == rows(&[&[Some("1"), Some("2"), None, None]]),
            "{}",
            tree.shape
        );
    }
}

/// `TRUNCATE` resolves the parent's shape against every leaf, which is the
/// statement the upstream `insert` test first tripped over.
#[tokio::test]
async fn truncate_walks_the_partition_tree_after_a_column_was_added() {
    let (_engine, mut s) = engine_with(&[
        "create table root (a int, b int) partition by range (a)",
        "create table leaf (a int, b int)",
        "alter table root attach partition leaf for values from (1) to (10)",
        "insert into root values (1, 2)",
        "alter table root add c text",
    ])
    .await;

    run(&mut s, "truncate root").await;
    assert!(query(&mut s, "select a from root").await.is_empty());
}

#[tokio::test]
async fn only_stops_the_recursion_at_the_named_relation() {
    let (_engine, mut s) = engine_with(&[
        "create table root (a int, b int)",
        "create table leaf () inherits (root)",
        "insert into leaf values (1, 2)",
    ])
    .await;

    run(&mut s, "alter table only root alter column b set default 9").await;
    run(&mut s, "insert into leaf (a) values (3)").await;
    // The default landed on the parent alone, so the child's insert took NULL.
    assert!(query(&mut s, "select b from leaf where a = 3").await == rows(&[&[None]]));
    run(&mut s, "insert into root (a) values (4)").await;
    assert!(query(&mut s, "select b from only root where a = 4").await == rows(&[&[Some("9")]]));

    // DROP COLUMN on an inheritance parent is local too: the child keeps its
    // own copy of the column, exactly as PostgreSQL leaves it.
    run(&mut s, "alter table only root drop column b").await;
    assert!(query(&mut s, "select b from leaf where a = 1").await == rows(&[&[Some("2")]]));
    assert!(s.simple_query("select b from root").await.is_err());
}

#[tokio::test]
async fn only_is_refused_for_the_subcommands_postgresql_will_not_let_stop() {
    let partitioned: &[&str] = &[
        "create table root (a int, b int) partition by range (a)",
        "create table leaf (a int, b int)",
        "alter table root attach partition leaf for values from (1) to (10)",
    ];
    let inherited: &[&str] = &[
        "create table root (a int, b int)",
        "create table leaf () inherits (root)",
    ];
    let cases: &[(&str, &[&str], &str, Option<&str>)] = &[
        (
            "alter table only root add column c int",
            partitioned,
            "column must be added to child tables too",
            None,
        ),
        (
            "alter table only root add column c int",
            inherited,
            "column must be added to child tables too",
            None,
        ),
        (
            "alter table only root drop column b",
            partitioned,
            "cannot drop column from only the partitioned table when partitions exist",
            Some("Do not specify the ONLY keyword."),
        ),
        (
            "alter table only root rename column b to renamed",
            inherited,
            "inherited column \"b\" must be renamed in child tables too",
            None,
        ),
        (
            "alter table only root alter column b type text",
            inherited,
            "type of inherited column \"b\" must be changed in child tables too",
            None,
        ),
        (
            "alter table only root alter column b set not null",
            partitioned,
            "constraint must be added to child tables too",
            Some("Do not specify the ONLY keyword."),
        ),
    ];

    for (sql, setup, message, hint) in cases {
        let (_engine, mut s) = engine_with(setup).await;
        let error = s.simple_query(sql).await.expect_err(sql);
        assert!(error.code == "42P16", "{sql}");
        assert!(error.message == *message, "{sql}");
        let carried = error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.hint.clone());
        assert!(carried.as_deref() == *hint, "{sql}");
    }
}

/// `ONLY` is only refused because the descendants would be left out of step.
/// A relation with none has nothing to fall out of step with.
#[tokio::test]
async fn only_is_accepted_when_the_relation_has_no_descendants() {
    let (_engine, mut s) = engine_with(&[
        "create table root (a int, b int)",
        "insert into root values (1, 2)",
        "alter table only root add column c text",
    ])
    .await;

    assert!(
        query(&mut s, "select a, b, c from root").await == rows(&[&[Some("1"), Some("2"), None]])
    );
}

#[tokio::test]
async fn a_child_that_already_declares_the_column_merges_when_the_types_agree() {
    let (_engine, mut s) = engine_with(&[
        "create table root (a int)",
        "create table leaf (c text) inherits (root)",
        "insert into leaf values (1, 'kept')",
        "alter table root add column c text",
    ])
    .await;

    // One column, not two: the child's own declaration absorbed the parent's,
    // so the value it already held is still there.
    assert!(
        query(&mut s, "select a, c from root order by a").await
            == rows(&[&[Some("1"), Some("kept")]])
    );
    assert!(query(&mut s, "select a, c from leaf").await == rows(&[&[Some("1"), Some("kept")]]));
}

/// `IF EXISTS` abandons the subcommand at the named relation, so there is
/// nothing left to propagate — and in particular a child's own, unrelated
/// column of that name must survive. (`ADD COLUMN IF NOT EXISTS` is skipped by
/// the same rule.)
#[tokio::test]
async fn a_subcommand_the_parent_skipped_does_not_reach_the_children() {
    let (_engine, mut s) = engine_with(&[
        "create table root (a int)",
        "create table leaf (c text) inherits (root)",
        "insert into leaf values (1, 'kept')",
        "alter table root drop column if exists c",
    ])
    .await;

    assert!(query(&mut s, "select c from leaf").await == rows(&[&[Some("kept")]]));
}

#[tokio::test]
async fn a_child_column_of_a_different_type_refuses_the_merge() {
    let (_engine, mut s) = engine_with(&[
        "create table root (a int)",
        "create table leaf (c text) inherits (root)",
    ])
    .await;

    let error = s
        .simple_query("alter table root add column c int")
        .await
        .expect_err("mismatched merge");
    assert!(error.code == "42804");
    assert!(error.message == "child table \"leaf\" has different type for column \"c\"");
    // The statement is one batch, so the parent must not have kept the column.
    assert!(s.simple_query("select c from root").await.is_err());
}

/// The notices an `ADD COLUMN` raises on its way down the tree.
///
/// `take_notices` is called after the setup so the receiver holds only what the
/// `ALTER TABLE` itself raised.
async fn add_column_notices(setup: &[&str], alter: &str) -> Vec<String> {
    let (_engine, mut s) = engine_with(setup).await;
    let mut notices = s.take_notices().expect("notice receiver");
    while notices.try_recv().is_ok() {}
    run(&mut s, alter).await;
    let mut seen = Vec::new();
    while let Ok(notice) = notices.try_recv() {
        seen.push(notice.message);
    }
    seen
}

/// `ATExecAddColumn` recurses one edge at a time — `tablecmds.c` says outright
/// that `find_all_inheritors` cannot be used — so a child reachable by two
/// paths is arrived at twice. The second arrival finds the column already
/// there and says so.
///
/// Every count here was captured from `postgres:18.4`.
#[tokio::test]
async fn a_child_reached_twice_reports_the_merge_once_per_extra_path() {
    // The `create_misc` diamond: `d` inherits both `b` and `c`, which both
    // inherit `a`.
    assert!(
        add_column_notices(
            &[
                "create table a_star (class char, aa int4)",
                "create table b_star (b text) inherits (a_star)",
                "create table c_star (c text) inherits (a_star)",
                "create table d_star (d float8) inherits (b_star, c_star)",
                "create table e_star (e int2) inherits (c_star)",
                "create table f_star (f polygon) inherits (e_star)",
            ],
            "alter table a_star add column a text",
        )
        .await
            == vec!["merging definition of column \"a\" for child \"d_star\"".to_string()]
    );

    // Three paths to the same child are two extra arrivals, so two notices.
    assert!(
        add_column_notices(
            &[
                "create table t0 (x int)",
                "create table t1 () inherits (t0)",
                "create table t2 () inherits (t0)",
                "create table t3 () inherits (t0)",
                "create table t4 () inherits (t1, t2, t3)",
            ],
            "alter table t0 add column q text",
        )
        .await
            == vec![
                "merging definition of column \"q\" for child \"t4\"".to_string(),
                "merging definition of column \"q\" for child \"t4\"".to_string(),
            ]
    );
}

/// A child that spelled the column out by hand is the same case one arrival
/// earlier, so it takes the notice too — and a tree with no second arrival and
/// no hand-written column takes none.
#[tokio::test]
async fn a_child_that_already_declares_the_column_reports_the_merge() {
    assert!(
        add_column_notices(
            &[
                "create table g0 (x int)",
                "create table g1 (m text) inherits (g0)",
                "create table g2 () inherits (g1)",
            ],
            "alter table g0 add column m text",
        )
        .await
            == vec!["merging definition of column \"m\" for child \"g1\"".to_string()]
    );

    let quiet: Vec<String> = Vec::new();
    assert!(
        add_column_notices(
            &[
                "create table h0 (x int)",
                "create table h1 () inherits (h0)",
                "create table h2 () inherits (h1)",
            ],
            "alter table h0 add column m text",
        )
        .await
            == quiet
    );
    // `IF NOT EXISTS` for a column the parent already has is dropped whole,
    // descendants included — so the child that inherited it is not merged into.
    // PostgreSQL raises its own `already exists, skipping` notice there and no
    // merge notice, which is what `quiet` here means: no *merge* notice.
    assert!(
        add_column_notices(
            &[
                "create table j0 (m text)",
                "create table j1 () inherits (j0)"
            ],
            "alter table j0 add column if not exists m text",
        )
        .await
        .iter()
        .filter(|notice| notice.starts_with("merging definition"))
        .count()
            == 0
    );
}

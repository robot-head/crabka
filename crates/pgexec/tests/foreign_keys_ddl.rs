//! `FOREIGN KEY` DDL against a real in-process engine: the validations a
//! constraint definition has to survive, the name it is given, which referenced
//! objects it may target, `NOT VALID` and `VALIDATE CONSTRAINT`, and the
//! dependency refusals a later `DROP` reports.
//!
//! Every SQLSTATE, message, `DETAIL` and `HINT` asserted here was captured from
//! a live `PostgreSQL` 18.4 server — the same major/minor the conformance
//! harness pins — rather than from documentation. Where this engine knowingly
//! diverges the test pins the *current* behaviour and says so at the assertion.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Cell, Engine, QueryResult, Session},
    error::PgError,
};

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("statement should succeed")
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

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

/// A failed statement, as everything `PostgreSQL` puts on the wire for it —
/// compared as one value so a case states its whole expected error rather than a
/// chain of field assertions.
#[derive(Debug, PartialEq, Eq)]
struct Failure {
    code: String,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
}

impl Failure {
    fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
            hint: None,
        }
    }

    fn detail(mut self, detail: &str) -> Self {
        self.detail = Some(detail.to_string());
        self
    }

    fn hint(mut self, hint: &str) -> Self {
        self.hint = Some(hint.to_string());
        self
    }

    fn of(error: PgError) -> Self {
        let diagnostics = error.diagnostics.unwrap_or_default();
        Self {
            code: error.code,
            message: error.message,
            detail: diagnostics.detail,
            hint: diagnostics.hint,
        }
    }
}

async fn failure_of(s: &mut SqlSession, sql: &str) -> Failure {
    Failure::of(s.simple_query(sql).await.expect_err("expected an error"))
}

/// The `HINT` every 2BP01 dependency refusal carries.
const CASCADE_HINT: &str = "Use DROP ... CASCADE to drop the dependent objects too.";

// ---------------------------------------------------------------------------
// DDL validation
// ---------------------------------------------------------------------------

/// Every validation a `FOREIGN KEY` clause has to survive before it becomes a
/// catalog record, with the SQLSTATE and wording `PostgreSQL` 18.4 reports.
#[tokio::test]
async fn foreign_key_ddl_validation_errors_match_postgresql() {
    struct Case {
        sql: &'static str,
        expect: Failure,
        why: &'static str,
    }

    let cases = [
        Case {
            sql: "CREATE TABLE c (a int4 REFERENCES nosuch (id))",
            expect: Failure::new("42P01", "relation \"nosuch\" does not exist"),
            why: "the referenced relation does not exist at all",
        },
        Case {
            sql: "CREATE TABLE c (a int4 REFERENCES v (id))",
            expect: Failure::new("42809", "referenced relation \"v\" is not a table"),
            why: "a view has no index to prove its columns unique, and PostgreSQL \
                  words this differently from its general wrong-object-type error",
        },
        Case {
            sql: "CREATE TABLE c (a int4 REFERENCES nopk (id))",
            expect: Failure::new(
                "42830",
                "there is no unique constraint matching given keys for referenced table \"nopk\"",
            ),
            why: "no unique index covers the referenced column",
        },
        Case {
            sql: "CREATE TABLE c (a int4, b int4, FOREIGN KEY (a, b) REFERENCES p (id))",
            expect: Failure::new(
                "42830",
                "number of referencing and referenced columns for foreign key disagree",
            ),
            why: "two referencing columns against one referenced column",
        },
        Case {
            sql: "CREATE TABLE c (a int4, b int4, FOREIGN KEY (a, b) REFERENCES p (id, id))",
            expect: Failure::new(
                "42830",
                "foreign key referenced-columns list must not contain duplicates",
            ),
            why: "the referenced list repeats a column, so the lists cannot pair",
        },
        Case {
            sql: "CREATE TABLE c6 (a text REFERENCES p (id))",
            expect: Failure::new(
                "42804",
                "foreign key constraint \"c6_a_fkey\" cannot be implemented",
            )
            .detail(
                "Key columns \"a\" of the referencing table and \"id\" of the referenced \
                 table are of incompatible types: text and integer.",
            ),
            why: "the primary message names only the constraint; the DETAIL names both \
                  sides and both types",
        },
        Case {
            sql: "CREATE TABLE c (a int4, FOREIGN KEY (nope) REFERENCES p (id))",
            expect: Failure::new(
                "42703",
                "column \"nope\" referenced in foreign key constraint does not exist",
            ),
            why: "the referencing column is not a column of the relation being created",
        },
        Case {
            sql: "CREATE TABLE c (a int4 REFERENCES p (id) MATCH PARTIAL)",
            expect: Failure::new("0A000", "MATCH PARTIAL not yet implemented"),
            why: "PostgreSQL has never implemented MATCH PARTIAL and refuses it at \
                  parse analysis",
        },
        Case {
            sql: "CREATE TABLE c (a int4, b int4, \
                  FOREIGN KEY (a) REFERENCES p (id) ON DELETE SET NULL (b))",
            expect: Failure::new(
                "42P10",
                "column \"b\" referenced in ON DELETE SET action must be part of foreign key",
            ),
            why: "the SET NULL column list may only name foreign-key columns",
        },
        Case {
            sql: "CREATE TABLE c (a int4 REFERENCES p (id) ON UPDATE SET NULL (a))",
            expect: Failure::new(
                "0A000",
                "a column list with SET NULL is only supported for ON DELETE actions",
            ),
            why: "the column list is an ON DELETE-only spelling, refused as unimplemented \
                  rather than as a syntax error",
        },
        Case {
            sql: "CREATE TABLE dup (a int4, b int4, \
                  CONSTRAINT dupname FOREIGN KEY (a) REFERENCES p (id), \
                  CONSTRAINT dupname FOREIGN KEY (b) REFERENCES p (id))",
            expect: Failure::new(
                "42710",
                "constraint \"dupname\" for relation \"dup\" already exists",
            ),
            why: "one constraint namespace per relation, and the second clause collides \
                  inside the very statement that defines both",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            "CREATE TABLE nopk (id int4)",
            "CREATE VIEW v AS SELECT id FROM p",
        ])
        .await;
        assert!(
            failure_of(&mut s, case.sql).await == case.expect,
            "{}",
            case.why
        );
    }
}

// ---------------------------------------------------------------------------
// Naming
// ---------------------------------------------------------------------------

/// The derived name joins *every* referencing column, in the order the clause
/// writes them, and a second unnamed key over the same columns takes the lowest
/// free numeric suffix. An explicit name that collides is 42710.
#[tokio::test]
async fn default_constraint_name_joins_every_referencing_column_and_uniquifies() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (x int4, y int4, PRIMARY KEY (x, y))",
        "CREATE TABLE c12 (a int4, b int4, FOREIGN KEY (a, b) REFERENCES p (x, y))",
        "ALTER TABLE c12 ADD FOREIGN KEY (a, b) REFERENCES p (x, y)",
    ])
    .await;

    assert!(
        query(
            &mut s,
            "SELECT conname FROM pg_constraint WHERE contype = 'f' ORDER BY conname",
        )
        .await
            == vec![text_row(&["c12_a_b_fkey"]), text_row(&["c12_a_b_fkey1"])]
    );

    // The name the first key was given is the one a violation quotes.
    assert!(
        failure_of(&mut s, "INSERT INTO c12 VALUES (1, 1)").await
            == Failure::new(
                "23503",
                "insert or update on table \"c12\" violates foreign key constraint \
                 \"c12_a_b_fkey\"",
            )
            .detail("Key (a, b)=(1, 1) is not present in table \"p\".")
    );

    assert!(
        failure_of(
            &mut s,
            "ALTER TABLE c12 ADD CONSTRAINT c12_a_b_fkey FOREIGN KEY (a, b) REFERENCES p (x, y)",
        )
        .await
            == Failure::new(
                "42710",
                "constraint \"c12_a_b_fkey\" for relation \"c12\" already exists",
            )
    );
}

// ---------------------------------------------------------------------------
// What a foreign key may target
// ---------------------------------------------------------------------------

/// A bare `CREATE UNIQUE INDEX` — no constraint anywhere — is a legitimate
/// target: `PostgreSQL` requires a unique *index*, not a unique constraint.
#[tokio::test]
async fn a_foreign_key_may_target_a_bare_unique_index() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE uniqidx (a int4)",
        "CREATE UNIQUE INDEX uniqidx_a_uq ON uniqidx (a)",
        "CREATE TABLE c11 (a int4 REFERENCES uniqidx (a))",
        "INSERT INTO uniqidx VALUES (1)",
        "INSERT INTO c11 VALUES (1)",
    ])
    .await;

    assert!(query(&mut s, "SELECT a FROM c11").await == vec![text_row(&["1"])]);
    assert!(
        failure_of(&mut s, "INSERT INTO c11 VALUES (2)").await
            == Failure::new(
                "23503",
                "insert or update on table \"c11\" violates foreign key constraint \
                 \"c11_a_fkey\"",
            )
            .detail("Key (a)=(2) is not present in table \"uniqidx\".")
    );
}

/// A `UNIQUE` constraint is a target, and an omitted referenced-column list
/// means the parent's primary key.
#[tokio::test]
async fn a_foreign_key_may_target_a_unique_constraint_or_the_primary_key_by_omission() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE pp (id int4 PRIMARY KEY, k int4 UNIQUE)",
        "CREATE TABLE cu (a int4 REFERENCES pp (k))",
        "CREATE TABLE cp (a int4 REFERENCES pp)",
        "INSERT INTO pp VALUES (1, 7)",
        "INSERT INTO cu VALUES (7)",
        "INSERT INTO cp VALUES (1)",
    ])
    .await;

    assert!(query(&mut s, "SELECT a FROM cu").await == vec![text_row(&["7"])]);
    assert!(query(&mut s, "SELECT a FROM cp").await == vec![text_row(&["1"])]);
    // The UNIQUE-constraint key probes `k`, and the omitted list resolved to the
    // primary key rather than to the first unique index.
    assert!(
        failure_of(&mut s, "INSERT INTO cu VALUES (1)").await
            == Failure::new(
                "23503",
                "insert or update on table \"cu\" violates foreign key constraint \"cu_a_fkey\"",
            )
            .detail("Key (a)=(1) is not present in table \"pp\".")
    );
    assert!(
        failure_of(&mut s, "INSERT INTO cp VALUES (7)").await
            == Failure::new(
                "23503",
                "insert or update on table \"cp\" violates foreign key constraint \"cp_a_fkey\"",
            )
            .detail("Key (a)=(7) is not present in table \"pp\".")
    );
}

/// `CREATE TABLE t (… REFERENCES t …)` resolves against the relation being
/// created: no catalog read can find its columns or the index its primary key is
/// about to become.
#[tokio::test]
async fn a_self_reference_inside_create_table_resolves_against_the_in_flight_definition() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE selfref (id int4 PRIMARY KEY, boss int4 REFERENCES selfref)",
        // The checks fire once the statement's rows exist, so a row that is its
        // own parent is accepted with no DEFERRABLE clause anywhere.
        "INSERT INTO selfref (id, boss) VALUES (1, 1)",
        "INSERT INTO selfref (id, boss) VALUES (2, NULL), (3, 2)",
    ])
    .await;

    assert!(
        query(&mut s, "SELECT id, boss FROM selfref ORDER BY id").await
            == vec![
                text_row(&["1", "1"]),
                vec![Some("2".to_string()), None],
                text_row(&["3", "2"]),
            ]
    );
    assert!(
        failure_of(&mut s, "INSERT INTO selfref (id, boss) VALUES (4, 99)").await
            == Failure::new(
                "23503",
                "insert or update on table \"selfref\" violates foreign key constraint \
                 \"selfref_boss_fkey\"",
            )
            .detail("Key (boss)=(99) is not present in table \"selfref\".")
    );
}

/// DROP COLUMN runs before ADD CONSTRAINT inside `PostgreSQL`'s ALTER TABLE pass
/// order. A self-reference therefore sees the referenced column as missing;
/// CASCADE does not change that analysis error, and neither failed statement
/// may disturb the original primary key or create the staged foreign key.
#[tokio::test]
async fn drop_column_precedes_an_added_self_referencing_foreign_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE staged_self_fk (id int4 PRIMARY KEY, parent int4)",
        "INSERT INTO staged_self_fk VALUES (1, NULL)",
    ])
    .await;

    for suffix in ["", " CASCADE"] {
        let sql = format!(
            "ALTER TABLE staged_self_fk \
             ADD CONSTRAINT staged_self_fk_parent_fkey \
             FOREIGN KEY (parent) REFERENCES staged_self_fk (id), \
             DROP COLUMN id{suffix}"
        );
        assert!(
            failure_of(&mut s, &sql).await
                == Failure::new(
                    "42703",
                    "column \"id\" referenced in foreign key constraint does not exist",
                ),
            "{sql}"
        );
        assert!(
            query(&mut s, "SELECT id, parent FROM staged_self_fk").await
                == vec![vec![Some("1".to_string()), None]],
            "{sql}"
        );
        assert!(
            query(
                &mut s,
                "SELECT conname FROM pg_constraint \
                 WHERE conname = 'staged_self_fk_parent_fkey'",
            )
            .await
                == Vec::<Vec<Option<String>>>::new(),
            "{sql}"
        );
    }
}

/// A composite key written in a different order from the referenced index's.
///
/// Both column lists are stored as written and paired positionally, and the
/// probe permutes into the index's order — so `FOREIGN KEY (b, a) REFERENCES
/// pperm (y, x)` over a `(x, y)` primary key must accept exactly the rows whose
/// `(a, b)` equals `(x, y)`. Probing without the permutation reads a byte string
/// that cannot exist while every single-column case still passes.
#[tokio::test]
async fn a_composite_foreign_key_probes_the_permuted_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE pperm (x int4, y int4, PRIMARY KEY (x, y))",
        "CREATE TABLE cperm (a int4, b int4, FOREIGN KEY (b, a) REFERENCES pperm (y, x))",
        "INSERT INTO pperm VALUES (1, 2)",
        // b -> y and a -> x, so this row pairs (x, y) = (1, 2), which exists.
        "INSERT INTO cperm (a, b) VALUES (1, 2)",
    ])
    .await;

    assert!(query(&mut s, "SELECT a, b FROM cperm").await == vec![text_row(&["1", "2"])]);

    // The mirror image pairs (x, y) = (2, 1), which does not exist. The DETAIL
    // names the columns in FOREIGN KEY clause order, not index order.
    assert!(
        failure_of(&mut s, "INSERT INTO cperm (a, b) VALUES (2, 1)").await
            == Failure::new(
                "23503",
                "insert or update on table \"cperm\" violates foreign key constraint \
                 \"cperm_b_a_fkey\"",
            )
            .detail("Key (b, a)=(1, 2) is not present in table \"pperm\".")
    );

    // The parent side finds the child through the same permutation.
    assert!(failure_of(&mut s, "DELETE FROM pperm").await.code == "23503");
}

// ---------------------------------------------------------------------------
// NOT VALID and VALIDATE CONSTRAINT
// ---------------------------------------------------------------------------

/// `ADD CONSTRAINT` scans the stored rows; `NOT VALID` skips that scan, still
/// governs every later write, and `VALIDATE CONSTRAINT` runs the scan it skipped
/// — reporting it in the same words a failing insert uses.
#[tokio::test]
async fn not_valid_defers_back_validation_but_still_governs_new_writes() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE bv_p (id int4 PRIMARY KEY)",
        "CREATE TABLE bv_c (a int4)",
        "INSERT INTO bv_c VALUES (7)",
    ])
    .await;

    let back_validation_failure = Failure::new(
        "23503",
        "insert or update on table \"bv_c\" violates foreign key constraint \"bv_fk\"",
    )
    .detail("Key (a)=(7) is not present in table \"bv_p\".");

    assert!(
        failure_of(
            &mut s,
            "ALTER TABLE bv_c ADD CONSTRAINT bv_fk FOREIGN KEY (a) REFERENCES bv_p (id)",
        )
        .await
            == back_validation_failure
    );

    run(
        &mut s,
        "ALTER TABLE bv_c ADD CONSTRAINT bv_fk FOREIGN KEY (a) REFERENCES bv_p (id) NOT VALID",
    )
    .await;

    // The unvalidated constraint governs new writes all the same.
    assert!(
        failure_of(&mut s, "INSERT INTO bv_c VALUES (8)").await
            == Failure::new(
                "23503",
                "insert or update on table \"bv_c\" violates foreign key constraint \"bv_fk\"",
            )
            .detail("Key (a)=(8) is not present in table \"bv_p\".")
    );
    // VALIDATE runs the skipped scan and reports the pre-existing row.
    assert!(
        failure_of(&mut s, "ALTER TABLE bv_c VALIDATE CONSTRAINT bv_fk").await
            == back_validation_failure
    );

    run(&mut s, "INSERT INTO bv_p VALUES (7)").await;
    run(&mut s, "ALTER TABLE bv_c VALIDATE CONSTRAINT bv_fk").await;
    assert!(query(&mut s, "SELECT a FROM bv_c").await == vec![text_row(&["7"])]);
}

/// A key added beside the column it references back-validates the *rewritten*
/// rows: storage still holds rows without the new column at all, so the scan has
/// to see the back-filled default rather than a missing value.
#[tokio::test]
async fn a_foreign_key_added_beside_a_column_validates_the_rewritten_rows() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE ap (id int4 PRIMARY KEY)",
        "INSERT INTO ap VALUES (5)",
        "CREATE TABLE ac (x int4)",
        "INSERT INTO ac VALUES (1)",
        "ALTER TABLE ac ADD COLUMN a int4 DEFAULT 5, \
         ADD CONSTRAINT ac_fk FOREIGN KEY (a) REFERENCES ap",
    ])
    .await;

    assert!(query(&mut s, "SELECT x, a FROM ac").await == vec![text_row(&["1", "5"])]);

    // The same shape with a default no parent holds fails the scan, which proves
    // the successful case above was validated rather than skipped.
    let (_engine2, mut s2) = engine_with(&[
        "CREATE TABLE ap (id int4 PRIMARY KEY)",
        "INSERT INTO ap VALUES (5)",
        "CREATE TABLE ac (x int4)",
        "INSERT INTO ac VALUES (1)",
    ])
    .await;
    assert!(
        failure_of(
            &mut s2,
            "ALTER TABLE ac ADD COLUMN a int4 DEFAULT 6, \
             ADD CONSTRAINT ac_fk FOREIGN KEY (a) REFERENCES ap",
        )
        .await
            == Failure::new(
                "23503",
                "insert or update on table \"ac\" violates foreign key constraint \"ac_fk\"",
            )
            .detail("Key (a)=(6) is not present in table \"ap\".")
    );
}

// ---------------------------------------------------------------------------
// Drop dependencies
// ---------------------------------------------------------------------------

/// Dropping a table, an index, or a constraint a foreign key depends on is
/// 2BP01, with one `DETAIL` line per dependent constraint and the `CASCADE`
/// hint. A dropped *constraint* is named as a constraint in the message and as
/// its backing index in the `DETAIL`.
#[tokio::test]
async fn dropping_a_referenced_object_lists_every_dependent_constraint() {
    struct Case {
        sql: &'static str,
        expect: Failure,
        why: &'static str,
    }

    let cases = [
        Case {
            sql: "DROP TABLE p1",
            expect: Failure::new(
                "2BP01",
                "cannot drop table p1 because other objects depend on it",
            )
            .detail("constraint cdel_a_fkey on table cdel depends on table p1")
            .hint(CASCADE_HINT),
            why: "the child's constraint depends on the whole relation",
        },
        Case {
            sql: "DROP INDEX uniqidx_a_uq",
            expect: Failure::new(
                "2BP01",
                "cannot drop index uniqidx_a_uq because other objects depend on it",
            )
            .detail("constraint c11_a_fkey on table c11 depends on index uniqidx_a_uq")
            .hint(CASCADE_HINT),
            why: "a bare unique index is depended on exactly as a constraint's index is",
        },
        Case {
            sql: "ALTER TABLE p DROP CONSTRAINT p_pkey",
            expect: Failure::new(
                "2BP01",
                "cannot drop constraint p_pkey on table p because other objects depend on it",
            )
            .detail(
                "constraint c12_a_b_fkey on table c12 depends on index p_pkey\n\
                 constraint cfull_a_b_fkey on table cfull depends on index p_pkey",
            )
            .hint(CASCADE_HINT),
            why: "several dependents are listed one per line inside a single DETAIL, \
                  and the DETAIL names the backing index rather than the constraint",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p1 (id int4 PRIMARY KEY)",
            "CREATE TABLE cdel (a int4 REFERENCES p1 (id))",
            "CREATE TABLE uniqidx (a int4)",
            "CREATE UNIQUE INDEX uniqidx_a_uq ON uniqidx (a)",
            "CREATE TABLE c11 (a int4 REFERENCES uniqidx (a))",
            "CREATE TABLE p (x int4, y int4, PRIMARY KEY (x, y))",
            "CREATE TABLE c12 (a int4, b int4, FOREIGN KEY (a, b) REFERENCES p)",
            "CREATE TABLE cfull (a int4, b int4, FOREIGN KEY (a, b) REFERENCES p MATCH FULL)",
        ])
        .await;
        assert!(
            failure_of(&mut s, case.sql).await == case.expect,
            "{}",
            case.why
        );
    }
}

/// The `DETAIL` lists dependents in the order they were created, not by name.
///
/// `PostgreSQL` walks `pg_depend` and reports what it finds in oid order, so
/// `zz` declared before `aa` is named first — on the same child relation and on
/// two different ones alike. Sorting the dependents by name would swap both
/// pairs.
#[tokio::test]
async fn dependent_constraints_are_listed_in_creation_order() {
    struct Case {
        setup: &'static [&'static str],
        sql: &'static str,
        expect: Failure,
        why: &'static str,
    }

    let cases = [
        Case {
            setup: &[
                "CREATE TABLE dp (id int4 PRIMARY KEY)",
                "CREATE TABLE dc (id int4 PRIMARY KEY, a int4, \
                 CONSTRAINT zz FOREIGN KEY (a) REFERENCES dp (id), \
                 CONSTRAINT aa FOREIGN KEY (a) REFERENCES dp (id))",
            ],
            sql: "DROP TABLE dp",
            expect: Failure::new(
                "2BP01",
                "cannot drop table dp because other objects depend on it",
            )
            .detail(
                "constraint zz on table dc depends on table dp\n\
                 constraint aa on table dc depends on table dp",
            )
            .hint(CASCADE_HINT),
            why: "two constraints on one child, declared later-name first",
        },
        Case {
            setup: &[
                "CREATE TABLE dp (id int4 PRIMARY KEY, u int4)",
                "CREATE UNIQUE INDEX dp_u_uq ON dp (u)",
                "CREATE TABLE zc (a int4, CONSTRAINT zz FOREIGN KEY (a) REFERENCES dp (u))",
                "CREATE TABLE ac (a int4, CONSTRAINT aa FOREIGN KEY (a) REFERENCES dp (u))",
            ],
            sql: "DROP INDEX dp_u_uq",
            expect: Failure::new(
                "2BP01",
                "cannot drop index dp_u_uq because other objects depend on it",
            )
            .detail(
                "constraint zz on table zc depends on index dp_u_uq\n\
                 constraint aa on table ac depends on index dp_u_uq",
            )
            .hint(CASCADE_HINT),
            why: "an index's dependents span two relations, and creation order is \
                  a total order across them",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(case.setup).await;
        assert!(
            failure_of(&mut s, case.sql).await == case.expect,
            "{}",
            case.why
        );
    }
}

/// `CASCADE` drops the referencing *constraint*, not the referencing relation:
/// the child survives and afterwards accepts anything.
#[tokio::test]
async fn cascade_drops_the_referencing_constraint_not_the_referencing_table() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p1 (id int4 PRIMARY KEY)",
        "CREATE TABLE cdel (a int4 REFERENCES p1 (id))",
        "DROP TABLE p1 CASCADE",
    ])
    .await;

    run(&mut s, "INSERT INTO cdel VALUES (42)").await;
    assert!(query(&mut s, "SELECT a FROM cdel").await == vec![text_row(&["42"])]);
    assert!(
        query(
            &mut s,
            "SELECT conname FROM pg_constraint WHERE contype = 'f'",
        )
        .await
            == Vec::<Vec<Option<String>>>::new()
    );

    // The same for a dropped index: the constraint goes, the child stays.
    let (_engine2, mut s2) = engine_with(&[
        "CREATE TABLE uniqidx (a int4)",
        "CREATE UNIQUE INDEX uniqidx_a_uq ON uniqidx (a)",
        "CREATE TABLE c11 (a int4 REFERENCES uniqidx (a))",
        "DROP INDEX uniqidx_a_uq CASCADE",
        "INSERT INTO c11 VALUES (99)",
    ])
    .await;
    assert!(query(&mut s2, "SELECT a FROM c11").await == vec![text_row(&["99"])]);
}

/// Two relations that reference each other can be dropped in one statement:
/// every dependent inside the drop set is discounted.
#[tokio::test]
async fn a_mutually_referencing_pair_can_be_dropped_in_one_statement() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE mp (id int4 PRIMARY KEY, other int4)",
        "CREATE TABLE mc (id int4 PRIMARY KEY, a int4 REFERENCES mp (id))",
        "ALTER TABLE mp ADD CONSTRAINT mp_other_fkey FOREIGN KEY (other) REFERENCES mc (id)",
    ])
    .await;

    // Either one alone is refused.
    assert!(failure_of(&mut s, "DROP TABLE mp").await.code == "2BP01");
    assert!(failure_of(&mut s, "DROP TABLE mc").await.code == "2BP01");
    run(&mut s, "DROP TABLE mp, mc").await;
    assert!(failure_of(&mut s, "SELECT id FROM mp").await.code == "42P01");
    assert!(failure_of(&mut s, "SELECT id FROM mc").await.code == "42P01");
}

// ---------------------------------------------------------------------------
// Renames and drops of the constraint itself
// ---------------------------------------------------------------------------

/// `RENAME COLUMN` on either side rewrites the stored key, `RENAME CONSTRAINT`
/// moves the name a violation quotes, and `DROP CONSTRAINT` ends the
/// enforcement.
#[tokio::test]
async fn renaming_columns_and_the_constraint_keeps_the_key_enforced() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE qp (id int4 PRIMARY KEY)",
        "CREATE TABLE qc (a int4 REFERENCES qp (id))",
        "INSERT INTO qp VALUES (1)",
        // Rename both sides: the referenced column, then the referencing one.
        "ALTER TABLE qp RENAME COLUMN id TO ident",
        "ALTER TABLE qc RENAME COLUMN a TO b",
        "INSERT INTO qc VALUES (1)",
    ])
    .await;

    // The key follows both renames, and the DETAIL renders the current names.
    assert!(
        failure_of(&mut s, "INSERT INTO qc VALUES (2)").await
            == Failure::new(
                "23503",
                "insert or update on table \"qc\" violates foreign key constraint \"qc_a_fkey\"",
            )
            .detail("Key (b)=(2) is not present in table \"qp\".")
    );

    run(
        &mut s,
        "ALTER TABLE qc RENAME CONSTRAINT qc_a_fkey TO qc_renamed",
    )
    .await;
    assert!(
        query(
            &mut s,
            "SELECT conname FROM pg_constraint WHERE contype = 'f'",
        )
        .await
            == vec![text_row(&["qc_renamed"])]
    );
    assert!(
        failure_of(&mut s, "INSERT INTO qc VALUES (2)").await
            == Failure::new(
                "23503",
                "insert or update on table \"qc\" violates foreign key constraint \"qc_renamed\"",
            )
            .detail("Key (b)=(2) is not present in table \"qp\".")
    );

    run(&mut s, "ALTER TABLE qc DROP CONSTRAINT qc_renamed").await;
    run(&mut s, "INSERT INTO qc VALUES (2)").await;
    assert!(
        query(&mut s, "SELECT b FROM qc ORDER BY b").await
            == vec![text_row(&["1"]), text_row(&["2"])]
    );
}

/// Dropping a *referencing* column drops the constraint with it, as
/// `PostgreSQL` does.
#[tokio::test]
async fn dropping_a_referencing_column_drops_its_foreign_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE dp (id int4 PRIMARY KEY)",
        "CREATE TABLE dc (a int4 REFERENCES dp (id), keep int4)",
        "ALTER TABLE dc DROP COLUMN a",
    ])
    .await;

    assert!(
        query(
            &mut s,
            "SELECT conname FROM pg_constraint WHERE contype = 'f'",
        )
        .await
            == Vec::<Vec<Option<String>>>::new()
    );
    run(&mut s, "INSERT INTO dc VALUES (5)").await;
    assert!(query(&mut s, "SELECT keep FROM dc").await == vec![text_row(&["5"])]);
}

/// **Known divergence.** Dropping a *referenced* column is refused, but this
/// engine reports the drop of the column's backing index rather than of the
/// column: `PostgreSQL` 18.4 says `cannot drop column id of table dp because
/// other objects depend on it`, with the same SQLSTATE, the same `DETAIL` and
/// the same `HINT`. The error type carries no column variant, so the refusal
/// borrows the index one. Pinned as-is.
#[tokio::test]
async fn dropping_a_referenced_column_reports_the_index_not_the_column() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE dp (id int4 PRIMARY KEY)",
        "CREATE TABLE dc (a int4 REFERENCES dp (id))",
    ])
    .await;

    assert!(
        failure_of(&mut s, "ALTER TABLE dp DROP COLUMN id").await
            == Failure::new(
                "2BP01",
                "cannot drop index dp_pkey because other objects depend on it",
            )
            .detail("constraint dc_a_fkey on table dc depends on index dp_pkey")
            .hint(CASCADE_HINT)
    );
}

// ---------------------------------------------------------------------------
// Relations this wave does not enforce across
// ---------------------------------------------------------------------------

/// Sharded and partitioned relations refuse foreign keys by name, on either
/// side of the key, with a typed 0A000 rather than a silently unenforced
/// constraint.
#[tokio::test]
async fn sharded_and_partitioned_relations_refuse_foreign_keys() {
    struct Case {
        sql: &'static str,
        expect: Failure,
        why: &'static str,
    }

    let cases = [
        Case {
            sql: "CREATE TABLE sc (a int4 REFERENCES p (id)) SHARDED",
            expect: Failure::new(
                "0A000",
                "foreign key constraint \"sc_a_fkey\" on a sharded table is not supported",
            ),
            why: "the referencing relation is sharded, so the probe would cross ranges",
        },
        Case {
            sql: "CREATE TABLE cs (a int4 REFERENCES sp (id))",
            expect: Failure::new(
                "0A000",
                "foreign key constraint \"cs_a_fkey\" on a sharded table is not supported",
            ),
            why: "the referenced relation is sharded, refused with the same message",
        },
        Case {
            sql: "CREATE TABLE part (id int4, a int4 REFERENCES p (id)) PARTITION BY RANGE (id)",
            expect: Failure::new(
                "0A000",
                "foreign key constraint \"part_a_fkey\" on a partitioned table is not supported",
            ),
            why: "a partitioned relation has no single index to key on",
        },
        Case {
            sql: "ALTER TABLE parted ADD CONSTRAINT parted_a_fkey \
                  FOREIGN KEY (a) REFERENCES p (id)",
            expect: Failure::new(
                "0A000",
                "foreign key constraint \"parted_a_fkey\" on a partitioned table is not supported",
            ),
            why: "the refusal names the constraint the ALTER would have created",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE p (id int4 PRIMARY KEY)",
            "CREATE TABLE sp (id int4) SHARDED",
            "CREATE TABLE parted (id int4, a int4) PARTITION BY RANGE (id)",
        ])
        .await;
        assert!(
            failure_of(&mut s, case.sql).await == case.expect,
            "{}",
            case.why
        );
    }
}

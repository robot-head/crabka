//! `ALTER TABLE` / `CREATE TABLE` DDL semantics that an adversarial diff
//! against `PostgreSQL` 18.4 found divergent: constraint validation over an
//! in-flight column rewrite, index maintenance across a type change, `NOT
//! VALID`, DDL-time analysis of `CHECK` and generation expressions, view
//! dependencies, and the SQLSTATEs each of those reports.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

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

async fn err_code(s: &mut SqlSession, sql: &str) -> String {
    s.simple_query(sql).await.expect_err("expected error").code
}

async fn err_message(s: &mut SqlSession, sql: &str) -> String {
    s.simple_query(sql)
        .await
        .expect_err("expected error")
        .message
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

fn row(values: &[Option<&str>]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|v| v.map(std::string::ToString::to_string))
        .collect()
}

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

#[tokio::test]
async fn gist_exclusion_constraints_reject_conflicting_ranges() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE test_range_excl (room int4range, speaker int4range, during tstzrange, \
         EXCLUDE USING gist (room WITH =, during WITH &&), \
         EXCLUDE USING gist (speaker WITH =, during WITH &&))",
        "INSERT INTO test_range_excl VALUES \
         ('[123,124)', '[1,2)', '[2010-01-02 10:00,2010-01-02 11:00)'), \
         ('[123,124)', '[2,3)', '[2010-01-02 11:00,2010-01-02 12:00)')",
        "SET datestyle = 'Postgres, MDY'",
    ])
    .await;

    assert!(
        err_code(
            &mut s,
            "INSERT INTO test_range_excl VALUES \
             ('[123,124)', '[3,4)', '[2010-01-02 10:10,2010-01-02 11:00)')",
        )
        .await
            == "23P01"
    );
    let error = s
        .simple_query(
            "INSERT INTO test_range_excl VALUES \
             ('[123,124)', '[3,4)', '[2010-01-02 10:10,2010-01-02 11:00)')",
        )
        .await
        .expect_err("expected exclusion violation");
    assert!(
        error
            .diagnostics
            .and_then(|diagnostics| diagnostics.detail)
            .is_some_and(|detail| detail.contains("Sat Jan 02 10:10:00 2010"))
    );
    assert!(
        err_code(
            &mut s,
            "INSERT INTO test_range_excl VALUES \
             ('[124,125)', '[1,2)', '[2010-01-02 10:10,2010-01-02 11:00)')",
        )
        .await
            == "23P01"
    );
}

// ---------------------------------------------------------------------------
// ADD COLUMN with a constraint, over a table that already has rows.

/// The back-validation of a constraint attached to a freshly added column must
/// read the *rewritten* rows, not the pre-`ALTER` ones still in storage. A read
/// of storage under the post-`ALTER` column list indexed past the end of every
/// row and aborted the server process.
#[tokio::test]
async fn add_column_with_constraint_over_existing_rows() {
    struct Case {
        alter: &'static str,
        expect: Result<&'static [&'static str], &'static str>,
        why: &'static str,
    }
    let cases = [
        Case {
            alter: "ALTER TABLE t ADD COLUMN c int4 CHECK (c > 0)",
            expect: Ok(&["1"]),
            why: "a NULL in the new column passes the three-valued CHECK",
        },
        Case {
            alter: "ALTER TABLE t ADD COLUMN c int4 UNIQUE",
            expect: Ok(&["1"]),
            why: "NULL keys are not indexed, so no duplicate arises",
        },
        Case {
            alter: "ALTER TABLE t ADD COLUMN c int4 DEFAULT 5 CHECK (c > 0)",
            expect: Ok(&["1"]),
            why: "the back-fill value satisfies the CHECK",
        },
        Case {
            alter: "ALTER TABLE t ADD COLUMN c int4 DEFAULT 0 CHECK (c > 0)",
            expect: Err("23514"),
            why: "the back-fill value violates the CHECK",
        },
        Case {
            alter: "ALTER TABLE t ADD COLUMN c int4 DEFAULT 1 PRIMARY KEY",
            expect: Err("23505"),
            why: "one back-fill value for two rows duplicates the key",
        },
    ];

    for case in cases {
        let (_engine, mut s) =
            engine_with(&["CREATE TABLE t (id int4)", "INSERT INTO t VALUES (1), (2)"]).await;
        match case.expect {
            Ok(_) => {
                run(&mut s, case.alter).await;
                assert!(
                    query(&mut s, "SELECT id, c FROM t ORDER BY id").await
                        == vec![row(&[Some("1"), None]), row(&[Some("2"), None])]
                        || query(&mut s, "SELECT id, c FROM t ORDER BY id").await
                            == vec![row(&[Some("1"), Some("5")]), row(&[Some("2"), Some("5")])],
                    "{}",
                    case.why
                );
            }
            Err(code) => {
                assert!(err_code(&mut s, case.alter).await == code, "{}", case.why);
                // The table is untouched and still usable after the refusal.
                assert!(
                    query(&mut s, "SELECT id FROM t ORDER BY id").await
                        == vec![text_row(&["1"]), text_row(&["2"])],
                    "{}",
                    case.why
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ALTER COLUMN TYPE.

/// The column's type encodes the index keys, so a type change has to re-encode
/// every index over that column. Otherwise an index scan misses live rows, and
/// a unique index silently stops rejecting duplicates.
#[tokio::test]
async fn type_change_rebuilds_indexes_over_the_column() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (u int4 UNIQUE, v int4)",
        "INSERT INTO t VALUES (1, 1), (2, 2)",
        "CREATE INDEX t_v ON t (v)",
    ])
    .await;

    run(&mut s, "ALTER TABLE t ALTER COLUMN v TYPE int8").await;
    assert!(query(&mut s, "SELECT u, v FROM t WHERE v = 2").await == vec![text_row(&["2", "2"])]);

    run(&mut s, "ALTER TABLE t ALTER COLUMN u TYPE int8").await;
    assert!(query(&mut s, "SELECT u, v FROM t WHERE u = 1").await == vec![text_row(&["1", "1"])]);
    assert!(err_code(&mut s, "INSERT INTO t VALUES (1, 3)").await == "23505");
    assert!(
        query(&mut s, "SELECT u, v FROM t ORDER BY u, v").await
            == vec![text_row(&["1", "1"]), text_row(&["2", "2"])]
    );
}

/// A `PRIMARY KEY` keeps enforcing uniqueness across a type change.
#[tokio::test]
async fn type_change_keeps_the_primary_key_enforced() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (u int4 PRIMARY KEY, v text)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
    ])
    .await;

    run(&mut s, "ALTER TABLE t ALTER COLUMN u TYPE int8").await;
    assert!(err_code(&mut s, "INSERT INTO t VALUES (1, 'dup')").await == "23505");
    assert!(
        query(&mut s, "SELECT u, v FROM t ORDER BY u, v").await
            == vec![text_row(&["1", "a"]), text_row(&["2", "b"])]
    );
    assert!(query(&mut s, "SELECT u, v FROM t WHERE u = 1").await == vec![text_row(&["1", "a"])]);
}

/// The rewrite must not fail on a value no snapshot can reach: a row the user
/// already deleted, or a value already updated away.
#[tokio::test]
async fn type_change_ignores_settled_dead_row_versions() {
    for (setup, expected) in [
        (
            "DELETE FROM t WHERE a = 'bad'",
            vec![text_row(&["1"])] as Vec<Vec<Option<String>>>,
        ),
        (
            "UPDATE t SET a = '2' WHERE a = 'bad'",
            vec![text_row(&["1"]), text_row(&["2"])],
        ),
    ] {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE t (a text)",
            "INSERT INTO t VALUES ('1'), ('bad')",
            setup,
        ])
        .await;
        run(
            &mut s,
            "ALTER TABLE t ALTER COLUMN a TYPE int4 USING a::int4",
        )
        .await;
        assert!(query(&mut s, "SELECT a FROM t ORDER BY a").await == expected);
    }
}

/// `PostgreSQL` coerces the stored value in assignment context, so a cast that
/// is explicit-only is 42804 and not an attempted per-row conversion.
#[tokio::test]
async fn type_change_requires_an_assignment_cast() {
    struct Case {
        alter: &'static str,
        code: Option<&'static str>,
        why: &'static str,
    }
    let cases = [
        Case {
            alter: "ALTER TABLE t ALTER COLUMN a TYPE text",
            code: None,
            why: "int4 -> text is an I/O conversion to a string type",
        },
        Case {
            alter: "ALTER TABLE t ALTER COLUMN c TYPE int4",
            code: None,
            why: "float8 -> int4 is an assignment cast",
        },
        Case {
            alter: "ALTER TABLE t ALTER COLUMN b TYPE int4",
            code: Some("42804"),
            why: "text -> int4 is explicit-only",
        },
        Case {
            alter: "ALTER TABLE t ALTER COLUMN d TYPE int4",
            code: Some("42804"),
            why: "bool -> int4 is explicit-only",
        },
        Case {
            alter: "ALTER TABLE t ALTER COLUMN b TYPE bogus_type",
            code: Some("42704"),
            why: "an unknown type name is undefined_object, not a syntax error",
        },
    ];

    for case in cases {
        let (_engine, mut s) =
            engine_with(&["CREATE TABLE t (a int4, b text, c float8, d bool)"]).await;
        match case.code {
            None => {
                run(&mut s, case.alter).await;
            }
            Some(code) => assert!(err_code(&mut s, case.alter).await == code, "{}", case.why),
        }
    }
}

/// The engine refuses a type change that would leave a stored `CHECK`
/// unresolvable, and does not commit a table that rejects every subsequent
/// write.
#[tokio::test]
async fn type_change_revalidates_dependent_check_constraints() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (a int4 CHECK (a > 0))",
        "INSERT INTO t VALUES (5)",
    ])
    .await;

    assert!(err_code(&mut s, "ALTER TABLE t ALTER COLUMN a TYPE text").await == "42883");
    run(&mut s, "INSERT INTO t VALUES (6)").await;
    assert!(
        query(&mut s, "SELECT a FROM t ORDER BY a").await
            == vec![text_row(&["5"]), text_row(&["6"])]
    );
}

// ---------------------------------------------------------------------------
// NOT VALID.

/// `NOT VALID` skips the existing-row scan and records the constraint
/// unvalidated. The constraint still governs every subsequent write, and
/// `VALIDATE CONSTRAINT` runs the scan it skipped.
#[tokio::test]
async fn not_valid_check_skips_back_validation_but_governs_new_rows() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (a int4)",
        "INSERT INTO t VALUES (-1)",
        "ALTER TABLE t ADD CONSTRAINT t_pos CHECK (a > 0) NOT VALID",
    ])
    .await;

    assert!(
        query(
            &mut s,
            "SELECT conname, convalidated FROM pg_constraint WHERE conname = 't_pos'"
        )
        .await
            == vec![text_row(&["t_pos", "f"])]
    );
    assert!(err_code(&mut s, "INSERT INTO t VALUES (-2)").await == "23514");
    assert!(query(&mut s, "SELECT a FROM t ORDER BY a").await == vec![text_row(&["-1"])]);

    assert!(err_code(&mut s, "ALTER TABLE t VALIDATE CONSTRAINT t_pos").await == "23514");
    run(&mut s, "DELETE FROM t").await;
    run(&mut s, "ALTER TABLE t VALIDATE CONSTRAINT t_pos").await;
    assert!(
        query(
            &mut s,
            "SELECT conname, convalidated FROM pg_constraint WHERE conname = 't_pos'"
        )
        .await
            == vec![text_row(&["t_pos", "t"])]
    );
}

/// `PostgreSQL` ignores `NOT VALID` in `CREATE TABLE`. A new table has no rows
/// to skip, so the constraint is recorded valid.
#[tokio::test]
async fn not_valid_is_ignored_by_create_table() {
    let (_engine, mut s) =
        engine_with(&["CREATE TABLE t (a int4, CONSTRAINT t_pos CHECK (a > 0) NOT VALID)"]).await;
    assert!(
        query(
            &mut s,
            "SELECT conname, convalidated FROM pg_constraint WHERE conname = 't_pos'"
        )
        .await
            == vec![text_row(&["t_pos", "t"])]
    );
    assert!(err_code(&mut s, "INSERT INTO t VALUES (0)").await == "23514");
}

// ---------------------------------------------------------------------------
// DDL-time analysis of CHECK and generation expressions.

/// Every `CHECK` predicate `PostgreSQL` rejects during parse analysis, in both
/// the `CREATE TABLE` and the `ALTER TABLE … ADD CONSTRAINT` spelling. A stored
/// predicate of any of these kinds leaves a table that fails writes, or that
/// silently mis-filters them.
#[tokio::test]
async fn check_predicates_are_analyzed_at_ddl_time() {
    struct Case {
        predicate: &'static str,
        code: &'static str,
        why: &'static str,
    }
    let cases = [
        Case {
            predicate: "nope > 0",
            code: "42703",
            why: "the predicate names a column the relation does not have",
        },
        Case {
            predicate: "a IN (SELECT 1)",
            code: "0A000",
            why: "a CHECK may not contain a subquery",
        },
        Case {
            predicate: "a",
            code: "42804",
            why: "a CHECK predicate must be boolean",
        },
        Case {
            predicate: "sum(a) > 0",
            code: "42803",
            why: "a CHECK may not contain an aggregate",
        },
        Case {
            predicate: "a > b",
            code: "42883",
            why: "integer > text resolves to no operator",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[]).await;
        let create = format!(
            "CREATE TABLE t (a int4, b text, CHECK ({}))",
            case.predicate
        );
        assert!(err_code(&mut s, &create).await == case.code, "{}", case.why);
        // The refused CREATE TABLE left nothing behind.
        assert!(
            err_code(&mut s, "SELECT a FROM t").await == "42P01",
            "{}",
            case.why
        );

        run(&mut s, "CREATE TABLE t (a int4, b text)").await;
        let alter = format!("ALTER TABLE t ADD CONSTRAINT c CHECK ({})", case.predicate);
        assert!(err_code(&mut s, &alter).await == case.code, "{}", case.why);
        // …and the refused ALTER left the table writable.
        run(&mut s, "INSERT INTO t VALUES (1, 'x')").await;
        assert!(
            query(&mut s, "SELECT a FROM t").await == vec![text_row(&["1"])],
            "{}",
            case.why
        );
    }
}

/// A generation expression may read only plain stored columns of the same row.
#[tokio::test]
async fn generation_expressions_are_analyzed_at_ddl_time() {
    struct Case {
        create: &'static str,
        code: &'static str,
        why: &'static str,
    }
    let cases = [
        Case {
            create: "CREATE TABLE t (a int4, b int4 GENERATED ALWAYS AS (a + 1) STORED, \
                     c int4 GENERATED ALWAYS AS (b + 1) STORED)",
            code: "42P17",
            why: "a generated column may not read another generated column",
        },
        Case {
            create: "CREATE TABLE t (a int4, b int4 GENERATED ALWAYS AS (nope + 1) STORED)",
            code: "42703",
            why: "the expression names a column the relation does not have",
        },
        Case {
            create: "CREATE TABLE t (a int4, b int4 GENERATED ALWAYS AS (sum(a)) STORED)",
            code: "42803",
            why: "a generation expression may not contain an aggregate",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[]).await;
        assert!(
            err_code(&mut s, case.create).await == case.code,
            "{}",
            case.why
        );
    }
}

// ---------------------------------------------------------------------------
// Constraint naming.

/// `PostgreSQL` assigns generated `CHECK` names in written order, and skips
/// those already taken. An explicit name that collides with one already
/// assigned in the same statement is 42710.
#[tokio::test]
async fn create_table_rejects_colliding_constraint_names() {
    let (_engine, mut s) = engine_with(&[]).await;

    assert!(
        err_code(
            &mut s,
            "CREATE TABLE t (a int4, CONSTRAINT d CHECK (a > 0), CONSTRAINT d CHECK (a < 5))"
        )
        .await
            == "42710"
    );
    assert!(
        err_code(
            &mut s,
            "CREATE TABLE t (a int4 CHECK (a > 0), CONSTRAINT t_a_check CHECK (a < 100))"
        )
        .await
            == "42710"
    );

    // The reverse order is legal: the generated name takes the free suffix.
    run(
        &mut s,
        "CREATE TABLE t (a int4, CONSTRAINT t_a_check CHECK (a < 100), CHECK (a > 0))",
    )
    .await;
    assert!(
        query(
            &mut s,
            "SELECT conname FROM pg_constraint WHERE conname LIKE 't_a_check%' ORDER BY conname"
        )
        .await
            == vec![text_row(&["t_a_check"]), text_row(&["t_a_check1"])]
    );
}

// ---------------------------------------------------------------------------
// ADD PRIMARY KEY ordering.

/// `PostgreSQL` builds the unique index before it attaches `NOT NULL`, so
/// duplicate data is 23505 even when the key column also holds NULLs, and the
/// message names the index build and not a row insertion.
#[tokio::test]
async fn add_primary_key_reports_duplicates_before_nulls() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (a int4, b int4)",
        "INSERT INTO t VALUES (1, 1), (1, 2), (NULL, 3)",
    ])
    .await;

    assert!(err_code(&mut s, "ALTER TABLE t ADD PRIMARY KEY (a)").await == "23505");
    assert!(
        err_message(&mut s, "ALTER TABLE t ADD PRIMARY KEY (a)").await
            == "could not create unique index \"t_pkey\""
    );

    run(&mut s, "DELETE FROM t WHERE b = 2").await;
    assert!(err_code(&mut s, "ALTER TABLE t ADD PRIMARY KEY (a)").await == "23502");
}

/// A `CREATE UNIQUE INDEX` that duplicate data defeats reports the same
/// index-build failure.
#[tokio::test]
async fn unique_index_build_over_duplicate_rows_reports_the_build() {
    let (_engine, mut s) =
        engine_with(&["CREATE TABLE t (a int4)", "INSERT INTO t VALUES (1), (1)"]).await;
    assert!(
        err_message(&mut s, "CREATE UNIQUE INDEX t_a ON t (a)").await
            == "could not create unique index \"t_a\""
    );
}

// ---------------------------------------------------------------------------
// View dependencies.

/// `PostgreSQL` tracks a view's dependency per column. A drop of a column no
/// view reads is allowed. A drop of one a view reads is 2BP01, and a retype of
/// one a view reads is 0A000.
#[tokio::test]
async fn view_dependencies_gate_drop_column_and_type_change() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (a int4, b int4)",
        "INSERT INTO t VALUES (1, 2)",
        "CREATE VIEW v AS SELECT a FROM t",
    ])
    .await;

    run(&mut s, "ALTER TABLE t DROP COLUMN b").await;
    assert!(query(&mut s, "SELECT a FROM v").await == vec![text_row(&["1"])]);

    assert!(err_code(&mut s, "ALTER TABLE t ALTER COLUMN a TYPE int8").await == "0A000");
    assert!(err_code(&mut s, "ALTER TABLE t DROP COLUMN a").await == "2BP01");
    assert!(err_code(&mut s, "DROP TABLE t").await == "2BP01");

    run(&mut s, "DROP TABLE t CASCADE").await;
    assert!(err_code(&mut s, "SELECT a FROM v").await == "42P01");
}

/// An unrelated view does not block a rename of a relation, and a view that
/// does read the renamed relation keeps working.
#[tokio::test]
async fn rename_to_only_touches_dependent_views() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE unrelated (a int4)",
        "CREATE VIEW unrelated_v AS SELECT a FROM unrelated",
        "CREATE TABLE lonely (b int4)",
    ])
    .await;

    run(&mut s, "ALTER TABLE lonely RENAME TO lonely2").await;
    run(&mut s, "INSERT INTO lonely2 VALUES (7)").await;
    assert!(query(&mut s, "SELECT b FROM lonely2").await == vec![text_row(&["7"])]);

    run(&mut s, "INSERT INTO unrelated VALUES (5)").await;
    run(&mut s, "ALTER TABLE unrelated RENAME TO unrelated2").await;
    assert!(query(&mut s, "SELECT a FROM unrelated_v").await == vec![text_row(&["5"])]);
    run(&mut s, "INSERT INTO unrelated2 VALUES (6)").await;
    assert!(
        query(&mut s, "SELECT a FROM unrelated_v ORDER BY a").await
            == vec![text_row(&["5"]), text_row(&["6"])]
    );
}

/// A generated column depends on every column its expression reads. A retype of
/// one is 0A000, a drop of one is 2BP01, and `CASCADE` takes the generated
/// column with it.
#[tokio::test]
async fn generated_columns_depend_on_the_columns_they_read() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (a int4, b int4 GENERATED ALWAYS AS (a * 2) STORED, c int4)",
        "INSERT INTO t (a, c) VALUES (3, 4)",
    ])
    .await;

    assert!(err_code(&mut s, "ALTER TABLE t ALTER COLUMN a TYPE int8").await == "0A000");
    // The generated column itself may be retyped: nothing reads it.
    run(&mut s, "ALTER TABLE t ALTER COLUMN b TYPE int8").await;
    assert!(query(&mut s, "SELECT a, b, c FROM t").await == vec![text_row(&["3", "6", "4"])]);

    assert!(err_code(&mut s, "ALTER TABLE t DROP COLUMN a").await == "2BP01");
    run(&mut s, "ALTER TABLE t DROP COLUMN a CASCADE").await;
    assert!(query(&mut s, "SELECT c FROM t").await == vec![text_row(&["4"])]);
    assert!(err_code(&mut s, "SELECT b FROM t").await == "42703");
}

/// `PostgreSQL` runs the DROP COLUMN pass before it builds constraints added by
/// the same ALTER TABLE, regardless of their written order. The missing key is
/// therefore a 42703 and the whole statement leaves both schema and catalog
/// untouched.
#[tokio::test]
async fn drop_column_precedes_an_added_unique_constraint() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE staged_unique (a int4, keep int4)",
        "INSERT INTO staged_unique VALUES (1, 2)",
    ])
    .await;

    let sql = "ALTER TABLE staged_unique ADD UNIQUE (a), DROP COLUMN a";
    assert!(err_code(&mut s, sql).await == "42703");
    assert!(err_message(&mut s, sql).await == "column \"a\" named in key does not exist");
    assert!(
        query(&mut s, "SELECT a, keep FROM staged_unique").await == vec![text_row(&["1", "2"])]
    );
    assert!(
        query(
            &mut s,
            "SELECT conname FROM pg_constraint WHERE conname = 'staged_unique_a_key'",
        )
        .await
            == Vec::<Vec<Option<String>>>::new()
    );
}

/// DROP COLUMN, ADD COLUMN, and ADD CONSTRAINT run in separate `PostgreSQL`
/// passes. Their written order therefore cannot make a constraint bind to the
/// dropped incarnation of a same-named column.
#[tokio::test]
async fn drop_and_readd_column_precedes_an_added_unique_constraint() {
    for (table, actions) in [
        (
            "staged_order_one",
            "ADD UNIQUE (a), DROP COLUMN a, ADD COLUMN a int4 DEFAULT 9",
        ),
        (
            "staged_order_two",
            "DROP COLUMN a, ADD UNIQUE (a), ADD COLUMN a int4 DEFAULT 9",
        ),
    ] {
        let (_engine, mut s) = engine_with(&[
            &format!("CREATE TABLE {table} (a int4, keep int4)"),
            &format!("INSERT INTO {table} VALUES (1, 2)"),
        ])
        .await;

        run(&mut s, &format!("ALTER TABLE {table} {actions}")).await;
        assert!(
            query(&mut s, &format!("SELECT a, keep FROM {table}")).await
                == vec![text_row(&["9", "2"])]
        );
        assert!(err_code(&mut s, &format!("INSERT INTO {table} VALUES (3, 9)")).await == "23505");
    }
}

/// SET NOT NULL is `PostgreSQL`'s column-attribute pass, before the pass that
/// builds a UNIQUE index. A row set containing both NULLs and duplicate keys
/// must therefore report the null failure first, independent of written order.
#[tokio::test]
async fn set_not_null_precedes_an_added_unique_constraint() {
    for actions in [
        "ADD UNIQUE (a), ALTER COLUMN a SET NOT NULL",
        "ALTER COLUMN a SET NOT NULL, ADD UNIQUE (a)",
    ] {
        let (_engine, mut s) = engine_with(&[
            "CREATE TABLE staged_not_null (a int4)",
            "INSERT INTO staged_not_null VALUES (NULL), (NULL), (1), (1)",
        ])
        .await;

        assert!(
            err_code(&mut s, &format!("ALTER TABLE staged_not_null {actions}")).await == "23502",
            "{actions}"
        );
        assert!(
            query(&mut s, "SELECT count(*) FROM staged_not_null").await == vec![text_row(&["4"])]
        );
    }
}

/// `NOT VALID` applies only to constraints `PostgreSQL` can validate lazily; an
/// index-backed one has to be built now.
#[tokio::test]
async fn not_valid_is_refused_for_index_backed_constraints() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int4, b int4)"]).await;
    for sql in [
        "ALTER TABLE t ADD CONSTRAINT t_p PRIMARY KEY (a) NOT VALID",
        "ALTER TABLE t ADD CONSTRAINT t_u UNIQUE (b) NOT VALID",
        "ALTER TABLE t ADD PRIMARY KEY (a) NOT VALID",
    ] {
        assert!(err_code(&mut s, sql).await == "0A000", "{sql}");
    }
}

/// One `ALTER TABLE` may not retype the same column twice.
#[tokio::test]
async fn one_statement_may_not_retype_a_column_twice() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int4)"]).await;
    assert!(
        err_code(
            &mut s,
            "ALTER TABLE t ALTER COLUMN a TYPE int8, ALTER COLUMN a TYPE int2"
        )
        .await
            == "0A000"
    );
    // Two separate statements are fine.
    run(&mut s, "ALTER TABLE t ALTER COLUMN a TYPE int8").await;
    run(&mut s, "ALTER TABLE t ALTER COLUMN a TYPE int2").await;
    run(&mut s, "INSERT INTO t VALUES (5)").await;
    assert!(query(&mut s, "SELECT a FROM t").await == vec![text_row(&["5"])]);
}

/// A view is a relation, so `PostgreSQL` reports the *action* as unsupported
/// for its kind, and does not claim the relation does not exist.
#[tokio::test]
async fn alter_table_on_a_view_reports_the_unsupported_action() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4)",
        "CREATE VIEW v AS SELECT id FROM t",
    ])
    .await;
    for (sql, action) in [
        ("ALTER TABLE v ADD COLUMN q int4", "ADD COLUMN"),
        ("ALTER TABLE v DROP COLUMN id", "DROP COLUMN"),
        (
            "ALTER TABLE v ALTER COLUMN id TYPE int8",
            "ALTER COLUMN ... SET DATA TYPE",
        ),
        (
            "ALTER TABLE v ADD CONSTRAINT c CHECK (id > 0)",
            "ADD CONSTRAINT",
        ),
        ("ALTER TABLE v ADD PRIMARY KEY (id)", "ADD CONSTRAINT"),
        ("ALTER TABLE v ADD UNIQUE (id)", "ADD CONSTRAINT"),
        ("ALTER TABLE v VALIDATE CONSTRAINT c", "VALIDATE CONSTRAINT"),
    ] {
        assert!(err_code(&mut s, sql).await == "42809", "{sql}");
        assert!(
            err_message(&mut s, sql).await
                == format!("ALTER action {action} cannot be performed on relation \"v\""),
            "{sql}"
        );
    }
}

/// A `PRIMARY KEY`/`UNIQUE` constraint on a sharded table has no global
/// enforcement, so it fails clear instead of creating a silent local-only index.
#[tokio::test]
async fn constraint_indexes_on_sharded_tables_fail_clear() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (id int4) SHARDED"]).await;
    for sql in [
        "ALTER TABLE t ADD PRIMARY KEY (id)",
        "ALTER TABLE t ADD UNIQUE (id)",
    ] {
        assert!(err_code(&mut s, sql).await == "0A000", "{sql}");
    }
}

// ---------------------------------------------------------------------------
// COMMENT ON.

/// An index and a sequence are relations, so `PostgreSQL` reports a failed
/// relation lookup for a missing one.
#[tokio::test]
async fn comment_on_missing_relation_kinds_report_42p01() {
    let (_engine, mut s) = engine_with(&[]).await;
    for sql in [
        "COMMENT ON INDEX nosuchidx IS 'x'",
        "COMMENT ON SEQUENCE nosuchseq IS 'x'",
        "COMMENT ON TABLE nosuchtab IS 'x'",
    ] {
        assert!(err_code(&mut s, sql).await == "42P01", "{sql}");
        assert!(
            err_message(&mut s, sql).await.starts_with("relation \""),
            "{sql}"
        );
    }
}

// ---------------------------------------------------------------------------
// Back-validation inside an open transaction.

/// One `BEGIN; INSERT …; <back-validating DDL>` case: the setup that seeds the
/// relation, the row the open transaction adds, the statement that must refuse
/// it, and the `(SQLSTATE, message)` `PostgreSQL` 18.4 reports.
struct UncommittedCase {
    setup: &'static [&'static str],
    insert: &'static str,
    ddl: &'static str,
    code: &'static str,
    message: &'static str,
}

/// `ALTER TABLE` back-validation scans the relation as the *open transaction*
/// sees it, not as the last commit left it.
///
/// A row this transaction inserted and has not committed is part of what the
/// relation will hold the moment the constraint takes effect, so validating
/// without it lets `BEGIN; INSERT …; ALTER TABLE … ADD CONSTRAINT …` commit
/// rows the new constraint forbids — a relation that violates its own
/// constraint from the instant it exists. Every back-validating subcommand
/// shares the one scan, so every one of them is checked here.
#[tokio::test]
async fn back_validation_sees_the_transactions_own_uncommitted_rows() {
    let cases = [
        UncommittedCase {
            setup: &["CREATE TABLE c (a int)"],
            insert: "INSERT INTO c VALUES (-1)",
            ddl: "ALTER TABLE c ADD CONSTRAINT c_ck CHECK (a > 0)",
            code: "23514",
            message: "check constraint \"c_ck\" of relation \"c\" is violated by some row",
        },
        UncommittedCase {
            setup: &["CREATE TABLE p (a int)"],
            insert: "INSERT INTO p VALUES (1), (1)",
            ddl: "ALTER TABLE p ADD PRIMARY KEY (a)",
            code: "23505",
            message: "could not create unique index \"p_pkey\"",
        },
        UncommittedCase {
            setup: &["CREATE TABLE n (a int)"],
            insert: "INSERT INTO n VALUES (NULL)",
            ddl: "ALTER TABLE n ALTER COLUMN a SET NOT NULL",
            code: "23502",
            message: "column \"a\" of relation \"n\" contains null values",
        },
        UncommittedCase {
            setup: &["CREATE TABLE u (a int)"],
            insert: "INSERT INTO u VALUES (2), (2)",
            ddl: "ALTER TABLE u ADD CONSTRAINT u_uq UNIQUE (a)",
            code: "23505",
            message: "could not create unique index \"u_uq\"",
        },
        UncommittedCase {
            setup: &[
                "CREATE TABLE e (room int4range, during tstzrange)",
                "INSERT INTO e VALUES ('[1,2)', tstzrange('2018-01-01','2018-02-01'))",
            ],
            insert: "INSERT INTO e VALUES ('[1,2)', tstzrange('2018-01-15','2018-03-01'))",
            ddl: "ALTER TABLE e ADD CONSTRAINT e_ex \
                  EXCLUDE USING gist (room WITH =, during WITH &&)",
            code: "23P01",
            message: "could not create exclusion constraint \"e_ex\"",
        },
        UncommittedCase {
            setup: &[
                "CREATE TABLE par (a int) PARTITION BY RANGE (a)",
                "CREATE TABLE ch (a int)",
            ],
            insert: "INSERT INTO ch VALUES (99)",
            ddl: "ALTER TABLE par ATTACH PARTITION ch FOR VALUES FROM (0) TO (10)",
            code: "23514",
            message: "partition constraint of relation \"ch\" is violated by some row",
        },
    ];
    for case in cases {
        let (_engine, mut s) = engine_with(case.setup).await;
        run(&mut s, "BEGIN").await;
        run(&mut s, case.insert).await;
        let error = s
            .simple_query(case.ddl)
            .await
            .expect_err("the uncommitted row must fail back-validation");
        assert!(error.code == case.code, "{}: {error:?}", case.ddl);
        assert!(error.message == case.message, "{}: {error:?}", case.ddl);
        run(&mut s, "ROLLBACK").await;
    }
}

/// The same scan must not over-reach: a row the transaction has *deleted* is
/// gone from what the constraint has to hold, and rows only the transaction can
/// see still validate normally when they conform.
#[tokio::test]
async fn back_validation_respects_the_transactions_own_deletes() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int)"]).await;
    run(&mut s, "INSERT INTO t VALUES (-1)").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "DELETE FROM t WHERE a = -1").await;
    run(&mut s, "INSERT INTO t VALUES (7)").await;
    // The only offending row is deleted, and the only surviving one is this
    // transaction's own — the constraint holds.
    run(&mut s, "ALTER TABLE t ADD CONSTRAINT t_ck CHECK (a > 0)").await;
    run(&mut s, "COMMIT").await;
    assert!(query(&mut s, "SELECT a FROM t").await == vec![text_row(&["7"])]);
    assert!(err_code(&mut s, "INSERT INTO t VALUES (-2)").await == "23514");
}

/// A `CHECK` written on an inheriting child may constrain a column the child
/// inherits rather than one it declares — that is the point of the clause — so
/// the predicate is analysed against the merged column list, and the
/// constraint's generated name is derived from the same list.
///
/// The name matters because it is what a violation reports and what `ALTER
/// TABLE … DROP CONSTRAINT` has to be given: `PostgreSQL` names a `CHECK` after
/// the single column its predicate references, falling back to
/// `<table>_check` when it references none or several.
#[tokio::test]
async fn a_check_on_an_inheriting_child_sees_the_inherited_columns() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE parent (a int)"]).await;
    run(
        &mut s,
        "CREATE TABLE only_inherited (CHECK (a > 0)) INHERITS (parent)",
    )
    .await;
    run(
        &mut s,
        "CREATE TABLE mixed (b int, CHECK (a > 0 AND b > 0)) INHERITS (parent)",
    )
    .await;
    assert!(
        err_message(
            &mut s,
            "CREATE TABLE unknown_col (CHECK (nosuch > 0)) INHERITS (parent)"
        )
        .await
            == "column \"nosuch\" does not exist"
    );
    assert!(
        query(
            &mut s,
            "SELECT conrelid::regclass::text, conname FROM pg_constraint
              WHERE conrelid IN ('only_inherited'::regclass, 'mixed'::regclass)
              ORDER BY 1, 2"
        )
        .await
            == vec![
                text_row(&["mixed", "mixed_check"]),
                text_row(&["only_inherited", "only_inherited_a_check"]),
            ]
    );
    run(&mut s, "INSERT INTO only_inherited VALUES (5)").await;
    assert!(err_code(&mut s, "INSERT INTO only_inherited VALUES (-5)").await == "23514");
    run(&mut s, "INSERT INTO mixed VALUES (5, 5)").await;
    assert!(err_code(&mut s, "INSERT INTO mixed VALUES (5, -5)").await == "23514");
    assert!(
        query(&mut s, "SELECT a FROM parent ORDER BY a").await
            == vec![text_row(&["5"]), text_row(&["5"])]
    );
}

/// Naming one column twice in an `INSERT` column list leaves the statement's
/// intent undecidable, so `PostgreSQL` refuses it rather than letting the
/// second value win.
#[tokio::test]
async fn an_insert_column_list_may_not_name_a_column_twice() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (a int, b int)"]).await;
    let error = s
        .simple_query("INSERT INTO t (a, b, a) VALUES (1, 2, 3)")
        .await
        .expect_err("expected error");
    assert!(error.code == "42701");
    assert!(error.message == "column \"a\" specified more than once");
    run(&mut s, "INSERT INTO t (a, b) VALUES (1, 2)").await;
    assert!(query(&mut s, "SELECT a, b FROM t").await == vec![text_row(&["1", "2"])]);
}

//! `PRIMARY KEY`/`UNIQUE (…, c WITHOUT OVERLAPS)` — `PostgreSQL` 18's temporal
//! keys. Such a key is catalogued as a primary key or unique constraint but
//! enforced like `EXCLUDE USING gist (a WITH =, c WITH &&)`, and every
//! expectation here is the behaviour of a live `PostgreSQL` 18.4.

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
        other => panic!("expected Rows, got {other:?}"),
    }
}

async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&run(s, sql).await[0])
}

async fn scalar(s: &mut SqlSession, sql: &str) -> Option<String> {
    query(s, sql)
        .await
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .flatten()
}

/// The `(SQLSTATE, message)` of a statement that must fail.
async fn failure(s: &mut SqlSession, sql: &str) -> (String, String) {
    let error = s.simple_query(sql).await.expect_err("expected an error");
    (error.code, error.message)
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in setup {
        run(&mut session, sql).await;
    }
    (engine, session)
}

/// A table with a temporal primary key over `(id, valid_at)`.
const TEMPORAL_PK: &[&str] = &["CREATE TABLE t (id int4range, valid_at daterange, \
     CONSTRAINT t_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"];

/// The same shape under `UNIQUE`, whose key columns stay nullable.
const TEMPORAL_UNIQUE: &[&str] = &["CREATE TABLE u (id int4range, valid_at daterange, \
     CONSTRAINT u_uq UNIQUE (id, valid_at WITHOUT OVERLAPS))"];

// The clause is only well-formed on the last column of a key that has a scalar
// part, over a type `&&` is defined for. PostgreSQL checks those three things
// in this order, and each has its own SQLSTATE.
#[tokio::test]
async fn a_malformed_temporal_key_is_refused_before_the_table_exists() {
    struct Case {
        sql: &'static str,
        code: &'static str,
        message: &'static str,
    }
    let cases = [
        Case {
            sql: "CREATE TABLE bad (valid_at daterange, \
                  CONSTRAINT bad_pk PRIMARY KEY (valid_at WITHOUT OVERLAPS))",
            code: "42601",
            message: "constraint using WITHOUT OVERLAPS needs at least two columns",
        },
        Case {
            sql: "CREATE TABLE bad (valid_at daterange, \
                  CONSTRAINT bad_uq UNIQUE (valid_at WITHOUT OVERLAPS))",
            code: "42601",
            message: "constraint using WITHOUT OVERLAPS needs at least two columns",
        },
        Case {
            sql: "CREATE TABLE bad (id integer, \
                  CONSTRAINT bad_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
            code: "42703",
            message: "column \"valid_at\" named in key does not exist",
        },
        Case {
            sql: "CREATE TABLE bad (id int4range, valid_at text, \
                  CONSTRAINT bad_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
            code: "42804",
            message: "column \"valid_at\" in WITHOUT OVERLAPS is not a range or multirange type",
        },
        Case {
            sql: "CREATE TABLE bad (id int4range, valid_at text, \
                  CONSTRAINT bad_uq UNIQUE (id, valid_at WITHOUT OVERLAPS))",
            code: "42804",
            message: "column \"valid_at\" in WITHOUT OVERLAPS is not a range or multirange type",
        },
    ];

    let (_engine, mut session) = engine_with(&[]).await;
    for case in cases {
        assert!(
            failure(&mut session, case.sql).await
                == (case.code.to_string(), case.message.to_string()),
            "{}",
            case.sql
        );
        // Nothing was created, so the name is still free for the next case.
        assert!(
            query(
                &mut session,
                "SELECT count(*) FROM pg_class WHERE relname = 'bad'",
            )
            .await
                == vec![vec![Some("0".into())]]
        );
    }
}

// `WITHOUT OVERLAPS` ends the key list, so anything but `)` after it is a
// syntax error rather than a semantic one — the clause cannot appear on a
// leading column, nor twice.
#[tokio::test]
async fn without_overlaps_may_only_end_the_key() {
    let (_engine, mut session) = engine_with(&[]).await;
    for sql in [
        "CREATE TABLE bad (a int4range, b daterange, \
         CONSTRAINT bad_pk PRIMARY KEY (b WITHOUT OVERLAPS, a))",
        "CREATE TABLE bad (a int4range, b daterange, \
         CONSTRAINT bad_pk PRIMARY KEY (a WITHOUT OVERLAPS, b WITHOUT OVERLAPS))",
    ] {
        assert!(failure(&mut session, sql).await.0 == "42601", "{sql}");
    }
}

// A range or multirange in the trailing position is accepted, including one
// over a domain and one of a user-defined range type — the operator resolves
// against the domain's base type.
#[tokio::test]
async fn every_range_shaped_type_is_accepted_in_the_temporal_position() {
    let (_engine, mut session) = engine_with(&[
        "CREATE DOMAIN int4range_d AS int4range",
        "CREATE TYPE textrange2 AS range (subtype=text, collation=\"C\")",
    ])
    .await;
    for (table, ty) in [
        ("k1", "daterange"),
        ("k2", "datemultirange"),
        ("k3", "int4range_d"),
        ("k4", "textrange2"),
    ] {
        run(
            &mut session,
            &format!(
                "CREATE TABLE {table} (id int4range, valid_at {ty}, \
                 CONSTRAINT {table}_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
            ),
        )
        .await;
    }
}

// The whole point: rows sharing the scalar key may not overlap in time, while
// adjacent ranges and differing scalar keys are free to coexist.
#[tokio::test]
async fn overlapping_rows_conflict_and_adjacent_ones_do_not() {
    let (_engine, mut session) = engine_with(TEMPORAL_PK).await;
    run(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2018-01-01','2018-02-01'))",
    )
    .await;

    let (code, message) = failure(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2018-01-15','2018-03-01'))",
    )
    .await;
    assert!(code == "23P01");
    assert!(message == "conflicting key value violates exclusion constraint \"t_pk\"");

    // Abutting at a shared bound is not an overlap: `[a,b)` and `[b,c)` share
    // no point.
    run(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2018-02-01','2018-03-01'))",
    )
    .await;
    // A different scalar key never conflicts, however the ranges lie.
    run(
        &mut session,
        "INSERT INTO t VALUES ('[2,3)', daterange('2018-01-15','2018-03-01'))",
    )
    .await;
    assert!(query(&mut session, "SELECT count(*) FROM t").await == vec![vec![Some("3".into())]]);
}

// An UPDATE that moves a range onto another row's is the same violation; one
// that leaves the key alone is not checked at all.
#[tokio::test]
async fn an_update_onto_an_occupied_range_conflicts() {
    let (_engine, mut session) = engine_with(TEMPORAL_PK).await;
    run(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2018-01-01','2018-02-01')), \
         ('[1,2)', daterange('2018-02-01','2018-03-01'))",
    )
    .await;

    let (code, _) = failure(
        &mut session,
        "UPDATE t SET valid_at = daterange('2018-01-01','2018-06-01') \
         WHERE valid_at = daterange('2018-02-01','2018-03-01')",
    )
    .await;
    assert!(code == "23P01");

    // Rewriting a non-key column touches no constraint.
    run(&mut session, "UPDATE t SET id = id").await;
    assert!(query(&mut session, "SELECT count(*) FROM t").await == vec![vec![Some("2".into())]]);
}

// A primary key makes every one of its columns NOT NULL, including the range;
// a unique constraint makes none of them, and NULLs never conflict.
#[tokio::test]
async fn a_temporal_primary_key_is_not_null_but_a_unique_one_is_not() {
    let (_engine, mut session) = engine_with(TEMPORAL_PK).await;
    for sql in [
        "INSERT INTO t VALUES (NULL, daterange('2018-01-01','2018-02-01'))",
        "INSERT INTO t VALUES ('[1,2)', NULL)",
    ] {
        assert!(failure(&mut session, sql).await.0 == "23502", "{sql}");
    }

    let (_engine, mut session) = engine_with(TEMPORAL_UNIQUE).await;
    // Under UNIQUE both columns stay nullable, and a NULL anywhere in the key
    // makes the row unconstrained — so the same row may be stored twice.
    for sql in [
        "INSERT INTO u VALUES (NULL, daterange('2018-01-01','2018-02-01'))",
        "INSERT INTO u VALUES (NULL, daterange('2018-01-01','2018-02-01'))",
        "INSERT INTO u VALUES ('[5,6)', NULL)",
        "INSERT INTO u VALUES ('[5,6)', NULL)",
    ] {
        run(&mut session, sql).await;
    }
    assert!(query(&mut session, "SELECT count(*) FROM u").await == vec![vec![Some("4".into())]]);
}

// An empty range overlaps nothing, so storing one would silently exempt the row
// from the constraint. PostgreSQL refuses it outright, on both write paths and
// for both range and multirange keys.
#[tokio::test]
async fn an_empty_range_is_refused_in_the_temporal_column() {
    let (_engine, mut session) = engine_with(TEMPORAL_PK).await;
    assert!(
        failure(&mut session, "INSERT INTO t VALUES ('[1,2)', 'empty')").await
            == (
                "23514".to_string(),
                "empty WITHOUT OVERLAPS value found in column \"valid_at\" in relation \"t\""
                    .to_string()
            )
    );

    run(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2018-01-01','2018-02-01'))",
    )
    .await;
    assert!(
        failure(&mut session, "UPDATE t SET valid_at = 'empty'::daterange")
            .await
            .0
            == "23514"
    );

    let (_engine, mut session) =
        engine_with(&["CREATE TABLE m (id int4range, valid_at datemultirange, \
         CONSTRAINT m_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"])
        .await;
    assert!(
        failure(&mut session, "INSERT INTO m VALUES ('[1,2)', '{}')").await
            == (
                "23514".to_string(),
                "empty WITHOUT OVERLAPS value found in column \"valid_at\" in relation \"m\""
                    .to_string()
            )
    );
}

// A multirange key compares with the multirange `&&`, so two rows conflict when
// any component overlaps.
#[tokio::test]
async fn a_multirange_key_conflicts_on_any_overlapping_component() {
    let (_engine, mut session) =
        engine_with(&["CREATE TABLE m (id int4range, valid_at datemultirange, \
         CONSTRAINT m_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"])
        .await;
    run(
        &mut session,
        "INSERT INTO m VALUES ('[1,2)', datemultirange(daterange('2018-01-01','2018-02-01')))",
    )
    .await;
    assert!(
        failure(
            &mut session,
            "INSERT INTO m VALUES ('[1,2)', \
             datemultirange(daterange('2018-01-15','2018-03-01')))",
        )
        .await
        .0 == "23P01"
    );
    run(
        &mut session,
        "INSERT INTO m VALUES ('[1,2)', datemultirange(daterange('2018-02-01','2018-03-01')))",
    )
    .await;
}

// `ALTER TABLE … ADD CONSTRAINT` reaches the same constraint, and back-validates
// the rows already stored — reporting the index build rather than a row.
#[tokio::test]
async fn alter_table_adds_a_temporal_key_and_back_validates() {
    let (_engine, mut session) =
        engine_with(&["CREATE TABLE t (id int4range, valid_at daterange)"]).await;
    run(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2018-01-02','2018-02-03')), \
         ('[1,2)', daterange('2018-01-01','2018-01-05'))",
    )
    .await;
    let (code, message) = failure(
        &mut session,
        "ALTER TABLE t ADD CONSTRAINT t_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
    )
    .await;
    assert!(code == "23P01");
    assert!(message == "could not create exclusion constraint \"t_pk\"");

    // With the overlap gone the same statement succeeds and starts enforcing.
    run(
        &mut session,
        "DELETE FROM t WHERE valid_at = daterange('2018-01-01','2018-01-05')",
    )
    .await;
    run(
        &mut session,
        "ALTER TABLE t ADD CONSTRAINT t_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
    )
    .await;
    assert!(
        failure(
            &mut session,
            "INSERT INTO t VALUES ('[1,2)', daterange('2018-01-10','2018-01-20'))",
        )
        .await
        .0 == "23P01"
    );
}

// The catalog reports the constraint as a primary key or unique constraint with
// `conperiod` set, backed by a unique GiST index — which is how psql's `\d`
// tells it apart from an ordinary key and echoes the definition verbatim.
#[tokio::test]
async fn the_catalog_reports_a_temporal_key_as_a_period_constraint() {
    let (_engine, mut session) = engine_with(&[
        TEMPORAL_PK[0],
        TEMPORAL_UNIQUE[0],
        "CREATE TABLE plain (id int4range, valid_at daterange, \
         CONSTRAINT plain_pk PRIMARY KEY (id, valid_at))",
    ])
    .await;

    assert!(
        query(
            &mut session,
            "SELECT conname, contype, conperiod FROM pg_constraint \
             WHERE conname IN ('t_pk', 'u_uq', 'plain_pk') ORDER BY conname",
        )
        .await
            == vec![
                vec![Some("plain_pk".into()), Some("p".into()), Some("f".into())],
                vec![Some("t_pk".into()), Some("p".into()), Some("t".into())],
                vec![Some("u_uq".into()), Some("u".into()), Some("t".into())],
            ]
    );

    assert!(
        query(
            &mut session,
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conname IN ('t_pk', 'u_uq', 'plain_pk') ORDER BY conname",
        )
        .await
            == vec![
                vec![Some("PRIMARY KEY (id, valid_at)".into())],
                vec![Some("PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)".into())],
                vec![Some("UNIQUE (id, valid_at WITHOUT OVERLAPS)".into())],
            ]
    );

    // `pretty` drops the schema for a relation on the search path; the plain
    // key keeps btree while the temporal one is a GiST index.
    assert!(
        query(
            &mut session,
            "SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint \
             WHERE conname IN ('t_pk', 'plain_pk') ORDER BY conname",
        )
        .await
            == vec![
                vec![Some(
                    "CREATE UNIQUE INDEX plain_pk ON plain USING btree (id, valid_at)".into()
                )],
                vec![Some(
                    "CREATE UNIQUE INDEX t_pk ON t USING gist (id, valid_at)".into()
                )],
            ]
    );

    // A temporal key is a unique constraint *and* an exclusion constraint.
    assert!(
        query(
            &mut session,
            "SELECT indisunique, indisprimary, indisexclusion FROM pg_index i \
             JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 't_pk'",
        )
        .await
            == vec![vec![Some("t".into()), Some("t".into()), Some("t".into())]]
    );
}

// `CREATE TABLE … (LIKE t INCLUDING ALL)` copies the constraint whole, temporal
// flag and access method included.
#[tokio::test]
async fn like_including_all_copies_the_temporal_flag() {
    let (_engine, mut session) = engine_with(TEMPORAL_PK).await;
    run(&mut session, "CREATE TABLE cloned (LIKE t INCLUDING ALL)").await;
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'cloned_pkey'",
        )
        .await
            == Some("PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)".into())
    );
    assert!(
        failure(
            &mut session,
            "INSERT INTO cloned VALUES ('[1,2)', daterange('2018-01-01','2018-02-01')), \
             ('[1,2)', daterange('2018-01-15','2018-03-01'))",
        )
        .await
        .0 == "23P01"
    );
}

// `ON CONFLICT` sees a temporal key as the exclusion constraint it is: column
// inference cannot name it, a bare `DO NOTHING` skips the overlapping row, and
// `DO UPDATE` has no single row to update.
#[tokio::test]
async fn on_conflict_treats_a_temporal_key_as_an_exclusion_constraint() {
    let (_engine, mut session) = engine_with(TEMPORAL_PK).await;
    run(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2000-01-01','2010-01-01'))",
    )
    .await;

    // Inference by column list arbitrates on equality, which this key is not.
    for action in ["DO NOTHING", "DO UPDATE SET id = EXCLUDED.id"] {
        let (code, message) = failure(
            &mut session,
            &format!(
                "INSERT INTO t VALUES ('[1,2)', daterange('2005-01-01','2006-01-01')) \
                 ON CONFLICT (id, valid_at) {action}"
            ),
        )
        .await;
        assert!(code == "42P10", "{action}");
        assert!(
            message
                == "there is no unique or exclusion constraint matching the ON CONFLICT \
                    specification"
        );
    }

    // Naming the constraint outright reaches it, but there is no one row to
    // update: an overlapping insert may conflict with several at once.
    let (code, message) = failure(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2005-01-01','2006-01-01')) \
         ON CONFLICT ON CONSTRAINT t_pk DO UPDATE SET id = EXCLUDED.id",
    )
    .await;
    assert!(code == "0A000");
    assert!(message == "ON CONFLICT DO UPDATE not supported with exclusion constraints");

    // `DO NOTHING` skips the overlapping row and stores the rest, whether the
    // constraint is named or inferred from the whole table.
    for target in ["", "ON CONSTRAINT t_pk"] {
        run(
            &mut session,
            &format!(
                "INSERT INTO t VALUES ('[1,2)', daterange('2005-01-01','2006-01-01')) \
                 ON CONFLICT {target} DO NOTHING"
            ),
        )
        .await;
    }
    run(
        &mut session,
        "INSERT INTO t VALUES ('[1,2)', daterange('2010-01-01','2020-01-01')) \
         ON CONFLICT DO NOTHING",
    )
    .await;
    assert!(query(&mut session, "SELECT count(*) FROM t").await == vec![vec![Some("2".into())]]);
}

// A temporal key is not an equality key, so no ordinary foreign key can prove a
// parent row unique through it — and a `PERIOD` foreign key, which could, is
// not implemented.
#[tokio::test]
async fn foreign_keys_onto_a_temporal_key_are_refused() {
    struct Case {
        sql: &'static str,
        code: &'static str,
        message: &'static str,
    }
    let (_engine, mut session) = engine_with(TEMPORAL_PK).await;
    let cases = [
        Case {
            sql: "CREATE TABLE c (parent_id int4range, valid_at daterange, \
                  FOREIGN KEY (parent_id, valid_at) REFERENCES t (id, valid_at))",
            code: "42830",
            message: "foreign key must use PERIOD when referencing a primary key using WITHOUT \
                      OVERLAPS",
        },
        Case {
            sql: "CREATE TABLE c (parent_id int4range, valid_at daterange, \
                  FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES t (id, valid_at))",
            code: "42830",
            message: "foreign key uses PERIOD on the referencing table but not the referenced \
                      table",
        },
        Case {
            sql: "CREATE TABLE c (parent_id int4range, valid_at daterange, \
                  FOREIGN KEY (parent_id, valid_at) REFERENCES t (id, PERIOD valid_at))",
            code: "42830",
            message: "foreign key uses PERIOD on the referenced table but not the referencing \
                      table",
        },
        Case {
            sql: "CREATE TABLE c (parent_id int4range, valid_at daterange, \
                  FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES t (id, PERIOD valid_at))",
            code: "0A000",
            message: "foreign keys using PERIOD are not supported",
        },
    ];
    for case in cases {
        assert!(
            failure(&mut session, case.sql).await
                == (case.code.to_string(), case.message.to_string()),
            "{}",
            case.sql
        );
    }
}

// `PERIOD` is an ordinary identifier everywhere else, so a column of that name
// still parses as one.
#[tokio::test]
async fn period_remains_usable_as_a_column_name() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE p (period int4range, CONSTRAINT p_pk PRIMARY KEY (period))",
        "CREATE TABLE q (period int4range, FOREIGN KEY (period) REFERENCES p (period))",
    ])
    .await;
    run(&mut session, "INSERT INTO p VALUES ('[1,2)')").await;
    run(&mut session, "INSERT INTO q VALUES ('[1,2)')").await;
    assert!(query(&mut session, "SELECT count(*) FROM q").await == vec![vec![Some("1".into())]]);
}

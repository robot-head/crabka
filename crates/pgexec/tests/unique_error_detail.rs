//! The `DETAIL` line that follows a key-constraint error, and who may see it.
//!
//! `PostgreSQL` builds all four of these from one function,
//! `BuildIndexValueDescription`, and wraps its `(cols)=(vals)` result in four
//! different sentences:
//!
//! * `Key (a)=(1) already exists.` — a write onto a taken key.
//! * `Key (a)=(1) is duplicated.` — a unique index that existing rows break.
//! * `Key … conflicts with existing key ….` — a write onto an excluded key.
//! * `Key … conflicts with key ….` — an exclusion constraint existing rows break.
//!
//! Every one of them prints stored values into an error message, so every one
//! is gated the way the function is: nothing at all when a row-level security
//! policy is active on the relation, and nothing at all when the caller holds no
//! `SELECT` on it. The exclusion pair keeps a bare sentence in that case, which
//! is upstream's own fallback and says nothing the primary message did not.
//!
//! Two things this line is *not*, both asserted below because both are easy to
//! assume from the neighbouring `Failing row contains` line, which does do them:
//! the values carry no quotes, and they are never truncated.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

/// The three fields of a failure this file judges, compared whole so that a
/// case which gains an unexpected `DETAIL` fails instead of passing quietly.
#[derive(Debug, PartialEq, Eq)]
struct Failure {
    code: String,
    message: String,
    detail: Option<String>,
}

impl Failure {
    fn new(code: &str, message: &str, detail: Option<&str>) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            detail: detail.map(ToString::to_string),
        }
    }
}

async fn failure(session: &mut SqlSession, sql: &str) -> Failure {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    Failure {
        code: error.code.clone(),
        message: error.message.clone(),
        detail: error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.detail.clone()),
    }
}

async fn engine_with(setup: &str) -> SqlEngine {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("in-memory engine");
    let mut session = engine.connect();
    run(&mut session, setup).await;
    engine
}

/// A duplicate-key case: the setup, the write that takes a taken key, the
/// constraint named, and the key description that must follow.
struct DuplicateCase {
    what: &'static str,
    setup: &'static str,
    write: &'static str,
    constraint: &'static str,
    key: &'static str,
}

/// A write onto an occupied key reports the key it could not have.
#[tokio::test]
async fn a_duplicate_key_reports_the_key_that_is_already_taken() {
    let cases = [
        DuplicateCase {
            what: "a primary key",
            setup: "CREATE TABLE pk (a int PRIMARY KEY); INSERT INTO pk VALUES (1)",
            write: "INSERT INTO pk VALUES (1)",
            constraint: "pk_pkey",
            key: "(a)=(1)",
        },
        DuplicateCase {
            what: "a multi-column key",
            setup: "CREATE TABLE mc (a int, b int, UNIQUE (a, b)); INSERT INTO mc VALUES (1, 2)",
            write: "INSERT INTO mc VALUES (1, 2)",
            constraint: "mc_a_b_key",
            key: "(a, b)=(1, 2)",
        },
        DuplicateCase {
            // The output function, not a literal: no quotes around the text.
            what: "a text key",
            setup: "CREATE TABLE tk (f1 text UNIQUE); INSERT INTO tk VALUES ('b')",
            write: "INSERT INTO tk VALUES ('b')",
            constraint: "tk_f1_key",
            key: "(f1)=(b)",
        },
        DuplicateCase {
            // `character(20)` pads, and the padding is part of the output.
            what: "a blank-padded key",
            setup: "CREATE TABLE bp (s char(20) UNIQUE); INSERT INTO bp VALUES ('pad')",
            write: "INSERT INTO bp VALUES ('pad')",
            constraint: "bp_s_key",
            key: "(s)=(pad                 )",
        },
        DuplicateCase {
            what: "an UPDATE onto a taken key",
            setup: "CREATE TABLE up (a int UNIQUE); INSERT INTO up VALUES (1), (2)",
            write: "UPDATE up SET a = 1 WHERE a = 2",
            constraint: "up_a_key",
            key: "(a)=(1)",
        },
        DuplicateCase {
            // Two rows of one statement claiming one key never reach storage,
            // and must still describe the key they collided on.
            what: "two rows of the same statement",
            setup: "CREATE TABLE ss (a int UNIQUE)",
            write: "INSERT INTO ss VALUES (7), (7)",
            constraint: "ss_a_key",
            key: "(a)=(7)",
        },
    ];
    for case in cases {
        let engine = engine_with(case.setup).await;
        let mut session = engine.connect();
        assert!(
            failure(&mut session, case.write).await
                == Failure::new(
                    "23505",
                    &format!(
                        "duplicate key value violates unique constraint \"{}\"",
                        case.constraint
                    ),
                    Some(&format!("Key {} already exists.", case.key)),
                ),
            "{}",
            case.what
        );
    }
}

/// A long value is printed whole.
///
/// `BuildIndexValueDescription` is the one description upstream does *not*
/// clip: it says so in as many words, on the reasoning that an index entry is
/// not as wide as a heap field can be. The sibling `Failing row contains` line
/// cuts at 64 bytes and appends `...`, and copying that rule here would corrupt
/// a key the reader needs to identify the row.
#[tokio::test]
async fn a_long_key_value_is_not_truncated() {
    let long = "x".repeat(100);
    let engine = engine_with("CREATE TABLE wide (s text PRIMARY KEY)").await;
    let mut session = engine.connect();
    run(&mut session, &format!("INSERT INTO wide VALUES ('{long}')")).await;
    assert!(
        failure(&mut session, &format!("INSERT INTO wide VALUES ('{long}')")).await
            == Failure::new(
                "23505",
                "duplicate key value violates unique constraint \"wide_pkey\"",
                Some(&format!("Key (s)=({long}) already exists.")),
            )
    );
}

/// A unique index that the stored rows already break names the duplicate, and
/// says `is duplicated` rather than `already exists` — the build failed, no
/// write did.
#[tokio::test]
async fn a_failed_unique_index_build_reports_the_duplicated_key() {
    let engine =
        engine_with("CREATE TABLE dup (a int, b int); INSERT INTO dup VALUES (1, 5), (1, 6)").await;
    let mut session = engine.connect();
    assert!(
        failure(&mut session, "CREATE UNIQUE INDEX dup_a ON dup (a)").await
            == Failure::new(
                "23505",
                "could not create unique index \"dup_a\"",
                Some("Key (a)=(1) is duplicated."),
            )
    );
    // `ALTER TABLE … ADD` builds the same index and reports the same way.
    assert!(
        failure(
            &mut session,
            "ALTER TABLE dup ADD CONSTRAINT dup_uq UNIQUE (a)"
        )
        .await
            == Failure::new(
                "23505",
                "could not create unique index \"dup_uq\"",
                Some("Key (a)=(1) is duplicated."),
            )
    );
}

/// An exclusion constraint names both keys: the one being written and the one
/// already stored.
#[tokio::test]
async fn an_exclusion_conflict_reports_both_keys() {
    let engine = engine_with(
        "CREATE TABLE ex (id int, valid_at daterange, \
         CONSTRAINT ex_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));
         INSERT INTO ex VALUES (1, daterange('2018-01-01', '2018-02-01'))",
    )
    .await;
    let mut session = engine.connect();
    assert!(
        failure(
            &mut session,
            "INSERT INTO ex VALUES (1, daterange('2018-01-15', '2018-03-01'))"
        )
        .await
            == Failure::new(
                "23P01",
                "conflicting key value violates exclusion constraint \"ex_pk\"",
                Some(
                    "Key (id, valid_at)=(1, [2018-01-15,2018-03-01)) conflicts with existing key \
                     (id, valid_at)=(1, [2018-01-01,2018-02-01))."
                ),
            )
    );
}

/// A caller who may not read the relation is told the key is taken and not what
/// the key is.
///
/// There is no middle form here, unlike the row description beside it: upstream
/// returns nothing rather than a partial key, because it cannot tell a key
/// column the caller supplied from one it could not have read. The exclusion
/// pair keeps its sentence and drops both keys.
#[tokio::test]
async fn a_caller_without_select_is_not_told_the_key() {
    let engine = engine_with(
        "CREATE TABLE secretkey (a int PRIMARY KEY, b text);
         INSERT INTO secretkey VALUES (1, 'classified');
         CREATE TABLE secretex (id int, valid_at daterange,
             CONSTRAINT secretex_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));
         INSERT INTO secretex VALUES (1, daterange('2018-01-01', '2018-02-01'));
         CREATE ROLE writer;
         GRANT INSERT ON secretkey TO writer;
         GRANT INSERT ON secretex TO writer",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "SET ROLE writer").await;
    let unique = "INSERT INTO secretkey VALUES (1, 'guess')";
    let exclusion = "INSERT INTO secretex VALUES (1, daterange('2018-01-15', '2018-03-01'))";
    assert!(
        failure(&mut session, unique).await
            == Failure::new(
                "23505",
                "duplicate key value violates unique constraint \"secretkey_pkey\"",
                None,
            )
    );
    assert!(
        failure(&mut session, exclusion).await
            == Failure::new(
                "23P01",
                "conflicting key value violates exclusion constraint \"secretex_pk\"",
                Some("Key conflicts with existing key."),
            )
    );
    // Both descriptions appear once the caller may read the relations.
    let mut owner = engine.connect();
    run(
        &mut owner,
        "GRANT SELECT ON secretkey TO writer; GRANT SELECT ON secretex TO writer",
    )
    .await;
    assert!(
        failure(&mut session, unique).await
            == Failure::new(
                "23505",
                "duplicate key value violates unique constraint \"secretkey_pkey\"",
                Some("Key (a)=(1) already exists."),
            )
    );
    assert!(
        failure(&mut session, exclusion).await
            == Failure::new(
                "23P01",
                "conflicting key value violates exclusion constraint \"secretex_pk\"",
                Some(
                    "Key (id, valid_at)=(1, [2018-01-15,2018-03-01)) conflicts with existing key \
                     (id, valid_at)=(1, [2018-01-01,2018-02-01))."
                ),
            )
    );
}

/// A column-level grant covering every key column still describes nothing.
///
/// This is the one place the gate is deliberately narrower than upstream.
/// `BuildIndexValueDescription` falls back to `pg_attribute_aclcheck` on each
/// key column and describes the key when the caller holds `SELECT` on all of
/// them. Here a column grant is recorded but does not admit a read at all —
/// `crate::privilege` takes the read permit before the projection is known — so
/// honouring the fallback would print values out of a relation the caller
/// cannot scan. Withholding is the fail-closed direction, and the cost is
/// nothing: no key description ever appears where upstream shows none.
#[tokio::test]
async fn a_column_grant_over_the_whole_key_is_still_not_enough_to_see_it() {
    let engine = engine_with(
        "CREATE TABLE colgrant (a int PRIMARY KEY, b text);
         INSERT INTO colgrant VALUES (1, 'classified');
         CREATE ROLE writer;
         GRANT INSERT ON colgrant TO writer;
         GRANT SELECT (a) ON colgrant TO writer",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "SET ROLE writer").await;
    assert!(
        failure(&mut session, "INSERT INTO colgrant VALUES (1, 'guess')").await
            == Failure::new(
                "23505",
                "duplicate key value violates unique constraint \"colgrant_pkey\"",
                None,
            )
    );
}

/// No key is described where a row-level security policy is active.
///
/// The caller here holds `SELECT` outright, so the policy is the only thing
/// withholding the key — and dropping the policy restores it. Without this gate
/// the message would answer, for any key the caller cares to guess, whether a
/// row it is forbidden to see holds that key.
#[tokio::test]
async fn an_active_row_security_policy_withholds_the_key() {
    let engine = engine_with(
        "CREATE TABLE hidden (a int PRIMARY KEY, owner text);
         INSERT INTO hidden VALUES (1, 'someone else');
         CREATE ROLE writer;
         GRANT INSERT, SELECT ON hidden TO writer;
         ALTER TABLE hidden ENABLE ROW LEVEL SECURITY;
         CREATE POLICY mine ON hidden USING (owner = CURRENT_USER) WITH CHECK (true)",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "SET ROLE writer").await;
    let probe = "INSERT INTO hidden VALUES (1, 'writer')";
    assert!(
        failure(&mut session, probe).await
            == Failure::new(
                "23505",
                "duplicate key value violates unique constraint \"hidden_pkey\"",
                None,
            )
    );
    let mut owner = engine.connect();
    run(&mut owner, "ALTER TABLE hidden DISABLE ROW LEVEL SECURITY").await;
    assert!(
        failure(&mut session, probe).await
            == Failure::new(
                "23505",
                "duplicate key value violates unique constraint \"hidden_pkey\"",
                Some("Key (a)=(1) already exists."),
            )
    );
}

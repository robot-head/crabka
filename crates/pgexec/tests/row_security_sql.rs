//! Row-level security through the SQL surface, end to end.
//!
//! The companion file `row_security.rs` reaches the catalog directly, because
//! it was written while no statement could enable row security. Everything here
//! goes through `CREATE POLICY` and `ALTER TABLE … ENABLE ROW LEVEL SECURITY`,
//! which is what this slice made reachable — and the reason the whole slice is
//! atomic: the moment a policy can be created, every command has to honour it.
//!
//! The tests that matter most are the ones that pin a *bypass*. The executor has
//! six paths that read a stored relation without going through the ordinary
//! scan (aggregate pushdown, streaming aggregate, join count, local index
//! probe, partition scan, inheritance scan) and four that write rows without
//! going through each other (`INSERT`, `UPDATE`/`DELETE`, `ON CONFLICT DO
//! UPDATE`, `MERGE`). A policy that hides a row has to hide it through every one
//! of them, and a leak through any single one is a total leak.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Every row of the first result, each rendered as a comma-joined string so a
/// whole expectation is one literal.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_text(cell.as_ref()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

/// The SQLSTATE, message and `HINT` a statement was refused with.
///
/// `PostgreSQL` carries the remedy in a separate `HINT` field rather than
/// running it onto the end of the message, so a test that pins only the message
/// cannot tell a correct split from a dropped sentence.
async fn error_and_hint_of(
    session: &mut SqlSession,
    sql: &str,
) -> (String, String, Option<String>) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    let hint = error
        .diagnostics
        .as_ref()
        .and_then(|fields| fields.hint.clone());
    (error.code.clone(), error.message.clone(), hint)
}

/// A `COPY … TO STDOUT`'s whole payload, as the client would receive it.
async fn copied(session: &mut SqlSession, sql: &str) -> String {
    let stream = session
        .begin_copy_out(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
        .unwrap_or_else(|| panic!("{sql} should be a copy-out"));
    let mut out = Vec::new();
    for row in &stream.rows {
        out.extend_from_slice(row);
    }
    String::from_utf8(out).expect("copy-out payload is utf8")
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// A relation owned by `alice`, five rows, with the index and tree shapes the
/// bypass tests need.
///
/// `bob` holds a grant on everything he reads or writes here — `ALL` on
/// `document`, which the tests below select from, insert into, update, delete
/// from and truncate, and `SELECT` on the two tree parents, which they only
/// read. Every test in this file is about which *rows* a policy admits; without
/// the grants a non-owner would be refused at the privilege gate first, and a
/// privilege denial would mask the row-security behaviour under test.
///
/// The children and the partition leaves are deliberately **not** granted. A
/// tree is read under the privileges of the relation the query named, so a
/// grant on `parent` reaches `child`'s rows and one on `measure` reaches both
/// leaves — the same rule that makes the parent's policies, and not a child's,
/// govern the whole tree. Granting them anyway would hide a regression in it.
const SETUP: &str = r"
CREATE ROLE alice;
CREATE ROLE bob;
CREATE TABLE document (id int4, holder text, title text);
INSERT INTO document VALUES
  (1, 'alice', 'a'), (2, 'bob', 'b'), (3, 'alice', 'c'), (4, 'bob', 'd'), (5, 'alice', 'e');
CREATE INDEX document_id_idx ON document (id);
ALTER TABLE document OWNER TO alice;
CREATE TABLE parent (id int4, holder text);
CREATE TABLE child () INHERITS (parent);
INSERT INTO parent VALUES (10, 'alice');
INSERT INTO child VALUES (11, 'bob');
ALTER TABLE parent OWNER TO alice;
ALTER TABLE child OWNER TO alice;
CREATE TABLE measure (id int4, bucket int4) PARTITION BY RANGE (bucket);
CREATE TABLE measure_low PARTITION OF measure FOR VALUES FROM (0) TO (10);
CREATE TABLE measure_high PARTITION OF measure FOR VALUES FROM (10) TO (20);
INSERT INTO measure VALUES (1, 5), (2, 15);
ALTER TABLE measure OWNER TO alice;
GRANT ALL ON document TO bob;
GRANT SELECT ON parent TO bob;
GRANT SELECT ON measure TO bob;
";

/// An engine with [`SETUP`] applied, and a session acting as `alice`, who owns
/// every relation and may therefore write its policies.
async fn owned_engine() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut alice = engine.connect();
    run(&mut alice, SETUP).await;
    run(&mut alice, "SET ROLE alice").await;
    (engine, alice)
}

/// A session acting as `bob`, who owns nothing and is exempt from nothing.
async fn as_bob(engine: &SqlEngine) -> SqlSession {
    let mut session = engine.connect();
    run(&mut session, "SET ROLE bob").await;
    session
}

// ------------------------------------------------------------------ the leaks

/// **One leak test per optimizer bypass.**
///
/// Each of these reads reaches a different path that can produce rows of a
/// stored relation without the ordinary scan. The policy hides ids 1–3; every
/// path must agree about that, including the ones that fold or truncate rows
/// inside the scanner, where the qual has not run yet.
#[tokio::test]
async fn a_policy_hides_a_row_through_every_optimizer_bypass() {
    struct Case {
        bypass: &'static str,
        sql: &'static str,
        expected: &'static [&'static str],
    }
    let cases = [
        Case {
            bypass: "the ordinary MVCC scan",
            sql: "SELECT id FROM document ORDER BY id",
            expected: &["4", "5"],
        },
        Case {
            bypass: "the streaming aggregate, which folds inside the scanner",
            sql: "SELECT count(*) FROM document",
            expected: &["2"],
        },
        Case {
            bypass: "the aggregate pushdown, which sums inside the range owner",
            sql: "SELECT sum(id) FROM document",
            expected: &["9"],
        },
        Case {
            bypass: "the local index equality probe, which answers from the index",
            sql: "SELECT id FROM document WHERE id = 1",
            expected: &[],
        },
        Case {
            bypass: "the join count, which counts joined rows",
            sql: "SELECT count(*) FROM document a JOIN document b ON a.id = b.id",
            expected: &["2"],
        },
        Case {
            bypass: "the top-K, which truncates before the qual runs",
            sql: "SELECT id FROM document ORDER BY id LIMIT 2",
            expected: &["4", "5"],
        },
        Case {
            bypass: "the inheritance scan",
            sql: "SELECT id FROM parent ORDER BY id",
            expected: &["10"],
        },
        Case {
            bypass: "the partition scan",
            sql: "SELECT id FROM measure ORDER BY id",
            expected: &["2"],
        },
        Case {
            bypass: "an aggregate over the whole partition tree",
            sql: "SELECT count(*) FROM measure",
            expected: &["1"],
        },
    ];

    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY high ON document USING (id > 3);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         CREATE POLICY high ON parent USING (id < 11);
         ALTER TABLE parent ENABLE ROW LEVEL SECURITY;
         CREATE POLICY high ON measure USING (bucket > 10);
         ALTER TABLE measure ENABLE ROW LEVEL SECURITY",
    )
    .await;

    let mut bob = as_bob(&engine).await;
    for case in cases {
        assert!(
            query(&mut bob, case.sql).await == rows(case.expected),
            "{}",
            case.bypass
        );
    }
}

/// Row security with no applicable policy hides every row rather than showing
/// every row. This is the default-deny identity, observed from SQL.
#[tokio::test]
async fn enabling_row_security_with_no_policy_hides_everything() {
    let (engine, mut alice) = owned_engine().await;
    run(&mut alice, "ALTER TABLE document ENABLE ROW LEVEL SECURITY").await;
    let mut bob = as_bob(&engine).await;
    assert!(query(&mut bob, "SELECT count(*) FROM document").await == rows(&["0"]));
    // And the owner, who bypasses, still sees them all — proving the rows are
    // there to be hidden.
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["5"]));
}

/// Permissive policies OR together; a restrictive policy ANDs onto the result
/// and can only ever remove rows.
#[tokio::test]
async fn permissive_policies_or_and_restrictive_policies_and() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         CREATE POLICY low ON document USING (id = 1);
         CREATE POLICY high ON document USING (id = 5)",
    )
    .await;
    let mut bob = as_bob(&engine).await;
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["1", "5"]));

    run(
        &mut alice,
        "CREATE POLICY odd ON document AS RESTRICTIVE USING (id > 2)",
    )
    .await;
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["5"]));

    // A restrictive policy never grants: dropping both permissive policies
    // leaves nothing visible even though the restrictive one admits four rows.
    run(&mut alice, "DROP POLICY low ON document").await;
    run(&mut alice, "DROP POLICY high ON document").await;
    assert!(query(&mut bob, "SELECT count(*) FROM document").await == rows(&["0"]));
}

/// The owner reads its own relation unfiltered until it asks not to; `BYPASSRLS`
/// beats even `FORCE`.
#[tokio::test]
async fn owner_and_bypassrls_exemptions() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY high ON document USING (id > 3);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["5"]));

    run(&mut alice, "ALTER TABLE document FORCE ROW LEVEL SECURITY").await;
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["2"]));

    run(
        &mut alice,
        "ALTER TABLE document NO FORCE ROW LEVEL SECURITY",
    )
    .await;
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["5"]));

    let mut bootstrap = engine.connect();
    run(&mut bootstrap, "CREATE ROLE exempt WITH BYPASSRLS").await;
    // `BYPASSRLS` exempts a role from policies, not from `GRANT`, so the
    // exempt role still needs the read privilege — what is under test is that
    // it sees the rows a policy would have hidden, not that it may read at all.
    run(&mut alice, "GRANT SELECT ON document TO exempt").await;
    run(&mut alice, "ALTER TABLE document FORCE ROW LEVEL SECURITY").await;
    let mut exempt = engine.connect();
    run(&mut exempt, "SET ROLE exempt").await;
    assert!(query(&mut exempt, "SELECT count(*) FROM document").await == rows(&["5"]));
}

// ---------------------------------------------------------------- write paths

/// An `UPDATE` cannot change a row it cannot see, and a `DELETE` cannot remove
/// one — silently, because a plain statement that matches nothing is not an
/// error. `RETURNING` reports only what was actually touched.
#[tokio::test]
async fn update_and_delete_cannot_touch_an_invisible_row() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY high ON document USING (id > 3);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    // Nothing returned, and nothing changed: the row is not there for bob.
    assert!(
        query(
            &mut bob,
            "UPDATE document SET title = 'hacked' WHERE id = 1 RETURNING id"
        )
        .await
            == Vec::<String>::new()
    );
    assert!(
        query(&mut bob, "DELETE FROM document WHERE id = 1 RETURNING id").await
            == Vec::<String>::new()
    );
    assert!(query(&mut alice, "SELECT title FROM document WHERE id = 1").await == rows(&["a"]));

    // A row it can see moves normally, and RETURNING respects the same qual.
    assert!(
        query(
            &mut bob,
            "UPDATE document SET title = 'seen' WHERE id = 4 RETURNING id, title"
        )
        .await
            == rows(&["4,seen"])
    );
    assert!(
        query(&mut bob, "DELETE FROM document WHERE id = 5 RETURNING id").await == rows(&["5"])
    );
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["4"]));

    // TRUNCATE desugars to a DELETE per relation here, so it meets the same
    // `USING` qual and empties only what the role can see. `PostgreSQL` exempts
    // TRUNCATE from row security and empties the whole relation; keeping the
    // qual is the conservative difference, and it never empties more than the
    // role could have deleted a row at a time.
    run(&mut bob, "TRUNCATE document").await;
    assert!(
        query(&mut alice, "SELECT id FROM document ORDER BY id").await == rows(&["1", "2", "3"])
    );
}

/// `WITH CHECK` rejects a row a statement would write, for both `INSERT` and
/// `UPDATE`, with the message `PostgreSQL` uses.
#[tokio::test]
async fn with_check_rejects_an_insert_and_an_update() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY own ON document USING (holder = current_user)
           WITH CHECK (holder = current_user);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    let (sqlstate, message) = error_of(
        &mut bob,
        "INSERT INTO document VALUES (6, 'alice', 'stolen')",
    )
    .await;
    assert!(sqlstate == "42501");
    assert!(message == "new row violates row-level security policy for table \"document\"");

    // Writing a row it may see is fine.
    run(&mut bob, "INSERT INTO document VALUES (6, 'bob', 'mine')").await;

    // And an UPDATE that would move a visible row out of the policy is the same
    // refusal — the row is visible, so this is a check failure, not a silent
    // skip.
    let (sqlstate, message) = error_of(
        &mut bob,
        "UPDATE document SET holder = 'alice' WHERE id = 6",
    )
    .await;
    assert!(sqlstate == "42501");
    assert!(message == "new row violates row-level security policy for table \"document\"");
}

/// A policy with `USING` and no `WITH CHECK` uses its `USING` as the check —
/// and the violation reads exactly like any other.
///
/// `PostgreSQL` is explicit that this one is *not* reported as a `USING`
/// expression: the qual originated as a `USING` clause for row security in
/// general, rather than being an explicit `USING` acting as a security barrier
/// against a row the statement found.
#[tokio::test]
async fn a_policy_without_with_check_falls_back_to_its_using_qual() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY own ON document USING (holder = current_user);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;
    let (sqlstate, message) = error_of(
        &mut bob,
        "INSERT INTO document VALUES (6, 'alice', 'stolen')",
    )
    .await;
    assert!(sqlstate == "42501");
    assert!(message == "new row violates row-level security policy for table \"document\"");
}

/// **Who a check violation blames.**
///
/// A permissive policy is never named: failing the permissive fold means *no*
/// policy granted permission to write the row, rather than any one policy
/// having been violated, so there is nothing to name. A restrictive policy is
/// the only thing that can reject a row the fold already admitted, so each is
/// checked on its own and named when it does.
///
/// The route the write took makes no difference — a statement rewritten through
/// an automatically updatable view reaches the same check on the same relation,
/// and has to report it the same way.
#[tokio::test]
async fn a_check_violation_names_a_restrictive_policy_and_never_a_permissive_one() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW docv AS SELECT * FROM document;
         GRANT SELECT, INSERT, UPDATE, DELETE ON docv TO bob;
         CREATE POLICY own ON document USING (holder = current_user)
           WITH CHECK (holder = current_user);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         -- FORCE, because the view runs as its owner and the owner owns the
         -- relation: without it the policy would not reach the view route at
         -- all, and the two routes would not be comparable.
         ALTER TABLE document FORCE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    // One permissive policy, rejecting the row: nameless.
    for target in ["document", "docv"] {
        let (sqlstate, message) = error_of(
            &mut bob,
            &format!("INSERT INTO {target} VALUES (6, 'alice', 'stolen')"),
        )
        .await;
        assert!(sqlstate == "42501", "{target}");
        assert!(
            message == "new row violates row-level security policy for table \"document\"",
            "{target}"
        );
    }

    // Several permissive policies, all rejecting the row: still nameless, since
    // the violation is of the fold rather than of either policy.
    run(
        &mut alice,
        "DROP POLICY own ON document;
         CREATE POLICY one ON document USING (id > 100) WITH CHECK (id > 100);
         CREATE POLICY two ON document USING (id > 200) WITH CHECK (id > 200)",
    )
    .await;
    let (_, message) = error_of(&mut bob, "INSERT INTO document VALUES (6, 'bob', 'x')").await;
    assert!(message == "new row violates row-level security policy for table \"document\"");

    // A restrictive policy rejecting a row the permissive fold admitted: named.
    run(
        &mut alice,
        "DROP POLICY one ON document;
         DROP POLICY two ON document;
         CREATE POLICY anyone ON document USING (true) WITH CHECK (true);
         CREATE POLICY not_bad ON document AS RESTRICTIVE
           USING (title <> 'bad') WITH CHECK (title <> 'bad')",
    )
    .await;
    for target in ["document", "docv"] {
        let (sqlstate, message) = error_of(
            &mut bob,
            &format!("INSERT INTO {target} VALUES (7, 'bob', 'bad')"),
        )
        .await;
        assert!(sqlstate == "42501", "{target}");
        assert!(
            message
                == "new row violates row-level security policy \"not_bad\" for table \"document\"",
            "{target}"
        );
    }
    run(&mut bob, "INSERT INTO docv VALUES (8, 'bob', 'fine')").await;
    assert!(query(&mut alice, "SELECT id FROM document WHERE id = 8").await == rows(&["8"]));
}

/// **A `BEFORE ROW` trigger may not launder a row past `WITH CHECK`.**
///
/// The trigger returns a *replacement* row, and the replacement is what gets
/// written. A check that ran before the trigger — the obvious place, next to
/// `NOT NULL` and `CHECK` — would see the row the caller wrote and never the row
/// that landed.
#[tokio::test]
async fn a_before_row_trigger_cannot_launder_a_row_past_with_check() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE FUNCTION steal() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           NEW.holder := 'alice';
           RETURN NEW;
         END $$",
    )
    .await;
    run(
        &mut alice,
        "CREATE TRIGGER launder BEFORE INSERT ON document FOR EACH ROW
           EXECUTE FUNCTION steal()",
    )
    .await;
    run(
        &mut alice,
        "CREATE POLICY own ON document USING (holder = current_user)
           WITH CHECK (holder = current_user);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;

    let mut bob = as_bob(&engine).await;
    // The row bob writes satisfies the policy; the row the trigger substitutes
    // does not, and it is the substituted one that is judged.
    let (sqlstate, message) =
        error_of(&mut bob, "INSERT INTO document VALUES (6, 'bob', 'mine')").await;
    assert!(sqlstate == "42501");
    assert!(message == "new row violates row-level security policy for table \"document\"");
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["5"]));
}

/// `INSERT … ON CONFLICT DO UPDATE` reaches its row through the arbiter index
/// probe, which shares no gate with anything else. Failing the `UPDATE` `USING`
/// qual on the *stored* conflicting row is an error, not a skip.
#[tokio::test]
async fn on_conflict_do_update_refuses_an_invisible_target_row() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE UNIQUE INDEX document_uid ON document (id);
         CREATE POLICY high ON document USING (id > 3) WITH CHECK (true);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    let (sqlstate, message) = error_of(
        &mut bob,
        "INSERT INTO document VALUES (1, 'bob', 'x')
           ON CONFLICT (id) DO UPDATE SET title = 'taken'",
    )
    .await;
    assert!(sqlstate == "42501");
    // PostgreSQL words this one `new row violates row-level security policy for
    // table "document"`: it reserves "target row" for MERGE, and reports a
    // `USING` expression only when an explicit security-barrier qual rejected
    // the row. Closing that needs the SELECT-policy/UPDATE-policy split its
    // conflict check draws, which this engine's policy model does not have —
    // so what is pinned here is the engine's own wording, deliberately, rather
    // than a string mistaken for PostgreSQL's.
    assert!(
        message
            == "target row violates row-level security policy (USING expression) for table \
                \"document\""
    );
    assert!(query(&mut alice, "SELECT title FROM document WHERE id = 1").await == rows(&["a"]));

    // A conflicting row it may see updates normally.
    run(
        &mut bob,
        "INSERT INTO document VALUES (4, 'bob', 'x')
           ON CONFLICT (id) DO UPDATE SET title = 'taken'",
    )
    .await;
    assert!(query(&mut alice, "SELECT title FROM document WHERE id = 4").await == rows(&["taken"]));
}

/// `MERGE` reaches its rows through its own join, so both sides need their own
/// enforcement: the target side by the `USING` qual, the inserted side by
/// `WITH CHECK`.
#[tokio::test]
async fn merge_respects_both_using_and_with_check() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        // The MERGE source is bob's to read and to delete from; what is under
        // test is the target side's `USING` and `WITH CHECK`, so the source
        // carries the grants that keep a privilege denial out of the way.
        "CREATE TABLE source (id int4, title text);
         INSERT INTO source VALUES (1, 'from-source'), (4, 'from-source'), (9, 'new');
         GRANT SELECT, DELETE ON source TO bob;
         CREATE POLICY high ON document USING (id > 3) WITH CHECK (id < 9);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    // Row 1 is invisible, so it is not matched and the NOT MATCHED branch would
    // try to insert it — which the WITH CHECK admits (id < 9). Row 4 is visible
    // and updates. Row 9 fails WITH CHECK.
    let (sqlstate, _) = error_of(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id
           WHEN MATCHED THEN UPDATE SET title = s.title
           WHEN NOT MATCHED THEN INSERT (id, holder, title) VALUES (s.id, 'bob', s.title)",
    )
    .await;
    assert!(sqlstate == "42501");

    // Without the row that fails the check, the target side still refuses to
    // update the row bob cannot see.
    run(&mut bob, "DELETE FROM source WHERE id = 9").await;
    run(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id
           WHEN MATCHED THEN UPDATE SET title = s.title",
    )
    .await;
    assert!(
        query(&mut alice, "SELECT id, title FROM document ORDER BY id").await
            == rows(&["1,a", "2,b", "3,c", "4,from-source", "5,e"])
    );
}

// ----------------------------------------------------------------------- COPY

/// `COPY … FROM` into a relation under row security is refused before the
/// statement runs.
///
/// The timing is the point. `COPY FROM STDIN` answers with a `CopyInResponse`
/// and psql then reads every following line as data; a refusal that arrived
/// after that would leave the client and the session desynchronised for the rest
/// of the script.
#[tokio::test]
async fn copy_from_is_refused_for_a_relation_under_row_security() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY high ON document USING (id > 3);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY",
    )
    .await;

    let (sqlstate, message, hint) = error_and_hint_of(&mut alice, "COPY document FROM STDIN").await;
    assert!(sqlstate == "0A000");
    assert!(message == "COPY FROM not supported with row-level security");
    assert!(hint.as_deref() == Some("Use INSERT statements instead."));

    // With the GUC off it is the ordinary 42501 instead, because a policy would
    // have applied — and it still arrives before copy-in mode.
    run(&mut alice, "SET row_security = off").await;
    let (sqlstate, message) = error_of(&mut alice, "COPY document FROM STDIN").await;
    assert!(sqlstate == "42501");
    assert!(
        message == "query would be affected by row-level security policy for table \"document\""
    );

    // A relation the role bypasses gets as far as the copy-in machinery, which
    // over the simple-query path is where the ordinary refusal lives: the point
    // is that row security is no longer what stopped it.
    let mut bootstrap = engine.connect();
    run(&mut bootstrap, "CREATE TABLE plain (id int4)").await;
    let (sqlstate, message) = error_of(&mut bootstrap, "COPY plain FROM STDIN").await;
    assert!(sqlstate == "0A000");
    assert!(message == "COPY FROM STDIN requires pgwire CopyData messages");
}

/// **A leak test for the `COPY … TO` read path.**
///
/// A `COPY` of a relation is a read, and one that wrote out rows the policy
/// hides would be a total leak whatever the ordinary scan does. It sits apart
/// from the optimizer-bypass table because a copy-out is not a result set: the
/// rows come back as an encoded payload, so the assertion is on those bytes.
///
/// The relation form and the query form are both checked, because they are the
/// same path only as long as nobody adds a shortcut for the first.
#[tokio::test]
async fn a_policy_hides_a_row_from_copy_to() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY high ON document USING (id > 3);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         CREATE POLICY high ON measure USING (bucket > 10);
         ALTER TABLE measure ENABLE ROW LEVEL SECURITY",
    )
    .await;

    let mut bob = as_bob(&engine).await;
    for (sql, expected) in [
        ("COPY document (id) TO STDOUT", "4\n5\n"),
        (
            "COPY (SELECT id FROM document ORDER BY id) TO STDOUT",
            "4\n5\n",
        ),
        ("COPY measure (id) TO STDOUT", "2\n"),
        ("COPY (SELECT count(*) FROM document) TO STDOUT", "2\n"),
    ] {
        assert!(copied(&mut bob, sql).await == expected, "{sql}");
    }

    // Alice owns `document` and does not FORCE, so she bypasses the policy —
    // which is what proves the rows bob did not see are still there.
    assert!(copied(&mut alice, "COPY document (id) TO STDOUT").await == "1\n2\n3\n4\n5\n");
}

/// A `COPY … TO` a role may not read at all is refused at the privilege gate,
/// before any row is encoded.
#[tokio::test]
async fn copy_to_is_refused_without_the_select_privilege() {
    let (engine, mut alice) = owned_engine().await;
    run(&mut alice, "REVOKE ALL ON document FROM bob").await;

    let mut bob = as_bob(&engine).await;
    let error = bob
        .begin_copy_out("COPY document TO STDOUT")
        .await
        .expect_err("bob may not read document");
    assert!(error.code == "42501");
}

// ------------------------------------------------------------- the SQL surface

/// The policy DDL lifecycle, and the ownership rule that had to land before any
/// of it was reachable.
#[tokio::test]
async fn policy_ddl_lifecycle_and_ownership() {
    let (engine, mut alice) = owned_engine().await;
    let mut bob = as_bob(&engine).await;

    // A role that does not own the relation may not write its policies — the
    // reason table ownership was a prerequisite for this slice.
    let (sqlstate, message) =
        error_of(&mut bob, "CREATE POLICY sneak ON document USING (true)").await;
    assert!(sqlstate == "42501");
    assert!(message == "must be owner of table document");
    assert!(error_of(&mut bob, "DROP POLICY sneak ON document").await.0 == "42501");

    // Turning row security *off* is the one ALTER TABLE subcommand that can
    // grant rows, so it is owner-only too.
    for sql in [
        "ALTER TABLE document ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE document DISABLE ROW LEVEL SECURITY",
        "ALTER TABLE document NO FORCE ROW LEVEL SECURITY",
    ] {
        assert!(error_of(&mut bob, sql).await.0 == "42501", "{sql}");
    }

    run(&mut alice, "CREATE POLICY own ON document USING (id > 3)").await;
    assert!(
        error_of(&mut alice, "CREATE POLICY own ON document USING (true)")
            .await
            .0
            == "42710"
    );
    run(&mut alice, "ALTER POLICY own ON document RENAME TO high").await;
    run(
        &mut alice,
        "ALTER POLICY high ON document TO bob USING (id > 4)",
    )
    .await;
    assert!(
        query(
            &mut alice,
            "SELECT policyname, cmd, permissive, roles, qual, with_check
               FROM pg_policies WHERE tablename = 'document'"
        )
        .await
            == rows(&["high,ALL,PERMISSIVE,{bob},(id > 4),NULL"])
    );

    run(&mut alice, "ALTER TABLE document ENABLE ROW LEVEL SECURITY").await;
    assert!(query(&mut bob, "SELECT id FROM document").await == rows(&["5"]));

    // The policy applied to bob and to nobody else.
    let mut carol = engine.connect();
    run(&mut carol, "CREATE ROLE carol; SET ROLE carol").await;
    // Carol reads nothing because the policy names bob, not because she lacks
    // the privilege — so she is given the same grant bob has.
    run(&mut alice, "GRANT SELECT ON document TO carol").await;
    assert!(query(&mut carol, "SELECT count(*) FROM document").await == rows(&["0"]));

    run(&mut alice, "DROP POLICY high ON document").await;
    assert!(error_of(&mut alice, "DROP POLICY high ON document").await.0 == "42704");
    run(&mut alice, "DROP POLICY IF EXISTS high ON document").await;
    assert!(query(&mut bob, "SELECT count(*) FROM document").await == rows(&["0"]));
}

/// The catalog reports what the DDL stored: `pg_policy` in `PostgreSQL`'s own
/// shape, and the two relation flags.
#[tokio::test]
async fn the_catalog_projects_the_stored_policies_and_flags() {
    let (_engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY ins ON document AS RESTRICTIVE FOR INSERT TO alice
           WITH CHECK (id > 0);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY",
    )
    .await;

    assert!(
        query(
            &mut alice,
            "SELECT polname, polcmd, polpermissive, polqual, polwithcheck FROM pg_policy"
        )
        .await
            == rows(&["ins,a,f,NULL,(id > 0)"])
    );
    assert!(
        query(
            &mut alice,
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = 'document'"
        )
        .await
            == rows(&["t,t"])
    );
    assert!(
        query(
            &mut alice,
            "SELECT rowsecurity FROM pg_tables WHERE tablename = 'document'"
        )
        .await
            == rows(&["t"])
    );
    // A relation nobody enabled it on still reads false.
    assert!(
        query(
            &mut alice,
            "SELECT rowsecurity FROM pg_tables WHERE tablename = 'parent'"
        )
        .await
            == rows(&["f"])
    );
}

/// The catalog reports the *deparsed* qual, the way `PostgreSQL`'s rule printer
/// renders one, and never the source text the author typed.
///
/// Each case is written deliberately unnormalized — cramped spacing, a
/// qualifier `PostgreSQL` resolves away, an unparenthesized operator, an
/// unqualified column inside a sub-select — so a projection that echoed the
/// stored text back would answer the left column and not the right one.
#[tokio::test]
async fn the_catalog_deparses_a_policy_qual_rather_than_echoing_it() {
    let (_engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE TABLE clearance (who text, lvl int4);
         ALTER TABLE clearance OWNER TO alice",
    )
    .await;

    let cases = [
        ("cramped spacing", "id>4", "(id > 4)"),
        (
            "a qualifier the sole relation resolves away",
            "document . id > 4",
            "(id > 4)",
        ),
        (
            "the session role, spelled as ruleutils spells it",
            "holder = current_user",
            "(holder = CURRENT_USER)",
        ),
        (
            "nested operators, each one parenthesized",
            "id > 1 AND holder <> 'bob'",
            "((id > 1) AND (holder <> 'bob'::text))",
        ),
        (
            "a sub-select, laid out and qualified by the printer",
            "id <= (SELECT lvl FROM clearance WHERE who = current_user)",
            "(id <= ( SELECT clearance.lvl\n   FROM clearance\n  WHERE (clearance.who = CURRENT_USER)))",
        ),
    ];

    for (label, source, deparsed) in cases {
        run(
            &mut alice,
            &format!("CREATE POLICY shape ON document USING ({source}) WITH CHECK ({source})"),
        )
        .await;
        let expected = format!("{deparsed},{deparsed}");
        assert!(
            query(
                &mut alice,
                "SELECT qual, with_check FROM pg_policies WHERE tablename = 'document'"
            )
            .await
                == rows(&[expected.as_str()]),
            "{label}"
        );
        assert!(
            query(&mut alice, "SELECT polqual, polwithcheck FROM pg_policy").await
                == rows(&[expected.as_str()]),
            "{label}"
        );
        run(&mut alice, "DROP POLICY shape ON document").await;
    }
}

/// A policy applies to the command it names and to `ALL`, never to a command it
/// does not name — so a `FOR SELECT` policy cannot be what admits an `INSERT`.
#[tokio::test]
async fn a_policy_only_governs_the_commands_it_names() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE POLICY readable ON document FOR SELECT USING (true);
         CREATE POLICY writable ON document FOR INSERT WITH CHECK (holder = current_user);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    assert!(query(&mut bob, "SELECT count(*) FROM document").await == rows(&["5"]));
    run(&mut bob, "INSERT INTO document VALUES (6, 'bob', 'mine')").await;
    assert!(
        error_of(
            &mut bob,
            "INSERT INTO document VALUES (7, 'alice', 'stolen')"
        )
        .await
        .0 == "42501"
    );
    // No UPDATE or DELETE policy exists, so the default-deny fold leaves nothing
    // for either to act on — even though SELECT shows every row.
    assert!(
        query(&mut bob, "UPDATE document SET title = 'x' RETURNING id").await
            == Vec::<String>::new()
    );
    assert!(query(&mut bob, "DELETE FROM document RETURNING id").await == Vec::<String>::new());
}

/// Row security is refused on a sharded relation rather than stored and never
/// enforced: its writes go through the timestamp path, which cannot evaluate a
/// policy qual.
#[tokio::test]
async fn a_sharded_relation_cannot_be_put_under_row_security() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE shipment (id int4, region text) SHARDED BY HASH (id) BUCKETS 4",
    )
    .await;
    for sql in [
        "ALTER TABLE shipment ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE shipment FORCE ROW LEVEL SECURITY",
        "CREATE POLICY p ON shipment USING (true)",
    ] {
        let (sqlstate, message) = error_of(&mut session, sql).await;
        assert!(sqlstate == "0A000", "{sql}");
        assert!(
            message == "row-level security on sharded relation \"shipment\" is not supported",
            "{sql}"
        );
    }
}

/// **A view shows what its owner sees, through every read shape.**
///
/// `PostgreSQL` filters a view body by the *view owner's* policies, and this
/// engine now does too. The bound that keeps that from being a bypass is the
/// view's own ACL, which is checked against the caller before the body runs
/// (pinned in `owner_rights_views.rs`); what this test adds is that the
/// substitution is total — a scan, an aggregate folded inside the scanner, and
/// a `security_invoker` view over the same relation all agree about whose
/// policies applied.
#[tokio::test]
async fn a_view_shows_the_rows_its_owner_can_read() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW all_documents AS SELECT id, holder, title FROM document;
         CREATE VIEW own_documents WITH (security_invoker) AS
             SELECT id, holder, title FROM document;
         GRANT SELECT ON all_documents TO bob;
         GRANT SELECT ON own_documents TO bob;
         CREATE POLICY high ON document USING (id > 3);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;
    // Alice owns `document` and does not `FORCE`, so she bypasses the policy —
    // and so, through her view, does bob.
    assert!(
        query(&mut bob, "SELECT id FROM all_documents ORDER BY id").await
            == rows(&["1", "2", "3", "4", "5"])
    );
    // Aggregating through the view folds inside the scanner and must agree.
    assert!(query(&mut bob, "SELECT count(*) FROM all_documents").await == rows(&["5"]));
    assert!(query(&mut alice, "SELECT count(*) FROM all_documents").await == rows(&["5"]));
    // The `security_invoker` view over the same relation is filtered by bob's
    // own policies, so it shows him what a direct read would.
    assert!(query(&mut bob, "SELECT id FROM own_documents ORDER BY id").await == rows(&["4", "5"]));
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["4", "5"]));
}

/// A `MERGE` meets each command's policies through the action it takes, not
/// through one command chosen for the whole statement.
///
/// This is the hole the per-action check closes. `MERGE` used to gather its
/// rows under the `UPDATE` policies and then delete them with no check at all,
/// so a permissive `UPDATE` policy beside a narrow `DELETE` one let a protected
/// row be deleted. Checked against `postgres:18.4`.
#[tokio::test]
async fn merge_meets_the_policies_of_the_action_it_takes() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE TABLE source (id int4);
         INSERT INTO source VALUES (1), (2), (3), (4), (5);
         GRANT SELECT ON source TO bob;
         GRANT SELECT, INSERT, UPDATE, DELETE ON document TO bob;
         CREATE POLICY readable ON document FOR SELECT USING (true);
         CREATE POLICY updatable ON document FOR UPDATE USING (true) WITH CHECK (true);
         CREATE POLICY removable ON document FOR DELETE USING (id > 4);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    // The UPDATE policy admits every row, and the statement is an update, so it
    // runs.
    run(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id
           WHEN MATCHED THEN UPDATE SET title = 'touched'",
    )
    .await;
    assert!(
        query(
            &mut alice,
            "SELECT count(*) FROM document WHERE title = 'touched'"
        )
        .await
            == rows(&["5"])
    );

    // The same rows under a delete action meet the DELETE policy instead, which
    // admits only id 5. PostgreSQL raises on the first row that fails it rather
    // than skipping the row.
    let (sqlstate, message) = error_of(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id WHEN MATCHED THEN DELETE",
    )
    .await;
    assert!(sqlstate == "42501");
    assert!(
        message
            == "target row violates row-level security policy (USING expression) for table \"document\""
    );
    // And the statement left every row where it was.
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["5"]));

    // A delete aimed only at the row the DELETE policy admits goes through.
    run(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id AND s.id = 5
           WHEN MATCHED THEN DELETE",
    )
    .await;
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["4"]));
}

/// The same rule the other way round: a narrow `UPDATE` policy does not stop a
/// delete the `DELETE` policy admits.
#[tokio::test]
async fn a_narrow_update_policy_does_not_gate_a_merge_delete() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE TABLE source (id int4);
         INSERT INTO source VALUES (1), (2), (3), (4), (5);
         GRANT SELECT ON source TO bob;
         GRANT SELECT, UPDATE, DELETE ON document TO bob;
         CREATE POLICY readable ON document FOR SELECT USING (true);
         CREATE POLICY updatable ON document FOR UPDATE USING (false) WITH CHECK (true);
         CREATE POLICY removable ON document FOR DELETE USING (true);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    run(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id WHEN MATCHED THEN DELETE",
    )
    .await;
    assert!(query(&mut alice, "SELECT count(*) FROM document").await == rows(&["0"]));
}

/// A `MERGE` finds its rows under the `SELECT` policies, because the join it
/// drives is a read.
///
/// The consequence is visible rather than internal: a row a `SELECT` policy
/// hides is not matched, so a `WHEN NOT MATCHED` clause fires for it. A relation
/// whose only policy is an `UPDATE` policy therefore shows a `MERGE` nothing at
/// all, exactly as it shows a `SELECT` nothing at all.
#[tokio::test]
async fn a_merge_sees_the_rows_a_select_policy_shows_it() {
    let (engine, mut alice) = owned_engine().await;
    // The checking session is the bootstrap superuser rather than `alice`:
    // `FORCE ROW LEVEL SECURITY` subjects the owner to the policies too, and
    // for most of this test there is no `SELECT` policy for anyone to read
    // through, so `alice` would report zero either way.
    let mut bootstrap = engine.connect();
    run(
        &mut alice,
        "CREATE TABLE source (id int4);
         INSERT INTO source VALUES (1), (2);
         GRANT SELECT ON source TO bob;
         GRANT SELECT, UPDATE ON document TO bob;
         CREATE POLICY updatable ON document FOR UPDATE USING (true) WITH CHECK (true);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    // No SELECT policy exists, so default-deny hides every row from both.
    assert!(query(&mut bob, "SELECT count(*) FROM document").await == rows(&["0"]));
    run(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id
           WHEN MATCHED THEN UPDATE SET title = 'touched'",
    )
    .await;
    assert!(
        query(
            &mut bootstrap,
            "SELECT count(*) FROM document WHERE title = 'touched'"
        )
        .await
            == rows(&["0"])
    );

    // Add the SELECT policy and the same statement finds the rows it names.
    run(
        &mut alice,
        "CREATE POLICY readable ON document FOR SELECT USING (true)",
    )
    .await;
    run(
        &mut bob,
        "MERGE INTO document d USING source s ON d.id = s.id
           WHEN MATCHED THEN UPDATE SET title = 'touched'",
    )
    .await;
    assert!(
        query(
            &mut bootstrap,
            "SELECT count(*) FROM document WHERE title = 'touched'"
        )
        .await
            == rows(&["2"])
    );
}

// ------------------------------------------------ a subquery inside the qual

/// A relation whose policy qual reads a *second* relation with a subquery,
/// where that second relation is itself under row security.
///
/// `clearance` holds one row per role and is readable only by the role it names,
/// so the subquery's answer differs by who asks. That is what makes the setup
/// worth the length: if the policy's subquery were run with row security off —
/// as the owner, say, because the policy belongs to the owner — `max(level)`
/// would be 9 instead of `bob`'s 2, and every check below would admit rows it
/// must refuse. The escalation is invisible in a fixture where the inner
/// relation is unprotected.
const NESTED: &str = r"
CREATE ROLE alice;
CREATE ROLE bob;
CREATE TABLE clearance (holder text, level int4);
INSERT INTO clearance VALUES ('bob', 2), ('carol', 9);
ALTER TABLE clearance OWNER TO alice;
ALTER TABLE clearance ENABLE ROW LEVEL SECURITY;
CREATE TABLE dossier (id int4 PRIMARY KEY, level int4, note text);
INSERT INTO dossier VALUES (1, 1, 'low'), (2, 9, 'high');
ALTER TABLE dossier OWNER TO alice;
ALTER TABLE dossier ENABLE ROW LEVEL SECURITY;
GRANT SELECT ON clearance TO bob;
GRANT ALL ON dossier TO bob;
";

/// An engine with [`NESTED`] applied and the two policies in place: `clearance`
/// shows a role only its own row, and `dossier` admits only rows at or below the
/// level that subquery returns.
async fn nested_engine() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut alice = engine.connect();
    run(&mut alice, NESTED).await;
    run(&mut alice, "SET ROLE alice").await;
    run(
        &mut alice,
        "CREATE POLICY own_row ON clearance FOR SELECT USING (holder = current_user)",
    )
    .await;
    run(
        &mut alice,
        "CREATE POLICY cleared ON dossier FOR ALL
           USING (level <= (SELECT max(level) FROM clearance))
           WITH CHECK (level <= (SELECT max(level) FROM clearance))",
    )
    .await;
    (engine, alice)
}

/// **A policy qual that holds a subquery governs every write path, not just
/// reads.**
///
/// The read gate has resolved subqueries in a qual for as long as policies have
/// existed; the four write paths compiled the same qual and handed it to a
/// row-at-a-time evaluator that executes none, so each refused the statement
/// outright. They are listed separately because they compile the check at four
/// different points and none of them reaches the others.
#[tokio::test]
async fn a_policy_qual_with_a_subquery_governs_every_write_path() {
    let (engine, _alice) = nested_engine().await;
    let mut bob = as_bob(&engine).await;

    // The read side, first, as the reference answer: bob is cleared to 2, so
    // the level-9 row is not his to see.
    assert!(query(&mut bob, "SELECT id FROM dossier ORDER BY id").await == rows(&["1"]));

    // INSERT: at or below his clearance is written, above it is refused.
    run(&mut bob, "INSERT INTO dossier VALUES (3, 2, 'ok')").await;
    let (sqlstate, message) = error_of(&mut bob, "INSERT INTO dossier VALUES (4, 5, 'no')").await;
    assert!(sqlstate == "42501");
    assert!(message == "new row violates row-level security policy for table \"dossier\"");

    // UPDATE: the new row is judged by the same qual.
    run(&mut bob, "UPDATE dossier SET note = 'edited' WHERE id = 1").await;
    assert!(
        error_of(&mut bob, "UPDATE dossier SET level = 7 WHERE id = 1")
            .await
            .0
            == "42501"
    );

    // UPDATE and DELETE also filter their candidate rows by the qual, so the
    // row above his clearance is not merely unwritable but unreachable.
    run(&mut bob, "UPDATE dossier SET note = 'reached' WHERE id = 2").await;
    run(&mut bob, "DELETE FROM dossier WHERE id = 2").await;

    // MERGE and ON CONFLICT DO UPDATE compile the check at their own points.
    run(
        &mut bob,
        "MERGE INTO dossier d USING (SELECT 1 AS sid) s ON d.id = s.sid
           WHEN MATCHED THEN UPDATE SET note = 'merged'",
    )
    .await;
    run(
        &mut bob,
        "INSERT INTO dossier VALUES (3, 2, 'again')
           ON CONFLICT DO NOTHING",
    )
    .await;

    // Nothing above bob's clearance moved, and the hidden row is still there.
    let mut alice = engine.connect();
    run(&mut alice, "SET ROLE alice").await;
    assert!(
        query(&mut alice, "SELECT id,level,note FROM dossier ORDER BY id").await
            == rows(&["1,1,merged", "2,9,high", "3,2,ok"])
    );
}

/// **The subquery inside a policy qual is subject to row security itself.**
///
/// This is the test the rest of the file's caution is for. `carol`'s clearance
/// of 9 exists in the same relation the qual reads, and the only thing keeping
/// it out of `max(level)` is `clearance`'s own policy. Were the qual's subquery
/// run as the relation's owner — the natural mistake, since the policy is the
/// owner's — the write below would be admitted, and a role would have escalated
/// itself by naming a relation it cannot read.
#[tokio::test]
async fn a_policy_subquery_reads_under_the_invoking_roles_own_policies() {
    let (engine, _alice) = nested_engine().await;
    let mut bob = as_bob(&engine).await;

    // What bob may see of the inner relation, stated so the expectation below
    // cannot be read as a coincidence.
    assert!(query(&mut bob, "SELECT max(level) FROM clearance").await == rows(&["2"]));

    // Carol's 9 would admit this row; bob's 2 must not.
    let (sqlstate, _) = error_of(&mut bob, "INSERT INTO dossier VALUES (5, 9, 'escalated')").await;
    assert!(sqlstate == "42501");

    let mut alice = engine.connect();
    run(&mut alice, "SET ROLE alice").await;
    assert!(query(&mut alice, "SELECT count(*) FROM dossier WHERE id = 5").await == rows(&["0"]));
}

/// **A qual that reads its own relation is reported on every write path, not
/// only on reads.**
///
/// The recursion guard is the reason a policy subquery may run at all: the qual
/// is attacker-supplied SQL, so a qual that re-enters its own relation has to be
/// caught rather than left to exhaust the stack. Each write path enters the
/// guard for itself, so each is checked.
#[tokio::test]
async fn a_self_referencing_policy_subquery_is_reported_on_every_path() {
    let engine = SqlEngine::new();
    let mut alice = engine.connect();
    run(
        &mut alice,
        "CREATE ROLE alice;
         CREATE ROLE bob;
         CREATE TABLE loop_tbl (a int4);
         INSERT INTO loop_tbl VALUES (1), (2);
         ALTER TABLE loop_tbl OWNER TO alice;
         ALTER TABLE loop_tbl ENABLE ROW LEVEL SECURITY;
         GRANT ALL ON loop_tbl TO bob;",
    )
    .await;
    run(&mut alice, "SET ROLE alice").await;
    run(
        &mut alice,
        "CREATE POLICY eats_itself ON loop_tbl USING (a IN (SELECT a FROM loop_tbl))",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    for sql in [
        "SELECT * FROM loop_tbl",
        "INSERT INTO loop_tbl VALUES (3)",
        "UPDATE loop_tbl SET a = 4",
        "DELETE FROM loop_tbl",
    ] {
        let (sqlstate, message) = error_of(&mut bob, sql).await;
        assert!(sqlstate == "42P17", "{sql} should report recursion");
        assert!(message == "infinite recursion detected in policy for relation \"loop_tbl\"");
    }
}

/// **A policy qual whose subquery reads the row it judges is left to the row
/// evaluator, not refused at compile time.**
///
/// `(SELECT dossier.level <= level FROM clearance)` has no single value to fold
/// to — it depends on the row under test — so the fold cannot answer it. The
/// check is compiled before the first row is read, so a fold that propagated
/// its failure would refuse statements that never reach a row to judge, which
/// is a statement that used to succeed now failing. Leaving the qual alone
/// keeps the failure exactly where it was: at the row, if a row arrives.
#[tokio::test]
async fn a_correlated_policy_qual_fails_no_earlier_than_it_used_to() {
    let engine = SqlEngine::new();
    let mut alice = engine.connect();
    run(&mut alice, NESTED).await;
    run(&mut alice, "SET ROLE alice").await;
    run(
        &mut alice,
        "CREATE POLICY own_row ON clearance FOR SELECT USING (holder = current_user)",
    )
    .await;
    run(
        &mut alice,
        "CREATE POLICY correlated ON dossier FOR ALL USING (true)
           WITH CHECK ((SELECT dossier.level <= level FROM clearance))",
    )
    .await;
    let mut bob = as_bob(&engine).await;

    // No row matches, so no row is ever judged and the statement succeeds —
    // the behaviour a compile-time refusal would have taken away.
    run(&mut bob, "UPDATE dossier SET note = 'x' WHERE id = 999").await;

    // A statement that does reach a row still fails, and at the row.
    let (_, message) = error_of(&mut bob, "UPDATE dossier SET note = 'x' WHERE id = 1").await;
    assert!(message == "subqueries are only supported in SELECT");
}

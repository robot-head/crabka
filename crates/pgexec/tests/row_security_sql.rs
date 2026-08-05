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

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// A relation owned by `alice`, five rows, with the index and tree shapes the
/// bypass tests need.
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
    // TRUNCATE from row security and empties the whole relation, but it gates
    // it on the TRUNCATE privilege, which is not enforced here — exempting it
    // without that gate would let any role destroy a relation it cannot read.
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
    assert!(message == "new row violates row-level security policy \"own\" for table \"document\"");

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
    assert!(message == "new row violates row-level security policy \"own\" for table \"document\"");
}

/// A policy with `USING` and no `WITH CHECK` uses its `USING` as the check, and
/// says so.
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
    assert!(
        message
            == "new row violates row-level security policy \"own\" (USING expression) for table \
                \"document\""
    );
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
    assert!(message == "new row violates row-level security policy \"own\" for table \"document\"");
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
    assert!(
        message
            == "target row violates row-level security policy \"high\" (USING expression) for \
                table \"document\""
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
        "CREATE TABLE source (id int4, title text);
         INSERT INTO source VALUES (1, 'from-source'), (4, 'from-source'), (9, 'new');
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

    let (sqlstate, message) = error_of(&mut alice, "COPY document FROM STDIN").await;
    assert!(sqlstate == "0A000");
    assert!(
        message
            == "COPY FROM not supported with row-level security. Use INSERT statements instead."
    );

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

/// **Invoker semantics for views.**
///
/// `PostgreSQL` filters a view body by the *view owner's* policies. That is only
/// safe where `GRANT` is enforced, and it is not enforced here — every
/// `has_*_privilege` returns true — so an owner-rights view over a relation
/// under row security would be a universal bypass. Invoker semantics guarantee
/// the bound that matters: a view can never show a row the caller could not
/// have read from the base relation itself.
#[tokio::test]
async fn a_view_cannot_show_a_row_the_caller_could_not_read() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW all_documents AS SELECT id, holder, title FROM document;
         CREATE POLICY high ON document USING (id > 3);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY",
    )
    .await;
    let mut bob = as_bob(&engine).await;
    assert!(query(&mut bob, "SELECT id FROM all_documents ORDER BY id").await == rows(&["4", "5"]));
    // Aggregating through the view is the same read, and so is a join onto it.
    assert!(query(&mut bob, "SELECT count(*) FROM all_documents").await == rows(&["2"]));
    // The owner, who bypasses the policy on the base relation, still sees all
    // five — so the view itself is not what is filtering.
    assert!(query(&mut alice, "SELECT count(*) FROM all_documents").await == rows(&["5"]));
}

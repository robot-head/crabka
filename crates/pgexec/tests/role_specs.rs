//! The role-name keywords in a grantee, member, owner or authorization
//! position.
//!
//! `PostgreSQL`'s grammar spells all four positions `RoleSpec`, and admits
//! exactly three keywords there — `CURRENT_USER`, `CURRENT_ROLE`,
//! `SESSION_USER` — plus `PUBLIC` and an ordinary name. Before this landed the
//! engine carried a grantee as a written string, so `GRANT SELECT ON pg_proc TO
//! CURRENT_USER` — the second statement of the upstream `init_privs` test — was
//! `42704 object "current_user" does not exist`, and the owner position took
//! bare `USER` that `PostgreSQL` refuses outright.
//!
//! Two properties are worth pinning beyond "the keyword works". A keyword is
//! not a name: `"current_user"` in quotes is an ordinary role nobody holds, and
//! resolving on the folded string would silently grant that to the session.
//! And `SET ROLE` separates `CURRENT_USER` from `SESSION_USER`, so a grantee
//! resolved from the wrong one lands on the wrong role.

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

async fn scalar(session: &mut SqlSession, sql: &str) -> String {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => {
            let [row] = rows.as_slice() else {
                panic!("expected one row from {sql}, got {rows:?}");
            };
            let [cell] = row.as_slice() else {
                panic!("expected one column from {sql}, got {row:?}");
            };
            cell_text(cell.as_ref())
        }
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// The `SQLSTATE` and message of a statement that must fail.
async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

/// Whether the catalog says `role` holds `privilege` on `relation`.
async fn holds(session: &mut SqlSession, role: &str, relation: &str, privilege: &str) -> bool {
    scalar(
        session,
        &format!("SELECT has_table_privilege('{role}', '{relation}', '{privilege}')"),
    )
    .await
        == "t"
}

/// A session with two ordinary roles and a relation nobody has been granted
/// anything on.
async fn engine_with_roles() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE ROLE authenticated LOGIN;
         CREATE ROLE assumed;
         CREATE ROLE bystander;
         GRANT assumed TO authenticated;
         CREATE SCHEMA vault AUTHORIZATION assumed;
         CREATE TABLE doc (id int4);",
    )
    .await;
    (engine, session)
}

/// Every role position, as a format string with one `{}` for the role.
///
/// One list rather than a case per statement, because the point of the change
/// is that these positions cannot disagree about what a role name is.
const POSITIONS: &[&str] = &[
    "GRANT SELECT ON doc TO {}",
    "REVOKE SELECT ON doc FROM {}",
    "GRANT USAGE ON SCHEMA vault TO {}",
    "REVOKE USAGE ON SCHEMA vault FROM {}",
    "GRANT bystander TO {}",
    "REVOKE bystander FROM {}",
    "ALTER TABLE doc OWNER TO {}",
    "ALTER SCHEMA vault OWNER TO {}",
    "CREATE SCHEMA fresh AUTHORIZATION {}",
];

fn at(position: &str, role: &str) -> String {
    position.replace("{}", role)
}

// ------------------------------------------------ the keywords name a session

/// **The observation this change exists to invert.**
///
/// `SET ROLE` moves `CURRENT_USER` and leaves `SESSION_USER` behind, so the two
/// keywords name different roles and a grant lands on whichever one was
/// written. `assumed` is the discriminator: `authenticated` is a member of it
/// and so inherits its grants, but not the other way round.
#[tokio::test]
async fn a_grantee_keyword_names_the_role_the_session_holds_under_that_name() {
    struct Case {
        keyword: &'static str,
        relation: &'static str,
        /// Whether `assumed` — the role in force, not the one that logged in —
        /// ends up holding the grant.
        assumed_holds: bool,
    }
    let cases = [
        Case {
            keyword: "CURRENT_USER",
            relation: "cu",
            assumed_holds: true,
        },
        Case {
            keyword: "CURRENT_ROLE",
            relation: "cr",
            assumed_holds: true,
        },
        Case {
            keyword: "SESSION_USER",
            relation: "su",
            assumed_holds: false,
        },
    ];

    let (engine, mut owner) = engine_with_roles().await;
    for case in &cases {
        run(
            &mut owner,
            &format!("CREATE TABLE {} (id int4)", case.relation),
        )
        .await;
    }

    let mut session = engine.connect();
    run(&mut session, "SET SESSION AUTHORIZATION authenticated").await;
    run(&mut session, "SET ROLE assumed").await;
    for case in &cases {
        run(
            &mut session,
            &format!("GRANT SELECT ON {} TO {}", case.relation, case.keyword),
        )
        .await;
    }

    for case in &cases {
        assert!(
            holds(&mut owner, "assumed", case.relation, "SELECT").await == case.assumed_holds,
            "assumed after GRANT … TO {}",
            case.keyword
        );
        // Whichever of the two was named, the role that logged in can reach the
        // relation: directly when it was the grantee, through its membership
        // when the role in force was.
        assert!(
            holds(&mut owner, "authenticated", case.relation, "SELECT").await,
            "authenticated after GRANT … TO {}",
            case.keyword
        );
        assert!(
            !holds(&mut owner, "bystander", case.relation, "SELECT").await,
            "bystander after GRANT … TO {}",
            case.keyword
        );
    }
}

/// The keyword spellings reach every role position, not only the grantee.
#[tokio::test]
async fn every_role_position_takes_the_keyword_spellings() {
    for keyword in ["CURRENT_USER", "CURRENT_ROLE", "SESSION_USER"] {
        for position in POSITIONS {
            // The default session authenticated as nobody and so acts as the
            // bootstrap superuser, which is the role every keyword resolves to
            // here. A session that had assumed an ordinary role would be
            // refused the role-administration positions for want of the ADMIN
            // option, which is a different question from what the keyword names.
            let (_engine, mut session) = engine_with_roles().await;
            let sql = at(position, keyword);
            run(&mut session, &sql).await;
        }
    }
}

// ------------------------------------------------------ a keyword is not a name

/// A quoted identifier is a name, never a keyword.
///
/// `PostgreSQL` reads `"current_user"` as an ordinary role that nobody holds.
/// Resolving a grantee from the folded string would make this the session's own
/// role, which is a grant to somebody the statement never named.
#[tokio::test]
async fn a_quoted_role_keyword_is_an_ordinary_name() {
    for spelling in [
        "\"current_user\"",
        "\"CURRENT_USER\"",
        "\"session_user\"",
        "\"current_role\"",
        "\"PUBLIC\"",
    ] {
        for position in POSITIONS {
            let (_engine, mut session) = engine_with_roles().await;
            run(&mut session, "SET SESSION AUTHORIZATION authenticated").await;
            let sql = at(position, spelling);
            let (sqlstate, message) = error_of(&mut session, &sql).await;
            let bare = spelling.trim_matches('"');
            assert!(sqlstate == "42704", "{sql}");
            assert!(
                message == format!("role \"{bare}\" does not exist"),
                "{sql}"
            );
        }
    }
}

/// `public` in any case that folds to it is the pseudo-role, quoted or not —
/// which is `gram.y` comparing the written name, not the token.
#[tokio::test]
async fn the_written_name_public_is_the_pseudo_role() {
    for spelling in ["PUBLIC", "public", "Public", "\"public\""] {
        let (_engine, mut session) = engine_with_roles().await;
        run(&mut session, &at("GRANT SELECT ON doc TO {}", spelling)).await;
        assert!(
            holds(&mut session, "bystander", "doc", "SELECT").await,
            "GRANT … TO {spelling}"
        );
    }
}

/// Bare `USER` is not a role name anywhere.
///
/// It is a reserved keyword to `PostgreSQL`, so `GRANT … TO USER` and `OWNER TO
/// USER` are both syntax errors rather than a role nobody holds. The owner
/// position used to accept it and hand the relation to the session.
#[tokio::test]
async fn bare_user_is_a_syntax_error_in_every_role_position() {
    for position in POSITIONS {
        let (_engine, mut session) = engine_with_roles().await;
        let sql = at(position, "USER");
        let (sqlstate, _) = error_of(&mut session, &sql).await;
        assert!(sqlstate == "42601", "{sql}");
    }
    // The list a `GRANT`/`REVOKE ROLE` hands out is not a `RoleSpec` list
    // either: PostgreSQL reaches it through `privilege_list`, so a keyword
    // there is a syntax error too.
    let (_engine, mut session) = engine_with_roles().await;
    for sql in ["GRANT CURRENT_USER TO bystander", "GRANT USER TO bystander"] {
        let (sqlstate, _) = error_of(&mut session, sql).await;
        assert!(sqlstate == "42601", "{sql}");
    }
}

// --------------------------------------------------------------- PUBLIC's reach

/// `PUBLIC` is a grantee of privileges and a member of nothing.
///
/// It is the one role every session holds, so a privilege may be granted to it;
/// it has no record, so there is no membership to move either into or out of
/// it, and `PostgreSQL` says so in both directions.
#[tokio::test]
async fn public_is_a_grantee_of_privileges_and_a_member_of_nothing() {
    let (_engine, mut session) = engine_with_roles().await;
    for sql in [
        "GRANT SELECT ON doc TO PUBLIC",
        "REVOKE SELECT ON doc FROM PUBLIC",
        "GRANT USAGE ON SCHEMA vault TO PUBLIC",
        "REVOKE USAGE ON SCHEMA vault FROM PUBLIC",
    ] {
        run(&mut session, sql).await;
    }
    for sql in [
        "GRANT bystander TO PUBLIC",
        "REVOKE bystander FROM PUBLIC",
        "GRANT PUBLIC TO bystander",
        "REVOKE PUBLIC FROM bystander",
        "ALTER TABLE doc OWNER TO PUBLIC",
        "ALTER SCHEMA vault OWNER TO PUBLIC",
        "CREATE SCHEMA fresh AUTHORIZATION PUBLIC",
    ] {
        let (sqlstate, message) = error_of(&mut session, sql).await;
        assert!(sqlstate == "42704", "{sql}");
        assert!(message == "role \"public\" does not exist", "{sql}");
    }
}

// ------------------------------------------- the machinery an ordinary name uses

/// **The shared machinery, driven by an ordinary named role.**
///
/// Every `GRANT`/`REVOKE` in the engine goes through the same grantee
/// resolution, so a plain name has to keep reaching each of these — and reach
/// them with the effect the statement asked for, not merely without an error.
///
/// Each path is read back through whatever answers for it: the ACL for a table
/// privilege, `SET ROLE` for a membership, `pg_tables` for ownership. The
/// schema-privilege path has nothing to read back — the SQL
/// `has_schema_privilege` answers `true` unconditionally and no statement
/// consults the stored schema ACL — so it is covered by its refusals instead,
/// in [`an_unheld_name_is_refused_as_a_role_in_every_position`].
#[tokio::test]
async fn an_ordinary_named_role_still_reaches_every_grant_path() {
    let (engine, mut session) = engine_with_roles().await;

    run(&mut session, "GRANT SELECT ON doc TO bystander").await;
    assert!(holds(&mut session, "bystander", "doc", "SELECT").await);
    run(&mut session, "REVOKE SELECT ON doc FROM bystander").await;
    assert!(!holds(&mut session, "bystander", "doc", "SELECT").await);

    run(&mut session, "GRANT USAGE ON SCHEMA vault TO bystander").await;
    run(&mut session, "REVOKE USAGE ON SCHEMA vault FROM bystander").await;

    // A membership is what lets a session assume the role, so that is what
    // reads it back.
    run(&mut session, "CREATE ROLE joiner LOGIN").await;
    let mut member = engine.connect();
    run(&mut member, "SET SESSION AUTHORIZATION joiner").await;
    assert!(error_of(&mut member, "SET ROLE assumed").await.0 == "42501");
    run(&mut session, "GRANT assumed TO joiner").await;
    run(&mut member, "SET ROLE assumed").await;
    run(&mut member, "RESET ROLE").await;
    run(&mut session, "REVOKE assumed FROM joiner").await;
    assert!(error_of(&mut member, "SET ROLE assumed").await.0 == "42501");

    run(&mut session, "ALTER TABLE doc OWNER TO bystander").await;
    assert!(
        scalar(
            &mut session,
            "SELECT tableowner FROM pg_tables WHERE tablename = 'doc'"
        )
        .await
            == "bystander"
    );
}

/// A name no role holds is refused as a *role*, in every position.
///
/// The catalog seam calls an absent record an undefined object because it
/// answers for every kind it stores; `PostgreSQL` says `role` here, and the
/// regression corpus compares the sentence.
#[tokio::test]
async fn an_unheld_name_is_refused_as_a_role_in_every_position() {
    for position in POSITIONS {
        let (_engine, mut session) = engine_with_roles().await;
        let sql = at(position, "nobody");
        let (sqlstate, message) = error_of(&mut session, &sql).await;
        assert!(sqlstate == "42704", "{sql}");
        assert!(message == "role \"nobody\" does not exist", "{sql}");
    }
    let (_engine, mut session) = engine_with_roles().await;
    for sql in ["GRANT nobody TO bystander", "REVOKE nobody FROM bystander"] {
        let (sqlstate, message) = error_of(&mut session, sql).await;
        assert!(sqlstate == "42704", "{sql}");
        assert!(message == "role \"nobody\" does not exist", "{sql}");
    }
}

/// A statement that names a bad grantee second still writes nothing for the
/// first, and reports the bad one.
#[tokio::test]
async fn a_grantee_list_is_all_or_nothing() {
    let (_engine, mut session) = engine_with_roles().await;
    let (sqlstate, message) =
        error_of(&mut session, "GRANT SELECT ON doc TO bystander, nobody").await;
    assert!(sqlstate == "42704");
    assert!(message == "role \"nobody\" does not exist");
    assert!(!holds(&mut session, "bystander", "doc", "SELECT").await);
}

// ------------------------------------------------------------------- spellings

/// `GROUP` before a grantee is `PostgreSQL`'s pre-8.1 spelling of the same
/// thing, and belongs to the privilege-grantee list alone.
#[tokio::test]
async fn group_prefixes_a_privilege_grantee_and_nothing_else() {
    let (_engine, mut session) = engine_with_roles().await;
    run(
        &mut session,
        "GRANT SELECT ON doc TO GROUP bystander, GROUP assumed",
    )
    .await;
    assert!(holds(&mut session, "bystander", "doc", "SELECT").await);
    run(
        &mut session,
        "GRANT USAGE ON SCHEMA vault TO GROUP bystander",
    )
    .await;

    for sql in [
        "GRANT assumed TO GROUP bystander",
        "ALTER TABLE doc OWNER TO GROUP bystander",
        "CREATE SCHEMA fresh AUTHORIZATION GROUP bystander",
    ] {
        let (sqlstate, _) = error_of(&mut session, sql).await;
        assert!(sqlstate == "42601", "{sql}");
    }
}

/// `CREATE SCHEMA AUTHORIZATION <keyword>` names the schema after the role the
/// keyword resolves to, not after the word that was written.
#[tokio::test]
async fn create_schema_authorization_names_the_schema_after_the_resolved_role() {
    let (_engine, mut session) = engine_with_roles().await;
    run(&mut session, "SET SESSION AUTHORIZATION authenticated").await;
    run(&mut session, "CREATE SCHEMA AUTHORIZATION CURRENT_ROLE").await;
    assert!(
        scalar(
            &mut session,
            "SELECT nspname FROM pg_namespace WHERE nspname = 'authenticated'"
        )
        .await
            == "authenticated"
    );
}

/// The whole relation list is resolved before the grantee list.
///
/// `PostgreSQL` resolves the objects a `GRANT` names and only then its
/// grantees, so a statement whose second relation and whose role are both
/// missing reports the relation.
#[tokio::test]
async fn every_relation_is_resolved_before_any_grantee() {
    let (_engine, mut session) = engine_with_roles().await;
    for sql in [
        "GRANT SELECT ON doc, nosuchtable TO nobody",
        "GRANT SELECT ON nosuchtable, doc TO nobody",
        "REVOKE SELECT ON doc, nosuchtable FROM nobody",
    ] {
        let (sqlstate, message) = error_of(&mut session, sql).await;
        assert!(sqlstate == "42P01", "{sql}");
        assert!(
            message == "relation \"nosuchtable\" does not exist",
            "{sql}"
        );
    }
}

//! `search_path` entries the current role cannot search are not on the path.
//!
//! `PostgreSQL` rebuilds the effective path in `recomputeNamespacePath`
//! (`src/backend/catalog/namespace.c`), and an explicit entry survives only
//! when the schema exists *and* the current role holds `USAGE` on it. The two
//! implicit entries are added afterwards and are not tested: `pg_catalog`, and
//! the session's own temporary namespace.
//!
//! Every expectation here was measured against `postgres:18.4` side by side.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn query(session: &mut SqlSession, sql: &str) -> QueryResult {
    session
        .simple_query(sql)
        .await
        .expect("query succeeds")
        .into_iter()
        .next()
        .expect("one result")
}

fn rows(result: &QueryResult) -> &Vec<Vec<Option<Cell>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn row_text(result: &QueryResult, index: usize) -> Vec<Option<String>> {
    rows(result)[index]
        .iter()
        .map(|cell| {
            cell.as_ref()
                .map(|cell| String::from_utf8(cell.text.to_vec()).expect("valid text cell"))
        })
        .collect()
}

/// A session holding the brief's setup: `secret` and `public` each with a `pp`,
/// a relation only `secret` holds, and a role with no rights on `secret`.
async fn session_with_a_secret_schema(engine: &SqlEngine) -> SqlSession {
    let mut session = engine.connect();
    for setup in [
        "CREATE ROLE lowly LOGIN",
        "CREATE SCHEMA secret",
        "CREATE TABLE secret.pp (a int)",
        "CREATE TABLE secret.onlysecret (a int)",
        "CREATE TABLE public.pp (a int)",
        "GRANT SELECT ON secret.onlysecret TO lowly",
        // So that a creation reaching `public` is refused for no other reason.
        "GRANT CREATE ON SCHEMA public TO lowly",
    ] {
        session.simple_query(setup).await.expect(setup);
    }
    session
}

/// `current_schemas` and `current_schema` report the path the role can actually
/// search. `pg_catalog` is still there — it is implicit and never tested — and
/// `secret` is not, in either the true or the false form:
///
/// ```text
/// SET ROLE lowly; SET search_path = secret, public;
/// current_schemas(true)   {pg_catalog,public}
/// current_schemas(false)  {public}
/// current_schema          public
/// ```
#[tokio::test]
async fn current_schemas_reports_only_the_entries_the_role_can_search() {
    let engine = SqlEngine::new();
    let mut session = session_with_a_secret_schema(&engine).await;
    let report = "SELECT current_schemas(true)::text, current_schemas(false)::text, \
                  current_schema()";
    for (role, path, expected) in [
        (
            "SET ROLE lowly",
            "SET search_path = secret, public",
            vec![
                Some("{pg_catalog,public}".to_string()),
                Some("{public}".into()),
                Some("public".into()),
            ],
        ),
        // Every entry unsearchable leaves the implicit `pg_catalog` alone, and
        // `current_schema` is NULL rather than the schema that was written.
        (
            "SET ROLE lowly",
            "SET search_path = secret",
            vec![Some("{pg_catalog}".to_string()), Some("{}".into()), None],
        ),
        // The bootstrap role searches everything, which is the superuser bypass
        // `object_aclcheck` makes before it reads an ACL at all.
        (
            "RESET ROLE",
            "SET search_path = secret, public",
            vec![
                Some("{pg_catalog,secret,public}".to_string()),
                Some("{secret,public}".into()),
                Some("secret".into()),
            ],
        ),
    ] {
        session.simple_query(role).await.expect(role);
        session.simple_query(path).await.expect(path);
        assert!(
            row_text(&query(&mut session, report).await, 0) == expected,
            "{role}; {path}"
        );
    }
}

/// The filter changes name *resolution*, not only reporting. A relation only
/// the unsearchable schema holds does not resolve, and `postgres:18.4` reports
/// that as `42P01 relation "onlysecret" does not exist` — the report for a name
/// nothing answers to, not a permission report naming the schema, which would
/// itself disclose that the schema holds it.
#[tokio::test]
async fn an_unqualified_name_does_not_reach_a_schema_the_role_cannot_search() {
    let engine = SqlEngine::new();
    let mut session = session_with_a_secret_schema(&engine).await;
    for setup in ["SET ROLE lowly", "SET search_path = secret, public"] {
        session.simple_query(setup).await.expect(setup);
    }

    let error = session
        .simple_query("SELECT * FROM onlysecret")
        .await
        .expect_err("42P01");
    assert!(error.code == "42P01");
    assert!(error.message == "relation \"onlysecret\" does not exist");

    // A name both schemas hold lands on the one the role can reach, rather than
    // on the earlier entry it cannot. The comparison is against `public.pp` and
    // not against `secret.pp`, because naming the unsearchable schema outright
    // is its own refusal on `postgres:18.4`: `42501 permission denied for
    // schema secret`.
    let oids = "SELECT 'pp'::regclass::oid = 'public.pp'::regclass::oid";
    assert!(row_text(&query(&mut session, oids).await, 0) == vec![Some("t".to_string())]);
}

/// A `CREATE` with no qualifier lands in the first entry the role can search,
/// and a path that leaves none reports `3F000 no schema has been selected to
/// create in` — the same report an empty path gives, and not one that names the
/// schema the path did write.
#[tokio::test]
async fn a_creation_skips_an_entry_the_role_cannot_search() {
    let engine = SqlEngine::new();
    let mut session = session_with_a_secret_schema(&engine).await;
    for setup in ["SET ROLE lowly", "SET search_path = secret, public"] {
        session.simple_query(setup).await.expect(setup);
    }
    session
        .simple_query("CREATE TABLE landed (x int)")
        .await
        .expect("CREATE TABLE");
    assert!(
        row_text(
            &query(&mut session, "SELECT 'public.landed'::regclass::text").await,
            0
        ) == vec![Some("landed".to_string())]
    );

    session
        .simple_query("SET search_path = secret")
        .await
        .expect("SET");
    let error = session
        .simple_query("CREATE TABLE nowhere (x int)")
        .await
        .expect_err("3F000");
    assert!(error.code == "3F000");
    assert!(error.message == "no schema has been selected to create in");
}

/// `pg_table_is_visible` answers the same walk, so a relation in an
/// unsearchable schema is not visible and does not hide the relation of the
/// same name that follows it.
#[tokio::test]
async fn visibility_follows_the_same_filter() {
    let engine = SqlEngine::new();
    let mut session = session_with_a_secret_schema(&engine).await;
    for setup in ["SET ROLE lowly", "SET search_path = secret, public"] {
        session.simple_query(setup).await.expect(setup);
    }

    let result = query(
        &mut session,
        "SELECT n.nspname, c.relname, pg_table_is_visible(c.oid) FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname IN ('pp', 'onlysecret') ORDER BY 1, 2",
    )
    .await;
    let reported: Vec<Vec<Option<String>>> = (0..rows(&result).len())
        .map(|index| row_text(&result, index))
        .collect();
    assert!(
        reported
            == vec![
                vec![
                    Some("public".to_string()),
                    Some("pp".into()),
                    Some("t".into())
                ],
                vec![
                    Some("secret".to_string()),
                    Some("onlysecret".into()),
                    Some("f".into())
                ],
                vec![
                    Some("secret".to_string()),
                    Some("pp".into()),
                    Some("f".into())
                ],
            ]
    );
}

/// A grant puts the schema back on the path with no `SET search_path` in
/// between, because the path is recomputed per statement rather than cached —
/// which is what `PostgreSQL`'s catalog invalidation of `recomputeNamespacePath`
/// achieves.
#[tokio::test]
async fn a_grant_puts_the_schema_back_on_the_path() {
    let engine = SqlEngine::new();
    let mut session = session_with_a_secret_schema(&engine).await;
    for setup in ["SET ROLE lowly", "SET search_path = secret, public"] {
        session.simple_query(setup).await.expect(setup);
    }
    let report = "SELECT current_schemas(false)::text";
    assert!(row_text(&query(&mut session, report).await, 0) == vec![Some("{public}".to_string())]);

    session
        .simple_query("RESET ROLE")
        .await
        .expect("RESET ROLE");
    session
        .simple_query("GRANT USAGE ON SCHEMA secret TO lowly")
        .await
        .expect("GRANT");
    session.simple_query("SET ROLE lowly").await.expect("SET");

    assert!(
        row_text(&query(&mut session, report).await, 0)
            == vec![Some("{secret,public}".to_string())]
    );
}

/// The session's own temporary namespace is searched however the path writes
/// it. `PostgreSQL` skips the `USAGE` test for the `pg_temp` alias outright,
/// and `pg_namespace_aclmask` gives a session every right on its own namespace,
/// so the literal name passes too — even for a role that owns nothing.
#[tokio::test]
async fn a_role_still_searches_its_own_temporary_namespace() {
    let engine = SqlEngine::new();
    let mut session = session_with_a_secret_schema(&engine).await;
    for setup in [
        "SET ROLE lowly",
        "SET search_path = secret, public",
        "CREATE TEMP TABLE tt (x int)",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    // Implicit and first, ahead of the implicit `pg_catalog`, with `secret`
    // still filtered out from between them.
    let reported = row_text(
        &query(&mut session, "SELECT current_schemas(true)::text").await,
        0,
    );
    let temp = row_text(
        &query(
            &mut session,
            "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'pg\\_temp\\_%'",
        )
        .await,
        0,
    );
    let temp = temp[0].clone().expect("a temporary namespace");
    assert!(reported == vec![Some(format!("{{{temp},pg_catalog,public}}"))]);

    // Written by its alias it sits where it was written, and it is still not
    // filtered.
    session
        .simple_query("SET search_path = secret, public, pg_temp")
        .await
        .expect("SET");
    let reported = row_text(
        &query(&mut session, "SELECT current_schemas(false)::text").await,
        0,
    );
    assert!(reported == vec![Some(format!("{{public,{temp}}}"))]);

    assert!(
        row_text(&query(&mut session, "SELECT 'tt'::regclass::text").await, 0)
            == vec![Some("tt".to_string())]
    );
}

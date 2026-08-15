//! Which command tags an event trigger may name, and which tag it then sees.
//!
//! `CREATE EVENT TRIGGER … WHEN TAG IN (…)` used to be checked against a
//! hand-written list of 51 tags, compared exactly. That was wrong three ways at
//! once, and each way had a consequence:
//!
//! * The comparison was case-sensitive where `GetCommandTagEnum` uses
//!   `pg_strcasecmp`, so `when tag in ('create table')` — the spelling
//!   `PostgreSQL`'s own regression suite uses — was refused.
//! * Every refusal was one refusal. `PostgreSQL` has two, and the difference is
//!   the whole answer: `'sandwich'` is a syntax error (42601, no such tag),
//!   while `'CREATE ROLE'` is a real tag that event triggers may not have
//!   (0A000). A caller told "not recognized" about `CREATE ROLE` would go
//!   looking for a typo.
//! * The 141 tags missing from the list were all refused, including the ones
//!   `PostgreSQL` accepts, so a filter that should have narrowed a trigger
//!   instead stopped it being created.
//!
//! The table now comes from `cmdtaglist.h` unedited, and it carries
//! `event_trigger_ok` with each tag, so the same flag answers both "may this
//! filter name the tag" and "does this command fire event triggers at all".
//! [`a_command_closed_to_event_triggers_fires_none`] is the second question:
//! `ALTER ROLE` has the flag off, and a trigger that fired for it would be
//! watching role administration, which is exactly what the flag is for.
//!
//! [`only_a_superuser_may_create_an_event_trigger`] is a different rule that
//! this file has to hold down too, because the tag fix removes the accident
//! that used to hide it: an event trigger runs its function for every DDL
//! statement anyone issues, so an ordinary role that can create one can act on
//! statements it did not write.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, QueryResult, Session};
use tokio::sync::mpsc::Receiver;

/// A trigger function that reports the tag it was handed, the way
/// `PostgreSQL`'s own `event_trigger` test does.
const SETUP: &str = r"
CREATE FUNCTION report() RETURNS event_trigger AS $$
BEGIN
  RAISE NOTICE 'fired % %', tg_event, tg_tag;
END
$$ LANGUAGE plpgsql;
CREATE TABLE target (id int4);
";

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

/// The SQLSTATE, message and `HINT` a statement was refused with.
async fn refusal(session: &mut SqlSession, sql: &str) -> (String, String, Option<String>) {
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

/// Everything the session has been sent since the last drain.
fn drained(notices: &mut Receiver<crabka_pgwire::error::PgError>) -> Vec<String> {
    let mut seen = Vec::new();
    while let Ok(notice) = notices.try_recv() {
        seen.push(notice.message);
    }
    seen
}

/// A bootstrap session with [`SETUP`] applied, and its notice channel drained
/// of whatever the setup itself raised.
async fn engine() -> (
    SqlEngine,
    SqlSession,
    Receiver<crabka_pgwire::error::PgError>,
) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let mut notices = session.take_notices().expect("notice receiver");
    run(&mut session, SETUP).await;
    drained(&mut notices);
    (engine, session, notices)
}

// --------------------------------------------------------------- tag matching

/// Case is not part of a tag. Each spelling below has to create the trigger and
/// then actually fire it, because a filter that is accepted and never matches
/// is the same bug wearing a different hat.
#[tokio::test]
async fn a_tag_filter_is_matched_without_regard_to_case() {
    let spellings = [
        "'create table'",
        "'CREATE TABLE'",
        "'Create Table'",
        "'cReAtE tAbLe'",
        "'drop view', 'create table'",
    ];

    for (index, spelling) in spellings.iter().enumerate() {
        let (_engine, mut session, mut notices) = engine().await;
        run(
            &mut session,
            &format!(
                "CREATE EVENT TRIGGER spelling ON ddl_command_start
                 WHEN TAG IN ({spelling}) EXECUTE PROCEDURE report()"
            ),
        )
        .await;
        drained(&mut notices);

        run(
            &mut session,
            &format!("CREATE TABLE fired_{index} (id int4)"),
        )
        .await;

        assert!(
            drained(&mut notices) == vec!["fired ddl_command_start CREATE TABLE".to_string()],
            "{spelling}"
        );
    }
}

/// A filter that names a tag the statement does not carry keeps the trigger
/// quiet. Without this, [`a_tag_filter_is_matched_without_regard_to_case`]
/// would also pass for a filter that was ignored entirely.
#[tokio::test]
async fn a_tag_filter_that_does_not_match_keeps_the_trigger_quiet() {
    let (_engine, mut session, mut notices) = engine().await;
    run(
        &mut session,
        "CREATE EVENT TRIGGER only_views ON ddl_command_start
         WHEN TAG IN ('create view') EXECUTE PROCEDURE report()",
    )
    .await;
    drained(&mut notices);

    run(&mut session, "CREATE TABLE unwatched (id int4)").await;

    assert!(drained(&mut notices) == Vec::<String>::new());
}

// -------------------------------------------------------------- the refusals

/// The two refusals, told apart by SQLSTATE.
///
/// 42601 means there is no such command tag; 0A000 means there is, and event
/// triggers may not have it. Both quote the caller's spelling rather than the
/// canonical tag, which is what `validate_ddl_tags` does with its `%s` and what
/// makes the message useful to somebody who wrote the tag in lower case.
#[tokio::test]
async fn a_refused_tag_says_which_of_the_two_things_is_wrong() {
    let cases = [
        (
            "'sandwich'",
            "42601",
            "filter value \"sandwich\" not recognized for filter variable \"tag\"",
        ),
        (
            "'create table', 'create skunkcabbage'",
            "42601",
            "filter value \"create skunkcabbage\" not recognized for filter variable \"tag\"",
        ),
        (
            "''",
            "42601",
            "filter value \"\" not recognized for filter variable \"tag\"",
        ),
        (
            "'DROP EVENT TRIGGER'",
            "0A000",
            "event triggers are not supported for DROP EVENT TRIGGER",
        ),
        (
            "'CREATE ROLE'",
            "0A000",
            "event triggers are not supported for CREATE ROLE",
        ),
        (
            "'CREATE DATABASE'",
            "0A000",
            "event triggers are not supported for CREATE DATABASE",
        ),
        (
            "'CREATE TABLESPACE'",
            "0A000",
            "event triggers are not supported for CREATE TABLESPACE",
        ),
        // The user's spelling, not the canonical tag.
        (
            "'create role'",
            "0A000",
            "event triggers are not supported for create role",
        ),
    ];

    let (_engine, mut session, _notices) = engine().await;
    for (values, sqlstate, message) in cases {
        let sql = format!(
            "CREATE EVENT TRIGGER refused ON ddl_command_start
             WHEN TAG IN ({values}) EXECUTE PROCEDURE report()"
        );
        assert!(
            refusal(&mut session, &sql).await == (sqlstate.to_string(), message.to_string(), None),
            "{values}"
        );
    }
}

/// The `WHEN` clause's own errors, which come before any tag is looked at: an
/// unknown filter variable, the same variable twice, and tag filtering on a
/// `login` trigger, which has no command to tag.
#[tokio::test]
async fn the_when_clause_is_checked_before_the_tags_in_it() {
    let cases = [
        (
            "ddl_command_start",
            "WHEN food IN ('sandwich')",
            "42601",
            "unrecognized filter variable \"food\"",
        ),
        (
            "ddl_command_start",
            "WHEN tag IN ('create table') AND tag IN ('CREATE FUNCTION')",
            "42601",
            "filter variable \"tag\" specified more than once",
        ),
        (
            "login",
            "WHEN tag IN ('create table')",
            "0A000",
            "tag filtering is not supported for login event triggers",
        ),
        // `table_rewrite` takes the other flag, and an invented tag comes back
        // as "not supported" there rather than as the 42601 a DDL event raises.
        (
            "table_rewrite",
            "WHEN tag IN ('create table')",
            "0A000",
            "event triggers are not supported for create table",
        ),
        (
            "table_rewrite",
            "WHEN tag IN ('sandwich')",
            "0A000",
            "event triggers are not supported for sandwich",
        ),
    ];

    let (_engine, mut session, _notices) = engine().await;
    for (event, when, sqlstate, message) in cases {
        let sql =
            format!("CREATE EVENT TRIGGER refused ON {event} {when} EXECUTE PROCEDURE report()");
        assert!(
            refusal(&mut session, &sql).await == (sqlstate.to_string(), message.to_string(), None),
            "{when} on {event}"
        );
    }

    // The tag `table_rewrite` does take, so the refusals above are about the
    // flag and not about the event.
    run(
        &mut session,
        "CREATE EVENT TRIGGER rewrites ON table_rewrite
         WHEN TAG IN ('alter table') EXECUTE PROCEDURE report()",
    )
    .await;
}

// ------------------------------------------------------------- the tag it sees

/// The tag a fired trigger reports is the command's real tag.
///
/// `CREATE POLICY` and `DROP POLICY` are the cases that had no tag of their own
/// and arrived as `UNKNOWN` — a tag no `PostgreSQL` client has ever seen, and
/// one no `WHEN TAG IN` filter could match, so a filtered trigger silently
/// never fired for a command gres runs perfectly well.
#[tokio::test]
async fn a_fired_trigger_reports_the_commands_own_tag() {
    let cases = [
        ("CREATE POLICY p ON target USING (true)", "CREATE POLICY"),
        ("ALTER POLICY p ON target USING (false)", "ALTER POLICY"),
        ("DROP POLICY p ON target", "DROP POLICY"),
        ("CREATE INDEX target_id ON target (id)", "CREATE INDEX"),
        ("DROP INDEX target_id", "DROP INDEX"),
        ("COMMENT ON TABLE target IS 'x'", "COMMENT"),
    ];

    let (_engine, mut session, mut notices) = engine().await;
    run(
        &mut session,
        "CREATE EVENT TRIGGER watch ON ddl_command_start EXECUTE PROCEDURE report()",
    )
    .await;
    drained(&mut notices);

    for (sql, tag) in cases {
        run(&mut session, sql).await;
        assert!(
            drained(&mut notices) == vec![format!("fired ddl_command_start {tag}")],
            "{sql}"
        );
    }
}

/// A trigger can also be narrowed to one of those tags, which is the pair of
/// [`a_fired_trigger_reports_the_commands_own_tag`]: the tag the trigger
/// reports and the tag a filter matches have to be the same string.
#[tokio::test]
async fn a_filter_can_name_a_tag_that_used_to_arrive_as_unknown() {
    let (_engine, mut session, mut notices) = engine().await;
    run(
        &mut session,
        "CREATE EVENT TRIGGER policies ON ddl_command_start
         WHEN TAG IN ('create policy') EXECUTE PROCEDURE report()",
    )
    .await;
    drained(&mut notices);

    run(&mut session, "CREATE TABLE ignored (id int4)").await;
    run(&mut session, "CREATE POLICY p ON target USING (true)").await;

    assert!(drained(&mut notices) == vec!["fired ddl_command_start CREATE POLICY".to_string()]);
}

/// A command whose tag has `event_trigger_ok` off fires nothing at all, filter
/// or no filter. `ALTER ROLE` is the one that matters: it is how a role becomes
/// a superuser, and a trigger that saw it would be reading role administration
/// from inside an ordinary DDL session.
#[tokio::test]
async fn a_command_closed_to_event_triggers_fires_none() {
    let (_engine, mut session, mut notices) = engine().await;
    run(&mut session, "CREATE ROLE watched").await;
    run(
        &mut session,
        "CREATE EVENT TRIGGER watch ON ddl_command_start EXECUTE PROCEDURE report()",
    )
    .await;
    drained(&mut notices);

    run(&mut session, "ALTER ROLE watched CREATEDB").await;
    run(&mut session, "CREATE ROLE also_watched").await;
    run(&mut session, "GRANT watched TO also_watched").await;
    run(&mut session, "DROP ROLE also_watched").await;

    assert!(drained(&mut notices) == Vec::<String>::new());

    // And the trigger is alive, so the silence above is the flag and not a
    // trigger that stopped working.
    run(&mut session, "CREATE TABLE proof (id int4)").await;
    assert!(drained(&mut notices) == vec!["fired ddl_command_start CREATE TABLE".to_string()]);
}

// ---------------------------------------------------------- who may create one

/// Only a superuser may create an event trigger, and only a superuser may be
/// given one.
///
/// The reach is the point: `mallory` creates a trigger, and from then on every
/// DDL statement any session runs — including the bootstrap superuser's — calls
/// `mallory`'s function. So the create is refused, and the proof is that the
/// bootstrap session's later DDL raises nothing.
#[tokio::test]
async fn only_a_superuser_may_create_an_event_trigger() {
    let (engine, mut bootstrap, mut notices) = engine().await;
    run(&mut bootstrap, "CREATE ROLE mallory").await;
    let mut mallory = engine.connect();
    run(&mut mallory, "SET SESSION AUTHORIZATION mallory").await;

    assert!(
        refusal(
            &mut mallory,
            "CREATE EVENT TRIGGER watch_everything ON ddl_command_start
             EXECUTE PROCEDURE report()"
        )
        .await
            == (
                "42501".to_string(),
                "permission denied to create event trigger \"watch_everything\"".to_string(),
                Some("Must be superuser to create an event trigger.".to_string()),
            )
    );

    // The reach the statement was for.
    drained(&mut notices);
    run(&mut bootstrap, "CREATE TABLE unwatched (id int4)").await;
    assert!(drained(&mut notices) == Vec::<String>::new());

    // Nor by the back door: the superuser creates it and hands it over.
    run(
        &mut bootstrap,
        "CREATE EVENT TRIGGER watch ON ddl_command_start EXECUTE PROCEDURE report()",
    )
    .await;
    assert!(
        refusal(&mut bootstrap, "ALTER EVENT TRIGGER watch OWNER TO mallory").await
            == (
                "42501".to_string(),
                "permission denied to change owner of event trigger \"watch\"".to_string(),
                Some("The owner of an event trigger must be a superuser.".to_string()),
            )
    );

    // A superuser may be given one.
    run(&mut bootstrap, "ALTER ROLE mallory SUPERUSER").await;
    run(&mut bootstrap, "ALTER EVENT TRIGGER watch OWNER TO mallory").await;
}

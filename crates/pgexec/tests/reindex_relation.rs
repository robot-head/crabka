//! `REINDEX` resolves the relation it names.
//!
//! There are no indexes to rebuild here, so the rebuild is an accepted hint.
//! Which *names* `PostgreSQL` accepts is not: a relation that is not there, or
//! is there and is the wrong kind, is a refusal, and answering `REINDEX` to
//! every spelling is how a suite quietly stops testing what it wrote.
//!
//! The order the checks run in is the part that had to be measured rather than
//! derived, because two of them sit on opposite sides of the tablespace lookup
//! in `PostgreSQL`'s `ExecReindex`: `CONCURRENTLY`'s transaction-block guard
//! runs before it and `SCHEMA`'s runs after, so the same block with the same
//! bad tablespace reports different things depending on the spelling.
//!
//! Every expectation here was measured against `PostgreSQL` 18.4.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Engine, Session},
    error::PgError,
};

/// The whole reportable shape of a statement's outcome, so a case compares one
/// value rather than three fields.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Ok,
    Failed { code: String, message: String },
}

impl From<PgError> for Outcome {
    fn from(error: PgError) -> Self {
        Self::Failed {
            code: error.code,
            message: error.message,
        }
    }
}

fn failed(code: &str, message: &str) -> Outcome {
    Outcome::Failed {
        code: code.to_string(),
        message: message.to_string(),
    }
}

/// `42P01`, which names the relation the way the statement wrote it.
fn missing_relation(written: &str) -> Outcome {
    failed("42P01", &format!("relation \"{written}\" does not exist"))
}

/// `3F000`, which a qualified relation name reports in place of the `42P01`
/// when the qualifier is what is missing.
fn missing_schema(written: &str) -> Outcome {
    failed("3F000", &format!("schema \"{written}\" does not exist"))
}

/// `REINDEX TABLE`'s wrong-kind refusal, worded after the kinds it accepts.
fn not_a_table(relation: &str) -> Outcome {
    failed(
        "42809",
        &format!("\"{relation}\" is not a table or materialized view"),
    )
}

/// `REINDEX INDEX`'s wrong-kind refusal, worded after the kind it was asked
/// for.
fn not_an_index(relation: &str) -> Outcome {
    failed("42809", &format!("\"{relation}\" is not an index"))
}

fn not_the_open_database() -> Outcome {
    failed("0A000", "can only reindex the currently open database")
}

fn no_concurrent_catalogs() -> Outcome {
    failed("0A000", "cannot reindex system catalogs concurrently")
}

/// `PreventInTransactionBlock`, which names the statement it refused.
fn in_transaction_block(statement: &str) -> Outcome {
    failed(
        "25001",
        &format!("{statement} cannot run inside a transaction block"),
    )
}

async fn outcome(session: &mut SqlSession, sql: &str) -> Outcome {
    match session.simple_query(sql).await {
        Ok(_) => Outcome::Ok,
        Err(error) => error.into(),
    }
}

/// One relation of every kind that shares the relation namespace, in a schema
/// that is not `public` — which is what proves a refusal spells the relation
/// the way the statement wrote it rather than the way it resolved.
async fn fixture() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for ddl in [
        "CREATE SCHEMA sh",
        "SET search_path = sh, public",
        "CREATE TABLE st (i int)",
        "CREATE INDEX st_idx ON st (i)",
        "CREATE VIEW sv AS SELECT * FROM st",
        "CREATE MATERIALIZED VIEW smv AS SELECT * FROM st",
        "CREATE SEQUENCE ss",
        "CREATE FOREIGN DATA WRAPPER sfdw",
        "CREATE SERVER ssrv FOREIGN DATA WRAPPER sfdw",
        "CREATE FOREIGN TABLE sft (i int) SERVER ssrv",
        "CREATE TABLE sp (i int) PARTITION BY RANGE (i)",
        "CREATE TABLE sp1 PARTITION OF sp FOR VALUES FROM (0) TO (10)",
        "CREATE INDEX sp_idx ON sp (i)",
        "SET allow_in_place_tablespaces = on",
        // An empty LOCATION keeps the fixture off the host filesystem, so no
        // case pins a path that only exists on one operating system.
        "CREATE TABLESPACE sts LOCATION ''",
    ] {
        session
            .simple_query(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e:?}"));
    }
    (engine, session)
}

async fn check(cases: &[(&str, Outcome)]) {
    let (_engine, mut session) = fixture().await;
    for (sql, expected) in cases {
        let actual = outcome(&mut session, sql).await;
        assert!(actual == *expected, "{sql}");
    }
}

/// `REINDEX TABLE` takes the two kinds that carry a heap of their own, and a
/// partitioned table because it stands for the ones that do.
#[tokio::test]
async fn reindex_table_accepts_a_heap_bearing_relation_and_refuses_every_other_kind() {
    check(&[
        ("REINDEX TABLE st", Outcome::Ok),
        ("REINDEX TABLE smv", Outcome::Ok),
        ("REINDEX TABLE sp", Outcome::Ok),
        ("REINDEX TABLE sp1", Outcome::Ok),
        ("REINDEX TABLE sh.st", Outcome::Ok),
        ("REINDEX TABLE sv", not_a_table("sv")),
        ("REINDEX TABLE ss", not_a_table("ss")),
        ("REINDEX TABLE st_idx", not_a_table("st_idx")),
        ("REINDEX TABLE sft", not_a_table("sft")),
        // A partitioned index is an index, and this is the pair
        // `create_index.sql` writes back to back.
        ("REINDEX TABLE sp_idx", not_a_table("sp_idx")),
    ])
    .await;
}

/// `REINDEX INDEX` takes exactly one kind, and says so about every other.
#[tokio::test]
async fn reindex_index_accepts_an_index_and_refuses_every_other_kind() {
    check(&[
        ("REINDEX INDEX st_idx", Outcome::Ok),
        ("REINDEX INDEX sp_idx", Outcome::Ok),
        ("REINDEX INDEX sh.st_idx", Outcome::Ok),
        ("REINDEX INDEX st", not_an_index("st")),
        ("REINDEX INDEX sv", not_an_index("sv")),
        ("REINDEX INDEX smv", not_an_index("smv")),
        ("REINDEX INDEX ss", not_an_index("ss")),
        ("REINDEX INDEX sft", not_an_index("sft")),
        ("REINDEX INDEX sp", not_an_index("sp")),
    ])
    .await;
}

/// A name that resolves to nothing is `42P01`, and the report spells it the way
/// the statement wrote it. A qualifier that resolves to nothing is `3F000`
/// instead, and beats the relation it qualified: the schema is looked for
/// first, so `nosuchschema.nosuchrel` never reaches the relation lookup.
#[tokio::test]
async fn a_missing_schema_is_reported_ahead_of_the_relation_it_qualified() {
    check(&[
        ("REINDEX TABLE nosuchrel", missing_relation("nosuchrel")),
        ("REINDEX INDEX nosuchidx", missing_relation("nosuchidx")),
        ("REINDEX TABLE sh.nosuch", missing_relation("sh.nosuch")),
        ("REINDEX INDEX sh.nosuch", missing_relation("sh.nosuch")),
        (
            "REINDEX TABLE nosuchschema.t",
            missing_schema("nosuchschema"),
        ),
        (
            "REINDEX INDEX nosuchschema.i",
            missing_schema("nosuchschema"),
        ),
        (
            "REINDEX TABLE nosuchschema.nosuchrel",
            missing_schema("nosuchschema"),
        ),
        (
            "REINDEX TABLE CONCURRENTLY nosuchrel",
            missing_relation("nosuchrel"),
        ),
    ])
    .await;
}

/// A synthesised catalog relation is a relation of its kind here too, so
/// `REINDEX` sorts them the same way every other statement does — except that
/// a synthesised *table* is accepted rather than refused. `REINDEX` rebuilds
/// rather than redefines, so `allowSystemTableMods` never runs and the 42501
/// every neighbouring statement reports would be wrong.
#[tokio::test]
async fn a_synthesized_catalog_table_is_reindexable_and_a_synthesized_view_is_not() {
    check(&[
        ("REINDEX TABLE pg_class", Outcome::Ok),
        ("REINDEX TABLE pg_catalog.pg_class", Outcome::Ok),
        ("REINDEX TABLE pg_depend", Outcome::Ok),
        ("REINDEX TABLE pg_settings", not_a_table("pg_settings")),
        ("REINDEX TABLE pg_roles", not_a_table("pg_roles")),
        (
            "REINDEX TABLE information_schema.tables",
            not_a_table("tables"),
        ),
        ("REINDEX INDEX pg_class", not_an_index("pg_class")),
        ("REINDEX INDEX pg_settings", not_an_index("pg_settings")),
    ])
    .await;
}

/// The kind test runs before the catalog test, so a synthesised *view* named
/// with `CONCURRENTLY` reports the kind rather than the catalog — which is what
/// makes the order observable at all.
#[tokio::test]
async fn a_concurrent_rebuild_of_a_catalog_is_refused_after_the_kind_is_checked() {
    check(&[
        ("REINDEX TABLE CONCURRENTLY st", Outcome::Ok),
        ("REINDEX TABLE CONCURRENTLY smv", Outcome::Ok),
        ("REINDEX INDEX CONCURRENTLY st_idx", Outcome::Ok),
        (
            "REINDEX TABLE CONCURRENTLY pg_class",
            no_concurrent_catalogs(),
        ),
        (
            "REINDEX (CONCURRENTLY) TABLE pg_class",
            no_concurrent_catalogs(),
        ),
        (
            "REINDEX (CONCURRENTLY true) TABLE pg_class",
            no_concurrent_catalogs(),
        ),
        // The option can turn the rebuild back off, and then there is no
        // refusal to report.
        ("REINDEX (CONCURRENTLY false) TABLE pg_class", Outcome::Ok),
        (
            "REINDEX TABLE CONCURRENTLY pg_settings",
            not_a_table("pg_settings"),
        ),
        (
            "REINDEX INDEX CONCURRENTLY pg_class",
            not_an_index("pg_class"),
        ),
        ("REINDEX TABLE CONCURRENTLY sv", not_a_table("sv")),
    ])
    .await;
}

/// `SCHEMA` names a namespace and nothing else: a relation of that name is not
/// one, and the refusal says so about the schema rather than about the
/// relation.
#[tokio::test]
async fn reindex_schema_resolves_a_namespace_and_not_a_relation() {
    check(&[
        ("REINDEX SCHEMA sh", Outcome::Ok),
        ("REINDEX SCHEMA public", Outcome::Ok),
        ("REINDEX SCHEMA pg_catalog", Outcome::Ok),
        ("REINDEX SCHEMA information_schema", Outcome::Ok),
        ("REINDEX SCHEMA CONCURRENTLY sh", Outcome::Ok),
        (
            "REINDEX SCHEMA nosuchschema",
            missing_schema("nosuchschema"),
        ),
        ("REINDEX SCHEMA st", missing_schema("st")),
        ("REINDEX SCHEMA CONCURRENTLY st", missing_schema("st")),
    ])
    .await;
}

/// `DATABASE` and `SYSTEM` reindex the open database or nothing, so the name is
/// compared rather than looked up — a database that exists but is not this one
/// is refused with the same words as one that does not exist. `SYSTEM` reaches
/// only catalogs, so it refuses a concurrent rebuild without waiting to be told
/// which relation.
#[tokio::test]
async fn reindex_database_and_system_accept_only_the_open_database() {
    check(&[
        ("REINDEX DATABASE", Outcome::Ok),
        ("REINDEX DATABASE postgres", Outcome::Ok),
        ("REINDEX SYSTEM", Outcome::Ok),
        ("REINDEX SYSTEM postgres", Outcome::Ok),
        ("REINDEX DATABASE CONCURRENTLY postgres", Outcome::Ok),
        ("REINDEX DATABASE nosuchdb", not_the_open_database()),
        ("REINDEX SYSTEM nosuchdb", not_the_open_database()),
        (
            "REINDEX DATABASE CONCURRENTLY nosuchdb",
            not_the_open_database(),
        ),
        (
            "REINDEX SYSTEM CONCURRENTLY postgres",
            no_concurrent_catalogs(),
        ),
        // The concurrent refusal beats the name comparison.
        (
            "REINDEX SYSTEM CONCURRENTLY nosuchdb",
            no_concurrent_catalogs(),
        ),
        ("REINDEX (CONCURRENTLY) SYSTEM", no_concurrent_catalogs()),
    ])
    .await;
}

/// The option list is read before anything else happens, so its refusals beat
/// even the transaction-block guard — and a tablespace has to exist even though
/// nothing will be moved into it.
#[tokio::test]
async fn the_option_list_is_read_before_any_name_is_resolved() {
    check(&[
        ("REINDEX (VERBOSE) TABLE st", Outcome::Ok),
        ("REINDEX (VERBOSE true) TABLE st", Outcome::Ok),
        ("REINDEX (TABLESPACE sts) TABLE st", Outcome::Ok),
        ("REINDEX (TABLESPACE pg_default) INDEX st_idx", Outcome::Ok),
        ("REINDEX (VERBOSE, TABLESPACE sts) TABLE st", Outcome::Ok),
        (
            "REINDEX (nosuchopt) TABLE st",
            failed("42601", "unrecognized REINDEX option \"nosuchopt\""),
        ),
        (
            "REINDEX (nosuchopt) TABLE nosuchrel",
            failed("42601", "unrecognized REINDEX option \"nosuchopt\""),
        ),
        (
            "REINDEX (VERBOSE notabool) TABLE st",
            failed("42601", "verbose requires a Boolean value"),
        ),
        (
            "REINDEX (CONCURRENTLY notabool) TABLE st",
            failed("42601", "concurrently requires a Boolean value"),
        ),
        (
            "REINDEX (TABLESPACE) TABLE st",
            failed("42601", "tablespace requires a parameter"),
        ),
        (
            "REINDEX (TABLESPACE nosuchts) TABLE st",
            failed("42704", "tablespace \"nosuchts\" does not exist"),
        ),
        // The tablespace is looked up before the relation, the schema and the
        // database name, so all three of these report it.
        (
            "REINDEX (TABLESPACE nosuchts) TABLE nosuchrel",
            failed("42704", "tablespace \"nosuchts\" does not exist"),
        ),
        (
            "REINDEX (TABLESPACE nosuchts) SCHEMA nosuchschema",
            failed("42704", "tablespace \"nosuchts\" does not exist"),
        ),
        (
            "REINDEX (TABLESPACE nosuchts) DATABASE nosuchdb",
            failed("42704", "tablespace \"nosuchts\" does not exist"),
        ),
    ])
    .await;
}

/// Four spellings cannot run inside a transaction block, and the guard runs
/// before the name is resolved — but `CONCURRENTLY`'s guard runs *before* the
/// tablespace lookup and the other three run *after* it, which is the one
/// ordering that cannot be guessed from the messages.
#[tokio::test]
async fn the_transaction_block_guard_straddles_the_tablespace_lookup() {
    check(&[
        ("BEGIN", Outcome::Ok),
        ("REINDEX TABLE st", Outcome::Ok),
        ("REINDEX INDEX st_idx", Outcome::Ok),
        (
            "REINDEX TABLE CONCURRENTLY st",
            in_transaction_block("REINDEX CONCURRENTLY"),
        ),
        ("ROLLBACK", Outcome::Ok),
    ])
    .await;
    // Each refusal aborts the block it was written in, so the rest of the
    // orderings need one block apiece.
    let ordering = [
        // The guard beats the name, so a relation that is not there is not what
        // gets reported.
        (
            "REINDEX TABLE CONCURRENTLY nosuchrel",
            in_transaction_block("REINDEX CONCURRENTLY"),
        ),
        (
            "REINDEX SCHEMA nosuchschema",
            in_transaction_block("REINDEX SCHEMA"),
        ),
        (
            "REINDEX DATABASE nosuchdb",
            in_transaction_block("REINDEX DATABASE"),
        ),
        (
            "REINDEX SYSTEM nosuchdb",
            in_transaction_block("REINDEX SYSTEM"),
        ),
        // `CONCURRENTLY`'s guard is ahead of the tablespace lookup …
        (
            "REINDEX (TABLESPACE nosuchts) TABLE CONCURRENTLY nosuchrel",
            in_transaction_block("REINDEX CONCURRENTLY"),
        ),
        // … and `SCHEMA`'s is behind it.
        (
            "REINDEX (TABLESPACE nosuchts) SCHEMA sh",
            failed("42704", "tablespace \"nosuchts\" does not exist"),
        ),
        // An unrecognized option is ahead of both.
        (
            "REINDEX (nosuchopt) TABLE CONCURRENTLY st",
            failed("42601", "unrecognized REINDEX option \"nosuchopt\""),
        ),
    ];
    for (sql, expected) in ordering {
        check(&[
            ("BEGIN", Outcome::Ok),
            (sql, expected),
            ("ROLLBACK", Outcome::Ok),
        ])
        .await;
    }
}

/// `CLUSTER` reads a schema-qualified target through the same relation-name
/// production `REINDEX` now uses. `public` is lexed as a keyword here, so a
/// bare-name test that asked only for an identifier read `CLUSTER public.t` as
/// the target-less spelling and then choked on the name it had left behind.
#[tokio::test]
async fn cluster_reads_a_public_qualified_target_as_a_target() {
    let (_engine, mut session) = fixture().await;
    session
        .simple_query("CREATE TABLE public.ct (i int)")
        .await
        .expect("create");
    // `PostgreSQL` reports the missing clustered index, which is a refusal that
    // can only be reached by a statement whose target was read at all.
    assert!(
        outcome(&mut session, "CLUSTER public.ct").await
            == failed(
                "42704",
                "there is no previously clustered index for table \"ct\"",
            )
    );
    assert!(
        outcome(&mut session, "CLUSTER public.nosuchrel").await == missing_relation("nosuchrel")
    );
}

/// `CLUSTER`'s wrong-kind refusal has to be phrased as the kinds it accepts,
/// not as the kinds it refuses.
///
/// A foreign table is stored under the table catalog key here, so a refusal
/// listed as view-or-sequence-or-index let one through, and `CLUSTER` then
/// reported the clustered-index lookup's `42704` for a relation `PostgreSQL`
/// never lets that far. Nothing in this test names a `REINDEX`.
#[tokio::test]
async fn cluster_refuses_a_foreign_table_as_a_kind_rather_than_as_a_missing_index() {
    check(&[
        ("CLUSTER sft", not_a_table("sft")),
        ("CLUSTER sv", not_a_table("sv")),
        ("CLUSTER ss", not_a_table("ss")),
        ("CLUSTER st_idx", not_a_table("st_idx")),
        (
            "CLUSTER st",
            failed(
                "42704",
                "there is no previously clustered index for table \"st\"",
            ),
        ),
    ])
    .await;
}

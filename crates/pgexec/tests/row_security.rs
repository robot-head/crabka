//! Row-level security, reached through the catalog rather than through SQL.
//!
//! These were written while no statement could enable row security, so they
//! reach the catalog directly through the seam the enforcement path reads:
//! `crabka_pgcatalog::policy` for the policies and
//! `crabka_pgcatalog::set_row_security_ops` for the relation's flag. The engine
//! is built over a store the test also holds, so a policy written this way is
//! exactly the policy the executor sees.
//!
//! They are kept as written: the first two pin that a *stored* policy on a
//! relation whose flag is clear changes nothing, which is the property every
//! relation in every other test in the repository depends on. The SQL surface
//! is covered by `row_security_sql.rs`.

use std::sync::Arc;

use assert2::assert;
use crabka_pgcatalog::policy::{Policy, PolicyCommand};
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// An engine and the store behind it, so a test can write catalog records the
/// SQL surface has no syntax for.
struct Fixture {
    engine: SqlEngine,
    kv: Arc<dyn Kv>,
}

fn fixture() -> Fixture {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("in-memory engine");
    Fixture { engine, kv }
}

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

/// Every row of the first result, each row rendered as a comma-joined string so
/// a whole expectation is one literal.
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

/// The relations, rows and indexes every read shape in the dormancy proof needs.
const FIXTURE_SQL: &str = r"
CREATE TABLE document (id int4 NOT NULL, title text);
INSERT INTO document VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
CREATE INDEX document_id_idx ON document (id);
CREATE TABLE shipment (id int4 NOT NULL, region text) SHARDED BY HASH (id) BUCKETS 4;
INSERT INTO shipment VALUES (1, 'north'), (2, 'south'), (3, 'east'), (4, 'west');
CREATE TABLE parent (id int4, note text);
CREATE TABLE child () INHERITS (parent);
INSERT INTO parent VALUES (10, 'p');
INSERT INTO child VALUES (11, 'c');
CREATE TABLE measure (id int4, bucket int4) PARTITION BY RANGE (bucket);
CREATE TABLE measure_low PARTITION OF measure FOR VALUES FROM (0) TO (10);
CREATE TABLE measure_high PARTITION OF measure FOR VALUES FROM (10) TO (20);
INSERT INTO measure VALUES (1, 5), (2, 15);
";

/// One read of every shape the executor has a separate path for. Each of these
/// reaches a different one of the five stored-relation scan exits or one of the
/// optimizer pushdowns that now takes an `UnrestrictedTable`.
const READ_SHAPES: [&str; 11] = [
    // the ordinary MVCC scan
    "SELECT id, title FROM document ORDER BY id",
    // the local index equality probe
    "SELECT id, title FROM document WHERE id = 3",
    // the local streaming aggregate
    "SELECT count(*) FROM document",
    "SELECT sum(id) FROM document",
    // the local join count
    "SELECT count(*) FROM document a JOIN document b ON a.id = b.id",
    // the sharded partial-aggregate pushdown
    "SELECT count(*) FROM shipment",
    // the sharded top-K pushdown
    "SELECT id FROM shipment ORDER BY id LIMIT 2",
    // the sharded scan-plan predicate pushdown
    "SELECT id, region FROM shipment WHERE id = 2",
    // the inheritance scan
    "SELECT id, note FROM parent ORDER BY id",
    // the partition scan
    "SELECT id, bucket FROM measure ORDER BY id",
    // a whole-tree aggregate over the partitioned parent
    "SELECT count(*) FROM measure",
];

fn deny_everything(table_id: crabka_pgcatalog::TableId, name: &str) -> Policy {
    Policy {
        oid: 0,
        name: name.into(),
        table_id,
        command: PolicyCommand::All,
        permissive: true,
        roles: Vec::new(),
        using: Some("false".into()),
        with_check: None,
    }
}

fn store_policy(kv: &dyn Kv, policy: &Policy) {
    let ops = crabka_pgcatalog::policy::create_policy_ops(kv, policy).expect("create policy");
    kv.write_batch(&ops).expect("apply policy");
}

fn table_id(kv: &dyn Kv, name: &str) -> crabka_pgcatalog::TableId {
    crabka_pgcatalog::get_table(kv, &crabka_pgcatalog::RelationName::public(name))
        .expect("relation exists")
        .id
}

fn enable_row_security(kv: &dyn Kv, name: &str, force: bool) {
    let ops = crabka_pgcatalog::set_row_security_ops(
        kv,
        &crabka_pgcatalog::RelationName::public(name),
        true,
        force,
    )
    .expect("set row security");
    kv.write_batch(&ops).expect("apply row security");
}

/// **The dormancy proof.**
///
/// Two engines run the same statements over the same data. The second one has a
/// deny-everything policy stored against every relation, and its relations'
/// `row_security` flags left clear — which is the only state SQL can reach
/// today, because nothing sets that flag. Every read shape the executor has a
/// separate path for must answer identically on both.
///
/// If any of the eleven shapes diverged, this slice would have changed the
/// meaning of an existing query. None may.
#[tokio::test]
async fn stored_policies_do_not_change_any_read_while_row_security_is_off() {
    let plain = fixture();
    let mut plain_session = plain.engine.connect();
    run(&mut plain_session, FIXTURE_SQL).await;

    let policed = fixture();
    let mut policed_session = policed.engine.connect();
    run(&mut policed_session, FIXTURE_SQL).await;
    for relation in [
        "document",
        "shipment",
        "parent",
        "child",
        "measure",
        "measure_low",
        "measure_high",
    ] {
        let id = table_id(policed.kv.as_ref(), relation);
        store_policy(policed.kv.as_ref(), &deny_everything(id, "deny_all"));
    }

    for sql in READ_SHAPES {
        let expected = query(&mut plain_session, sql).await;
        let actual = query(&mut policed_session, sql).await;
        assert!(actual == expected, "{sql}");
    }

    // And the rows are really there — a proof that compared two empty results
    // would prove nothing.
    assert!(
        query(&mut plain_session, "SELECT count(*) FROM document").await == vec!["5".to_string()]
    );
}

/// The same, for the write path: an `UPDATE`/`DELETE` still touches every row
/// it used to while the flag is clear.
#[tokio::test]
async fn stored_policies_do_not_change_a_write_while_row_security_is_off() {
    let fixture = fixture();
    let mut session = fixture.engine.connect();
    run(&mut session, FIXTURE_SQL).await;
    let id = table_id(fixture.kv.as_ref(), "document");
    store_policy(fixture.kv.as_ref(), &deny_everything(id, "deny_all"));

    run(&mut session, "UPDATE document SET title = 'x' WHERE id > 3").await;
    assert!(
        query(
            &mut session,
            "SELECT count(*) FROM document WHERE title = 'x'"
        )
        .await
            == vec!["2".to_string()]
    );
    run(&mut session, "DELETE FROM document WHERE id = 1").await;
    assert!(query(&mut session, "SELECT count(*) FROM document").await == vec!["4".to_string()]);
}

/// A session acting as a role that neither owns the relation nor is exempt.
async fn as_stranger(engine: &SqlEngine) -> SqlSession {
    let mut session = engine.connect();
    run(&mut session, "CREATE ROLE stranger; SET ROLE stranger").await;
    session
}

/// With the flag set, the fold decides what the role sees — and the aggregate
/// and top-K pushdowns must not answer from rows the qual removes.
#[tokio::test]
async fn an_enabled_relation_filters_every_read_shape() {
    let fixture = fixture();
    let mut owner = fixture.engine.connect();
    run(&mut owner, FIXTURE_SQL).await;
    for relation in ["document", "shipment"] {
        let id = table_id(fixture.kv.as_ref(), relation);
        store_policy(
            fixture.kv.as_ref(),
            &Policy {
                using: Some("id > 3".into()),
                ..deny_everything(id, "high_ids_only")
            },
        );
        enable_row_security(fixture.kv.as_ref(), relation, false);
    }

    let mut stranger = as_stranger(&fixture.engine).await;
    // The materializing scan.
    assert!(
        query(&mut stranger, "SELECT id FROM document ORDER BY id").await
            == vec!["4".to_string(), "5".to_string()]
    );
    // The streaming aggregate would have folded all five rows inside the
    // scanner, where the qual has not run.
    assert!(query(&mut stranger, "SELECT count(*) FROM document").await == vec!["2".to_string()]);
    // The local index probe would have answered from the index alone.
    assert!(
        query(&mut stranger, "SELECT id FROM document WHERE id = 1").await == Vec::<String>::new()
    );
    // The partial-aggregate pushdown counts inside the range owner.
    assert!(query(&mut stranger, "SELECT count(*) FROM shipment").await == vec!["1".to_string()]);
    // The top-K pushdown would have taken the two lowest ids and then filtered
    // them away, answering empty instead of the one visible row.
    assert!(
        query(&mut stranger, "SELECT id FROM shipment ORDER BY id LIMIT 2").await
            == vec!["4".to_string()]
    );
    // The join count would have counted joined rows the qual removes.
    assert!(
        query(
            &mut stranger,
            "SELECT count(*) FROM document a JOIN document b ON a.id = b.id"
        )
        .await
            == vec!["2".to_string()]
    );
    // The owner still reads everything: no FORCE.
    assert!(query(&mut owner, "SELECT count(*) FROM document").await == vec!["5".to_string()]);
}

/// The parent's policies govern the whole inheritance tree, and the child's
/// govern none of it — `PostgreSQL`'s rule, and the reason a `RawScan` carries
/// the relation that was named rather than the one a row came from.
#[tokio::test]
async fn a_tree_is_governed_by_the_relation_that_was_named() {
    let fixture = fixture();
    let mut owner = fixture.engine.connect();
    run(&mut owner, FIXTURE_SQL).await;
    let parent = table_id(fixture.kv.as_ref(), "parent");
    let child = table_id(fixture.kv.as_ref(), "child");
    store_policy(
        fixture.kv.as_ref(),
        &Policy {
            using: Some("id < 11".into()),
            ..deny_everything(parent, "parent_policy")
        },
    );
    store_policy(fixture.kv.as_ref(), &deny_everything(child, "child_denies"));
    enable_row_security(fixture.kv.as_ref(), "parent", false);
    enable_row_security(fixture.kv.as_ref(), "child", false);

    let mut stranger = as_stranger(&fixture.engine).await;
    // The parent's qual hides the child's row; the child's own deny-everything
    // policy took no part.
    assert!(
        query(&mut stranger, "SELECT id FROM parent ORDER BY id").await == vec!["10".to_string()]
    );
    // Naming the child directly is a different read, governed by the child.
    assert!(query(&mut stranger, "SELECT id FROM child").await == Vec::<String>::new());
}

/// `row_security = off` fails the statement instead of filtering it, and only
/// where a policy would in fact have applied.
#[tokio::test]
async fn row_security_off_refuses_a_query_a_policy_would_have_affected() {
    let fixture = fixture();
    let mut session = fixture.engine.connect();
    run(&mut session, FIXTURE_SQL).await;
    let id = table_id(fixture.kv.as_ref(), "document");
    store_policy(
        fixture.kv.as_ref(),
        &Policy {
            using: Some("id > 3".into()),
            ..deny_everything(id, "high_ids_only")
        },
    );
    enable_row_security(fixture.kv.as_ref(), "document", false);

    let mut stranger = as_stranger(&fixture.engine).await;
    run(&mut stranger, "SET row_security = off").await;
    let (sqlstate, message) = error_of(&mut stranger, "SELECT id FROM document").await;
    assert!(sqlstate == "42501");
    assert!(
        message == "query would be affected by row-level security policy for table \"document\""
    );

    // A relation with no policy applicable to this role has nothing to refuse.
    assert!(
        query(&mut stranger, "SELECT id FROM parent ORDER BY id").await
            == vec!["10".to_string(), "11".to_string()]
    );
    // And the setting never turns into a bypass for the role it applies to.
    run(&mut stranger, "SET row_security = on").await;
    assert!(query(&mut stranger, "SELECT count(*) FROM document").await == vec!["2".to_string()]);
}

/// A policy qual that reads the relation its own policy protects is reported,
/// not followed. The qual is user-supplied SQL, so following it would be a
/// remotely triggerable stack overflow.
#[tokio::test]
async fn a_self_referencing_policy_raises_infinite_recursion() {
    let fixture = fixture();
    let mut owner = fixture.engine.connect();
    run(&mut owner, FIXTURE_SQL).await;
    let id = table_id(fixture.kv.as_ref(), "document");
    store_policy(
        fixture.kv.as_ref(),
        &Policy {
            using: Some("id IN (SELECT id FROM document)".into()),
            ..deny_everything(id, "reads_itself")
        },
    );
    enable_row_security(fixture.kv.as_ref(), "document", false);

    let mut stranger = as_stranger(&fixture.engine).await;
    let (sqlstate, message) = error_of(&mut stranger, "SELECT id FROM document").await;
    assert!(sqlstate == "42P17");
    assert!(message == "infinite recursion detected in policy for relation \"document\"");

    // The guard unwinds: a later read of a relation whose policy does not read
    // itself still works in the same session.
    assert!(
        query(&mut stranger, "SELECT id FROM parent ORDER BY id").await
            == vec!["10".to_string(), "11".to_string()]
    );
}

/// `FORCE ROW LEVEL SECURITY` subjects the owner to its own policies.
#[tokio::test]
async fn force_row_level_security_binds_the_owner() {
    let fixture = fixture();
    let mut bootstrap = fixture.engine.connect();
    run(&mut bootstrap, FIXTURE_SQL).await;
    // The bootstrap role is a superuser and would bypass regardless, so the
    // relation is handed to an ordinary role first.
    run(
        &mut bootstrap,
        "CREATE ROLE keeper; ALTER TABLE document OWNER TO keeper",
    )
    .await;
    let id = table_id(fixture.kv.as_ref(), "document");
    store_policy(
        fixture.kv.as_ref(),
        &Policy {
            using: Some("id > 3".into()),
            ..deny_everything(id, "high_ids_only")
        },
    );
    enable_row_security(fixture.kv.as_ref(), "document", false);

    let mut owner = fixture.engine.connect();
    run(&mut owner, "SET ROLE keeper").await;
    assert!(query(&mut owner, "SELECT count(*) FROM document").await == vec!["5".to_string()]);

    enable_row_security(fixture.kv.as_ref(), "document", true);
    assert!(query(&mut owner, "SELECT count(*) FROM document").await == vec!["2".to_string()]);
}

/// A policy qual built around a privilege probe is refused rather than applied:
/// `has_table_privilege` and its family return true unconditionally today, so
/// the policy would admit every row instead of the subset it appears to name.
#[tokio::test]
async fn a_policy_qual_that_probes_privileges_is_refused() {
    let fixture = fixture();
    let mut owner = fixture.engine.connect();
    run(&mut owner, FIXTURE_SQL).await;
    let id = table_id(fixture.kv.as_ref(), "document");
    store_policy(
        fixture.kv.as_ref(),
        &Policy {
            using: Some("has_table_privilege('document', 'SELECT')".into()),
            ..deny_everything(id, "trusts_privileges")
        },
    );
    enable_row_security(fixture.kv.as_ref(), "document", false);

    let mut stranger = as_stranger(&fixture.engine).await;
    let (sqlstate, message) = error_of(&mut stranger, "SELECT id FROM document").await;
    assert!(sqlstate == "0A000");
    assert!(message.contains("has_table_privilege"));
}

/// The SQL surface reaches the flag now, and reaches it through the statements
/// `PostgreSQL` spells it with — the whole point of this slice.
#[tokio::test]
async fn the_four_row_security_subcommands_move_the_stored_flags() {
    struct Case {
        sql: &'static str,
        row_security: bool,
        force: bool,
    }
    let cases = [
        Case {
            sql: "ALTER TABLE document ENABLE ROW LEVEL SECURITY",
            row_security: true,
            force: false,
        },
        Case {
            sql: "ALTER TABLE document FORCE ROW LEVEL SECURITY",
            row_security: true,
            force: true,
        },
        Case {
            sql: "ALTER TABLE document NO FORCE ROW LEVEL SECURITY",
            row_security: true,
            force: false,
        },
        Case {
            sql: "ALTER TABLE document DISABLE ROW LEVEL SECURITY",
            row_security: false,
            force: false,
        },
    ];
    let fixture = fixture();
    let mut session = fixture.engine.connect();
    run(&mut session, "CREATE TABLE document (id int4)").await;
    for case in cases {
        run(&mut session, case.sql).await;
        let table = crabka_pgcatalog::get_table(
            fixture.kv.as_ref(),
            &crabka_pgcatalog::RelationName::public("document"),
        )
        .expect("relation exists");
        assert!(table.row_security == case.row_security, "{}", case.sql);
        assert!(table.force_row_security == case.force, "{}", case.sql);
    }
}

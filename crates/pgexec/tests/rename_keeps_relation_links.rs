//! `ALTER TABLE … RENAME TO` is invisible to inheritance and to partitioning,
//! as it is in `PostgreSQL`.
//!
//! Crabka keys those links by relation *name*, so a rename used to walk away
//! from them. Renaming a partitioned parent was the worst of it: the parent
//! stopped being partitioned at all, its leaf's rows stopped being reachable
//! through it, and the empty heap it became started taking rows that belong in
//! no partition. Every expectation below was measured on `PostgreSQL` 18.4.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8"))
}

async fn query(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match session.simple_query(sql).await {
        Ok(results) => match results.into_iter().next() {
            Some(QueryResult::Rows { rows, .. }) => rows
                .iter()
                .map(|row| row.iter().map(|cell| cell_text(cell.as_ref())).collect())
                .collect(),
            other => panic!("expected rows from {sql}, got {other:?}"),
        },
        Err(error) => panic!("{sql} failed: {error:?}"),
    }
}

/// The single scalar `sql` selects.
async fn scalar(session: &mut SqlSession, sql: &str) -> String {
    let rows = query(session, sql).await;
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [Some(value)] => value.clone(),
            other => panic!("expected one column from {sql}, got {other:?}"),
        },
        other => panic!("expected one row from {sql}, got {other:?}"),
    }
}

async fn err_message(session: &mut SqlSession, sql: &str) -> String {
    session
        .simple_query(sql)
        .await
        .expect_err("expected an error")
        .message
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in setup {
        session
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("setup {sql} failed: {error:?}"));
    }
    (engine, session)
}

/// The link both directions, as psql's `\d` reads it.
const LINKS: &str = "SELECT ch.relname, pa.relname FROM pg_inherits i \
     JOIN pg_class ch ON ch.oid = i.inhrelid \
     JOIN pg_class pa ON pa.oid = i.inhparent ORDER BY 1";

fn link(child: &str, parent: &str) -> Vec<Vec<Option<String>>> {
    vec![vec![Some(child.to_string()), Some(parent.to_string())]]
}

#[tokio::test]
async fn a_renamed_partitioned_parent_is_still_partitioned() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE prt (i int4) PARTITION BY RANGE (i)",
        "CREATE TABLE prt_1 PARTITION OF prt FOR VALUES FROM (0) TO (10)",
        "INSERT INTO prt VALUES (1)",
        "ALTER TABLE prt RENAME TO prt_renamed",
    ])
    .await;

    // The scheme itself. `relkind` fell to `r` and `pg_partitioned_table`
    // emptied, which is the whole of partitioning gone in two catalog reads.
    assert!(
        scalar(
            &mut session,
            "SELECT relkind::text FROM pg_class WHERE relname = 'prt_renamed'",
        )
        .await
            == "p"
    );
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM pg_partitioned_table"
        )
        .await
            == "1"
    );
    assert!(query(&mut session, LINKS).await == link("prt_1", "prt_renamed"));

    // The row is still reached through the parent, and is still stored once.
    assert!(scalar(&mut session, "SELECT count(*)::text FROM prt_renamed").await == "1");
    assert!(scalar(&mut session, "SELECT count(*)::text FROM prt_1").await == "1");

    // A partitioned parent owns no rows of its own, so a row matching no
    // partition is refused rather than stored in the parent.
    assert!(
        err_message(&mut session, "INSERT INTO prt_renamed VALUES (99999)")
            .await
            .contains("no partition of relation \"prt_renamed\" found for row")
    );
    assert!(scalar(&mut session, "SELECT count(*)::text FROM prt_renamed").await == "1");

    // The leaf's bound is still enforced, and still reported as a bound
    // violation rather than as the parent having gone missing.
    assert!(
        err_message(&mut session, "INSERT INTO prt_1 VALUES (999)")
            .await
            .contains("violates partition constraint")
    );
    assert!(scalar(&mut session, "SELECT count(*)::text FROM prt_1").await == "1");
}

#[tokio::test]
async fn recreating_a_renamed_parents_old_name_adopts_nothing() {
    // The quiet half of the defect. The stranded child kept naming `prt`, so
    // the next relation to take that name inherited it — a brand-new empty
    // table answered `SELECT count(*)` with somebody else's rows.
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE prt (i int4) PARTITION BY RANGE (i)",
        "CREATE TABLE prt_1 PARTITION OF prt FOR VALUES FROM (0) TO (10)",
        "INSERT INTO prt VALUES (1)",
        "ALTER TABLE prt RENAME TO prt_renamed",
        "CREATE TABLE prt (i int4)",
    ])
    .await;

    assert!(scalar(&mut session, "SELECT count(*)::text FROM prt").await == "0");
    assert!(query(&mut session, LINKS).await == link("prt_1", "prt_renamed"));
}

#[tokio::test]
async fn a_renamed_partition_leaves_its_parent_readable_and_writable() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE q (i int4) PARTITION BY RANGE (i)",
        "CREATE TABLE q_1 PARTITION OF q FOR VALUES FROM (0) TO (10)",
        "INSERT INTO q VALUES (5)",
        "ALTER TABLE q_1 RENAME TO q_1_renamed",
    ])
    .await;

    // Every one of these raised `relation "q_1" does not exist`: the parent's
    // child index still named the leaf's old name, so reading or routing
    // through the parent resolved a relation that no longer answered.
    assert!(scalar(&mut session, "SELECT count(*)::text FROM q").await == "1");
    session
        .simple_query("INSERT INTO q VALUES (6)")
        .await
        .expect("routing through the renamed leaf");
    assert!(scalar(&mut session, "SELECT count(*)::text FROM q").await == "2");
    assert!(scalar(&mut session, "SELECT count(*)::text FROM q_1_renamed").await == "2");
    assert!(query(&mut session, LINKS).await == link("q_1_renamed", "q"));
}

#[tokio::test]
async fn a_renamed_inheritance_parent_keeps_its_children() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE inh_p (a int4)",
        "CREATE TABLE inh_c (b int4) INHERITS (inh_p)",
        "INSERT INTO inh_p VALUES (1)",
        "INSERT INTO inh_c VALUES (2, 20)",
        "ALTER TABLE inh_p RENAME TO inh_p_renamed",
    ])
    .await;

    assert!(scalar(&mut session, "SELECT count(*)::text FROM inh_p_renamed").await == "2");
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM ONLY inh_p_renamed"
        )
        .await
            == "1"
    );
    assert!(query(&mut session, LINKS).await == link("inh_c", "inh_p_renamed"));

    // The ancestor walk every row write makes: this failed outright before.
    session
        .simple_query("INSERT INTO inh_c VALUES (3, 30)")
        .await
        .expect("writing a child whose parent was renamed");
    assert!(scalar(&mut session, "SELECT count(*)::text FROM inh_p_renamed").await == "3");
}

#[tokio::test]
async fn a_renamed_inheritance_child_leaves_its_parent_readable() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE r_p (a int4)",
        "CREATE TABLE r_c () INHERITS (r_p)",
        "INSERT INTO r_c VALUES (7)",
        "ALTER TABLE r_c RENAME TO r_c_renamed",
    ])
    .await;

    assert!(scalar(&mut session, "SELECT count(*)::text FROM r_p").await == "1");
    assert!(query(&mut session, LINKS).await == link("r_c_renamed", "r_p"));
}

#[tokio::test]
async fn a_rename_keeps_the_statistics_pg_class_stores() {
    // `reltuples` and `relhassubclass` are stored rather than derived, and both
    // are keyed by relation name. A rename reset the first to unknown and
    // cleared the second; PostgreSQL 18.4 keeps both.
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE rs_p (a int4)",
        "CREATE TABLE rs_c () INHERITS (rs_p)",
        "INSERT INTO rs_p VALUES (1), (2), (3)",
        "ANALYZE rs_p",
    ])
    .await;
    let stats = "SELECT reltuples::text, relhassubclass::text FROM pg_class \
                 WHERE relname = 'rs_p2'";

    session
        .simple_query("ALTER TABLE rs_p RENAME TO rs_p2")
        .await
        .expect("rename");

    assert!(
        query(&mut session, stats).await
            == vec![vec![Some("3".to_string()), Some("true".to_string())]]
    );
}

#[tokio::test]
async fn a_superuser_can_durably_update_pg_class_planner_statistics() {
    let (engine, mut session) = engine_with(&["CREATE TABLE planner_stats (a int4)"]).await;

    let update = session
        .simple_query(
            "UPDATE pg_class SET reltuples = 11, relpages = 7, relallvisible = 5 \
             WHERE relname = 'planner_stats'",
        )
        .await
        .expect("superuser updates planner statistics");
    assert!(matches!(update.as_slice(), [QueryResult::Command { tag }] if tag == "UPDATE 1"));
    assert!(
        query(
            &mut session,
            "SELECT reltuples::text, relpages::text, relallvisible::text \
             FROM pg_class WHERE relname = 'planner_stats'",
        )
        .await
            == vec![vec![
                Some("11".to_string()),
                Some("7".to_string()),
                Some("5".to_string()),
            ]]
    );

    drop(session);
    let mut restarted = engine.connect();
    assert!(
        query(
            &mut restarted,
            "SELECT reltuples::text, relpages::text, relallvisible::text \
             FROM pg_class WHERE relname = 'planner_stats'",
        )
        .await
            == vec![vec![
                Some("11".to_string()),
                Some("7".to_string()),
                Some("5".to_string()),
            ]]
    );
}

#[tokio::test]
async fn renaming_a_partition_key_column_rewrites_the_key() {
    // The scheme stores each key column by name as well as by ordinal, and
    // `pg_get_partkeydef` prints the name. Routing reads the ordinal, so this
    // stayed quiet: the partition kept working while `\d` named a column the
    // relation no longer had.
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE pk_p (a int4, b int4) PARTITION BY RANGE (a)",
        "CREATE TABLE pk_p1 PARTITION OF pk_p FOR VALUES FROM (0) TO (10)",
        "ALTER TABLE pk_p RENAME COLUMN a TO a_renamed",
    ])
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_partkeydef('pk_p'::regclass)::text",
        )
        .await
            == "RANGE (a_renamed)"
    );
    session
        .simple_query("INSERT INTO pk_p VALUES (2, 2)")
        .await
        .expect("routing still reads the ordinal");
    assert!(scalar(&mut session, "SELECT count(*)::text FROM pk_p1").await == "1");
}

/// Every relation the store still names, spelled `needle`, as `key -> value`.
///
/// The scan is over the whole store rather than over a list of key families,
/// because a list is the thing that goes stale. A seventh name-keyed family
/// added later is caught by this without anybody remembering to add it here.
fn still_naming(kv: &dyn Kv, needle: &str) -> Vec<String> {
    let mut found = Vec::new();
    for (key, value) in kv.scan_prefix(&[]).expect("scan the whole store") {
        let key_text = String::from_utf8_lossy(&key).into_owned();
        let value_text = String::from_utf8_lossy(&value).into_owned();
        if key_text.contains(needle) || value_text.contains(needle) {
            found.push(format!("{key_text:?} -> {value_text:?}"));
        }
    }
    found
}

/// A renamed relation's old name survives nowhere in the store.
///
/// The individual tests above each check one family, and a family nobody
/// thought of is exactly how this defect happened: inheritance and partitioning
/// were keyed by name for as long as they had existed, and the rename path grew
/// tablespace, sharding, index, privilege, foreign-key, comment, view and
/// trigger rewrites one at a time without either of them ever being noticed.
///
/// So the subject here takes part in every one of them at once, and the
/// assertion is over the whole keyspace rather than over a list of prefixes. The
/// object names are deliberately unrelated to the relation's, because
/// `PostgreSQL` does *not* rename a table's indexes and constraints with it, and
/// a derived name would make the sweep report a correct answer as a leak.
#[tokio::test]
async fn a_renamed_relation_is_named_nowhere_by_its_old_name() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::default());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine over a store the test holds");
    let mut session = engine.connect();
    for sql in [
        // An inheritance child, an inheritance parent, and every id-keyed
        // family that carries a denormalized relation name beside it.
        "CREATE TABLE zz_anc (a int4)",
        "CREATE TABLE zzsubj_old (b int4 CONSTRAINT zz_pk PRIMARY KEY) INHERITS (zz_anc)",
        "CREATE TABLE zz_child () INHERITS (zzsubj_old)",
        "CREATE TABLE zz_ref (r int4 CONSTRAINT zz_fk REFERENCES zzsubj_old(b))",
        "CREATE INDEX zz_idx ON zzsubj_old (a)",
        "COMMENT ON TABLE zzsubj_old IS 'commented'",
        "COMMENT ON COLUMN zzsubj_old.b IS 'commented'",
        "CREATE ROLE zz_role",
        "GRANT SELECT ON zzsubj_old TO zz_role",
        "CREATE VIEW zz_view AS SELECT a FROM zzsubj_old",
        "INSERT INTO zzsubj_old VALUES (1, 1)",
        "ANALYZE zzsubj_old",
        // A middle level of a partition tree: a partition of one relation and
        // the partitioned parent of another, so both directions are covered.
        "CREATE TABLE zz_top (i int4) PARTITION BY RANGE (i)",
        "CREATE TABLE zzpart_old PARTITION OF zz_top FOR VALUES FROM (0) TO (10) \
         PARTITION BY RANGE (i)",
        "CREATE TABLE zz_leaf PARTITION OF zzpart_old FOR VALUES FROM (0) TO (5)",
        "INSERT INTO zz_top VALUES (1)",
        "ALTER TABLE zzsubj_old RENAME TO zzsubj_new",
        "ALTER TABLE zzpart_old RENAME TO zzpart_new",
    ] {
        session
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("setup {sql} failed: {error:?}"));
    }

    for old in ["zzsubj_old", "zzpart_old"] {
        let found = still_naming(kv.as_ref(), old);
        assert!(found.is_empty(), "{old} survives at {found:#?}");
    }

    // The sweep would also pass over a store that lost the links outright, so
    // the tree is read back through both renamed relations as well.
    assert!(scalar(&mut session, "SELECT count(*)::text FROM zz_anc").await == "1");
    assert!(scalar(&mut session, "SELECT count(*)::text FROM zz_top").await == "1");
    assert!(scalar(&mut session, "SELECT count(*)::text FROM zzpart_new").await == "1");
}

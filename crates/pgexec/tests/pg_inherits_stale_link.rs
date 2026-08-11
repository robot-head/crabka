//! An inheritance link naming a relation the catalog cannot resolve costs that
//! link, and nothing else.
//!
//! `pg_inherits` is read whole by psql's `\d`, so the projection failing as a
//! statement is a database-wide outage rather than one odd row: `\d` stopped
//! working on relations with no inheritance at all. `PostgreSQL` cannot behave
//! that way, because it stores the parent as an oid — measured on 18.4 with
//! `inhparent` hand-set to an oid no relation carries, `SELECT * FROM
//! pg_inherits` prints the number and `\d` is unaffected everywhere.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv, WriteOp, key::push_key_part};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8"))
}

async fn rows_of(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
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

fn row(values: &[&str]) -> Vec<Vec<Option<String>>> {
    vec![values.iter().map(|v| Some((*v).to_string())).collect()]
}

/// Record `child` in `public` as inheriting a `public` relation named `parent`,
/// without creating `parent`.
///
/// This writes the inheritance keyspace's own encoding: the family prefix, then
/// the relation as two length-prefixed key parts, then a versioned record of
/// length-prefixed parent names. Only the one direction is written, because the
/// projection under test reads only that one.
fn plant_stale_parent_link(kv: &dyn Kv, child: &str, parent: &str) {
    let mut key = b"\0\0\0\0catalog_inheritance/parents/".to_vec();
    push_key_part(&mut key, "public");
    push_key_part(&mut key, child);

    let mut value = vec![1u8];
    value.extend_from_slice(&1u32.to_be_bytes());
    for part in ["public", parent] {
        value.extend_from_slice(&u32::try_from(part.len()).expect("short name").to_be_bytes());
        value.extend_from_slice(part.as_bytes());
    }

    kv.write_batch(&[WriteOp::Put { key, value }])
        .expect("plant a stale inheritance link");
}

/// The state was reached through `ALTER TABLE … RENAME TO`, which was a defect
/// of its own: crabka keys inheritance links by relation *name*, and the rename
/// rewrote neither the child's parent list nor the parent's child index. That
/// producer is fixed, and no SQL spelling reaches the state any more — a rename
/// now moves the links, and a drop removes them. So the link is planted in the
/// store directly, and the plant is checked before anything is asserted about
/// it: an encoding change that made the plant a no-op would otherwise leave
/// every assertion below passing over a healthy database.
///
/// What the test asserts is containment: the healthy set still reads correctly,
/// the affected child still has its row, and an unrelated relation is still
/// describable. Every one of these reads raised `relation "amp_bad_p" does not
/// exist` before.
#[tokio::test]
async fn a_stale_inheritance_link_costs_its_own_row_and_no_other_relation() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::default());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine over a store the test holds");
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE amp_unrelated (x int4)",
        "CREATE TABLE amp_ok_p (i int4)",
        "CREATE TABLE amp_ok_c () INHERITS (amp_ok_p)",
        "CREATE TABLE amp_bad_c (i int4)",
    ] {
        session
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("setup {sql} failed: {error:?}"));
    }
    assert!(rows_of(&mut session, "SELECT count(*)::text FROM pg_inherits").await == row(&["1"]));

    plant_stale_parent_link(kv.as_ref(), "amp_bad_c", "amp_bad_p");

    // Both links are still described, so the stale one is a row rather than an
    // erased fact — a catalog table owes the odd row, not silence. This count
    // is also what proves the plant took.
    assert!(rows_of(&mut session, "SELECT count(*)::text FROM pg_inherits").await == row(&["2"]));
    assert!(
        rows_of(
            &mut session,
            "SELECT count(*)::text FROM pg_inherits WHERE inhrelid = 'amp_bad_c'::regclass",
        )
        .await
            == row(&["1"])
    );

    // The healthy set, read the way psql's `\d` reads it.
    assert!(
        rows_of(
            &mut session,
            "SELECT ch.relname, pa.relname FROM pg_inherits i \
             JOIN pg_class ch ON ch.oid = i.inhrelid \
             JOIN pg_class pa ON pa.oid = i.inhparent \
             WHERE ch.relname = 'amp_ok_c'",
        )
        .await
            == row(&["amp_ok_c", "amp_ok_p"])
    );

    // A relation with no inheritance at all, which is what the wholesale
    // failure took down with it.
    assert!(
        rows_of(
            &mut session,
            "SELECT attname FROM pg_attribute \
             WHERE attrelid = 'amp_unrelated'::regclass AND attnum > 0",
        )
        .await
            == row(&["x"])
    );
}

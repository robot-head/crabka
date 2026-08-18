//! A lateral item memoizes its inner relation per distinct binding.
//!
//! Caching deliberately does not require the join index over the outer
//! relation, because re-running the inner query is the expensive half — for a
//! lateral over a wide table it is a full scan per outer row — while an
//! index-less entry still skips that and probes a relation that is usually a
//! row or two. Requiring the index meant nothing was cached under a small
//! memory budget.
//!
//! The two paths are observationally identical apart from speed, so what is
//! pinned here is that they agree: the same query under a budget too small for
//! the index must return exactly what it returns under a generous one.

use assert2::assert;
use crabka_pgexec::{RuntimePolicy, SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn rows_under(budget: crabka_units::ByteSize, sql: &str) -> Vec<Vec<Option<String>>> {
    let engine = SqlEngine::new_with_policy(RuntimePolicy {
        blocking_query_memory: budget,
        ..RuntimePolicy::default()
    })
    .expect("runtime policy");
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE outer_t (k int4, tag text);
         CREATE TABLE inner_t (k int4, v int4);
         INSERT INTO outer_t VALUES (1, 'a'), (2, 'b'), (1, 'c'), (3, 'd'), (2, 'e'), (1, 'f');
         INSERT INTO inner_t VALUES (1, 10), (1, 11), (2, 20), (3, 30), (4, 40)",
    )
    .await;
    query(&mut session, sql).await
}

async fn run(session: &mut SqlSession, sql: &str) {
    session.simple_query(sql).await.expect("setup");
}

async fn query(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &session.simple_query(sql).await.expect(sql)[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|c: &Option<Cell>| {
                        c.as_ref()
                            .map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// The cached path and the indexed path must agree, for a plain lateral and for
/// a `LEFT JOIN LATERAL` whose inner side produces nothing for one key.
#[tokio::test]
async fn a_memoized_lateral_returns_what_the_indexed_one_returns() {
    let cases = [
        "SELECT o.tag, i.v FROM outer_t o, LATERAL (SELECT v FROM inner_t WHERE k = o.k) i \
         ORDER BY o.tag, i.v",
        "SELECT o.tag, i.v FROM outer_t o LEFT JOIN LATERAL \
         (SELECT v FROM inner_t WHERE k = o.k AND v > 15) i ON true ORDER BY o.tag, i.v",
        "SELECT o.tag, i.v FROM outer_t o, LATERAL (SELECT v FROM inner_t WHERE k = o.k OFFSET 0) i \
         ORDER BY o.tag, i.v",
    ];
    for sql in cases {
        // The budget bounds what a lateral entry may retain, so a tight one
        // takes the index-less cached path and a generous one the indexed
        // path. Both must return the same rows.
        let cramped = rows_under(crabka_units::kibibytes(8), sql).await;
        let roomy = rows_under(crabka_units::mebibytes(4), sql).await;
        assert!(cramped == roomy, "{sql}");
        assert!(!cramped.is_empty(), "{sql}");
    }
}

/// A repeated binding must not multiply the inner rows: three outer rows share
/// key 1, which matches two inner rows, so the join yields six — not the
/// eighteen a cache keyed carelessly would produce.
#[tokio::test]
async fn a_repeated_binding_reuses_its_entry_without_duplicating_rows() {
    let rows = rows_under(
        crabka_units::kibibytes(8),
        "SELECT o.tag, i.v FROM outer_t o, LATERAL (SELECT v FROM inner_t WHERE k = o.k) i \
         ORDER BY o.tag, i.v",
    )
    .await;

    // k=1 three times x two inner rows, k=2 twice x one, k=3 once x one.
    assert!(rows.len() == 9);
    let for_key_one: Vec<_> = rows
        .iter()
        .filter(|row| matches!(row[0].as_deref(), Some("a" | "c" | "f")))
        .collect();
    assert!(for_key_one.len() == 6);
}

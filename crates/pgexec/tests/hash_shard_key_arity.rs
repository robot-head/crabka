//! Hash shard-key arity: a hash-sharded table hashes exactly one column, and
//! the bucket a row is *stored* under is the hash of that column's value.
//!
//! The gateway derives a statement's route from the same encoding, so the two
//! must not drift. `CREATE TABLE` fixes the single-column arity in two places.
//! The grammar fixes it for a SQL statement, and the catalog API fixes it for a
//! caller that builds a sharding directly. So a table that could never be
//! written to is never created either.

use std::sync::Arc;

use assert2::assert;
use crabka_pgcatalog::{Column, HashSharding, RelationName, ShardingStrategy, TableOptions};
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgtypes::ColumnType;
use crabka_pgwire::engine::{Engine, Session};

const BUCKETS: u32 = 16;

fn hash_sharding(columns: &[&str]) -> ShardingStrategy {
    ShardingStrategy::Hash(HashSharding {
        columns: columns.iter().map(|column| (*column).to_string()).collect(),
        buckets: BUCKETS,
        co_location_group: None,
    })
}

// The bucket the single stored row of a table physically lives in.
fn stored_bucket(kv: &dyn Kv, table_id: u32) -> u32 {
    let buckets = kv
        .scan_prefix(&crabka_pgkv::key::table_prefix(table_id))
        .expect("scan table prefix")
        .into_iter()
        .map(|(key, _)| {
            let prefix = crabka_pgmvcc::version::row_prefix_of(&key).expect("version key");
            crabka_pgkv::key::bucket_rowid_of(table_id, prefix)
                .expect("hash row key")
                .0
        })
        .collect::<Vec<_>>();
    assert!(
        buckets.len() == 1,
        "expected exactly one stored row version"
    );
    buckets[0]
}

// A table whose sharding was attached outside CREATE TABLE, which the catalog
// API allows at the one arity that has a row encoding, plus a session on the
// engine that sees it.
fn engine_with_catalog_sharding(
    columns: Vec<Column>,
    sharding: &ShardingStrategy,
) -> (Arc<dyn Kv>, SqlEngine, SqlSession) {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("in-memory engine");
    let (_, ops) = crabka_pgcatalog::create_table_with_sharding_ops(
        kv.as_ref(),
        &RelationName::public("t"),
        columns,
        TableOptions { sharded: true },
        Some(sharding),
        Vec::new(),
        crabka_pgcatalog::TableIdSource::Counter,
    )
    .expect("catalog write batch");
    kv.write_batch(&ops).expect("attach sharding");
    let session = engine.connect();
    (kv, engine, session)
}

// SHARDED BY HASH accepts exactly one column: the arity the row-storage encoder
// hashes. A wider key is refused before the catalog is written.
#[tokio::test]
async fn create_table_accepts_exactly_one_hash_shard_column() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    for ddl in [
        "CREATE TABLE two (a int4, b int4) SHARDED BY HASH (a, b) BUCKETS 16",
        "CREATE TABLE three (a int4, b int4, c int4) SHARDED BY HASH (a, b, c) BUCKETS 16",
    ] {
        let error = session
            .simple_query(ddl)
            .await
            .expect_err("a multi-column hash shard key is refused");
        assert!(error.code == "42601", "{ddl}: {error:?}");
        assert!(error.message.contains("exactly one column"), "{error:?}");
    }
    for name in ["two", "three"] {
        assert!(
            engine.catalog_table(&RelationName::public(name)).is_err(),
            "{name} was created"
        );
    }

    session
        .simple_query("CREATE TABLE one (a int4, b int4) SHARDED BY HASH (a) BUCKETS 16")
        .await
        .expect("a single-column hash shard key is accepted");
    assert!(
        engine
            .catalog_table(&RelationName::public("one"))
            .expect("catalog")
            .sharding
            == Some(hash_sharding(&["a"]))
    );
}

// For every shard-key type a table can be created with, the bucket the row is
// stored under is the hash of that one column's value — the same bytes the
// gateway hashes to pick the range the statement is routed to.
#[tokio::test]
async fn stored_bucket_is_the_hash_of_the_single_shard_column() {
    for (ty, literal, key_bytes) in [
        ("int4", "42", 42_i32.to_be_bytes().to_vec()),
        ("int8", "42", 42_i64.to_be_bytes().to_vec()),
        ("text", "'abc'", b"abc".to_vec()),
    ] {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(&format!(
                "CREATE TABLE t (k {ty}, v int4) SHARDED BY HASH (k) BUCKETS {BUCKETS}"
            ))
            .await
            .expect("create");
        session
            .simple_query(&format!("INSERT INTO t VALUES ({literal}, 7)"))
            .await
            .expect("insert");

        let table = engine
            .catalog_table(&RelationName::public("t"))
            .expect("catalog");
        let expected =
            crabka_pgkv::key::hash_bucket(&key_bytes, BUCKETS).expect("power-of-two buckets");
        assert!(
            stored_bucket(engine.kv_handle().as_ref(), table.id) == expected,
            "{ty} shard key"
        );
    }
}

// `regclass` is one of the types CREATE TABLE accepts as a hash shard key, and
// a `regclass` value hashes on its oid — the identity it keeps once the name it
// renders is attached to it. Without a write-path encoding for the type, the
// INSERT would be refused at run time by a table the DDL had already allowed.
#[tokio::test]
async fn a_regclass_shard_key_hashes_on_the_relation_oid() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE target (a int4)")
        .await
        .expect("create the relation the shard key names");
    session
        .simple_query(&format!(
            "CREATE TABLE t (k regclass, v int4) SHARDED BY HASH (k) BUCKETS {BUCKETS}"
        ))
        .await
        .expect("regclass is an accepted shard-key type");
    session
        .simple_query("INSERT INTO t VALUES ('target'::regclass, 7)")
        .await
        .expect("a regclass shard key has a write-path encoding");

    // A `regclass` value carries the table's `pg_class` oid, which is its
    // catalog id inside the table oid band — not the bare id.
    let target_oid = crabka_pgexec::table_relation_oid(
        engine
            .catalog_table(&RelationName::public("target"))
            .expect("catalog")
            .id,
    )
    .expect("an oid fits in int4");
    let table = engine
        .catalog_table(&RelationName::public("t"))
        .expect("catalog");
    let expected = crabka_pgkv::key::hash_bucket(&target_oid.to_be_bytes(), BUCKETS)
        .expect("power-of-two buckets");
    assert!(stored_bucket(engine.kv_handle().as_ref(), table.id) == expected);
}

// The catalog API refuses the same arity the grammar does, so the seam that
// bypasses the parser cannot create a table whose rows the write path would
// have to refuse. Attaching such a sharding to an existing table is refused
// too — the outcome is the same broken table either way.
#[test]
fn the_catalog_api_refuses_a_multi_column_hash_shard_key() {
    let kv = MemKv::new();
    let columns = vec![
        Column::new("a", ColumnType::Int4),
        Column::new("b", ColumnType::Int4),
    ];
    crabka_pgcatalog::create_table_with_options(
        &kv,
        &RelationName::public("existing"),
        columns.clone(),
        TableOptions { sharded: true },
    )
    .expect("create the table the sharding is attached to");

    for error in [
        crabka_pgcatalog::create_table_with_sharding_ops(
            &kv,
            &RelationName::public("t"),
            columns,
            TableOptions { sharded: true },
            Some(&hash_sharding(&["a", "b"])),
            Vec::new(),
            crabka_pgcatalog::TableIdSource::Counter,
        )
        .expect_err("a multi-column hash shard key has no row encoding"),
        crabka_pgcatalog::set_table_sharding_ops(
            &kv,
            &RelationName::public("existing"),
            Some(&hash_sharding(&["a", "b"])),
        )
        .expect_err("a multi-column hash shard key has no row encoding"),
    ] {
        assert!(error.sqlstate() == "0A000", "{error:?}");
        assert!(
            error.to_string()
                == "invalid sharding definition: hash sharding requires exactly one column",
            "{error:?}"
        );
    }
}

// The same seam with a single column still writes, so the refusal above is
// about the arity and not about catalog-attached sharding in general.
#[tokio::test]
async fn writing_a_row_accepts_catalog_attached_single_column_sharding() {
    let (kv, engine, mut session) = engine_with_catalog_sharding(
        vec![
            Column::new("a", ColumnType::Int4),
            Column::new("b", ColumnType::Int4),
        ],
        &hash_sharding(&["a"]),
    );

    session
        .simple_query("INSERT INTO t VALUES (1, 2)")
        .await
        .expect("single-column sharding writes");
    let table = engine
        .catalog_table(&RelationName::public("t"))
        .expect("catalog");
    let expected =
        crabka_pgkv::key::hash_bucket(&1_i32.to_be_bytes(), BUCKETS).expect("power-of-two buckets");
    assert!(stored_bucket(kv.as_ref(), table.id) == expected);
}

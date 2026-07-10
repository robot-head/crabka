use std::sync::{Arc, Mutex};

use crabka_pgcatalog::{
    CatalogError, Column, ForeignServer, HashSharding, ShardingStrategy, Table, TablePrivilege,
    UserMapping, get_table, list_table_privileges,
};
use crabka_pgexec::{Committer, LocalLinearizer, SqlEngine, foreign};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_pgwire::engine::{Engine, Session};

struct RecordingCommitter {
    kv: Arc<dyn Kv>,
    batches: Mutex<Vec<Vec<WriteOp>>>,
}

impl RecordingCommitter {
    fn new(kv: Arc<dyn Kv>) -> Self {
        Self {
            kv,
            batches: Mutex::new(Vec::new()),
        }
    }

    fn batches(&self) -> Vec<Vec<WriteOp>> {
        self.batches.lock().expect("batches mutex").clone()
    }
}

#[async_trait::async_trait]
impl Committer for RecordingCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), crabka_pgexec::ExecError> {
        self.kv.write_batch(&ops)?;
        self.batches.lock().expect("batches mutex").push(ops);
        Ok(())
    }
}

struct ImportingScanner;

impl foreign::ForeignScanner for ImportingScanner {
    fn scan(
        &self,
        _table: &Table,
        _server: &ForeignServer,
        _mapping: Option<&UserMapping>,
        _bounds: &foreign::ScanBounds,
        _ctx: &crabka_pgexec::clock::EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, crabka_pgexec::ExecError> {
        Ok(Vec::new())
    }

    fn import_schema(
        &self,
        _server: &ForeignServer,
        _mapping: Option<&UserMapping>,
        filter: &foreign::ImportFilter,
    ) -> Result<Vec<foreign::ImportedTable>, crabka_pgexec::ExecError> {
        let tables = ["imported_one", "imported_two"]
            .into_iter()
            .filter(|name| filter.retains(name))
            .map(|name| foreign::ImportedTable {
                name: name.to_string(),
                columns: vec![Column::new("value", ColumnType::Text)],
                options: vec![("topic".into(), name.to_string())],
            })
            .collect();
        Ok(tables)
    }
}

fn engine_with_recording_committer() -> (SqlEngine, Arc<RecordingCommitter>) {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let committer = Arc::new(RecordingCommitter::new(Arc::clone(&kv)));
    let committer_trait: Arc<dyn Committer> = committer.clone();
    let engine = SqlEngine::replicated(
        Arc::clone(&kv),
        kv,
        committer_trait,
        Arc::new(LocalLinearizer),
    )
    .expect("replicated test engine");
    (engine, committer)
}

fn engine_with_separate_catalog_and_data_stores() -> (SqlEngine, Arc<dyn Kv>, Arc<dyn Kv>) {
    let catalog_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let data_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let committer: Arc<dyn Committer> = Arc::new(RecordingCommitter::new(Arc::clone(&catalog_kv)));
    let engine = SqlEngine::replicated(
        Arc::clone(&catalog_kv),
        Arc::clone(&data_kv),
        committer,
        Arc::new(LocalLinearizer),
    )
    .expect("replicated test engine with separate catalog store");
    (engine, catalog_kv, data_kv)
}

async fn run(session: &mut crabka_pgexec::SqlSession, sql: &str) {
    session.simple_query(sql).await.expect(sql);
}

#[tokio::test]
async fn fdw_ddl_routes_all_catalog_writes_through_committer() {
    let (engine, committer) = engine_with_recording_committer();
    let mut session = engine.connect();

    run(&mut session, "CREATE FOREIGN DATA WRAPPER kafka_fdw").await;
    run(
        &mut session,
        "CREATE SERVER kafka_srv FOREIGN DATA WRAPPER kafka_fdw",
    )
    .await;
    run(
        &mut session,
        "CREATE USER MAPPING FOR CURRENT_USER SERVER kafka_srv",
    )
    .await;
    run(
        &mut session,
        "CREATE FOREIGN TABLE ft (value text) SERVER kafka_srv OPTIONS (topic 'ft')",
    )
    .await;
    run(&mut session, "DROP FOREIGN TABLE ft").await;
    run(
        &mut session,
        "DROP USER MAPPING FOR CURRENT_USER SERVER kafka_srv",
    )
    .await;
    run(&mut session, "DROP SERVER kafka_srv").await;
    run(&mut session, "DROP FOREIGN DATA WRAPPER kafka_fdw").await;

    let batches = committer.batches();
    assert_eq!(batches.len(), 8);
    assert!(batches.iter().all(|batch| !batch.is_empty()));
}

#[tokio::test]
async fn alter_table_rename_uses_authoritative_catalog_and_preserves_sharding_and_acl() {
    let (engine, catalog_kv, data_kv) = engine_with_separate_catalog_and_data_stores();
    let mut session = engine.connect();

    run(
        &mut session,
        "CREATE TABLE orders (id int4, region text) SHARDED BY HASH (id) BUCKETS 8",
    )
    .await;
    let original_table_id = get_table(catalog_kv.as_ref(), "orders")
        .expect("original table exists in the authoritative catalog")
        .id;
    run(&mut session, "CREATE ROLE analyst").await;
    run(&mut session, "GRANT SELECT ON TABLE orders TO analyst").await;
    run(
        &mut session,
        "ALTER TABLE orders RENAME TO fulfilled_orders",
    )
    .await;

    let expected_sharding = ShardingStrategy::Hash(HashSharding {
        columns: vec!["id".into()],
        buckets: 8,
        co_location_group: None,
    });
    let renamed = get_table(catalog_kv.as_ref(), "fulfilled_orders")
        .expect("renamed table exists in the authoritative catalog");
    assert_eq!(renamed.id, original_table_id);
    assert_eq!(renamed.name, "fulfilled_orders");
    assert!(renamed.sharded);
    assert_eq!(renamed.sharding, Some(expected_sharding.clone()));
    assert_eq!(
        engine
            .table_sharding("fulfilled_orders")
            .expect("engine catalog lookup"),
        Some(expected_sharding),
    );
    assert_eq!(
        list_table_privileges(catalog_kv.as_ref()).expect("catalog privileges"),
        vec![TablePrivilege {
            table: "fulfilled_orders".into(),
            grantee: "analyst".into(),
            privilege: "SELECT".into(),
        }],
    );
    assert_eq!(
        get_table(catalog_kv.as_ref(), "orders"),
        Err(CatalogError::UndefinedTable("orders".into())),
    );
    assert_eq!(
        catalog_kv
            .get(&crabka_pgkv::key::catalog_sharding_key("orders"))
            .expect("old sharding metadata lookup"),
        None,
    );
    assert_eq!(
        data_kv
            .get(&crabka_pgkv::key::catalog_key("fulfilled_orders"))
            .expect("data store lookup"),
        None,
    );
}

#[tokio::test]
async fn import_foreign_schema_routes_created_tables_through_committer() {
    let (mut engine, committer) = engine_with_recording_committer();
    engine.set_foreign_scanner(Arc::new(ImportingScanner));
    let mut session = engine.connect();

    run(&mut session, "CREATE FOREIGN DATA WRAPPER kafka_fdw").await;
    run(
        &mut session,
        "CREATE SERVER kafka_srv FOREIGN DATA WRAPPER kafka_fdw",
    )
    .await;
    run(
        &mut session,
        "IMPORT FOREIGN SCHEMA remote LIMIT TO (imported_one) FROM SERVER kafka_srv",
    )
    .await;

    let batches = committer.batches();
    let import_batch = batches.last().expect("import batch recorded");
    assert_eq!(batches.len(), 3);
    assert_eq!(import_batch.len(), 3);

    let rows = session
        .simple_query("SELECT value FROM imported_one")
        .await
        .expect("imported table is visible through committed catalog ops");
    let [crabka_pgwire::engine::QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows from imported foreign table");
    };
    assert!(rows.is_empty());
}

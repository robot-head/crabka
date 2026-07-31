use std::sync::{Arc, Mutex};

use assert2::assert;
use crabka_pgcatalog::{Column, ForeignServer, Table, TableId, UserMapping, list_tables};
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

    /// Batches whose shape is a table-id claim: one `Put` of the shared counter
    /// and nothing else. Every other catalog batch carries relation keys too.
    fn table_id_reservations(&self) -> usize {
        self.batches()
            .iter()
            .filter(|batch| {
                matches!(
                    batch.as_slice(),
                    [WriteOp::Put { key, .. }] if *key == crabka_pgkv::key::meta_next_table_id_key()
                )
            })
            .count()
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

/// A scanner whose remote schema holds three tables, so one
/// `IMPORT FOREIGN SCHEMA` creates three relations in a single statement.
struct ThreeTableScanner;

const IMPORTED: [&str; 3] = ["alpha", "beta", "gamma"];

impl foreign::ForeignScanner for ThreeTableScanner {
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
        Ok(IMPORTED
            .into_iter()
            .filter(|name| filter.retains(name))
            .map(|name| foreign::ImportedTable {
                name: name.to_string(),
                columns: vec![Column::new("value", ColumnType::Text)],
                options: vec![("topic".into(), name.to_string())],
            })
            .collect())
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

async fn run(session: &mut crabka_pgexec::SqlSession, sql: &str) {
    session.simple_query(sql).await.expect(sql);
}

fn catalog_table_ids(engine: &SqlEngine) -> Vec<TableId> {
    list_tables(engine.catalog_kv())
        .expect("catalog tables")
        .iter()
        .map(|table| table.id)
        .collect()
}

fn catalog_table_names(engine: &SqlEngine) -> Vec<String> {
    list_tables(engine.catalog_kv())
        .expect("catalog tables")
        .iter()
        .map(|table| table.name.name.clone())
        .collect()
}

/// `ids` sorted, and that same sorted list with adjacent duplicates removed.
/// The two are equal exactly when every id is distinct, and comparing them
/// reports which id collided rather than just how many did.
fn sorted_and_deduped(ids: &[TableId]) -> (Vec<TableId>, Vec<TableId>) {
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    let mut deduped = sorted.clone();
    deduped.dedup();
    (sorted, deduped)
}

#[tokio::test]
async fn interleaved_sessions_never_share_a_table_id() {
    let (engine, _committer) = engine_with_recording_committer();
    let mut left = engine.connect();
    let mut right = engine.connect();

    for n in 0..5 {
        run(&mut left, &format!("CREATE TABLE left_{n} (id int4)")).await;
        run(&mut right, &format!("CREATE TABLE right_{n} (id int4)")).await;
    }

    assert!(
        catalog_table_names(&engine)
            == [
                "left_0", "left_1", "left_2", "left_3", "left_4", "right_0", "right_1", "right_2",
                "right_3", "right_4",
            ]
    );
    let (sorted, deduped) = sorted_and_deduped(&catalog_table_ids(&engine));
    assert!(sorted == deduped);
}

#[tokio::test]
async fn a_session_that_creates_no_relation_claims_no_table_ids() {
    let (engine, committer) = engine_with_recording_committer();
    let mut session = engine.connect();

    run(&mut session, "CREATE SCHEMA s").await;
    run(&mut session, "CREATE ROLE analyst").await;

    // Both statements committed; neither of them bought a block of ids.
    assert!(committer.batches().len() == 2);
    assert!(committer.table_id_reservations() == 0);
}

#[tokio::test]
async fn one_reservation_covers_a_whole_block_of_creations() {
    for (creations, expected_reservations) in [(8, 1), (9, 2)] {
        let (engine, committer) = engine_with_recording_committer();
        let mut session = engine.connect();

        for n in 0..creations {
            run(&mut session, &format!("CREATE TABLE t{n} (id int4)")).await;
        }

        let (sorted, deduped) = sorted_and_deduped(&catalog_table_ids(&engine));
        assert!(sorted == deduped);
        assert!(committer.table_id_reservations() == expected_reservations);
    }
}

#[tokio::test]
async fn import_foreign_schema_gives_every_imported_table_a_distinct_id() {
    let (mut engine, _committer) = engine_with_recording_committer();
    engine.set_foreign_scanner(Arc::new(ThreeTableScanner));
    let mut session = engine.connect();

    run(&mut session, "CREATE FOREIGN DATA WRAPPER kafka_fdw").await;
    run(
        &mut session,
        "CREATE SERVER kafka_srv FOREIGN DATA WRAPPER kafka_fdw",
    )
    .await;
    run(
        &mut session,
        "IMPORT FOREIGN SCHEMA remote FROM SERVER kafka_srv",
    )
    .await;

    assert!(catalog_table_names(&engine) == IMPORTED);
    let (sorted, deduped) = sorted_and_deduped(&catalog_table_ids(&engine));
    assert!(sorted == deduped);
    let counter = crabka_pgcatalog::read_next_table_id(engine.catalog_kv()).expect("next table id");
    assert!(counter > *sorted.last().expect("an imported table id"));
}

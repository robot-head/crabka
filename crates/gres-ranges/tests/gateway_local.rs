use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use crabka_gres_control::{RangeLayoutEntry, SqlUser, TenantId, TenantRecord, TenantState};
use crabka_gres_ranges::{
    CheckpointManifest, ClaimedStagedSuccessor, CommittedTailRecord, FramedTcpClient,
    HostedRangeService, LocalSqlSplitError, MultiRangeTenant, MultiRangeTenantConfig, RangeId,
    RangeRegistry, RangeService, RangeTlsClientConfig, RangeTlsServerConfig, RangeTransferBarrier,
    RangeTransferCapability, RangeTransferError, StagedRangeSuccessor, TableId,
    TableTransferRequest, TenantName, serve_tls, tenant::EmptyTableSplitTestHook,
};
use crabka_pgexec::SqlEngine;
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

struct MtlsFixture {
    _dir: tempfile::TempDir,
    server: RangeTlsServerConfig,
    client: RangeTlsClientConfig,
}

impl MtlsFixture {
    fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = tempfile::tempdir().expect("temporary certificate directory");
        let server_cert = write_fixture(&dir, "server-cert.pem", "dev_cert.pem");
        let server_key = write_fixture(&dir, "server-key.pem", "dev_key.pem");
        let client_ca = write_fixture(&dir, "client-ca.pem", "dev_client_ca.pem");
        let client_cert = write_fixture(&dir, "client-cert.pem", "dev_client_cert.pem");
        let client_key = write_fixture(&dir, "client-key.pem", "dev_client_key.pem");
        Self {
            _dir: dir,
            server: RangeTlsServerConfig {
                tenant: "tenant_gateway_remote".to_string(),
                tls: crabka_security::TlsConfig {
                    cert_chain_path: server_cert.clone(),
                    private_key_path: server_key,
                    trust_roots_path: Some(server_cert.clone()),
                    client_ca_path: Some(client_ca),
                    client_auth: crabka_security::ClientAuthMode::Required,
                },
                allowed_principals: BTreeSet::from([
                    "CN=test-client,OU=integration,O=crabka".to_string()
                ]),
            },
            client: RangeTlsClientConfig {
                tls: crabka_security::TlsConfig {
                    cert_chain_path: client_cert,
                    private_key_path: client_key,
                    trust_roots_path: Some(server_cert),
                    client_ca_path: None,
                    client_auth: crabka_security::ClientAuthMode::Disabled,
                },
                server_name: "crabka-dev".to_string(),
            },
        }
    }
}

fn write_fixture(dir: &tempfile::TempDir, name: &str, fixture: &str) -> PathBuf {
    let path = dir.path().join(name);
    let contents: &[u8] = match fixture {
        "dev_cert.pem" => include_bytes!("../../security/tests/fixtures/dev_cert.pem"),
        "dev_key.pem" => include_bytes!("../../security/tests/fixtures/dev_key.pem"),
        "dev_client_ca.pem" => include_bytes!("../../security/tests/fixtures/dev_client_ca.pem"),
        "dev_client_cert.pem" => {
            include_bytes!("../../security/tests/fixtures/dev_client_cert.pem")
        }
        "dev_client_key.pem" => include_bytes!("../../security/tests/fixtures/dev_client_key.pem"),
        _ => unreachable!("fixture name is fixed by this test"),
    };
    std::fs::write(&path, contents).expect("write certificate fixture");
    path
}

async fn spawn_tls(
    service: Arc<dyn RangeService>,
    config: RangeTlsServerConfig,
) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let address = listener.local_addr().expect("TLS listener address");
    tokio::spawn(async move {
        let _ = serve_tls(listener, service, config).await;
    });
    address
}

fn start_gateway() -> MultiRangeTenant {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_gateway").expect("tenant"),
        "0,100,200",
    )
    .expect("config");
    MultiRangeTenant::start(config).expect("tenant").0
}

#[tokio::test]
async fn gateway_serves_local_range_reads_after_routed_writes() {
    let gateway = start_gateway();
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t150 VALUES (9)")
        .await
        .expect("insert");
    let rows = session
        .simple_query("SELECT id FROM t150")
        .await
        .expect("select");

    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0].as_ref().expect("cell").text, "9");
}

#[tokio::test]
async fn empty_table_boundary_split_publishes_ready_successor_and_requires_old_session_reconnect() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_empty_table_split").expect("tenant"),
        "0,200",
    )
    .expect("config");
    let gateway = MultiRangeTenant::start(config).expect("tenant").0;
    let mut old_session = gateway.connect();
    old_session
        .simple_query("CREATE TABLE t100 (id int4)")
        .await
        .expect("create empty moved table");
    old_session
        .parse("stale", "SELECT id FROM t100", &[])
        .await
        .expect("parse before split");
    old_session
        .bind("stale", "stale", &[], &[])
        .await
        .expect("bind before split");

    gateway
        .split_empty_table_successor("empty-t100", RangeId::new(2), TableId::new(100))
        .await
        .expect("empty table split");

    let old_session_error = old_session
        .simple_query("SELECT id FROM t100")
        .await
        .expect_err("old session must reconnect after map publication");
    assert_eq!(old_session_error.code, "0A000");
    assert_eq!(
        old_session
            .describe_statement("stale")
            .await
            .expect_err("stale statement describe")
            .code,
        "0A000"
    );
    assert_eq!(
        old_session
            .describe_portal("stale")
            .await
            .expect_err("stale portal describe")
            .code,
        "0A000"
    );

    let mut current_session = gateway.connect();
    current_session
        .simple_query("INSERT INTO t100 VALUES (9)")
        .await
        .expect("successor insert");
    let rows = current_session
        .simple_query("SELECT id FROM t100")
        .await
        .expect("successor select");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0].as_ref().expect("cell").text, "9");
}

#[tokio::test]
async fn populated_table_boundary_split_fails_closed_without_publishing_a_partial_map() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_populated_table_split").expect("tenant"),
        "0,200",
    )
    .expect("config");
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let initial_map = handles.range_map();
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t100 (id int4)")
        .await
        .expect("create moved table");
    session
        .simple_query("INSERT INTO t100 VALUES (9)")
        .await
        .expect("write predecessor row");

    let error = gateway
        .split_empty_table_successor("populated-t100", RangeId::new(2), TableId::new(100))
        .await
        .expect_err("nonempty SQL table requires durable snapshot transfer");

    assert!(
        matches!(error, LocalSqlSplitError::NonEmptyTable(table_id) if table_id == TableId::new(100))
    );
    assert_eq!(handles.range_map(), initial_map);
    assert!(
        !gateway
            .hosted_range_engines()
            .contains_key(&RangeId::new(2))
    );

    session
        .simple_query("INSERT INTO t100 VALUES (10)")
        .await
        .expect("predecessor remains writable after rejected split");
    let rows = session
        .simple_query("SELECT id FROM t100")
        .await
        .expect("predecessor remains readable after rejected split");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_ref().expect("first cell").text, "9");
    assert_eq!(rows[1][0].as_ref().expect("second cell").text, "10");
}

struct InProcessTransfer {
    source: SqlEngine,
    paused: AtomicBool,
    fail_before_stage: bool,
    staged: Mutex<Option<SqlEngine>>,
    stage_started: Option<Arc<tokio::sync::Notify>>,
    allow_stage: Option<Arc<tokio::sync::Notify>>,
}

impl InProcessTransfer {
    fn new(source: SqlEngine, fail_before_stage: bool) -> Self {
        Self {
            source,
            paused: AtomicBool::new(false),
            fail_before_stage,
            staged: Mutex::new(None),
            stage_started: None,
            allow_stage: None,
        }
    }

    fn blocked_after_pause(
        source: SqlEngine,
    ) -> (Self, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let stage_started = Arc::new(tokio::sync::Notify::new());
        let allow_stage = Arc::new(tokio::sync::Notify::new());
        (
            Self {
                source,
                paused: AtomicBool::new(false),
                fail_before_stage: false,
                staged: Mutex::new(None),
                stage_started: Some(Arc::clone(&stage_started)),
                allow_stage: Some(Arc::clone(&allow_stage)),
            },
            stage_started,
            allow_stage,
        )
    }

    fn error(range_id: RangeId, reason: &str) -> RangeTransferError {
        RangeTransferError::Runtime {
            range_id,
            reason: reason.to_owned(),
        }
    }
}

#[async_trait]
impl RangeTransferCapability for InProcessTransfer {
    async fn force_checkpoint(
        &self,
        range_id: RangeId,
    ) -> Result<CheckpointManifest, RangeTransferError> {
        Ok(CheckpointManifest {
            range_id,
            covered_offset: 0,
            manifest_key: "in-process".to_owned(),
        })
    }

    async fn pause_at_checkpoint(
        &self,
        checkpoint: &CheckpointManifest,
    ) -> Result<RangeTransferBarrier, RangeTransferError> {
        if self.paused.swap(true, Ordering::AcqRel) {
            return Err(RangeTransferError::AlreadyPaused {
                range_id: checkpoint.range_id,
            });
        }
        Ok(RangeTransferBarrier {
            range_id: checkpoint.range_id,
            offset: 1,
        })
    }

    async fn read_committed_tail(
        &self,
        _range_id: RangeId,
        _after_offset: i64,
        _barrier: RangeTransferBarrier,
    ) -> Result<Vec<CommittedTailRecord>, RangeTransferError> {
        Ok(vec![CommittedTailRecord {
            offset: 1,
            bytes: Vec::new(),
        }])
    }

    async fn resume(&self, barrier: RangeTransferBarrier) -> Result<(), RangeTransferError> {
        if !self.paused.swap(false, Ordering::AcqRel) {
            return Err(Self::error(barrier.range_id, "source was not paused"));
        }
        Ok(())
    }

    fn resume_after_drop(&self, _barrier: RangeTransferBarrier) {
        self.paused.store(false, Ordering::Release);
    }

    async fn stage_empty_successor(
        &self,
        request: TableTransferRequest,
        _checkpoint: &CheckpointManifest,
        _tail: &[CommittedTailRecord],
        _barrier: RangeTransferBarrier,
    ) -> Result<StagedRangeSuccessor, RangeTransferError> {
        if let Some(stage_started) = &self.stage_started {
            stage_started.notify_one();
            self.allow_stage
                .as_ref()
                .expect("blocked transfer has a release notification")
                .notified()
                .await;
        }
        if self.fail_before_stage {
            return Err(Self::error(
                request.target_range,
                "injected prepublish fault",
            ));
        }
        let target_kv = Arc::new(MemKv::new());
        for (key, value) in self
            .source
            .kv_handle()
            .scan_range(&[], &[u8::MAX])
            .map_err(|error| Self::error(request.target_range, &error.to_string()))?
        {
            target_kv
                .put(key, value)
                .map_err(|error| Self::error(request.target_range, &error.to_string()))?;
        }
        let target = SqlEngine::with_kv(target_kv)
            .map_err(|error| Self::error(request.target_range, &format!("{error:?}")))?;
        *self.staged.lock().expect("staged lock") = Some(target);
        Ok(StagedRangeSuccessor {
            range_id: request.target_range,
            bootstrap_checkpoint: CheckpointManifest {
                range_id: request.target_range,
                covered_offset: 1,
                manifest_key: "in-process-target".to_owned(),
            },
        })
    }

    async fn claim_staged_successor(
        &self,
        staged: &StagedRangeSuccessor,
        _barrier: RangeTransferBarrier,
    ) -> Result<ClaimedStagedSuccessor, RangeTransferError> {
        let engine = self
            .staged
            .lock()
            .expect("staged lock")
            .take()
            .ok_or_else(|| Self::error(staged.range_id, "successor was not staged"))?;
        Ok(ClaimedStagedSuccessor {
            engine,
            keepalive: Arc::new(()),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_populated_transfer_resumes_the_source_after_the_pause_barrier() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_cancelled_populated_transfer").expect("tenant"),
        "0,5,20",
    )
    .expect("config");
    let (gateway, _) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t10 (id int4)")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t10 VALUES (10)")
        .await
        .expect("insert");
    let source = gateway
        .hosted_range_engines()
        .remove(&RangeId::new(1))
        .expect("source engine");
    let (transfer, stage_started, allow_stage) = InProcessTransfer::blocked_after_pause(source);
    let transfer = Arc::new(transfer);

    let split_gateway = gateway.clone();
    let split_transfer = Arc::clone(&transfer);
    let split = tokio::spawn(async move {
        split_gateway
            .split_populated_table_successor(
                "cancel-t10",
                RangeId::new(3),
                TableId::new(10),
                split_transfer.as_ref(),
            )
            .await
    });
    stage_started.notified().await;
    split.abort();
    let _ = split.await;

    assert!(!transfer.paused.load(Ordering::Acquire));
    session
        .simple_query("INSERT INTO t10 VALUES (11)")
        .await
        .expect("cancelled transfer releases source writes");
    allow_stage.notify_one();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_in_the_successor_interval_waits_for_populated_transfer_publication() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_populated_transfer_ddl_gate").expect("tenant"),
        "0,5,20",
    )
    .expect("config");
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut setup = gateway.connect();
    setup
        .simple_query("CREATE TABLE t10 (id int4)")
        .await
        .expect("create transferred table");
    setup
        .simple_query("INSERT INTO t10 VALUES (10)")
        .await
        .expect("insert transferred row");
    let source = gateway
        .hosted_range_engines()
        .remove(&RangeId::new(1))
        .expect("source engine");
    let (transfer, stage_started, allow_stage) = InProcessTransfer::blocked_after_pause(source);
    let transfer = Arc::new(transfer);

    let split_gateway = gateway.clone();
    let split_transfer = Arc::clone(&transfer);
    let split = tokio::spawn(async move {
        split_gateway
            .split_populated_table_successor(
                "move-t10-with-ddl",
                RangeId::new(3),
                TableId::new(10),
                split_transfer.as_ref(),
            )
            .await
    });
    stage_started.notified().await;

    let ddl_gateway = gateway.clone();
    let mut ddl = tokio::spawn(async move {
        let mut session = ddl_gateway.connect();
        session.simple_query("CREATE TABLE t15 (id int4)").await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut ddl)
            .await
            .is_err(),
        "DDL must wait until the interval publication is complete"
    );
    allow_stage.notify_one();
    split
        .await
        .expect("split task")
        .expect("publish populated successor");
    let ddl_error = ddl
        .await
        .expect("DDL task")
        .expect_err("pre-publication session must reconnect after the map changes");
    assert_eq!(ddl_error.code, "0A000");

    assert_eq!(
        handles
            .range_map()
            .route_table(TableId::new(15))
            .expect("route newly created table")
            .range_id,
        RangeId::new(3)
    );
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t15 (id int4)")
        .await
        .expect("create table after publication with a current session");
    session
        .simple_query("INSERT INTO t15 VALUES (15)")
        .await
        .expect("new table is not stranded on its former interval");
}

#[tokio::test]
async fn populated_transfer_publishes_a_sql_ready_successor_and_releases_source_on_failure() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_populated_transfer").expect("tenant"),
        "0,5,20",
    )
    .expect("config");
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut old_session = gateway.connect();
    old_session
        .simple_query("CREATE TABLE t10 (id int4, v text)")
        .await
        .expect("create");
    old_session
        .simple_query("INSERT INTO t10 VALUES (10, 'first'), (11, 'second')")
        .await
        .expect("insert");
    old_session
        .simple_query("UPDATE t10 SET v = 'updated' WHERE id = 10")
        .await
        .expect("update");
    old_session
        .simple_query("DELETE FROM t10 WHERE id = 11")
        .await
        .expect("delete");

    let source = gateway
        .hosted_range_engines()
        .remove(&RangeId::new(1))
        .expect("source engine");
    let failed_transfer = InProcessTransfer::new(source.clone_handle(), true);
    let initial_map = handles.range_map();
    gateway
        .split_populated_table_successor(
            "fault-t10",
            RangeId::new(3),
            TableId::new(10),
            &failed_transfer,
        )
        .await
        .expect_err("fault before publication");
    assert!(!failed_transfer.paused.load(Ordering::Acquire));
    assert_eq!(handles.range_map(), initial_map);
    old_session
        .simple_query("SELECT id FROM t10")
        .await
        .expect("source remains serving after prepublish failure");

    let transfer = InProcessTransfer::new(source, false);
    gateway
        .split_populated_table_successor("move-t10", RangeId::new(3), TableId::new(10), &transfer)
        .await
        .expect("publish populated successor");
    assert!(!transfer.paused.load(Ordering::Acquire));
    assert_eq!(
        handles
            .range_map()
            .route_table(TableId::new(10))
            .expect("route")
            .range_id,
        RangeId::new(3)
    );
    let error = old_session
        .simple_query("SELECT id FROM t10")
        .await
        .expect_err("old epoch");
    assert_eq!(error.code, "0A000");

    let mut new_session = gateway.connect();
    let rows = new_session
        .simple_query("SELECT id, v FROM t10")
        .await
        .expect("successor read");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0].as_ref().expect("id").text, "10");
    assert_eq!(rows[0][1].as_ref().expect("value").text, "updated");
    new_session
        .simple_query("INSERT INTO t10 VALUES (12, 'target')")
        .await
        .expect("successor write");
    assert_eq!(
        handles.route_log().await.last().expect("route").range_id,
        RangeId::new(3)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_table_split_revalidates_after_a_concurrent_predecessor_insert() {
    let split_hook = EmptyTableSplitTestHook::new();
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_empty_table_split_race").expect("tenant"),
        "0,200",
    )
    .expect("config")
    .with_empty_table_split_test_hook(split_hook.clone());
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut setup = gateway.connect();
    setup
        .simple_query("CREATE TABLE t100 (id int4)")
        .await
        .expect("create empty moved table");

    let split_gateway = gateway.clone();
    let split = tokio::spawn(async move {
        split_gateway
            .split_empty_table_successor("empty-t100-race", RangeId::new(2), TableId::new(100))
            .await
    });
    split_hook.initial_validation_complete().await;

    let mut writer = gateway.connect();
    writer
        .simple_query("INSERT INTO t100 VALUES (9)")
        .await
        .expect("concurrent predecessor insert");
    split_hook.allow_final_validation();

    let split_error = split
        .await
        .expect("split task completes")
        .expect_err("split must reject the no-longer-empty table");
    let rejected_table = match split_error {
        crabka_gres_ranges::LocalSqlSplitError::NonEmptyTable(table_id)
        | crabka_gres_ranges::LocalSqlSplitError::AllocatedRowIds(table_id) => table_id,
        error => panic!("unexpected split error: {error}"),
    };
    assert_eq!(rejected_table, TableId::new(100));

    let rows = writer
        .simple_query("SELECT id FROM t100")
        .await
        .expect("row remains routed to predecessor");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0].as_ref().expect("cell").text, "9");
    assert!(handles.route_log().await.iter().any(|route| {
        route.kind == crabka_gres_ranges::StatementKind::Dml
            && route.range_id == RangeId::COORDINATOR
            && route.table_id == Some(TableId::new(100))
    }));
}

#[tokio::test]
async fn cross_range_single_statement_returns_feature_not_supported() {
    let gateway = start_gateway();
    let mut session = gateway.connect();

    let error = session
        .simple_query("SELECT * FROM t50 JOIN t150 ON t50.id = t150.id")
        .await
        .expect_err("unsupported");

    assert_eq!(error.code, "0A000");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_forwards_remote_autocommit_over_tcp() {
    let mut remote = crabka_pgexec::SqlEngine::new();
    let mut remote_session = remote.connect();
    remote_session
        .simple_query("CREATE TABLE t150 (id int4) SHARDED")
        .await
        .expect("create remote table");
    let fixture = MtlsFixture::new();
    let remote_address = spawn_tls(
        Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            remote.clone_handle(),
        )]))),
        fixture.server,
    )
    .await;
    let tenant_name = TenantName::parse("tenant_gateway_remote").expect("tenant");
    let record = TenantRecord::new(
        1,
        TenantId::try_from("tenant-gateway-remote").expect("tenant id"),
        crabka_gres_control::TenantName::try_from("tenant-gateway-remote").expect("record tenant"),
        TenantState::Active,
        SqlUser::try_from("alice").expect("user"),
        "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
        1,
    )
    .expect("record")
    .with_range_layout(vec![
        RangeLayoutEntry {
            range_id: 0,
            end_key: Some(crabka_gres_control::RangeBoundary::table_start(100)),
            endpoint: "unhosted-r0-is-local".to_string(),
            wal_generation: 1,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: remote_address.to_string(),
            wal_generation: 1,
        },
    ])
    .expect("layout");
    let config = MultiRangeTenantConfig::from_boundaries(tenant_name, "0,100")
        .expect("config")
        .with_hosted_ranges(vec![RangeId::COORDINATOR])
        .expect("host r0")
        .with_range_registry(RangeRegistry::from_tenant_record(&record).expect("registry"))
        .with_range_client(FramedTcpClient::with_tls(fixture.client).expect("mTLS range client"));
    let gateway = MultiRangeTenant::start(config).expect("gateway").0;
    gateway
        .hosted_range_engines()
        .get(&RangeId::COORDINATOR)
        .expect("gateway range 0")
        .share_gtm_to(&mut remote);
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4) SHARDED")
        .await
        .expect("create gateway catalog table");
    session
        .simple_query("INSERT INTO t150 VALUES (7)")
        .await
        .expect("forward autocommit insert");

    let rows = remote_session
        .simple_query("SELECT id FROM t150")
        .await
        .expect("read forwarded remote row");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0].as_ref().expect("cell").text, "7");
}

#[tokio::test]
async fn remote_extended_statement_participates_in_cross_range_commit() {
    use crabka_pgwire::engine::BoundParam;

    let mut remote = crabka_pgexec::SqlEngine::new();
    let mut remote_setup = remote.connect();
    remote_setup
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create owner table");
    let fixture = MtlsFixture::new();
    let record = TenantRecord::new(
        1,
        TenantId::try_from("tenant-gateway-explicit").expect("tenant id"),
        crabka_gres_control::TenantName::try_from("tenant-gateway-explicit")
            .expect("record tenant"),
        TenantState::Active,
        SqlUser::try_from("alice").expect("user"),
        "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
        1,
    )
    .expect("record")
    .with_range_layout(vec![
        RangeLayoutEntry {
            range_id: 0,
            end_key: Some(crabka_gres_control::RangeBoundary::table_start(100)),
            endpoint: "local".to_string(),
            wal_generation: 1,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "127.0.0.1:1".to_string(),
            wal_generation: 1,
        },
    ])
    .expect("layout");
    let registry = RangeRegistry::from_tenant_record(&record).expect("registry");
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_gateway_explicit").expect("tenant"),
        "0,100",
    )
    .expect("config")
    .with_hosted_ranges(vec![RangeId::COORDINATOR])
    .expect("host r0")
    .with_range_registry(registry.clone())
    .with_range_client(FramedTcpClient::with_tls(fixture.client).expect("mTLS range client"));
    let gateway = MultiRangeTenant::start(config).expect("gateway").0;
    gateway
        .hosted_range_engines()
        .get(&RangeId::COORDINATOR)
        .expect("gateway range 0")
        .share_gtm_to(&mut remote);
    remote.set_catalog_kv(
        gateway
            .hosted_range_engines()
            .get(&RangeId::COORDINATOR)
            .expect("gateway range 0")
            .kv_handle(),
    );
    let remote_address = spawn_tls(
        Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            remote.clone_handle(),
        )]))),
        fixture.server,
    )
    .await;
    let mut live_record = record.clone();
    live_record.ranges[1].endpoint = remote_address.to_string();
    registry
        .refresh_from_tenant_record(&live_record)
        .await
        .expect("publish live remote endpoint");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t50 (id int4)")
        .await
        .expect("create local table");
    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create remote table catalog");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t50 VALUES (1)")
        .await
        .expect("local participant is touched");

    session
        .extended_query_v2(
            "INSERT INTO t150 VALUES ($1)",
            &[BoundParam {
                type_oid: Some(23),
                format: 0,
                value: Some(Vec::from(b"2".as_slice()).into()),
            }],
        )
        .await
        .expect("remote extended participant executes");
    session
        .simple_query("COMMIT")
        .await
        .expect("cross-range commit");
    assert_eq!(session.tx_status(), crabka_pgwire::engine::TxStatus::Idle);
    let rows = session
        .simple_query("SELECT * FROM t50")
        .await
        .expect("local participant committed");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 1);
    let remote_rows = remote
        .connect()
        .simple_query("SELECT id FROM t150")
        .await
        .expect("remote participant committed");
    let [QueryResult::Rows { rows, .. }] = &remote_rows[..] else {
        panic!("expected remote rows");
    };
    assert_eq!(rows.len(), 1, "remote participant row must become visible");
    assert_eq!(rows[0][0].as_ref().expect("id").text, "2");
}
mod support;

use support::ExtendedQueryV2 as _;

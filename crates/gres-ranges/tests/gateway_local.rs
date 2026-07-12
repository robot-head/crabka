use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use crabka_gres_control::{RangeLayoutEntry, SqlUser, TenantId, TenantRecord, TenantState};
use crabka_gres_ranges::{
    CheckpointManifest, ClaimedStagedSuccessor, ClaimedStagedSuccessors, CommittedTailRecord,
    FramedTcpClient, GatewayCommitFault, HostedRangeService, LocalSqlSplitError, MultiRangeTenant,
    MultiRangeTenantConfig, RangeId, RangeKey, RangeRegistry, RangeRequest, RangeResponse,
    RangeService, RangeSpec, RangeTlsClientConfig, RangeTlsServerConfig, RangeTransferBarrier,
    RangeTransferCapability, RangeTransferError, SplitCommand, StagedRangeSuccessor,
    StagedRangeSuccessors, SuccessorDescriptor, TableId, TableTransferRequest, TenantName,
    serve_tls, tenant::EmptyTableSplitTestHook,
};
use crabka_pgexec::SqlEngine;
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

struct CountingTimestampService {
    inner: HostedRangeService,
    prewrites: AtomicUsize,
    resolves: AtomicUsize,
    recoveries: AtomicUsize,
}

#[async_trait]
impl RangeService for CountingTimestampService {
    async fn handle(&self, request: RangeRequest) -> RangeResponse {
        match &request {
            RangeRequest::TimestampPrewrite(_) => self.prewrites.fetch_add(1, Ordering::Relaxed),
            RangeRequest::TimestampResolve(_) => self.resolves.fetch_add(1, Ordering::Relaxed),
            RangeRequest::TimestampRecover(_) => self.recoveries.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
        self.inner.handle(request).await
    }
}

struct MtlsFixture {
    _dir: tempfile::TempDir,
    server: RangeTlsServerConfig,
    client: RangeTlsClientConfig,
}

fn split_fixture(
    gateway: &MultiRangeTenant,
    right_range: RangeId,
    table_id: TableId,
) -> (SplitCommand, [TableTransferRequest; 2]) {
    let current_map = gateway.control_range_map();
    let split_at = RangeKey::table_start(table_id);
    let predecessor_id = current_map
        .range_for_key(table_id, 0)
        .expect("split predecessor")
        .range_id;
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|range| range.range_id == predecessor_id)
        .expect("split predecessor interval")
        .clone();
    let left_range = if predecessor_id.is_coordinator() {
        predecessor_id
    } else {
        RangeId::new(
            current_map
                .ranges()
                .iter()
                .map(|range| range.range_id.as_u32())
                .chain(std::iter::once(right_range.as_u32()))
                .max()
                .expect("range id")
                + 1,
        )
    };
    let command = SplitCommand {
        current_map,
        predecessor: predecessor_id,
        predecessor_generation: 0,
        left: SuccessorDescriptor {
            range_id: left_range,
            endpoint: format!("left-r{left_range}.internal:7443"),
            wal_generation: 1,
            interval: RangeSpec::for_interval(left_range, predecessor.start, Some(split_at)),
        },
        right: SuccessorDescriptor {
            range_id: right_range,
            endpoint: format!("right-r{right_range}.internal:7443"),
            wal_generation: 1,
            interval: RangeSpec::for_interval(right_range, split_at, predecessor.end),
        },
    };
    let requests = [
        TableTransferRequest {
            target_range: left_range,
            routing_table_id: table_id,
            physical_table_id: 1,
        },
        TableTransferRequest {
            target_range: right_range,
            routing_table_id: table_id,
            physical_table_id: 1,
        },
    ];
    (command, requests)
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

async fn spawn_tls_with_handle(
    service: Arc<dyn RangeService>,
    config: RangeTlsServerConfig,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let address = listener.local_addr().expect("TLS listener address");
    let handle = tokio::spawn(async move {
        let _ = serve_tls(listener, service, config).await;
    });
    (address, handle)
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

    let (command, _) = split_fixture(&gateway, RangeId::new(2), TableId::new(100));
    gateway
        .split_empty_successors("empty-t100", command)
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

    let (command, _) = split_fixture(&gateway, RangeId::new(2), TableId::new(100));
    let error = gateway
        .split_empty_successors("populated-t100", command)
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
    staged: Mutex<BTreeMap<RangeId, SqlEngine>>,
    stage_started: Option<Arc<tokio::sync::Notify>>,
    allow_stage: Option<Arc<tokio::sync::Notify>>,
}

impl InProcessTransfer {
    fn new(source: SqlEngine, fail_before_stage: bool) -> Self {
        Self {
            source,
            paused: AtomicBool::new(false),
            fail_before_stage,
            staged: Mutex::new(BTreeMap::new()),
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
                staged: Mutex::new(BTreeMap::new()),
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

    async fn stage_range(
        &self,
        request: TableTransferRequest,
    ) -> Result<StagedRangeSuccessor, RangeTransferError> {
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
        self.staged
            .lock()
            .expect("staged lock")
            .insert(request.target_range, target);
        Ok(StagedRangeSuccessor {
            range_id: request.target_range,
            bootstrap_checkpoint: CheckpointManifest {
                range_id: request.target_range,
                covered_offset: 1,
                manifest_key: "in-process-target".to_owned(),
            },
        })
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

    async fn stage_successors(
        &self,
        requests: [TableTransferRequest; 2],
        _checkpoint: &CheckpointManifest,
        _tail: &[CommittedTailRecord],
        _barrier: RangeTransferBarrier,
    ) -> Result<StagedRangeSuccessors, RangeTransferError> {
        if let Some(stage_started) = &self.stage_started {
            stage_started.notify_one();
            self.allow_stage
                .as_ref()
                .expect("blocked transfer has a release notification")
                .notified()
                .await;
        }
        let [left, right] = requests;
        Ok(StagedRangeSuccessors {
            left: self.stage_range(left).await?,
            right: self.stage_range(right).await?,
        })
    }

    async fn claim_successors(
        &self,
        staged: &StagedRangeSuccessors,
        _barrier: RangeTransferBarrier,
    ) -> Result<ClaimedStagedSuccessors, RangeTransferError> {
        let mut engines = self.staged.lock().expect("staged lock");
        if !engines.contains_key(&staged.left.range_id)
            || !engines.contains_key(&staged.right.range_id)
        {
            return Err(Self::error(
                staged.left.range_id,
                "both successors must be staged",
            ));
        }
        let left = engines.remove(&staged.left.range_id).expect("checked left");
        let right = engines
            .remove(&staged.right.range_id)
            .expect("checked right");
        Ok(ClaimedStagedSuccessors {
            left: ClaimedStagedSuccessor {
                engine: left,
                keepalive: Arc::new(()),
            },
            right: ClaimedStagedSuccessor {
                engine: right,
                keepalive: Arc::new(()),
            },
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
    let (command, requests) = split_fixture(&gateway, RangeId::new(3), TableId::new(10));
    let split = tokio::spawn(async move {
        split_gateway
            .split_successors("cancel-t10", command, requests, split_transfer.as_ref())
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
    let (command, requests) = split_fixture(&gateway, RangeId::new(3), TableId::new(10));
    let split = tokio::spawn(async move {
        split_gateway
            .split_successors(
                "move-t10-with-ddl",
                command,
                requests,
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
    let (command, requests) = split_fixture(&gateway, RangeId::new(3), TableId::new(10));
    gateway
        .split_successors("fault-t10", command, requests, &failed_transfer)
        .await
        .expect_err("fault before publication");
    assert!(!failed_transfer.paused.load(Ordering::Acquire));
    assert_eq!(handles.range_map(), initial_map);
    old_session
        .simple_query("SELECT id FROM t10")
        .await
        .expect("source remains serving after prepublish failure");

    let transfer = InProcessTransfer::new(source, false);
    let (command, requests) = split_fixture(&gateway, RangeId::new(3), TableId::new(10));
    gateway
        .split_successors("move-t10", command, requests, &transfer)
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
    let (command, _) = split_fixture(&gateway, RangeId::new(2), TableId::new(100));
    let split = tokio::spawn(async move {
        split_gateway
            .split_empty_successors("empty-t100-race", command)
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
    let timestamp_oracle: Arc<dyn crabka_pgexec::TimestampOracle> =
        Arc::new(crabka_pgexec::timestamp_txn::LocalTimestampOracle::default());
    remote.set_timestamp_oracle(Arc::clone(&timestamp_oracle));
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
            lifecycle: Default::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: remote_address.to_string(),
            wal_generation: 1,
            lifecycle: Default::default(),
            retirement: None,
        },
    ])
    .expect("layout");
    let config = MultiRangeTenantConfig::from_boundaries(tenant_name, "0,100")
        .expect("config")
        .with_hosted_ranges(vec![RangeId::COORDINATOR])
        .expect("host r0")
        .with_range_registry(RangeRegistry::from_tenant_record(&record).expect("registry"))
        .with_range_client(FramedTcpClient::with_tls(fixture.client).expect("mTLS range client"));
    let gateway = MultiRangeTenant::start_with_engine_factory_and_timestamp_oracle(
        config,
        |_data_dir, _range_id| Ok(crabka_pgexec::SqlEngine::new()),
        Some(timestamp_oracle),
    )
    .expect("gateway")
    .0;
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
    let rows = session
        .simple_query("SELECT id FROM t150")
        .await
        .expect("read forwarded remote row");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0].as_ref().expect("cell").text, "7");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_remote_timestamp_commit_recovers_once_after_gateway_restart() {
    let local_dir = tempfile::tempdir().expect("local durable ranges");
    let remote_dir = tempfile::tempdir().expect("remote durable range");
    let mut remote = crabka_pgexec::SqlEngine::open(remote_dir.path()).expect("remote engine");
    let fixture = MtlsFixture::new();
    let record = TenantRecord::new(
        1,
        TenantId::try_from("tenant-remote-scatter").expect("tenant id"),
        crabka_gres_control::TenantName::try_from("tenant-remote-scatter").expect("record tenant"),
        TenantState::Active,
        SqlUser::try_from("alice").expect("user"),
        "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
        1,
    )
    .expect("record")
    .with_range_layout(vec![
        RangeLayoutEntry {
            range_id: 0,
            end_key: Some(crabka_gres_control::RangeBoundary::new(50, 0)),
            endpoint: "local-r0".into(),
            wal_generation: 1,
            lifecycle: Default::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: Some(crabka_gres_control::RangeBoundary::new(50, 10)),
            endpoint: "local-r1".into(),
            wal_generation: 1,
            lifecycle: Default::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 2,
            end_key: None,
            endpoint: "127.0.0.1:1".into(),
            wal_generation: 1,
            lifecycle: Default::default(),
            retirement: None,
        },
    ])
    .expect("layout");
    let registry = RangeRegistry::from_tenant_record(&record).expect("registry");
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_remote_scatter").expect("tenant"),
        "0,50:0,50:10",
    )
    .expect("config")
    .with_data_dir(local_dir.path().to_path_buf())
    .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(1)])
    .expect("host coordinator and first shard")
    .with_range_registry(registry.clone())
    .with_range_client(FramedTcpClient::with_tls(fixture.client).expect("mTLS range client"));
    let restart_config = config.clone();
    let (gateway, handles) = MultiRangeTenant::start(
        config.with_commit_fault_for_testing(GatewayCommitFault::AfterTimestampCommitDecision),
    )
    .expect("gateway");
    let hosted = gateway.hosted_range_engines();
    let coordinator = hosted.get(&RangeId::COORDINATOR).expect("r0");
    let (primary_address, primary_server) = spawn_tls_with_handle(
        Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            hosted[&RangeId::new(1)].clone_handle(),
        )]))),
        fixture.server.clone(),
    )
    .await;
    let mut live_record = record;
    live_record.ranges[1].endpoint = primary_address.to_string();
    let range_client = restart_config
        .range_client
        .clone()
        .expect("configured range client");
    remote.set_catalog_kv(coordinator.kv_handle());
    coordinator.share_gtm_to(&mut remote);
    remote.set_timestamp_oracle(coordinator.timestamp_oracle_handle());
    let service = Arc::new(CountingTimestampService {
        inner: HostedRangeService::new(BTreeMap::from([(RangeId::new(2), remote.clone_handle())]))
            .with_timestamp_primary_remote(registry.clone(), range_client.clone()),
        prewrites: AtomicUsize::new(0),
        resolves: AtomicUsize::new(0),
        recoveries: AtomicUsize::new(0),
    });
    let (address, server_task) =
        spawn_tls_with_handle(service.clone(), fixture.server.clone()).await;
    live_record.ranges[2].endpoint = address.to_string();
    registry
        .refresh_from_tenant_record(&live_record)
        .await
        .expect("publish endpoint");

    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t50 (id int4) SHARDED")
        .await
        .expect("create");
    let table = crabka_pgcatalog::list_tables(coordinator.catalog_kv())
        .expect("catalog")
        .into_iter()
        .find(|table| table.name == "t50")
        .expect("t50");
    assert!(
        table.sharding.is_none(),
        "row-range fixture must not use hash routing"
    );
    session
        .simple_query("INSERT INTO t50 VALUES (1),(11)")
        .await
        .expect_err("post-decision failure is ambiguous");
    assert!(service.prewrites.load(Ordering::Relaxed) > 0);
    assert_eq!(
        service.resolves.load(Ordering::Relaxed),
        1,
        "the remote primary is atomically resolved before the injected ambiguity"
    );
    assert_eq!(service.recoveries.load(Ordering::Relaxed), 0);
    let descriptor = hosted[&RangeId::new(1)]
        .timestamp_transaction_descriptors()
        .expect("first-write primary descriptor scan")
        .into_iter()
        .next()
        .expect("durable timestamp descriptor");
    drop(session);
    drop(gateway);
    drop(handles);
    drop(hosted);
    server_task.abort();
    let _ = server_task.await;
    drop(service);
    drop(remote);
    primary_server.abort();
    let _ = primary_server.await;

    let mut recovered_engines = BTreeMap::from([
        (
            RangeId::COORDINATOR,
            SqlEngine::open(local_dir.path().join("r0")).expect("reopen r0"),
        ),
        (
            RangeId::new(1),
            SqlEngine::open(local_dir.path().join("r1")).expect("reopen r1"),
        ),
    ]);
    let mut remote = SqlEngine::open(remote_dir.path()).expect("reopen remote compute");
    let recovered_coordinator = recovered_engines
        .get(&RangeId::COORDINATOR)
        .expect("recovered r0");
    let recovered_primary_address = spawn_tls(
        Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            recovered_engines[&RangeId::new(1)].clone_handle(),
        )]))),
        fixture.server.clone(),
    )
    .await;
    live_record.ranges[1].endpoint = recovered_primary_address.to_string();
    remote.set_catalog_kv(recovered_coordinator.kv_handle());
    let recovered_service = Arc::new(CountingTimestampService {
        inner: HostedRangeService::new(BTreeMap::from([(RangeId::new(2), remote.clone_handle())]))
            .with_timestamp_primary_remote(registry.clone(), range_client),
        prewrites: AtomicUsize::new(0),
        resolves: AtomicUsize::new(0),
        recoveries: AtomicUsize::new(0),
    });
    let (recovered_address, _recovered_server) =
        spawn_tls_with_handle(recovered_service.clone(), fixture.server).await;
    live_record.ranges[2].endpoint = recovered_address.to_string();
    registry
        .refresh_from_tenant_record(&live_record)
        .await
        .expect("publish restarted endpoint");
    let (restarted, _restart_handles) =
        MultiRangeTenant::start_with_engine_factory(restart_config, move |_dir, range_id| {
            Ok(recovered_engines
                .remove(&range_id)
                .expect("recovered engine"))
        })
        .expect("restart resolves durable remote decision");
    assert_eq!(
        recovered_service.resolves.load(Ordering::Relaxed),
        0,
        "normal resolve was never retried"
    );
    assert_eq!(
        recovered_service.recoveries.load(Ordering::Relaxed),
        1,
        "exactly one recovery RPC"
    );
    let restarted_hosted = restarted.hosted_range_engines();
    remote.set_timestamp_oracle(
        restarted_hosted
            .get(&RangeId::COORDINATOR)
            .expect("restarted r0")
            .timestamp_oracle_handle(),
    );
    let commit_ts = match descriptor.decision {
        crabka_pgexec::PrimaryTxnDecision::Committed(ts) => ts,
        other => panic!("expected committed descriptor, got {other:?}"),
    };
    for operation in &descriptor.operations {
        let kv = if operation.range_id == 2 {
            remote.kv_handle()
        } else {
            restarted_hosted
                .get(&RangeId::new(operation.range_id))
                .expect("reopened owner")
                .kv_handle()
        };
        let key = crabka_pgmvcc::version::version_key_ts(
            operation.table_id,
            operation.rowid,
            descriptor.start_ts.get(),
        );
        let bytes = kv
            .get(&key)
            .expect("read version")
            .expect("recovered version exists");
        let version = crabka_pgmvcc::version::decode_ts_tuple(&bytes).expect("decode version");
        assert_eq!(
            version.state,
            crabka_pgmvcc::version::TsVersionState::Committed {
                commit_ts: commit_ts.get()
            }
        );
    }
    let read_ts = restarted_hosted
        .get(&RangeId::new(1))
        .expect("r1")
        .allocate_timestamp_read_timestamp()
        .await
        .expect("read timestamp");
    assert!(
        read_ts.get() >= commit_ts.get(),
        "read_ts={} commit_ts={}",
        read_ts.get(),
        commit_ts.get()
    );
    let mut recovered_session = restarted.connect();
    let rows = recovered_session
        .simple_query("SELECT id FROM t50 ORDER BY id")
        .await
        .expect("recovered rows");
    assert!(
        matches!(&rows[..], [QueryResult::Rows { rows, .. }] if rows.len() == 2),
        "{rows:?}"
    );
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
            lifecycle: Default::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "127.0.0.1:1".to_string(),
            wal_generation: 1,
            lifecycle: Default::default(),
            retirement: None,
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

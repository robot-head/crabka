use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use crabka_gres_control::{RangeLayoutEntry, SqlUser, TenantId, TenantRecord, TenantState};
use crabka_gres_ranges::{
    FramedTcpClient, HostedRangeService, MultiRangeTenant, MultiRangeTenantConfig, RangeId,
    RangeRegistry, RangeService, RangeTlsClientConfig, RangeTlsServerConfig, TableId, TenantName,
    serve_tls, tenant::EmptyTableSplitTestHook,
};
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

    gateway
        .split_empty_table_successor("empty-t100", RangeId::new(2), TableId::new(100))
        .await
        .expect("empty table split");

    let old_session_error = old_session
        .simple_query("SELECT id FROM t100")
        .await
        .expect_err("old session must reconnect after map publication");
    assert_eq!(old_session_error.code, "0A000");

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
async fn remote_extended_statement_fails_explicit_transaction_until_rollback() {
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
            endpoint: "remote".to_string(),
            wal_generation: 1,
        },
    ])
    .expect("layout");
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_gateway_explicit").expect("tenant"),
        "0,100",
    )
    .expect("config")
    .with_hosted_ranges(vec![RangeId::COORDINATOR])
    .expect("host r0")
    .with_range_registry(RangeRegistry::from_tenant_record(&record).expect("registry"))
    .with_range_client(FramedTcpClient::with_tls(fixture.client).expect("mTLS range client"));
    let gateway = MultiRangeTenant::start(config).expect("gateway").0;
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

    let error = session
        .extended_query("INSERT INTO t150 VALUES ($1)", &[])
        .await
        .expect_err("remote extended operation is unsupported");
    assert_eq!(error.code, "0A000");
    assert_eq!(session.tx_status(), crabka_pgwire::engine::TxStatus::Failed);
    let error = session
        .simple_query("SELECT 1")
        .await
        .expect_err("failed transaction rejects statements");
    assert_eq!(error.code, "25P02");

    session.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(session.tx_status(), crabka_pgwire::engine::TxStatus::Idle);
    let rows = session
        .simple_query("SELECT * FROM t50")
        .await
        .expect("participant rollback clears local write");
    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert!(rows.is_empty());
}

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use clap::Parser as _;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_gres_ranges::{
    MoveRangeCommand, RangeId, RangeKey, RangeSpec, SplitCommand, SuccessorDescriptor, TableId,
};
use crabka_pgkv::Kv as _;
use crabka_pgwire::engine::{Engine as _, Session as _};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use tokio::net::TcpListener;

async fn broker_test_permit() -> tokio::sync::OwnedSemaphorePermit {
    const MAX_CONCURRENT_TEST_BROKERS: usize = 1;
    static GATE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    Arc::clone(
        GATE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_TEST_BROKERS))),
    )
    .acquire_owned()
    .await
    .expect("broker test gate should remain open")
}

fn fixture_password() -> String {
    std::process::id().to_string()
}

fn wrong_fixture_password() -> String {
    (!std::process::id()).to_string()
}

struct RangeMtlsFixture {
    _dir: tempfile::TempDir,
    server: crabka_gres_ranges::RangeTlsServerConfig,
    client: crabka_gres_ranges::RangeTlsClientConfig,
}

fn range_mtls_fixture() -> RangeMtlsFixture {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().expect("temporary certificate directory");
    let server_cert = write_range_fixture(&dir, "server-cert.pem", "dev_cert.pem");
    let server_key = write_range_fixture(&dir, "server-key.pem", "dev_key.pem");
    let client_ca = write_range_fixture(&dir, "client-ca.pem", "dev_client_ca.pem");
    let client_cert = write_range_fixture(&dir, "client-cert.pem", "dev_client_cert.pem");
    let client_key = write_range_fixture(&dir, "client-key.pem", "dev_client_key.pem");
    RangeMtlsFixture {
        _dir: dir,
        server: crabka_gres_ranges::RangeTlsServerConfig {
            tenant: "runtime-test".to_string(),
            tls: crabka_security::TlsConfig {
                cert_chain_path: server_cert.clone(),
                private_key_path: server_key,
                trust_roots_path: Some(server_cert.clone()),
                client_ca_path: Some(client_ca),
                client_auth: crabka_security::ClientAuthMode::Required,
            },
            range_rpc_principals: BTreeSet::from([
                "CN=test-client,OU=integration,O=crabka".to_string()
            ]),
            operator_control_principals: BTreeSet::from([
                "CN=test-client,OU=integration,O=crabka".to_string()
            ]),
        },
        client: crabka_gres_ranges::RangeTlsClientConfig {
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

fn write_range_fixture(dir: &tempfile::TempDir, name: &str, fixture: &str) -> PathBuf {
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

async fn spawn_range_tls(
    service: Arc<dyn crabka_gres_ranges::RangeService>,
    config: crabka_gres_ranges::RangeTlsServerConfig,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let address = listener.local_addr().expect("TLS listener address");
    tokio::spawn(async move {
        let _ = crabka_gres_ranges::serve_tls(listener, service, config).await;
    });
    address
}

struct FakeTenantConfigLoader {
    record: crabka_gres_control::TenantRecord,
}

#[async_trait::async_trait]
impl crabka_gres::TenantConfigLoader for FakeTenantConfigLoader {
    async fn load_tenant_config(
        &self,
        _bootstrap: &str,
        _tenant: &crabka_gres_control::TenantName,
        _security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> std::io::Result<Option<crabka_gres_control::TenantRecord>> {
        Ok(Some(self.record.clone()))
    }
}

fn tenant_record() -> crabka_gres_control::TenantRecord {
    let verifier = crabka_security::scram::PgScramVerifier::generate_with_salt(
        &fixture_password(),
        8192,
        vec![3; 16],
    )
    .expect("verifier");
    crabka_gres_control::TenantRecord::new(
        1,
        crabka_gres_control::TenantId::try_from("runtime-test").expect("tenant id"),
        crabka_gres_control::TenantName::try_from("runtime-test").expect("tenant name"),
        crabka_gres_control::TenantState::Active,
        crabka_gres_control::SqlUser::try_from("alice").expect("sql user"),
        verifier.to_string(),
        1,
    )
    .expect("tenant record")
}

#[test]
fn binary_help_exposes_only_single_node_serve_surface() {
    use assert2::assert;

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crabka-gres"))
        .arg("--help")
        .output()
        .expect("run crabka-gres --help");

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help is utf8");
    assert!(help.contains("--listen"));
    assert!(help.contains("--data-dir"));
    assert!(help.contains("--substrate-bootstrap"));
    assert!(help.contains("--tenant"));
    assert!(help.contains("--cache-dir"));
    assert!(help.contains("--ranges"));
    assert!(help.contains("--host-ranges"));
    assert!(help.contains("--timestamp-source"));
    assert!(help.contains("--hlc-max-offset-ms"));
    assert!(help.contains("--hlc-wall-offset-ms"));
    assert!(help.contains("--range-listen"));
    assert!(help.contains("--checkpoint-bucket"));
    assert!(help.contains("--checkpoint-store"));
    assert!(help.contains("--checkpoint-frames"));
    assert!(help.contains("--checkpoint-bytes"));
    assert!(help.contains("--auth"));
    assert!(help.contains("--tls-cert"));
    assert!(!help.contains("node"));
    assert!(!help.contains("--node-addr"));
    assert!(!help.contains("--sql-addr"));
    assert!(!help.contains("--peer"));
}

fn test_args(listen: String, data_dir: Option<std::path::PathBuf>) -> crabka_gres::ServeArgs {
    crabka_gres::ServeArgs {
        registry: crabka_gres::Cli::parse_from(["crabka-gres"]).serve.registry,
        listen,
        tls_cert: None,
        tls_key: None,
        auth: Some("trust".to_string()),
        user_creds: Vec::new(),
        data_dir,
        substrate_bootstrap: None,
        tenant: None,
        cache_dir: None,
        ranges: None,
        host_ranges: None,
        timestamp_source: crabka_gres::TimestampSourceKind::LogicalTso,
        hlc_max_offset_ms: 250,
        hlc_wall_offset_ms: 0,
        range_listen: None,
        range_tls_cert: None,
        range_tls_key: None,
        range_tls_ca: None,
        range_tls_server_name: None,
        range_allowed_principals: Vec::new(),
        operator_control_principals: Vec::new(),
        checkpoint_store: None,
        checkpoint_bucket: None,
        checkpoint_prefix: None,
        checkpoint_local_root: None,
        checkpoint_region: None,
        checkpoint_endpoint: None,
        checkpoint_access_key_id: None,
        checkpoint_secret_access_key: None,
        checkpoint_allow_http: false,
        checkpoint_gcs_service_account_path: None,
        checkpoint_gcs_service_account_key: None,
        checkpoint_gcs_application_credentials_path: None,
        checkpoint_frames: None,
        checkpoint_bytes: None,
        checkpoint_part_bytes: None,
        checkpoint_retain: None,
    }
}

fn substrate_test_args(listen: String) -> crabka_gres::ServeArgs {
    crabka_gres::ServeArgs {
        substrate_bootstrap: Some("memory://".to_string()),
        tenant: Some("runtime-test".to_string()),
        ..test_args(listen, None)
    }
}

fn checkpoint_substrate_test_args(listen: String) -> crabka_gres::ServeArgs {
    crabka_gres::ServeArgs {
        checkpoint_store: Some(crabka_gres::CheckpointStoreKind::InMemory),
        checkpoint_frames: Some(std::num::NonZeroU64::new(1).expect("nonzero")),
        ..substrate_test_args(listen)
    }
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let config = format!("host=127.0.0.1 port={port} user=postgres");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok((client, connection)) =
            tokio_postgres::connect(&config, tokio_postgres::NoTls).await
        {
            tokio::spawn(connection);
            return client;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "connect to crabka-gres did not succeed within 5s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn connect_with_password(port: u16, user: &str, password: &str) -> tokio_postgres::Client {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok((client, connection)) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(port)
            .user(user)
            .password(password)
            .connect(tokio_postgres::NoTls)
            .await
        {
            tokio::spawn(connection);
            return client;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "connect to crabka-gres did not succeed within 5s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn produce_raw_fixture(bootstrap: &str, topic: &str, payload: &'static [u8]) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("gres-g6-runtime-admin")
        .build()
        .await
        .expect("admin client");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create raw fixture topic");
    assert_eq!(response.topics[0].error_code, 0);
    client.close();

    let producer = Producer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("gres-g6-runtime-producer")
        .acks(Acks::All)
        .build()
        .await
        .expect("fixture producer");
    let ack = producer
        .send(ProducerRecord {
            topic: topic.to_string(),
            partition: Some(0),
            value: Some(bytes::Bytes::from_static(payload)),
            ..Default::default()
        })
        .await;
    producer.flush().await.expect("fixture flush");
    assert_eq!(
        ack.await
            .expect("fixture ack channel")
            .expect("fixture produce")
            .offset,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_multirange_substrate_default_fdw_server_reads_own_broker() {
    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    produce_raw_fixture(&bootstrap, "g6-runtime-events", b"substrate-fdw").await;

    let mut runtime = crabka_gres::open_substrate_runtime(&crabka_gres::SubstrateRuntimeConfig {
        bootstrap: bootstrap.clone(),
        tenant: "g6-runtime".to_string(),
        cache_dir: None,
        checkpoints: None,
        kafka_security: None,
        ranges: Some("0,5".to_string()),
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: None,
        timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
        hlc_wall_offset_ms: 0,
        registry_policy: crabka_gres_control::RegistryPolicy::default(),
    })
    .await
    .expect("open live multi-range substrate runtime");
    crabka_gres::register_kafka_scanner_with_default_bootstrap(
        &mut runtime.engine,
        Some(bootstrap),
    );
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE SERVER own_cluster FOREIGN DATA WRAPPER crabka_gres_fdw")
        .await
        .expect("create zero-config own-cluster server");
    session
        .simple_query(
            "CREATE FOREIGN TABLE g6_runtime_events (value bytea) SERVER own_cluster OPTIONS (topic 'g6-runtime-events', value_format 'raw')",
        )
        .await
        .expect("create raw foreign table");
    let results = session
        .simple_query("SELECT value FROM g6_runtime_events ORDER BY _offset")
        .await
        .expect("select through substrate default server");
    let [crabka_pgwire::engine::QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected one row result, got {results:?}");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_ref().expect("value").text,
        b"\\x7375627374726174652d666477"[..]
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_multirange_substrate_hlc_mode_commits_and_mints_wall_anchored_stamps() {
    use assert2::assert;
    use crabka_pgexec::WallClock as _;

    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();

    let before_ms = crabka_pgexec::SystemWallClock.now_ms();
    let runtime = crabka_gres::open_substrate_runtime(&crabka_gres::SubstrateRuntimeConfig {
        bootstrap,
        tenant: "g6-hlc-runtime".to_string(),
        cache_dir: None,
        checkpoints: None,
        kafka_security: None,
        ranges: Some("0,5".to_string()),
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: None,
        timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::Hlc { max_offset_ms: 500 },
        hlc_wall_offset_ms: 0,
        registry_policy: crabka_gres_control::RegistryPolicy::default(),
    })
    .await
    .expect("open live multi-range substrate runtime in HLC mode");

    // Writes commit end-to-end under the HLC-backed range-0 grant oracle.
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE hlc_live (id int4)")
        .await
        .expect("create table under hlc mode");
    session
        .simple_query("INSERT INTO hlc_live VALUES (7)")
        .await
        .expect("insert under hlc mode");
    let results = session
        .simple_query("SELECT id FROM hlc_live")
        .await
        .expect("select under hlc mode");
    let [crabka_pgwire::engine::QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected one row result, got {results:?}");
    };
    assert!(rows.len() == 1);

    // The tenant's installed timestamp source mints wall-anchored packed
    // stamps: their physical component reaches the wall reading taken before
    // boot. A logical oracle's small dense integers would unpack with a
    // physical component of zero, so this can only pass when HLC genuinely
    // engaged on the live boot path.
    let crabka_gres::RuntimeEngine::Multi(gateway) = &runtime.engine else {
        panic!("live multi-range runtime must expose the gateway");
    };
    let source = gateway.hosted_range_engines()[&RangeId::COORDINATOR].timestamp_oracle_handle();
    let minted = source
        .allocate_read_timestamp()
        .await
        .expect("allocate through the live tenant timestamp source")
        .get();
    assert!(crabka_pgexec::hlc::unpack(minted).physical_ms >= before_ms);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_constructs_substrate_mode_over_in_process_wal() {
    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let loader = FakeTenantConfigLoader {
        record: tenant_record(),
    };
    let server = tokio::spawn(async move {
        crabka_gres::serve_listener_with_tenant_config_loader(
            listener,
            substrate_test_args(format!("127.0.0.1:{port}")),
            &loader,
        )
        .await
    });

    let client = connect(port).await;
    client
        .simple_query("CREATE TABLE substrate_runtime (id int4)")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO substrate_runtime VALUES (9)")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT id FROM substrate_runtime")
        .await
        .expect("select");
    let first_value = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(first_value, Some("9"));

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_constructs_checkpoint_enabled_substrate_mode() {
    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let loader = FakeTenantConfigLoader {
        record: tenant_record(),
    };
    let server = tokio::spawn(async move {
        crabka_gres::serve_listener_with_tenant_config_loader(
            listener,
            checkpoint_substrate_test_args(format!("127.0.0.1:{port}")),
            &loader,
        )
        .await
    });

    let client = connect(port).await;
    client
        .simple_query("CREATE TABLE checkpoint_runtime (id int4)")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO checkpoint_runtime VALUES (11)")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT id FROM checkpoint_runtime")
        .await
        .expect("select");
    let first_value = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(first_value, Some("11"));

    server.abort();
    let _ = server.await;
}

#[allow(
    clippy::too_many_lines,
    reason = "the transfer integration test keeps its checkpoint-to-tail lifecycle visible"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_multirange_transfer_stages_populated_successor_without_publishing_it() {
    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
    let tenant = "runtime-transfer";
    let runtime = crabka_gres::open_substrate_runtime(&crabka_gres::SubstrateRuntimeConfig {
        bootstrap: broker.listen_addr().to_string(),
        tenant: tenant.to_string(),
        cache_dir: None,
        checkpoints: Some(crabka_gres::CheckpointRuntimeConfig {
            object_store: crabka_gres::CheckpointObjectStoreConfig::Local {
                root: checkpoint_dir.path().to_path_buf(),
            },
            frames_threshold: 1,
            bytes_threshold: 1,
            part_max_bytes: crabka_gres_substrate::DEFAULT_PART_MAX_BYTES,
            retain_newest: 2,
        }),
        kafka_security: None,
        ranges: Some("0,5".to_string()),
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: Some("127.0.0.1:7443".into()),
        timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
        hlc_wall_offset_ms: 0,
        registry_policy: crabka_gres_control::RegistryPolicy::default(),
    })
    .await
    .expect("open live multi-range runtime");
    let control_status =
        crabka_gres_ranges::RangeRequest::Control(crabka_gres_ranges::transport::RangeControlReq {
            tenant: tenant.into(),
            range_id: RangeId::COORDINATOR,
            generation: 0,
            operation_id: "control-status-attachment".into(),
            operation: crabka_gres_ranges::transport::RangeControlOperation::Status,
        });
    for _ in 0..2 {
        let response = runtime.handle_range_request(control_status.clone()).await;
        assert!(
            matches!(
                response,
                Some(crabka_gres_ranges::RangeResponse::Control(
                    crabka_gres_ranges::transport::RangeControlResp::Rejected { ref code, .. }
            )) if code == "intent_authority"
            ),
            "unexpected unjournaled status response: {response:?}"
        );
    }
    let transfer = runtime
        .range_transfer_capability()
        .expect("live multi-range transfer capability");
    let before_tso = match runtime
        .handle_range_request(crabka_gres_ranges::RangeRequest::Tso(
            crabka_gres_ranges::TsoReq::Grant { count: 2 },
        ))
        .await
        .expect("range service")
    {
        crabka_gres_ranges::RangeResponse::Tso(crabka_gres_ranges::TsoResp::Granted {
            first_ts,
            count: 2,
        }) => first_ts,
        response => panic!("unexpected pre-split TSO response: {response:?}"),
    };
    let range = RangeId::COORDINATOR;
    let target_range = RangeId::new(2);
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE t1 (id int4)")
        .await
        .expect("create transfer source");
    session
        .simple_query("CREATE TABLE transfer_unrelated (id int4)")
        .await
        .expect("create unrelated source table");
    let source_catalog = kv_from_pairs(
        runtime
            .inspect_hosted_range_kv(range)
            .expect("inspect source catalog"),
    );
    let table_id = crabka_pgcatalog::get_table(
        &source_catalog,
        &crabka_pgcatalog::RelationName::public("t1"),
    )
    .expect("source relation")
    .id;
    let unrelated_table_id = crabka_pgcatalog::get_table(
        &source_catalog,
        &crabka_pgcatalog::RelationName::public("transfer_unrelated"),
    )
    .expect("unrelated relation")
    .id;
    assert_ne!(table_id, unrelated_table_id);

    for sql in [
        "INSERT INTO t1 VALUES (7)",
        "UPDATE t1 SET id = 8 WHERE id = 7",
        "DELETE FROM t1 WHERE id = 8",
        "INSERT INTO transfer_unrelated VALUES (99)",
    ] {
        session
            .simple_query(sql)
            .await
            .expect("write source MVCC state");
    }
    let _checkpoint_source = runtime
        .inspect_hosted_range_kv(range)
        .expect("inspect source checkpoint state");

    let manifest = transfer
        .force_checkpoint(range)
        .await
        .expect("force checkpoint");
    assert_eq!(manifest.range_id, range);
    assert!(
        manifest
            .manifest_key
            .starts_with(&format!("gres/{tenant}/r0/ckpt/"))
    );
    session
        .simple_query("INSERT INTO t1 VALUES (9)")
        .await
        .expect("write transfer tail before pause");
    session
        .simple_query("UPDATE t1 SET id = 10 WHERE id = 9")
        .await
        .expect("write transfer update tail before pause");
    let _source_with_tail = runtime
        .inspect_hosted_range_kv(range)
        .expect("inspect source tail state");
    let barrier = transfer
        .pause_at_checkpoint(&manifest)
        .await
        .expect("pause and barrier");
    let tail = transfer
        .read_committed_tail(range, manifest.covered_offset, barrier)
        .await
        .expect("read bounded committed tail");

    assert!(!tail.is_empty());
    assert!(tail.iter().all(|record| {
        record.offset > manifest.covered_offset && record.offset <= barrier.offset
    }));
    assert_eq!(
        tail.last().map(|record| record.offset),
        Some(barrier.offset)
    );
    assert!(tail.iter().any(|record| record.offset < barrier.offset));

    session
        .simple_query("INSERT INTO t1 VALUES (9)")
        .await
        .expect_err("write while paused must be rejected");

    let split_at = RangeKey::table_start(TableId::new(u64::from(table_id)));
    let current_map = runtime.published_range_map().expect("source map");
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|spec| spec.range_id == RangeId::COORDINATOR)
        .expect("r0 interval")
        .clone();
    let command = SplitCommand {
        current_map,
        predecessor: RangeId::COORDINATOR,
        predecessor_generation: 0,
        left: SuccessorDescriptor {
            range_id: RangeId::COORDINATOR,
            endpoint: "127.0.0.1:7443".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(RangeId::COORDINATOR, RangeKey::MIN, Some(split_at)),
        },
        right: SuccessorDescriptor {
            range_id: target_range,
            endpoint: "127.0.0.1:7443".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(target_range, split_at, predecessor.end),
        },
    };
    transfer.resume(barrier).await.expect("resume writer");
    session
        .simple_query("INSERT INTO t1 VALUES (10)")
        .await
        .expect("write after resume");
    let original_map = runtime.published_range_map().expect("original serving map");
    for (index, fault) in [
        crabka_gres::PrepareTopologyFault::LockAcquisition,
        crabka_gres::PrepareTopologyFault::HorizonLoad,
        crabka_gres::PrepareTopologyFault::TsoConstruction,
        crabka_gres::PrepareTopologyFault::ServiceAssembly,
    ]
    .into_iter()
    .enumerate()
    {
        runtime.inject_prepare_topology_fault(fault);
        runtime
            .split_successors(format!("r0-prepare-fault-{index}"), command.clone())
            .await
            .expect_err("precommit preparation fault must abort publication");
        assert_eq!(runtime.published_range_map(), Some(original_map.clone()));
        let resumed_write = format!("INSERT INTO t1 VALUES ({})", 100 + index);
        session
            .simple_query(&resumed_write)
            .await
            .expect("predecessor writer resumes after preparation failure");
        assert!(matches!(
            runtime
                .handle_range_request(crabka_gres_ranges::RangeRequest::Tso(
                    crabka_gres_ranges::TsoReq::Grant { count: 1 },
                ))
                .await,
            Some(crabka_gres_ranges::RangeResponse::Tso(_))
        ));
    }
    runtime
        .split_successors("r0-live-transfer", command)
        .await
        .expect("publish replacement r0 and right successor");
    session
        .simple_query("INSERT INTO t1 VALUES (11)")
        .await
        .expect_err("pre-split session remains fenced on the retired r0 writer");
    let after_tso = match runtime
        .handle_range_request(crabka_gres_ranges::RangeRequest::Tso(
            crabka_gres_ranges::TsoReq::Grant { count: 2 },
        ))
        .await
        .expect("replacement range service")
    {
        crabka_gres_ranges::RangeResponse::Tso(crabka_gres_ranges::TsoResp::Granted {
            first_ts,
            count: 2,
        }) => first_ts,
        response => panic!("unexpected post-split TSO response: {response:?}"),
    };
    assert!(
        after_tso > before_tso,
        "replacement r0 TSO must advance monotonically"
    );
    runtime
        .verify_current_range0_receipt_store()
        .await
        .expect("replacement r0 durable receipt write/read/reopen");
    let mut post_split = runtime.engine.connect();
    let replacement_checkpoint = transfer
        .force_checkpoint(RangeId::COORDINATOR)
        .await
        .expect("checkpoint replacement r0");
    post_split
        .simple_query("CREATE TABLE after_r0_split (id int4)")
        .await
        .expect("catalog DDL through replacement r0");
    let replacement_barrier = transfer
        .pause_at_checkpoint(&replacement_checkpoint)
        .await
        .expect("pause replacement r0");
    let replacement_tail = transfer
        .read_committed_tail(
            RangeId::COORDINATOR,
            replacement_checkpoint.covered_offset,
            replacement_barrier,
        )
        .await
        .expect("read replacement r0 bounded tail");
    assert_eq!(
        replacement_tail.last().map(|record| record.offset),
        Some(replacement_barrier.offset),
        "replacement r0 tail reaches its barrier"
    );
    transfer
        .resume(replacement_barrier)
        .await
        .expect("resume replacement r0");
    post_split
        .simple_query("SELECT * FROM t1")
        .await
        .expect("catalog read and routed table read after r0 replacement");
    post_split
        .simple_query("CREATE TABLE t2 (id int4)")
        .await
        .expect("create second-split table");
    post_split
        .simple_query("INSERT INTO t2 VALUES (20)")
        .await
        .expect("populate second-split table");
    let current_map = runtime.published_range_map().expect("post-r0 map");
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|spec| spec.range_id == target_range)
        .expect("first successor interval")
        .clone();
    let second_split_at = RangeKey::table_start(TableId::new(2));
    runtime
        .split_successors(
            "split-first-successor",
            SplitCommand {
                current_map,
                predecessor: target_range,
                predecessor_generation: 1,
                left: SuccessorDescriptor {
                    range_id: RangeId::new(3),
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 2,
                    interval: RangeSpec::for_interval(
                        RangeId::new(3),
                        predecessor.start,
                        Some(second_split_at),
                    ),
                },
                right: SuccessorDescriptor {
                    range_id: RangeId::new(4),
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 2,
                    interval: RangeSpec::for_interval(
                        RangeId::new(4),
                        second_split_at,
                        predecessor.end,
                    ),
                },
            },
        )
        .await
        .expect("second split targets the first split successor");
    let mut after_second = runtime.engine.connect();
    after_second
        .simple_query("BEGIN")
        .await
        .expect("begin ordinary cross-range transaction");
    after_second
        .simple_query("INSERT INTO t1 VALUES (30)")
        .await
        .expect("write first GTM participant");
    after_second
        .simple_query("INSERT INTO t2 VALUES (40)")
        .await
        .expect("write second GTM participant");
    after_second
        .simple_query("COMMIT")
        .await
        .expect("replacement r0 records ordinary cross-range decision");
    after_second
        .simple_query("SELECT * FROM t2")
        .await
        .expect("second successor serves its restored table");
}

fn kv_from_pairs(pairs: crabka_pgkv::KvScan) -> crabka_pgkv::MemKv {
    let kv = crabka_pgkv::MemKv::default();
    for (key, value) in pairs {
        kv.put(key, value).expect("copy raw KV pair");
    }
    kv
}

fn activation_crash_config(
    bootstrap: String,
    tenant: String,
    checkpoint_root: PathBuf,
) -> crabka_gres::SubstrateRuntimeConfig {
    crabka_gres::SubstrateRuntimeConfig {
        bootstrap,
        tenant,
        cache_dir: None,
        checkpoints: Some(crabka_gres::CheckpointRuntimeConfig {
            object_store: crabka_gres::CheckpointObjectStoreConfig::Local {
                root: checkpoint_root,
            },
            // These tests force every checkpoint they need through the control
            // protocol. Leave the periodic thresholds at the runtime defaults:
            // a per-frame threshold makes the background poll trim the WAL in
            // the window between a forced checkpoint and the transfer pause,
            // which then fails the successor's bounded tail read.
            frames_threshold: 10_000,
            bytes_threshold: 64 * 1024 * 1024,
            part_max_bytes: crabka_gres_substrate::DEFAULT_PART_MAX_BYTES,
            retain_newest: 16,
        }),
        kafka_security: None,
        ranges: Some("0,5".to_owned()),
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: Some("127.0.0.1:7443".into()),
        timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
        hlc_wall_offset_ms: 0,
        registry_policy: crabka_gres_control::RegistryPolicy::default(),
    }
}

async fn control_request(
    runtime: &crabka_gres::GresRuntime,
    tenant: &str,
    operation_id: &str,
    mutation: &crabka_gres_ranges::SplitState,
    operation: crabka_gres_ranges::transport::RangeControlOperation,
) -> crabka_gres_ranges::transport::RangeControlResp {
    match runtime
        .handle_range_request(crabka_gres_ranges::RangeRequest::Control(
            crabka_gres_ranges::transport::RangeControlReq {
                tenant: tenant.into(),
                range_id: mutation.predecessor,
                generation: mutation.predecessor_generation,
                operation_id: operation_id.into(),
                operation,
            },
        ))
        .await
    {
        Some(crabka_gres_ranges::RangeResponse::Control(response)) => response,
        response => panic!("unexpected range-control response: {response:?}"),
    }
}

fn control_layout_entry(descriptor: &SuccessorDescriptor) -> crabka_gres_control::RangeLayoutEntry {
    crabka_gres_control::RangeLayoutEntry {
        range_id: descriptor.range_id.as_u32(),
        end_key: descriptor
            .interval
            .end
            .map(|end| crabka_gres_control::RangeBoundary {
                table_id: end.table_id.as_u64(),
                bucket: None,
                rowid: end.rowid,
            }),
        endpoint: descriptor.endpoint.clone(),
        wal_generation: descriptor.wal_generation,
        lifecycle: crabka_gres_control::RangeLifecycle::Serving,
        retirement: None,
    }
}

async fn seed_control_operation(
    bootstrap: &str,
    tenant: &str,
    split: &crabka_gres_ranges::SplitState,
) {
    let verifier = crabka_security::scram::PgScramVerifier::generate_with_salt(
        &fixture_password(),
        8192,
        vec![7; 16],
    )
    .unwrap();
    let mut record = crabka_gres_control::TenantRecord::new(
        1,
        crabka_gres_control::TenantId::try_from(tenant).unwrap(),
        crabka_gres_control::TenantName::try_from(tenant).unwrap(),
        crabka_gres_control::TenantState::Active,
        crabka_gres_control::SqlUser::try_from("operator").unwrap(),
        verifier.to_string(),
        1,
    )
    .unwrap();
    record = record
        .with_range_layout(
            split
                .current_map
                .ranges()
                .iter()
                .map(|range| crabka_gres_control::RangeLayoutEntry {
                    range_id: range.range_id.as_u32(),
                    end_key: range.end.map(|end| crabka_gres_control::RangeBoundary {
                        table_id: end.table_id.as_u64(),
                        bucket: None,
                        rowid: end.rowid,
                    }),
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: split.predecessor_generation,
                    lifecycle: crabka_gres_control::RangeLifecycle::Serving,
                    retirement: None,
                })
                .collect(),
        )
        .unwrap();
    record.record_version = 1;
    let mut target_layout = record.ranges.clone();
    let source_index = target_layout
        .iter()
        .position(|range| range.range_id == split.predecessor.as_u32())
        .unwrap();
    let operation = if let Some(right) = &split.right {
        let split_intent = crabka_gres_control::RangeLayoutSplit {
            source_range_id: split.predecessor.as_u32(),
            predecessor_generation: split.predecessor_generation,
            left: control_layout_entry(&split.left),
            right: control_layout_entry(right),
        };
        target_layout.splice(
            source_index..=source_index,
            [split_intent.left.clone(), split_intent.right.clone()],
        );
        crabka_gres_control::SplitOperationRecord::new(
            crabka_gres_control::TenantName::try_from(tenant).unwrap(),
            &split.operation_id,
            split_intent,
        )
        .unwrap()
    } else {
        let replacement = control_layout_entry(&split.left);
        target_layout[source_index] = replacement.clone();
        crabka_gres_control::SplitOperationRecord::new_move(
            crabka_gres_control::TenantName::try_from(tenant).unwrap(),
            &split.operation_id,
            split.predecessor.as_u32(),
            split.predecessor_generation,
            replacement,
        )
        .unwrap()
    }
    .with_plan(crabka_gres_control::SplitOperationPlan {
        source_record_version: record.record_version,
        source_map_epoch: split.current_map.epoch().as_u64(),
        routing_table_id: 1,
        current_layout: record.ranges.clone(),
        target_layout,
    })
    .unwrap();
    let mut registry = crabka_gres_control::Registry::connect(bootstrap)
        .await
        .unwrap();
    registry.ensure_topic().await.unwrap();
    registry.replace_if_version(&record, None).await.unwrap();
    let operation = registry.begin_split_operation(&operation).await.unwrap();
    if operation.phase == crabka_gres_control::SplitOperationPhase::Initiated {
        let running = operation
            .advance(crabka_gres_control::SplitOperationPhase::Running, 1, None)
            .unwrap();
        registry
            .compare_and_swap_split_operation(Some(operation.revision), &running)
            .await
            .unwrap();
    }
}

async fn advance_control_operation(
    bootstrap: &str,
    tenant: &str,
    operation_id: &str,
    phase: crabka_gres_control::SplitOperationPhase,
    evidence: Option<crabka_gres_control::SplitOperationEvidence>,
) {
    let mut registry = crabka_gres_control::Registry::connect(bootstrap)
        .await
        .unwrap();
    registry.ensure_topic().await.unwrap();
    let current = registry
        .load_split_operation(tenant, operation_id)
        .await
        .unwrap()
        .expect("seeded control operation");
    if current.phase == phase {
        return;
    }
    let next = current
        .advance_with_evidence(
            phase,
            current.attempts,
            None,
            evidence.unwrap_or_else(|| current.evidence.clone()),
        )
        .unwrap();
    registry
        .compare_and_swap_split_operation(Some(current.revision), &next)
        .await
        .unwrap();
}

async fn control_binding(bootstrap: &str, tenant: &str, operation_id: &str) -> (u64, String) {
    let mut registry = crabka_gres_control::Registry::connect(bootstrap)
        .await
        .unwrap();
    let record = registry
        .load_split_operation(tenant, operation_id)
        .await
        .unwrap()
        .unwrap();
    let revision = record.revision;
    let digest = crabka_gres_ranges::control::AuthorizedSplitIntent::from_record(record)
        .unwrap()
        .digest()
        .to_string();
    (revision, digest)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_authority_allows_exact_target_status_at_activated_before_layout_cutover() {
    use crabka_gres_ranges::{
        control::IntentAuthorizationContext,
        transport::{RangeControlOperation, RangeControlReq},
    };

    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let tenant = "authority-activated";
    let map = crabka_gres_ranges::RangeMap::new(
        crabka_gres_ranges::TenantName::parse(tenant).unwrap(),
        crabka_gres_ranges::MapEpoch::new(0),
        vec![RangeSpec::for_interval(
            RangeId::COORDINATOR,
            RangeKey::MIN,
            None,
        )],
    )
    .unwrap();
    let split_at = RangeKey::new(TableId::new(7), 50);
    let split = crabka_gres_ranges::SplitState::for_split(
        "authority-op",
        SplitCommand {
            current_map: map,
            predecessor: RangeId::COORDINATOR,
            predecessor_generation: 0,
            left: SuccessorDescriptor {
                range_id: RangeId::COORDINATOR,
                endpoint: "left:7443".into(),
                wal_generation: 1,
                interval: RangeSpec::for_interval(
                    RangeId::COORDINATOR,
                    RangeKey::MIN,
                    Some(split_at),
                ),
            },
            right: SuccessorDescriptor {
                range_id: RangeId::new(1),
                endpoint: "right:7443".into(),
                wal_generation: 1,
                interval: RangeSpec::for_interval(RangeId::new(1), split_at, None),
            },
        },
    )
    .unwrap();
    seed_control_operation(&bootstrap, tenant, &split).await;
    let evidence = crabka_gres_control::SplitOperationEvidence {
        manifest_key: Some("manifest".into()),
        covered_offset: Some(8),
        barrier_offset: Some(10),
        tail_sha256: Some("tail".into()),
        marker_digest: Some("markers".into()),
    };
    for phase in [
        crabka_gres_control::SplitOperationPhase::Checkpointed,
        crabka_gres_control::SplitOperationPhase::Paused,
        crabka_gres_control::SplitOperationPhase::Restored,
        crabka_gres_control::SplitOperationPhase::Activated,
    ] {
        advance_control_operation(
            &bootstrap,
            tenant,
            "authority-op",
            phase,
            Some(evidence.clone()),
        )
        .await;
    }
    let authority = crabka_gres::live_split_intent_authority(
        bootstrap,
        crabka_gres_control::TenantName::try_from(tenant).unwrap(),
    );
    let exact = RangeControlReq {
        tenant: tenant.into(),
        range_id: RangeId::new(1),
        generation: 1,
        operation_id: "authority-op".into(),
        operation: RangeControlOperation::Status,
    };
    assert!(
        authority
            .authorize_request(&exact, IntentAuthorizationContext::New)
            .await
            .unwrap()
            .is_some()
    );
    let mut forged = exact;
    forged.generation = 2;
    assert!(
        authority
            .authorize_request(&forged, IntentAuthorizationContext::New)
            .await
            .unwrap()
            .is_none()
    );

    let mut registry = crabka_gres_control::Registry::connect(&broker.listen_addr().to_string())
        .await
        .unwrap();
    registry.ensure_topic().await.unwrap();
    let operation = registry
        .load_split_operation(tenant, "authority-op")
        .await
        .unwrap()
        .unwrap();
    let source_record = registry.get(tenant).await.unwrap().unwrap();
    let target_record = source_record
        .with_range_layout(operation.plan.as_ref().unwrap().target_layout.clone())
        .unwrap();
    registry
        .replace_if_version(&target_record, Some(1))
        .await
        .unwrap();
    let published = operation
        .advance(
            crabka_gres_control::SplitOperationPhase::LayoutPublished,
            operation.attempts,
            None,
        )
        .unwrap();
    registry
        .compare_and_swap_split_operation(Some(operation.revision), &published)
        .await
        .unwrap();
    let exact_target = RangeControlReq {
        generation: 1,
        ..forged.clone()
    };
    assert!(
        authority
            .authorize_request(&exact_target, IntentAuthorizationContext::New)
            .await
            .unwrap()
            .is_some()
    );
    let stale_source = RangeControlReq {
        range_id: RangeId::COORDINATOR,
        generation: 0,
        ..exact_target
    };
    assert!(
        authority
            .authorize_request(&stale_source, IntentAuthorizationContext::New)
            .await
            .unwrap()
            .is_none()
    );
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ControlSplitDriverState {
    split: crabka_gres_ranges::SplitState,
    manifest_key: String,
    covered_offset: i64,
    barrier_offset: i64,
    stage_binding: (u64, String),
    post_stage_binding: Option<(u64, String)>,
}

fn persist_control_split_driver_state(
    state_path: &std::path::Path,
    state: &ControlSplitDriverState,
) {
    std::fs::write(
        state_path,
        serde_json::to_vec(state).expect("encode operator driver state"),
    )
    .expect("persist operator driver state");
}

fn persist_post_stage_binding(
    state_path: &std::path::Path,
    split: &crabka_gres_ranges::SplitState,
    manifest_key: &str,
    covered_offset: i64,
    barrier_offset: i64,
    stage_binding: &(u64, String),
    binding: &(u64, String),
) {
    persist_control_split_driver_state(
        state_path,
        &ControlSplitDriverState {
            split: split.clone(),
            manifest_key: manifest_key.to_owned(),
            covered_offset,
            barrier_offset,
            stage_binding: stage_binding.clone(),
            post_stage_binding: Some(binding.clone()),
        },
    );
}

async fn load_or_prepare_control_split(
    runtime: &crabka_gres::GresRuntime,
    bootstrap: &str,
    tenant: &str,
    operation_id: &str,
    state_path: &std::path::Path,
    initial_mutation: Option<crabka_gres_ranges::SplitState>,
) -> ControlSplitDriverState {
    use crabka_gres_ranges::transport::{RangeControlOperation as Operation, RangeControlResp};
    if state_path.exists() {
        return serde_json::from_slice(&std::fs::read(state_path).expect("read driver state"))
            .expect("decode driver state");
    }
    let current_map = runtime.published_range_map().expect("source range map");
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|range| range.range_id == RangeId::COORDINATOR)
        .cloned()
        .expect("source predecessor");
    let split_at = RangeKey::table_start(TableId::new(1));
    let split = initial_mutation.unwrap_or_else(|| {
        crabka_gres_ranges::SplitState::for_split(
            operation_id,
            SplitCommand {
                current_map,
                predecessor: RangeId::COORDINATOR,
                predecessor_generation: 0,
                left: SuccessorDescriptor {
                    range_id: RangeId::COORDINATOR,
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 1,
                    interval: RangeSpec::for_interval(
                        RangeId::COORDINATOR,
                        predecessor.start,
                        Some(split_at),
                    ),
                },
                right: SuccessorDescriptor {
                    range_id: RangeId::new(2),
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 1,
                    interval: RangeSpec::for_interval(RangeId::new(2), split_at, predecessor.end),
                },
            },
        )
        .expect("valid control split")
    });
    let transfer = runtime
        .range_transfer_capability()
        .expect("live control transfer capability");
    transfer
        .record_topology_activation_intent(&split)
        .await
        .expect("journal topology intent before checkpoint");
    seed_control_operation(bootstrap, tenant, &split).await;
    let checkpoint = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::ForceCheckpoint,
    )
    .await;
    let RangeControlResp::Checkpoint {
        covered_offset,
        manifest_key,
        ..
    } = checkpoint
    else {
        panic!("checkpoint failed: {checkpoint:?}");
    };
    advance_control_operation(
        bootstrap,
        tenant,
        operation_id,
        crabka_gres_control::SplitOperationPhase::Checkpointed,
        Some(crabka_gres_control::SplitOperationEvidence {
            manifest_key: Some(manifest_key.clone()),
            covered_offset: Some(covered_offset),
            ..Default::default()
        }),
    )
    .await;
    transfer
        .record_topology_activation_checkpoint(
            operation_id,
            &crabka_gres_ranges::CheckpointManifest {
                range_id: split.predecessor,
                covered_offset,
                manifest_key: manifest_key.clone(),
            },
        )
        .await
        .expect("journal topology checkpoint before pause");
    let pause = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::PauseAtCoveredOffset {
            manifest_key: manifest_key.clone(),
            covered_offset,
        },
    )
    .await;
    let RangeControlResp::Paused { barrier_offset } = pause else {
        panic!("pause failed: {pause:?}");
    };
    advance_control_operation(
        bootstrap,
        tenant,
        operation_id,
        crabka_gres_control::SplitOperationPhase::Paused,
        Some(crabka_gres_control::SplitOperationEvidence {
            manifest_key: Some(manifest_key.clone()),
            covered_offset: Some(covered_offset),
            barrier_offset: Some(barrier_offset),
            ..Default::default()
        }),
    )
    .await;
    let state = ControlSplitDriverState {
        stage_binding: control_binding(bootstrap, tenant, operation_id).await,
        split,
        manifest_key,
        covered_offset,
        barrier_offset,
        post_stage_binding: None,
    };
    persist_control_split_driver_state(state_path, &state);
    state
}

async fn drive_live_control_split(
    runtime: &crabka_gres::GresRuntime,
    bootstrap: &str,
    tenant: &str,
    operation_id: &str,
    state_path: &std::path::Path,
    initial_mutation: Option<crabka_gres_ranges::SplitState>,
) {
    use crabka_gres_ranges::transport::{RangeControlOperation as Operation, RangeControlResp};
    let ControlSplitDriverState {
        split,
        manifest_key,
        covered_offset,
        barrier_offset,
        stage_binding,
        mut post_stage_binding,
    } = load_or_prepare_control_split(
        runtime,
        bootstrap,
        tenant,
        operation_id,
        state_path,
        initial_mutation,
    )
    .await;
    if runtime.published_range_map().as_ref() == Some(&split.target_map) {
        let (journal_revision, journal_digest) = post_stage_binding
            .clone()
            .unwrap_or(control_binding(bootstrap, tenant, operation_id).await);
        let prologue = control_request(
            runtime,
            tenant,
            operation_id,
            &split,
            Operation::SuccessorFencePrologue {
                journal_revision,
                journal_digest,
            },
        )
        .await;
        assert!(
            matches!(
                prologue,
                RangeControlResp::Applied | RangeControlResp::AlreadyApplied
            ),
            "active prologue replay: {prologue:?}"
        );
        advance_control_operation(
            bootstrap,
            tenant,
            operation_id,
            crabka_gres_control::SplitOperationPhase::Activated,
            None,
        )
        .await;
        let retire = control_request(
            runtime,
            tenant,
            operation_id,
            &split,
            Operation::RetirePredecessor,
        )
        .await;
        assert!(
            matches!(
                retire,
                RangeControlResp::Applied | RangeControlResp::AlreadyApplied
            ),
            "active retire: {retire:?}"
        );
        advance_control_operation(
            bootstrap,
            tenant,
            operation_id,
            crabka_gres_control::SplitOperationPhase::Completed,
            None,
        )
        .await;
        return;
    }
    let checkpoint = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::ForceCheckpoint,
    )
    .await;
    assert!(matches!(checkpoint, RangeControlResp::Checkpoint { .. }));
    let pause = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::PauseAtCoveredOffset {
            manifest_key: manifest_key.clone(),
            covered_offset,
        },
    )
    .await;
    assert!(matches!(pause, RangeControlResp::Paused { .. }));
    let (journal_revision, journal_digest) = stage_binding.clone();
    let stage_response = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::StageFilteredRestore {
            journal_revision,
            journal_digest,
        },
    )
    .await;
    assert!(
        matches!(stage_response, RangeControlResp::Staged { .. }),
        "stage: {stage_response:?}"
    );
    advance_control_operation(
        bootstrap,
        tenant,
        operation_id,
        crabka_gres_control::SplitOperationPhase::Restored,
        None,
    )
    .await;
    let (journal_revision, journal_digest) = if let Some(binding) = post_stage_binding.take() {
        binding
    } else {
        let binding = control_binding(bootstrap, tenant, operation_id).await;
        persist_post_stage_binding(
            state_path,
            &split,
            &manifest_key,
            covered_offset,
            barrier_offset,
            &stage_binding,
            &binding,
        );
        binding
    };
    let markers = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::InheritMarkers {
            journal_revision,
            journal_digest: journal_digest.clone(),
        },
    )
    .await;
    assert!(
        matches!(markers, RangeControlResp::Markers { .. }),
        "markers: {markers:?}"
    );
    let prologue = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::SuccessorFencePrologue {
            journal_revision,
            journal_digest,
        },
    )
    .await;
    assert!(
        matches!(
            prologue,
            RangeControlResp::Applied | RangeControlResp::AlreadyApplied
        ),
        "prologue: {prologue:?}"
    );
    advance_control_operation(
        bootstrap,
        tenant,
        operation_id,
        crabka_gres_control::SplitOperationPhase::Activated,
        None,
    )
    .await;
    let retire = control_request(
        runtime,
        tenant,
        operation_id,
        &split,
        Operation::RetirePredecessor,
    )
    .await;
    assert!(
        matches!(
            retire,
            RangeControlResp::Applied | RangeControlResp::AlreadyApplied
        ),
        "retire: {retire:?}"
    );
    advance_control_operation(
        bootstrap,
        tenant,
        operation_id,
        crabka_gres_control::SplitOperationPhase::Completed,
        None,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_control_move_stages_claims_and_publishes_one_distinct_endpoint_successor() {
    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let checkpoint_root = tempfile::tempdir().expect("checkpoint root");
    let tenant = "runtime-control-move";
    let bootstrap = broker.listen_addr().to_string();
    let runtime = crabka_gres::open_substrate_runtime(&activation_crash_config(
        bootstrap.clone(),
        tenant.into(),
        checkpoint_root.path().to_path_buf(),
    ))
    .await
    .expect("open live runtime");
    let mut session = runtime.engine.connect();
    session
        .simple_query(
            "CREATE TABLE m1 (id int4); CREATE TABLE m2 (id int4); \
             CREATE TABLE m3 (id int4); CREATE TABLE m4 (id int4); \
             CREATE TABLE moved (id int4); INSERT INTO moved VALUES (7)",
        )
        .await
        .expect("seed table owned by r1");
    let current_map = runtime.published_range_map().expect("source map");
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|range| range.range_id == RangeId::new(1))
        .cloned()
        .expect("ordinary source range");
    let mutation = crabka_gres_ranges::SplitState::for_move(
        "live-control-move",
        MoveRangeCommand {
            current_map,
            range_id: predecessor.range_id,
            predecessor_generation: 0,
            replacement: SuccessorDescriptor {
                range_id: RangeId::new(9),
                endpoint: "replacement.service.internal:7443".into(),
                wal_generation: 1,
                interval: RangeSpec::for_interval(
                    RangeId::new(9),
                    predecessor.start,
                    predecessor.end,
                ),
            },
        },
    )
    .expect("sealed Move");
    let target_map = mutation.target_map.clone();

    drive_live_control_split(
        &runtime,
        &bootstrap,
        tenant,
        "live-control-move",
        &checkpoint_root.path().join("move-driver.json"),
        Some(mutation),
    )
    .await;

    assert_eq!(runtime.published_range_map(), Some(target_map));
    let mut post_move = runtime.engine.connect();
    let rows = post_move
        .simple_query("SELECT id FROM moved")
        .await
        .expect("replacement serves moved table");
    assert!(!rows.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_executor_crash_child() {
    let _permit = broker_test_permit().await;
    if std::env::var_os("CRABKA_GRES_CONTROL_CRASH_CHILD").is_none() {
        return;
    }
    let bootstrap = std::env::var("CRABKA_GRES_ACTIVATION_BOOTSTRAP").expect("bootstrap env");
    let tenant = std::env::var("CRABKA_GRES_ACTIVATION_TENANT").expect("tenant env");
    let checkpoint_root = PathBuf::from(
        std::env::var("CRABKA_GRES_ACTIVATION_CHECKPOINT_ROOT").expect("checkpoint env"),
    );
    let runtime = crabka_gres::open_substrate_runtime(&activation_crash_config(
        bootstrap.clone(),
        tenant.clone(),
        checkpoint_root.clone(),
    ))
    .await
    .expect("open control crash child");
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE t1 (id int4); INSERT INTO t1 VALUES (7), (8), (9)")
        .await
        .expect("seed control crash ledger");
    drive_live_control_split(
        &runtime,
        &bootstrap,
        &tenant,
        "control-crash",
        &checkpoint_root.join("control-driver.json"),
        None,
    )
    .await;
    panic!("control crash failpoint returned");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_executor_hard_crash_matrix_reconciles_and_replays() {
    const MAX_CONTROL_RECOVERY_PAUSE: std::time::Duration = std::time::Duration::from_secs(30);

    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    for step in ["stage", "markers", "prologue", "retire"] {
        let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
        let tenant = format!("control-crash-{step}");
        let executable = std::env::current_exe().expect("test executable");
        let bootstrap = broker.listen_addr().to_string();
        let checkpoint_root = checkpoint_dir.path().to_path_buf();
        let status = tokio::task::spawn_blocking({
            let tenant = tenant.clone();
            let bootstrap = bootstrap.clone();
            let checkpoint_root = checkpoint_root.clone();
            move || {
                std::process::Command::new(executable)
                    .args(["--exact", "control_executor_crash_child", "--nocapture"])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .env("CRABKA_GRES_CONTROL_CRASH_CHILD", "1")
                    .env("CRABKA_GRES_CONTROL_CRASH_AFTER_EFFECT", step)
                    .env("CRABKA_GRES_ACTIVATION_BOOTSTRAP", bootstrap)
                    .env("CRABKA_GRES_ACTIVATION_TENANT", tenant)
                    .env("CRABKA_GRES_ACTIVATION_CHECKPOINT_ROOT", checkpoint_root)
                    .status()
                    .expect("run control crash child")
            }
        })
        .await
        .expect("join crash child");
        assert_eq!(
            status.code(),
            Some(86),
            "{step} must hard-exit after its effect"
        );

        let started = std::time::Instant::now();
        let runtime = crabka_gres::open_substrate_runtime(&activation_crash_config(
            bootstrap.clone(),
            tenant.clone(),
            checkpoint_root.clone(),
        ))
        .await
        .unwrap_or_else(|error| panic!("pre-readiness reconcile after {step}: {error}"));
        drive_live_control_split(
            &runtime,
            &bootstrap,
            &tenant,
            "control-crash",
            &checkpoint_root.join("control-driver.json"),
            None,
        )
        .await;
        let recovery_elapsed = started.elapsed();
        eprintln!(
            "control crash step={step} reopen_and_replay_ms={}",
            recovery_elapsed.as_millis()
        );
        assert!(
            recovery_elapsed < MAX_CONTROL_RECOVERY_PAUSE,
            "{step} exceeded bounded pause/recovery window"
        );
        let map = runtime.published_range_map().expect("successor map");
        assert_eq!(
            map.ranges()
                .iter()
                .filter(|range| range.contains_key(RangeKey::table_start(TableId::new(1))))
                .count(),
            1,
            "exactly one successor owns the table"
        );
        let ledger_rowids = runtime
            .hosted_range_kv_scan(RangeId::new(2))
            .expect("scan successor durable fold")
            .into_iter()
            .filter_map(|(key, _)| match crabka_pgkv::key::classify_key(&key) {
                crabka_pgkv::key::KeyClass::PrimaryRow { table_id: 1, rowid }
                | crabka_pgkv::key::KeyClass::PrimaryVersion {
                    table_id: 1, rowid, ..
                } => Some(rowid),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            ledger_rowids.len(),
            3,
            "exact acknowledged ledger row identities"
        );
    }
}

fn activation_fault_from_env(value: &str) -> crabka_gres::TopologyActivationFault {
    match value {
        "before_must_activate" => crabka_gres::TopologyActivationFault::BeforeMustActivate,
        "after_must_activate" => crabka_gres::TopologyActivationFault::AfterMustActivate,
        "before_producer_init" => crabka_gres::TopologyActivationFault::BeforeProducerInit,
        "after_producer_init" => crabka_gres::TopologyActivationFault::AfterProducerInit,
        "before_deferred_bind" => crabka_gres::TopologyActivationFault::BeforeDeferredBind,
        "after_deferred_bind" => crabka_gres::TopologyActivationFault::AfterDeferredBind,
        "first_writer" => crabka_gres::TopologyActivationFault::FirstWriterActivated,
        "second_writer" => crabka_gres::TopologyActivationFault::SecondWriterActivated,
        "first_checkpoint" => crabka_gres::TopologyActivationFault::FirstCheckpointDurable,
        "second_checkpoint" => crabka_gres::TopologyActivationFault::SecondCheckpointDurable,
        "checkpoint_phase" => crabka_gres::TopologyActivationFault::CheckpointDurable,
        "topology_swap" => crabka_gres::TopologyActivationFault::TopologySwap,
        "topology_committed" => crabka_gres::TopologyActivationFault::TopologyCommitted,
        other => panic!("unknown activation fault {other}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_crash_child() {
    let _permit = broker_test_permit().await;
    let Ok(fault_name) = std::env::var("CRABKA_GRES_ACTIVATION_CRASH_CHILD") else {
        return;
    };
    let bootstrap = std::env::var("CRABKA_GRES_ACTIVATION_BOOTSTRAP").expect("bootstrap env");
    let tenant = std::env::var("CRABKA_GRES_ACTIVATION_TENANT").expect("tenant env");
    let checkpoint_root = PathBuf::from(
        std::env::var("CRABKA_GRES_ACTIVATION_CHECKPOINT_ROOT").expect("checkpoint env"),
    );
    let runtime = crabka_gres::open_substrate_runtime(&activation_crash_config(
        bootstrap,
        tenant,
        checkpoint_root,
    ))
    .await
    .expect("open crash child runtime");
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE t1 (id int4)")
        .await
        .expect("create crash ledger");
    for value in [7, 8, 9] {
        session
            .simple_query(&format!("INSERT INTO t1 VALUES ({value})"))
            .await
            .expect("append acknowledged crash ledger value");
    }
    let current_map = runtime.published_range_map().expect("crash source map");
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|spec| spec.range_id == RangeId::COORDINATOR)
        .expect("crash predecessor")
        .clone();
    let split_at = RangeKey::table_start(TableId::new(1));
    runtime.inject_topology_activation_fault(activation_fault_from_env(&fault_name));
    let result = runtime
        .split_successors(
            "activation-crash",
            SplitCommand {
                current_map,
                predecessor: RangeId::COORDINATOR,
                predecessor_generation: 0,
                left: SuccessorDescriptor {
                    range_id: RangeId::COORDINATOR,
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 1,
                    interval: RangeSpec::for_interval(
                        RangeId::COORDINATOR,
                        RangeKey::MIN,
                        Some(split_at),
                    ),
                },
                right: SuccessorDescriptor {
                    range_id: RangeId::new(2),
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 1,
                    interval: RangeSpec::for_interval(RangeId::new(2), split_at, predecessor.end),
                },
            },
        )
        .await;
    panic!("hard crash failpoint returned to the child: {result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_generation_chain_child() {
    let _permit = broker_test_permit().await;
    if std::env::var_os("CRABKA_GRES_ACTIVATION_CHAIN_CHILD").is_none() {
        return;
    }
    let bootstrap = std::env::var("CRABKA_GRES_ACTIVATION_BOOTSTRAP").expect("bootstrap env");
    let tenant = std::env::var("CRABKA_GRES_ACTIVATION_TENANT").expect("tenant env");
    let checkpoint_root = PathBuf::from(
        std::env::var("CRABKA_GRES_ACTIVATION_CHECKPOINT_ROOT").expect("checkpoint env"),
    );
    let runtime = crabka_gres::open_substrate_runtime(&activation_crash_config(
        bootstrap,
        tenant,
        checkpoint_root,
    ))
    .await
    .expect("open chain child runtime");
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE t1 (id int4); CREATE TABLE t2 (id int4)")
        .await
        .expect("create chain tables");
    session
        .simple_query("INSERT INTO t1 VALUES (11); INSERT INTO t2 VALUES (22)")
        .await
        .expect("seed chain tables");

    let current_map = runtime.published_range_map().expect("g0 map");
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|spec| spec.range_id == RangeId::COORDINATOR)
        .expect("g0 r0")
        .clone();
    let split_at_t2 = RangeKey::table_start(TableId::new(2));
    runtime
        .split_successors(
            "activation-chain-g1",
            SplitCommand {
                current_map,
                predecessor: RangeId::COORDINATOR,
                predecessor_generation: 0,
                left: SuccessorDescriptor {
                    range_id: RangeId::COORDINATOR,
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 1,
                    interval: RangeSpec::for_interval(
                        RangeId::COORDINATOR,
                        RangeKey::MIN,
                        Some(split_at_t2),
                    ),
                },
                right: SuccessorDescriptor {
                    range_id: RangeId::new(2),
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 1,
                    interval: RangeSpec::for_interval(
                        RangeId::new(2),
                        split_at_t2,
                        predecessor.end,
                    ),
                },
            },
        )
        .await
        .expect("activate g1");

    let current_map = runtime.published_range_map().expect("g1 map");
    let predecessor = current_map
        .ranges()
        .iter()
        .find(|spec| spec.range_id == RangeId::COORDINATOR)
        .expect("g1 r0")
        .clone();
    let split_at_t1 = RangeKey::table_start(TableId::new(1));
    runtime
        .inject_topology_activation_fault(crabka_gres::TopologyActivationFault::AfterMustActivate);
    let result = runtime
        .split_successors(
            "activation-chain-g2",
            SplitCommand {
                current_map,
                predecessor: RangeId::COORDINATOR,
                predecessor_generation: 1,
                left: SuccessorDescriptor {
                    range_id: RangeId::COORDINATOR,
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 2,
                    interval: RangeSpec::for_interval(
                        RangeId::COORDINATOR,
                        RangeKey::MIN,
                        Some(split_at_t1),
                    ),
                },
                right: SuccessorDescriptor {
                    range_id: RangeId::new(3),
                    endpoint: "127.0.0.1:7443".into(),
                    wal_generation: 2,
                    interval: RangeSpec::for_interval(
                        RangeId::new(3),
                        split_at_t1,
                        predecessor.end,
                    ),
                },
            },
        )
        .await;
    panic!("chain hard-crash failpoint returned: {result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activation_discovery_follows_g0_g1_g2_with_distinct_operation_ids() {
    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
    let tenant = "activation-generation-chain".to_owned();
    let bootstrap = broker.listen_addr().to_string();
    let checkpoint_root = checkpoint_dir.path().to_path_buf();
    let executable = std::env::current_exe().expect("test executable");
    let status = tokio::task::spawn_blocking({
        let bootstrap = bootstrap.clone();
        let tenant = tenant.clone();
        let checkpoint_root = checkpoint_root.clone();
        move || {
            std::process::Command::new(executable)
                .args([
                    "--exact",
                    "activation_generation_chain_child",
                    "--nocapture",
                ])
                .env("CRABKA_GRES_ACTIVATION_CHAIN_CHILD", "1")
                .env("CRABKA_GRES_ACTIVATION_HARD_CRASH", "1")
                .env("CRABKA_GRES_ACTIVATION_BOOTSTRAP", bootstrap)
                .env("CRABKA_GRES_ACTIVATION_TENANT", tenant)
                .env("CRABKA_GRES_ACTIVATION_CHECKPOINT_ROOT", checkpoint_root)
                .status()
                .expect("run chain child")
        }
    })
    .await
    .expect("join chain child");
    assert!(!status.success(), "g2 crash must kill the child");

    let runtime = crabka_gres::open_substrate_runtime(&activation_crash_config(
        bootstrap,
        tenant,
        checkpoint_root,
    ))
    .await
    .expect("reconcile chained activation before readiness");
    let map = runtime.published_range_map().expect("g2 map");
    assert!(
        map.ranges()
            .iter()
            .any(|range| range.range_id == RangeId::new(2))
    );
    assert!(
        map.ranges()
            .iter()
            .any(|range| range.range_id == RangeId::new(3))
    );
    let mut session = runtime.engine.connect();
    session
        .simple_query("SELECT * FROM t1")
        .await
        .expect("g2 t1");
    session
        .simple_query("SELECT * FROM t2")
        .await
        .expect("g2 t2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activation_crash_matrix_reopens_before_readiness() {
    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    for (index, fault) in [
        "before_must_activate",
        "after_must_activate",
        "before_producer_init",
        "after_producer_init",
        "before_deferred_bind",
        "after_deferred_bind",
        "first_writer",
        "second_writer",
        "first_checkpoint",
        "second_checkpoint",
        "checkpoint_phase",
        "topology_swap",
        "topology_committed",
    ]
    .into_iter()
    .enumerate()
    {
        let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
        let tenant = format!("activation-crash-{index}");
        let executable = std::env::current_exe().expect("current test executable");
        let bootstrap = broker.listen_addr().to_string();
        let checkpoint_root = checkpoint_dir.path().to_path_buf();
        let child_status = tokio::task::spawn_blocking({
            let tenant = tenant.clone();
            let bootstrap = bootstrap.clone();
            let checkpoint_root = checkpoint_root.clone();
            move || {
                std::process::Command::new(executable)
                    .args(["--exact", "activation_crash_child", "--nocapture"])
                    .env("CRABKA_GRES_ACTIVATION_CRASH_CHILD", fault)
                    .env("CRABKA_GRES_ACTIVATION_BOOTSTRAP", bootstrap)
                    .env("CRABKA_GRES_ACTIVATION_TENANT", tenant)
                    .env("CRABKA_GRES_ACTIVATION_CHECKPOINT_ROOT", checkpoint_root)
                    .env("CRABKA_GRES_ACTIVATION_HARD_CRASH", "1")
                    .status()
                    .expect("run crash child")
            }
        })
        .await
        .expect("join crash child");
        assert!(
            !child_status.success(),
            "fault {fault} must kill its process"
        );

        let config = activation_crash_config(bootstrap, tenant, checkpoint_root);
        let runtime = crabka_gres::open_substrate_runtime(&config)
            .await
            .unwrap_or_else(|error| panic!("reopen after {fault}: {error}"));
        let post_activation = index >= 1;
        assert_eq!(
            runtime
                .published_range_map()
                .expect("recovered range map")
                .ranges()
                .iter()
                .any(|spec| spec.range_id == RangeId::new(2)),
            post_activation,
            "fault {fault} recovered the wrong ownership side"
        );
        let mut recovered = runtime.engine.connect();
        let rows = recovered
            .simple_query("SELECT id FROM t1")
            .await
            .unwrap_or_else(|error| panic!("query recovered ledger after {fault}: {error:?}"));
        let mut values = rows
            .iter()
            .flat_map(|result| match result {
                crabka_pgwire::engine::QueryResult::Rows { rows, .. } => rows.as_slice(),
                _ => &[],
            })
            .filter_map(|row| {
                row.first()
                    .and_then(Option::as_ref)
                    .map(|cell| String::from_utf8_lossy(&cell.text).into_owned())
            })
            .collect::<Vec<_>>();
        values.sort();
        assert_eq!(values, ["7", "8", "9"], "fault {fault} exact ledger");

        if post_activation {
            let repeated = crabka_gres::open_substrate_runtime(&config)
                .await
                .unwrap_or_else(|error| panic!("repeat reopen after {fault}: {error}"));
            recovered
                .simple_query("INSERT INTO t1 VALUES (99)")
                .await
                .expect_err("repeat recovery fences a session from the prior writer epoch");
            assert!(
                repeated
                    .published_range_map()
                    .expect("repeat recovered map")
                    .ranges()
                    .iter()
                    .any(|spec| spec.range_id == RangeId::new(2)),
                "repeat recovery after {fault} preserves successor ownership"
            );
            let mut repeated_session = repeated.engine.connect();
            let repeated_rows = repeated_session
                .simple_query("SELECT id FROM t1")
                .await
                .expect("query repeat recovered ledger");
            let mut repeated_values = repeated_rows
                .iter()
                .flat_map(|result| match result {
                    crabka_pgwire::engine::QueryResult::Rows { rows, .. } => rows.as_slice(),
                    _ => &[],
                })
                .filter_map(|row| {
                    row.first()
                        .and_then(Option::as_ref)
                        .map(|cell| String::from_utf8_lossy(&cell.text).into_owned())
                })
                .collect::<Vec<_>>();
            repeated_values.sort();
            assert_eq!(
                repeated_values,
                ["7", "8", "9"],
                "repeat recovery after {fault} is idempotent"
            );
        }
    }
}

#[allow(dead_code)]
fn assert_selected_table_transfer(
    checkpoint_source: &crabka_pgkv::KvScan,
    source_with_tail: &crabka_pgkv::KvScan,
    staged: &crabka_pgkv::KvScan,
    table_id: u32,
    unrelated_table_id: u32,
) {
    use crabka_pgkv::key::{self, KeyClass};
    use crabka_pgmvcc::{FROZEN_XID, INVALID_XID, version};

    let source_versions = primary_versions(source_with_tail, table_id);
    let staged_versions = primary_versions(staged, table_id);
    assert!(
        !source_versions.is_empty(),
        "source has selected MVCC versions"
    );
    assert_eq!(
        staged_versions, source_versions,
        "all selected versions restored"
    );

    let checkpoint_versions = primary_versions(checkpoint_source, table_id);
    let tail_versions: Vec<_> = source_versions
        .iter()
        .filter(|(key, _)| !checkpoint_versions.contains_key(*key))
        .collect();
    assert!(
        !tail_versions.is_empty(),
        "post-checkpoint table tail is staged"
    );

    let staged_pairs: BTreeMap<_, _> = staged.iter().cloned().collect();
    assert_eq!(
        staged_pairs.get(&key::seq_key(table_id)),
        source_with_tail
            .iter()
            .find(|(key, _)| *key == key::seq_key(table_id))
            .map(|(_, value)| value),
        "selected table sequence is restored"
    );
    assert!(
        staged
            .iter()
            .all(|(key, _)| !matches!(key::classify_key(key), KeyClass::PrimaryVersion { table_id: found, .. } if found == unrelated_table_id)),
        "unrelated table versions are absent"
    );
    assert!(
        !staged_pairs.contains_key(&key::catalog_key(crabka_pgcatalog::PUBLIC_SCHEMA, "t10"))
            && !staged_pairs.contains_key(&key::catalog_key(
                crabka_pgcatalog::PUBLIC_SCHEMA,
                "transfer_unrelated"
            )),
        "catalog entries are absent"
    );

    let mut referenced_xids = BTreeSet::new();
    for tuple in source_versions.values() {
        let (xmin, xmax, _) = version::decode_tuple(tuple).expect("decode staged MVCC tuple");
        for xid in [xmin, xmax] {
            if xid != INVALID_XID && xid != FROZEN_XID {
                referenced_xids.insert(xid);
            }
        }
    }
    let expected_clog: BTreeMap<_, _> = referenced_xids
        .into_iter()
        .map(|xid| {
            let clog_key = key::clog_key(xid);
            let value = source_with_tail
                .iter()
                .find(|(key, _)| *key == clog_key)
                .map(|(_, value)| value.clone())
                .expect("selected-table XID has source CLOG status");
            (clog_key, value)
        })
        .collect();
    let staged_clog: BTreeMap<_, _> = staged
        .iter()
        .filter(|(key, _)| matches!(key::classify_key(key), KeyClass::Clog { .. }))
        .cloned()
        .collect();
    assert_eq!(
        staged_clog, expected_clog,
        "staged CLOG is exactly the selected-table tuple XID closure"
    );
}

fn primary_versions(pairs: &crabka_pgkv::KvScan, table_id: u32) -> BTreeMap<Vec<u8>, Vec<u8>> {
    pairs
        .iter()
        .filter(|(key, _)| {
            matches!(
                crabka_pgkv::key::classify_key(key),
                crabka_pgkv::key::KeyClass::PrimaryVersion { table_id: found, .. }
                | crabka_pgkv::key::KeyClass::HashPrimaryVersion { table_id: found, .. }
                    if found == table_id
            )
        })
        .cloned()
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_populated_hash_split_partitions_physical_rows_and_sequence() {
    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
    let runtime = crabka_gres::open_substrate_runtime(&crabka_gres::SubstrateRuntimeConfig {
        bootstrap: broker.listen_addr().to_string(),
        tenant: "runtime-physical-t10".to_string(),
        cache_dir: None,
        checkpoints: Some(crabka_gres::CheckpointRuntimeConfig {
            object_store: crabka_gres::CheckpointObjectStoreConfig::Local {
                root: checkpoint_dir.path().to_path_buf(),
            },
            frames_threshold: 1,
            bytes_threshold: 1,
            part_max_bytes: crabka_gres_substrate::DEFAULT_PART_MAX_BYTES,
            retain_newest: 2,
        }),
        kafka_security: None,
        ranges: Some("0,5".to_string()),
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: Some("127.0.0.1:7443".into()),
        timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
        hlc_wall_offset_ms: 0,
        registry_policy: crabka_gres_control::RegistryPolicy::default(),
    })
    .await
    .expect("open live multi-range runtime");
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE t10 (id int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create t10");
    session
        .simple_query(
            "INSERT INTO t10 VALUES (0), (1), (2), (3), (4), (5), (6), (7), \
             (8), (9), (10), (11), (12), (13), (14), (15)",
        )
        .await
        .expect("insert t10 rows");
    let source_catalog = kv_from_pairs(
        runtime
            .inspect_hosted_range_kv(RangeId::COORDINATOR)
            .expect("inspect source catalog"),
    );
    let physical_table_id = crabka_pgcatalog::get_table(
        &source_catalog,
        &crabka_pgcatalog::RelationName::public("t10"),
    )
    .expect("t10 relation")
    .id;
    assert_ne!(u64::from(physical_table_id), 10);
    let predecessor_before = runtime
        .inspect_hosted_range_kv(RangeId::new(1))
        .expect("inspect predecessor before split");

    let current_map = runtime.published_range_map().expect("current map");
    let split_at = RangeKey::hash(TableId::new(10), 8, 0);
    let command = SplitCommand {
        current_map,
        predecessor: RangeId::new(1),
        predecessor_generation: 0,
        left: SuccessorDescriptor {
            range_id: RangeId::new(3),
            endpoint: "127.0.0.1:7443".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(
                RangeId::new(3),
                RangeKey::table_start(TableId::new(5)),
                Some(split_at),
            ),
        },
        right: SuccessorDescriptor {
            range_id: RangeId::new(2),
            endpoint: "127.0.0.1:7443".into(),
            wal_generation: 1,
            interval: RangeSpec::for_interval(RangeId::new(2), split_at, None),
        },
    };
    runtime
        .split_successors("physical-t10", command)
        .await
        .expect("publish live populated t10 successor");

    let successor = runtime
        .inspect_hosted_range_kv(RangeId::new(2))
        .expect("inspect published successor");
    let left_successor = runtime
        .inspect_hosted_range_kv(RangeId::new(3))
        .expect("inspect published left successor");
    assert_eq!(
        primary_versions(&left_successor, physical_table_id).len(),
        8,
        "the left successor contains exactly buckets 0 through 7"
    );
    assert_eq!(
        primary_versions(&successor, physical_table_id).len(),
        8,
        "the right successor contains exactly buckets 8 through 15"
    );
    let successor_buckets = |pairs: &crabka_pgkv::KvScan| {
        pairs
            .iter()
            .filter_map(|(key, _)| match crabka_pgkv::key::classify_key(key) {
                crabka_pgkv::key::KeyClass::HashPrimaryVersion {
                    table_id, bucket, ..
                } if table_id == physical_table_id => Some(bucket),
                _ => None,
            })
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        successor_buckets(&left_successor),
        (0..8).collect(),
        "left physical fold has no cross-bucket leakage"
    );
    assert_eq!(
        successor_buckets(&successor),
        (8..16).collect(),
        "right physical fold has no cross-bucket leakage"
    );
    assert!(
        !left_successor
            .iter()
            .chain(successor.iter())
            .any(|(key, _)| {
                matches!(
                    crabka_pgkv::key::classify_key(key),
                    crabka_pgkv::key::KeyClass::PrimaryVersion { table_id, .. }
                        if table_id == physical_table_id
                )
            }),
        "hash successors contain no ordinary primary keys"
    );
    let predecessor_versions = primary_versions(&predecessor_before, physical_table_id);
    let mut successor_versions = primary_versions(&left_successor, physical_table_id);
    successor_versions.extend(primary_versions(&successor, physical_table_id));
    assert_eq!(
        successor_versions.keys().collect::<Vec<_>>(),
        predecessor_versions.keys().collect::<Vec<_>>(),
        "successor intervals partition every predecessor primary version key"
    );
    let mut post_split = runtime.engine.connect();
    let rows = post_split
        .simple_query("SELECT id FROM t10 ORDER BY id")
        .await
        .expect("scan t10 after bucket midpoint split");
    let [crabka_pgwire::engine::QueryResult::Rows { rows, .. }] = rows.as_slice() else {
        panic!("expected one row result, got {rows:?}");
    };
    let ids = rows
        .iter()
        .map(|row| {
            std::str::from_utf8(&row[0].as_ref().expect("id").text)
                .expect("utf8 integer")
                .parse::<i32>()
                .expect("integer id")
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, (0..16).collect::<Vec<_>>(), "SQL union is unchanged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_multirange_transfer_rejects_concurrent_pause_without_waiting() {
    let _permit = broker_test_permit().await;
    let broker_dir = tempfile::tempdir().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
    let runtime = crabka_gres::open_substrate_runtime(&crabka_gres::SubstrateRuntimeConfig {
        bootstrap: broker.listen_addr().to_string(),
        tenant: "runtime-transfer-pause".to_string(),
        cache_dir: None,
        checkpoints: Some(crabka_gres::CheckpointRuntimeConfig {
            object_store: crabka_gres::CheckpointObjectStoreConfig::Local {
                root: checkpoint_dir.path().to_path_buf(),
            },
            frames_threshold: 1,
            bytes_threshold: 1,
            part_max_bytes: crabka_gres_substrate::DEFAULT_PART_MAX_BYTES,
            retain_newest: 2,
        }),
        kafka_security: None,
        ranges: Some("0,200".to_string()),
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: None,
        timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
        hlc_wall_offset_ms: 0,
        registry_policy: crabka_gres_control::RegistryPolicy::default(),
    })
    .await
    .expect("open live multi-range runtime");
    let transfer = runtime
        .range_transfer_capability()
        .expect("live multi-range transfer capability");
    let range = RangeId::COORDINATOR;
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE transfer_pause_source (id int4)")
        .await
        .expect("create transfer source");
    session
        .simple_query("INSERT INTO transfer_pause_source VALUES (7)")
        .await
        .expect("write source range");
    let manifest = transfer
        .force_checkpoint(range)
        .await
        .expect("force checkpoint");

    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            transfer.pause_at_checkpoint(&manifest),
            transfer.pause_at_checkpoint(&manifest),
        )
    })
    .await
    .expect("concurrent pause must not wait for a held barrier");
    let (barrier, rejected) = match (first, second) {
        (Ok(barrier), Err(error)) | (Err(error), Ok(barrier)) => (barrier, error),
        results => panic!("expected exactly one pause to succeed, got {results:?}"),
    };
    assert!(matches!(
        rejected,
        crabka_gres_ranges::RangeTransferError::AlreadyPaused { range_id } if range_id == range
    ));

    transfer
        .resume(barrier)
        .await
        .expect("resume winning pause");
    session
        .simple_query("INSERT INTO transfer_pause_source VALUES (8)")
        .await
        .expect("write after concurrent pause is released");
}

#[tokio::test]
async fn non_live_runtimes_do_not_expose_range_transfer_capability() {
    let _permit = broker_test_permit().await;
    let single = crabka_gres::open_substrate_runtime(&crabka_gres::SubstrateRuntimeConfig {
        bootstrap: "memory://".to_string(),
        tenant: "runtime-transfer".to_string(),
        cache_dir: None,
        checkpoints: None,
        kafka_security: None,
        ranges: None,
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: None,
        timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
        hlc_wall_offset_ms: 0,
        registry_policy: crabka_gres_control::RegistryPolicy::default(),
    })
    .await
    .expect("open in-memory single-range runtime");
    let multi = crabka_gres::open_substrate_runtime(&crabka_gres::SubstrateRuntimeConfig {
        ranges: Some("0,100".to_string()),
        ..crabka_gres::SubstrateRuntimeConfig {
            bootstrap: "memory://".to_string(),
            tenant: "runtime-transfer".to_string(),
            cache_dir: None,
            checkpoints: None,
            kafka_security: None,
            ranges: None,
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
            timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
            hlc_wall_offset_ms: 0,
            registry_policy: crabka_gres_control::RegistryPolicy::default(),
        }
    })
    .await
    .expect("open in-memory multi-range runtime");

    assert!(single.range_transfer_capability().is_none());
    assert!(multi.range_transfer_capability().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_uses_tenant_scram_by_default_and_rejects_wrong_password() {
    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let loader = FakeTenantConfigLoader {
        record: tenant_record(),
    };
    let mut args = substrate_test_args(format!("127.0.0.1:{port}"));
    args.auth = None;
    let server = tokio::spawn(async move {
        crabka_gres::serve_listener_with_tenant_config_loader(listener, args, &loader).await
    });

    let client = connect_with_password(port, "alice", &fixture_password()).await;
    client.simple_query("SELECT 1").await.expect("select");
    let wrong_password = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("alice")
        .password(wrong_fixture_password())
        .connect(tokio_postgres::NoTls)
        .await;
    assert!(wrong_password.is_err());

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_tenant_scram_accepts_libpq_psql() {
    let _permit = broker_test_permit().await;
    if std::process::Command::new("psql")
        .arg("--version")
        .output()
        .is_err()
    {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let loader = FakeTenantConfigLoader {
        record: tenant_record(),
    };
    let mut args = substrate_test_args(format!("127.0.0.1:{port}"));
    args.auth = None;
    let server = tokio::spawn(async move {
        crabka_gres::serve_listener_with_tenant_config_loader(listener, args, &loader).await
    });

    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("psql")
            .env("PGPASSWORD", fixture_password())
            .arg(format!(
                "host=127.0.0.1 port={port} user=alice dbname=crab sslmode=disable"
            ))
            .args(["-tAc", "SELECT 1"])
            .output()
            .expect("run psql")
    })
    .await
    .expect("join psql");
    assert!(
        output.status.success(),
        "psql stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_serves_sql_over_pgwire() {
    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(crabka_gres::serve_listener(
        listener,
        test_args(format!("127.0.0.1:{port}"), None),
    ));

    let client = connect(port).await;
    client
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO t VALUES (7)")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");
    let first_value = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(first_value, Some("7"));

    server.abort();
    let _ = server.await;
}

/// The binary's `run_serve` path uses the real `LiveTenantConfigLoader`
/// rather than the injected test stubs, so `memory://` must not be resolved
/// as a broker address list during tenant-config loading.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_live_loader_serves_sql_over_memory_substrate() {
    use assert2::assert;

    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let cache_dir = tempfile::tempdir().expect("cache dir");
    let mut args = substrate_test_args(format!("127.0.0.1:{port}"));
    args.cache_dir = Some(cache_dir.path().to_path_buf());
    let server = tokio::spawn(async move {
        crabka_gres::serve_listener_with_tenant_config_loader(
            listener,
            args,
            &crabka_gres::LiveTenantConfigLoader,
        )
        .await
    });

    let client = connect(port).await;
    client
        .simple_query("CREATE TABLE binary_smoke (id int4)")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO binary_smoke VALUES (42)")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT id FROM binary_smoke")
        .await
        .expect("select");
    let first_value = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert!(first_value == Some("42"));

    server.abort();
    let _ = server.await;
}

/// Read one pgwire backend message (type byte + length-prefixed payload).
async fn read_backend_message(
    stream: &mut tokio::net::TcpStream,
) -> std::io::Result<(u8, Vec<u8>)> {
    use tokio::io::AsyncReadExt as _;
    let kind = stream.read_u8().await?;
    let len = stream.read_i32().await?;
    let body_len = usize::try_from(len - 4).expect("message length fits usize");
    let mut body = vec![0_u8; body_len];
    stream.read_exact(&mut body).await?;
    Ok((kind, body))
}

/// Drain backend messages until `ReadyForQuery`, returning the types seen.
async fn read_until_ready(
    stream: &mut tokio::net::TcpStream,
) -> std::io::Result<Vec<(u8, Vec<u8>)>> {
    let mut seen = Vec::new();
    loop {
        let (kind, body) = read_backend_message(stream).await?;
        let done = kind == b'Z';
        seen.push((kind, body));
        if done {
            return Ok(seen);
        }
    }
}

async fn send_simple_query(stream: &mut tokio::net::TcpStream, sql: &str) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let mut msg = vec![b'Q'];
    let payload_len = i32::try_from(4 + sql.len() + 1).expect("query length fits i32");
    msg.extend_from_slice(&payload_len.to_be_bytes());
    msg.extend_from_slice(sql.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await
}

/// COPY FROM STDIN through the binary's front door uses the simple-query
/// protocol (the path psql's `\copy` takes), so this drives raw pgwire frames:
/// Query -> `CopyInResponse` -> `CopyData`/`CopyDone` -> `CommandComplete`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_serves_copy_from_stdin_over_pgwire() {
    use assert2::assert;
    use tokio::io::AsyncWriteExt as _;

    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(crabka_gres::serve_listener(
        listener,
        test_args(format!("127.0.0.1:{port}"), None),
    ));

    let client = connect(port).await;
    client
        .simple_query("CREATE TABLE copied (id int4 PRIMARY KEY, note text)")
        .await
        .expect("create");

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("raw connect");
    let params = b"user\0postgres\0database\0postgres\0\0";
    let mut startup = Vec::new();
    startup.extend_from_slice(
        &i32::try_from(8 + params.len())
            .expect("startup length")
            .to_be_bytes(),
    );
    startup.extend_from_slice(&196_608_i32.to_be_bytes());
    startup.extend_from_slice(params);
    stream.write_all(&startup).await.expect("startup");
    read_until_ready(&mut stream).await.expect("auth handshake");

    send_simple_query(&mut stream, "COPY copied FROM STDIN")
        .await
        .expect("send copy");
    let (kind, _) = read_backend_message(&mut stream)
        .await
        .expect("copy-in response");
    assert!(
        kind == b'G',
        "expected CopyInResponse, got {}",
        char::from(kind)
    );

    for chunk in [&b"1\tfirst\n"[..], &b"2\tsecond\n3\t\\N\n"[..]] {
        let mut msg = vec![b'd'];
        msg.extend_from_slice(
            &i32::try_from(4 + chunk.len())
                .expect("chunk length")
                .to_be_bytes(),
        );
        msg.extend_from_slice(chunk);
        stream.write_all(&msg).await.expect("copy data");
    }
    stream
        .write_all(&[b'c', 0, 0, 0, 4])
        .await
        .expect("copy done");
    let seen = read_until_ready(&mut stream)
        .await
        .expect("copy completion");
    let complete = seen
        .iter()
        .find(|(kind, _)| *kind == b'C')
        .expect("CommandComplete after CopyDone");
    assert!(complete.1.starts_with(b"COPY 3\0"));

    let rows = client
        .simple_query("SELECT id, note FROM copied ORDER BY id")
        .await
        .expect("select");
    let values: Vec<(Option<&str>, Option<&str>)> = rows
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get(0), row.get(1))),
            _ => None,
        })
        .collect();
    assert!(
        values
            == vec![
                (Some("1"), Some("first")),
                (Some("2"), Some("second")),
                (Some("3"), None),
            ]
    );

    server.abort();
    let _ = server.await;
}

/// COPY FROM STDIN via the extended protocol: tokio-postgres prepares the
/// COPY statement and pipelines Bind/Execute/Sync, so the server must answer
/// Execute with `CopyInResponse`, ignore the pipelined Sync during copy-in,
/// and complete on `CopyDone` + Sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_serves_copy_from_stdin_over_extended_protocol() {
    use assert2::assert;
    use futures_util::SinkExt as _;

    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(crabka_gres::serve_listener(
        listener,
        test_args(format!("127.0.0.1:{port}"), None),
    ));

    let client = connect(port).await;
    client
        .simple_query("CREATE TABLE copied_ext (id int4 PRIMARY KEY, note text)")
        .await
        .expect("create");

    let sink = client
        .copy_in::<_, bytes::Bytes>("COPY copied_ext FROM STDIN")
        .await
        .expect("begin extended copy");
    let mut sink = Box::pin(sink);
    sink.send(bytes::Bytes::from_static(b"1\tfirst\n"))
        .await
        .expect("send first chunk");
    sink.send(bytes::Bytes::from_static(b"2\tsecond\n3\t\\N\n"))
        .await
        .expect("send second chunk");
    let copied = sink.as_mut().finish().await.expect("finish copy");
    assert!(copied == 3);

    let rows = client
        .simple_query("SELECT id, note FROM copied_ext ORDER BY id")
        .await
        .expect("select");
    let values: Vec<(Option<&str>, Option<&str>)> = rows
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get(0), row.get(1))),
            _ => None,
        })
        .collect();
    assert!(
        values
            == vec![
                (Some("1"), Some("first")),
                (Some("2"), Some("second")),
                (Some("3"), None),
            ]
    );

    // Error path: malformed copy data fails at CopyDone and the session must
    // recover (extended-protocol discard-until-Sync) for later statements.
    let sink = client
        .copy_in::<_, bytes::Bytes>("COPY copied_ext FROM STDIN")
        .await
        .expect("begin failing copy");
    let mut sink = Box::pin(sink);
    sink.send(bytes::Bytes::from_static(
        b"not-an-int\ttoo\tmany\tcolumns\n",
    ))
    .await
    .expect("send bad chunk");
    let failure = sink.as_mut().finish().await;
    assert!(failure.is_err());

    let rows = client
        .simple_query("SELECT count(*) FROM copied_ext")
        .await
        .expect("post-failure select");
    let count = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert!(count == Some("3"));

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_reopens_durable_local_storage() {
    let _permit = broker_test_permit().await;
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let data_dir = temp_dir.path().join("gres");

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("local addr").port();
        let server = tokio::spawn(crabka_gres::serve_listener(
            listener,
            test_args(format!("127.0.0.1:{port}"), Some(data_dir.clone())),
        ));
        let client = connect(port).await;
        client
            .simple_query("CREATE TABLE durable (id int4)")
            .await
            .expect("create");
        client
            .simple_query("INSERT INTO durable VALUES (42)")
            .await
            .expect("insert");
        server.abort();
        let _ = server.await;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(crabka_gres::serve_listener(
        listener,
        test_args(format!("127.0.0.1:{port}"), Some(data_dir)),
    ));
    let client = connect(port).await;
    let rows = client
        .simple_query("SELECT id FROM durable")
        .await
        .expect("select");
    let first_value = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(first_value, Some("42"));

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosted_range_compute_forwards_dml_and_grants_timestamps_over_real_tcp() {
    let _permit = broker_test_permit().await;
    let engine = crabka_pgexec::SqlEngine::new();
    let horizon = crabka_gres_ranges::MemoryTsoHorizon::new(engine.kv_handle(), 1);
    let persisted_max_ts = horizon.load_max_ts().expect("load TSO horizon");
    let tso =
        crabka_gres_ranges::tso_rpc_from_horizon(horizon.clone(), horizon, 1, persisted_max_ts)
            .expect("build durable TSO rpc");
    let service =
        crabka_gres_ranges::HostedRangeService::new(std::collections::BTreeMap::from([(
            crabka_gres_ranges::RangeId::COORDINATOR,
            engine.clone_handle(),
        )]))
        .with_tso(tso);
    let fixture = range_mtls_fixture();
    let address = spawn_range_tls(Arc::new(service), fixture.server).await;
    let client =
        crabka_gres_ranges::FramedTcpClient::with_tls(fixture.client).expect("mTLS range client");

    for sql in [
        "CREATE TABLE forwarded (id int4)",
        "INSERT INTO forwarded VALUES (7)",
    ] {
        let response = client
            .call(
                &address.to_string(),
                &crabka_gres_ranges::RangeRequest::Sql {
                    range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                    sql: sql.to_string(),
                },
            )
            .await
            .expect("range RPC response");
        assert!(matches!(
            response,
            crabka_gres_ranges::RangeResponse::SqlResults { .. }
        ));
    }

    let response = client
        .call(
            &address.to_string(),
            &crabka_gres_ranges::RangeRequest::Tso(crabka_gres_ranges::TsoReq::Grant { count: 2 }),
        )
        .await
        .expect("TSO RPC response");
    assert_eq!(
        response,
        crabka_gres_ranges::RangeResponse::Tso(crabka_gres_ranges::TsoResp::Granted {
            first_ts: 1,
            count: 2,
        })
    );

    let mut session = engine.connect();
    let rows = session
        .simple_query("SELECT id FROM forwarded")
        .await
        .expect("query locally after forwarded DML");
    assert!(matches!(
        rows.as_slice(),
        [crabka_pgwire::engine::QueryResult::Rows { .. }]
    ));
}

/// A `tokio_postgres` connection whose asynchronous messages are observable:
/// notifications arrive on the *connection*, not the client, so the connection
/// is driven with `poll_message` instead of being spawned as a bare future.
async fn connect_with_notifications(
    port: u16,
) -> (
    tokio_postgres::Client,
    tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
) {
    use assert2::assert;

    let config = format!("host=127.0.0.1 port={port} user=postgres");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok((client, mut connection)) =
            tokio_postgres::connect(&config, tokio_postgres::NoTls).await
        {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(async move {
                while let Some(message) =
                    std::future::poll_fn(|cx| connection.poll_message(cx)).await
                {
                    match message {
                        Ok(tokio_postgres::AsyncMessage::Notification(notification)) => {
                            if tx.send(notification).is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            });
            return (client, rx);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "connect to crabka-gres did not succeed within 5s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// The next notification, or a failure if none arrives promptly.
async fn next_notification(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
) -> tokio_postgres::Notification {
    tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .expect("a notification within 10s")
        .expect("the connection task is still running")
}

/// Assert that nothing is delivered in the next `millis` — used only for the
/// "not yet" half of a case whose positive half is asserted right afterwards.
async fn no_notification_within(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
    millis: u64,
) {
    use assert2::assert;

    let idle = tokio::time::timeout(std::time::Duration::from_millis(millis), rx.recv()).await;
    assert!(idle.is_err(), "unexpected notification: {idle:?}");
}

/// A hand-rolled pgwire connection. tokio-postgres keeps the pid the server
/// announced in `BackendKeyData` private, so the test that pins
/// `NotificationResponse.process_id == BackendKeyData.pid` needs its own client.
struct RawPgConnection {
    stream: tokio::net::TcpStream,
    /// The pid the server announced in `BackendKeyData`.
    pid: i32,
}

impl RawPgConnection {
    async fn connect(port: u16) -> Self {
        use tokio::io::AsyncWriteExt as _;

        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("raw pgwire connection");
        let mut body = Vec::new();
        body.extend_from_slice(&196_608i32.to_be_bytes());
        body.extend_from_slice(b"user\0postgres\0\0");
        let mut startup = Vec::new();
        startup.extend_from_slice(
            &i32::try_from(body.len() + 4)
                .expect("startup message length")
                .to_be_bytes(),
        );
        startup.extend_from_slice(&body);
        stream.write_all(&startup).await.expect("send startup");

        let startup_burst = read_until_ready(&mut stream).await.expect("startup burst");
        let pid = startup_burst
            .iter()
            .find_map(|(kind, body)| {
                (*kind == b'K').then(|| i32::from_be_bytes([body[0], body[1], body[2], body[3]]))
            })
            .expect("BackendKeyData");
        RawPgConnection { stream, pid }
    }

    /// Run one simple-protocol Query, panicking on an `ErrorResponse`.
    async fn simple_query(&mut self, sql: &str) {
        use tokio::io::AsyncWriteExt as _;

        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut message = vec![b'Q'];
        message.extend_from_slice(
            &i32::try_from(body.len() + 4)
                .expect("query length")
                .to_be_bytes(),
        );
        message.extend_from_slice(&body);
        self.stream.write_all(&message).await.expect("send query");

        let messages = read_until_ready(&mut self.stream)
            .await
            .expect("query burst");
        if let Some((_, body)) = messages.iter().find(|(kind, _)| *kind == b'E') {
            panic!("{sql} failed: {}", String::from_utf8_lossy(body));
        }
    }
}

/// The end-to-end proof: a real driver's `LISTEN` receives a
/// `NotificationResponse` raised by another connection, stamped with that
/// connection's `BackendKeyData` pid, and only once the notifier commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_delivers_notifications_between_pgwire_connections() {
    use assert2::assert;

    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(crabka_gres::serve_listener(
        listener,
        test_args(format!("127.0.0.1:{port}"), None),
    ));

    let (listening_client, mut notifications) = connect_with_notifications(port).await;
    listening_client
        .simple_query("LISTEN news")
        .await
        .expect("listen");
    let mut notifier = RawPgConnection::connect(port).await;
    assert!(notifier.pid != 0, "the server must announce a real pid");

    notifier.simple_query("NOTIFY news, 'hello'").await;
    let delivered = next_notification(&mut notifications).await;
    assert!(delivered.channel() == "news");
    assert!(delivered.payload() == "hello");
    assert!(delivered.process_id() == notifier.pid);

    // Queued in a transaction: nothing until COMMIT, then both in order.
    notifier.simple_query("BEGIN").await;
    notifier.simple_query("NOTIFY news, 'first'").await;
    notifier.simple_query("NOTIFY news, 'second'").await;
    no_notification_within(&mut notifications, 250).await;
    notifier.simple_query("COMMIT").await;
    let first = next_notification(&mut notifications).await;
    let second = next_notification(&mut notifications).await;
    assert!((first.payload(), second.payload()) == ("first", "second"));

    // A rolled-back NOTIFY is never delivered; the following one still is.
    notifier.simple_query("BEGIN").await;
    notifier.simple_query("NOTIFY news, 'dropped'").await;
    notifier.simple_query("ROLLBACK").await;
    notifier.simple_query("NOTIFY news, 'kept'").await;
    assert!(next_notification(&mut notifications).await.payload() == "kept");

    // UNLISTEN really unsubscribes: pg_notify from the other connection is
    // accepted but delivered nowhere.
    listening_client
        .simple_query("UNLISTEN news")
        .await
        .expect("unlisten");
    notifier
        .simple_query("SELECT pg_notify('news', 'after unlisten')")
        .await;
    no_notification_within(&mut notifications, 250).await;

    server.abort();
    let _ = server.await;
}

/// A connection that listens on a channel it notifies hears itself, and the pid
/// on the message identifies the notifying connection (a second connection's
/// notification carries a different one).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_delivers_a_self_notification_to_the_notifying_connection() {
    use assert2::assert;

    let _permit = broker_test_permit().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(crabka_gres::serve_listener(
        listener,
        test_args(format!("127.0.0.1:{port}"), None),
    ));

    let (client, mut notifications) = connect_with_notifications(port).await;
    client.simple_query("LISTEN news").await.expect("listen");
    client
        .simple_query("NOTIFY news, 'to myself'")
        .await
        .expect("notify");

    let own = next_notification(&mut notifications).await;
    assert!(own.channel() == "news");
    assert!(own.payload() == "to myself");
    assert!(own.process_id() != 0);

    // pg_notify takes the same route on the same connection.
    client
        .simple_query("SELECT pg_notify('news', 'from the function')")
        .await
        .expect("pg_notify");
    let from_function = next_notification(&mut notifications).await;
    assert!(from_function.payload() == "from the function");
    assert!(from_function.process_id() == own.process_id());

    // A different connection stamps a different pid on the same channel.
    let other = connect(port).await;
    other
        .simple_query("NOTIFY news, 'from elsewhere'")
        .await
        .expect("notify from the second connection");
    let foreign = next_notification(&mut notifications).await;
    assert!(foreign.payload() == "from elsewhere");
    assert!(foreign.process_id() != own.process_id());

    server.abort();
    let _ = server.await;
}

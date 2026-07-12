use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_gres_ranges::{
    RangeId, RangeKey, RangeSpec, SplitCommand, SuccessorDescriptor, TableId,
};
use crabka_pgkv::Kv as _;
use crabka_pgwire::engine::{Engine as _, Session as _};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use tokio::net::TcpListener;

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
            allowed_principals: BTreeSet::from([
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
        "g5-secret-password",
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
        range_listen: None,
        range_tls_cert: None,
        range_tls_key: None,
        range_tls_ca: None,
        range_tls_server_name: None,
        range_allowed_principals: Vec::new(),
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
async fn runtime_constructs_substrate_mode_over_in_process_wal() {
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
    })
    .await
    .expect("open live multi-range runtime");
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
    let table_id = crabka_pgcatalog::get_table(&source_catalog, "t1")
        .expect("source relation")
        .id;
    let unrelated_table_id = crabka_pgcatalog::get_table(&source_catalog, "transfer_unrelated")
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
            frames_threshold: 1,
            bytes_threshold: 1,
            part_max_bytes: crabka_gres_substrate::DEFAULT_PART_MAX_BYTES,
            retain_newest: 16,
        }),
        kafka_security: None,
        ranges: Some("0,5".to_owned()),
        host_ranges: None,
        range_rpc: None,
        advertised_endpoint: Some("127.0.0.1:7443".into()),
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
        !staged_pairs.contains_key(&key::catalog_key("t10"))
            && !staged_pairs.contains_key(&key::catalog_key("transfer_unrelated")),
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
                crabka_pgkv::key::KeyClass::PrimaryVersion { table_id: found, .. } if found == table_id
            )
        })
        .cloned()
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_populated_split_uses_physical_catalog_id_for_t10_rows_and_sequence() {
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
    })
    .await
    .expect("open live multi-range runtime");
    let mut session = runtime.engine.connect();
    session
        .simple_query("CREATE TABLE t10 (id int4)")
        .await
        .expect("create t10");
    session
        .simple_query("INSERT INTO t10 VALUES (10), (11)")
        .await
        .expect("insert t10 rows");
    let source_catalog = kv_from_pairs(
        runtime
            .inspect_hosted_range_kv(RangeId::COORDINATOR)
            .expect("inspect source catalog"),
    );
    let physical_table_id = crabka_pgcatalog::get_table(&source_catalog, "t10")
        .expect("t10 relation")
        .id;
    assert_ne!(u64::from(physical_table_id), 10);
    let predecessor_before = runtime
        .inspect_hosted_range_kv(RangeId::new(1))
        .expect("inspect predecessor before split");

    let current_map = runtime.published_range_map().expect("current map");
    let split_at = RangeKey::table_start(TableId::new(10));
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
        primary_versions(&successor, physical_table_id).len(),
        2,
        "the published successor contains t10's physical MVCC rows"
    );
    assert!(
        successor
            .iter()
            .any(|(key, _)| *key == crabka_pgkv::key::seq_key(physical_table_id)),
        "the physical table sequence is transferred with t10"
    );
    assert!(primary_versions(&left_successor, physical_table_id).is_empty());
    assert!(
        !left_successor
            .iter()
            .any(|(key, _)| *key == crabka_pgkv::key::seq_key(physical_table_id)),
        "the left and right successor folds are disjoint"
    );
    let predecessor_versions = primary_versions(&predecessor_before, physical_table_id);
    let mut successor_versions = primary_versions(&left_successor, physical_table_id);
    successor_versions.extend(primary_versions(&successor, physical_table_id));
    assert_eq!(
        successor_versions.keys().collect::<Vec<_>>(),
        predecessor_versions.keys().collect::<Vec<_>>(),
        "successor intervals partition every predecessor primary version key"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_multirange_transfer_rejects_concurrent_pause_without_waiting() {
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
        }
    })
    .await
    .expect("open in-memory multi-range runtime");

    assert!(single.range_transfer_capability().is_none());
    assert!(multi.range_transfer_capability().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_uses_tenant_scram_by_default_and_rejects_wrong_password() {
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

    let client = connect_with_password(port, "alice", "g5-secret-password").await;
    client.simple_query("SELECT 1").await.expect("select");
    let wrong_password = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("alice")
        .password("wrong")
        .connect(tokio_postgres::NoTls)
        .await;
    assert!(wrong_password.is_err());

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_tenant_scram_accepts_libpq_psql() {
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
            .env("PGPASSWORD", "g5-secret-password")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_reopens_durable_local_storage() {
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
            crabka_gres_ranges::RangeResponse::Sql { .. }
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

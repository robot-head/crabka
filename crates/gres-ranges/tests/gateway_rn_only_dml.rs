//! DML coordination from an rN-only gateway over real mTLS TCP.
//!
//! The gateway under test hosts only the data range r1 and carries a
//! read-only range-0 replica. Ranges r0 and r2 live behind one remote
//! [`HostedRangeService`], so timestamps mint through the real
//! `RangeRequest::Tso(Grant)` wire path and DML for r2 forwards through the
//! remote-session, prewrite/resolve, and GTM RPCs.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use assert2::assert;
use async_trait::async_trait;
use crabka_gres_control::{
    HashPlacement, RangeBoundary, RangeLayoutEntry, RangeLifecycle, SqlUser, TenantId,
    TenantRecord, TenantState,
};
use crabka_gres_ranges::{
    BarrierError, FramedTcpClient, HostedRangeService, MemoryTsoHorizon, MultiRangeTenant,
    MultiRangeTenantConfig, Range0EndSampler, Range0Tail, RangeId, RangeRegistry, RangeService,
    RangeTlsClientConfig, RangeTlsServerConfig, ReadOnlyRange0Replica, TenantName, serve_tls,
    tso_rpc_from_horizon,
};
use crabka_pgexec::SqlEngine;
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Engine, QueryResult, Session, TxStatus};

/// Hash-sharded table whose buckets straddle the r1/r2 boundary.
const SHARDED_TABLE: &str = "t80";
/// Plain table owned entirely by the gateway-hosted range r1.
const LOCAL_PLAIN_TABLE: &str = "t60";
/// Plain table owned entirely by the remote range r2.
const REMOTE_PLAIN_TABLE: &str = "t90";
const HASH_BUCKET_COUNT: u32 = 16;
/// First hash bucket of [`SHARDED_TABLE`] owned by the remote range r2.
const REMOTE_BUCKET_START: u32 = 8;
/// r0 = `[t0, t60)`, r1 = `[t60, t80 bucket 8)`, r2 = `[t80 bucket 8, ∞)`.
const BOUNDARIES: &str = "0,60,80:8:0";

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
                tenant: "tenant_rn_only_dml".to_string(),
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
                    "CN=test-client,OU=integration,O=crabka".to_string(),
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

/// The replica shares range 0's store `Arc`, so it is always current and the
/// barrier needs no committed frames beyond offset -1.
struct ZeroLagRange0End;

#[async_trait]
impl Range0EndSampler for ZeroLagRange0End {
    async fn sample_end_after_call_begins(&self) -> Result<i64, BarrierError> {
        Ok(-1)
    }
}

struct RnOnlyTopology {
    gateway: MultiRangeTenant,
    /// The remote node's range-0 engine (catalog owner, GTM, TSO backing store).
    catalog_engine: SqlEngine,
    /// The remote node's r2 data engine, sharing range 0's catalog store.
    remote_engine: SqlEngine,
    _tls: MtlsFixture,
}

/// Start the shared topology: a remote node serving `{r0, r2}` over mTLS TCP
/// and an rN-only gateway hosting r1 with a zero-lag range-0 replica. The
/// gateway gets no injected timestamp oracle, so assembly installs the real
/// remote TSO wire path from its registry and range client.
async fn rn_only_topology(tenant: &str, record_tenant: &str) -> RnOnlyTopology {
    let fixture = MtlsFixture::new();
    let catalog_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
    let mut catalog_engine = SqlEngine::with_kv(Arc::clone(&catalog_kv)).expect("range-0 engine");
    catalog_engine
        .init_gtm_coordinator()
        .expect("range-0 GTM coordinator");
    let mut remote_engine = SqlEngine::new();
    catalog_engine.share_gtm_to(&mut remote_engine);
    remote_engine.set_catalog_kv(catalog_engine.kv_handle());

    let mut ddl = catalog_engine.connect();
    for statement in [
        format!(
            "CREATE TABLE {SHARDED_TABLE} (id int4, note int4) \
             SHARDED BY HASH (id) BUCKETS {HASH_BUCKET_COUNT}"
        ),
        format!("CREATE TABLE {LOCAL_PLAIN_TABLE} (id int4)"),
        format!("CREATE TABLE {REMOTE_PLAIN_TABLE} (id int4)"),
    ] {
        ddl.simple_query(&statement)
            .await
            .expect("create table on the range-0 owner");
    }
    drop(ddl);

    let horizon = MemoryTsoHorizon::new(catalog_engine.kv_handle(), 1);
    let persisted_max_ts = horizon.load_max_ts().expect("load TSO horizon");
    let tso = tso_rpc_from_horizon(horizon.clone(), horizon, 1, persisted_max_ts)
        .expect("build durable TSO rpc");
    let mut record = tenant_record(record_tenant);
    let registry = RangeRegistry::from_tenant_record(&record).expect("registry");
    let range_client = FramedTcpClient::with_tls(fixture.client.clone()).expect("mTLS client");
    let service = HostedRangeService::new(BTreeMap::from([
        (RangeId::COORDINATOR, catalog_engine.clone_handle()),
        (RangeId::new(2), remote_engine.clone_handle()),
    ]))
    .with_tso(tso)
    .with_timestamp_primary_remote(registry.clone(), range_client.clone());
    let remote_address = spawn_tls(Arc::new(service), fixture.server.clone()).await;

    let replica =
        ReadOnlyRange0Replica::new(Range0Tail::new(catalog_kv), Arc::new(ZeroLagRange0End));
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse(tenant).expect("tenant name"),
        BOUNDARIES,
    )
    .expect("config")
    .with_hosted_ranges(vec![RangeId::new(1)])
    .expect("host r1 only")
    .with_read_only_range0_replica(replica)
    .with_range_registry(registry.clone())
    .with_range_client(range_client);
    let gateway =
        MultiRangeTenant::start_with_engine_factory(config, |_dir, _range_id| Ok(SqlEngine::new()))
            .expect("rN-only gateway")
            .0;

    // The gateway node also serves its hosted range: secondary participants
    // authenticate a timestamp primary at r1's registry endpoint, so r1 must be
    // reachable over the same mTLS transport.
    let gateway_service = HostedRangeService::new(BTreeMap::from([(
        RangeId::new(1),
        gateway.hosted_range_engines()[&RangeId::new(1)].clone_handle(),
    )]));
    let gateway_address = spawn_tls(Arc::new(gateway_service), fixture.server.clone()).await;
    record.ranges[0].endpoint = remote_address.to_string();
    record.ranges[1].endpoint = gateway_address.to_string();
    record.ranges[2].endpoint = remote_address.to_string();
    registry
        .refresh_from_tenant_record(&record)
        .await
        .expect("publish live endpoints");
    RnOnlyTopology {
        gateway,
        catalog_engine,
        remote_engine,
        _tls: fixture,
    }
}

/// Record with placeholder endpoints; live addresses are published through
/// [`RangeRegistry::refresh_from_tenant_record`] once the services are up.
fn tenant_record(record_tenant: &str) -> TenantRecord {
    TenantRecord::new(
        1,
        TenantId::try_from(record_tenant).expect("tenant id"),
        crabka_gres_control::TenantName::try_from(record_tenant).expect("record tenant"),
        TenantState::Active,
        SqlUser::try_from("alice").expect("user"),
        "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
        1,
    )
    .expect("record")
    .with_hash_placements(vec![HashPlacement {
        table_id: 80,
        hash_columns: vec!["id".to_string()],
        bucket_count: HASH_BUCKET_COUNT,
        co_location_group: None,
    }])
    .expect("hash placements")
    .with_range_layout(vec![
        RangeLayoutEntry {
            range_id: 0,
            end_key: Some(RangeBoundary::table_start(60)),
            endpoint: "127.0.0.1:1".to_string(),
            wal_generation: 1,
            lifecycle: RangeLifecycle::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: Some(RangeBoundary::hash(80, REMOTE_BUCKET_START, 0)),
            endpoint: "127.0.0.1:1".to_string(),
            wal_generation: 1,
            lifecycle: RangeLifecycle::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 2,
            end_key: None,
            endpoint: "127.0.0.1:1".to_string(),
            wal_generation: 1,
            lifecycle: RangeLifecycle::default(),
            retirement: None,
        },
    ])
    .expect("layout")
}

fn bucket_of(id: i32) -> u32 {
    crabka_pgkv::key::hash_bucket(&id.to_be_bytes(), HASH_BUCKET_COUNT)
        .expect("power-of-two bucket count")
}

/// The first `count` shard-key values whose hash bucket is owned by the remote
/// range r2 (`remote`) or by the gateway-hosted range r1 (`!remote`).
fn sharded_ids(remote: bool, count: usize) -> Vec<i32> {
    let ids = (1..=10_000)
        .filter(|id| (bucket_of(*id) >= REMOTE_BUCKET_START) == remote)
        .take(count)
        .collect::<Vec<_>>();
    assert!(ids.len() == count, "not enough shard-key candidates");
    ids
}

fn text_rows(results: &[QueryResult]) -> Vec<Vec<Option<String>>> {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected one row result: {results:?}")
    };
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    cell.as_ref()
                        .map(|cell| String::from_utf8(cell.text.to_vec()).expect("UTF-8 cell"))
                })
                .collect()
        })
        .collect()
}

fn sorted_rows(results: &[QueryResult]) -> Vec<Vec<Option<String>>> {
    let mut rows = text_rows(results);
    rows.sort();
    rows
}

fn expected_rows(rows: &[&[i32]]) -> Vec<Vec<Option<String>>> {
    let mut expected = rows
        .iter()
        .map(|row| row.iter().map(|value| Some(value.to_string())).collect())
        .collect::<Vec<Vec<Option<String>>>>();
    expected.sort();
    expected
}

fn catalog_table_id(engine: &SqlEngine, name: &str) -> u32 {
    crabka_pgcatalog::list_tables(engine.catalog_kv())
        .expect("list catalog tables")
        .into_iter()
        .find(|table| table.name.name == name)
        .expect("table is catalog-visible")
        .id
}

/// Hash buckets of every committed timestamp version of `table_id` in `kv`.
fn committed_hash_buckets(kv: &dyn Kv, table_id: u32) -> Vec<u32> {
    kv.scan_range(&[], &[u8::MAX])
        .expect("scan storage")
        .into_iter()
        .filter_map(|(key, value)| match crabka_pgkv::key::classify_key(&key) {
            crabka_pgkv::key::KeyClass::HashPrimaryVersion {
                table_id: version_table,
                bucket,
                ..
            } if version_table == table_id => {
                let version =
                    crabka_pgmvcc::version::decode_ts_tuple(&value).expect("decode ts tuple");
                matches!(
                    version.state,
                    crabka_pgmvcc::version::TsVersionState::Committed { .. }
                )
                .then_some(bucket)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rn_only_gateway_scatters_autocommit_sharded_insert_across_local_and_remote_ranges() {
    let topology = rn_only_topology("rn_only_dml_scatter", "rn-only-dml-scatter").await;
    let mut session = topology.gateway.connect();
    let local_id = sharded_ids(false, 1)[0];
    let remote_id = sharded_ids(true, 1)[0];

    let results = session
        .simple_query(&format!(
            "INSERT INTO {SHARDED_TABLE} VALUES ({local_id}, 1), ({remote_id}, 2)"
        ))
        .await
        .expect("autocommit scatter insert across a hosted and a remote range");
    assert!(
        results
            == vec![QueryResult::Command {
                tag: "INSERT 0 2".to_string()
            }]
    );

    let rows = session
        .simple_query(&format!("SELECT id, note FROM {SHARDED_TABLE}"))
        .await
        .expect("gateway scatter read");
    assert!(sorted_rows(&rows) == expected_rows(&[&[local_id, 1], &[remote_id, 2]]));

    // Prove the scatter: one committed version lives in the REMOTE r2 engine's
    // own KV and one in the LOCAL r1 engine's KV, each in its owned buckets.
    let table_id = catalog_table_id(&topology.catalog_engine, SHARDED_TABLE);
    let remote_buckets =
        committed_hash_buckets(topology.remote_engine.kv_handle().as_ref(), table_id);
    assert!(
        remote_buckets.len() == 1,
        "remote committed row: {remote_buckets:?}"
    );
    assert!(
        remote_buckets
            .iter()
            .all(|bucket| *bucket >= REMOTE_BUCKET_START)
    );
    let hosted = topology.gateway.hosted_range_engines();
    let local_buckets =
        committed_hash_buckets(hosted[&RangeId::new(1)].kv_handle().as_ref(), table_id);
    assert!(
        local_buckets.len() == 1,
        "local committed row: {local_buckets:?}"
    );
    assert!(
        local_buckets
            .iter()
            .all(|bucket| *bucket < REMOTE_BUCKET_START)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rn_only_gateway_commits_and_rolls_back_explicit_timestamp_transactions_with_remote_participants()
 {
    let topology = rn_only_topology("rn_only_dml_explicit_ts", "rn-only-dml-explicit-ts").await;
    let mut session = topology.gateway.connect();
    let local = sharded_ids(false, 3);
    let remote = sharded_ids(true, 2);

    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query(&format!(
            "INSERT INTO {SHARDED_TABLE} VALUES ({}, 10), ({}, 20)",
            local[0], remote[0]
        ))
        .await
        .expect("scatter write with a remote participant");
    session
        .simple_query(&format!(
            "INSERT INTO {SHARDED_TABLE} VALUES ({}, 30)",
            local[1]
        ))
        .await
        .expect("second write in the same timestamp transaction");
    session
        .simple_query("COMMIT")
        .await
        .expect("commit through the remote timestamp oracle");
    assert!(session.tx_status() == TxStatus::Idle);
    let committed = expected_rows(&[&[local[0], 10], &[remote[0], 20], &[local[1], 30]]);
    let rows = session
        .simple_query(&format!("SELECT id, note FROM {SHARDED_TABLE}"))
        .await
        .expect("read committed rows");
    assert!(sorted_rows(&rows) == committed);

    session
        .simple_query("BEGIN")
        .await
        .expect("begin the rollback transaction");
    session
        .simple_query(&format!(
            "INSERT INTO {SHARDED_TABLE} VALUES ({}, 40), ({}, 50)",
            local[2], remote[1]
        ))
        .await
        .expect("scatter write that will be rolled back");
    session
        .simple_query("ROLLBACK")
        .await
        .expect("rollback with a remote participant");
    assert!(session.tx_status() == TxStatus::Idle);
    let rows = session
        .simple_query(&format!("SELECT id, note FROM {SHARDED_TABLE}"))
        .await
        .expect("read after rollback");
    assert!(sorted_rows(&rows) == committed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rn_only_gateway_forwards_ordinary_dml_for_unhosted_and_hosted_ranges() {
    let topology = rn_only_topology("rn_only_dml_ordinary", "rn-only-dml-ordinary").await;
    let mut session = topology.gateway.connect();

    for (table, value) in [(LOCAL_PLAIN_TABLE, 6), (REMOTE_PLAIN_TABLE, 9)] {
        let results = session
            .simple_query(&format!("INSERT INTO {table} VALUES ({value})"))
            .await
            .expect("ordinary autocommit insert through the gateway");
        assert!(
            results
                == vec![QueryResult::Command {
                    tag: "INSERT 0 1".to_string()
                }]
        );
        let rows = session
            .simple_query(&format!("SELECT id FROM {table}"))
            .await
            .expect("gateway read");
        assert!(sorted_rows(&rows) == expected_rows(&[&[value]]));
    }

    // The forwarded row genuinely lives on the remote r2 engine.
    let mut owner_session = topology.remote_engine.connect();
    let rows = owner_session
        .simple_query(&format!("SELECT id FROM {REMOTE_PLAIN_TABLE}"))
        .await
        .expect("owner-side read");
    assert!(sorted_rows(&rows) == expected_rows(&[&[9]]));
}

/// The contended-update regression: an update-heavy autocommit workload
/// forwarded to an unhosted range must not grow the hot row's physical
/// version chain without bound. The owner engine prunes superseded versions
/// inside each forwarded statement's own commit batch (there is no vacuum on
/// multi-range engines), so the chain stays O(1) while throughput-relevant
/// scans stay O(chain).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rn_only_gateway_forwarded_update_loop_keeps_owner_chain_bounded() {
    let topology = rn_only_topology("rn_only_dml_prune", "rn-only-dml-prune").await;
    let mut session = topology.gateway.connect();

    session
        .simple_query(&format!("INSERT INTO {REMOTE_PLAIN_TABLE} VALUES (1)"))
        .await
        .expect("seed the hot row through the gateway");
    for _ in 0..40 {
        let results = session
            .simple_query(&format!(
                "UPDATE {REMOTE_PLAIN_TABLE} SET id = 1 WHERE id = 1"
            ))
            .await
            .expect("forwarded autocommit update");
        assert!(
            results
                == vec![QueryResult::Command {
                    tag: "UPDATE 1".to_string()
                }]
        );
    }

    // The row is still current and unique through the gateway.
    let rows = session
        .simple_query(&format!("SELECT id FROM {REMOTE_PLAIN_TABLE}"))
        .await
        .expect("gateway read");
    assert!(sorted_rows(&rows) == expected_rows(&[&[1]]));

    // Superseded versions were reclaimed on the owner: the physical chain in
    // the remote r2 store stays bounded instead of holding all 40 versions.
    let table_id = catalog_table_id(&topology.catalog_engine, REMOTE_PLAIN_TABLE);
    let versions = topology
        .remote_engine
        .kv_handle()
        .scan_prefix(&crabka_pgkv::key::table_prefix(table_id))
        .expect("scan owner store")
        .iter()
        .filter(|(_, value)| crabka_pgmvcc::version::decode_tuple(value).is_ok())
        .count();
    assert!(versions <= 3, "owner chain grew to {versions} versions");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rn_only_gateway_commits_ordinary_multi_range_explicit_transaction() {
    let topology = rn_only_topology("rn_only_dml_global", "rn-only-dml-global").await;
    let mut session = topology.gateway.connect();

    session
        .simple_query("BEGIN")
        .await
        .expect("begin an ordinary transaction from the rN-only gateway");
    session
        .simple_query(&format!("INSERT INTO {LOCAL_PLAIN_TABLE} VALUES (61)"))
        .await
        .expect("hosted participant write");
    session
        .simple_query(&format!("INSERT INTO {REMOTE_PLAIN_TABLE} VALUES (91)"))
        .await
        .expect("remote participant write");
    session
        .simple_query("COMMIT")
        .await
        .expect("global commit driven from a non-r0 gateway");
    assert!(session.tx_status() == TxStatus::Idle);

    let rows = session
        .simple_query(&format!("SELECT id FROM {LOCAL_PLAIN_TABLE}"))
        .await
        .expect("hosted row is visible after the global commit");
    assert!(sorted_rows(&rows) == expected_rows(&[&[61]]));
    let rows = session
        .simple_query(&format!("SELECT id FROM {REMOTE_PLAIN_TABLE}"))
        .await
        .expect("remote row is visible after the global commit");
    assert!(sorted_rows(&rows) == expected_rows(&[&[91]]));

    let mut owner_session = topology.remote_engine.connect();
    let rows = owner_session
        .simple_query(&format!("SELECT id FROM {REMOTE_PLAIN_TABLE}"))
        .await
        .expect("owner-side committed read");
    assert!(sorted_rows(&rows) == expected_rows(&[&[91]]));
}

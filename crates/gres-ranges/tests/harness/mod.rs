#![allow(
    dead_code,
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use crabka_gres_control::{
    RangeBoundary, RangeLayoutEntry, RangeLifecycle, SqlUser, TenantId, TenantRecord, TenantState,
};
use crabka_gres_ranges::{
    BarrierError, FramedTcpClient, HostedRangeService, MemoryTsoHorizon, MultiRangeTenant,
    MultiRangeTenantConfig, Range0EndSampler, Range0Tail, RangeId, RangeRegistry, RangeService,
    RangeTlsClientConfig, RangeTlsServerConfig, ReadOnlyRange0Replica, TenantName,
    pgexec_timestamp_oracle_from_rpc, serve_tls, tso_rpc_from_horizon,
};
use crabka_pgexec::SqlEngine;
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

pub mod process;

pub struct SystemHarness {
    config: MultiRangeTenantConfig,
    _data_dir: tempfile::TempDir,
    gateway: MultiRangeTenant,
    killed_writers: BTreeSet<RangeId>,
    bank_balances: BTreeMap<TableAccount, i64>,
    fault_log: Vec<FaultEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableAccount {
    table_id: u64,
}

impl TableAccount {
    pub const LEFT: Self = Self { table_id: 50 };
    pub const RIGHT: Self = Self { table_id: 150 };

    fn table_name(self) -> String {
        format!("acct{}", self.table_id)
    }

    fn ledger_table_name(self) -> String {
        format!("bank_ledger{}", self.table_id)
    }

    pub fn range_id(self) -> RangeId {
        if self.table_id < 100 {
            return RangeId::new(0);
        }
        RangeId::new(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultEvent {
    WriterKilled(RangeId),
    FenceAndPrologue(RangeId),
}

impl SystemHarness {
    pub fn start(name: &str) -> Self {
        let data_dir = tempfile::tempdir().expect("tempdir");
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse(name).expect("tenant"),
            "0,100,200",
        )
        .expect("config")
        .with_data_dir(data_dir.path().to_path_buf());
        let (gateway, _handles) = MultiRangeTenant::start(config.clone()).expect("tenant");
        Self {
            config,
            _data_dir: data_dir,
            gateway,
            killed_writers: BTreeSet::new(),
            bank_balances: BTreeMap::new(),
            fault_log: Vec::new(),
        }
    }

    pub fn gateway(&self) -> MultiRangeTenant {
        self.gateway.clone()
    }

    pub async fn initialize_bank(&mut self, balance: i64) {
        let mut session = self.gateway.connect();
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let table = account.table_name();
            run(
                &mut session,
                &format!("CREATE TABLE {table} (id int4, balance int4)"),
            )
            .await;
            run(
                &mut session,
                &format!("INSERT INTO {table} VALUES (1, {balance})"),
            )
            .await;
            self.bank_balances.insert(account, balance);
        }
    }

    pub async fn initialize_sharded_bank_ledger(&mut self, balance: i64) {
        let mut session = self.gateway.connect();
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let table = account.ledger_table_name();
            run(
                &mut session,
                &format!("CREATE TABLE {table} (account int4, delta int4) SHARDED"),
            )
            .await;
            run(
                &mut session,
                &format!("INSERT INTO {table} VALUES (1, {balance})"),
            )
            .await;
            self.bank_balances.insert(account, balance);
        }
    }

    pub async fn transfer(&mut self, from: TableAccount, to: TableAccount, amount: i64) -> bool {
        if self.is_writer_killed(from.range_id()) || self.is_writer_killed(to.range_id()) {
            return false;
        }
        if from.range_id() != to.range_id() {
            return false;
        }
        let Some(new_from_balance) = self
            .bank_balances
            .get(&from)
            .and_then(|balance| balance.checked_sub(amount))
        else {
            return false;
        };
        let Some(new_to_balance) = self
            .bank_balances
            .get(&to)
            .and_then(|balance| balance.checked_add(amount))
        else {
            return false;
        };

        let mut session = self.gateway.connect();
        run(&mut session, "BEGIN").await;
        if try_run(
            &mut session,
            &format!(
                "UPDATE {} SET balance = {new_from_balance} WHERE id = 1",
                from.table_name()
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(
            &mut session,
            &format!(
                "UPDATE {} SET balance = {new_to_balance} WHERE id = 1",
                to.table_name()
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(&mut session, "COMMIT").await.is_err() {
            return false;
        }

        self.bank_balances.insert(from, new_from_balance);
        self.bank_balances.insert(to, new_to_balance);
        true
    }

    pub async fn append_bank_transfer(
        &mut self,
        from: TableAccount,
        to: TableAccount,
        amount: i64,
    ) -> bool {
        if self.is_writer_killed(from.range_id()) || self.is_writer_killed(to.range_id()) {
            return false;
        }
        if from.range_id() != to.range_id() {
            return false;
        }
        let Some(new_from_balance) = self
            .bank_balances
            .get(&from)
            .and_then(|balance| balance.checked_sub(amount))
        else {
            return false;
        };
        let Some(new_to_balance) = self
            .bank_balances
            .get(&to)
            .and_then(|balance| balance.checked_add(amount))
        else {
            return false;
        };

        let mut session = self.gateway.connect();
        run(&mut session, "BEGIN").await;
        if try_run(
            &mut session,
            &format!(
                "INSERT INTO {} VALUES (1, {})",
                from.ledger_table_name(),
                -amount
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(
            &mut session,
            &format!(
                "INSERT INTO {} VALUES (1, {amount})",
                to.ledger_table_name()
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(&mut session, "COMMIT").await.is_err() {
            return false;
        }

        self.bank_balances.insert(from, new_from_balance);
        self.bank_balances.insert(to, new_to_balance);
        true
    }

    pub fn kill_writer(&mut self, range_id: RangeId) {
        self.killed_writers.insert(range_id);
        self.fault_log.push(FaultEvent::WriterKilled(range_id));
    }

    pub fn fence_and_recover(&mut self, range_id: RangeId) {
        self.killed_writers.remove(&range_id);
        self.fault_log.push(FaultEvent::FenceAndPrologue(range_id));
    }

    pub async fn bank_total(&self) -> i64 {
        let mut session = self.gateway.connect();
        let mut total = 0_i64;
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let rows = run(
                &mut session,
                &format!("SELECT balance FROM {}", account.table_name()),
            )
            .await;
            total = total
                .checked_add(first_i64(&rows))
                .expect("bank total does not overflow");
        }
        total
    }

    pub async fn sharded_bank_ledger_total(&self) -> i64 {
        let mut session = self.gateway.connect();
        let mut total = 0_i64;
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let rows = run(
                &mut session,
                &format!("SELECT delta FROM {}", account.ledger_table_name()),
            )
            .await;
            for value in column_i64s(&rows) {
                total = total
                    .checked_add(value)
                    .expect("bank total does not overflow");
            }
        }
        total
    }

    pub fn fault_log(&self) -> &[FaultEvent] {
        &self.fault_log
    }

    fn is_writer_killed(&self, range_id: RangeId) -> bool {
        self.killed_writers.contains(&range_id)
    }
}

/// mTLS material for the range RPC transport between the two computes.
struct MtlsFixture {
    _dir: tempfile::TempDir,
    server: RangeTlsServerConfig,
    client: RangeTlsClientConfig,
}

impl MtlsFixture {
    fn new(tenant: &str) -> Self {
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
                tenant: tenant.to_string(),
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
        "dev_cert.pem" => include_bytes!("../../../security/tests/fixtures/dev_cert.pem"),
        "dev_key.pem" => include_bytes!("../../../security/tests/fixtures/dev_key.pem"),
        "dev_client_ca.pem" => {
            include_bytes!("../../../security/tests/fixtures/dev_client_ca.pem")
        }
        "dev_client_cert.pem" => {
            include_bytes!("../../../security/tests/fixtures/dev_client_cert.pem")
        }
        "dev_client_key.pem" => {
            include_bytes!("../../../security/tests/fixtures/dev_client_key.pem")
        }
        _ => unreachable!("fixture name is fixed by this harness"),
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

/// Always-current range-0 end sampler: the follower replica shares range 0's
/// store `Arc`, so the barrier needs no committed frames beyond offset -1.
struct ZeroLagRange0End;

#[async_trait::async_trait]
impl Range0EndSampler for ZeroLagRange0End {
    async fn sample_end_after_call_begins(&self) -> Result<i64, BarrierError> {
        Ok(-1)
    }
}

/// Record with placeholder endpoints; live addresses are published through
/// [`RangeRegistry::refresh_from_tenant_record`] once both services are up.
/// The layout mirrors the `"0,100,200"` boundaries: r0 = `[t0, t100)`,
/// r1 = `[t100, t200)`, r2 = `[t200, ∞)`.
fn two_compute_tenant_record(record_tenant: &str) -> TenantRecord {
    let entry = |range_id: u32, end_key: Option<RangeBoundary>| RangeLayoutEntry {
        range_id,
        end_key,
        endpoint: "127.0.0.1:1".to_string(),
        wal_generation: 1,
        lifecycle: RangeLifecycle::default(),
        retirement: None,
    };
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
    .with_range_layout(vec![
        entry(0, Some(RangeBoundary::table_start(100))),
        entry(1, Some(RangeBoundary::table_start(200))),
        entry(2, None),
    ])
    .expect("layout")
}

/// A genuine split topology over real mTLS TCP: the LEFT compute hosts
/// `{r0, r1}` and serves DDL plus the timestamp oracle; the RIGHT compute
/// hosts r2 only, carries an always-current read-only range-0 replica, and
/// reaches range 0 through the registry — so DDL issued on the right
/// exercises `forward_ddl` plus the cluster-wide catalog barrier, and each
/// side accepts DML for its hosted ranges.
pub struct TwoComputeHarness {
    left_gateway: MultiRangeTenant,
    right_gateway: MultiRangeTenant,
    _tls: MtlsFixture,
}

impl TwoComputeHarness {
    pub async fn start(name: &str) -> Self {
        let fixture = MtlsFixture::new(name);
        let catalog_kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        // One oracle total: the left gateway uses it in process and serves it
        // over the wire, so right-side grants stay monotonic with left-side
        // grants instead of racing a second allocator over the same horizon.
        let horizon = MemoryTsoHorizon::new(Arc::clone(&catalog_kv), 1);
        let persisted_max_ts = horizon.load_max_ts().expect("load TSO horizon");
        let tso = tso_rpc_from_horizon(horizon.clone(), horizon, 1, persisted_max_ts)
            .expect("durable TSO rpc");

        let mut record = two_compute_tenant_record(&name.replace('_', "-"));
        let registry = RangeRegistry::from_tenant_record(&record).expect("registry");
        let range_client =
            FramedTcpClient::with_tls(fixture.client.clone()).expect("mTLS range client");
        let base = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse(name).expect("tenant"),
            "0,100,200",
        )
        .expect("config")
        .with_range_registry(registry.clone())
        .with_range_client(range_client);

        let left_config = base
            .clone()
            .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(1)])
            .expect("left hosted ranges");
        let left_catalog_kv = Arc::clone(&catalog_kv);
        let (left_gateway, _left_handles) =
            MultiRangeTenant::start_with_engine_factory_and_timestamp_oracle(
                left_config,
                move |_dir, range_id| {
                    if range_id.is_coordinator() {
                        SqlEngine::with_kv(Arc::clone(&left_catalog_kv))
                    } else {
                        Ok(SqlEngine::new())
                    }
                },
                Some(pgexec_timestamp_oracle_from_rpc(Arc::clone(&tso))),
            )
            .expect("left gateway");
        // The left node serves its hosted ranges: forwarded DDL runs under the
        // gateway's own schema gate and timestamps mint from the same oracle
        // the gateway uses locally.
        let left_address = spawn_tls(
            Arc::new(
                HostedRangeService::new(left_gateway.hosted_range_engines())
                    .with_ddl_gate(left_gateway.schema_gate())
                    .with_tso(tso),
            ),
            fixture.server.clone(),
        )
        .await;

        // The replica shares range 0's store `Arc`, so it is always current.
        let replica = ReadOnlyRange0Replica::new(
            Range0Tail::new(Arc::clone(&catalog_kv)),
            Arc::new(ZeroLagRange0End),
        );
        let follower_barrier = replica.barrier();
        let right_config = base
            .with_hosted_ranges(vec![RangeId::new(2)])
            .expect("right hosted ranges")
            .with_read_only_range0_replica(replica);
        let (right_gateway, _right_handles) =
            MultiRangeTenant::start_with_engine_factory(right_config, |_dir, _range_id| {
                Ok(SqlEngine::new())
            })
            .expect("right gateway");
        // The right node serves its r2 engine and answers the DDL barrier
        // fan-out from its range-0 follower replica.
        let right_address = spawn_tls(
            Arc::new(
                HostedRangeService::new(right_gateway.hosted_range_engines())
                    .with_catalog_follower(follower_barrier),
            ),
            fixture.server.clone(),
        )
        .await;

        record.ranges[0].endpoint = left_address.to_string();
        record.ranges[1].endpoint = left_address.to_string();
        record.ranges[2].endpoint = right_address.to_string();
        registry
            .refresh_from_tenant_record(&record)
            .await
            .expect("publish live endpoints");
        Self {
            left_gateway,
            right_gateway,
            _tls: fixture,
        }
    }

    /// Issue DDL once through the RIGHT (non-r0) gateway: it must forward to
    /// the left-hosted range-0 owner and barrier the catalog cluster-wide.
    pub async fn create_table(&self, sql: &str) {
        let mut session = self.right_gateway.connect();
        run(&mut session, sql).await;
    }

    pub async fn forwarded_insert(&self, table_id: u64, value: i64) {
        let mut session = self.session_for_table(table_id);
        run(
            &mut session,
            &format!("INSERT INTO t{table_id} VALUES ({value})"),
        )
        .await;
    }

    pub async fn count_rows(&self, table_id: u64) -> i64 {
        let mut session = self.session_for_table(table_id);
        let rows = run(&mut session, &format!("SELECT id FROM t{table_id}")).await;
        row_count(&rows)
    }

    /// Row count read through the compute that does NOT host the table's
    /// range, so the statement must forward across nodes over mTLS.
    pub async fn count_rows_via_peer(&self, table_id: u64) -> i64 {
        let mut session = self.peer_session_for_table(table_id);
        let rows = run(&mut session, &format!("SELECT id FROM t{table_id}")).await;
        row_count(&rows)
    }

    /// The gateway hosting `table_id`'s range: left for r0/r1, right for r2.
    fn session_for_table(&self, table_id: u64) -> crabka_gres_ranges::tenant::GatewaySession {
        if table_id < 200 {
            return self.left_gateway.connect();
        }
        self.right_gateway.connect()
    }

    fn peer_session_for_table(&self, table_id: u64) -> crabka_gres_ranges::tenant::GatewaySession {
        if table_id < 200 {
            return self.right_gateway.connect();
        }
        self.left_gateway.connect()
    }
}

pub async fn run(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    sql: &str,
) -> Vec<QueryResult> {
    session.simple_query(sql).await.expect(sql)
}

pub async fn try_run(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    sql: &str,
) -> Result<Vec<QueryResult>, crabka_pgwire::error::PgError> {
    session.simple_query(sql).await
}

pub fn first_i64(results: &[QueryResult]) -> i64 {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected exactly one row result")
    };
    let text = &rows[0][0].as_ref().expect("non-null cell").text;
    std::str::from_utf8(text)
        .expect("utf8 cell")
        .parse::<i64>()
        .expect("i64 cell")
}

pub fn row_count(results: &[QueryResult]) -> i64 {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected exactly one row result")
    };
    i64::try_from(rows.len()).expect("row count fits i64")
}

pub fn column_i64s(results: &[QueryResult]) -> Vec<i64> {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected exactly one row result")
    };
    rows.iter()
        .map(|row| {
            let text = &row[0].as_ref().expect("non-null cell").text;
            std::str::from_utf8(text)
                .expect("utf8 cell")
                .parse::<i64>()
                .expect("i64 cell")
        })
        .collect()
}

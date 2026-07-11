use std::{
    collections::BTreeMap,
    fs::File,
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_gres_control::{
    RangeBoundary, RangeLayoutEntry, Registry, SqlUser, TenantId, TenantName, TenantRecord,
    TenantState,
};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
};

const PASSWORD: &str = "process-secret";

pub struct ProcessHarness {
    _root: tempfile::TempDir,
    _broker: BrokerHandle,
    bootstrap: String,
    tenant: String,
    tls: TlsPaths,
    r0: ProcessNode,
    r1: ProcessNode,
}

struct ProcessNode {
    range: u32,
    sql_port: u16,
    range_port: u16,
    _cache_dir: PathBuf,
    log_path: PathBuf,
    child: Child,
}

impl ProcessHarness {
    pub async fn start(name: &str) -> Self {
        let root = tempfile::tempdir().expect("process harness root");
        let broker_dir = root.path().join("broker");
        let broker = Broker::start(BrokerConfig::for_tests(broker_dir))
            .await
            .expect("start real broker");
        let bootstrap = broker.listen_addr().to_string();
        let tenant = name.to_owned();
        let r0_sql = reserve_port().await;
        let r0_range = reserve_port().await;
        let r1_sql = reserve_port().await;
        let r1_range = reserve_port().await;
        let tls = write_tls_fixture(root.path());
        provision_control(&bootstrap, &tenant, r0_range, r1_range).await;

        let r0 = spawn_node(
            root.path(),
            &bootstrap,
            &tenant,
            0,
            r0_sql,
            r0_range,
            "r0",
            &tls,
        );
        let r1 = spawn_node(
            root.path(),
            &bootstrap,
            &tenant,
            1,
            r1_sql,
            r1_range,
            "r1",
            &tls,
        );
        let mut harness = Self {
            _root: root,
            _broker: broker,
            bootstrap,
            tenant,
            tls,
            r0,
            r1,
        };
        harness.wait_ready(0).await;
        harness.wait_ready(1).await;
        harness
    }

    pub async fn sql(&self, range: u32) -> tokio_postgres::Client {
        connect(self.node(range).sql_port, &self.tenant).await
    }

    pub fn pid(&self, range: u32) -> u32 {
        self.node(range).child.id().expect("child pid")
    }

    pub async fn create_table_on_all(&mut self, sql: &str) {
        self.stop_node(0).await;
        self.stop_node(1).await;

        self.restart_node(1, "r0,r1").await;
        self.sql(1)
            .await
            .simple_query(sql)
            .await
            .expect("create table in participant catalog");
        self.stop_node(1).await;

        self.restart_node(0, "r0").await;
        self.restart_node(1, "r1").await;
    }

    pub async fn kill_and_restart(&mut self, range: u32) {
        self.kill(range).await;
        self.restart(range).await;
    }

    pub async fn kill(&mut self, range: u32) {
        self.stop_node(range).await;
    }

    pub async fn restart(&mut self, range: u32) {
        let hosted_ranges = format!("r{range}");
        self.restart_node(range, &hosted_ranges).await;
    }

    async fn stop_node(&mut self, range: u32) {
        let node = self.node_mut(range);
        if node.child.try_wait().expect("child status").is_none() {
            node.child.kill().await.expect("kill compute child");
            let _ = node.child.wait().await;
        }
    }

    async fn restart_node(&mut self, range: u32, hosted_ranges: &str) {
        let (node_range, sql_port, range_port) = {
            let node = self.node(range);
            (node.range, node.sql_port, node.range_port)
        };
        let replacement = spawn_node(
            self._root.path(),
            &self.bootstrap,
            &self.tenant,
            node_range,
            sql_port,
            range_port,
            hosted_ranges,
            &self.tls,
        );
        self.node_mut(range).child = replacement.child;
        self.wait_ready(range).await;
    }

    async fn wait_ready(&mut self, range: u32) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if self
                .node_mut(range)
                .child
                .try_wait()
                .expect("child status")
                .is_some()
            {
                let log_path = &self.node(range).log_path;
                let log = std::fs::read_to_string(log_path).unwrap_or_else(|error| {
                    format!("<read {} failed: {error}>", log_path.display())
                });
                panic!("r{range} child exited; {}:\n{log}", log_path.display());
            }
            if try_connect(self.node(range).sql_port, &self.tenant)
                .await
                .is_some()
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "r{range} readiness timeout"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn node(&self, range: u32) -> &ProcessNode {
        if range == 0 { &self.r0 } else { &self.r1 }
    }
    fn node_mut(&mut self, range: u32) -> &mut ProcessNode {
        if range == 0 {
            &mut self.r0
        } else {
            &mut self.r1
        }
    }
}

impl Drop for ProcessHarness {
    fn drop(&mut self) {
        let _ = self.r0.child.start_kill();
        let _ = self.r1.child.start_kill();
    }
}

struct TlsPaths {
    server_cert: PathBuf,
    server_key: PathBuf,
    ca: PathBuf,
}

fn write_tls_fixture(root: &Path) -> TlsPaths {
    fn write(root: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = root.join(name);
        std::fs::write(&path, bytes).expect("write TLS fixture");
        path
    }
    let ca = crabka_security::ca::generate_cluster_ca("process-range-ca", 1)
        .expect("generate range test CA");
    let peer = crabka_security::ca::issue_broker_cert(
        &ca.cert_pem,
        &ca.key_pem,
        "process-range",
        &[crabka_security::ca::SubjectAltName::Dns(
            "crabka-dev".to_owned(),
        )],
        &[],
        1,
    )
    .expect("issue range peer certificate");
    TlsPaths {
        server_cert: write(root, "range-server.crt", peer.cert_pem.as_bytes()),
        server_key: write(root, "range-server.key", peer.key_pem.as_bytes()),
        ca: write(root, "range-ca.crt", ca.cert_pem.as_bytes()),
    }
}

fn spawn_node(
    root: &Path,
    bootstrap: &str,
    tenant: &str,
    range: u32,
    sql_port: u16,
    range_port: u16,
    hosted_ranges: &str,
    tls: &TlsPaths,
) -> ProcessNode {
    let cache_dir = root.join(format!("r{range}-cache"));
    std::fs::create_dir_all(&cache_dir).expect("cache dir");
    let log_path = root.join(format!("r{range}.log"));
    let stdout = File::create(&log_path).expect("node log");
    let stderr = stdout.try_clone().expect("clone log");
    let binary = gres_binary();
    let child = Command::new(binary)
        .args([
            "--listen",
            &format!("127.0.0.1:{sql_port}"),
            "--substrate-bootstrap",
            bootstrap,
            "--tenant",
            tenant,
            "--cache-dir",
            cache_dir.to_str().expect("cache path"),
            "--ranges",
            "0:0,50:10",
            "--host-ranges",
            hosted_ranges,
            "--range-listen",
            &format!("127.0.0.1:{range_port}"),
            "--range-tls-cert",
            tls.server_cert.to_str().expect("cert"),
            "--range-tls-key",
            tls.server_key.to_str().expect("key"),
            "--range-tls-ca",
            tls.ca.to_str().expect("ca"),
            "--range-tls-server-name",
            "crabka-dev",
            "--range-allowed-principal",
            "CN=process-range",
        ])
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .expect("spawn gres child");
    ProcessNode {
        range,
        sql_port,
        range_port,
        _cache_dir: cache_dir,
        log_path,
        child,
    }
}

async fn provision_control(bootstrap: &str, tenant: &str, r0_port: u16, r1_port: u16) {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin");
    let topics = [
        format!("__gres_wal.{tenant}.r0"),
        format!("__gres_wal.{tenant}.r1"),
    ]
    .into_iter()
    .map(|name| CreateTopicSpec {
        name,
        partitions: 1,
        replicas: 1,
        configs: BTreeMap::new(),
    })
    .collect::<Vec<_>>();
    let outcomes = admin
        .create_topics(&topics, 30_000)
        .await
        .expect("create WAL topics");
    assert!(
        outcomes.iter().all(|outcome| outcome.error.is_none()),
        "WAL topics: {outcomes:?}"
    );
    let verifier =
        crabka_security::scram::PgScramVerifier::generate_with_salt(PASSWORD, 8192, vec![7; 16])
            .expect("verifier");
    let tenant_name = TenantName::try_from(tenant).expect("tenant name");
    let record = TenantRecord::new(
        1,
        TenantId::try_from(tenant).expect("tenant id"),
        tenant_name.clone(),
        TenantState::Active,
        SqlUser::try_from("alice").expect("user"),
        verifier.to_string(),
        1,
    )
    .expect("record")
    .with_range_layout(vec![
        RangeLayoutEntry {
            range_id: 0,
            end_key: Some(RangeBoundary::new(50, 10)),
            endpoint: format!("127.0.0.1:{r0_port}"),
            wal_generation: 0,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: format!("127.0.0.1:{r1_port}"),
            wal_generation: 0,
        },
    ])
    .expect("range layout");
    let mut registry = Registry::connect(bootstrap).await.expect("registry");
    registry.ensure_topic(1).await.expect("registry topic");
    registry.upsert(&record).await.expect("registry record");
    registry
        .upsert_tenant_config(&record, 1)
        .await
        .expect("tenant config");
}

async fn reserve_port() -> u16 {
    let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .expect("bind port");
    listener.local_addr().expect("addr").port()
}

async fn try_connect(port: u16, database: &str) -> Option<tokio_postgres::Client> {
    let connection = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("alice")
        .password(PASSWORD)
        .dbname(database)
        .connect(tokio_postgres::NoTls)
        .await
        .ok()?;
    let (client, driver) = connection;
    tokio::spawn(driver);
    Some(client)
}
async fn connect(port: u16, database: &str) -> tokio_postgres::Client {
    try_connect(port, database).await.expect("connect compute")
}

fn gres_binary() -> PathBuf {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let configured = std::env::var_os("CRABKA_GRES_TEST_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target/debug/crabka-gres"));
    let candidate = configured
        .canonicalize()
        .or_else(|_| workspace.join(&configured).canonicalize())
        .unwrap_or_else(|error| {
            panic!(
                "resolve crabka-gres binary {}: {error}",
                configured.display()
            )
        });
    assert!(
        candidate.is_file(),
        "crabka-gres binary is not a file: {}",
        candidate.display()
    );
    candidate
}

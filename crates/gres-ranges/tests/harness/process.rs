use std::{
    collections::BTreeMap,
    fs::File,
    net::{IpAddr, Ipv4Addr, SocketAddr},
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
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, copy_bidirectional},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

const PASSWORD: &str = "process-secret";

pub struct ProcessHarness {
    _root: tempfile::TempDir,
    _broker: BrokerHandle,
    bootstrap: String,
    tenant: String,
    tls: TlsPaths,
    commit_fault: Option<String>,
    r0_proxy: RangeProxy,
    r1_proxy: RangeProxy,
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
    ready: Option<oneshot::Receiver<ProcessReady>>,
    log_task: JoinHandle<()>,
}

struct ProcessReady {
    sql: SocketAddr,
    range: Option<SocketAddr>,
}

impl ProcessHarness {
    pub async fn start(name: &str) -> Self {
        Self::start_inner(name, None).await
    }

    pub async fn start_with_commit_fault(name: &str, fault: &str) -> Self {
        Self::start_inner(name, Some(fault.to_owned())).await
    }

    async fn start_inner(name: &str, commit_fault: Option<String>) -> Self {
        let root = tempfile::tempdir().expect("process harness root");
        let broker_dir = root.path().join("broker");
        let broker = Broker::start(BrokerConfig::for_tests(broker_dir))
            .await
            .expect("start real broker");
        let bootstrap = broker.listen_addr().to_string();
        let tenant = name.to_owned();
        let r0_proxy = RangeProxy::start().await;
        let r1_proxy = RangeProxy::start().await;
        let tls = write_tls_fixture(root.path());
        provision_control(&bootstrap, &tenant, r0_proxy.port, r1_proxy.port).await;

        let r0 = spawn_node(
            root.path(),
            &bootstrap,
            &tenant,
            0,
            "r0",
            &tls,
            commit_fault.as_deref(),
        );
        let r1 = spawn_node(root.path(), &bootstrap, &tenant, 1, "r1", &tls, None);
        let mut harness = Self {
            _root: root,
            _broker: broker,
            bootstrap,
            tenant,
            tls,
            commit_fault,
            r0_proxy,
            r1_proxy,
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

    pub async fn try_sql(&self, range: u32) -> Option<tokio_postgres::Client> {
        try_connect(self.node(range).sql_port, &self.tenant).await
    }

    pub fn pid(&self, range: u32) -> u32 {
        self.node(range).child.id().expect("child pid")
    }

    pub fn endpoints(&self) -> [(u16, u16); 2] {
        [
            (self.r0.sql_port, self.r0_proxy.port),
            (self.r1.sql_port, self.r1_proxy.port),
        ]
    }

    pub fn log(&self, range: u32) -> String {
        let path = &self.node(range).log_path;
        std::fs::read_to_string(path)
            .unwrap_or_else(|error| format!("<read {} failed: {error}>", path.display()))
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

    pub fn clear_commit_fault(&mut self) {
        self.commit_fault = None;
    }

    pub async fn partition(&self, range: u32) {
        self.proxy(range).set_enabled(false).await;
    }

    pub async fn heal(&self, range: u32) {
        self.proxy(range).set_enabled(true).await;
    }

    pub async fn shutdown(mut self) {
        self.stop_node(0).await;
        self.stop_node(1).await;
    }

    async fn stop_node(&mut self, range: u32) {
        let node = self.node_mut(range);
        if node.child.try_wait().expect("child status").is_none() {
            node.child.kill().await.expect("kill compute child");
            let _ = tokio::time::timeout(Duration::from_secs(5), node.child.wait())
                .await
                .expect("compute child shutdown timeout");
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut node.log_task)
            .await
            .expect("compute log task shutdown timeout");
    }

    async fn restart_node(&mut self, range: u32, hosted_ranges: &str) {
        let node_range = {
            let node = self.node(range);
            node.range
        };
        let replacement = spawn_node(
            self._root.path(),
            &self.bootstrap,
            &self.tenant,
            node_range,
            hosted_ranges,
            &self.tls,
            if range == 0 {
                self.commit_fault.as_deref()
            } else {
                None
            },
        );
        *self.node_mut(range) = replacement;
        self.wait_ready(range).await;
    }

    async fn wait_ready(&mut self, range: u32) {
        let ready = self
            .node_mut(range)
            .ready
            .take()
            .expect("readiness receiver");
        let ready = tokio::time::timeout(Duration::from_secs(30), ready)
            .await
            .expect("compute readiness timeout")
            .expect("compute readiness channel closed");
        let range_addr = ready.range.expect("range listener address");
        let node = self.node_mut(range);
        node.sql_port = ready.sql.port();
        node.range_port = range_addr.port();
        self.proxy(range).set_backend(range_addr.port());
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

    fn proxy(&self, range: u32) -> &RangeProxy {
        if range == 0 {
            &self.r0_proxy
        } else {
            &self.r1_proxy
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

struct RangeProxy {
    port: u16,
    backend: watch::Sender<Option<u16>>,
    commands: mpsc::Sender<ProxyCommand>,
    task: JoinHandle<()>,
}

struct ProxyCommand {
    enabled: bool,
    acknowledged: oneshot::Sender<()>,
}

impl RangeProxy {
    async fn start() -> Self {
        let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .expect("bind range proxy");
        let port = listener.local_addr().expect("range proxy address").port();
        let (enabled, _) = watch::channel(true);
        let (backend, _) = watch::channel(None);
        let task_backend = backend.clone();
        let (commands, mut command_rx) = mpsc::channel::<ProxyCommand>(4);
        let task = tokio::spawn(async move {
            let mut streams = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut frontend, _)) = accepted else { return; };
                        let mut enabled = enabled.subscribe();
                        let backend = task_backend.subscribe();
                        streams.spawn(async move {
                            if !*enabled.borrow() {
                                return;
                            }
                            let Some(backend_port) = *backend.borrow() else { return; };
                            let Ok(mut backend) = TcpStream::connect((IpAddr::V4(Ipv4Addr::LOCALHOST), backend_port)).await
                            else {
                                return;
                            };
                            tokio::select! {
                                _ = copy_bidirectional(&mut frontend, &mut backend) => {}
                                _ = wait_until_disabled(&mut enabled) => {}
                            }
                        });
                    }
                    command = command_rx.recv() => {
                        let Some(command) = command else { return; };
                        enabled.send_replace(command.enabled);
                        if !command.enabled {
                            while streams.join_next().await.is_some() {}
                        }
                        let _ = command.acknowledged.send(());
                    }
                    _ = streams.join_next(), if !streams.is_empty() => {}
                }
            }
        });
        Self {
            port,
            backend,
            commands,
            task,
        }
    }

    fn set_backend(&self, port: u16) {
        self.backend.send_replace(Some(port));
    }

    async fn set_enabled(&self, enabled: bool) {
        let (acknowledged, wait) = oneshot::channel();
        self.commands
            .send(ProxyCommand {
                enabled,
                acknowledged,
            })
            .await
            .expect("range proxy control task");
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("range proxy control acknowledgement timeout")
            .expect("range proxy control acknowledgement");
    }
}

impl Drop for RangeProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn wait_until_disabled(enabled: &mut watch::Receiver<bool>) {
    while *enabled.borrow_and_update() {
        if enabled.changed().await.is_err() {
            return;
        }
    }
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
    hosted_ranges: &str,
    tls: &TlsPaths,
    commit_fault: Option<&str>,
) -> ProcessNode {
    let cache_dir = root.join(format!("r{range}-cache"));
    std::fs::create_dir_all(&cache_dir).expect("cache dir");
    let log_path = root.join(format!("r{range}.log"));
    let stderr = File::create(&log_path).expect("node log");
    let binary = gres_binary();
    let mut command = Command::new(binary);
    command.args([
        "--listen",
        "127.0.0.1:0",
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
        "127.0.0.1:0",
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
        "--operator-control-principal",
        "CN=process-range",
    ]);
    if let Some(fault) = commit_fault {
        command.env("CRABKA_GRES_TEST_COMMIT_FAULT", fault);
    }
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .expect("spawn gres child");
    let mut child = child;
    let stdout = child.stdout.take().expect("child stdout pipe");
    let (ready_tx, ready) = oneshot::channel();
    let task_log_path = log_path.clone();
    let log_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut ready_tx = Some(ready_tx);
        let mut log = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&task_log_path)
            .await
            .expect("open child log append");
        while let Ok(Some(line)) = lines.next_line().await {
            let _ = log.write_all(line.as_bytes()).await;
            let _ = log.write_all(b"\n").await;
            if let Some(payload) = line.strip_prefix("CRABKA_GRES_READY ") {
                let mut addresses = payload.split_whitespace();
                let event = addresses
                    .next()
                    .and_then(|sql| sql.parse().ok())
                    .zip(addresses.next())
                    .and_then(|(sql, range)| {
                        Some(ProcessReady {
                            sql,
                            range: (range != "-").then(|| range.parse().ok()).flatten(),
                        })
                    });
                if let (Some(event), Some(sender)) = (event, ready_tx.take()) {
                    let _ = sender.send(event);
                }
            }
        }
    });
    ProcessNode {
        range,
        sql_port: 0,
        range_port: 0,
        _cache_dir: cache_dir,
        log_path,
        child,
        ready: Some(ready),
        log_task,
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
            lifecycle: Default::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: format!("127.0.0.1:{r1_port}"),
            wal_generation: 0,
            lifecycle: Default::default(),
            retirement: None,
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

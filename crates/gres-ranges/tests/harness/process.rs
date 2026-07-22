#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
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
    RangeBoundary, RangeLayoutEntry, RangeLifecycle, Registry, SqlUser, TenantId, TenantName,
    TenantRecord, TenantState,
};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, copy_bidirectional},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

pub(super) fn fixture_password() -> String {
    std::process::id().to_string()
}

pub struct ProcessHarness {
    root: tempfile::TempDir,
    _broker: BrokerHandle,
    bootstrap: String,
    tenant: String,
    tls: TlsPaths,
    commit_fault: Option<String>,
    sql_proxy: RangeProxy,
    r0_proxy: RangeProxy,
    r1_proxy: RangeProxy,
    r2_proxy: RangeProxy,
    r3_proxy: RangeProxy,
    r0: ProcessNode,
    r1: Option<ProcessNode>,
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

#[allow(dead_code)]
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
        let sql_proxy = RangeProxy::start().await;
        let r0_proxy = RangeProxy::start().await;
        let r1_proxy = RangeProxy::start().await;
        let r2_proxy = RangeProxy::start().await;
        let r3_proxy = RangeProxy::start().await;
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
            root,
            _broker: broker,
            bootstrap,
            tenant,
            tls,
            commit_fault,
            sql_proxy,
            r0_proxy,
            r1_proxy,
            r2_proxy,
            r3_proxy,
            r0,
            r1: Some(r1),
        };
        harness.wait_ready(0).await;
        harness.wait_ready(1).await;
        harness
    }

    pub async fn start_all_on_zero(name: &str) -> Self {
        Self::start_all_on_zero_inner(name, None).await
    }

    pub async fn start_all_on_zero_with_commit_fault(name: &str, fault: &str) -> Self {
        Self::start_all_on_zero_inner(name, Some(fault.to_owned())).await
    }

    async fn start_all_on_zero_inner(name: &str, commit_fault: Option<String>) -> Self {
        let root = tempfile::tempdir().expect("process harness root");
        let broker = Broker::start(BrokerConfig::for_tests(root.path().join("broker")))
            .await
            .expect("start real broker");
        let bootstrap = broker.listen_addr().to_string();
        let tenant = name.to_owned();
        let sql_proxy = RangeProxy::start().await;
        let r0_proxy = RangeProxy::start().await;
        let r1_proxy = RangeProxy::start().await;
        let r2_proxy = RangeProxy::start().await;
        let r3_proxy = RangeProxy::start().await;
        let tls = write_tls_fixture(root.path());
        provision_control(&bootstrap, &tenant, r0_proxy.port, r1_proxy.port).await;
        let r0 = spawn_node(
            root.path(),
            &bootstrap,
            &tenant,
            0,
            "r0,r1",
            &tls,
            commit_fault.as_deref(),
        );
        let mut harness = Self {
            root,
            _broker: broker,
            bootstrap,
            tenant,
            tls,
            commit_fault,
            sql_proxy,
            r0_proxy,
            r1_proxy,
            r2_proxy,
            r3_proxy,
            r0,
            r1: None,
        };
        harness.wait_ready(0).await;
        harness.r1_proxy.set_backend(harness.r0.range_port);
        harness.r2_proxy.set_backend(harness.r0.range_port);
        harness.r3_proxy.set_backend(harness.r0.range_port);
        harness
    }

    pub async fn sql(&self, range: u32) -> tokio_postgres::Client {
        connect(self.node(range).sql_port, &self.tenant).await
    }

    pub async fn sql_with_driver(
        &self,
        range: u32,
    ) -> (
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    ) {
        connect_with_driver(self.node(range).sql_port, &self.tenant)
            .await
            .expect("connect compute with driver")
    }

    pub async fn try_sql(&self, range: u32) -> Option<tokio_postgres::Client> {
        try_connect(self.node(range).sql_port, &self.tenant).await
    }

    pub fn pid(&self, range: u32) -> u32 {
        self.node(range).child.id().expect("child pid")
    }

    pub fn process_group(&self, range: u32) -> u32 {
        self.pid(range)
    }

    pub fn endpoints(&self) -> [(u16, u16); 2] {
        [
            (self.r0.sql_port, self.r0_proxy.port),
            (
                self.r1
                    .as_ref()
                    .map_or(self.r0.sql_port, |node| node.sql_port),
                self.r1_proxy.port,
            ),
        ]
    }

    pub fn stable_sql_port(&self) -> u16 {
        self.sql_proxy.port
    }

    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    pub async fn preserve_logs(&self, destination: &Path) {
        tokio::fs::create_dir_all(destination)
            .await
            .expect("create preserved process log directory");
        let mut logs = vec![("r0.log", &self.r0.log_path)];
        if let Some(r1) = &self.r1 {
            logs.push(("r1.log", &r1.log_path));
        }
        for (name, source) in logs {
            tokio::fs::copy(source, destination.join(name))
                .await
                .expect("preserve process log");
        }
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn range_endpoint(&self, range: u32) -> String {
        format!("127.0.0.1:{}", self.proxy(range).port)
    }

    pub fn split_successor_endpoints(&self) -> [String; 2] {
        [self.range_endpoint(2), self.range_endpoint(3)]
    }

    pub fn operator_control_client(&self) -> crabka_gres_ranges::FramedTcpClient {
        crabka_gres_ranges::FramedTcpClient::with_tls_pem(
            &std::fs::read(&self.tls.server_cert).expect("operator certificate"),
            &std::fs::read(&self.tls.server_key).expect("operator key"),
            &std::fs::read(&self.tls.ca).expect("range CA"),
            "crabka-dev".to_owned(),
        )
        .expect("operator mTLS client")
    }

    pub async fn inspect_durable_records(
        &self,
        request: crabka_gres_ranges::InspectDurableRecordsReq,
    ) -> crabka_gres_ranges::InspectDurableRecordsResp {
        let range_id = request.range_id;
        let response = self
            .operator_control_client()
            .call(
                &self.range_endpoint(range_id.as_u32()),
                &crabka_gres_ranges::RangeRequest::InspectDurableRecords(request),
            )
            .await
            .expect("authenticated durable-record inspection");
        let crabka_gres_ranges::RangeResponse::InspectDurableRecords(response) = response else {
            panic!("unexpected durable-record inspection response: {response:?}");
        };
        *response
    }

    pub fn log(&self, range: u32) -> String {
        let path = &self.node(range).log_path;
        std::fs::read_to_string(path)
            .unwrap_or_else(|error| format!("<read {} failed: {error}>", path.display()))
    }

    /// DDL through the non-r0 node: exercises forwarding plus the cluster barrier.
    pub async fn create_table(&self, sql: &str) {
        self.sql(1)
            .await
            .simple_query(sql)
            .await
            .expect("create table through non-r0 node");
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

    pub async fn restart_with_hosted_ranges(&mut self, range: u32, hosted_ranges: &str) {
        self.restart_node(range, hosted_ranges).await;
    }

    pub async fn host_all_ranges_on_source_zero(&mut self) {
        self.stop_node(0).await;
        self.stop_node(1).await;
        self.restart_node(0, "r0,r1").await;
        self.r1_proxy.set_backend(self.r0.range_port);
    }

    pub fn clear_commit_fault(&mut self) {
        self.commit_fault = None;
    }

    pub fn set_commit_fault_for_next_child(&mut self, fault: &str) {
        self.commit_fault = Some(fault.to_owned());
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
        if range == 1 && self.r1.is_none() {
            return;
        }
        let node = self.node_mut(range);
        if node.child.try_wait().expect("child status").is_none() {
            let pid = node.child.id().expect("compute child PID");
            #[cfg(unix)]
            {
                let status = std::process::Command::new("kill")
                    .args(["-KILL", "--", &format!("-{pid}")])
                    .status()
                    .expect("kill compute process group");
                assert!(status.success(), "kill compute process group r{range}");
            }
            #[cfg(not(unix))]
            node.child.kill().await.expect("kill compute child");
            let _ = tokio::time::timeout(Duration::from_secs(5), node.child.wait())
                .await
                .expect("compute child shutdown timeout");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while process_group_exists(pid) && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(
                !process_group_exists(pid),
                "compute process group r{range} reaped"
            );
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut node.log_task)
            .await
            .expect("compute log task shutdown timeout");
    }

    async fn restart_node(&mut self, range: u32, hosted_ranges: &str) {
        self.restart_spawned(range, hosted_ranges);
        self.finish_restart(range).await;
    }

    /// Spawn the replacement child without waiting for its recovery-complete
    /// readiness event, so a test can observe the child while it recovers.
    /// Follow with [`Self::finish_restart`].
    pub fn restart_spawned(&mut self, range: u32, hosted_ranges: &str) {
        let node_range = {
            let node = self.node(range);
            node.range
        };
        let replacement = spawn_node(
            self.root.path(),
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
        if range == 0 && self.r1.is_none() {
            let replacement_port = self.r0.range_port;
            self.r0_proxy.set_backend(replacement_port);
            self.r1_proxy.set_backend(replacement_port);
            self.r2_proxy.set_backend(replacement_port);
            self.r3_proxy.set_backend(replacement_port);
        }
    }

    /// Wait for a [`Self::restart_spawned`] child's recovery-complete
    /// readiness event and re-point the stable proxies at it.
    pub async fn finish_restart(&mut self, range: u32) {
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
            .unwrap_or_else(|_| panic!("compute readiness channel closed: {}", self.log(range)));
        let range_addr = ready.range.expect("range listener address");
        let node = self.node_mut(range);
        node.sql_port = ready.sql.port();
        node.range_port = range_addr.port();
        self.proxy(range).set_backend(range_addr.port());
        if range == 0 {
            self.sql_proxy.set_backend(ready.sql.port());
            self.r2_proxy.set_backend(range_addr.port());
            self.r3_proxy.set_backend(range_addr.port());
        }
        if range == 0 && self.r1.is_none() {
            self.r1_proxy.set_backend(range_addr.port());
        }
    }

    fn node(&self, range: u32) -> &ProcessNode {
        if range == 0 {
            &self.r0
        } else {
            self.r1.as_ref().expect("r1 child is not configured")
        }
    }
    fn node_mut(&mut self, range: u32) -> &mut ProcessNode {
        if range == 0 {
            &mut self.r0
        } else {
            self.r1.as_mut().expect("r1 child is not configured")
        }
    }

    fn proxy(&self, range: u32) -> &RangeProxy {
        match range {
            0 => &self.r0_proxy,
            1 => &self.r1_proxy,
            2 => &self.r2_proxy,
            3 => &self.r3_proxy,
            _ => panic!("range proxy r{range} is not configured"),
        }
    }
}

fn process_group_exists(process_group: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", "--", &format!("-{process_group}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn kill_process_group(process_group: u32) {
    let _ = std::process::Command::new("kill")
        .args(["-KILL", "--", &format!("-{process_group}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

impl Drop for ProcessHarness {
    fn drop(&mut self) {
        if let Some(pid) = self.r0.child.id() {
            kill_process_group(pid);
        }
        let _ = self.r0.child.start_kill();
        if let Some(r1) = &mut self.r1 {
            if let Some(pid) = r1.child.id() {
                kill_process_group(pid);
            }
            let _ = r1.child.start_kill();
        }
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
    #[allow(dead_code)]
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
                                () = wait_until_disabled(&mut enabled) => {}
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

    #[allow(dead_code)]
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
    let checkpoint_dir = root.join("checkpoints");
    std::fs::create_dir_all(&cache_dir).expect("cache dir");
    std::fs::create_dir_all(&checkpoint_dir).expect("checkpoint dir");
    let log_path = std::env::var_os("CRABKA_G8_PROCESS_LOG_DIR").map_or_else(
        || root.join(format!("r{range}.log")),
        |directory| {
            let directory = PathBuf::from(directory);
            std::fs::create_dir_all(&directory).expect("durable process log directory");
            directory.join(format!("{tenant}-r{range}.log"))
        },
    );
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
    if range == 0 {
        command.args([
            "--checkpoint-store",
            "local",
            "--checkpoint-local-root",
            checkpoint_dir.to_str().expect("checkpoint path"),
            "--checkpoint-frames",
            "1",
        ]);
    }
    if let Some(fault) = commit_fault {
        command.env("CRABKA_GRES_TEST_COMMIT_FAULT", fault);
    }
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true);
    #[cfg(unix)]
    command.as_std_mut().process_group(0);
    let child = command.spawn().expect("spawn gres child");
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
                    .map(|(sql, range)| ProcessReady {
                        sql,
                        range: (range != "-").then(|| range.parse().ok()).flatten(),
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
    let verifier = crabka_security::scram::PgScramVerifier::generate_with_salt(
        &fixture_password(),
        8192,
        vec![7; 16],
    )
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
            lifecycle: RangeLifecycle::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: format!("127.0.0.1:{r1_port}"),
            wal_generation: 0,
            lifecycle: RangeLifecycle::default(),
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
    let (client, driver) = connect_with_driver(port, database).await?;
    tokio::spawn(async move {
        let _ = driver.await;
    });
    Some(client)
}

async fn connect_with_driver(
    port: u16,
    database: &str,
) -> Option<(
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
)> {
    let connection = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("alice")
        .password(fixture_password())
        .dbname(database)
        .connect(tokio_postgres::NoTls)
        .await
        .ok()?;
    let (client, driver) = connection;
    Some((client, tokio::spawn(driver)))
}
async fn connect(port: u16, database: &str) -> tokio_postgres::Client {
    try_connect(port, database).await.expect("connect compute")
}

fn gres_binary() -> PathBuf {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let configured = std::env::var_os("CRABKA_GRES_TEST_BINARY")
        .map_or_else(|| workspace.join("target/debug/crabka-gres"), PathBuf::from);
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

#[cfg(test)]
mod process_group_tests {
    use super::*;

    #[tokio::test]
    async fn kill_process_group_reaps_child_and_descendant() {
        let mut command = Command::new("bash");
        command.args(["-c", "sleep 300 & wait"]).kill_on_drop(true);
        #[cfg(unix)]
        command.as_std_mut().process_group(0);
        let mut child = command.spawn().expect("spawn process group fixture");
        let process_group = child.id().expect("fixture PID");
        assert!(process_group_exists(process_group));
        kill_process_group(process_group);
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("fixture group exit timeout")
            .expect("wait fixture group");
        tokio::time::timeout(Duration::from_secs(5), async {
            while process_group_exists(process_group) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fixture process group reap timeout");
    }
}

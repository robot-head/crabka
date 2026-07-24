//! Cluster orchestration: broker + `crabka-gres` nodes behind chaos proxies.
//!
//! Launch sequence (adapted from `crates/gres-ranges/tests/harness/process.rs`
//! and `scripts/gres-range-scaling.sh`):
//!
//! 1. Format a broker data dir and start `crabka-broker` as a child process
//!    (a child, not in-process, so `/proc` CPU accounting attributes broker
//!    cost separately from the harness).
//! 2. Spawn one [`ChaosProxy`] per range-RPC endpoint and one per node SQL
//!    front door.
//! 3. Generate a range mTLS CA + peer cert via `crabka_security::ca` and
//!    provision the tenant through `crabka_gres_control::Registry`: WAL
//!    topics per range, `TenantRecord` whose range-layout endpoints point at
//!    the **proxy** ports (so inter-node traffic is interceptable).
//! 4. Spawn one `crabka-gres` child per node with `--host-ranges` for its
//!    round-robin range subset (range `r` → node `r % nodes`), the
//!    timestamp-source flags for the scenario mode, and per-node
//!    `--hlc-wall-offset-ms` skew. Parse `CRABKA_GRES_READY <sql> <range>`
//!    from stdout for the OS-assigned ports, then point the proxies at them.
//!
//! Schema DDL needs no special phase: any node's gateway routes DDL to
//! every range engine and returns only after the cluster-wide catalog
//! barrier, so callers issue it through an arbitrary [`Cluster::sql_endpoint`]
//! once the topology is up.
//!
//! Node processes are `SIGKILL`ed on kill/shutdown (process-group kill), and a
//! restart re-spawns with identical flags and repoints the node's proxies.

#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::{
    collections::BTreeMap,
    fs::File,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use anyhow::{Context as _, anyhow, bail, ensure};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_gres_control::{
    RangeBoundary, RangeLayoutEntry, RangeLifecycle, Registry, RegistryPolicy, SqlUser, TenantId,
    TenantName, TenantRecord, TenantState,
};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, ChildStdout, Command},
    sync::oneshot,
    task::JoinHandle,
};

use crate::{
    proxy::ChaosProxy,
    scenario::{ModeSpec, TopologySpec},
};

/// Tenant name the harness provisions and serves.
const TENANT: &str = "loadtest";
/// SQL login role provisioned for the tenant.
const SQL_USER_NAME: &str = "loadtest";
/// Password backing the provisioned SCRAM verifier, generated fresh per
/// cluster launch. Nodes run `--auth trust`, so this is provisioning
/// completeness, not a live credential check — generating it (rather than
/// hard-coding one) just ensures no fixed string ever doubles as a real
/// secret.
fn generate_sql_password() -> String {
    use rand::RngExt as _;
    format!("loadtest-{:016x}", rand::rng().random::<u64>())
}
/// Database name clients connect to (mirrors the scaling script's conninfo).
const SQL_DATABASE: &str = "crab";
/// Range `i` starts at table id `i * RANGE_TABLE_STRIDE`, so workload tables
/// `t0`, `t1000000`, ... land on ranges 0, 1, ... (mirrors the script).
const RANGE_TABLE_STRIDE: u64 = 1_000_000;
/// Fixed broker cluster id (mirrors the scaling script).
const BROKER_CLUSTER_ID: &str = "00000000-0000-0000-0000-000000000001";
/// DNS identity on the range mTLS peer certificate.
const TLS_SERVER_NAME: &str = "crabka-dev";
/// Subject DN authorized for range and operator-control RPCs.
const TLS_PRINCIPAL: &str = "CN=loadtest-range";
/// Overall bound for a child process to report ready (debug builds replaying
/// WAL are slow).
const LAUNCH_TIMEOUT: Duration = Duration::from_mins(2);
/// Bound for a `SIGKILL`ed child to be reaped.
const KILL_TIMEOUT: Duration = Duration::from_secs(10);
/// Lines of a child's log included in launch/teardown error messages.
const LOG_TAIL_LINES: usize = 40;

/// Paths to the binaries the cluster launches.
#[derive(Debug, Clone)]
pub struct Binaries {
    /// `crabka-gres` server binary.
    pub gres: PathBuf,
    /// `crabka-broker` binary.
    pub broker: PathBuf,
    /// `crabka` CLI binary (storage format and admin commands).
    pub crabka_cli: PathBuf,
}

impl Binaries {
    /// Resolves binary paths from `CRABKA_GRES_LOADTEST_{GRES,BROKER,CLI}_BIN`
    /// env overrides, falling back to `target/debug/` relative to the
    /// workspace root.
    ///
    /// # Errors
    ///
    /// Returns an error if a resolved path does not exist.
    pub fn resolve() -> anyhow::Result<Self> {
        let root = workspace_root()?;
        let from_env = |key: &str| std::env::var_os(key).map(PathBuf::from);
        Ok(Self {
            gres: resolve_binary(
                from_env("CRABKA_GRES_LOADTEST_GRES_BIN"),
                &root,
                "crabka-gres",
            )?,
            broker: resolve_binary(
                from_env("CRABKA_GRES_LOADTEST_BROKER_BIN"),
                &root,
                "crabka-broker",
            )?,
            crabka_cli: resolve_binary(from_env("CRABKA_GRES_LOADTEST_CLI_BIN"), &root, "crabka")?,
        })
    }
}

/// Everything needed to launch a cluster.
#[derive(Debug, Clone)]
pub struct ClusterOptions {
    /// Cluster shape.
    pub topology: TopologySpec,
    /// Timestamp-source mode for the tenant.
    pub mode: ModeSpec,
    /// Directory for broker data, node caches, and per-process log files.
    pub work_dir: PathBuf,
    /// Binaries to launch.
    pub binaries: Binaries,
    /// Shared registry policy used by provisioning and spawned computes.
    pub registry_policy: RegistryPolicy,
}

/// Connection parameters for a node's SQL front door (via its chaos proxy).
#[derive(Debug, Clone)]
pub struct SqlEndpoint {
    /// Proxy address to dial.
    pub addr: SocketAddr,
    /// SQL user.
    pub user: String,
    /// SQL password.
    pub password: String,
    /// Database name.
    pub database: String,
}

/// A process the harness launched, for resource sampling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessInfo {
    /// Human-readable label (`broker`, `node0`, ...).
    pub label: String,
    /// OS process id.
    pub pid: u32,
}

/// Live, append-only roster of every process the harness has launched for
/// the current topology (broker plus every node incarnation). Shared with
/// the `/proc` sampler, which re-snapshots it on every tick so a node
/// restarted mid-run (fresh pid, `#N`-suffixed label) is attached as it
/// appears. Cloning is cheap; clones share the same list.
#[derive(Debug, Clone, Default)]
pub struct ProcessRoster(Arc<Mutex<Vec<ProcessInfo>>>);

impl ProcessRoster {
    /// Appends a newly-launched process. Existing entries are never removed
    /// or reordered, so index-based consumers only ever see new tail
    /// entries.
    pub fn push(&self, process: ProcessInfo) {
        self.lock().push(process);
    }

    /// The roster contents, in launch order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ProcessInfo> {
        self.lock().clone()
    }

    fn lock(&self) -> MutexGuard<'_, Vec<ProcessInfo>> {
        // A poisoned lock only means another thread panicked mid-push; the
        // Vec is still coherent, so keep serving it.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A running cluster: broker child, node children, and their proxies.
#[derive(Debug)]
pub struct Cluster {
    topology: TopologySpec,
    binaries: Binaries,
    log_dir: PathBuf,
    broker: BrokerProcess,
    range_proxies: Vec<ChaosProxy>,
    sql_proxies: Vec<ChaosProxy>,
    nodes: Vec<NodeSlot>,
    roster: ProcessRoster,
    sql_password: String,
}

impl Cluster {
    /// Launches the cluster and waits until every node reports ready.
    ///
    /// # Errors
    ///
    /// Returns an error if any process fails to start, provisioning fails,
    /// or readiness times out.
    pub async fn launch(options: ClusterOptions) -> anyhow::Result<Self> {
        let ClusterOptions {
            topology,
            mode,
            work_dir,
            binaries,
            registry_policy,
        } = options;
        ensure!(topology.nodes >= 1, "topology needs at least one node");
        ensure!(topology.ranges >= 1, "topology needs at least one range");
        ensure!(
            topology.nodes <= topology.ranges,
            "every node must host at least one range ({} nodes > {} ranges)",
            topology.nodes,
            topology.ranges
        );
        let log_dir = work_dir.join("logs");
        std::fs::create_dir_all(&log_dir)
            .with_context(|| format!("create log dir {}", log_dir.display()))?;
        std::fs::create_dir_all(work_dir.join("checkpoints")).context("create checkpoint dir")?;

        let allocation = topology
            .cpus_per_node
            .map(|cpus| {
                cpu_allocation(
                    topology.nodes,
                    cpus,
                    topology.broker_cpus.unwrap_or(DEFAULT_BROKER_CPUS),
                    online_cpu_count()?,
                )
            })
            .transpose()?;
        if let Some(allocation) = &allocation {
            tracing::info!(
                broker = %allocation.broker,
                nodes = ?allocation.nodes,
                "CPU pinning active"
            );
        }

        let (broker_port, controller_port) = pick_free_ports().await?;
        run_crabka_format(&binaries.crabka_cli, &work_dir, &log_dir, controller_port).await?;
        let broker = start_broker(
            &binaries.broker,
            &work_dir,
            &log_dir,
            broker_port,
            allocation.as_ref().map(|cpus| cpus.broker.as_str()),
        )
        .await?;
        let kafka_bootstrap = format!("127.0.0.1:{broker_port}");
        tracing::info!(bootstrap = %kafka_bootstrap, "broker ready");

        let range_proxies = spawn_proxies(topology.ranges)
            .await
            .context("range proxies")?;
        let sql_proxies = spawn_proxies(topology.nodes).await.context("sql proxies")?;
        let tls = write_tls_fixture(&work_dir)?;
        let range_endpoints: Vec<SocketAddr> = range_proxies.iter().map(ChaosProxy::addr).collect();
        let sql_password = generate_sql_password();
        provision_tenant(
            &kafka_bootstrap,
            topology.ranges,
            &range_endpoints,
            &sql_password,
            &registry_policy,
        )
        .await?;
        tracing::info!(
            tenant = TENANT,
            ranges = topology.ranges,
            "tenant provisioned"
        );

        let context = SpecContext {
            topology: &topology,
            mode,
            kafka_bootstrap: &kafka_bootstrap,
            work_dir: &work_dir,
            log_dir: &log_dir,
            tls: &tls,
            cpu_allocation: allocation.as_ref(),
            registry_policy: &registry_policy,
        };
        let node_specs: Vec<NodeSpec> = (0..topology.nodes)
            .map(|node| node_spec(node, &context))
            .collect();
        let roster = ProcessRoster::default();
        roster.push(ProcessInfo {
            label: "broker".to_owned(),
            pid: broker.pid,
        });
        let mut nodes = Vec::with_capacity(node_specs.len());
        for spec in node_specs {
            std::fs::create_dir_all(&spec.cache_dir)
                .with_context(|| format!("create cache dir {}", spec.cache_dir.display()))?;
            let running = spawn_node(&binaries.gres, &spec)?;
            roster.push(ProcessInfo {
                label: spec.label.clone(),
                pid: running.pid,
            });
            nodes.push(NodeSlot {
                spec,
                running: Some(running),
                incarnation: 1,
            });
        }
        let mut cluster = Self {
            topology,
            binaries,
            log_dir,
            broker,
            range_proxies,
            sql_proxies,
            nodes,
            roster,
            sql_password,
        };
        for index in 0..cluster.nodes.len() {
            wait_node_ready(&mut cluster.nodes[index]).await?;
        }
        for node in 0..cluster.topology.nodes {
            cluster.point_proxies_at(node);
        }
        tracing::info!(
            nodes = cluster.topology.nodes,
            ranges = cluster.topology.ranges,
            "cluster ready"
        );
        Ok(cluster)
    }

    /// Number of compute nodes.
    #[must_use]
    pub fn node_count(&self) -> u16 {
        self.topology.nodes
    }

    /// The node hosting a given range (round-robin assignment).
    #[must_use]
    pub fn node_for_range(&self, range: u16) -> u16 {
        node_for_range(range, self.topology.nodes)
    }

    /// SQL connection parameters for a node's front door.
    #[must_use]
    pub fn sql_endpoint(&self, node: u16) -> SqlEndpoint {
        sql_endpoint_at(
            self.sql_proxies[usize::from(node)].addr(),
            &self.sql_password,
        )
    }

    /// The chaos proxy in front of a range's RPC endpoint.
    #[must_use]
    pub fn range_proxy(&self, range: u16) -> &ChaosProxy {
        &self.range_proxies[usize::from(range)]
    }

    /// The chaos proxy in front of a node's SQL listener.
    #[must_use]
    pub fn sql_proxy(&self, node: u16) -> &ChaosProxy {
        &self.sql_proxies[usize::from(node)]
    }

    /// Launched processes (broker plus every live node) for `/proc` sampling.
    #[must_use]
    pub fn processes(&self) -> Vec<ProcessInfo> {
        let mut processes = vec![ProcessInfo {
            label: "broker".to_owned(),
            pid: self.broker.pid,
        }];
        for slot in &self.nodes {
            if let Some(process) = &slot.running {
                processes.push(ProcessInfo {
                    label: slot.spec.label.clone(),
                    pid: process.pid,
                });
            }
        }
        processes
    }

    /// Directory holding per-process log files.
    #[must_use]
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    /// Live roster of every process launched for this topology (broker plus
    /// every node incarnation). [`Cluster::restart_node`] appends the
    /// replacement process under a `label#N` entry, so a `/proc` sampler
    /// holding this roster attaches restarted nodes mid-run.
    #[must_use]
    pub fn process_roster(&self) -> ProcessRoster {
        self.roster.clone()
    }

    /// SIGKILLs a node's process group. The node's proxies keep refusing
    /// until [`Cluster::restart_node`].
    ///
    /// # Errors
    ///
    /// Returns an error if the node index is unknown or already down.
    pub async fn kill_node(&mut self, node: u16) -> anyhow::Result<()> {
        let index = usize::from(node);
        ensure!(
            index < self.nodes.len(),
            "node {node} is out of range (cluster has {} nodes)",
            self.nodes.len()
        );
        let Some(mut process) = self.nodes[index].running.take() else {
            bail!("node {node} is already down");
        };
        let label = self.nodes[index].spec.label.clone();
        kill_child(&label, &mut process.child, process.pid).await?;
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut process.log_task).await;
        tracing::info!(node, "node killed");
        Ok(())
    }

    /// Re-spawns a killed node with identical flags and repoints its
    /// proxies at the new OS-assigned ports.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is running or fails to become ready.
    pub async fn restart_node(&mut self, node: u16) -> anyhow::Result<()> {
        let index = usize::from(node);
        ensure!(
            index < self.nodes.len(),
            "node {node} is out of range (cluster has {} nodes)",
            self.nodes.len()
        );
        ensure!(
            self.nodes[index].running.is_none(),
            "node {node} is already running"
        );
        let running = spawn_node(&self.binaries.gres, &self.nodes[index].spec)?;
        let pid = running.pid;
        self.nodes[index].running = Some(running);
        self.nodes[index].incarnation += 1;
        // Append (never replace): the previous incarnation's entry keeps its
        // sampled totals; the fresh pid gets a disambiguated label.
        self.roster.push(ProcessInfo {
            label: incarnation_label(&self.nodes[index].spec.label, self.nodes[index].incarnation),
            pid,
        });
        wait_node_ready(&mut self.nodes[index]).await?;
        self.point_proxies_at(node);
        tracing::info!(node, "node restarted");
        Ok(())
    }

    /// Tears the cluster down: kills node processes, then the broker.
    ///
    /// # Errors
    ///
    /// Returns an error if teardown fails; the caller should still treat the
    /// cluster as gone.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        let mut failures = Vec::new();
        for index in 0..self.nodes.len() {
            let Some(mut process) = self.nodes[index].running.take() else {
                continue;
            };
            let label = self.nodes[index].spec.label.clone();
            if let Err(error) = kill_child(&label, &mut process.child, process.pid).await {
                failures.push(format!("{error:#}"));
            }
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut process.log_task).await;
        }
        if let Err(error) = kill_child("broker", &mut self.broker.child, self.broker.pid).await {
            failures.push(format!("{error:#}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("cluster teardown failed: {}", failures.join("; ")))
        }
    }

    /// Repoints the proxies of every range hosted by `node` (plus its SQL
    /// proxy) at the node's live listeners.
    fn point_proxies_at(&self, node: u16) {
        let Some(process) = &self.nodes[usize::from(node)].running else {
            return;
        };
        for range in 0..self.topology.ranges {
            if node_for_range(range, self.topology.nodes) == node {
                self.range_proxies[usize::from(range)].set_backend(process.range_addr);
            }
        }
        self.sql_proxies[usize::from(node)].set_backend(process.sql_addr);
    }
}

/// The broker child process.
#[derive(Debug)]
struct BrokerProcess {
    child: Child,
    pid: u32,
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        // Best-effort: `kill_on_drop` reaps only the direct child, so kill
        // the process group too in case the broker forked.
        kill_process_group(self.pid);
        let _ = self.child.start_kill();
    }
}

/// One node's slot: its immutable spawn parameters plus the live child, if
/// any (`None` between [`Cluster::kill_node`] and [`Cluster::restart_node`]).
/// `incarnation` counts spawns of this slot (1 = original), naming roster
/// entries for restarts.
#[derive(Debug)]
struct NodeSlot {
    spec: NodeSpec,
    running: Option<NodeProcess>,
    incarnation: u32,
}

/// Everything needed to (re-)spawn one node with identical flags.
#[derive(Debug)]
struct NodeSpec {
    label: String,
    args: Vec<String>,
    cache_dir: PathBuf,
    log_path: PathBuf,
    /// `taskset -c` CPU list pinning this node, when the topology asks for
    /// fixed-capacity nodes.
    cpuset: Option<String>,
    registry_policy: RegistryPolicy,
}

/// Default CPUs pinned to the broker when `cpus_per_node` pinning is
/// active and the topology does not say otherwise.
const DEFAULT_BROKER_CPUS: u32 = 2;

/// Disjoint CPU slices for the broker and each node.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CpuAllocation {
    broker: String,
    nodes: Vec<String>,
}

/// Carves `available` CPUs into a broker slice (`0..broker_cpus`) and one
/// disjoint `cpus_per_node`-wide slice per node, so each process behaves
/// like a fixed-capacity host.
///
/// # Errors
///
/// Returns an error if the machine has fewer CPUs than the layout needs;
/// oversubscribed pinning would silently reintroduce the shared-CPU
/// distortion the knob exists to remove.
fn cpu_allocation(
    node_count: u16,
    cpus_per_node: u32,
    broker_cpus: u32,
    available: u32,
) -> anyhow::Result<CpuAllocation> {
    let needed = broker_cpus + u32::from(node_count) * cpus_per_node;
    ensure!(
        needed <= available,
        "cpus_per_node = {cpus_per_node} needs {needed} CPUs \
         ({broker_cpus} broker + {node_count} x {cpus_per_node}) but only {available} exist"
    );
    let span = |start: u32, width: u32| {
        if width == 1 {
            start.to_string()
        } else {
            format!("{start}-{}", start + width - 1)
        }
    };
    Ok(CpuAllocation {
        broker: span(0, broker_cpus),
        nodes: (0..u32::from(node_count))
            .map(|node| span(broker_cpus + node * cpus_per_node, cpus_per_node))
            .collect(),
    })
}

/// CPUs that exist on the machine, from `/sys/devices/system/cpu/online`.
///
/// Deliberately NOT `available_parallelism()`: that respects the calling
/// process's own affinity mask, and the recommended setup runs the harness
/// under `taskset` on leftover CPUs — children may still be pinned to any
/// online CPU regardless of the parent's mask.
fn online_cpu_count() -> anyhow::Result<u32> {
    let raw = std::fs::read_to_string("/sys/devices/system/cpu/online")
        .context("read /sys/devices/system/cpu/online")?;
    parse_cpu_list_count(raw.trim()).with_context(|| format!("parse online CPU list {raw:?}"))
}

/// Counts CPUs in a kernel CPU-list string such as `0-15` or `0,2-3,7`.
fn parse_cpu_list_count(list: &str) -> anyhow::Result<u32> {
    let mut count: u32 = 0;
    for part in list.split(',') {
        let part = part.trim();
        count += if let Some((low, high)) = part.split_once('-') {
            let low: u32 = low.parse().with_context(|| format!("bad bound {part:?}"))?;
            let high: u32 = high
                .parse()
                .with_context(|| format!("bad bound {part:?}"))?;
            ensure!(low <= high, "inverted CPU range {part:?}");
            high - low + 1
        } else {
            let _: u32 = part
                .parse()
                .with_context(|| format!("bad CPU id {part:?}"))?;
            1
        };
    }
    ensure!(count > 0, "empty CPU list");
    Ok(count)
}

/// The command for `binary`, wrapped in `taskset -c <cpuset>` when pinned.
fn pinned_command(binary: &Path, cpuset: Option<&str>) -> Command {
    match cpuset {
        Some(cpus) => {
            let mut command = Command::new("taskset");
            command.arg("-c").arg(cpus).arg(binary);
            command
        }
        None => Command::new(binary),
    }
}

/// A live node child and its OS-assigned listener addresses.
#[derive(Debug)]
struct NodeProcess {
    child: Child,
    pid: u32,
    sql_addr: SocketAddr,
    range_addr: SocketAddr,
    ready: Option<oneshot::Receiver<NodeReady>>,
    log_task: JoinHandle<()>,
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        // Best-effort: `kill_on_drop` reaps only the direct child, so kill
        // the process group too in case the node forked.
        kill_process_group(self.pid);
        let _ = self.child.start_kill();
    }
}

/// Addresses parsed from a node's `CRABKA_GRES_READY` line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeReady {
    sql: SocketAddr,
    range: Option<SocketAddr>,
}

/// Shared per-cluster inputs for building node specs.
struct SpecContext<'a> {
    topology: &'a TopologySpec,
    mode: ModeSpec,
    kafka_bootstrap: &'a str,
    work_dir: &'a Path,
    log_dir: &'a Path,
    tls: &'a TlsPaths,
    cpu_allocation: Option<&'a CpuAllocation>,
    registry_policy: &'a RegistryPolicy,
}

/// The SQL endpoint clients use for a given listener address.
fn sql_endpoint_at(addr: SocketAddr, password: &str) -> SqlEndpoint {
    SqlEndpoint {
        addr,
        user: SQL_USER_NAME.to_owned(),
        password: password.to_owned(),
        database: SQL_DATABASE.to_owned(),
    }
}

/// The node hosting `range` under round-robin assignment.
fn node_for_range(range: u16, nodes: u16) -> u16 {
    range % nodes
}

/// Roster label for the `incarnation`-th process spawned into a node slot
/// (1-based): the first incarnation keeps the bare label, later ones get a
/// `#N` suffix (`node2` → `node2#2`).
fn incarnation_label(label: &str, incarnation: u32) -> String {
    if incarnation <= 1 {
        label.to_owned()
    } else {
        format!("{label}#{incarnation}")
    }
}

/// The `--ranges` boundary list: range `i` starts at table id
/// `i * RANGE_TABLE_STRIDE` (for example `0,1000000,2000000`).
fn ranges_flag(ranges: u16) -> String {
    (0..ranges)
        .map(|range| (u64::from(range) * RANGE_TABLE_STRIDE).to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// The `--host-ranges` list for `node` (for example `r0,r3`).
fn host_ranges_flag(node: u16, nodes: u16, ranges: u16) -> String {
    (0..ranges)
        .filter(|range| node_for_range(*range, nodes) == node)
        .map(|range| format!("r{range}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The registry range layout matching [`ranges_flag`], with each range's
/// endpoint pointing at its chaos proxy.
fn range_layout(ranges: u16, endpoints: &[SocketAddr]) -> Vec<RangeLayoutEntry> {
    (0..ranges)
        .map(|range| RangeLayoutEntry {
            range_id: u32::from(range),
            end_key: (range + 1 < ranges)
                .then(|| RangeBoundary::table_start(u64::from(range + 1) * RANGE_TABLE_STRIDE)),
            endpoint: endpoints[usize::from(range)].to_string(),
            wal_generation: 0,
            lifecycle: RangeLifecycle::default(),
            retirement: None,
        })
        .collect()
}

/// Timestamp-source CLI flags for `mode`, plus the node's HLC wall-clock
/// skew when nonzero.
fn timestamp_args(mode: ModeSpec, skew_ms: i64) -> Vec<String> {
    let mut args = match mode {
        ModeSpec::LogicalTso => vec!["--timestamp-source".to_owned(), "logical-tso".to_owned()],
        ModeSpec::Hlc { max_offset_ms } => vec![
            "--timestamp-source".to_owned(),
            "hlc".to_owned(),
            "--hlc-max-offset-ms".to_owned(),
            max_offset_ms.to_string(),
        ],
    };
    if skew_ms != 0 {
        args.push("--hlc-wall-offset-ms".to_owned());
        args.push(skew_ms.to_string());
    }
    args
}

/// Diagnostic knob for the coordinator-vs-data per-write cost investigation:
/// which nodes are launched with the local-checkpoint flags. Defaults to node 0
/// only (the shipped harness behaviour). `CRABKA_GRES_LOADTEST_CHECKPOINT_NODES`
/// may be set to `all` (every node checkpoints) or `none` (no node does) to
/// A/B whether the node0/data-node CPU asymmetry tracks the checkpoint config.
fn checkpoints_enabled_for(node: u16) -> bool {
    match std::env::var("CRABKA_GRES_LOADTEST_CHECKPOINT_NODES")
        .ok()
        .as_deref()
    {
        Some("all") => true,
        Some("none") => false,
        _ => node == 0,
    }
}

/// Builds the restart-stable spawn parameters for the real-topology node
/// `node`: round-robin `--host-ranges`, checkpoint flags on node 0 (the
/// range-0 host), and the node's configured HLC wall-clock skew.
fn node_spec(node: u16, context: &SpecContext<'_>) -> NodeSpec {
    build_spec(
        &format!("node{node}"),
        host_ranges_flag(node, context.topology.nodes, context.topology.ranges),
        checkpoints_enabled_for(node),
        context
            .topology
            .clock_skew_ms
            .get(&node)
            .copied()
            .unwrap_or(0),
        context
            .cpu_allocation
            .map(|allocation| allocation.nodes[usize::from(node)].clone()),
        context,
    )
}

/// Assembles one node's full argument vector.
fn build_spec(
    label: &str,
    host_ranges: String,
    with_checkpoints: bool,
    skew_ms: i64,
    cpuset: Option<String>,
    context: &SpecContext<'_>,
) -> NodeSpec {
    let cache_dir = context.work_dir.join(format!("{label}-cache"));
    let mut args = vec![
        "--listen".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--substrate-bootstrap".to_owned(),
        context.kafka_bootstrap.to_owned(),
        "--tenant".to_owned(),
        TENANT.to_owned(),
        "--cache-dir".to_owned(),
        cache_dir.display().to_string(),
        "--ranges".to_owned(),
        ranges_flag(context.topology.ranges),
        "--host-ranges".to_owned(),
        host_ranges,
        "--range-listen".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--range-tls-cert".to_owned(),
        context.tls.cert.display().to_string(),
        "--range-tls-key".to_owned(),
        context.tls.key.display().to_string(),
        "--range-tls-ca".to_owned(),
        context.tls.ca.display().to_string(),
        "--range-tls-server-name".to_owned(),
        TLS_SERVER_NAME.to_owned(),
        "--range-allowed-principal".to_owned(),
        TLS_PRINCIPAL.to_owned(),
        "--operator-control-principal".to_owned(),
        TLS_PRINCIPAL.to_owned(),
        "--auth".to_owned(),
        "trust".to_owned(),
    ];
    if with_checkpoints {
        args.extend([
            "--checkpoint-store".to_owned(),
            "local".to_owned(),
            "--checkpoint-local-root".to_owned(),
            context.work_dir.join("checkpoints").display().to_string(),
        ]);
    }
    args.extend(timestamp_args(context.mode, skew_ms));
    NodeSpec {
        label: label.to_owned(),
        args,
        cache_dir,
        log_path: context.log_dir.join(format!("{label}.log")),
        cpuset,
        registry_policy: context.registry_policy.clone(),
    }
}

fn registry_policy_args(policy: &RegistryPolicy) -> [String; 10] {
    [
        "--registry-replication-factor".to_owned(),
        policy.replication_factor().to_string(),
        "--registry-topic-create-timeout-ms".to_owned(),
        policy.topic_create_timeout_ms().to_string(),
        "--registry-reader-retry-backoff-ms".to_owned(),
        policy.reader_retry_backoff().as_millis().to_string(),
        "--registry-fetch-max-wait-ms".to_owned(),
        policy.fetch_max_wait_ms().to_string(),
        "--registry-fetch-partition-max-bytes".to_owned(),
        policy.fetch_partition_max_bytes().to_string(),
    ]
}

/// Spawns one `crabka-gres` child in its own process group, capturing
/// stdout+stderr to the spec's log file and watching for the ready line.
fn spawn_node(gres_binary: &Path, spec: &NodeSpec) -> anyhow::Result<NodeProcess> {
    let stderr = File::create(&spec.log_path)
        .with_context(|| format!("create {}", spec.log_path.display()))?;
    let mut command = pinned_command(gres_binary, spec.cpuset.as_deref());
    command
        .args(&spec.args)
        .args(registry_policy_args(&spec.registry_policy))
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true);
    #[cfg(unix)]
    command.as_std_mut().process_group(0);
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn {} for {}", gres_binary.display(), spec.label))?;
    let pid = child.id().with_context(|| format!("{} pid", spec.label))?;
    let stdout = child
        .stdout
        .take()
        .with_context(|| format!("{} stdout pipe", spec.label))?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let log_task = tokio::spawn(pump_node_stdout(stdout, spec.log_path.clone(), ready_tx));
    let placeholder = placeholder_addr();
    Ok(NodeProcess {
        child,
        pid,
        sql_addr: placeholder,
        range_addr: placeholder,
        ready: Some(ready_rx),
        log_task,
    })
}

/// Waits for a spawned node to print `CRABKA_GRES_READY` and records its
/// OS-assigned SQL and range addresses on the slot.
async fn wait_node_ready(slot: &mut NodeSlot) -> anyhow::Result<()> {
    let label = slot.spec.label.clone();
    let log_path = slot.spec.log_path.clone();
    let receiver = slot
        .running
        .as_mut()
        .with_context(|| format!("{label} is not running"))?
        .ready
        .take()
        .with_context(|| format!("{label} readiness already consumed"))?;
    let ready = tokio::time::timeout(LAUNCH_TIMEOUT, receiver)
        .await
        .map_err(|_| {
            anyhow!(
                "{label} did not report CRABKA_GRES_READY within {LAUNCH_TIMEOUT:?}; \
                 log tail:\n{}",
                log_tail(&log_path)
            )
        })?
        .map_err(|_| {
            anyhow!(
                "{label} exited before reporting ready; log tail:\n{}",
                log_tail(&log_path)
            )
        })?;
    let range_addr = ready.range.ok_or_else(|| {
        anyhow!(
            "{label} reported no range listener; log tail:\n{}",
            log_tail(&log_path)
        )
    })?;
    let process = slot
        .running
        .as_mut()
        .with_context(|| format!("{label} is not running"))?;
    process.sql_addr = ready.sql;
    process.range_addr = range_addr;
    Ok(())
}

/// Copies a node's stdout into its log file line by line, resolving `ready`
/// on the first `CRABKA_GRES_READY` line.
async fn pump_node_stdout(
    stdout: ChildStdout,
    log_path: PathBuf,
    ready: oneshot::Sender<NodeReady>,
) {
    let mut lines = BufReader::new(stdout).lines();
    let mut ready = Some(ready);
    let Ok(mut log) = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .await
    else {
        return;
    };
    while let Ok(Some(line)) = lines.next_line().await {
        let _ = log.write_all(line.as_bytes()).await;
        let _ = log.write_all(b"\n").await;
        if let Some(payload) = line.strip_prefix("CRABKA_GRES_READY ")
            && let Some(event) = parse_ready_line(payload)
            && let Some(sender) = ready.take()
        {
            let _ = sender.send(event);
        }
    }
}

/// Parses the payload of a `CRABKA_GRES_READY <sql> <range>` line, where
/// `<range>` is `-` when the node has no range listener.
fn parse_ready_line(payload: &str) -> Option<NodeReady> {
    let mut parts = payload.split_whitespace();
    let sql = parts.next()?.parse().ok()?;
    let range = parts
        .next()
        .and_then(|token| (token != "-").then(|| token.parse().ok()).flatten());
    Some(NodeReady { sql, range })
}

/// Spawns `count` chaos proxies pointing at a refusing placeholder backend;
/// they are repointed once nodes report ready.
async fn spawn_proxies(count: u16) -> anyhow::Result<Vec<ChaosProxy>> {
    let mut proxies = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        proxies.push(
            ChaosProxy::spawn(placeholder_addr())
                .await
                .context("spawn chaos proxy")?,
        );
    }
    Ok(proxies)
}

/// A localhost address nothing listens on, used before real backends exist.
fn placeholder_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 1))
}

/// Runs `crabka format` for a standalone single-voter broker (mirrors
/// `scripts/gres-range-scaling.sh`), logging to `logs/format.log`.
async fn run_crabka_format(
    cli_binary: &Path,
    work_dir: &Path,
    log_dir: &Path,
    controller_port: u16,
) -> anyhow::Result<()> {
    let output = Command::new(cli_binary)
        .arg("format")
        .arg("--log-dir")
        .arg(work_dir.join("broker-data"))
        .args([
            "--cluster-id",
            BROKER_CLUSTER_ID,
            "--standalone",
            "--node-id",
            "1",
        ])
        .args([
            "--controller-listener",
            &format!("127.0.0.1:{controller_port}"),
        ])
        .output()
        .await
        .with_context(|| format!("run {} format", cli_binary.display()))?;
    let mut log = output.stdout;
    log.extend_from_slice(&output.stderr);
    std::fs::write(log_dir.join("format.log"), &log).context("write format.log")?;
    ensure!(
        output.status.success(),
        "crabka format failed with {}:\n{}",
        output.status,
        String::from_utf8_lossy(&log)
    );
    Ok(())
}

/// Writes `broker.toml` and starts `crabka-broker` as a child process,
/// waiting until its Kafka listener accepts connections.
async fn start_broker(
    broker_binary: &Path,
    work_dir: &Path,
    log_dir: &Path,
    broker_port: u16,
    cpuset: Option<&str>,
) -> anyhow::Result<BrokerProcess> {
    let broker_data = work_dir.join("broker-data");
    let config_path = work_dir.join("broker.toml");
    let config = format!(
        r#"broker_id = 1
log_dir = "{log_dir_toml}"
cluster_id = "{BROKER_CLUSTER_ID}"
inter_broker_listener_name = "plain"

[[listeners]]
name = "plain"
bind_addr = "127.0.0.1:{broker_port}"
advertised = "127.0.0.1:{broker_port}"
protocol = "Plaintext"

[authorization]
type = "simple"
super_users = ["ANONYMOUS"]
"#,
        log_dir_toml = broker_data.display(),
    );
    std::fs::write(&config_path, config)
        .with_context(|| format!("write {}", config_path.display()))?;
    let log_path = log_dir.join("broker.log");
    let log_file =
        File::create(&log_path).with_context(|| format!("create {}", log_path.display()))?;
    let stderr_file = log_file.try_clone().context("clone broker log handle")?;
    let mut command = pinned_command(broker_binary, cpuset);
    command
        .arg("--log-dir")
        .arg(&broker_data)
        .args(["--cluster-id", BROKER_CLUSTER_ID, "--broker-id", "1"])
        .arg("--config-file")
        .arg(&config_path)
        // Deviation from the script: disable the fixed-port (9404) metrics
        // listener so concurrent harness runs cannot collide on it.
        .args(["--metrics-listen-addr", "none"])
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(stderr_file))
        .kill_on_drop(true);
    #[cfg(unix)]
    command.as_std_mut().process_group(0);
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn {}", broker_binary.display()))?;
    let pid = child.id().context("broker pid")?;
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, broker_port));
    wait_for_broker(addr, &log_path, &mut child).await?;
    Ok(BrokerProcess { child, pid })
}

/// Polls the broker's Kafka port until it accepts a TCP connection.
async fn wait_for_broker(
    addr: SocketAddr,
    log_path: &Path,
    child: &mut Child,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + LAUNCH_TIMEOUT;
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        if let Some(status) = child.try_wait().context("poll broker status")? {
            bail!(
                "crabka-broker exited with {status} before listening on {addr}; log tail:\n{}",
                log_tail(log_path)
            );
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "crabka-broker did not listen on {addr} within {LAUNCH_TIMEOUT:?}; log tail:\n{}",
                log_tail(log_path)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Creates the tenant's WAL topics and registry record. The record's range
/// endpoints are the chaos proxies, which is the fault-injection seam.
async fn provision_tenant(
    bootstrap: &str,
    ranges: u16,
    range_endpoints: &[SocketAddr],
    sql_password: &str,
    registry_policy: &RegistryPolicy,
) -> anyhow::Result<()> {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .context("connect admin client")?;
    let topics = (0..ranges)
        .map(|range| CreateTopicSpec {
            name: format!("__gres_wal.{TENANT}.r{range}"),
            partitions: 1,
            replicas: 1,
            configs: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    let outcomes = admin
        .create_topics(&topics, 30_000)
        .await
        .context("create WAL topics")?;
    ensure!(
        outcomes.iter().all(|outcome| outcome.error.is_none()),
        "WAL topic creation failed: {outcomes:?}"
    );
    let record = tenant_record(ranges, range_endpoints, sql_password)?;
    let mut registry = Registry::connect_with_policy(bootstrap, registry_policy.clone())
        .await
        .context("connect registry")?;
    registry.ensure_topic().await.context("registry topic")?;
    registry.upsert(&record).await.context("registry record")?;
    registry
        .upsert_tenant_config(&record, 1)
        .await
        .context("tenant config")?;
    Ok(())
}

/// Builds the tenant registry record: SQL user + SCRAM verifier plus the
/// proxy-fronted range layout.
fn tenant_record(
    ranges: u16,
    range_endpoints: &[SocketAddr],
    sql_password: &str,
) -> anyhow::Result<TenantRecord> {
    let verifier = crabka_security::scram::PgScramVerifier::generate(sql_password, 4096)
        .context("generate SCRAM verifier")?;
    let record = TenantRecord::new(
        1,
        TenantId::try_from(TENANT).context("tenant id")?,
        TenantName::try_from(TENANT).context("tenant name")?,
        TenantState::Active,
        SqlUser::try_from(SQL_USER_NAME).context("sql user")?,
        verifier.to_string(),
        1,
    )
    .context("tenant record")?
    .with_range_layout(range_layout(ranges, range_endpoints))
    .context("range layout")?;
    Ok(record)
}

/// Paths of the range mTLS fixture written into the work dir.
#[derive(Debug)]
struct TlsPaths {
    cert: PathBuf,
    key: PathBuf,
    ca: PathBuf,
}

/// Generates a throwaway range mTLS CA + peer certificate and writes the PEM
/// files into `dir`.
fn write_tls_fixture(dir: &Path) -> anyhow::Result<TlsPaths> {
    let ca = crabka_security::ca::generate_cluster_ca("loadtest-range-ca", 1)
        .context("generate range CA")?;
    let peer = crabka_security::ca::issue_broker_cert(
        &ca.cert_pem,
        &ca.key_pem,
        "loadtest-range",
        &[crabka_security::ca::SubjectAltName::Dns(
            TLS_SERVER_NAME.to_owned(),
        )],
        &[],
        1,
    )
    .context("issue range peer certificate")?;
    let write = |name: &str, bytes: &str| -> anyhow::Result<PathBuf> {
        let path = dir.join(name);
        std::fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))?;
        Ok(path)
    };
    Ok(TlsPaths {
        cert: write("range-server.crt", &peer.cert_pem)?,
        key: write("range-server.key", &peer.key_pem)?,
        ca: write("range-ca.crt", &ca.cert_pem)?,
    })
}

/// Picks two distinct free localhost ports (both probes held open together so
/// they cannot collide).
async fn pick_free_ports() -> anyhow::Result<(u16, u16)> {
    let first = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .context("bind port probe")?;
    let second = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .context("bind port probe")?;
    Ok((
        first.local_addr().context("probe addr")?.port(),
        second.local_addr().context("probe addr")?.port(),
    ))
}

/// SIGKILLs `child` (whole process group on Unix) and waits for it to be
/// reaped. Succeeds if the child already exited on its own.
async fn kill_child(label: &str, child: &mut Child, pid: u32) -> anyhow::Result<()> {
    if child
        .try_wait()
        .with_context(|| format!("poll {label} status"))?
        .is_none()
    {
        kill_process_group(pid);
        let _ = child.start_kill();
    }
    tokio::time::timeout(KILL_TIMEOUT, child.wait())
        .await
        .map_err(|_| anyhow!("{label} did not exit within {KILL_TIMEOUT:?} of SIGKILL"))?
        .with_context(|| format!("wait for {label} to exit"))?;
    Ok(())
}

/// SIGKILLs a process group by id (the group leader was spawned with
/// `process_group(0)`, so its pid is the group id).
#[cfg(unix)]
fn kill_process_group(process_group: u32) {
    let _ = std::process::Command::new("kill")
        .args(["-KILL", "--", &format!("-{process_group}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(not(unix))]
fn kill_process_group(_process_group: u32) {}

/// The last [`LOG_TAIL_LINES`] lines of a child's log, for error messages.
fn log_tail(path: &Path) -> String {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let lines: Vec<&str> = contents.lines().collect();
            let start = lines.len().saturating_sub(LOG_TAIL_LINES);
            lines[start..].join("\n")
        }
        Err(error) => format!("<read {} failed: {error}>", path.display()),
    }
}

/// Walks up from `start` to the first directory containing `Cargo.lock`.
fn find_workspace_root(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|dir| dir.join("Cargo.lock").is_file())
        .map(Path::to_path_buf)
}

/// The workspace root, found by walking up from the crate manifest dir and
/// then the current dir.
fn workspace_root() -> anyhow::Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Some(root) = find_workspace_root(&manifest_dir) {
        return Ok(root);
    }
    let current = std::env::current_dir().context("resolve current directory")?;
    find_workspace_root(&current).ok_or_else(|| {
        anyhow!(
            "no Cargo.lock found walking up from {} or {}",
            manifest_dir.display(),
            current.display()
        )
    })
}

/// Resolves one binary: the env override wins, else `target/debug/<name>`
/// under the workspace root. Errors if the file does not exist.
fn resolve_binary(
    override_path: Option<PathBuf>,
    workspace_root: &Path,
    name: &str,
) -> anyhow::Result<PathBuf> {
    let path =
        override_path.unwrap_or_else(|| workspace_root.join("target").join("debug").join(name));
    ensure!(
        path.is_file(),
        "binary {name} not found at {}; build it with \
         `cargo build -p crabka-gres -p crabka-broker -p crabka-cli` \
         or point the CRABKA_GRES_LOADTEST_*_BIN env override at it",
        path.display()
    );
    Ok(path)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// The value following `flag` in an argument vector.
    fn arg_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        args.iter()
            .position(|arg| arg == flag)
            .and_then(|index| args.get(index + 1))
            .map(String::as_str)
    }

    fn test_tls() -> TlsPaths {
        TlsPaths {
            cert: PathBuf::from("/tls/server.crt"),
            key: PathBuf::from("/tls/server.key"),
            ca: PathBuf::from("/tls/ca.crt"),
        }
    }

    #[test]
    fn incarnation_labels_disambiguate_restarts() {
        let cases = [
            ("node2", 1, "node2"),
            ("node2", 2, "node2#2"),
            ("node0", 3, "node0#3"),
            ("broker", 1, "broker"),
        ];
        for (label, incarnation, expected) in cases {
            assert!(
                incarnation_label(label, incarnation) == expected,
                "label {label} incarnation {incarnation}"
            );
        }
    }

    #[test]
    fn roster_appends_in_order_and_clones_share_state() {
        let roster = ProcessRoster::default();
        let clone = roster.clone();
        roster.push(ProcessInfo {
            label: "broker".to_owned(),
            pid: 10,
        });
        clone.push(ProcessInfo {
            label: "node0".to_owned(),
            pid: 20,
        });
        let expected = vec![
            ProcessInfo {
                label: "broker".to_owned(),
                pid: 10,
            },
            ProcessInfo {
                label: "node0".to_owned(),
                pid: 20,
            },
        ];
        assert!(roster.snapshot() == expected);
        assert!(clone.snapshot() == expected);
    }

    #[test]
    fn round_robin_assigns_ranges_to_nodes() {
        let cases = [
            (0, 3, 0),
            (1, 3, 1),
            (2, 3, 2),
            (3, 3, 0),
            (4, 3, 1),
            (5, 2, 1),
            (7, 1, 0),
        ];
        for (range, nodes, expected) in cases {
            assert!(
                node_for_range(range, nodes) == expected,
                "range {range} on {nodes} nodes"
            );
        }
    }

    #[test]
    fn range_and_host_range_flags_cover_topologies() {
        let cases: [(u16, u16, &str, &[&str]); 4] = [
            (1, 1, "0", &["r0"]),
            (2, 2, "0,1000000", &["r0", "r1"]),
            (3, 4, "0,1000000,2000000,3000000", &["r0,r3", "r1", "r2"]),
            (
                2,
                5,
                "0,1000000,2000000,3000000,4000000",
                &["r0,r2,r4", "r1,r3"],
            ),
        ];
        for (nodes, ranges, expected_ranges, expected_hosts) in cases {
            assert!(
                ranges_flag(ranges) == expected_ranges,
                "{nodes} nodes {ranges} ranges"
            );
            for (node, expected) in expected_hosts.iter().enumerate() {
                let node = u16::try_from(node).expect("node index");
                assert!(
                    host_ranges_flag(node, nodes, ranges) == *expected,
                    "node {node} of {nodes} with {ranges} ranges"
                );
            }
        }
    }

    /// Expected outcome of one [`cpu_allocation`] case: broker slice plus
    /// per-node slices, or `None` for an overcommitted layout.
    type ExpectedAllocation = Option<(&'static str, &'static [&'static str])>;

    #[test]
    fn cpu_allocation_carves_disjoint_slices_and_rejects_overcommit() {
        let cases: [(u16, u32, u32, u32, ExpectedAllocation); 7] = [
            (1, 3, 2, 16, Some(("0-1", &["2-4"]))),
            (4, 3, 2, 16, Some(("0-1", &["2-4", "5-7", "8-10", "11-13"]))),
            (2, 1, 2, 4, Some(("0-1", &["2", "3"]))),
            (4, 2, 4, 16, Some(("0-3", &["4-5", "6-7", "8-9", "10-11"]))),
            (2, 2, 1, 16, Some(("0", &["1-2", "3-4"]))),
            (4, 4, 2, 16, None),
            (1, 15, 2, 16, None),
        ];
        for (nodes, cpus, broker, available, expected) in cases {
            let result = cpu_allocation(nodes, cpus, broker, available);
            match expected {
                Some((broker, node_slices)) => {
                    let allocation = result.unwrap_or_else(|error| {
                        panic!("{nodes} nodes x {cpus} on {available}: {error}")
                    });
                    let expected = CpuAllocation {
                        broker: broker.to_owned(),
                        nodes: node_slices.iter().map(|s| (*s).to_owned()).collect(),
                    };
                    assert!(allocation == expected, "{nodes} nodes x {cpus}");
                }
                None => {
                    assert!(
                        result.is_err(),
                        "{nodes} nodes x {cpus} on {available} must overcommit"
                    );
                }
            }
        }
    }

    #[test]
    fn cpu_list_counting_handles_ranges_singles_and_rejects_garbage() {
        let good = [("0-15", 16), ("0", 1), ("0,2-3,7", 4), ("0-1,4-7", 6)];
        for (list, expected) in good {
            assert!(
                parse_cpu_list_count(list).expect(list) == expected,
                "list {list}"
            );
        }
        for bad in ["", "3-1", "x", "1-", "0,,2"] {
            assert!(parse_cpu_list_count(bad).is_err(), "list {bad:?}");
        }
    }

    #[test]
    fn node_specs_carry_their_cpu_slice_when_pinning_is_active() {
        let topology = TopologySpec {
            nodes: 2,
            ranges: 2,
            clock_skew_ms: BTreeMap::new(),
            cpus_per_node: Some(3),
            broker_cpus: None,
        };
        let allocation = cpu_allocation(2, 3, 2, 16).expect("fits");
        let tls = test_tls();
        let registry_policy = RegistryPolicy::default();
        let context = SpecContext {
            topology: &topology,
            mode: ModeSpec::LogicalTso,
            kafka_bootstrap: "127.0.0.1:19092",
            work_dir: Path::new("/work"),
            log_dir: Path::new("/work/logs"),
            tls: &tls,
            cpu_allocation: Some(&allocation),
            registry_policy: &registry_policy,
        };
        assert!(node_spec(0, &context).cpuset.as_deref() == Some("2-4"));
        assert!(node_spec(1, &context).cpuset.as_deref() == Some("5-7"));
    }

    #[test]
    fn node_specs_wire_topology_flags() {
        let topology = TopologySpec {
            nodes: 3,
            ranges: 4,
            clock_skew_ms: BTreeMap::new(),
            cpus_per_node: None,
            broker_cpus: None,
        };
        let tls = test_tls();
        let context = SpecContext {
            topology: &topology,
            mode: ModeSpec::LogicalTso,
            kafka_bootstrap: "127.0.0.1:19092",
            work_dir: Path::new("/work"),
            log_dir: Path::new("/work/logs"),
            tls: &tls,
            cpu_allocation: None,
            registry_policy: &RegistryPolicy::new(3, 15_002, 252, 502, 1_048_578).expect("policy"),
        };
        let node0 = node_spec(0, &context);
        let mut spawned_args = node0.args.clone();
        spawned_args.extend(registry_policy_args(&node0.registry_policy));
        assert!(node0.registry_policy == *context.registry_policy);
        assert!(node0.label == "node0");
        assert!(arg_value(&node0.args, "--ranges") == Some("0,1000000,2000000,3000000"));
        assert!(arg_value(&node0.args, "--host-ranges") == Some("r0,r3"));
        assert!(arg_value(&node0.args, "--tenant") == Some(TENANT));
        assert!(arg_value(&node0.args, "--substrate-bootstrap") == Some("127.0.0.1:19092"));
        assert!(arg_value(&spawned_args, "--registry-replication-factor") == Some("3"));
        assert!(arg_value(&spawned_args, "--registry-topic-create-timeout-ms") == Some("15002"));
        assert!(arg_value(&spawned_args, "--registry-reader-retry-backoff-ms") == Some("252"));
        assert!(arg_value(&spawned_args, "--registry-fetch-max-wait-ms") == Some("502"));
        assert!(
            arg_value(&spawned_args, "--registry-fetch-partition-max-bytes") == Some("1048578")
        );
    }

    #[test]
    fn node_specs_gate_checkpoints_and_skew_flags() {
        let topology = TopologySpec {
            nodes: 2,
            ranges: 2,
            clock_skew_ms: BTreeMap::from([(1, 250)]),
            cpus_per_node: None,
            broker_cpus: None,
        };
        let tls = test_tls();
        let registry_policy = RegistryPolicy::default();
        let context = SpecContext {
            topology: &topology,
            mode: ModeSpec::Hlc { max_offset_ms: 300 },
            kafka_bootstrap: "127.0.0.1:19092",
            work_dir: Path::new("/work"),
            log_dir: Path::new("/work/logs"),
            tls: &tls,
            cpu_allocation: None,
            registry_policy: &registry_policy,
        };
        let node0 = node_spec(0, &context);
        assert!(arg_value(&node0.args, "--checkpoint-store") == Some("local"));
        // Thresholds stay at the runtime defaults: a per-frame threshold would
        // make the measured workload checkpoint-bound.
        assert!(arg_value(&node0.args, "--checkpoint-frames") == None);
        assert!(arg_value(&node0.args, "--hlc-max-offset-ms") == Some("300"));
        assert!(arg_value(&node0.args, "--hlc-wall-offset-ms") == None);

        let node1 = node_spec(1, &context);
        assert!(!node1.args.iter().any(|arg| arg == "--checkpoint-store"));
        assert!(arg_value(&node1.args, "--hlc-wall-offset-ms") == Some("250"));
    }

    #[test]
    fn tenant_range_layout_uses_million_stride_boundaries() {
        let endpoints: Vec<SocketAddr> = vec![
            "127.0.0.1:1001".parse().expect("addr"),
            "127.0.0.1:1002".parse().expect("addr"),
            "127.0.0.1:1003".parse().expect("addr"),
        ];
        let entry =
            |range_id: u32, end_key: Option<RangeBoundary>, endpoint: &str| RangeLayoutEntry {
                range_id,
                end_key,
                endpoint: endpoint.to_owned(),
                wal_generation: 0,
                lifecycle: RangeLifecycle::default(),
                retirement: None,
            };
        let expected = vec![
            entry(
                0,
                Some(RangeBoundary::table_start(1_000_000)),
                "127.0.0.1:1001",
            ),
            entry(
                1,
                Some(RangeBoundary::table_start(2_000_000)),
                "127.0.0.1:1002",
            ),
            entry(2, None, "127.0.0.1:1003"),
        ];
        assert!(range_layout(3, &endpoints) == expected);
    }

    #[test]
    fn timestamp_args_cover_modes_and_skew() {
        let cases: [(ModeSpec, i64, &[&str]); 4] = [
            (
                ModeSpec::LogicalTso,
                0,
                &["--timestamp-source", "logical-tso"],
            ),
            (
                ModeSpec::Hlc { max_offset_ms: 250 },
                0,
                &["--timestamp-source", "hlc", "--hlc-max-offset-ms", "250"],
            ),
            (
                ModeSpec::Hlc { max_offset_ms: 300 },
                -100,
                &[
                    "--timestamp-source",
                    "hlc",
                    "--hlc-max-offset-ms",
                    "300",
                    "--hlc-wall-offset-ms",
                    "-100",
                ],
            ),
            (
                ModeSpec::Hlc { max_offset_ms: 250 },
                400,
                &[
                    "--timestamp-source",
                    "hlc",
                    "--hlc-max-offset-ms",
                    "250",
                    "--hlc-wall-offset-ms",
                    "400",
                ],
            ),
        ];
        for (mode, skew, expected) in cases {
            assert!(
                timestamp_args(mode, skew) == expected,
                "mode {mode} skew {skew}"
            );
        }
    }

    #[test]
    fn ready_line_parses_sql_and_range_addresses() {
        let sql: SocketAddr = "127.0.0.1:5433".parse().expect("addr");
        let range: SocketAddr = "127.0.0.1:7443".parse().expect("addr");
        assert!(
            parse_ready_line("127.0.0.1:5433 127.0.0.1:7443")
                == Some(NodeReady {
                    sql,
                    range: Some(range),
                })
        );
        assert!(parse_ready_line("127.0.0.1:5433 -") == Some(NodeReady { sql, range: None }));
        assert!(parse_ready_line("not-an-address") == None);
        assert!(parse_ready_line("") == None);
    }

    #[test]
    fn binary_resolution_prefers_override_and_reports_missing() {
        let root = tempfile::tempdir().expect("tempdir");
        let debug_dir = root.path().join("target").join("debug");
        std::fs::create_dir_all(&debug_dir).expect("target/debug");
        let default_path = debug_dir.join("crabka-gres");
        std::fs::write(&default_path, b"stub").expect("default binary");
        let override_path = root.path().join("elsewhere-gres");
        std::fs::write(&override_path, b"stub").expect("override binary");

        let resolved = resolve_binary(None, root.path(), "crabka-gres").expect("default");
        assert!(resolved == default_path);

        let resolved = resolve_binary(Some(override_path.clone()), root.path(), "crabka-gres")
            .expect("override");
        assert!(resolved == override_path);

        let missing = resolve_binary(None, root.path(), "crabka-broker")
            .expect_err("missing binary must fail");
        assert!(missing.to_string().contains("cargo build"));
        assert!(missing.to_string().contains("crabka-broker"));

        let missing_override =
            resolve_binary(Some(root.path().join("nope")), root.path(), "crabka-gres")
                .expect_err("missing override must fail");
        assert!(missing_override.to_string().contains("nope"));
    }

    #[test]
    fn workspace_root_is_found_by_cargo_lock() {
        let root = tempfile::tempdir().expect("tempdir");
        let nested = root.path().join("crates").join("something");
        std::fs::create_dir_all(&nested).expect("nested dirs");
        assert!(find_workspace_root(&nested) == None);
        std::fs::write(root.path().join("Cargo.lock"), b"").expect("lockfile");
        let found = find_workspace_root(&nested).expect("workspace root");
        assert!(found == root.path());
    }

    #[tokio::test]
    #[ignore = "needs built crabka-broker, crabka, and crabka-gres binaries"]
    async fn live_two_node_cluster_serves_any_gateway_and_survives_kill_and_restart() {
        let binaries = Binaries::resolve().expect("resolve binaries");
        let work_dir = tempfile::tempdir().expect("work dir");
        let options = ClusterOptions {
            topology: TopologySpec {
                nodes: 2,
                ranges: 2,
                clock_skew_ms: BTreeMap::new(),
                cpus_per_node: None,
                broker_cpus: None,
            },
            mode: ModeSpec::LogicalTso,
            work_dir: work_dir.path().to_path_buf(),
            binaries,
            registry_policy: RegistryPolicy::default(),
        };
        let mut cluster = Cluster::launch(options).await.expect("launch cluster");
        assert!(cluster.node_count() == 2);
        assert!(cluster.node_for_range(0) == 0);
        assert!(cluster.node_for_range(1) == 1);
        assert!(cluster.processes().len() == 3);
        let roster = cluster.process_roster();
        let labels: Vec<String> = roster
            .snapshot()
            .into_iter()
            .map(|process| process.label)
            .collect();
        assert!(labels == ["broker", "node0", "node1"]);

        // DDL through node 1's gateway — a node NOT hosting range 0 — and
        // then a write for each range through the opposite node's gateway:
        // any gateway routes DDL and DML to remotely-hosted range engines.
        run_sql(
            &cluster.sql_endpoint(1),
            &[
                "CREATE TABLE t0 (id int4)",
                "CREATE TABLE t1000000 (id int4)",
            ],
        )
        .await;
        run_sql(
            &cluster.sql_endpoint(0),
            &["INSERT INTO t1000000 VALUES (1)"],
        )
        .await;
        run_sql(&cluster.sql_endpoint(1), &["INSERT INTO t0 VALUES (1)"]).await;

        cluster.kill_node(1).await.expect("kill node 1");
        assert!(cluster.processes().len() == 2);
        // A kill leaves the roster untouched (the dead pid keeps its totals).
        assert!(roster.snapshot().len() == 3);
        cluster.restart_node(1).await.expect("restart node 1");
        // The restart appended the replacement pid under a `#2` label; the
        // original entry (and its pid) survives. The pre-restart roster
        // clone observes the append — it is live, not a snapshot.
        let entries = roster.snapshot();
        assert!(entries.len() == 4);
        assert!(entries[2].label == "node1");
        assert!(entries[3].label == "node1#2");
        assert!(entries[3].pid != entries[2].pid);
        // The restarted node serves range 1 writes through its own gateway.
        run_sql(
            &cluster.sql_endpoint(1),
            &["INSERT INTO t1000000 VALUES (2)"],
        )
        .await;
        cluster.shutdown().await.expect("shutdown");
    }

    /// Connects to an endpoint and runs each statement with `simple_query`.
    async fn run_sql(endpoint: &SqlEndpoint, statements: &[&str]) {
        let (client, connection) = tokio_postgres::Config::new()
            .host(endpoint.addr.ip().to_string())
            .port(endpoint.addr.port())
            .user(&endpoint.user)
            .password(&endpoint.password)
            .dbname(&endpoint.database)
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to sql endpoint");
        let driver = tokio::spawn(connection);
        for statement in statements {
            client.simple_query(statement).await.expect(statement);
        }
        drop(client);
        let _ = driver.await;
    }
}

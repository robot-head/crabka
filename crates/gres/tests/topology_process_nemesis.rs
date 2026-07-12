#![allow(clippy::missing_panics_doc, clippy::too_many_lines)]

#[path = "../../gres-ranges/tests/harness/process.rs"]
mod process;

use std::{
    collections::BTreeMap,
    os::unix::process::CommandExt as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use crabka_gres_control::{Registry, SplitOperationPhase, SplitOperationRecord, TenantName};
use crabka_operator::{
    context::{GresControlHandle, GresControlLike, GresControlWriteError},
    controller::{
        gres_split_operation::{
            MtlsRangeMutationClient, RangeMutationClient, SplitReconcileError,
            reconcile_activated_cutover, reconcile_one_rpc_phase, verify_target_topology_ready,
        },
        gres_tenant::reconcile_one_retiring_range_wal,
    },
};
use futures_util::FutureExt as _;
use process::ProcessHarness;
use tokio::sync::Mutex;

#[derive(Debug)]
struct LedgerEvent {
    kind: String,
    seq: i64,
    timestamp_ms: u128,
}

#[derive(Debug)]
struct AckLedger {
    acknowledgements: BTreeMap<i64, u128>,
    recovered: usize,
    max_ack_gap_ms: u128,
}

fn parse_ack_ledger(contents: &str) -> Result<AckLedger, String> {
    let mut acknowledgements = BTreeMap::new();
    let mut recovered = 0;
    for line in contents.lines() {
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|error| error.to_string())?;
        let event = LedgerEvent {
            kind: value["kind"]
                .as_str()
                .ok_or_else(|| "ledger event kind is missing".to_owned())?
                .to_owned(),
            seq: value["seq"]
                .as_i64()
                .ok_or_else(|| "ledger event sequence is missing".to_owned())?,
            timestamp_ms: u128::from(
                value["timestamp_ms"]
                    .as_u64()
                    .ok_or_else(|| "ledger event timestamp is missing".to_owned())?,
            ),
        };
        if event.kind == "ack" || event.kind == "recovered_ack" {
            if acknowledgements
                .insert(event.seq, event.timestamp_ms)
                .is_some()
            {
                return Err(format!(
                    "duplicate acknowledgement for sequence {}",
                    event.seq
                ));
            }
            recovered += usize::from(event.kind == "recovered_ack");
        }
    }
    for (expected, actual) in (0_i64..).zip(acknowledgements.keys().copied()) {
        if actual != expected {
            return Err(format!(
                "acknowledgement sequence is not contiguous: expected {expected}, found {actual}"
            ));
        }
    }
    let max_ack_gap_ms = acknowledgements
        .values()
        .zip(acknowledgements.values().skip(1))
        .map(|(left, right)| right.saturating_sub(*left))
        .max()
        .unwrap_or_default();
    Ok(AckLedger {
        acknowledgements,
        recovered,
        max_ack_gap_ms,
    })
}

fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis()
}

struct WorkloadChild {
    child: tokio::process::Child,
    process_group: u32,
    stop_path: PathBuf,
    stopped: bool,
}

impl WorkloadChild {
    async fn shutdown(&mut self) {
        std::fs::write(&self.stop_path, b"stop").expect("signal workload child stop");
        let (status, forced) =
            match tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await {
                Ok(status) => (status.expect("wait workload child"), false),
                Err(_) => {
                    terminate_process_group(self.process_group);
                    let status = tokio::time::timeout(Duration::from_secs(5), self.child.wait())
                        .await
                        .expect("terminated workload child stop timeout")
                        .expect("wait terminated workload child");
                    (status, true)
                }
            };
        assert!(forced || status.success(), "workload child failed");
        self.stopped = true;
        wait_for_process_group_exit(self.process_group).await;
    }
}

impl Drop for WorkloadChild {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        let _ = std::fs::write(&self.stop_path, b"stop");
        terminate_process_group(self.process_group);
        let _ = self.child.start_kill();
    }
}

fn terminate_process_group(process_group: u32) {
    let _ = std::process::Command::new("kill")
        .args(["-TERM", "--", &format!("-{process_group}")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

fn process_group_exists(process_group: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", "--", &format!("-{process_group}")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

async fn wait_for_process_group_exit(process_group: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_group_exists(process_group) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !process_group_exists(process_group),
        "workload process group remains"
    );
}

async fn run_with_workload_cleanup<F, T>(
    workload: &mut WorkloadChild,
    case: F,
) -> std::thread::Result<T>
where
    F: std::future::Future<Output = T>,
{
    let outcome = std::panic::AssertUnwindSafe(case).catch_unwind().await;
    workload.shutdown().await;
    outcome
}

#[tokio::test]
async fn workload_cleanup_reaps_descendants_on_error_path() {
    let root = tempfile::tempdir().expect("cleanup root");
    let stop_path = root.path().join("stop");
    let mut command = tokio::process::Command::new("bash");
    command.args(["-c", "sleep 60 & wait"]).kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let child = command.spawn().expect("spawn cleanup fixture");
    let process_group = child.id().expect("cleanup fixture pid");
    let mut workload = WorkloadChild {
        child,
        process_group,
        stop_path,
        stopped: false,
    };
    assert!(process_group_exists(process_group));
    let outcome =
        run_with_workload_cleanup(&mut workload, async { panic!("intentional case failure") })
            .await;
    assert!(outcome.is_err());
    assert!(!process_group_exists(process_group));
}

async fn wait_for_ack_count(path: &Path, minimum: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let contents = std::fs::read_to_string(path).unwrap_or_default();
        if parse_ack_ledger(&contents).is_ok_and(|ledger| ledger.acknowledgements.len() >= minimum)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "workload did not acknowledge {minimum} writes; ledger={contents:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
fn ack_ledger_rejects_duplicate_acknowledgements() {
    let ledger = concat!(
        "{\"kind\":\"attempt\",\"seq\":1,\"timestamp_ms\":10}\n",
        "{\"kind\":\"ack\",\"seq\":1,\"timestamp_ms\":11}\n",
        "{\"kind\":\"recovered_ack\",\"seq\":1,\"timestamp_ms\":12}\n",
    );
    assert!(parse_ack_ledger(ledger).is_err());
}

#[test]
fn ack_ledger_rejects_noncontiguous_sequences() {
    let ledger = concat!(
        "{\"kind\":\"ack\",\"seq\":0,\"timestamp_ms\":10}\n",
        "{\"kind\":\"ack\",\"seq\":2,\"timestamp_ms\":12}\n",
    );
    assert!(parse_ack_ledger(ledger).is_err());
}

struct BrokerControl {
    registry: Mutex<Registry>,
}

#[async_trait]
impl GresControlLike for BrokerControl {
    async fn get_tenant(
        &self,
        tenant: &TenantName,
    ) -> Result<Option<crabka_gres_control::TenantRecord>, GresControlWriteError> {
        Ok(self.registry.lock().await.get(tenant.as_str()).await?)
    }

    async fn replace_tenant_if_version(
        &self,
        record: &crabka_gres_control::TenantRecord,
        expected: Option<u64>,
    ) -> Result<crabka_gres_control::TenantRecord, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .replace_if_version(record, expected)
            .await?)
    }

    async fn delete_tenant(&self, tenant: &TenantName) -> Result<(), GresControlWriteError> {
        self.registry.lock().await.delete(tenant.as_str()).await?;
        Ok(())
    }

    async fn validate_final_checkpoint_manifest(
        &self,
        _record: &crabka_gres_control::TenantRecord,
    ) -> Result<(), GresControlWriteError> {
        Ok(())
    }

    async fn compare_and_swap_split_operation(
        &self,
        expected: u64,
        operation: &SplitOperationRecord,
    ) -> Result<SplitOperationRecord, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .compare_and_swap_split_operation(Some(expected), operation)
            .await?)
    }
}

fn cli_binary() -> PathBuf {
    std::env::var_os("CRABKA_G8_CLI_BIN").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target/debug/crabka")
        },
        PathBuf::from,
    )
}

async fn initiate_move_with_cli(system: &ProcessHarness, operation_id: &str) {
    let endpoint = format!("localhost:{}", system.endpoints()[1].1);
    let output = tokio::process::Command::new(cli_binary())
        .args([
            "gres",
            "move",
            "--bootstrap",
            system.bootstrap(),
            "--tenant",
            system.tenant(),
            "--source-range-id",
            "1",
            "--table",
            "50",
            "--operation-id",
            operation_id,
            "--replacement-range-id",
            "2",
            "--replacement-endpoint",
            &endpoint,
            "--replacement-wal-generation",
            "1",
        ])
        .output()
        .await
        .expect("run actual crabka CLI");
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn load_operation(bootstrap: &str, tenant: &str, operation_id: &str) -> SplitOperationRecord {
    let mut registry = Registry::connect(bootstrap).await.expect("registry");
    registry
        .load_split_operation(tenant, operation_id)
        .await
        .expect("load operation")
        .expect("journaled operation")
}

async fn drive_operation(
    system: &mut ProcessHarness,
    operation_id: &str,
    max_operation_duration: std::time::Duration,
    kill_after_stage: Option<&Path>,
) -> (SplitOperationRecord, Option<(u32, u32, u128)>) {
    let tenant = TenantName::try_from(system.tenant()).expect("tenant");
    let mut registry = Registry::connect(system.bootstrap())
        .await
        .expect("registry");
    registry.ensure_topic(1).await.expect("registry topic");
    let mut control: GresControlHandle = Arc::new(BrokerControl {
        registry: Mutex::new(registry),
    });
    let mut mutation_client = MtlsRangeMutationClient::new(system.operator_control_client());
    let started = Instant::now();
    let mut restarted_pids = None;
    loop {
        let current = load_operation(system.bootstrap(), system.tenant(), operation_id).await;
        if kill_after_stage.is_some_and(|_| {
            current.phase == SplitOperationPhase::Paused
                && current.evidence.tail_sha256.is_some()
                && restarted_pids.is_none()
        }) {
            wait_for_ack_count(kill_after_stage.expect("checked"), 8).await;
            let old_pid = system.pid(0);
            system.kill(0).await;
            tokio::time::sleep(Duration::from_millis(150)).await;
            system.restart_with_hosted_ranges(0, "r0,r1").await;
            let new_pid = system.pid(0);
            assert_ne!(
                old_pid, new_pid,
                "SIGKILL restart must replace the real child"
            );
            let mut fresh_registry = Registry::connect(system.bootstrap())
                .await
                .expect("fresh post-kill registry");
            fresh_registry
                .ensure_topic(1)
                .await
                .expect("registry topic");
            control = Arc::new(BrokerControl {
                registry: Mutex::new(fresh_registry),
            });
            mutation_client = MtlsRangeMutationClient::new(system.operator_control_client());
            restarted_pids = Some((old_pid, new_pid, timestamp_ms()));
        }
        match current.phase {
            SplitOperationPhase::Activated => {
                let readiness_deadline = Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    match verify_target_topology_ready(&mutation_client, &current).await {
                        Ok(()) => break,
                        Err(error) if Instant::now() < readiness_deadline => {
                            let mut debug_registry = Registry::connect(system.bootstrap())
                                .await
                                .expect("debug registry");
                            let debug_tenant = debug_registry
                                .get(system.tenant())
                                .await
                                .expect("debug tenant")
                                .expect("debug tenant present");
                            let debug_operation = debug_registry
                                .load_split_operation(system.tenant(), operation_id)
                                .await
                                .expect("debug operation")
                                .expect("debug operation present");
                            eprintln!(
                                "target readiness retry: {error}; tenant_version={} phase={:?} source_version={} current_match={} target_match={}",
                                debug_tenant.record_version,
                                debug_operation.phase,
                                debug_operation
                                    .plan
                                    .as_ref()
                                    .expect("plan")
                                    .source_record_version,
                                debug_tenant.ranges
                                    == debug_operation.plan.as_ref().expect("plan").current_layout,
                                debug_tenant.ranges
                                    == debug_operation.plan.as_ref().expect("plan").target_layout,
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        Err(error) => panic!("target readiness: {error}"),
                    }
                }
                reconcile_activated_cutover(&control, &current)
                    .await
                    .expect("atomic cutover");
            }
            SplitOperationPhase::Retiring => {
                let mut admin =
                    crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
                        .await
                        .expect("admin");
                assert!(
                    reconcile_one_retiring_range_wal(&control, &mut admin, &tenant)
                        .await
                        .expect("WAL retirement")
                );
                reconcile_one_rpc_phase(&control, &mutation_client, &current)
                    .await
                    .expect("retire predecessor RPC");
            }
            SplitOperationPhase::Completed => {
                assert!(
                    started.elapsed() < max_operation_duration,
                    "operation exceeded duration bound"
                );
                return (current, restarted_pids);
            }
            _ => match reconcile_one_rpc_phase(&control, &mutation_client, &current).await {
                Ok(_) => {}
                Err(
                    error @ (SplitReconcileError::Transport(_)
                    | SplitReconcileError::Ambiguous(_)
                    | SplitReconcileError::Registry(_)),
                ) => {
                    assert!(
                        started.elapsed() < max_operation_duration,
                        "operation deadline after transient reconcile error: {error}; source log: {}",
                        system.log(0)
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => panic!("non-retryable reconcile error: {error}"),
            },
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_move_cli_operator_and_wal_retirement() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    assert!(cli_binary().is_file(), "dedicated CI must build crabka CLI");
    let mut system = ProcessHarness::start_all_on_zero("tenant-g8-live-move").await;
    let mut ddl = String::new();
    for table in 1..50 {
        ddl.push_str(&format!("CREATE TABLE filler_{table} (id int4);"));
    }
    ddl.push_str("CREATE TABLE live_ledger (seq int4) SHARDED");
    let source = system.sql(0).await;
    source.simple_query(&ddl).await.expect("create live ledger");
    for seq in 0..32_i32 {
        source
            .execute("INSERT INTO live_ledger VALUES ($1)", &[&seq])
            .await
            .expect("acknowledged source write");
    }
    initiate_move_with_cli(&system, "g8-live-move").await;
    let operation_started = Instant::now();
    let completed = tokio::time::timeout(
        Duration::from_secs(30),
        drive_operation(&mut system, "g8-live-move", Duration::from_secs(30), None),
    )
    .await
    .expect("whole Move operation deadline")
    .0;
    assert_eq!(completed.phase, SplitOperationPhase::Completed);
    let target = system.sql(0).await;
    let count: i64 = target
        .query_one("SELECT count(*) FROM live_ledger", &[])
        .await
        .expect("target ledger")
        .get(0);
    assert_eq!(count, 32, "exact acknowledged ledger, no resurrection/loss");
    if let Some(path) = std::env::var_os("CRABKA_G8_NEMESIS_EVIDENCE") {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create evidence directory");
        }
        let evidence = serde_json::json!({
            "operation": "move",
            "completed": true,
            "acknowledged_rows": 32,
            "target_rows": count,
            "predecessor_wal_retired": true,
            "operation_elapsed_ms": operation_started.elapsed().as_millis(),
        });
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&evidence).expect("serialize evidence"),
        )
        .expect("write evidence");
    }
    system.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_move_recovers_after_paused_stage_sigkill_with_exact_ack_ledger() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    let mut system = ProcessHarness::start_all_on_zero("tenant-g8-paused-stage-kill").await;
    let mut ddl = String::new();
    for table in 1..50 {
        ddl.push_str(&format!("CREATE TABLE filler_{table} (id int4);"));
    }
    ddl.push_str("CREATE TABLE live_ledger (id int4, checksum text NOT NULL) SHARDED");
    let ddl_client = system.sql(0).await;
    ddl_client
        .simple_query(&ddl)
        .await
        .expect("create sharded workload ledger");
    drop(ddl_client);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let workload_root = tempfile::tempdir().expect("workload root");
    let ledger_path = workload_root.path().join("acks.jsonl");
    let workload_error_path = workload_root.path().join("workload-errors.log");
    let response_loss_path = workload_root.path().join("response-loss-injected");
    let stop_path = workload_root.path().join("stop");
    assert!(!ledger_path.exists(), "one fresh workload ledger per case");
    let sql_port = system.stable_sql_port();
    let workload_script = r#"
set -u
seq=0
while [[ ! -e "$CRABKA_G8_WORKLOAD_STOP" ]]; do
  checksum=$(printf 'g8-%016x' "$seq")
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  printf '{"kind":"attempt","seq":%s,"timestamp_ms":%s}\n' "$seq" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
  sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
  if timeout 2s psql -X -q -v ON_ERROR_STOP=1 -c "INSERT INTO live_ledger (id, checksum) VALUES ($seq, '$checksum')" >/dev/null 2>>"$CRABKA_G8_WORKLOAD_ERRORS"; then
    if [[ "$seq" -eq 2 && ! -e "$CRABKA_G8_RESPONSE_LOSS" ]]; then
      touch "$CRABKA_G8_RESPONSE_LOSS"
      response_known=false
    else
      response_known=true
    fi
  else
    response_known=false
  fi
  if [[ "$response_known" == true ]]; then
    kind=ack
  else
    actual=$(timeout 2s psql -X -A -t -q -v ON_ERROR_STOP=1 -c "SELECT checksum FROM live_ledger WHERE id = $seq" 2>>"$CRABKA_G8_WORKLOAD_ERRORS" || true)
    [[ "$actual" == "$checksum" ]] || continue
    kind=recovered_ack
  fi
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  printf '{"kind":"%s","seq":%s,"timestamp_ms":%s}\n' "$kind" "$seq" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
  sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
  seq=$((seq + 1))
  sleep 0.02
done
"#;
    let mut workload_command = tokio::process::Command::new("bash");
    workload_command
        .args(["-c", workload_script])
        .env("CRABKA_G8_WORKLOAD_LEDGER", &ledger_path)
        .env("CRABKA_G8_WORKLOAD_STOP", &stop_path)
        .env("CRABKA_G8_WORKLOAD_ERRORS", &workload_error_path)
        .env("CRABKA_G8_RESPONSE_LOSS", &response_loss_path)
        .env("PGHOST", "127.0.0.1")
        .env("PGPORT", sql_port.to_string())
        .env("PGUSER", "alice")
        .env("PGPASSWORD", "process-secret")
        .env("PGDATABASE", system.tenant())
        .kill_on_drop(true);
    workload_command.as_std_mut().process_group(0);
    let child = workload_command.spawn().expect("spawn real workload child");
    let workload_pid = child.id().expect("workload child pid");
    assert!(
        process_group_exists(workload_pid),
        "workload process group started"
    );
    let mut workload = WorkloadChild {
        child,
        process_group: workload_pid,
        stop_path: stop_path.clone(),
        stopped: false,
    };
    let case = async {
        wait_for_ack_count(&ledger_path, 8).await;
        initiate_move_with_cli(&system, "g8-paused-stage-kill").await;
        let operation_started = Instant::now();
        let (completed, restart) = tokio::time::timeout(
            Duration::from_secs(45),
            drive_operation(
                &mut system,
                "g8-paused-stage-kill",
                Duration::from_secs(45),
                Some(&ledger_path),
            ),
        )
        .await
        .expect("whole PausedAfterStage Move operation deadline");
        assert_eq!(completed.phase, SplitOperationPhase::Completed);
        let acknowledgements_at_completion = parse_ack_ledger(
            &std::fs::read_to_string(&ledger_path).expect("live acknowledgement ledger"),
        )
        .expect("valid live acknowledgement ledger")
        .acknowledgements
        .len();
        wait_for_ack_count(&ledger_path, acknowledgements_at_completion + 1).await;
        (completed, restart, operation_started.elapsed().as_millis())
    };
    let case = run_with_workload_cleanup(&mut workload, case).await;
    let (completed, restart, operation_elapsed_ms) = match case {
        Ok(result) => result,
        Err(_) => panic!(
            "workload case failed; psql errors: {}",
            std::fs::read_to_string(&workload_error_path).unwrap_or_default()
        ),
    };
    let (old_pid, new_pid, restart_ms) = restart.expect("PausedAfterStage SIGKILL occurred");

    let ledger = parse_ack_ledger(
        &std::fs::read_to_string(&ledger_path).expect("read final acknowledgement ledger"),
    )
    .expect("valid final acknowledgement ledger");
    const MAX_OBSERVED_SAFE_ACK_GAP_MS: u128 = 7_000;
    assert!(
        ledger.recovered >= 1,
        "deterministic response loss must recover an ACK"
    );
    assert!(
        ledger.max_ack_gap_ms <= MAX_OBSERVED_SAFE_ACK_GAP_MS,
        "maximum acknowledged-write gap {}ms exceeded observed-safe {}ms bound",
        ledger.max_ack_gap_ms,
        MAX_OBSERVED_SAFE_ACK_GAP_MS
    );
    assert!(
        ledger
            .acknowledgements
            .values()
            .any(|timestamp| *timestamp < restart_ms),
        "at least one write must be acknowledged before SIGKILL"
    );
    assert!(
        ledger
            .acknowledgements
            .values()
            .any(|timestamp| *timestamp > restart_ms),
        "at least one write must be acknowledged after restart"
    );
    let expected = ledger
        .acknowledgements
        .keys()
        .map(|seq| (*seq, format!("g8-{seq:016x}")))
        .collect::<Vec<_>>();
    let rows = system
        .sql(0)
        .await
        .query("SELECT id, checksum FROM live_ledger ORDER BY id", &[])
        .await
        .expect("read final workload ledger")
        .into_iter()
        .map(|row| (i64::from(row.get::<_, i32>(0)), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        rows, expected,
        "database rows must exactly equal durable ACK ledger"
    );

    let durable_tenant = {
        let mut registry = Registry::connect(system.bootstrap())
            .await
            .expect("registry");
        registry
            .get(system.tenant())
            .await
            .expect("tenant lookup")
            .expect("tenant")
    };
    let moved_owner = durable_tenant
        .ranges
        .iter()
        .find(|range| range.range_id == 2)
        .expect("replacement owner r2");
    assert_eq!(moved_owner.wal_generation, 1);
    assert!(
        durable_tenant
            .ranges
            .iter()
            .all(|range| range.range_id != 1)
    );
    let retirement = durable_tenant
        .range_retirements
        .iter()
        .find(|retirement| retirement.operation_id == "g8-paused-stage-kill")
        .expect("exact retirement sidecar");
    assert_eq!(
        retirement.phase,
        crabka_gres_control::RangeRetirementPhase::Parked
    );
    assert_eq!(
        completed.evidence.marker_digest.as_deref(),
        Some(retirement.checkpoint.marker_digest.as_str())
    );

    let mut admin = crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
        .await
        .expect("admin");
    let topic_names = admin
        .metadata(&[])
        .await
        .expect("topic metadata")
        .topics
        .into_iter()
        .filter(|topic| topic.error.is_none())
        .map(|topic| topic.name)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(!topic_names.contains(&format!("__gres_wal.{}.r1", system.tenant())));
    assert!(topic_names.contains(&format!("__gres_wal.{}.r2.g0000000001", system.tenant())));
    assert!(topic_names.contains(&format!("__gres_wal.{}.r0", system.tenant())));

    if let Some(path) = std::env::var_os("CRABKA_G8_KILL_EVIDENCE") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "operation": "move",
                "kill_point": "paused_after_stage",
                "completed": true,
                "old_pid": old_pid,
                "new_pid": new_pid,
                "acknowledged_rows": ledger.acknowledgements.len(),
                "recovered_acknowledgements": ledger.recovered,
                "max_ack_gap_ms": ledger.max_ack_gap_ms,
                "max_ack_gap_bound_ms": MAX_OBSERVED_SAFE_ACK_GAP_MS,
                "operation_elapsed_ms": operation_elapsed_ms,
                "predecessor_wal_retired": true,
                "replacement_owner": {"range_id": 2, "generation": 1},
                "marker_digest": completed.evidence.marker_digest,
            }))
            .expect("serialize kill evidence"),
        )
        .expect("write kill evidence");
    }
    system.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_child_keeps_range_control_receipt_runtime_alive_while_serving() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    let system = ProcessHarness::start_all_on_zero("tenant-g8-receipt-lifetime").await;
    let client = MtlsRangeMutationClient::new(system.operator_control_client());
    let response = client
        .mutate(
            &system.range_endpoint(0),
            crabka_gres_ranges::RangeControlReq {
                tenant: system.tenant().into(),
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                generation: 0,
                operation_id: "missing-operation".into(),
                operation: crabka_gres_ranges::RangeControlOperation::Status,
            },
        )
        .await
        .expect("authenticated control response");
    assert!(
        !matches!(
            response,
            crabka_gres_ranges::RangeControlResp::Rejected { ref code, .. }
                if code == "receipt_store"
        ),
        "serve path must retain the transfer backing durable receipts"
    );
    system.shutdown().await;
}

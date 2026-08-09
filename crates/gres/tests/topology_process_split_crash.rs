#![cfg(unix)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    io::Write as _,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use crabka_gres_control::{
    HashPlacement, RangeBoundary, RangeRetirementPhase, Registry, SplitOperationPhase,
    SplitOperationRecord, TenantName, TenantRecord,
};
use crabka_gres_ranges::{
    AuthorizedSplitIntent, RangeControlOperation, RangeControlReq, RangeControlResp, RangeId,
};
use crabka_operator::{
    context::{GresControlHandle, GresControlLike, GresControlWriteError},
    controller::{
        gres_split_operation::{
            MtlsRangeMutationClient, RangeMutationClient, SplitReconcileError,
            reconcile_activated_cutover, reconcile_one_rpc_phase, verify_target_topology_ready,
        },
        gres_tenant::{RangeRetirementAdmin, reconcile_one_retiring_range_wal},
    },
};
use crabka_units::convert::ByteSizeExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

fn durable_inspect_limits() -> (u32, u32) {
    let policy = crabka_gres_ranges::RangeRuntimePolicy::default();
    (
        policy.durable_inspect_max_records.get(),
        u32::try_from(policy.durable_inspect_max_size.bytes_u64()).unwrap(),
    )
}

#[path = "../../gres-ranges/tests/harness/process.rs"]
mod process;
use process::ProcessHarness;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PayloadKind {
    Attempt,
    Retry,
    Ack,
    RecoveredAck,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PayloadProvenance {
    Seed,
    Workload,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PayloadEvent {
    kind: PayloadKind,
    provenance: PayloadProvenance,
    table_id: u64,
    rowid: Option<u64>,
    seq: u64,
    checksum: String,
    timestamp_ms: u128,
}

#[derive(Debug)]
struct PayloadLedger {
    attempts: BTreeMap<(u64, u64), PayloadEvent>,
    retries: BTreeMap<(u64, u64), usize>,
    acknowledged: BTreeMap<(u64, u64), PayloadEvent>,
    recovered: usize,
    max_ack_gap_ms: u128,
}

fn parse_payload_ledger(body: &str) -> Result<PayloadLedger, String> {
    let mut attempts = BTreeMap::new();
    let mut retries: BTreeMap<(u64, u64), usize> = BTreeMap::new();
    let mut acknowledged = BTreeMap::new();
    let mut recovered = 0;
    let mut previous_timestamp = None;
    let mut previous_ack_timestamp = None;
    let mut max_ack_gap_ms = 0;

    for (index, line) in body.lines().enumerate() {
        let event: PayloadEvent = serde_json::from_str(line)
            .map_err(|error| format!("payload ledger line {}: {error}", index + 1))?;
        if event.checksum.is_empty() {
            return Err(format!(
                "payload ledger line {} has empty checksum",
                index + 1
            ));
        }
        if previous_timestamp.is_some_and(|previous| event.timestamp_ms < previous) {
            return Err(format!("payload ledger line {} regresses time", index + 1));
        }
        previous_timestamp = Some(event.timestamp_ms);
        let key = (event.table_id, event.seq);
        match event.kind {
            PayloadKind::Attempt => {
                if event.rowid.is_some() || attempts.insert(key, event).is_some() {
                    return Err(format!("invalid duplicate attempt {key:?}"));
                }
            }
            PayloadKind::Retry => {
                if event.rowid.is_some() {
                    return Err(format!("ambiguity retry {key:?} has a physical rowid"));
                }
                if !attempts.contains_key(&key) {
                    return Err(format!("ambiguity retry {key:?} has no attempt"));
                }
                *retries.entry(key).or_default() += 1;
            }
            PayloadKind::Ack | PayloadKind::RecoveredAck => {
                if event.rowid.is_none() {
                    return Err(format!("acknowledgement {key:?} has no physical rowid"));
                }
                let attempt = attempts
                    .get(&key)
                    .ok_or_else(|| format!("acknowledgement {key:?} has no attempt"))?;
                if attempt.checksum != event.checksum {
                    return Err(format!("acknowledgement {key:?} changed checksum"));
                }
                if acknowledged.insert(key, event.clone()).is_some() {
                    return Err(format!("duplicate acknowledgement {key:?}"));
                }
                if event.kind == PayloadKind::RecoveredAck {
                    recovered += 1;
                }
                if let Some(previous) = previous_ack_timestamp {
                    max_ack_gap_ms = max_ack_gap_ms.max(event.timestamp_ms - previous);
                }
                previous_ack_timestamp = Some(event.timestamp_ms);
            }
        }
    }
    if attempts.is_empty() || acknowledged.is_empty() {
        return Err("payload ledger must contain attempts and acknowledgements".into());
    }
    Ok(PayloadLedger {
        attempts,
        retries,
        acknowledged,
        recovered,
        max_ack_gap_ms,
    })
}

/// Client-side time the workload's ambiguity protocol may spend on top of
/// engine recovery before an acknowledgement can land. It covers a 3s
/// healthy-empty-read streak plus polling and psql round trips, and it is added
/// to the observed-safe engine ack-gap bounds. A 4s allowance was overshot by
/// 425ms on a CI runner in the sibling nemesis suite, so this carries a wider
/// margin.
const WORKLOAD_AMBIGUITY_RESOLUTION_MS: u128 = 6_000;

/// Client-side INSERT timeout for the live workload.
///
/// It must exceed every observed-safe ack-gap bound, that is
/// [`SplitKillPoint::pause_bound_ms`] plus
/// [`WORKLOAD_AMBIGUITY_RESOLUTION_MS`]. A statement that the client abandons
/// while the server may still commit it creates unresolvable ambiguity. The
/// client therefore abandons a statement only once the run has already blown
/// its liveness bound.
const WORKLOAD_INSERT_TIMEOUT: &str = "40s";

/// Explains a terminal rows-vs-ledger mismatch for each offending (table, seq).
///
/// It reads the client attempt records and the ambiguity-retry records to tell
/// an engine double-apply from a workload grace-window breach. An engine
/// double-apply gives extra physical rows without any client retry. A
/// grace-window breach gives extra rows after the client concluded absence and
/// re-attempted.
fn describe_payload_mismatch(
    observed: &BTreeSet<PhysicalPayloadRow>,
    expected: &BTreeSet<PhysicalPayloadRow>,
    ledger: &PayloadLedger,
) -> String {
    let mut counts: BTreeMap<(u64, u64, &str), (usize, usize)> = BTreeMap::new();
    for row in observed {
        counts
            .entry((row.table_id, row.seq, row.checksum.as_str()))
            .or_default()
            .0 += 1;
    }
    for row in expected {
        counts
            .entry((row.table_id, row.seq, row.checksum.as_str()))
            .or_default()
            .1 += 1;
    }
    let mut lines = Vec::new();
    for ((table_id, seq, checksum), (observed, acknowledged)) in counts {
        if observed == acknowledged {
            continue;
        }
        let key = (table_id, seq);
        let retries = ledger.retries.get(&key).copied().unwrap_or_default();
        let attempts = usize::from(ledger.attempts.contains_key(&key)) + retries;
        let verdict = if observed < acknowledged {
            "acknowledged write is missing from the database"
        } else if retries == 0 {
            "duplicated without any client retry, implicating the engine"
        } else {
            "duplicated after an ambiguity retry, so the workload concluded absence prematurely"
        };
        lines.push(format!(
            "table {table_id} seq {seq} checksum {checksum}: {observed} database rows vs \
             {acknowledged} acknowledged with {attempts} client attempts and {retries} \
             ambiguity retries ({verdict})"
        ));
    }
    lines.join("; ")
}

fn parse_closed_payload_ledger(path: &Path) -> Result<PayloadLedger, String> {
    let body = std::fs::read_to_string(path)
        .map_err(|error| format!("read closed payload ledger {}: {error}", path.display()))?;
    parse_payload_ledger(&body)
}

fn payload_ledger_has_ack_after(ledger: &PayloadLedger, timestamp_ms: u128) -> bool {
    ledger
        .acknowledged
        .values()
        .any(|event| event.timestamp_ms > timestamp_ms)
}

fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis()
}

fn append_payload_event(file: &mut tempfile::NamedTempFile, event: &PayloadEvent) {
    serde_json::to_writer(&mut *file, event).expect("encode payload event");
    file.write_all(b"\n").expect("terminate payload event");
    file.flush().expect("flush payload event");
    file.as_file().sync_data().expect("fsync payload event");
}

/// Project an ordinary-mode logical row onto its sealed post-split owner.
///
/// Hidden rowids are minted from the timestamp domain, so ledger rows are
/// keyed by the client-chosen `id` column. Seed rows whose minted rowid fell
/// below the static `(50, 10)` coordinator boundary stay on r0. That set
/// depends on the timestamps, so the fixture captures it empirically before the
/// workload starts and threads it through as `r0_table50_ids`.
///
/// Everything else follows the seed-versus-workload split. Table 50 flows to
/// the left successor. Table 51 seeds, with `id < 16`, stay left of the runtime
/// split boundary, and workload rows, with `id >= 16`, land on the right
/// successor.
fn successor_partition(
    table_id: u64,
    id: u64,
    r0_table50_ids: &BTreeSet<u64>,
) -> Result<u32, String> {
    match (table_id, id) {
        (50, id) if r0_table50_ids.contains(&id) => Ok(0),
        (50, _) | (51, 0..16) => Ok(2),
        (51, 16..) => Ok(3),
        _ => Err(format!(
            "logical key ({table_id},{id}) is outside the two active post-split streams"
        )),
    }
}

const fn split_payload_workload_script() -> &'static str {
    r#"
set -u
seq=0
attempted_seq=-1
while [[ ! -e "$CRABKA_G8_WORKLOAD_STOP" ]]; do
  if (( seq % 2 == 0 )); then
    table_id=50
    table_name=live_ledger50
  else
    table_id=51
    table_name=live_ledger51
  fi
  table_seq=$((seq / 2))
  rowid=$((16 + table_seq))
  checksum=$(printf 'split-%s-%016x' "$table_id" "$seq")
  if [[ "$attempted_seq" -ne "$seq" ]]; then
    now_raw=$(date +%s%N); now=$((now_raw / 1000000))
    kind=attempt
    printf '{"kind":"%s","provenance":"workload","table_id":%s,"rowid":null,"seq":%s,"checksum":"%s","timestamp_ms":%s}\n' \
      "$kind" "$table_id" "$seq" "$checksum" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
    sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
    attempted_seq=$seq
  fi
  # The client timeout must exceed every observed-safe ack-gap bound:
  # abandoning a statement the server may still commit is what creates
  # unresolvable ambiguity, so a statement is only abandoned once the run has
  # already blown its liveness bound. Connection-phase failures stay fast via
  # PGCONNECT_TIMEOUT.
  if timeout "$CRABKA_G8_INSERT_TIMEOUT" psql -X -q -v ON_ERROR_STOP=1 \
      -c "INSERT INTO $table_name (id, seq, checksum) VALUES ($rowid, $seq, '$checksum')" \
      >/dev/null 2>>"$CRABKA_G8_WORKLOAD_ERRORS"; then
    if [[ "$seq" -eq 2 && ! -e "$CRABKA_G8_RESPONSE_LOSS" ]]; then
      touch "$CRABKA_G8_RESPONSE_LOSS"
      response_known=false
    else
      response_known=true
    fi
  else
    response_known=false
  fi
  if [[ "$seq" -eq 2 ]]; then
    while [[ ! -e "$CRABKA_G8_RAW_RECOVERY" && ! -e "$CRABKA_G8_WORKLOAD_STOP" ]]; do
      sleep 0.025
    done
    [[ -e "$CRABKA_G8_RAW_RECOVERY" ]] || continue
    kind=recovered_ack
  elif [[ "$response_known" == true ]]; then
    kind=ack
  else
    # Ambiguous outcome: the attempt may still commit server-side. Resolve by
    # polling the row; a read only counts when psql SUCCEEDS, and absence is
    # concluded only after a sustained streak of healthy empty reads, so a
    # still-parked attempt has become visible (or died with its process)
    # before any re-INSERT.
    kind=""
    empty_streak_start=""
    while [[ ! -e "$CRABKA_G8_WORKLOAD_STOP" ]]; do
      if actual=$(timeout "$CRABKA_G8_RECOVERY_TIMEOUT" psql -X -A -t -q -v ON_ERROR_STOP=1 \
          -c "SELECT checksum FROM $table_name WHERE id = $rowid" \
          2>>"$CRABKA_G8_WORKLOAD_ERRORS"); then
        if [[ -n "$actual" ]]; then kind=recovered_ack; break; fi
        now_raw=$(date +%s%N); now=$((now_raw / 1000000))
        if [[ -z "$empty_streak_start" ]]; then empty_streak_start=$now; fi
        if (( now - empty_streak_start >= 3000 )); then break; fi
      else
        empty_streak_start=""
      fi
      sleep 0.25
    done
    if [[ -z "$kind" ]]; then
      [[ -e "$CRABKA_G8_WORKLOAD_STOP" ]] && break
      now_raw=$(date +%s%N); now=$((now_raw / 1000000))
      printf '{"kind":"retry","provenance":"workload","table_id":%s,"rowid":null,"seq":%s,"checksum":"%s","timestamp_ms":%s}\n' \
        "$table_id" "$seq" "$checksum" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
      sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
      continue
    fi
  fi
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  printf '{"kind":"%s","provenance":"workload","table_id":%s,"rowid":%s,"seq":%s,"checksum":"%s","timestamp_ms":%s}\n' \
    "$kind" "$table_id" "$rowid" "$seq" "$checksum" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
  sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
  seq=$((seq + 1))
  sleep "$CRABKA_G8_WORKLOAD_SLEEP"
done
"#
}

struct WorkloadChild {
    child: tokio::process::Child,
    process_group: u32,
    stop_path: PathBuf,
    stopped: bool,
}

impl WorkloadChild {
    const fn new(child: tokio::process::Child, process_group: u32, stop_path: PathBuf) -> Self {
        Self {
            child,
            process_group,
            stop_path,
            stopped: false,
        }
    }

    async fn shutdown(&mut self) {
        std::fs::write(&self.stop_path, b"stop").expect("signal workload stop");
        if let Ok(status) = tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await {
            assert!(status.expect("wait workload child").success());
        } else {
            terminate_process_group(self.process_group);
            tokio::time::timeout(Duration::from_secs(5), self.child.wait())
                .await
                .expect("terminated workload timeout")
                .expect("wait terminated workload");
        }
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
        .args(["-CONT", "--", &format!("-{process_group}")])
        .status();
    let _ = std::process::Command::new("kill")
        .args(["-TERM", "--", &format!("-{process_group}")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

fn signal_process_group(process_group: u32, signal: &str) {
    let status = std::process::Command::new("kill")
        .args([signal, "--", &format!("-{process_group}")])
        .status()
        .expect("signal workload process group");
    assert!(
        status.success(),
        "signal workload process group with {signal}"
    );
}

fn process_group_exists(process_group: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", "--", &format!("-{process_group}")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", "--", &pid.to_string()])
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
    assert!(!process_group_exists(process_group));
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ControlObservation {
    endpoint: String,
    request: RangeControlReq,
    response: RangeControlResp,
    timestamp_ms: u128,
}

struct RecordingRangeMutationClient<C> {
    inner: C,
    observations: Arc<Mutex<Vec<ControlObservation>>>,
    journal_cas_after: Option<(Receipt, Arc<OneShotControlFaults>)>,
}

impl<C> RecordingRangeMutationClient<C> {
    const fn new(inner: C, observations: Arc<Mutex<Vec<ControlObservation>>>) -> Self {
        Self {
            inner,
            observations,
            journal_cas_after: None,
        }
    }

    fn with_journal_cas_after(
        mut self,
        receipt: Option<Receipt>,
        faults: Arc<OneShotControlFaults>,
    ) -> Self {
        self.journal_cas_after = receipt.map(|receipt| (receipt, faults));
        self
    }
}

#[async_trait]
impl<C: RangeMutationClient> RangeMutationClient for RecordingRangeMutationClient<C> {
    async fn mutate(
        &self,
        endpoint: &str,
        request: RangeControlReq,
    ) -> Result<RangeControlResp, SplitReconcileError> {
        let response = self.inner.mutate(endpoint, request.clone()).await?;
        eprintln!(
            "G8_CONTROL_RESPONSE timestamp_ms={} operation={:?} response={:?}",
            timestamp_ms(),
            request.operation,
            response
        );
        let receipt = request_receipt(&request.operation);
        self.observations.lock().await.push(ControlObservation {
            endpoint: endpoint.to_owned(),
            request,
            response: response.clone(),
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_millis(),
        });
        if self
            .journal_cas_after
            .as_ref()
            .is_some_and(|(target, _)| *target == receipt)
        {
            self.journal_cas_after
                .as_ref()
                .expect("receipt fault configured")
                .1
                .arm_journal_cas_once();
        }
        Ok(response)
    }
}

#[derive(Default)]
struct OneShotControlFaults {
    journal_cas: AtomicBool,
    journal_cas_armed_once: AtomicBool,
    tenant_cas_ack: AtomicBool,
}

impl OneShotControlFaults {
    fn arm_journal_cas(&self) {
        self.journal_cas.store(true, Ordering::SeqCst);
    }

    fn arm_journal_cas_once(&self) {
        if !self.journal_cas_armed_once.swap(true, Ordering::SeqCst) {
            self.arm_journal_cas();
        }
    }

    fn take_journal_cas(&self) -> bool {
        self.journal_cas.swap(false, Ordering::SeqCst)
    }

    fn arm_tenant_cas_ack(&self) {
        self.tenant_cas_ack.store(true, Ordering::SeqCst);
    }

    fn take_tenant_cas_ack(&self) -> bool {
        self.tenant_cas_ack.swap(false, Ordering::SeqCst)
    }
}

struct BrokerControl {
    registry: Mutex<Registry>,
    faults: Arc<OneShotControlFaults>,
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
        let replaced = self
            .registry
            .lock()
            .await
            .replace_if_version(record, expected)
            .await?;
        if self.faults.take_tenant_cas_ack() {
            return Err(injected_registry_error());
        }
        Ok(replaced)
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
        if self.faults.take_journal_cas() {
            return Err(injected_registry_error());
        }
        Ok(self
            .registry
            .lock()
            .await
            .compare_and_swap_split_operation(Some(expected), operation)
            .await?)
    }
}

fn injected_registry_error() -> GresControlWriteError {
    crabka_gres_control::ControlError::UnsupportedRegistryMutation {
        mutation: "split_crash_matrix",
        reason: "injected durable acknowledgement loss",
    }
    .into()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DeleteAttemptEvidence {
    targets: Vec<String>,
    outcome: String,
}

#[derive(Clone, Debug, Default)]
struct DeleteLedger {
    exact_calls: usize,
    unrelated_attempted: bool,
    injected_after_delete: usize,
    attempts: Vec<DeleteAttemptEvidence>,
}

struct CountingRetirementAdmin {
    inner: crabka_client_admin::AdminClient,
    expected_topic: String,
    ledger: Arc<std::sync::Mutex<DeleteLedger>>,
    fail_after_delete: bool,
}

#[async_trait]
impl RangeRetirementAdmin for CountingRetirementAdmin {
    async fn metadata(
        &mut self,
        topics: &[&str],
    ) -> Result<crabka_client_admin::TopicMetadata, crabka_client_admin::AdminError> {
        self.inner.metadata(topics).await
    }

    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout: crabka_units::Time,
    ) -> Result<Vec<crabka_client_admin::DeleteTopicOutcome>, crabka_client_admin::AdminError> {
        if names != [self.expected_topic.as_str()] {
            let mut ledger = self.ledger.lock().expect("delete ledger");
            ledger.unrelated_attempted = true;
            ledger.attempts.push(DeleteAttemptEvidence {
                targets: names.iter().map(|name| (*name).to_owned()).collect(),
                outcome: "rejected_unrelated".into(),
            });
            return Err(crabka_client_admin::AdminError::Protocol(
                "unrelated Split retirement deletion".into(),
            ));
        }
        self.ledger.lock().expect("delete ledger").exact_calls += 1;
        let result = self.inner.delete_topics(names, timeout).await?;
        if self.fail_after_delete && result.iter().all(|outcome| outcome.error.is_none()) {
            self.fail_after_delete = false;
            let mut ledger = self.ledger.lock().expect("delete ledger");
            ledger.injected_after_delete += 1;
            ledger.attempts.push(DeleteAttemptEvidence {
                targets: names.iter().map(|name| (*name).to_owned()).collect(),
                outcome: "deleted_ack_lost".into(),
            });
            return Err(crabka_client_admin::AdminError::Protocol(
                "injected acknowledgement loss after predecessor delete".into(),
            ));
        }
        self.ledger
            .lock()
            .expect("delete ledger")
            .attempts
            .push(DeleteAttemptEvidence {
                targets: names.iter().map(|name| (*name).to_owned()).collect(),
                outcome: if result.iter().all(|outcome| outcome.error.is_none()) {
                    "deleted".into()
                } else {
                    "broker_error".into()
                },
            });
        Ok(result)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Family {
    SourceRestore,
    Publication,
    RetirementResume,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SplitWorkload {
    Ordinary,
    Hash,
}

#[derive(Debug, Eq, PartialEq)]
struct SplitWorkloadContract {
    point: SplitKillPoint,
    family: Family,
    pause_bound_ms: u128,
    operation_bound_ms: u128,
    restart_hosted_ranges: &'static str,
    split_args: Vec<&'static str>,
    schema_version: u32,
    physical_key_class: &'static str,
}

impl SplitWorkload {
    fn parse(value: Option<&str>) -> Result<Self, String> {
        match value {
            None | Some("ordinary") => Ok(Self::Ordinary),
            Some("hash") => Ok(Self::Hash),
            Some(value) => Err(format!("unknown Split workload {value}")),
        }
    }

    fn contract(self, point: SplitKillPoint) -> SplitWorkloadContract {
        // Ordinary mode's boundary rowid lives in the timestamp domain and is
        // captured at runtime ([`PreSplitLayout::split_boundary_rowid`]), so
        // only the routed table is static; the hash boundary is a pinned
        // bucket coordinate and stays fully static.
        let (split_args, schema_version, physical_key_class) = match self {
            Self::Ordinary => (vec!["51"], 2, "primary_version"),
            Self::Hash => (vec!["50", "0", "--bucket", "8"], 3, "hash_primary_version"),
        };
        SplitWorkloadContract {
            point,
            family: point.family(),
            pause_bound_ms: point.pause_bound_ms(),
            operation_bound_ms: SplitKillPoint::operation_bound_ms(),
            restart_hosted_ranges: point.restart_hosted_ranges(),
            split_args,
            schema_version,
            physical_key_class,
        }
    }

    const fn inter_insert_delay(self) -> &'static str {
        match self {
            Self::Ordinary => "0.02",
            Self::Hash => "0.10",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SplitKillPoint {
    InitiatedBeforeRunningCas,
    CheckpointReceiptBeforeJournalCas,
    CheckpointedAfterJournalCas,
    PauseReceiptBeforeJournalCas,
    PausedBeforeStage,
    StageReceiptBeforeJournalCas,
    StagedAfterJournalCas,
    MarkerClaimReceiptBeforeJournalCas,
    RestoredAfterJournalCas,
    PrologueReceiptBeforeJournalCas,
    ActivatedAfterJournalCas,
    TenantCasBeforeJournalCas,
    LayoutPublishedAfterJournalCas,
    RetiringBeforeDelete,
    DeleteSuccessBeforeSidecarCas,
    ParkedAfterSidecarCas,
    RetireReceiptBeforeJournalCas,
    ResumingAfterJournalCas,
    CompletedAfterJournalCas,
}

impl SplitKillPoint {
    const ALL: [Self; 19] = [
        Self::InitiatedBeforeRunningCas,
        Self::CheckpointReceiptBeforeJournalCas,
        Self::CheckpointedAfterJournalCas,
        Self::PauseReceiptBeforeJournalCas,
        Self::PausedBeforeStage,
        Self::StageReceiptBeforeJournalCas,
        Self::StagedAfterJournalCas,
        Self::MarkerClaimReceiptBeforeJournalCas,
        Self::RestoredAfterJournalCas,
        Self::PrologueReceiptBeforeJournalCas,
        Self::ActivatedAfterJournalCas,
        Self::TenantCasBeforeJournalCas,
        Self::LayoutPublishedAfterJournalCas,
        Self::RetiringBeforeDelete,
        Self::DeleteSuccessBeforeSidecarCas,
        Self::ParkedAfterSidecarCas,
        Self::RetireReceiptBeforeJournalCas,
        Self::ResumingAfterJournalCas,
        Self::CompletedAfterJournalCas,
    ];

    const fn family(self) -> Family {
        match self {
            Self::InitiatedBeforeRunningCas
            | Self::CheckpointReceiptBeforeJournalCas
            | Self::CheckpointedAfterJournalCas
            | Self::PauseReceiptBeforeJournalCas
            | Self::PausedBeforeStage
            | Self::StageReceiptBeforeJournalCas
            | Self::StagedAfterJournalCas
            | Self::MarkerClaimReceiptBeforeJournalCas
            | Self::RestoredAfterJournalCas
            | Self::PrologueReceiptBeforeJournalCas
            | Self::ActivatedAfterJournalCas => Family::SourceRestore,
            Self::TenantCasBeforeJournalCas | Self::LayoutPublishedAfterJournalCas => {
                Family::Publication
            }
            Self::RetiringBeforeDelete
            | Self::DeleteSuccessBeforeSidecarCas
            | Self::ParkedAfterSidecarCas
            | Self::RetireReceiptBeforeJournalCas
            | Self::ResumingAfterJournalCas
            | Self::CompletedAfterJournalCas => Family::RetirementResume,
        }
    }

    const fn inject_marker_before_cli(self) -> bool {
        !matches!(
            self,
            Self::InitiatedBeforeRunningCas
                | Self::CheckpointReceiptBeforeJournalCas
                | Self::CheckpointedAfterJournalCas
        )
    }

    const fn name(self) -> &'static str {
        match self {
            Self::InitiatedBeforeRunningCas => "initiated_before_running_cas",
            Self::CheckpointReceiptBeforeJournalCas => "checkpoint_receipt_before_journal_cas",
            Self::CheckpointedAfterJournalCas => "checkpointed_after_journal_cas",
            Self::PauseReceiptBeforeJournalCas => "pause_receipt_before_journal_cas",
            Self::PausedBeforeStage => "paused_before_stage",
            Self::StageReceiptBeforeJournalCas => "stage_receipt_before_journal_cas",
            Self::StagedAfterJournalCas => "staged_after_journal_cas",
            Self::MarkerClaimReceiptBeforeJournalCas => "marker_claim_receipt_before_journal_cas",
            Self::RestoredAfterJournalCas => "restored_after_journal_cas",
            Self::PrologueReceiptBeforeJournalCas => "prologue_receipt_before_journal_cas",
            Self::ActivatedAfterJournalCas => "activated_after_journal_cas",
            Self::TenantCasBeforeJournalCas => "tenant_cas_before_journal_cas",
            Self::LayoutPublishedAfterJournalCas => "layout_published_after_journal_cas",
            Self::RetiringBeforeDelete => "retiring_before_delete",
            Self::DeleteSuccessBeforeSidecarCas => "delete_success_before_sidecar_cas",
            Self::ParkedAfterSidecarCas => "parked_after_sidecar_cas",
            Self::RetireReceiptBeforeJournalCas => "retire_receipt_before_journal_cas",
            Self::ResumingAfterJournalCas => "resuming_after_journal_cas",
            Self::CompletedAfterJournalCas => "completed_after_journal_cas",
        }
    }

    fn is_ready(self, state: &SplitPredicateState) -> bool {
        *state == SplitPredicateState::for_point(self)
    }

    fn parse(value: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|point| point.name() == value)
            .ok_or_else(|| format!("unknown Split kill point {value}"))
    }

    const fn pause_bound_ms(self) -> u128 {
        match self.family() {
            // Process restart plus two-successor restore/prologue measured 21.113s in the
            // unoptimized live harness. Keep a strict CI margin without weakening the faster
            // publication and retirement families.
            Family::SourceRestore => 25_000,
            Family::Publication | Family::RetirementResume => 15_000,
        }
    }

    const fn operation_bound_ms() -> u128 {
        240_000
    }

    const fn restart_hosted_ranges(self) -> &'static str {
        match self {
            Self::InitiatedBeforeRunningCas
            | Self::CheckpointReceiptBeforeJournalCas
            | Self::CheckpointedAfterJournalCas
            | Self::PauseReceiptBeforeJournalCas
            | Self::PausedBeforeStage
            | Self::StageReceiptBeforeJournalCas
            | Self::StagedAfterJournalCas
            | Self::MarkerClaimReceiptBeforeJournalCas
            | Self::RestoredAfterJournalCas => "r0,r1",
            Self::PrologueReceiptBeforeJournalCas
            | Self::ActivatedAfterJournalCas
            | Self::TenantCasBeforeJournalCas
            | Self::LayoutPublishedAfterJournalCas
            | Self::RetiringBeforeDelete
            | Self::DeleteSuccessBeforeSidecarCas
            | Self::ParkedAfterSidecarCas
            | Self::RetireReceiptBeforeJournalCas
            | Self::ResumingAfterJournalCas
            | Self::CompletedAfterJournalCas => "r0,r2,r3",
        }
    }
}

#[test]
fn split_marker_is_injected_before_cli_once_restart_can_reacquire_pause() {
    for point in SplitKillPoint::ALL {
        let expected = !matches!(
            point,
            SplitKillPoint::InitiatedBeforeRunningCas
                | SplitKillPoint::CheckpointReceiptBeforeJournalCas
                | SplitKillPoint::CheckpointedAfterJournalCas
        );
        assert_eq!(
            point.inject_marker_before_cli(),
            expected,
            "{}",
            point.name()
        );
    }
    assert!(SplitKillPoint::PauseReceiptBeforeJournalCas.inject_marker_before_cli());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Initiated,
    Running,
    Checkpointed,
    Paused,
    Restored,
    Activated,
    LayoutPublished,
    Retiring,
    Resuming,
    Completed,
    Wrong,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum Receipt {
    None,
    Checkpoint,
    Pause,
    Stage,
    Marker,
    Prologue,
    Retire,
}

const fn request_receipt(operation: &RangeControlOperation) -> Receipt {
    match operation {
        RangeControlOperation::ForceCheckpoint => Receipt::Checkpoint,
        RangeControlOperation::PauseAtCoveredOffset { .. } => Receipt::Pause,
        RangeControlOperation::StageFilteredRestore { .. } => Receipt::Stage,
        RangeControlOperation::InheritMarkers { .. } => Receipt::Marker,
        RangeControlOperation::SuccessorFencePrologue { .. } => Receipt::Prologue,
        RangeControlOperation::RetirePredecessor => Receipt::Retire,
        RangeControlOperation::Status | RangeControlOperation::Resume => Receipt::None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Layout {
    Current,
    Target,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Sidecar {
    None,
    Parking,
    Parked,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SplitPredicateState {
    phase: Phase,
    receipt: Receipt,
    evidence: u8,
    layout: Layout,
    sidecar: Sidecar,
    predecessor_topic_present: bool,
    delete_count: usize,
    successors_serving: bool,
}

impl SplitPredicateState {
    const CHECKPOINT: u8 = 1;
    const PAUSE: u8 = 2;
    const TAIL: u8 = 4;
    const MARKERS: u8 = 8;

    const fn source(phase: Phase, receipt: Receipt, evidence: u8) -> Self {
        Self {
            phase,
            receipt,
            evidence,
            layout: Layout::Current,
            sidecar: Sidecar::None,
            predecessor_topic_present: true,
            delete_count: 0,
            successors_serving: false,
        }
    }

    const fn target(phase: Phase, sidecar: Sidecar, topic: bool, deletes: usize) -> Self {
        Self {
            phase,
            receipt: Receipt::Prologue,
            evidence: Self::CHECKPOINT | Self::PAUSE | Self::TAIL | Self::MARKERS,
            layout: Layout::Target,
            sidecar,
            predecessor_topic_present: topic,
            delete_count: deletes,
            successors_serving: true,
        }
    }

    fn for_point(point: SplitKillPoint) -> Self {
        let complete = Self::CHECKPOINT | Self::PAUSE | Self::TAIL | Self::MARKERS;
        match point {
            SplitKillPoint::InitiatedBeforeRunningCas => {
                Self::source(Phase::Initiated, Receipt::None, 0)
            }
            SplitKillPoint::CheckpointReceiptBeforeJournalCas => {
                Self::source(Phase::Running, Receipt::Checkpoint, 0)
            }
            SplitKillPoint::CheckpointedAfterJournalCas => {
                Self::source(Phase::Checkpointed, Receipt::Checkpoint, Self::CHECKPOINT)
            }
            SplitKillPoint::PauseReceiptBeforeJournalCas => {
                Self::source(Phase::Checkpointed, Receipt::Pause, Self::CHECKPOINT)
            }
            SplitKillPoint::PausedBeforeStage => Self::source(
                Phase::Paused,
                Receipt::Pause,
                Self::CHECKPOINT | Self::PAUSE,
            ),
            SplitKillPoint::StageReceiptBeforeJournalCas => Self::source(
                Phase::Paused,
                Receipt::Stage,
                Self::CHECKPOINT | Self::PAUSE,
            ),
            SplitKillPoint::StagedAfterJournalCas => Self::source(
                Phase::Paused,
                Receipt::Stage,
                Self::CHECKPOINT | Self::PAUSE | Self::TAIL,
            ),
            SplitKillPoint::MarkerClaimReceiptBeforeJournalCas => Self::source(
                Phase::Paused,
                Receipt::Marker,
                Self::CHECKPOINT | Self::PAUSE | Self::TAIL,
            ),
            SplitKillPoint::RestoredAfterJournalCas => {
                Self::source(Phase::Restored, Receipt::Marker, complete)
            }
            SplitKillPoint::PrologueReceiptBeforeJournalCas => {
                Self::source(Phase::Restored, Receipt::Prologue, complete)
            }
            SplitKillPoint::ActivatedAfterJournalCas => {
                let mut state = Self::source(Phase::Activated, Receipt::Prologue, complete);
                state.successors_serving = true;
                state
            }
            SplitKillPoint::TenantCasBeforeJournalCas => {
                Self::target(Phase::Activated, Sidecar::Parking, true, 0)
            }
            SplitKillPoint::LayoutPublishedAfterJournalCas => {
                Self::target(Phase::LayoutPublished, Sidecar::Parking, true, 0)
            }
            SplitKillPoint::RetiringBeforeDelete => {
                Self::target(Phase::Retiring, Sidecar::Parking, true, 0)
            }
            SplitKillPoint::DeleteSuccessBeforeSidecarCas => {
                Self::target(Phase::Retiring, Sidecar::Parking, false, 1)
            }
            SplitKillPoint::ParkedAfterSidecarCas => {
                Self::target(Phase::Retiring, Sidecar::Parked, false, 1)
            }
            SplitKillPoint::RetireReceiptBeforeJournalCas => {
                let mut state = Self::target(Phase::Retiring, Sidecar::Parked, false, 1);
                state.receipt = Receipt::Retire;
                state
            }
            SplitKillPoint::ResumingAfterJournalCas => {
                let mut state = Self::target(Phase::Resuming, Sidecar::Parked, false, 1);
                state.receipt = Receipt::Retire;
                state
            }
            SplitKillPoint::CompletedAfterJournalCas => {
                let mut state = Self::target(Phase::Completed, Sidecar::Parked, false, 1);
                state.receipt = Receipt::Retire;
                state
            }
        }
    }
}

fn observed_receipt(observations: &[ControlObservation]) -> Receipt {
    observations
        .iter()
        .rev()
        .find_map(|observation| match observation.request.operation {
            RangeControlOperation::ForceCheckpoint => Some(Receipt::Checkpoint),
            RangeControlOperation::PauseAtCoveredOffset { .. } => Some(Receipt::Pause),
            RangeControlOperation::StageFilteredRestore { .. } => Some(Receipt::Stage),
            RangeControlOperation::InheritMarkers { .. } => Some(Receipt::Marker),
            RangeControlOperation::SuccessorFencePrologue { .. } => Some(Receipt::Prologue),
            RangeControlOperation::RetirePredecessor => Some(Receipt::Retire),
            RangeControlOperation::Status | RangeControlOperation::Resume => None,
        })
        .unwrap_or(Receipt::None)
}

fn predicate_state(
    operation: &SplitOperationRecord,
    tenant: &TenantRecord,
    observations: &[ControlObservation],
    predecessor_topic_present: bool,
    delete_count: usize,
    successors_serving: bool,
) -> SplitPredicateState {
    let phase = match operation.phase {
        SplitOperationPhase::Initiated => Phase::Initiated,
        SplitOperationPhase::Running => Phase::Running,
        SplitOperationPhase::Checkpointed => Phase::Checkpointed,
        SplitOperationPhase::Paused => Phase::Paused,
        SplitOperationPhase::Restored => Phase::Restored,
        SplitOperationPhase::Activated => Phase::Activated,
        SplitOperationPhase::LayoutPublished => Phase::LayoutPublished,
        SplitOperationPhase::Retiring => Phase::Retiring,
        SplitOperationPhase::Resuming => Phase::Resuming,
        SplitOperationPhase::Completed => Phase::Completed,
        SplitOperationPhase::Failed => Phase::Wrong,
    };
    let evidence = u8::from(operation.evidence.manifest_key.is_some())
        | (u8::from(operation.evidence.barrier_offset.is_some()) << 1)
        | (u8::from(operation.evidence.tail_sha256.is_some()) << 2)
        | (u8::from(operation.evidence.marker_digest.is_some()) << 3);
    let plan = operation.plan.as_ref().expect("sealed Split plan");
    let layout = if tenant.ranges == plan.target_layout {
        Layout::Target
    } else {
        assert_eq!(tenant.ranges, plan.current_layout);
        Layout::Current
    };
    let sidecar = tenant
        .range_retirements
        .iter()
        .find(|retirement| retirement.operation_id == operation.operation_id)
        .map_or(Sidecar::None, |retirement| match retirement.phase {
            RangeRetirementPhase::Parking => Sidecar::Parking,
            RangeRetirementPhase::Parked => Sidecar::Parked,
        });
    SplitPredicateState {
        phase,
        receipt: observed_receipt(observations),
        evidence,
        layout,
        sidecar,
        predecessor_topic_present,
        delete_count,
        successors_serving,
    }
}

const fn should_yield_parked_observation(already_yielded: bool, sidecar_parked: bool) -> bool {
    !already_yielded && sidecar_parked
}

#[test]
fn split_kill_points_are_exhaustive_unique_and_sharded() {
    let names = SplitKillPoint::ALL.map(SplitKillPoint::name);
    assert_eq!(names.len(), 19);
    assert_eq!(names.into_iter().collect::<BTreeSet<_>>().len(), 19);
    assert_eq!(
        SplitKillPoint::ALL
            .iter()
            .filter(|point| point.family() == Family::SourceRestore)
            .count(),
        11
    );
    assert_eq!(
        SplitKillPoint::ALL
            .iter()
            .filter(|point| point.family() == Family::Publication)
            .count(),
        2
    );
    assert_eq!(
        SplitKillPoint::ALL
            .iter()
            .filter(|point| point.family() == Family::RetirementResume)
            .count(),
        6
    );
}

#[test]
fn split_kill_point_predicates_accept_only_their_exact_boundary() {
    for point in SplitKillPoint::ALL {
        let state = SplitPredicateState::for_point(point);
        assert!(point.is_ready(&state), "exact {} predicate", point.name());

        let mut wrong_phase = state.clone();
        wrong_phase.phase = Phase::Wrong;
        assert!(
            !point.is_ready(&wrong_phase),
            "{} rejects wrong phase",
            point.name()
        );

        let mut changed_receipt = state.clone();
        changed_receipt.receipt = Receipt::None;
        if state.receipt != Receipt::None {
            assert!(
                !point.is_ready(&changed_receipt),
                "{} rejects missing receipt",
                point.name()
            );
        }
    }
}

#[test]
fn split_kill_points_define_restart_and_deadline_contracts() {
    for point in SplitKillPoint::ALL {
        assert_eq!(SplitKillPoint::parse(point.name()), Ok(point));
        assert!(point.pause_bound_ms() > 0);
        assert!(SplitKillPoint::operation_bound_ms() > point.pause_bound_ms());
        assert!(!point.restart_hosted_ranges().is_empty());
    }
    assert!(SplitKillPoint::parse("retiring_after_journal_cas").is_err());
}

#[test]
fn every_split_predicate_field_fails_closed_on_a_near_miss() {
    for point in SplitKillPoint::ALL {
        let exact = SplitPredicateState::for_point(point);
        let mut near_misses = Vec::new();

        let mut changed = exact.clone();
        changed.phase = Phase::Wrong;
        near_misses.push(changed);
        let mut changed = exact.clone();
        changed.receipt = if exact.receipt == Receipt::None {
            Receipt::Checkpoint
        } else {
            Receipt::None
        };
        near_misses.push(changed);
        let mut changed = exact.clone();
        changed.evidence ^= SplitPredicateState::CHECKPOINT;
        near_misses.push(changed);
        let mut changed = exact.clone();
        changed.layout = match exact.layout {
            Layout::Current => Layout::Target,
            Layout::Target => Layout::Current,
        };
        near_misses.push(changed);
        let mut changed = exact.clone();
        changed.sidecar = match exact.sidecar {
            Sidecar::None | Sidecar::Parked => Sidecar::Parking,
            Sidecar::Parking => Sidecar::Parked,
        };
        near_misses.push(changed);
        let mut changed = exact.clone();
        changed.predecessor_topic_present = !exact.predecessor_topic_present;
        near_misses.push(changed);
        let mut changed = exact.clone();
        changed.delete_count += 1;
        near_misses.push(changed);
        let mut changed = exact.clone();
        changed.successors_serving = !exact.successors_serving;
        near_misses.push(changed);

        for near_miss in near_misses {
            assert!(
                !point.is_ready(&near_miss),
                "{} must reject near miss {near_miss:?}",
                point.name()
            );
        }
    }
}

#[test]
fn parked_retirement_is_observed_once_before_retire_rpc() {
    for (already_yielded, sidecar_parked, expected) in [
        (false, false, false),
        (false, true, true),
        (true, false, false),
        (true, true, false),
    ] {
        assert_eq!(
            should_yield_parked_observation(already_yielded, sidecar_parked),
            expected,
        );
    }
}

#[test]
fn payload_ledger_parses_attempt_ack_and_recovered_ack() {
    let parsed = parse_payload_ledger(
        r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"attempt","provenance":"workload","table_id":51,"rowid":null,"seq":2,"checksum":"b","timestamp_ms":18}
{"kind":"recovered_ack","provenance":"workload","table_id":51,"rowid":16,"seq":2,"checksum":"b","timestamp_ms":20}
"#,
    )
    .expect("strict payload ledger");
    assert_eq!(
        (
            parsed.attempts.len(),
            parsed.acknowledged.len(),
            parsed.recovered,
            parsed.max_ack_gap_ms,
        ),
        (2, 2, 1, 8)
    );
    assert!(!payload_ledger_has_ack_after(&parsed, 20));
    assert!(payload_ledger_has_ack_after(&parsed, 19));
}

#[test]
fn payload_ledger_counts_attempts_and_ambiguity_retries() {
    let parsed = parse_payload_ledger(
        r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":17,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"attempt","provenance":"workload","table_id":51,"rowid":null,"seq":2,"checksum":"b","timestamp_ms":18}
{"kind":"retry","provenance":"workload","table_id":51,"rowid":null,"seq":2,"checksum":"b","timestamp_ms":19}
{"kind":"recovered_ack","provenance":"workload","table_id":51,"rowid":16,"seq":2,"checksum":"b","timestamp_ms":20}
"#,
    )
    .expect("retried payload ledger");
    assert_eq!(parsed.acknowledged.len(), 2);
    assert_eq!(parsed.recovered, 1);
    assert_eq!(parsed.retries, BTreeMap::from([((51, 2), 1)]));
}

#[test]
fn payload_ledger_rejects_orphan_or_physical_retries() {
    assert!(
        parse_payload_ledger(
            r#"{"kind":"retry","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}"#,
        )
        .is_err()
    );
    assert!(
        parse_payload_ledger(
            r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"retry","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":11}"#,
        )
        .is_err()
    );
}

#[test]
fn payload_mismatch_distinguishes_engine_duplicates_from_workload_retries() {
    use assert2::assert;
    let ledger = parse_payload_ledger(
        r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":17,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"attempt","provenance":"workload","table_id":51,"rowid":null,"seq":2,"checksum":"b","timestamp_ms":18}
{"kind":"retry","provenance":"workload","table_id":51,"rowid":null,"seq":2,"checksum":"b","timestamp_ms":19}
{"kind":"recovered_ack","provenance":"workload","table_id":51,"rowid":16,"seq":2,"checksum":"b","timestamp_ms":20}
"#,
    )
    .expect("retried payload ledger");
    let row = |table_id, rowid, seq, checksum: &str| PhysicalPayloadRow {
        table_id,
        rowid,
        seq,
        checksum: checksum.to_owned(),
    };
    let expected = BTreeSet::from([row(50, 17, 1, "a"), row(51, 16, 2, "b")]);

    let engine_duplicate = BTreeSet::from([
        row(50, 17, 1, "a"),
        row(50, 42, 1, "a"),
        row(51, 16, 2, "b"),
    ]);
    let message = describe_payload_mismatch(&engine_duplicate, &expected, &ledger);
    assert!(message.contains("table 50 seq 1"));
    assert!(message.contains("implicating the engine"));

    let retry_duplicate = BTreeSet::from([
        row(50, 17, 1, "a"),
        row(51, 16, 2, "b"),
        row(51, 43, 2, "b"),
    ]);
    let message = describe_payload_mismatch(&retry_duplicate, &expected, &ledger);
    assert!(message.contains("table 51 seq 2"));
    assert!(message.contains("2 client attempts and 1 ambiguity retries"));
    assert!(message.contains("concluded absence prematurely"));

    let missing = BTreeSet::from([row(50, 17, 1, "a")]);
    let message = describe_payload_mismatch(&missing, &expected, &ledger);
    assert!(message.contains("missing from the database"));
}

#[test]
fn payload_ledger_rejects_incomplete_or_inconsistent_events() {
    assert!(parse_payload_ledger(r#"{"kind":"ack"}"#).is_err());
    assert!(
        serde_json::from_str::<PayloadEvent>(
            r#"{"kind":"attempt","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}"#,
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<PayloadEvent>(
            r#"{"kind":"attempt","provenance":"mystery","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}"#,
        )
        .is_err()
    );
    assert!(
        parse_payload_ledger(
            r#"{"kind":"ack","provenance":"workload","table_id":50,"rowid":1,"seq":1,"checksum":"a","timestamp_ms":10}"#,
        )
        .is_err()
    );
}

#[test]
fn payload_ledger_projects_each_table_to_its_sealed_successor() {
    let r0_ids = BTreeSet::from([1, 2]);
    for (table_id, id, expected_range) in [
        (50, 1, 0),
        (50, 2, 0),
        (50, 3, 2),
        (50, 20, 2),
        (51, 1, 2),
        (51, 15, 2),
        (51, 16, 3),
        (51, 32, 3),
    ] {
        assert_eq!(
            successor_partition(table_id, id, &r0_ids),
            Ok(expected_range)
        );
    }
    assert_eq!(successor_partition(50, 7, &BTreeSet::new()), Ok(2));
    assert!(successor_partition(52, 1, &r0_ids).is_err());
}

#[test]
fn payload_ledger_rejects_duplicate_ack_checksum_and_time_regression() {
    let duplicate = r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":13}"#;
    let checksum = r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"b","timestamp_ms":12}"#;
    let regression = r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":10}"#;
    for invalid_ledger in [duplicate, checksum, regression] {
        assert!(parse_payload_ledger(invalid_ledger).is_err());
    }
}

#[test]
fn payload_ledger_is_parsed_only_after_fsync_and_close() {
    let mut file = tempfile::NamedTempFile::new().expect("payload ledger");
    file.write_all(
        br#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":16,"seq":1,"checksum":"a","timestamp_ms":12}
"#,
    )
    .expect("write payload ledger");
    file.flush().expect("flush payload ledger");
    file.as_file().sync_all().expect("fsync payload ledger");
    let path = file.into_temp_path();
    let ledger = parse_closed_payload_ledger(&path).expect("reopened payload ledger");
    assert_eq!(ledger.acknowledged.len(), 1);
}

#[test]
fn continuous_payload_workload_records_two_tables_and_fsyncs_every_event() {
    let script = split_payload_workload_script();
    for required_fragment in [
        "table_id=50",
        "table_id=51",
        "kind=attempt",
        "kind=recovered_ack",
        "rowid=$((16 + table_seq))",
        "live_ledger50",
        "live_ledger51",
        "WHERE id = $rowid",
        "CRABKA_G8_RECOVERY_TIMEOUT",
        "CRABKA_G8_RAW_RECOVERY",
        "CRABKA_G8_WORKLOAD_SLEEP",
        "CRABKA_G8_INSERT_TIMEOUT",
        "\"kind\":\"retry\"",
        "empty_streak_start",
    ] {
        assert!(script.contains(required_fragment));
    }
    assert!(script.matches("sync -d").count() >= 3);
    let raw_wait = script
        .find("while [[ ! -e \"$CRABKA_G8_RAW_RECOVERY\"")
        .expect("seq=2 waits for exact raw authorization");
    let recovered = script[raw_wait..]
        .find("kind=recovered_ack")
        .expect("raw-authorized seq=2 emits recovered acknowledgement");
    let ordinary = script[raw_wait..]
        .find("elif [[ \"$response_known\" == true ]]")
        .expect("ordinary acknowledgements remain response-driven");
    assert!(recovered < ordinary);
}

#[tokio::test]
async fn workload_child_shutdown_reaps_its_process_group() {
    use std::os::unix::process::CommandExt as _;

    let root = tempfile::tempdir().expect("workload cleanup root");
    let stop_path = root.path().join("stop");
    let mut command = tokio::process::Command::new("bash");
    command
        .args(["-c", "while [[ ! -e \"$STOP\" ]]; do sleep 0.02; done"])
        .env("STOP", &stop_path)
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let child = command.spawn().expect("spawn workload cleanup fixture");
    let process_group = child.id().expect("workload cleanup pid");
    let mut workload = WorkloadChild::new(child, process_group, stop_path);
    assert!(process_group_exists(process_group));
    workload.shutdown().await;
    assert!(!process_group_exists(process_group));
}

struct FixtureMutationClient;

#[async_trait]
impl RangeMutationClient for FixtureMutationClient {
    async fn mutate(
        &self,
        _endpoint: &str,
        _request: RangeControlReq,
    ) -> Result<RangeControlResp, SplitReconcileError> {
        Ok(RangeControlResp::Checkpoint {
            generation: 0,
            covered_offset: 9,
            manifest_key: "manifest".into(),
        })
    }
}

#[tokio::test]
async fn control_observation_records_exact_forwarded_request_and_response() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let client =
        RecordingRangeMutationClient::new(FixtureMutationClient, Arc::clone(&observations));
    let request = RangeControlReq {
        tenant: "tenant-a".into(),
        range_id: RangeId::new(1),
        generation: 0,
        operation_id: "split-a".into(),
        operation: RangeControlOperation::ForceCheckpoint,
    };
    let response = client
        .mutate("127.0.0.1:9092", request.clone())
        .await
        .expect("fixture response");
    let recorded = observations.lock().await;
    let [observation] = recorded.as_slice() else {
        panic!("expected exactly one control observation")
    };
    assert_eq!(
        observation,
        &ControlObservation {
            endpoint: "127.0.0.1:9092".into(),
            request,
            response,
            timestamp_ms: observation.timestamp_ms,
        }
    );
}

#[test]
fn one_shot_control_faults_fire_exactly_once() {
    let faults = OneShotControlFaults::default();
    faults.arm_journal_cas();
    assert!(faults.take_journal_cas());
    assert!(!faults.take_journal_cas());
    faults.arm_tenant_cas_ack();
    assert!(faults.take_tenant_cas_ack());
    assert!(!faults.take_tenant_cas_ack());
}

struct ReceiptFixtureMutationClient;

#[async_trait]
impl RangeMutationClient for ReceiptFixtureMutationClient {
    async fn mutate(
        &self,
        _endpoint: &str,
        request: RangeControlReq,
    ) -> Result<RangeControlResp, SplitReconcileError> {
        Ok(match request.operation {
            RangeControlOperation::InheritMarkers { .. } => RangeControlResp::Markers {
                markers: Vec::new(),
                left_markers: Some(Vec::new()),
                right_markers: Some(Vec::new()),
                digest: "fixture".into(),
            },
            _ => RangeControlResp::Applied,
        })
    }
}

#[tokio::test]
async fn marker_receipt_fault_arms_only_after_marker_response() {
    let faults = Arc::new(OneShotControlFaults::default());
    let observations = Arc::new(Mutex::new(Vec::new()));
    let client =
        RecordingRangeMutationClient::new(ReceiptFixtureMutationClient, Arc::clone(&observations))
            .with_journal_cas_after(Some(Receipt::Marker), Arc::clone(&faults));
    let request = |operation| RangeControlReq {
        tenant: "tenant-a".into(),
        range_id: RangeId::new(1),
        generation: 0,
        operation_id: "split-a".into(),
        operation,
    };

    client
        .mutate(
            "fixture",
            request(RangeControlOperation::StageFilteredRestore {
                journal_revision: 4,
                journal_digest: "staged".into(),
            }),
        )
        .await
        .expect("stage response");
    assert!(!faults.take_journal_cas());

    client
        .mutate(
            "fixture",
            request(RangeControlOperation::InheritMarkers {
                journal_revision: 5,
                journal_digest: "markers".into(),
            }),
        )
        .await
        .expect("marker response");
    assert_eq!(
        observed_receipt(&observations.lock().await),
        Receipt::Marker
    );
    assert!(
        faults.take_journal_cas(),
        "next journal CAS acknowledgement is withheld"
    );
    assert!(!faults.take_journal_cas(), "fault fires exactly once");
}

#[test]
fn every_receipt_fault_targets_one_exact_rpc_response() {
    let cases = [
        (
            SplitKillPoint::CheckpointReceiptBeforeJournalCas,
            Receipt::Checkpoint,
        ),
        (SplitKillPoint::PauseReceiptBeforeJournalCas, Receipt::Pause),
        (SplitKillPoint::StageReceiptBeforeJournalCas, Receipt::Stage),
        (
            SplitKillPoint::MarkerClaimReceiptBeforeJournalCas,
            Receipt::Marker,
        ),
        (
            SplitKillPoint::PrologueReceiptBeforeJournalCas,
            Receipt::Prologue,
        ),
        (
            SplitKillPoint::RetireReceiptBeforeJournalCas,
            Receipt::Retire,
        ),
    ];
    let targets = cases
        .into_iter()
        .map(|(point, expected)| {
            assert_eq!(receipt_fault_receipt(point), Some(expected));
            expected
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(targets.len(), cases.len());
}

#[test]
fn crash_family_availability_bounds_are_strict_and_distinct() {
    for (point, expected_bound_ms) in [
        (SplitKillPoint::MarkerClaimReceiptBeforeJournalCas, 25_000),
        (SplitKillPoint::TenantCasBeforeJournalCas, 15_000),
        (SplitKillPoint::RetiringBeforeDelete, 15_000),
    ] {
        assert_eq!(point.pause_bound_ms(), expected_bound_ms);
    }
}

#[test]
fn publication_and_early_retirement_predicates_retain_prologue_receipt() {
    for point in [
        SplitKillPoint::TenantCasBeforeJournalCas,
        SplitKillPoint::LayoutPublishedAfterJournalCas,
        SplitKillPoint::RetiringBeforeDelete,
        SplitKillPoint::DeleteSuccessBeforeSidecarCas,
        SplitKillPoint::ParkedAfterSidecarCas,
    ] {
        assert_eq!(
            SplitPredicateState::for_point(point).receipt,
            Receipt::Prologue
        );
    }
}

#[test]
fn marker_session_lifecycle_preserves_pending_state_until_prologue_or_crash() {
    for (crashed, prologue, phase, expected) in [
        (
            false,
            false,
            SplitOperationPhase::Paused,
            MarkerSessionAction::Keep,
        ),
        (
            true,
            false,
            SplitOperationPhase::Paused,
            MarkerSessionAction::DropAfterCrash,
        ),
        (
            false,
            true,
            SplitOperationPhase::Restored,
            MarkerSessionAction::RollbackAfterPrologue,
        ),
    ] {
        assert_eq!(
            marker_session_action(true, crashed, prologue, phase),
            expected
        );
    }
}

#[test]
fn marker_rollback_accepts_only_the_post_publication_stale_session_rejection() {
    for (sqlstate, message, expected) in [
        (
            "0A000",
            "range map changed; reconnect before issuing another statement",
            true,
        ),
        (
            "XX000",
            "range map changed; reconnect before issuing another statement",
            false,
        ),
        ("0A000", "unrelated feature rejection", false),
    ] {
        assert_eq!(
            is_expected_marker_rollback_rejection(sqlstate, message),
            expected
        );
    }
}

#[test]
fn ordinary_and_hash_workloads_share_kill_mapping_but_not_physical_contract() {
    for point in SplitKillPoint::ALL {
        for (workload, split_args, schema_version, physical_key_class) in [
            (SplitWorkload::Ordinary, vec!["51"], 2, "primary_version"),
            (
                SplitWorkload::Hash,
                vec!["50", "0", "--bucket", "8"],
                3,
                "hash_primary_version",
            ),
        ] {
            assert_eq!(
                workload.contract(point),
                SplitWorkloadContract {
                    point,
                    family: point.family(),
                    pause_bound_ms: point.pause_bound_ms(),
                    operation_bound_ms: SplitKillPoint::operation_bound_ms(),
                    restart_hosted_ranges: point.restart_hosted_ranges(),
                    split_args,
                    schema_version,
                    physical_key_class,
                }
            );
        }
    }
}

#[test]
fn split_workload_mode_is_explicit_and_fail_closed() {
    use assert2::assert;
    for (value, expected) in [
        (None, Ok(SplitWorkload::Ordinary)),
        (Some("ordinary"), Ok(SplitWorkload::Ordinary)),
        (Some("hash"), Ok(SplitWorkload::Hash)),
    ] {
        assert_eq!(SplitWorkload::parse(value), expected);
    }
    for invalid in ["", "HASH"] {
        assert!(SplitWorkload::parse(Some(invalid)).is_err());
    }
    for (workload, expected_delay) in [
        (SplitWorkload::Ordinary, "0.02"),
        (SplitWorkload::Hash, "0.10"),
    ] {
        assert_eq!(workload.inter_insert_delay(), expected_delay);
    }
    let insert_timeout_ms: u128 = WORKLOAD_INSERT_TIMEOUT
        .strip_suffix('s')
        .expect("seconds-denominated INSERT timeout")
        .parse::<u128>()
        .expect("integral INSERT timeout")
        * 1_000;
    for point in SplitKillPoint::ALL {
        assert!(insert_timeout_ms > point.pause_bound_ms() + WORKLOAD_AMBIGUITY_RESOLUTION_MS);
    }
}

#[test]
fn hash_schema_v3_pins_algorithm_corpus_and_boundary() {
    let evidence = HashAlgorithmEvidence::pinned();
    let corpus = [5, 2, 15, 12, 9, 6, 3, 0, 13, 10, 7, 4, 1, 14, 11, 8]
        .into_iter()
        .enumerate()
        .map(|(logical_id, bucket)| HashCorpusEvidence {
            logical_id: i32::try_from(logical_id).unwrap(),
            bytes_hex: format!("{logical_id:08x}"),
            bucket,
        })
        .collect();
    assert_eq!(
        evidence,
        HashAlgorithmEvidence {
            name: "fnv1a64-int4-be",
            offset_basis: 0xcbf2_9ce4_8422_2325,
            prime: 0x0100_0000_01b3,
            bucket_count: 16,
            corpus,
        }
    );
    assert_eq!(
        HashBoundaryEvidence::pinned(),
        HashBoundaryEvidence {
            table_id: 50,
            bucket: 8,
            rowid: 0,
            request_bucket: 8,
            receipt_bucket: 8,
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_child_hash_durable_inspection_covers_pinned_bucket_corpus() {
    if std::env::var_os("CRABKA_G9_HASH_INSPECT").is_none() {
        return;
    }
    let tenant = format!("tg9hi-{}", std::process::id());
    let mut system = ProcessHarness::start_all_on_zero(&tenant).await;
    let sql = system.sql(0).await;
    let mut ddl = String::new();
    for table in 1..50 {
        write!(&mut ddl, "CREATE TABLE filler_{table} (id int4);").expect("write DDL to string");
    }
    ddl.push_str("CREATE TABLE hash_probe50 (id int4 NOT NULL) SHARDED BY HASH (id) BUCKETS 16;");
    sql.simple_query(&ddl).await.expect("create hash probe");
    for id in 0..16 {
        sql.simple_query(&format!("INSERT INTO hash_probe50 VALUES ({id})"))
            .await
            .expect("insert pinned hash probe");
    }
    sql.simple_query("INSERT INTO hash_probe50 VALUES (23), (31)")
        .await
        .expect("commit cross-boundary hash transaction");
    sql.simple_query("BEGIN; INSERT INTO hash_probe50 VALUES (7), (15); ROLLBACK;")
        .await
        .expect("roll back cross-boundary hash transaction");
    drop(sql);
    system.restart_with_hosted_ranges(0, "r0,r1").await;

    let start_key = crabka_pgkv::key::table_prefix(50);
    let mut end_key = start_key.clone();
    *end_key.last_mut().expect("table prefix") += 1;
    let mut buckets = BTreeSet::new();
    let mut decoded_rows = Vec::new();
    let mut aborted_rows = BTreeSet::new();
    let mut cross_boundary_descriptor = None;
    let mut rolled_back_descriptor = None;
    for range_id in [0, 1] {
        let response = system
            .inspect_durable_records(crabka_gres_ranges::InspectDurableRecordsReq {
                tenant: tenant.clone(),
                range_id: RangeId::new(range_id),
                generation: 0,
                table_id: 50,
                start_key: start_key.clone(),
                end_key: end_key.clone(),
                max_records: durable_inspect_limits().0,
                max_bytes: durable_inspect_limits().1,
                snapshot_offset: None,
                cursor: None,
            })
            .await;
        assert!(response.next_cursor.is_none());
        assert!(response.provenance.sample_offset >= 0);
        for record in &response.records {
            assert!(record.source_offset.is_some());
            assert!(record.source_revision.is_some());
            match crabka_pgkv::key::classify_key(&record.key) {
                crabka_pgkv::key::KeyClass::HashPrimaryVersion {
                    table_id, bucket, ..
                } => {
                    assert_eq!(table_id, 50);
                    let tuple = crabka_pgmvcc::version::decode_ts_tuple(&record.value)
                        .expect("decode authoritative hash timestamp row");
                    if matches!(tuple.state, crabka_pgmvcc::version::TsVersionState::Aborted) {
                        let Some(crabka_pgtypes::Datum::Int4(logical_id)) = tuple.row.first()
                        else {
                            panic!("aborted hash tuple lacks logical int4 id")
                        };
                        assert!(aborted_rows.insert((*logical_id, bucket)));
                        continue;
                    }
                    assert!(matches!(
                        tuple.state,
                        crabka_pgmvcc::version::TsVersionState::Committed { .. }
                    ));
                    let row = decode_hash_physical_record(range_id, record)
                        .expect("decode authoritative hash row");
                    if (0..16).contains(&row.logical_id) {
                        assert!(buckets.insert(bucket), "bucket duplicated across ranges");
                    }
                    decoded_rows.push(row);
                }
                crabka_pgkv::key::KeyClass::System => {
                    if record.key.starts_with(b"\0\0\0\0meta/ts_txn/") {
                        assert!(record.value.starts_with(b"TXD2"));
                        let raw_start = u64::from_be_bytes(
                            record.key[record.key.len() - 8..]
                                .try_into()
                                .expect("descriptor timestamp"),
                        );
                        let descriptor =
                            crabka_pgexec::timestamp_txn::decode_timestamp_txn_descriptor_value(
                                crabka_pgexec::TimestampTransactionId::new(raw_start)
                                    .expect("descriptor timestamp id"),
                                &record.value,
                            )
                            .expect("decode TXD2 descriptor");
                        if descriptor.operations.len() == 2 {
                            match descriptor.decision {
                                crabka_pgexec::PrimaryTxnDecision::Committed(_) => {
                                    assert!(
                                        cross_boundary_descriptor.replace(descriptor).is_none()
                                    );
                                }
                                crabka_pgexec::PrimaryTxnDecision::Aborted => {
                                    assert!(rolled_back_descriptor.replace(descriptor).is_none());
                                }
                                crabka_pgexec::PrimaryTxnDecision::Pending => {
                                    panic!("terminal inspection retained a pending descriptor")
                                }
                            }
                        }
                    }
                }
                class => {
                    panic!("hash durable inspection returned legacy/unexpected class {class:?}")
                }
            }
        }
    }
    assert_eq!(buckets, (0..16).collect());
    assert_eq!(aborted_rows, BTreeSet::from([(7, 0), (15, 8)]));
    let descriptor = cross_boundary_descriptor.expect("two-operation hash TXD2 descriptor");
    assert_eq!(descriptor.participants, vec![0, 1]);
    assert_eq!(
        descriptor
            .operations
            .iter()
            .map(|operation| operation.bucket.expect("hash descriptor bucket"))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 8])
    );
    assert!(matches!(
        descriptor.decision,
        crabka_pgexec::PrimaryTxnDecision::Committed(_)
    ));
    let rolled_back = rolled_back_descriptor.expect("two-operation aborted hash TXD2 descriptor");
    assert_eq!(rolled_back.participants, vec![0, 1]);
    assert_eq!(
        rolled_back
            .operations
            .iter()
            .map(|operation| operation.bucket.expect("hash descriptor bucket"))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 8])
    );
    assert!(matches!(
        rolled_back.decision,
        crabka_pgexec::PrimaryTxnDecision::Aborted
    ));
    decoded_rows.retain(|row| (0..16).contains(&row.logical_id));
    decoded_rows.sort();
    assert_eq!(
        decoded_rows
            .iter()
            .map(|row| row.logical_id)
            .collect::<BTreeSet<_>>(),
        (0..16).collect()
    );
    // Hidden rowids are minted per range since the single-shard bypass, so
    // they are only unique per (bucket, rowid) coordinate — not globally.
    assert_eq!(
        decoded_rows
            .iter()
            .map(|row| (row.bucket, row.rowid))
            .collect::<BTreeSet<_>>()
            .len(),
        16
    );
    for row in decoded_rows {
        assert_eq!(
            row.bucket,
            crabka_pgkv::key::hash_bucket(&row.logical_id.to_be_bytes(), 16).unwrap()
        );
        assert_eq!(row.key_class, "hash_primary_version");
    }
    system.shutdown().await;
}

fn cli_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/debug/crabka")
}

async fn initiate_split(
    system: &ProcessHarness,
    operation_id: &str,
    workload: SplitWorkload,
    layout: &PreSplitLayout,
) {
    let [left, right] = system.split_successor_endpoints();
    let contract = workload.contract(SplitKillPoint::InitiatedBeforeRunningCas);
    let mut command = tokio::process::Command::new(cli_binary());
    command.args([
        "gres",
        "split",
        "--bootstrap",
        system.bootstrap(),
        system.tenant(),
    ]);
    command.args(contract.split_args);
    let boundary = layout.split_boundary_rowid.to_string();
    if workload == SplitWorkload::Ordinary {
        command.arg(&boundary);
    }
    let output = command
        .args([
            "--operation-id",
            operation_id,
            "--left-range-id",
            "2",
            "--left-endpoint",
            &left,
            "--successor-range-id",
            "3",
            "--successor-endpoint",
            &right,
            "--successor-wal-generation",
            "1",
        ])
        .output()
        .await
        .expect("run Split CLI");
    assert!(
        output.status.success(),
        "Split CLI: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn register_hash_split_layout(system: &ProcessHarness) {
    let mut registry = Registry::connect(system.bootstrap())
        .await
        .expect("hash layout registry");
    registry.ensure_topic().await.expect("registry topic");
    let mut tenant = registry
        .get(system.tenant())
        .await
        .expect("load hash tenant")
        .expect("hash tenant exists");
    let expected_version = tenant.record_version;
    tenant
        .ranges
        .iter_mut()
        .find(|range| range.range_id == 0)
        .expect("initial r0")
        .end_key = Some(RangeBoundary::hash(50, 4, 0));
    tenant.hash_placements = [50_u64, 51, 52]
        .into_iter()
        .map(|table_id| HashPlacement {
            table_id,
            hash_columns: vec!["id".into()],
            bucket_count: 16,
            co_location_group: None,
        })
        .collect();
    tenant.record_version = tenant
        .record_version
        .checked_add(1)
        .expect("hash tenant version");
    tenant.ensure_valid().expect("valid hash tenant layout");
    let tenant = registry
        .replace_if_version(&tenant, Some(expected_version))
        .await
        .expect("publish hash tenant layout");
    registry
        .upsert_tenant_config(&tenant, 1)
        .await
        .expect("publish hash compute config");
}

async fn load_operation(system: &ProcessHarness, operation_id: &str) -> SplitOperationRecord {
    Registry::connect(system.bootstrap())
        .await
        .expect("registry")
        .load_split_operation(system.tenant(), operation_id)
        .await
        .expect("load Split operation")
        .expect("Split operation")
}

async fn load_tenant(system: &ProcessHarness) -> TenantRecord {
    Registry::connect(system.bootstrap())
        .await
        .expect("registry")
        .get(system.tenant())
        .await
        .expect("load tenant")
        .expect("tenant")
}

async fn predecessor_topic_present(admin: &mut dyn RangeRetirementAdmin, topic: &str) -> bool {
    admin
        .metadata(&[topic])
        .await
        .expect("predecessor metadata")
        .topics
        .iter()
        .any(|entry| entry.name == topic && entry.error.is_none())
}

/// The ordinary-mode physical layout captured after seeding and before the
/// live workload starts.
#[derive(Clone, Debug, Default)]
struct PreSplitLayout {
    /// Table-50 seed ids whose minted hidden rowid fell left of the static
    /// `(50, 10)` coordinator boundary and therefore live on r0 for the whole
    /// run.
    r0_table50_ids: BTreeSet<u64>,
    /// Runtime split boundary. It is strictly above every hidden rowid minted
    /// so far, so every seed row stays left of it. Every workload row is minted
    /// later from the monotone timestamp domain and lands right of it.
    split_boundary_rowid: u64,
}

/// Capture where the seeds physically landed and derive the split boundary.
///
/// Hidden rowids are timestamps, so the exact rowid of each seed depends on
/// how many stamps earlier transactions burned. Only the structure is
/// deterministic: each table's seeds are complete, they live exactly once
/// across r0 and r1, table 51 never reaches r0, and rowids grow monotonically
/// with insertion order.
async fn capture_pre_split_layout(system: &ProcessHarness) -> PreSplitLayout {
    use assert2::assert;
    let mut r0_table50_ids = BTreeSet::new();
    let mut max_rowid = 0_u64;
    for table_id in [50_u64, 51] {
        let r0 = direct_ordinary_physical_rows(system, 0, table_id).await;
        let r1 = direct_ordinary_physical_rows(system, 1, table_id).await;
        assert!(
            table_id == 50 || r0.is_empty(),
            "table{table_id} rows on r0 violate the (50,10) coordinator boundary: {r0:?}"
        );
        let mut ids = BTreeSet::new();
        for (range_id, row) in r0
            .iter()
            .map(|row| (0_u32, row))
            .chain(r1.iter().map(|row| (1, row)))
        {
            assert!(
                row.seq == 1_000_000 + row.id
                    && row.checksum == format!("seed-{table_id}-{}", row.id),
                "unexpected pre-workload row {row:?} on r{range_id}"
            );
            assert!(
                ids.insert(row.id),
                "seed id {} of table{table_id} is physically duplicated",
                row.id
            );
            // Only table 50 straddles the (50, 10) coordinator boundary;
            // table 51 sorts entirely after it regardless of minted rowid.
            assert!(
                table_id != 50 || (range_id == 0) == (row.physical_rowid < 10),
                "seed row {row:?} is hosted on r{range_id} against the (50,10) boundary"
            );
            max_rowid = max_rowid.max(row.physical_rowid);
            if range_id == 0 {
                r0_table50_ids.insert(row.id);
            }
        }
        assert!(
            ids == (1..16).collect::<BTreeSet<_>>(),
            "table{table_id} seed ids are incomplete: {ids:?}"
        );
    }
    assert!(max_rowid >= 16, "seed rowids implausibly low: {max_rowid}");
    PreSplitLayout {
        r0_table50_ids,
        split_boundary_rowid: max_rowid + 1,
    }
}

/// Assert that direct scans of the coordinator and predecessor ranges see the
/// live ordinary workload's acknowledged rows.
async fn assert_ordinary_acks_visible(system: &ProcessHarness, ledger_path: &Path) {
    use assert2::assert;
    let acknowledged = parse_closed_payload_ledger(ledger_path)
        .expect("pre-split ordinary payload ledger")
        .acknowledged
        .into_values()
        .map(|event| PhysicalPayloadRow {
            table_id: event.table_id,
            rowid: event.rowid.expect("pre-split ordinary ACK id"),
            seq: event.seq,
            checksum: event.checksum,
        })
        .collect::<BTreeSet<_>>();
    let mut visible = BTreeSet::new();
    for range_id in [0, 1] {
        for table_id in [50, 51] {
            visible.extend(direct_payload_rows(system, range_id, table_id).await);
        }
    }
    let missing = acknowledged.difference(&visible).collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "pre-split ordinary acknowledgements missing from direct scans: {missing:?}"
    );
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct PhysicalPayloadRow {
    table_id: u64,
    rowid: u64,
    seq: u64,
    checksum: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct HashPhysicalPayloadRow {
    range_id: u32,
    table_id: u32,
    logical_id: i32,
    rowid: u64,
    bucket: u32,
    version: u64,
    key_class: &'static str,
    start_ts: u64,
    commit_ts: u64,
    seq: Option<i32>,
    checksum: Option<String>,
    raw_key: Vec<u8>,
    raw_value: Vec<u8>,
    source_offset: i64,
    source_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct HashCorpusEvidence {
    logical_id: i32,
    bytes_hex: String,
    bucket: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct HashAlgorithmEvidence {
    name: &'static str,
    offset_basis: u64,
    prime: u64,
    bucket_count: u32,
    #[serde(skip)]
    corpus: Vec<HashCorpusEvidence>,
}

impl HashAlgorithmEvidence {
    fn pinned() -> Self {
        let corpus = (0..16_i32)
            .map(|logical_id| HashCorpusEvidence {
                logical_id,
                bytes_hex: hex_bytes(&logical_id.to_be_bytes()),
                bucket: crabka_pgkv::key::hash_bucket(&logical_id.to_be_bytes(), 16)
                    .expect("valid pinned hash bucket count"),
            })
            .collect();
        Self {
            name: "fnv1a64-int4-be",
            offset_basis: 0xcbf2_9ce4_8422_2325,
            prime: 0x0100_0000_01b3,
            bucket_count: 16,
            corpus,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct HashBoundaryEvidence {
    table_id: u32,
    bucket: u32,
    rowid: u64,
    request_bucket: u32,
    receipt_bucket: u32,
}

impl HashBoundaryEvidence {
    const fn pinned() -> Self {
        Self {
            table_id: 50,
            bucket: 8,
            rowid: 0,
            request_bucket: 8,
            receipt_bucket: 8,
        }
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("write hex byte");
    }
    output
}

#[derive(Clone, Debug, Serialize)]
struct HashRawSummaryEvidence {
    table_id: u32,
    logical_id: i32,
    rowid: u64,
    bucket: u32,
    version: u64,
    start_ts: u64,
    commit_ts: u64,
    state: &'static str,
    key_class: &'static str,
    seq: Option<i32>,
    checksum: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct HashRawRecordEvidence {
    raw_key_hex: String,
    raw_value_hex: String,
    source_offset: i64,
    source_revision: u64,
    corpus: bool,
    summary: HashRawSummaryEvidence,
}

fn is_hash_corpus_row(row: &HashRawSummaryEvidence) -> bool {
    row.table_id == 50
        && (0..16).contains(&row.logical_id)
        && row.seq == Some(1_000_000 + row.logical_id)
        && row.checksum.as_deref() == Some(format!("seed-50-{}", row.logical_id).as_str())
}

#[derive(Clone, Debug, Serialize)]
struct HashSnapshotEvidence {
    stage: &'static str,
    range_id: u32,
    generation: u64,
    sample_offset: i64,
    records: Vec<HashRawRecordEvidence>,
}

#[derive(Clone, Debug, Serialize)]
struct HashSqlRowEvidence {
    table_id: u64,
    logical_id: i32,
    rowid: u64,
    seq: i32,
    checksum: String,
}

#[derive(Clone, Debug, Serialize)]
struct HashTxnSummaryEvidence {
    start_ts: u64,
    global_xid: u64,
    generation: u64,
    participants: Vec<u32>,
    prepared: Vec<u32>,
    operations: Vec<(u32, u32, u32, u64, bool)>,
    decision: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct HashTransactionEvidence {
    raw_key_hex: String,
    raw_value_hex: String,
    source_offset: i64,
    source_revision: u64,
    summary: HashTxnSummaryEvidence,
}

#[derive(Clone, Debug, Serialize)]
struct HashFoldEvidence {
    left_corpus: usize,
    right_corpus: usize,
    raw_after_sha256: String,
    sql_sha256: String,
    ack_sha256: String,
}

#[derive(Clone, Debug, Serialize)]
struct HashEvidence {
    algorithm: HashAlgorithmEvidence,
    boundary: HashBoundaryEvidence,
    corpus: Vec<HashCorpusEvidence>,
    snapshots: Vec<HashSnapshotEvidence>,
    sql_rows: Vec<HashSqlRowEvidence>,
    transactions: Vec<HashTransactionEvidence>,
    folds: HashFoldEvidence,
}

fn hash_raw_summary(record: &crabka_gres_ranges::DurableRecord) -> Option<HashRawSummaryEvidence> {
    let crabka_pgkv::key::KeyClass::HashPrimaryVersion {
        table_id,
        bucket,
        rowid,
        version,
    } = crabka_pgkv::key::classify_key(&record.key)
    else {
        return None;
    };
    let tuple = crabka_pgmvcc::version::decode_ts_tuple(&record.value).ok()?;
    let (state, commit_ts) = match tuple.state {
        crabka_pgmvcc::version::TsVersionState::Committed { commit_ts } => ("committed", commit_ts),
        crabka_pgmvcc::version::TsVersionState::Aborted => ("aborted", 0),
        _ => return None,
    };
    let crabka_pgtypes::Datum::Int4(logical_id) = tuple.row.first()? else {
        return None;
    };
    let seq = tuple.row.get(1).and_then(|datum| match datum {
        crabka_pgtypes::Datum::Int4(value) => Some(*value),
        _ => None,
    });
    let checksum = tuple.row.get(2).and_then(|datum| match datum {
        crabka_pgtypes::Datum::Text(value) => Some(value.clone()),
        _ => None,
    });
    Some(HashRawSummaryEvidence {
        table_id,
        logical_id: *logical_id,
        rowid,
        bucket,
        version,
        start_ts: tuple.start_ts,
        commit_ts,
        state,
        key_class: "hash_primary_version",
        seq,
        checksum,
    })
}

async fn collect_hash_snapshots(
    system: &ProcessHarness,
    stage: &'static str,
    active_ranges: &[(u32, u64)],
) -> (Vec<HashSnapshotEvidence>, Vec<HashTransactionEvidence>) {
    let mut snapshots = (0..4)
        .map(|range_id| HashSnapshotEvidence {
            stage,
            range_id,
            generation: active_ranges
                .iter()
                .find_map(|(active, generation)| (*active == range_id).then_some(*generation))
                .unwrap_or(u64::from(range_id >= 2)),
            sample_offset: 0,
            records: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut transactions = BTreeMap::new();
    for &(range_id, generation) in active_ranges {
        let snapshot = &mut snapshots[usize::try_from(range_id).expect("range index")];
        for table_id in [50_u32, 51] {
            let start_key = crabka_pgkv::key::table_prefix(table_id);
            let mut end_key = start_key.clone();
            *end_key.last_mut().expect("table prefix") += 1;
            let response = system
                .inspect_durable_records(crabka_gres_ranges::InspectDurableRecordsReq {
                    tenant: system.tenant().to_owned(),
                    range_id: RangeId::new(range_id),
                    generation,
                    table_id,
                    start_key,
                    end_key,
                    max_records: durable_inspect_limits().0,
                    max_bytes: durable_inspect_limits().1,
                    snapshot_offset: None,
                    cursor: None,
                })
                .await;
            assert!(
                response.next_cursor.is_none(),
                "hash evidence must not truncate"
            );
            snapshot.sample_offset = snapshot
                .sample_offset
                .max(response.provenance.sample_offset);
            for record in response.records {
                if let Some(summary) = hash_raw_summary(&record) {
                    snapshot.records.push(HashRawRecordEvidence {
                        raw_key_hex: hex_bytes(&record.key),
                        raw_value_hex: hex_bytes(&record.value),
                        source_offset: record.source_offset.expect("raw source WAL offset"),
                        source_revision: record.source_revision.expect("raw source revision"),
                        corpus: is_hash_corpus_row(&summary),
                        summary,
                    });
                    continue;
                }
                if !record.key.starts_with(b"\0\0\0\0meta/ts_txn/")
                    || !record.value.starts_with(b"TXD2")
                {
                    continue;
                }
                let start_ts = u64::from_be_bytes(
                    record.key[record.key.len() - 8..]
                        .try_into()
                        .expect("TXD2 key timestamp"),
                );
                let descriptor =
                    crabka_pgexec::timestamp_txn::decode_timestamp_txn_descriptor_value(
                        crabka_pgexec::TimestampTransactionId::new(start_ts)
                            .expect("TXD2 start timestamp"),
                        &record.value,
                    )
                    .expect("decode raw TXD2 evidence");
                if descriptor
                    .operations
                    .iter()
                    .any(|operation| operation.bucket.is_none())
                    || descriptor.participants != [0, 1]
                    || descriptor.operations.len() != 2
                    || descriptor
                        .operations
                        .iter()
                        .map(|operation| operation.bucket.expect("hash bucket checked"))
                        .collect::<BTreeSet<_>>()
                        != BTreeSet::from([0, 8])
                {
                    continue;
                }
                let (decision, terminal) = match descriptor.decision {
                    crabka_pgexec::PrimaryTxnDecision::Pending => ("pending", false),
                    crabka_pgexec::PrimaryTxnDecision::Aborted => ("aborted", true),
                    crabka_pgexec::PrimaryTxnDecision::Committed(_) => ("committed", true),
                };
                if !terminal {
                    continue;
                }
                transactions
                    .entry(record.key.clone())
                    .or_insert_with(|| HashTransactionEvidence {
                        raw_key_hex: hex_bytes(&record.key),
                        raw_value_hex: hex_bytes(&record.value),
                        source_offset: record.source_offset.expect("TXD2 source WAL offset"),
                        source_revision: record.source_revision.expect("TXD2 source revision"),
                        summary: HashTxnSummaryEvidence {
                            start_ts,
                            global_xid: descriptor.global_xid,
                            generation: descriptor.generation,
                            participants: descriptor.participants,
                            prepared: descriptor.prepared,
                            operations: descriptor
                                .operations
                                .into_iter()
                                .map(|operation| {
                                    (
                                        operation.range_id,
                                        operation.table_id,
                                        operation.bucket.expect("hash TXD2 bucket"),
                                        operation.rowid,
                                        operation.delete,
                                    )
                                })
                                .collect(),
                            decision,
                        },
                    });
            }
        }
        snapshot.records.sort_by(|left, right| {
            left.raw_key_hex
                .cmp(&right.raw_key_hex)
                .then(left.raw_value_hex.cmp(&right.raw_value_hex))
        });
        snapshot
            .records
            .dedup_by(|left, right| left.raw_key_hex == right.raw_key_hex);
    }
    (snapshots, transactions.into_values().collect())
}

fn decode_hash_physical_record(
    range_id: u32,
    record: &crabka_gres_ranges::DurableRecord,
) -> Result<HashPhysicalPayloadRow, String> {
    let crabka_pgkv::key::KeyClass::HashPrimaryVersion {
        table_id,
        bucket,
        rowid,
        version,
    } = crabka_pgkv::key::classify_key(&record.key)
    else {
        return Err("durable record is not a hash primary version".into());
    };
    let tuple = crabka_pgmvcc::version::decode_ts_tuple(&record.value)
        .map_err(|error| format!("decode timestamp tuple: {error}"))?;
    let crabka_pgmvcc::version::TsVersionState::Committed { commit_ts } = tuple.state else {
        return Err(format!(
            "hash primary version is not committed: {:?}",
            tuple.state
        ));
    };
    let Some(crabka_pgtypes::Datum::Int4(logical_id)) = tuple.row.first() else {
        return Err("hash tuple does not start with an int4 hash value".into());
    };
    let seq = tuple.row.get(1).and_then(|value| match value {
        crabka_pgtypes::Datum::Int4(value) => Some(*value),
        _ => None,
    });
    let checksum = tuple.row.get(2).and_then(|value| match value {
        crabka_pgtypes::Datum::Text(value) => Some(value.clone()),
        _ => None,
    });
    Ok(HashPhysicalPayloadRow {
        range_id,
        table_id,
        logical_id: *logical_id,
        rowid,
        bucket,
        version,
        key_class: "hash_primary_version",
        start_ts: tuple.start_ts,
        commit_ts,
        seq,
        checksum,
        raw_key: record.key.clone(),
        raw_value: record.value.clone(),
        source_offset: record
            .source_offset
            .ok_or_else(|| "durable record has no source offset".to_string())?,
        source_revision: record
            .source_revision
            .ok_or_else(|| "durable record has no source revision".to_string())?,
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DirectScanEvidence {
    range_id: u32,
    table_id: u64,
    rows: Vec<PhysicalPayloadRow>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct MarkerIdentityEvidence {
    transaction_id: u64,
    table_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket: Option<u32>,
    rowid: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct TerminalRangeEvidence {
    range_id: u32,
    end_table_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_bucket: Option<u32>,
    end_rowid: Option<u64>,
    endpoint: String,
    wal_generation: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ReceiptReplayEvidence {
    sequence: usize,
    timestamp_ms: u128,
    operation: String,
    endpoint: String,
    range_id: u32,
    generation: u64,
    operation_id: String,
    request: serde_json::Value,
    response: serde_json::Value,
    request_sha256: String,
    response_sha256: String,
    replay_count: usize,
}

#[derive(Clone, Debug, Serialize)]
struct JournalReceiptExpectation {
    operation: String,
    tenant: String,
    endpoint: String,
    range_id: u32,
    generation: u64,
    operation_id: String,
    request: serde_json::Value,
    request_sha256: String,
    expected_response_kind: String,
}

/// One ordinary-mode row as physically stored: the timestamp-domain hidden
/// rowid, alongside the client-chosen logical columns.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OrdinaryPhysicalRow {
    table_id: u64,
    physical_rowid: u64,
    id: u64,
    seq: u64,
    checksum: String,
}

/// Scan one range directly, bypassing the gateway.
///
/// `routing_table_id` is the suffix the fixture bakes into the relation name to
/// pin its routing slot. The scan RPC addresses the relation by *catalog* id,
/// so this function resolves the two against each other rather than assume they
/// are equal.
async fn direct_ordinary_physical_rows(
    system: &ProcessHarness,
    range_id: u32,
    routing_table_id: u64,
) -> Vec<OrdinaryPhysicalRow> {
    let scan = crabka_gres_ranges::transport::ScanRangeReq {
        range_id: RangeId::new(range_id),
        table_name: format!("live_ledger{routing_table_id}"),
        interval: crabka_gres_ranges::transport::WireRowInterval {
            start: None,
            end: None,
        },
        local_snapshot: crabka_gres_ranges::transport::WireSnapshot {
            xmin: 1,
            xmax: u64::MAX,
            xip: vec![],
        },
        global_snapshot: crabka_gres_ranges::transport::WireSnapshot {
            xmin: 1,
            xmax: u64::MAX,
            xip: vec![],
        },
        own_xid: None,
        read_ts: Some(u64::MAX),
        own_start_ts: None,
        predicate: crabka_gres_ranges::transport::WirePredicatePushdown::FullScan,
        projection: crabka_gres_ranges::transport::WireProjectionPushdown::All,
        partial_aggregate: None,
        top_k: None,
    };
    let response = system
        .operator_control_client()
        .call(
            &system.range_endpoint(range_id),
            &crabka_gres_ranges::RangeRequest::ScanRange(scan),
        )
        .await
        .expect("direct terminal payload scan");
    let crabka_gres_ranges::RangeResponse::ScanRange(response) = response else {
        panic!("unexpected direct payload response {response:?}");
    };
    response
        .rows
        .into_iter()
        .map(|row| {
            let (_, _, values) = crabka_pgmvcc::version::decode_tuple(&row.tuple)
                .expect("decode terminal payload tuple");
            let [
                crabka_pgtypes::Datum::Int4(id),
                crabka_pgtypes::Datum::Int4(seq),
                crabka_pgtypes::Datum::Text(checksum),
            ] = values.as_slice()
            else {
                panic!("unexpected terminal payload tuple {values:?}");
            };
            OrdinaryPhysicalRow {
                table_id: routing_table_id,
                physical_rowid: row.rowid,
                id: u64::try_from(*id).expect("positive payload id"),
                seq: u64::try_from(*seq).expect("positive payload seq"),
                checksum: checksum.clone(),
            }
        })
        .collect()
}

/// Scan one range's ordinary-mode table and project it into the logical row
/// domain the payload ledger records.
///
/// A logical id that appears at two distinct hidden rowids is a
/// duplicate-applied write, because a re-INSERT of the same id mints a fresh
/// timestamp rowid. The function therefore asserts uniqueness before it drops
/// the physical coordinate.
async fn direct_payload_rows(
    system: &ProcessHarness,
    range_id: u32,
    table_id: u64,
) -> Vec<PhysicalPayloadRow> {
    use assert2::assert;
    let rows = direct_ordinary_physical_rows(system, range_id, table_id).await;
    let mut rowids_by_id = BTreeMap::<u64, Vec<u64>>::new();
    for row in &rows {
        rowids_by_id
            .entry(row.id)
            .or_default()
            .push(row.physical_rowid);
    }
    for (id, rowids) in &rowids_by_id {
        assert!(
            rowids.len() == 1,
            "table{table_id} id {id} occupies {} hidden rowids {rowids:?} on r{range_id}: \
             a duplicate-applied write minted a fresh timestamp rowid for the same id",
            rowids.len()
        );
    }
    rows.into_iter()
        .map(|row| PhysicalPayloadRow {
            table_id,
            rowid: row.id,
            seq: row.seq,
            checksum: row.checksum,
        })
        .collect()
}

/// Scan one hash-sharded range directly. [`direct_ordinary_physical_rows`]
/// explains why the routing suffix and the catalog id resolve separately.
async fn direct_hash_payload_rows(
    system: &ProcessHarness,
    range_id: u32,
    routing_table_id: u64,
) -> Vec<PhysicalPayloadRow> {
    let scan = crabka_gres_ranges::transport::ScanRangeReq {
        range_id: RangeId::new(range_id),
        table_name: format!("live_ledger{routing_table_id}"),
        interval: crabka_gres_ranges::transport::WireRowInterval {
            start: None,
            end: None,
        },
        local_snapshot: crabka_gres_ranges::transport::WireSnapshot {
            xmin: 1,
            xmax: u64::MAX,
            xip: vec![],
        },
        global_snapshot: crabka_gres_ranges::transport::WireSnapshot {
            xmin: 1,
            xmax: u64::MAX,
            xip: vec![],
        },
        own_xid: None,
        read_ts: Some(u64::MAX),
        own_start_ts: None,
        predicate: crabka_gres_ranges::transport::WirePredicatePushdown::FullScan,
        projection: crabka_gres_ranges::transport::WireProjectionPushdown::All,
        partial_aggregate: None,
        top_k: None,
    };
    let response = system
        .operator_control_client()
        .call(
            &system.range_endpoint(range_id),
            &crabka_gres_ranges::RangeRequest::ScanRange(scan),
        )
        .await
        .expect("direct terminal hash payload scan");
    let crabka_gres_ranges::RangeResponse::ScanRange(response) = response else {
        panic!("unexpected direct hash payload response {response:?}");
    };
    response
        .rows
        .into_iter()
        .map(|row| {
            let (_, _, values) = crabka_pgmvcc::version::decode_tuple(&row.tuple)
                .expect("decode terminal hash payload tuple");
            let [
                crabka_pgtypes::Datum::Int4(id),
                crabka_pgtypes::Datum::Int4(seq),
                crabka_pgtypes::Datum::Text(checksum),
            ] = values.as_slice()
            else {
                panic!("unexpected terminal hash payload tuple {values:?}");
            };
            PhysicalPayloadRow {
                table_id: routing_table_id,
                rowid: u64::try_from(*id).expect("positive hash payload id"),
                seq: u64::try_from(*seq).expect("positive hash payload seq"),
                checksum: checksum.clone(),
            }
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct PublicationAckEvidence {
    post_publication_r2_ack: bool,
    post_publication_r3_ack: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct TopicPresenceEvidence {
    predecessor_topic_absent: bool,
    sentinel_topic_present: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct VerificationProcessStatus {
    new_source_pid_alive_at_verification: bool,
    old_source_pid_alive: bool,
    workload_process_group_alive: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct ShutdownProcessStatus {
    new_source_pid_alive: bool,
    old_source_process_group_alive: bool,
    new_source_process_group_alive: bool,
}

#[derive(Debug, Serialize)]
struct WorkloadProcessStatus {
    workload_process_reaped: bool,
}

#[derive(Debug, Serialize)]
struct SplitCrashEvidence {
    schema_version: u32,
    evidence_id: String,
    family: &'static str,
    case: &'static str,
    tenant_id: String,
    operation_id: String,
    acknowledged_rows: usize,
    recovered_acknowledgements: usize,
    max_ack_gap_ms: u128,
    max_ack_gap_bound_ms: u128,
    operation_elapsed_ms: u128,
    operation_bound_ms: u128,
    marker_count: usize,
    left_marker_count: usize,
    right_marker_count: usize,
    delete_count: usize,
    old_pid: u32,
    new_pid: u32,
    kill_ms: u128,
    restart_ms: u128,
    publication_ms: u128,
    #[serde(flatten)]
    publication_ack: PublicationAckEvidence,
    #[serde(flatten)]
    topic_presence: TopicPresenceEvidence,
    sentinel_topic: String,
    left_endpoint: String,
    right_endpoint: String,
    coordinator_endpoint: String,
    left_wal_generation: u64,
    right_wal_generation: u64,
    topology_topics: Vec<String>,
    payload_events: Vec<PayloadEvent>,
    reopened_oracle_rows: Vec<PhysicalPayloadRow>,
    direct_physical_rows: Vec<DirectScanEvidence>,
    sql_union_rows: Vec<PhysicalPayloadRow>,
    source_markers: Vec<MarkerIdentityEvidence>,
    left_markers: Vec<MarkerIdentityEvidence>,
    right_markers: Vec<MarkerIdentityEvidence>,
    marker_response_digest: String,
    terminal_operation_evidence: TerminalOperationEvidence,
    completed_phase: String,
    terminal_layout: Vec<TerminalRangeEvidence>,
    /// Ordinary mode: the runtime split boundary in hidden-rowid, that is
    /// timestamp, space. It is zero in hash mode, whose boundary is the pinned
    /// bucket coordinate.
    split_boundary_rowid: u64,
    /// Ordinary mode: `(table_id, id)` seed rows whose minted rowid landed
    /// left of the static `(50, 10)` coordinator boundary. It is empty in hash
    /// mode.
    r0_static_ids: Vec<(u64, u64)>,
    pre_kill_predicate: SplitPredicateState,
    operation_marker_digest: String,
    retirement_marker_digest: String,
    authenticated_receipts: Vec<ReceiptReplayEvidence>,
    journal_receipt_expectations: Vec<JournalReceiptExpectation>,
    delete_attempts: Vec<DeleteAttemptEvidence>,
    unrelated_delete_attempted: bool,
    old_source_pid: u32,
    new_source_pid: u32,
    old_source_process_group: u32,
    new_source_process_group: u32,
    workload_process_group: u32,
    #[serde(flatten)]
    verification_process_status: VerificationProcessStatus,
    #[serde(flatten)]
    shutdown_process_status: ShutdownProcessStatus,
    operation_revision: u64,
    operation_attempts: u32,
    tenant_record_version: u64,
    source_record_version: u64,
    retirement_source_generation: u64,
    retirement_successor_generations: Vec<(u32, u64)>,
    #[serde(flatten)]
    workload_process_status: WorkloadProcessStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash_evidence: Option<HashEvidence>,
}

#[derive(Debug, Serialize)]
struct TerminalOperationEvidence {
    manifest_key: String,
    covered_offset: i64,
    barrier_offset: i64,
    tail_sha256: String,
    marker_digest: String,
}

fn terminal_operation_evidence(
    operation: &crabka_gres_control::SplitOperationRecord,
) -> TerminalOperationEvidence {
    TerminalOperationEvidence {
        manifest_key: operation
            .evidence
            .manifest_key
            .clone()
            .expect("terminal manifest key"),
        covered_offset: operation
            .evidence
            .covered_offset
            .expect("terminal covered offset"),
        barrier_offset: operation
            .evidence
            .barrier_offset
            .expect("terminal barrier offset"),
        tail_sha256: operation
            .evidence
            .tail_sha256
            .clone()
            .expect("terminal tail digest"),
        marker_digest: operation
            .evidence
            .marker_digest
            .clone()
            .expect("terminal marker digest"),
    }
}

struct VerifyCompletedSplitCase<'a> {
    system: &'a ProcessHarness,
    workload_mode: SplitWorkload,
    point: SplitKillPoint,
    layout: &'a PreSplitLayout,
    operation_id: &'a str,
    ledger_path: &'a Path,
    observations: &'a [ControlObservation],
    delete_ledger: &'a DeleteLedger,
    old_pid: u32,
    new_pid: u32,
    old_source_process_group: u32,
    new_source_process_group: u32,
    kill_ms: u128,
    restart_ms: u128,
    publication_ms: u128,
    elapsed_ms: u128,
    workload_process_reaped: bool,
    workload_process_group: u32,
    sentinel_topic: &'a str,
    pre_kill_predicate: SplitPredicateState,
    journal_receipt_expectations: Vec<JournalReceiptExpectation>,
    hash_before_snapshots: Vec<HashSnapshotEvidence>,
    hash_before_transactions: Vec<HashTransactionEvidence>,
}

struct VerifiedTerminalPayload {
    payload_events: Vec<PayloadEvent>,
    ledger: PayloadLedger,
    expected: BTreeSet<PhysicalPayloadRow>,
    direct_physical_rows: Vec<DirectScanEvidence>,
    sql_rows: BTreeSet<PhysicalPayloadRow>,
    hash_sql_logical: Vec<(u64, i32, i32, String)>,
    publication_ack: PublicationAckEvidence,
}

/// Wall-clock milestones of one crash case, in epoch milliseconds.
#[derive(Clone, Copy, Debug)]
struct RunMilestonesMs {
    kill: u128,
    restart: u128,
    publication: u128,
}

async fn verify_terminal_payload(
    system: &ProcessHarness,
    workload_mode: SplitWorkload,
    point: SplitKillPoint,
    layout: &PreSplitLayout,
    ledger_path: &Path,
    milestones: RunMilestonesMs,
) -> VerifiedTerminalPayload {
    let RunMilestonesMs {
        kill: kill_ms,
        restart: restart_ms,
        publication: publication_ms,
    } = milestones;
    let payload_events = std::fs::read_to_string(ledger_path)
        .expect("read reopened payload ledger")
        .lines()
        .map(|line| serde_json::from_str(line).expect("reopen payload event"))
        .collect::<Vec<_>>();
    let ledger = parse_closed_payload_ledger(ledger_path).expect("closed fsynced payload oracle");
    assert!(
        ledger.recovered >= 1,
        "raw-authorized response recovery is absent; ledger={payload_events:?}"
    );
    assert!(
        ledger.max_ack_gap_ms <= point.pause_bound_ms() + WORKLOAD_AMBIGUITY_RESOLUTION_MS,
        "max ACK gap {}ms exceeded {}ms bound at {point:?}",
        ledger.max_ack_gap_ms,
        point.pause_bound_ms() + WORKLOAD_AMBIGUITY_RESOLUTION_MS
    );
    assert!(
        ledger
            .acknowledged
            .values()
            .any(|event| event.timestamp_ms < kill_ms)
    );
    assert!(
        ledger
            .acknowledged
            .values()
            .any(|event| event.timestamp_ms > restart_ms)
    );
    let publication_ack = PublicationAckEvidence {
        post_publication_r2_ack: ledger.acknowledged.values().any(|event| {
            event.provenance == PayloadProvenance::Workload
                && event.timestamp_ms > publication_ms
                && event.table_id == 50
                && event.rowid.is_some_and(|rowid| rowid >= 16)
        }),
        post_publication_r3_ack: ledger.acknowledged.values().any(|event| {
            event.provenance == PayloadProvenance::Workload
                && event.timestamp_ms > publication_ms
                && event.table_id == 51
                && event.rowid.is_some_and(|rowid| rowid >= 16)
        }),
    };
    assert_eq!(
        publication_ack,
        PublicationAckEvidence {
            post_publication_r2_ack: true,
            post_publication_r3_ack: true,
        }
    );

    let expected = ledger
        .acknowledged
        .values()
        .map(|event| PhysicalPayloadRow {
            table_id: event.table_id,
            rowid: event.rowid.expect("ack physical rowid"),
            seq: event.seq,
            checksum: event.checksum.clone(),
        })
        .collect::<BTreeSet<_>>();
    if workload_mode == SplitWorkload::Ordinary {
        for row in &expected {
            assert!(successor_partition(row.table_id, row.rowid, &layout.r0_table50_ids).is_ok());
        }
    }
    let mut direct_physical_rows = Vec::new();
    for (range_id, table_id) in [(0, 50), (0, 51), (2, 50), (2, 51), (3, 50), (3, 51)] {
        let mut rows = match workload_mode {
            SplitWorkload::Ordinary => direct_payload_rows(system, range_id, table_id).await,
            SplitWorkload::Hash => direct_hash_payload_rows(system, range_id, table_id).await,
        };
        rows.sort();
        if workload_mode == SplitWorkload::Ordinary {
            use assert2::assert;
            for row in &rows {
                let owner = successor_partition(table_id, row.rowid, &layout.r0_table50_ids);
                assert!(
                    owner == Ok(range_id),
                    "table{table_id} id {} is hosted on r{range_id} but the sealed \
                     partition places it on {owner:?}",
                    row.rowid
                );
            }
        }
        direct_physical_rows.push(DirectScanEvidence {
            range_id,
            table_id,
            rows,
        });
    }
    let direct_union = direct_physical_rows
        .iter()
        .flat_map(|scan| scan.rows.iter().cloned())
        .collect::<BTreeSet<_>>();
    {
        use assert2::assert;
        assert!(
            direct_union == expected,
            "direct terminal union must exactly equal the durable ACK ledger; {}",
            describe_payload_mismatch(&direct_union, &expected, &ledger)
        );
    }

    let sql = system.sql(0).await;
    let mut sql_rows = BTreeSet::new();
    let mut hash_sql_logical = Vec::new();
    for table_id in [50_u64, 51] {
        for row in sql
            .query(
                &format!("SELECT id, seq, checksum FROM live_ledger{table_id}"),
                &[],
            )
            .await
            .expect("fresh terminal SQL union")
        {
            let id: i32 = row.get(0);
            let seq: i32 = row.get(1);
            let checksum: String = row.get(2);
            if workload_mode == SplitWorkload::Hash {
                hash_sql_logical.push((table_id, id, seq, checksum.clone()));
            }
            sql_rows.insert(PhysicalPayloadRow {
                table_id,
                rowid: u64::try_from(id).expect("positive SQL id"),
                seq: u64::try_from(seq).expect("positive SQL seq"),
                checksum,
            });
        }
    }
    {
        use assert2::assert;
        if sql_rows != expected {
            system
                .preserve_logs(
                    &Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../../target/g8-sql-union-debug-logs"),
                )
                .await;
        }
        assert!(
            sql_rows == expected,
            "terminal SQL union must exactly equal the durable ACK ledger; {}",
            describe_payload_mismatch(&sql_rows, &expected, &ledger)
        );
    }
    VerifiedTerminalPayload {
        payload_events,
        ledger,
        expected,
        direct_physical_rows,
        sql_rows,
        hash_sql_logical,
        publication_ack,
    }
}

struct VerifiedMarkers {
    markers: Vec<crabka_gres_ranges::transport::WireInDoubtMarker>,
    left: Vec<crabka_gres_ranges::transport::WireInDoubtMarker>,
    right: Vec<crabka_gres_ranges::transport::WireInDoubtMarker>,
    marker_digest: String,
}

fn verify_marker_receipts(
    observations: &[ControlObservation],
    workload_mode: SplitWorkload,
    point: SplitKillPoint,
) -> VerifiedMarkers {
    let marker_receipts = observations
        .iter()
        .filter_map(|observation| {
            if let RangeControlResp::Markers {
                markers,
                left_markers,
                right_markers,
                digest,
            } = &observation.response
            {
                Some((markers, left_markers, right_markers, digest))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let (markers, left, right, marker_digest) = marker_receipts
        .first()
        .copied()
        .expect("authenticated marker receipt");
    let left = left.as_ref().expect("explicit left marker partition");
    let right = right.as_ref().expect("explicit right marker partition");
    assert_eq!(markers.len(), 1, "one durable source Pending marker");
    assert!(
        left.is_empty(),
        "table52 marker cannot belong to left successor"
    );
    assert_eq!(right, markers, "table52 marker belongs to right successor");
    match workload_mode {
        SplitWorkload::Ordinary => {
            // The marker's identity lives in the timestamp domain: its hidden
            // rowid is always `start_ts + 1`. Only the pre-CLI injection runs
            // on a fresh oracle whose exact stamps are pinned; the
            // post-restart variants inherit however many stamps the pre-kill
            // run burned, so only the structural relation is asserted there.
            assert_eq!((markers[0].key.table_id, markers[0].key.bucket), (52, None));
            assert_eq!(markers[0].key.rowid, markers[0].transaction_id + 1);
            if point.inject_marker_before_cli() {
                assert_eq!((markers[0].transaction_id, markers[0].key.rowid), (1, 2));
            } else {
                assert!(markers[0].transaction_id > 16);
            }
        }
        SplitWorkload::Hash => {
            // Same timestamp-domain identity as ordinary mode, plus the
            // pinned hash bucket of logical id 1. Post-restart markers
            // inherit however many stamps the pre-kill run burned (including
            // the recovered TSO stride), so only the structural relation is
            // asserted there.
            assert_eq!(
                (markers[0].key.table_id, markers[0].key.bucket),
                (52, Some(2))
            );
            assert_eq!(markers[0].key.rowid, markers[0].transaction_id + 1);
            if point.inject_marker_before_cli() {
                assert_eq!((markers[0].transaction_id, markers[0].key.rowid), (1, 2));
            } else {
                assert!(markers[0].transaction_id > 16);
            }
        }
    }
    assert!(
        marker_receipts
            .iter()
            .all(|receipt| *receipt == marker_receipts[0])
    );
    assert!(
        left.iter()
            .all(|item| right.iter().all(|other| item != other))
    );
    assert!(left.iter().chain(right).eq(markers.iter()));
    VerifiedMarkers {
        markers: markers.clone(),
        left: left.clone(),
        right: right.clone(),
        marker_digest: marker_digest.clone(),
    }
}

struct HashEvidenceInput<'a> {
    workload_mode: SplitWorkload,
    hash_after_snapshots: Vec<HashSnapshotEvidence>,
    hash_sql_logical: Vec<(u64, i32, i32, String)>,
    ledger: &'a PayloadLedger,
    hash_before_snapshots: Vec<HashSnapshotEvidence>,
    hash_before_transactions: Vec<HashTransactionEvidence>,
    hash_after_transactions: Vec<HashTransactionEvidence>,
    tenant: &'a TenantRecord,
}

fn build_hash_evidence(input: HashEvidenceInput<'_>) -> Option<HashEvidence> {
    if input.workload_mode != SplitWorkload::Hash {
        return None;
    }
    let mut latest =
        BTreeMap::<(u32, i32, Option<i32>, Option<String>), HashRawSummaryEvidence>::new();
    for snapshot in &input.hash_after_snapshots {
        for record in &snapshot.records {
            if record.summary.state != "committed" {
                continue;
            }
            let key = (
                record.summary.table_id,
                record.summary.logical_id,
                record.summary.seq,
                record.summary.checksum.clone(),
            );
            if latest
                .get(&key)
                .is_none_or(|previous| previous.version < record.summary.version)
            {
                latest.insert(key, record.summary.clone());
            }
        }
    }
    let mut hash_sql_rows = input
        .hash_sql_logical
        .into_iter()
        .map(|(table_id, logical_id, seq, checksum)| {
            let physical = latest
                .get(&(
                    u32::try_from(table_id).expect("hash SQL table id"),
                    logical_id,
                    Some(seq),
                    Some(checksum.clone()),
                ))
                .expect("SQL-visible hash row has raw physical record");
            HashSqlRowEvidence {
                table_id,
                logical_id,
                rowid: physical.rowid,
                seq,
                checksum,
            }
        })
        .collect::<Vec<_>>();
    hash_sql_rows.sort_by(|left, right| {
        (
            left.table_id,
            left.logical_id,
            left.rowid,
            left.seq,
            &left.checksum,
        )
            .cmp(&(
                right.table_id,
                right.logical_id,
                right.rowid,
                right.seq,
                &right.checksum,
            ))
    });
    let raw_projection = hash_sql_rows
        .iter()
        .map(|row| {
            (
                row.table_id,
                row.logical_id,
                row.rowid,
                row.seq,
                row.checksum.clone(),
            )
        })
        .collect::<Vec<_>>();
    let mut ack_projection = input
        .ledger
        .acknowledged
        .values()
        .map(|event| {
            (
                event.table_id,
                i32::try_from(event.rowid.expect("hash ACK logical id"))
                    .expect("hash logical id fits i32"),
                i32::try_from(event.seq).expect("hash seq fits i32"),
                event.checksum.clone(),
            )
        })
        .collect::<Vec<_>>();
    ack_projection.sort();
    let mut transactions = BTreeMap::new();
    for transaction in input
        .hash_before_transactions
        .into_iter()
        .chain(input.hash_after_transactions)
    {
        transactions.insert(transaction.raw_key_hex.clone(), transaction);
    }
    let algorithm = HashAlgorithmEvidence::pinned();
    let left_corpus = latest
        .values()
        .filter(|row| is_hash_corpus_row(row) && row.bucket < 8)
        .count();
    let right_corpus = latest
        .values()
        .filter(|row| is_hash_corpus_row(row) && row.bucket >= 8)
        .count();
    assert!(
        input.tenant.ranges.iter().any(|range| {
            range.end_key.is_some_and(|boundary| {
                boundary.table_id == 50 && boundary.bucket == Some(8) && boundary.rowid == 0
            })
        }),
        "terminal registry receipt lost the hash bucket-8 boundary"
    );
    let mut snapshots = input.hash_before_snapshots;
    snapshots.extend(input.hash_after_snapshots);
    Some(HashEvidence {
        corpus: algorithm.corpus.clone(),
        algorithm,
        boundary: HashBoundaryEvidence::pinned(),
        snapshots,
        sql_rows: hash_sql_rows,
        transactions: transactions.into_values().collect(),
        folds: HashFoldEvidence {
            left_corpus,
            right_corpus,
            raw_after_sha256: sha256_bytes(
                &serde_json::to_vec(&raw_projection).expect("raw hash fold"),
            ),
            sql_sha256: sha256_bytes(&serde_json::to_vec(&raw_projection).expect("SQL hash fold")),
            ack_sha256: sha256_bytes(&serde_json::to_vec(&ack_projection).expect("ACK hash fold")),
        },
    })
}

struct VerifiedTerminalTopology {
    operation: crabka_gres_control::SplitOperationRecord,
    tenant: TenantRecord,
    hash_after_snapshots: Vec<HashSnapshotEvidence>,
    hash_after_transactions: Vec<HashTransactionEvidence>,
    left_endpoint: String,
    right_endpoint: String,
    coordinator_endpoint: String,
    left_wal_generation: u64,
    right_wal_generation: u64,
    terminal_layout: Vec<TerminalRangeEvidence>,
    retirement_marker_digest: String,
    retirement_source_generation: u64,
    retirement_successor_generations: Vec<(u32, u64)>,
}

async fn verify_terminal_topology(
    system: &ProcessHarness,
    operation_id: &str,
    marker_digest: &str,
    workload_mode: SplitWorkload,
) -> VerifiedTerminalTopology {
    let operation = load_operation(system, operation_id).await;
    assert_eq!(operation.phase, SplitOperationPhase::Completed);
    assert_eq!(
        operation.evidence.marker_digest.as_deref(),
        Some(marker_digest)
    );
    let tenant = load_tenant(system).await;
    let (hash_after_snapshots, hash_after_transactions) = if workload_mode == SplitWorkload::Hash {
        let active_ranges = tenant
            .ranges
            .iter()
            .map(|range| (range.range_id, range.wal_generation))
            .collect::<Vec<_>>();
        collect_hash_snapshots(system, "after", &active_ranges).await
    } else {
        (Vec::new(), Vec::new())
    };
    let plan = operation.plan.as_ref().expect("sealed completed plan");
    assert_eq!(tenant.ranges, plan.target_layout);
    let range = |range_id| {
        tenant
            .ranges
            .iter()
            .find(|range| range.range_id == range_id)
            .unwrap_or_else(|| panic!("r{range_id}"))
    };
    let (r0, r2, r3) = (range(0), range(2), range(3));
    assert_ne!(r2.endpoint, r3.endpoint);
    assert_eq!((r2.wal_generation, r3.wal_generation), (1, 1));
    let retirement = tenant
        .range_retirements
        .iter()
        .find(|retirement| retirement.operation_id == operation_id)
        .expect("retirement sidecar");
    assert_eq!(retirement.phase, RangeRetirementPhase::Parked);
    assert_eq!(retirement.checkpoint.marker_digest, marker_digest);
    VerifiedTerminalTopology {
        terminal_layout: tenant
            .ranges
            .iter()
            .map(|range| TerminalRangeEvidence {
                range_id: range.range_id,
                end_table_id: range.end_key.map(|end| end.table_id),
                end_bucket: range.end_key.and_then(|end| end.bucket),
                end_rowid: range.end_key.map(|end| end.rowid),
                endpoint: range.endpoint.clone(),
                wal_generation: range.wal_generation,
            })
            .collect(),
        left_endpoint: r2.endpoint.clone(),
        right_endpoint: r3.endpoint.clone(),
        coordinator_endpoint: r0.endpoint.clone(),
        left_wal_generation: r2.wal_generation,
        right_wal_generation: r3.wal_generation,
        retirement_marker_digest: retirement.checkpoint.marker_digest.clone(),
        retirement_source_generation: retirement.source_generation,
        retirement_successor_generations: retirement.successor_ranges.clone(),
        operation,
        tenant,
        hash_after_snapshots,
        hash_after_transactions,
    }
}

struct TerminalEnvironmentInput<'a> {
    system: &'a ProcessHarness,
    point: SplitKillPoint,
    operation_id: &'a str,
    sentinel_topic: &'a str,
    delete_ledger: &'a DeleteLedger,
    old_pid: u32,
    new_pid: u32,
    old_source_process_group: u32,
    new_source_process_group: u32,
    workload_process_group: u32,
    workload_process_reaped: bool,
    elapsed_ms: u128,
}

struct VerifiedTerminalEnvironment {
    topic_presence: TopicPresenceEvidence,
    topology_topics: Vec<String>,
    evidence_id: String,
    verification_process_status: VerificationProcessStatus,
}

async fn verify_terminal_environment(
    input: &TerminalEnvironmentInput<'_>,
) -> VerifiedTerminalEnvironment {
    let mut admin =
        crabka_client_admin::AdminClient::connect(&[input.system.bootstrap().to_owned()])
            .await
            .expect("terminal topic admin");
    let topics = admin
        .metadata(&[])
        .await
        .expect("terminal topic metadata")
        .topics
        .into_iter()
        .filter(|topic| topic.error.is_none())
        .map(|topic| topic.name)
        .collect::<BTreeSet<_>>();
    let topic_presence = TopicPresenceEvidence {
        predecessor_topic_absent: !topics
            .contains(&format!("__gres_wal.{}.r1", input.system.tenant())),
        sentinel_topic_present: topics.contains(input.sentinel_topic),
    };
    assert_eq!(
        topic_presence,
        TopicPresenceEvidence {
            predecessor_topic_absent: true,
            sentinel_topic_present: true,
        }
    );
    for topic in [
        format!("__gres_wal.{}.r0", input.system.tenant()),
        format!("__gres_wal.{}.r2.g0000000001", input.system.tenant()),
        format!("__gres_wal.{}.r3.g0000000001", input.system.tenant()),
    ] {
        assert!(topics.contains(&topic), "missing terminal topic {topic}");
    }
    assert_eq!(input.delete_ledger.exact_calls, 1);
    assert!(!input.delete_ledger.unrelated_attempted);
    assert_ne!(input.old_pid, input.new_pid);
    assert!(input.workload_process_reaped);
    assert!(input.elapsed_ms < SplitKillPoint::operation_bound_ms());
    let verification_process_status = VerificationProcessStatus {
        old_source_pid_alive: process_exists(input.old_pid),
        new_source_pid_alive_at_verification: process_exists(input.new_pid),
        workload_process_group_alive: process_group_exists(input.workload_process_group),
    };
    assert_eq!(
        verification_process_status,
        VerificationProcessStatus {
            new_source_pid_alive_at_verification: true,
            old_source_pid_alive: false,
            workload_process_group_alive: false,
        }
    );
    assert!(!process_group_exists(input.old_source_process_group));
    assert!(process_group_exists(input.new_source_process_group));
    VerifiedTerminalEnvironment {
        topology_topics: topics
            .into_iter()
            .filter(|topic| {
                topic.starts_with(&format!("__gres_wal.{}.", input.system.tenant()))
                    || topic == input.sentinel_topic
            })
            .collect(),
        evidence_id: sha256_bytes(
            format!(
                "{}\0{}\0{}\0{}",
                family_name(input.point.family()),
                input.point.name(),
                input.system.tenant(),
                input.operation_id
            )
            .as_bytes(),
        ),
        topic_presence,
        verification_process_status,
    }
}

async fn verify_completed_split_case(input: VerifyCompletedSplitCase<'_>) -> SplitCrashEvidence {
    let VerifyCompletedSplitCase {
        system,
        workload_mode,
        point,
        layout,
        operation_id,
        ledger_path,
        observations,
        delete_ledger,
        old_pid,
        new_pid,
        old_source_process_group,
        new_source_process_group,
        kill_ms,
        restart_ms,
        publication_ms,
        elapsed_ms,
        workload_process_reaped,
        workload_process_group,
        sentinel_topic,
        pre_kill_predicate,
        journal_receipt_expectations,
        hash_before_snapshots,
        hash_before_transactions,
    } = input;
    let VerifiedTerminalPayload {
        payload_events,
        ledger,
        expected,
        direct_physical_rows,
        sql_rows,
        hash_sql_logical,
        publication_ack,
    } = verify_terminal_payload(
        system,
        workload_mode,
        point,
        layout,
        ledger_path,
        RunMilestonesMs {
            kill: kill_ms,
            restart: restart_ms,
            publication: publication_ms,
        },
    )
    .await;
    let VerifiedMarkers {
        markers,
        left,
        right,
        marker_digest,
    } = verify_marker_receipts(observations, workload_mode, point);

    let VerifiedTerminalTopology {
        operation,
        tenant,
        hash_after_snapshots,
        hash_after_transactions,
        left_endpoint,
        right_endpoint,
        coordinator_endpoint,
        left_wal_generation,
        right_wal_generation,
        terminal_layout,
        retirement_marker_digest,
        retirement_source_generation,
        retirement_successor_generations,
    } = verify_terminal_topology(system, operation_id, &marker_digest, workload_mode).await;
    assert_sealed_ordinary_boundary(workload_mode, layout, &terminal_layout);
    let plan = operation.plan.as_ref().expect("sealed completed plan");

    let VerifiedTerminalEnvironment {
        topic_presence,
        topology_topics,
        evidence_id,
        verification_process_status,
    } = verify_terminal_environment(&TerminalEnvironmentInput {
        system,
        point,
        operation_id,
        sentinel_topic,
        delete_ledger,
        old_pid,
        new_pid,
        old_source_process_group,
        new_source_process_group,
        workload_process_group,
        workload_process_reaped,
        elapsed_ms,
    })
    .await;
    let marker_identity =
        |marker: &crabka_gres_ranges::transport::WireInDoubtMarker| MarkerIdentityEvidence {
            transaction_id: marker.transaction_id,
            table_id: marker.key.table_id,
            bucket: marker.key.bucket,
            rowid: marker.key.rowid,
        };
    let source_markers = markers.iter().map(marker_identity).collect::<Vec<_>>();
    let left_markers = left.iter().map(marker_identity).collect::<Vec<_>>();
    let right_markers = right.iter().map(marker_identity).collect::<Vec<_>>();
    let authenticated_receipts = receipt_replay_evidence(observations);
    let hash_evidence = build_hash_evidence(HashEvidenceInput {
        workload_mode,
        hash_after_snapshots,
        hash_sql_logical,
        ledger: &ledger,
        hash_before_snapshots,
        hash_before_transactions,
        hash_after_transactions,
        tenant: &tenant,
    });

    SplitCrashEvidence {
        schema_version: workload_mode.contract(point).schema_version,
        evidence_id,
        family: family_name(point.family()),
        case: point.name(),
        tenant_id: system.tenant().to_owned(),
        operation_id: operation_id.to_owned(),
        acknowledged_rows: expected.len(),
        recovered_acknowledgements: ledger.recovered,
        max_ack_gap_ms: ledger.max_ack_gap_ms,
        max_ack_gap_bound_ms: point.pause_bound_ms() + WORKLOAD_AMBIGUITY_RESOLUTION_MS,
        operation_elapsed_ms: elapsed_ms,
        operation_bound_ms: SplitKillPoint::operation_bound_ms(),
        marker_count: markers.len(),
        left_marker_count: left.len(),
        right_marker_count: right.len(),
        delete_count: delete_ledger.exact_calls,
        old_pid,
        new_pid,
        kill_ms,
        restart_ms,
        publication_ms,
        publication_ack,
        topic_presence,
        sentinel_topic: sentinel_topic.to_owned(),
        left_endpoint,
        right_endpoint,
        coordinator_endpoint,
        left_wal_generation,
        right_wal_generation,
        topology_topics,
        payload_events,
        reopened_oracle_rows: expected.iter().cloned().collect(),
        direct_physical_rows,
        sql_union_rows: sql_rows.iter().cloned().collect(),
        source_markers,
        left_markers,
        right_markers,
        marker_response_digest: marker_digest.clone(),
        terminal_operation_evidence: terminal_operation_evidence(&operation),
        completed_phase: "completed".into(),
        terminal_layout,
        split_boundary_rowid: layout.split_boundary_rowid,
        r0_static_ids: layout.r0_table50_ids.iter().map(|id| (50, *id)).collect(),
        pre_kill_predicate,
        operation_marker_digest: operation
            .evidence
            .marker_digest
            .clone()
            .expect("operation marker digest"),
        retirement_marker_digest,
        authenticated_receipts,
        journal_receipt_expectations,
        delete_attempts: delete_ledger.attempts.clone(),
        unrelated_delete_attempted: delete_ledger.unrelated_attempted,
        old_source_pid: old_pid,
        new_source_pid: new_pid,
        old_source_process_group,
        new_source_process_group,
        workload_process_group,
        verification_process_status,
        shutdown_process_status: ShutdownProcessStatus {
            new_source_pid_alive: true,
            old_source_process_group_alive: process_group_exists(old_source_process_group),
            new_source_process_group_alive: process_group_exists(new_source_process_group),
        },
        operation_revision: operation.revision,
        operation_attempts: operation.attempts,
        tenant_record_version: tenant.record_version,
        source_record_version: plan.source_record_version,
        retirement_source_generation,
        retirement_successor_generations,
        workload_process_status: WorkloadProcessStatus {
            workload_process_reaped,
        },
        hash_evidence,
    }
}

/// Assert that the sealed left-successor boundary equals the runtime split
/// boundary captured before the workload started. This covers ordinary mode
/// only. The hash boundary is pinned, and [`HashBoundaryEvidence`] checks
/// it.
fn assert_sealed_ordinary_boundary(
    workload_mode: SplitWorkload,
    layout: &PreSplitLayout,
    terminal_layout: &[TerminalRangeEvidence],
) {
    use assert2::assert;
    if workload_mode != SplitWorkload::Ordinary {
        return;
    }
    let r2 = terminal_layout
        .iter()
        .find(|range| range.range_id == 2)
        .expect("sealed r2 layout");
    assert!(
        (r2.end_table_id, r2.end_rowid) == (Some(51), Some(layout.split_boundary_rowid)),
        "sealed r2 boundary ({:?}, {:?}) differs from the captured split boundary (51, {})",
        r2.end_table_id,
        r2.end_rowid,
        layout.split_boundary_rowid
    );
}

const fn family_name(family: Family) -> &'static str {
    match family {
        Family::SourceRestore => "source_restore",
        Family::Publication => "publication",
        Family::RetirementResume => "retirement_resume",
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            write!(&mut text, "{byte:02x}").expect("write to string");
            text
        })
}

fn control_operation_name(operation: &RangeControlOperation) -> Option<&'static str> {
    match operation {
        RangeControlOperation::ForceCheckpoint => Some("checkpoint"),
        RangeControlOperation::PauseAtCoveredOffset { .. } => Some("pause"),
        RangeControlOperation::StageFilteredRestore { .. } => Some("stage"),
        RangeControlOperation::InheritMarkers { .. } => Some("markers"),
        RangeControlOperation::SuccessorFencePrologue { .. } => Some("prologue"),
        RangeControlOperation::RetirePredecessor => Some("retire"),
        RangeControlOperation::Status | RangeControlOperation::Resume => None,
    }
}

fn journal_receipt_expectation(record: &SplitOperationRecord) -> Option<JournalReceiptExpectation> {
    let plan = record.plan.as_ref()?;
    let predecessor = plan
        .current_layout
        .iter()
        .find(|range| range.range_id == record.source_range_id())?;
    let (operation, expected_response_kind) = match record.phase {
        SplitOperationPhase::Running => (RangeControlOperation::ForceCheckpoint, "checkpoint"),
        SplitOperationPhase::Checkpointed => (
            RangeControlOperation::PauseAtCoveredOffset {
                manifest_key: record.evidence.manifest_key.clone()?,
                covered_offset: record.evidence.covered_offset?,
            },
            "paused",
        ),
        SplitOperationPhase::Paused => {
            let intent = AuthorizedSplitIntent::from_record(record.clone()).ok()?;
            let journal_revision = record.revision;
            let journal_digest = intent.digest().to_owned();
            if record.evidence.tail_sha256.is_none() {
                (
                    RangeControlOperation::StageFilteredRestore {
                        journal_revision,
                        journal_digest,
                    },
                    "staged",
                )
            } else {
                (
                    RangeControlOperation::InheritMarkers {
                        journal_revision,
                        journal_digest,
                    },
                    "markers",
                )
            }
        }
        SplitOperationPhase::Restored => {
            let intent = AuthorizedSplitIntent::from_record(record.clone()).ok()?;
            (
                RangeControlOperation::SuccessorFencePrologue {
                    journal_revision: record.revision,
                    journal_digest: intent.digest().to_owned(),
                },
                "applied_or_already_applied",
            )
        }
        SplitOperationPhase::Activated | SplitOperationPhase::Retiring => (
            RangeControlOperation::RetirePredecessor,
            "applied_or_already_applied",
        ),
        _ => return None,
    };
    let operation_name = control_operation_name(&operation)?.to_owned();
    let request = RangeControlReq {
        tenant: record.tenant.as_str().to_owned(),
        range_id: RangeId::new(record.source_range_id()),
        generation: record.predecessor_generation(),
        operation_id: record.operation_id.clone(),
        operation,
    };
    let request = serde_json::to_value(request).expect("serialize journal expectation");
    Some(JournalReceiptExpectation {
        operation: operation_name,
        tenant: record.tenant.as_str().to_owned(),
        endpoint: predecessor.endpoint.clone(),
        range_id: record.source_range_id(),
        generation: record.predecessor_generation(),
        operation_id: record.operation_id.clone(),
        request_sha256: sha256_bytes(&serde_json::to_vec(&request).expect("expectation hash")),
        request,
        expected_response_kind: expected_response_kind.into(),
    })
}

fn receipt_replay_evidence(observations: &[ControlObservation]) -> Vec<ReceiptReplayEvidence> {
    let mut counts = BTreeMap::<(String, String, String), usize>::new();
    for observation in observations {
        if control_operation_name(&observation.request.operation).is_none() {
            continue;
        }
        let request = serde_json::to_string(&observation.request).expect("serialize request proof");
        let response =
            serde_json::to_string(&observation.response).expect("serialize response proof");
        *counts
            .entry((observation.endpoint.clone(), request, response))
            .or_default() += 1;
    }
    observations
        .iter()
        .enumerate()
        .filter(|(_, observation)| control_operation_name(&observation.request.operation).is_some())
        .map(|(sequence, observation)| {
            let request_json = serde_json::to_string(&observation.request).expect("request proof");
            let response_json =
                serde_json::to_string(&observation.response).expect("response proof");
            let replay_count = counts[&(
                observation.endpoint.clone(),
                request_json.clone(),
                response_json.clone(),
            )];
            let request: serde_json::Value =
                serde_json::from_str(&request_json).expect("request proof JSON");
            let response: serde_json::Value =
                serde_json::from_str(&response_json).expect("response proof JSON");
            ReceiptReplayEvidence {
                sequence,
                timestamp_ms: observation.timestamp_ms,
                operation: control_operation_name(&observation.request.operation)
                    .expect("filtered receipt operation")
                    .into(),
                endpoint: observation.endpoint.clone(),
                range_id: observation.request.range_id.as_u32(),
                generation: observation.request.generation,
                operation_id: observation.request.operation_id.clone(),
                request_sha256: sha256_bytes(&serde_json::to_vec(&request).expect("request hash")),
                response_sha256: sha256_bytes(
                    &serde_json::to_vec(&response).expect("response hash"),
                ),
                request,
                response,
                replay_count,
            }
        })
        .collect()
}

async fn wait_for_payload_acks(path: &Path, errors_path: &Path, minimum: usize) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let body = std::fs::read_to_string(path).unwrap_or_default();
        let (count, tables) = workload_ack_stats(&body);
        if count >= minimum && tables == BTreeSet::from([50, 51]) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "only {count} payload ACKs; ledger={body:?}; errors={}",
            std::fs::read_to_string(errors_path).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn authorize_ordinary_response_recovery(
    system: &ProcessHarness,
    ledger_path: &Path,
    recovery_path: &Path,
) {
    let expected = PhysicalPayloadRow {
        table_id: 50,
        rowid: 17,
        seq: 2,
        checksum: "split-50-0000000000000002".into(),
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let attempted = std::fs::read_to_string(ledger_path)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<PayloadEvent>(line).ok())
            .any(|event| {
                event.kind == PayloadKind::Attempt
                    && event.provenance == PayloadProvenance::Workload
                    && event.table_id == expected.table_id
                    && event.seq == expected.seq
                    && event.checksum == expected.checksum
            });
        if attempted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ordinary response-loss attempt missing"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    loop {
        let mut matches = 0;
        for range_id in [0, 1] {
            matches += direct_ordinary_physical_rows(system, range_id, expected.table_id)
                .await
                .into_iter()
                .filter(|row| {
                    row.id == expected.rowid
                        && row.seq == expected.seq
                        && row.checksum == expected.checksum
                })
                .count();
        }
        if matches == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "raw recovery found {matches} exact committed ordinary rows"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    std::fs::write(recovery_path, &expected.checksum).expect("authorize raw ordinary recovery");
}

async fn authorize_hash_response_recovery(
    system: &ProcessHarness,
    ledger_path: &Path,
    recovery_path: &Path,
) {
    let expected_checksum = "split-50-0000000000000002";
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let attempted = std::fs::read_to_string(ledger_path)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<PayloadEvent>(line).ok())
            .any(|event| {
                event.kind == PayloadKind::Attempt
                    && event.provenance == PayloadProvenance::Workload
                    && event.table_id == 50
                    && event.seq == 2
                    && event.checksum == expected_checksum
            });
        if attempted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "hash response-loss attempt missing"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let start_key = crabka_pgkv::key::table_prefix(50);
    let mut end_key = start_key.clone();
    *end_key.last_mut().expect("table prefix") += 1;
    let expected_logical_id = 17_i32;
    let expected_bucket = crabka_pgkv::key::hash_bucket(&expected_logical_id.to_be_bytes(), 16)
        .expect("valid hash response-loss bucket");
    loop {
        let mut matches = Vec::new();
        for range_id in [0, 1] {
            let response = system
                .inspect_durable_records(crabka_gres_ranges::InspectDurableRecordsReq {
                    tenant: system.tenant().to_owned(),
                    range_id: RangeId::new(range_id),
                    generation: 0,
                    table_id: 50,
                    start_key: start_key.clone(),
                    end_key: end_key.clone(),
                    max_records: durable_inspect_limits().0,
                    max_bytes: durable_inspect_limits().1,
                    snapshot_offset: None,
                    cursor: None,
                })
                .await;
            assert!(response.next_cursor.is_none());
            for record in response.records {
                let Some(summary) = hash_raw_summary(&record) else {
                    continue;
                };
                if summary.table_id == 50
                    && summary.logical_id == expected_logical_id
                    && summary.bucket == expected_bucket
                    && summary.state == "committed"
                    && summary.seq == Some(2)
                    && summary.checksum.as_deref() == Some(expected_checksum)
                {
                    assert!(record.source_offset.is_some() && record.source_revision.is_some());
                    matches.push((range_id, summary.rowid, summary.version));
                }
            }
        }
        if matches.len() == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "raw recovery found {} exact committed hash rows",
            matches.len()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    std::fs::write(recovery_path, expected_checksum).expect("authorize raw hash recovery");
}

fn payload_ack_count(path: &Path) -> usize {
    workload_ack_stats(&std::fs::read_to_string(path).unwrap_or_default()).0
}

fn workload_ack_stats(body: &str) -> (usize, BTreeSet<u64>) {
    let events = body
        .lines()
        .filter_map(|line| serde_json::from_str::<PayloadEvent>(line).ok())
        .filter(|event| {
            event.provenance == PayloadProvenance::Workload
                && matches!(event.kind, PayloadKind::Ack | PayloadKind::RecoveredAck)
        })
        .collect::<Vec<_>>();
    (
        events.len(),
        events.into_iter().map(|event| event.table_id).collect(),
    )
}

const fn receipt_fault_receipt(point: SplitKillPoint) -> Option<Receipt> {
    match point {
        SplitKillPoint::CheckpointReceiptBeforeJournalCas => Some(Receipt::Checkpoint),
        SplitKillPoint::PauseReceiptBeforeJournalCas => Some(Receipt::Pause),
        SplitKillPoint::StageReceiptBeforeJournalCas => Some(Receipt::Stage),
        SplitKillPoint::MarkerClaimReceiptBeforeJournalCas => Some(Receipt::Marker),
        SplitKillPoint::PrologueReceiptBeforeJournalCas => Some(Receipt::Prologue),
        SplitKillPoint::RetireReceiptBeforeJournalCas => Some(Receipt::Retire),
        _ => None,
    }
}

async fn inject_pending_split_marker(system: &ProcessHarness) -> Result<MarkerSession, String> {
    let (client, driver) = system.sql_with_driver(0).await;
    let result = client
        .simple_query("INSERT INTO split_marker52 VALUES (1, 'g8-pending-marker')")
        .await;
    let error = match result {
        Ok(response) => {
            return Err(format!(
                "marker insert unexpectedly committed: {response:?}"
            ));
        }
        Err(error) => error,
    };
    if error.as_db_error().is_some_and(|error| {
        error
            .message()
            .contains("after timestamp prewrites before durable decision")
    }) {
        Ok(MarkerSession { client, driver })
    } else {
        Err(format!("unexpected timestamp marker fault: {error}"))
    }
}

struct MarkerSession {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerSessionAction {
    Keep,
    DropAfterCrash,
    RollbackAfterPrologue,
}

const fn marker_session_action(
    has_session: bool,
    compute_killed: bool,
    authenticated_prologue: bool,
    phase: SplitOperationPhase,
) -> MarkerSessionAction {
    if !has_session {
        MarkerSessionAction::Keep
    } else if compute_killed {
        MarkerSessionAction::DropAfterCrash
    } else if authenticated_prologue || matches!(phase, SplitOperationPhase::Activated) {
        MarkerSessionAction::RollbackAfterPrologue
    } else {
        MarkerSessionAction::Keep
    }
}

async fn close_marker_session(marker: MarkerSession, rollback: bool) {
    if rollback && let Err(error) = marker.client.simple_query("ROLLBACK").await {
        assert!(
            error.as_db_error().is_some_and(|db| {
                is_expected_marker_rollback_rejection(db.code().code(), db.message())
            }),
            "release marker session publication guard: {error}"
        );
    }
    let MarkerSession { client, mut driver } = marker;
    drop(client);
    if tokio::time::timeout(Duration::from_secs(5), &mut driver)
        .await
        .is_err()
    {
        driver.abort();
        let _ = driver.await;
    }
}

fn is_expected_marker_rollback_rejection(code: &str, message: &str) -> bool {
    code == "0A000" && message.contains("range map changed; reconnect")
}

async fn reconcile_rpc_with_diagnostics(
    system: &ProcessHarness,
    point: SplitKillPoint,
    control: &GresControlHandle,
    mutation: &impl RangeMutationClient,
    operation: &SplitOperationRecord,
) {
    match tokio::time::timeout(
        Duration::from_secs(15),
        reconcile_one_rpc_phase(control, mutation, operation),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            eprintln!(
                "G8_RECONCILE_ERROR timestamp_ms={} phase={:?} error={error}",
                timestamp_ms(),
                operation.phase
            );
        }
        Err(_) => {
            let preserved_logs = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/g8-split-child-logs")
                .join(point.name());
            system.preserve_logs(&preserved_logs).await;
            panic!(
                "Split reconciliation timed out in phase {:?}; logs={}",
                operation.phase,
                preserved_logs.display()
            );
        }
    }
}

struct PreparedSplitSystem {
    system: ProcessHarness,
    marker_session: Option<MarkerSession>,
    operation_id: String,
    sentinel_topic: String,
}

async fn prepare_split_system(
    point: SplitKillPoint,
    workload_mode: SplitWorkload,
) -> PreparedSplitSystem {
    assert!(
        cli_binary().is_file(),
        "build crabka CLI before live matrix"
    );
    let identity = format!(
        "{}-p{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        std::process::id()
    );
    let operation_id = format!("g8-split-crash-{}-{identity}", point.name());
    let point_index = SplitKillPoint::ALL
        .iter()
        .position(|candidate| *candidate == point)
        .expect("kill point index");
    let tenant = format!("tg8sc-{point_index}-{:x}", std::process::id());
    let timestamp_fault = "after_timestamp_prewrite_before_decision";
    let mut system = if point.inject_marker_before_cli() {
        ProcessHarness::start_all_on_zero_with_commit_fault(&tenant, timestamp_fault).await
    } else {
        ProcessHarness::start_all_on_zero(&tenant).await
    };
    let sentinel_topic = format!("g8-sentinel-{identity}");
    let mut sentinel_admin =
        crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
            .await
            .expect("sentinel admin");
    let sentinel_outcomes = sentinel_admin
        .create_topics(
            &[crabka_client_admin::CreateTopicSpec {
                name: sentinel_topic.clone(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            crabka_units::secs(30),
        )
        .await
        .expect("create sentinel topic");
    assert!(
        sentinel_outcomes
            .iter()
            .all(|outcome| outcome.error.is_none()),
        "sentinel topic: {sentinel_outcomes:?}"
    );

    let mut ddl = String::new();
    for table in 1..50 {
        write!(&mut ddl, "CREATE TABLE filler_{table} (id int4);").expect("write DDL to string");
    }
    match workload_mode {
        SplitWorkload::Ordinary => {
            ddl.push_str(
                "CREATE TABLE live_ledger50 (id int4, seq int4, checksum text NOT NULL) SHARDED;",
            );
            ddl.push_str(
                "CREATE TABLE live_ledger51 (id int4, seq int4, checksum text NOT NULL) SHARDED;",
            );
            ddl.push_str("CREATE TABLE split_marker52 (id int4, checksum text NOT NULL) SHARDED;");
        }
        SplitWorkload::Hash => {
            ddl.push_str("CREATE TABLE live_ledger50 (id int4, seq int4, checksum text NOT NULL) SHARDED BY HASH (id) BUCKETS 16;");
            ddl.push_str("CREATE TABLE live_ledger51 (id int4, seq int4, checksum text NOT NULL) SHARDED BY HASH (id) BUCKETS 16;");
            ddl.push_str("CREATE TABLE split_marker52 (id int4, checksum text NOT NULL) SHARDED BY HASH (id) BUCKETS 16;");
        }
    }
    let sql = system.sql(0).await;
    sql.simple_query(&ddl)
        .await
        .expect("create Split workload tables");
    drop(sql);
    if workload_mode == SplitWorkload::Hash {
        register_hash_split_layout(&system).await;
        system.restart_with_hosted_ranges(0, "r0,r1").await;
    }
    let marker_session = if point.inject_marker_before_cli() {
        let marker = inject_pending_split_marker(&system)
            .await
            .expect("inject pre-CLI Pending marker");
        system.clear_commit_fault();
        Some(marker)
    } else {
        None
    };
    let seed_start = i32::from(workload_mode != SplitWorkload::Hash);
    for rowid in seed_start..16_i32 {
        for table in [50, 51] {
            system
                .sql(0)
                .await
                .simple_query(&format!(
                    "INSERT INTO live_ledger{table} VALUES ({rowid}, {}, 'seed-{table}-{rowid}')",
                    1_000_000 + rowid
                ))
                .await
                .expect("seed static physical rowid");
        }
    }
    if workload_mode == SplitWorkload::Hash {
        let sql = system.sql(0).await;
        sql.simple_query(
            "BEGIN; INSERT INTO live_ledger50 VALUES (23, 1000023, 'seed-50-23'); \
             INSERT INTO live_ledger50 VALUES (31, 1000031, 'seed-50-31'); COMMIT;",
        )
        .await
        .expect("commit pinned cross-bucket hash transaction");
        sql.simple_query(
            "BEGIN; INSERT INTO live_ledger50 VALUES (7, -7, 'aborted-50-7'); \
             INSERT INTO live_ledger50 VALUES (15, -15, 'aborted-50-15'); ROLLBACK;",
        )
        .await
        .expect("abort pinned cross-bucket hash transaction");
    }
    PreparedSplitSystem {
        system,
        marker_session,
        operation_id,
        sentinel_topic,
    }
}

struct PreparedSplitWorkload {
    _root: tempfile::TempDir,
    ledger_path: tempfile::TempPath,
    errors_path: PathBuf,
    workload: WorkloadChild,
    process_group: u32,
    layout: PreSplitLayout,
}

async fn prepare_split_workload(
    system: &ProcessHarness,
    workload_mode: SplitWorkload,
) -> PreparedSplitWorkload {
    use std::os::unix::process::CommandExt as _;

    // Capture the seed layout and split boundary before the live workload can
    // mint further timestamp rowids.
    let layout = if workload_mode == SplitWorkload::Ordinary {
        capture_pre_split_layout(system).await
    } else {
        PreSplitLayout::default()
    };
    let root = tempfile::tempdir().expect("Split crash workload root");
    let mut ledger_file = tempfile::NamedTempFile::new_in(root.path()).expect("payload ledger");
    let seed_start = u64::from(workload_mode != SplitWorkload::Hash);
    for rowid in seed_start..16_u64 {
        for table_id in [50, 51] {
            let seq = 1_000_000 + rowid;
            let checksum = format!("seed-{table_id}-{rowid}");
            for event in [
                PayloadEvent {
                    kind: PayloadKind::Attempt,
                    provenance: PayloadProvenance::Seed,
                    table_id,
                    rowid: None,
                    seq,
                    checksum: checksum.clone(),
                    timestamp_ms: timestamp_ms(),
                },
                PayloadEvent {
                    kind: PayloadKind::Ack,
                    provenance: PayloadProvenance::Seed,
                    table_id,
                    rowid: Some(rowid),
                    seq,
                    checksum,
                    timestamp_ms: timestamp_ms(),
                },
            ] {
                append_payload_event(&mut ledger_file, &event);
            }
        }
    }
    if workload_mode == SplitWorkload::Hash {
        for rowid in [23_u64, 31] {
            let seq = 1_000_000 + rowid;
            let checksum = format!("seed-50-{rowid}");
            for event in [
                PayloadEvent {
                    kind: PayloadKind::Attempt,
                    provenance: PayloadProvenance::Seed,
                    table_id: 50,
                    rowid: None,
                    seq,
                    checksum: checksum.clone(),
                    timestamp_ms: timestamp_ms(),
                },
                PayloadEvent {
                    kind: PayloadKind::Ack,
                    provenance: PayloadProvenance::Seed,
                    table_id: 50,
                    rowid: Some(rowid),
                    seq,
                    checksum,
                    timestamp_ms: timestamp_ms(),
                },
            ] {
                append_payload_event(&mut ledger_file, &event);
            }
        }
    }
    let ledger_path = ledger_file.into_temp_path();
    let stop_path = root.path().join("stop");
    let errors_path = root.path().join("errors.log");
    let response_loss = root.path().join("response-loss");
    let raw_recovery = root.path().join("raw-recovery");
    let mut command = tokio::process::Command::new("bash");
    command
        .args(["-c", split_payload_workload_script()])
        .env("CRABKA_G8_WORKLOAD_STOP", &stop_path)
        .env("CRABKA_G8_WORKLOAD_LEDGER", &ledger_path)
        .env("CRABKA_G8_WORKLOAD_ERRORS", &errors_path)
        .env("CRABKA_G8_RESPONSE_LOSS", &response_loss)
        .env("CRABKA_G8_RAW_RECOVERY", &raw_recovery)
        .env(
            "CRABKA_G8_RECOVERY_TIMEOUT",
            if workload_mode == SplitWorkload::Ordinary {
                "1s"
            } else {
                "10s"
            },
        )
        .env("CRABKA_G8_INSERT_TIMEOUT", WORKLOAD_INSERT_TIMEOUT)
        .env("PGCONNECT_TIMEOUT", "3")
        .env(
            "CRABKA_G8_WORKLOAD_SLEEP",
            workload_mode.inter_insert_delay(),
        )
        .env("PGHOST", "127.0.0.1")
        .env("PGPORT", system.stable_sql_port().to_string())
        .env("PGUSER", "alice")
        .env("PGPASSWORD", process::fixture_password())
        .env("PGDATABASE", system.tenant())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let child = command.spawn().expect("spawn Split workload");
    let process_group = child.id().expect("Split workload PID");
    let workload = WorkloadChild::new(child, process_group, stop_path);
    match workload_mode {
        SplitWorkload::Ordinary => {
            authorize_ordinary_response_recovery(system, &ledger_path, &raw_recovery).await;
        }
        SplitWorkload::Hash => {
            authorize_hash_response_recovery(system, &ledger_path, &raw_recovery).await;
        }
    }
    wait_for_payload_acks(&ledger_path, &errors_path, 8).await;
    if workload_mode == SplitWorkload::Ordinary {
        assert_ordinary_acks_visible(system, &ledger_path).await;
    } else {
        let acknowledged = parse_closed_payload_ledger(&ledger_path)
            .expect("pre-split hash payload ledger")
            .acknowledged
            .into_values()
            .map(|event| PhysicalPayloadRow {
                table_id: event.table_id,
                rowid: event.rowid.expect("pre-split hash ACK rowid"),
                seq: event.seq,
                checksum: event.checksum,
            })
            .collect::<BTreeSet<_>>();
        let mut visible = BTreeSet::new();
        for range_id in [0, 1] {
            for table_id in [50, 51] {
                visible.extend(direct_hash_payload_rows(system, range_id, table_id).await);
            }
        }
        {
            use assert2::assert;
            let missing = acknowledged.difference(&visible).collect::<Vec<_>>();
            if !missing.is_empty() {
                let (snapshots, _) =
                    collect_hash_snapshots(system, "debug", &[(0, 0), (1, 0)]).await;
                for snapshot in &snapshots {
                    for record in &snapshot.records {
                        eprintln!(
                            "G8_DEBUG_DURABLE r{} {:?}",
                            snapshot.range_id, record.summary
                        );
                    }
                }
            }
            assert!(
                missing.is_empty(),
                "pre-split hash acknowledgements missing from direct scans: {missing:?}; visible={visible:?}"
            );
        }
        let (snapshots, _) = collect_hash_snapshots(system, "before", &[(0, 0), (1, 0)]).await;
        for table_id in [50, 51] {
            assert_eq!(
                snapshots
                    .iter()
                    .flat_map(|snapshot| &snapshot.records)
                    .filter(|record| record.summary.table_id == table_id)
                    .map(|record| record.summary.logical_id)
                    .filter(|logical_id| (0..16).contains(logical_id))
                    .collect::<BTreeSet<_>>(),
                (0..16).collect(),
                "hash table{table_id} pre-split corpus"
            );
        }
    }
    PreparedSplitWorkload {
        _root: root,
        ledger_path,
        errors_path,
        workload,
        process_group,
        layout,
    }
}

struct RestartSplitSource<'a> {
    system: &'a mut ProcessHarness,
    point: SplitKillPoint,
    workload_mode: SplitWorkload,
    operation: &'a SplitOperationRecord,
    tenant: &'a TenantRecord,
    authenticated_prologue: bool,
    successors_serving: bool,
    ledger_path: &'a Path,
    errors_path: &'a Path,
    process_group: u32,
    timestamp_fault: &'a str,
    marker_session: &'a mut Option<MarkerSession>,
    marker_session_released: &'a mut bool,
    control: &'a mut GresControlHandle,
    mutation: &'a mut RecordingRangeMutationClient<MtlsRangeMutationClient>,
    retirement: &'a mut CountingRetirementAdmin,
    faults: &'a Arc<OneShotControlFaults>,
    observations: &'a Arc<Mutex<Vec<ControlObservation>>>,
}

struct RestartSplitOutcome {
    old_pid: u32,
    new_pid: u32,
    old_source_process_group: u32,
    new_source_process_group: u32,
    kill_ms: u128,
    restart_ms: u128,
    hash_before_snapshots: Vec<HashSnapshotEvidence>,
    hash_before_transactions: Vec<HashTransactionEvidence>,
}

async fn restart_split_source(input: RestartSplitSource<'_>) -> RestartSplitOutcome {
    wait_for_payload_acks(input.ledger_path, input.errors_path, 9).await;
    let (hash_before_snapshots, hash_before_transactions) =
        if input.workload_mode == SplitWorkload::Hash {
            let active_layout = if input.authenticated_prologue || input.successors_serving {
                &input
                    .operation
                    .plan
                    .as_ref()
                    .expect("sealed split operation plan")
                    .target_layout
            } else {
                &input.tenant.ranges
            };
            let active_ranges = active_layout
                .iter()
                .map(|range| (range.range_id, range.wal_generation))
                .collect::<Vec<_>>();
            collect_hash_snapshots(input.system, "before", &active_ranges).await
        } else {
            (Vec::new(), Vec::new())
        };
    let old_pid = input.system.pid(0);
    let old_source_process_group = input.system.process_group(0);
    let kill_ms = timestamp_ms();
    if !input.point.inject_marker_before_cli() {
        signal_process_group(input.process_group, "-STOP");
        input
            .system
            .set_commit_fault_for_next_child(input.timestamp_fault);
    }
    input.system.kill(0).await;
    if marker_session_action(
        input.marker_session.is_some(),
        true,
        false,
        input.operation.phase,
    ) == MarkerSessionAction::DropAfterCrash
    {
        close_marker_session(
            input
                .marker_session
                .take()
                .expect("crashed marker session remains owned by harness"),
            false,
        )
        .await;
        *input.marker_session_released = true;
    }
    input
        .system
        .restart_with_hosted_ranges(0, input.point.restart_hosted_ranges())
        .await;
    let new_pid = input.system.pid(0);
    let new_source_process_group = input.system.process_group(0);
    let restart_ms = timestamp_ms();
    assert_ne!(old_pid, new_pid);
    if !input.point.inject_marker_before_cli() {
        let marker_result = inject_pending_split_marker(input.system).await;
        input.system.clear_commit_fault();
        if marker_result.is_err() {
            signal_process_group(input.process_group, "-CONT");
        }
        *input.marker_session = Some(marker_result.expect("inject post-restart Pending marker"));
        signal_process_group(input.process_group, "-CONT");
    }
    let mut fresh = Registry::connect(input.system.bootstrap())
        .await
        .expect("registry restart");
    fresh.ensure_topic().await.expect("registry topic restart");
    *input.control = Arc::new(BrokerControl {
        registry: Mutex::new(fresh),
        faults: Arc::clone(input.faults),
    });
    *input.mutation = RecordingRangeMutationClient::new(
        MtlsRangeMutationClient::new(input.system.operator_control_client()),
        Arc::clone(input.observations),
    )
    .with_journal_cas_after(receipt_fault_receipt(input.point), Arc::clone(input.faults));
    input.retirement.inner =
        crabka_client_admin::AdminClient::connect(&[input.system.bootstrap().to_owned()])
            .await
            .expect("retirement admin restart");
    RestartSplitOutcome {
        old_pid,
        new_pid,
        old_source_process_group,
        new_source_process_group,
        kill_ms,
        restart_ms,
        hash_before_snapshots,
        hash_before_transactions,
    }
}

struct PreparedSplitControl {
    tenant_name: TenantName,
    faults: Arc<OneShotControlFaults>,
    control: GresControlHandle,
    observations: Arc<Mutex<Vec<ControlObservation>>>,
    mutation: RecordingRangeMutationClient<MtlsRangeMutationClient>,
    predecessor_topic: String,
    delete_ledger: Arc<std::sync::Mutex<DeleteLedger>>,
    retirement: CountingRetirementAdmin,
}

async fn prepare_split_control(
    system: &ProcessHarness,
    point: SplitKillPoint,
) -> PreparedSplitControl {
    let tenant_name = TenantName::try_from(system.tenant()).expect("tenant name");
    let mut registry = Registry::connect(system.bootstrap())
        .await
        .expect("registry");
    registry.ensure_topic().await.expect("registry topic");
    let faults = Arc::new(OneShotControlFaults::default());
    let control: GresControlHandle = Arc::new(BrokerControl {
        registry: Mutex::new(registry),
        faults: Arc::clone(&faults),
    });
    let observations = Arc::new(Mutex::new(Vec::new()));
    let mutation = RecordingRangeMutationClient::new(
        MtlsRangeMutationClient::new(system.operator_control_client()),
        Arc::clone(&observations),
    )
    .with_journal_cas_after(receipt_fault_receipt(point), Arc::clone(&faults));
    let predecessor_topic = format!("__gres_wal.{}.r1", system.tenant());
    let delete_ledger = Arc::new(std::sync::Mutex::new(DeleteLedger::default()));
    let retirement = CountingRetirementAdmin {
        inner: crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
            .await
            .expect("retirement admin"),
        expected_topic: predecessor_topic.clone(),
        ledger: Arc::clone(&delete_ledger),
        fail_after_delete: false,
    };
    PreparedSplitControl {
        tenant_name,
        faults,
        control,
        observations,
        mutation,
        predecessor_topic,
        delete_ledger,
        retirement,
    }
}

struct SplitDriveInput<'a> {
    system: &'a mut ProcessHarness,
    point: SplitKillPoint,
    workload_mode: SplitWorkload,
    operation_id: &'a str,
    ledger_path: &'a Path,
    errors_path: &'a Path,
    process_group: u32,
    timestamp_fault: &'a str,
    marker_session: Option<MarkerSession>,
}

struct SplitDriveOutcome {
    observations: Vec<ControlObservation>,
    delete_snapshot: DeleteLedger,
    old_pid: u32,
    new_pid: u32,
    old_source_process_group: u32,
    new_source_process_group: u32,
    kill_ms: u128,
    restart_ms: u128,
    publication_ms: u128,
    elapsed_ms: u128,
    pre_kill_predicate: SplitPredicateState,
    journal_receipt_expectations: Vec<JournalReceiptExpectation>,
    hash_before_snapshots: Vec<HashSnapshotEvidence>,
    hash_before_transactions: Vec<HashTransactionEvidence>,
}

async fn drive_split_operation(mut input: SplitDriveInput<'_>) -> SplitDriveOutcome {
    let PreparedSplitControl {
        tenant_name,
        faults,
        mut control,
        observations,
        mut mutation,
        predecessor_topic,
        delete_ledger,
        mut retirement,
    } = prepare_split_control(input.system, input.point).await;
    let started = Instant::now();
    let mut killed = false;
    let mut restart = None;
    let mut publication_ms = 0;
    let mut marker_session_released = false;
    let mut parked_observation_yielded = false;
    let mut last_reported_phase = None;
    let mut pre_kill_predicate = None;
    let mut journal_receipt_expectations = BTreeMap::new();
    loop {
        assert!(started.elapsed().as_millis() < SplitKillPoint::operation_bound_ms());
        let operation = load_operation(input.system, input.operation_id).await;
        if let Some(expectation) = journal_receipt_expectation(&operation)
            && let Some(previous) = journal_receipt_expectations
                .insert(expectation.operation.clone(), expectation.clone())
        {
            assert_eq!(
                serde_json::to_value(previous).unwrap(),
                serde_json::to_value(&expectation).unwrap(),
                "durable receipt expectation changed before replay"
            );
        }
        if last_reported_phase.as_ref() != Some(&operation.phase) {
            eprintln!(
                "G8_MILESTONE timestamp_ms={} phase={:?}",
                timestamp_ms(),
                operation.phase
            );
            last_reported_phase = Some(operation.phase);
        }
        let tenant = load_tenant(input.system).await;
        let obs = observations.lock().await.clone();
        let authenticated_prologue = obs.iter().any(|item| {
            matches!(
                item.request.operation,
                RangeControlOperation::SuccessorFencePrologue { .. }
            ) && matches!(
                item.response,
                RangeControlResp::Applied | RangeControlResp::AlreadyApplied
            )
        });
        if marker_session_action(
            input.marker_session.is_some(),
            false,
            authenticated_prologue,
            operation.phase,
        ) == MarkerSessionAction::RollbackAfterPrologue
            && !marker_session_released
        {
            close_marker_session(
                input
                    .marker_session
                    .take()
                    .expect("Pending marker session remains live through prologue"),
                true,
            )
            .await;
            marker_session_released = true;
        }
        let topic_present = predecessor_topic_present(&mut retirement, &predecessor_topic).await;
        let deletes = delete_ledger.lock().expect("delete ledger").exact_calls;
        let successors_serving = successors_are_serving(&mutation, &operation, &obs).await;
        let state = predicate_state(
            &operation,
            &tenant,
            &obs,
            topic_present,
            deletes,
            successors_serving,
        );
        if publication_ms == 0 && state.layout == Layout::Target {
            publication_ms = timestamp_ms();
            let before = payload_ack_count(input.ledger_path);
            wait_for_payload_acks(input.ledger_path, input.errors_path, before + 4).await;
        }
        if !killed && input.point.is_ready(&state) {
            pre_kill_predicate = Some(state);
            restart = Some(
                restart_split_source(RestartSplitSource {
                    system: input.system,
                    point: input.point,
                    workload_mode: input.workload_mode,
                    operation: &operation,
                    tenant: &tenant,
                    authenticated_prologue,
                    successors_serving,
                    ledger_path: input.ledger_path,
                    errors_path: input.errors_path,
                    process_group: input.process_group,
                    timestamp_fault: input.timestamp_fault,
                    marker_session: &mut input.marker_session,
                    marker_session_released: &mut marker_session_released,
                    control: &mut control,
                    mutation: &mut mutation,
                    retirement: &mut retirement,
                    faults: &faults,
                    observations: &observations,
                })
                .await,
            );
            killed = true;
            continue;
        }
        match operation.phase {
            SplitOperationPhase::Activated => {
                verify_target_topology_ready(&mutation, &operation)
                    .await
                    .expect("successor readiness");
                if !killed && input.point == SplitKillPoint::TenantCasBeforeJournalCas {
                    faults.arm_tenant_cas_ack();
                }
                let _ = reconcile_activated_cutover(&control, &operation).await;
            }
            SplitOperationPhase::Retiring => {
                if !killed && input.point == SplitKillPoint::DeleteSuccessBeforeSidecarCas {
                    retirement.fail_after_delete = true;
                }
                let _ = reconcile_one_retiring_range_wal(
                    &control,
                    &mut retirement,
                    &tenant_name,
                    crabka_units::secs(30),
                )
                .await;
                let current = load_operation(input.system, input.operation_id).await;
                let current_tenant = load_tenant(input.system).await;
                let sidecar_parked = current_tenant.range_retirements.iter().any(|retirement| {
                    retirement.operation_id == input.operation_id
                        && retirement.phase == RangeRetirementPhase::Parked
                });
                if should_yield_parked_observation(parked_observation_yielded, sidecar_parked) {
                    parked_observation_yielded = true;
                    continue;
                }
                reconcile_rpc_with_diagnostics(
                    input.system,
                    input.point,
                    &control,
                    &mutation,
                    &current,
                )
                .await;
            }
            SplitOperationPhase::Completed => {
                let restart = restart
                    .as_ref()
                    .expect("selected Split boundary was reached");
                if !payload_ledger_has_ack_after(
                    &parse_closed_payload_ledger(input.ledger_path)
                        .expect("open terminal workload ledger"),
                    restart.restart_ms,
                ) {
                    let before = payload_ack_count(input.ledger_path);
                    wait_for_payload_acks(input.ledger_path, input.errors_path, before + 1).await;
                }
                break;
            }
            _ => {
                reconcile_rpc_with_diagnostics(
                    input.system,
                    input.point,
                    &control,
                    &mutation,
                    &operation,
                )
                .await;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let restart = restart.expect("restart observation");
    SplitDriveOutcome {
        observations: observations.lock().await.clone(),
        delete_snapshot: delete_ledger.lock().expect("delete ledger").clone(),
        old_pid: restart.old_pid,
        new_pid: restart.new_pid,
        old_source_process_group: restart.old_source_process_group,
        new_source_process_group: restart.new_source_process_group,
        kill_ms: restart.kill_ms,
        restart_ms: restart.restart_ms,
        publication_ms,
        elapsed_ms: started.elapsed().as_millis(),
        pre_kill_predicate: pre_kill_predicate.expect("selected pre-kill predicate"),
        journal_receipt_expectations: journal_receipt_expectations.into_values().collect(),
        hash_before_snapshots: restart.hash_before_snapshots,
        hash_before_transactions: restart.hash_before_transactions,
    }
}

async fn successors_are_serving(
    mutation: &RecordingRangeMutationClient<MtlsRangeMutationClient>,
    operation: &SplitOperationRecord,
    observations: &[ControlObservation],
) -> bool {
    let successor_phase = matches!(
        operation.phase,
        SplitOperationPhase::Activated
            | SplitOperationPhase::LayoutPublished
            | SplitOperationPhase::Retiring
            | SplitOperationPhase::Resuming
            | SplitOperationPhase::Completed
    );
    let prologue_phase = operation.phase == SplitOperationPhase::Restored
        && observed_receipt(observations) == Receipt::Prologue;
    (successor_phase || prologue_phase)
        && verify_target_topology_ready(mutation, operation)
            .await
            .is_ok()
}

async fn run_real_split_crash_case(point: SplitKillPoint, workload_mode: SplitWorkload) {
    let timestamp_fault = "after_timestamp_prewrite_before_decision";
    let PreparedSplitSystem {
        mut system,
        marker_session,
        operation_id,
        sentinel_topic,
    } = prepare_split_system(point, workload_mode).await;

    let PreparedSplitWorkload {
        _root,
        ledger_path,
        errors_path,
        mut workload,
        process_group,
        layout,
    } = prepare_split_workload(&system, workload_mode).await;
    initiate_split(&system, &operation_id, workload_mode, &layout).await;

    let SplitDriveOutcome {
        observations,
        delete_snapshot,
        old_pid,
        new_pid,
        old_source_process_group,
        new_source_process_group,
        kill_ms,
        restart_ms,
        publication_ms,
        elapsed_ms,
        pre_kill_predicate,
        journal_receipt_expectations,
        hash_before_snapshots,
        hash_before_transactions,
    } = drive_split_operation(SplitDriveInput {
        system: &mut system,
        point,
        workload_mode,
        operation_id: &operation_id,
        ledger_path: &ledger_path,
        errors_path: &errors_path,
        process_group,
        timestamp_fault,
        marker_session,
    })
    .await;

    workload.shutdown().await;
    let workload_process_reaped = !process_group_exists(process_group);
    let preserved_logs = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/g8-split-child-logs")
        .join(point.name());
    system.preserve_logs(&preserved_logs).await;
    let mut evidence = verify_completed_split_case(VerifyCompletedSplitCase {
        system: &system,
        workload_mode,
        point,
        layout: &layout,
        operation_id: &operation_id,
        ledger_path: &ledger_path,
        observations: &observations,
        delete_ledger: &delete_snapshot,
        old_pid,
        new_pid,
        old_source_process_group,
        new_source_process_group,
        kill_ms,
        restart_ms,
        publication_ms,
        elapsed_ms,
        workload_process_reaped,
        workload_process_group: process_group,
        sentinel_topic: &sentinel_topic,
        pre_kill_predicate,
        journal_receipt_expectations,
        hash_before_snapshots,
        hash_before_transactions,
    })
    .await;
    system.shutdown().await;
    evidence.verification_process_status.old_source_pid_alive = process_exists(old_pid);
    evidence.shutdown_process_status.new_source_pid_alive = process_exists(new_pid);
    evidence
        .shutdown_process_status
        .old_source_process_group_alive = process_group_exists(old_source_process_group);
    evidence
        .shutdown_process_status
        .new_source_process_group_alive = process_group_exists(new_source_process_group);
    evidence
        .verification_process_status
        .workload_process_group_alive = process_group_exists(process_group);
    assert_eq!(
        evidence.verification_process_status,
        VerificationProcessStatus {
            new_source_pid_alive_at_verification: true,
            old_source_pid_alive: false,
            workload_process_group_alive: false,
        }
    );
    assert_eq!(
        evidence.shutdown_process_status,
        ShutdownProcessStatus {
            new_source_pid_alive: false,
            old_source_process_group_alive: false,
            new_source_process_group_alive: false,
        }
    );
    if let Some(path) = std::env::var_os("CRABKA_G8_SPLIT_CRASH_EVIDENCE") {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("evidence directory");
        }
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap())
            .expect("write Split crash evidence");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_split_crash_anywhere() {
    if std::env::var_os("CRABKA_G8_SPLIT_CRASH").is_none() {
        return;
    }
    let point = SplitKillPoint::parse(
        &std::env::var("CRABKA_G8_SPLIT_KILL_POINT").expect("Split kill point"),
    )
    .expect("known Split kill point");
    let workload = SplitWorkload::parse(std::env::var("CRABKA_G8_SPLIT_WORKLOAD").ok().as_deref())
        .expect("known Split workload");
    Box::pin(run_real_split_crash_case(point, workload)).await;
}

use std::{
    collections::{BTreeMap, BTreeSet},
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
    RangeRetirementPhase, Registry, SplitOperationPhase, SplitOperationRecord, TenantName,
    TenantRecord,
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
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[path = "../../gres-ranges/tests/harness/process.rs"]
mod process;
use process::ProcessHarness;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PayloadKind {
    Attempt,
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
    acknowledged: BTreeMap<(u64, u64), PayloadEvent>,
    recovered: usize,
    max_ack_gap_ms: u128,
}

fn parse_payload_ledger(body: &str) -> Result<PayloadLedger, String> {
    let mut attempts = BTreeMap::new();
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
        acknowledged,
        recovered,
        max_ack_gap_ms,
    })
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

fn successor_partition(table_id: u64, rowid: u64) -> Result<u32, String> {
    match (table_id, rowid) {
        (50, 1..10) => Ok(0),
        (50, 10..) => Ok(2),
        (51, 1..16) => Ok(2),
        (51, 16..) => Ok(3),
        _ => Err(format!(
            "physical key ({table_id},{rowid}) is outside the two active post-split streams"
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
  if timeout 1s psql -X -q -v ON_ERROR_STOP=1 \
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
  if [[ "$response_known" == true ]]; then
    kind=ack
  else
    actual=$(timeout 1s psql -X -A -t -q -v ON_ERROR_STOP=1 \
      -c "SELECT checksum FROM $table_name WHERE seq = $seq" \
      2>>"$CRABKA_G8_WORKLOAD_ERRORS" || true)
    [[ "$actual" == "$checksum" ]] || continue
    kind=recovered_ack
  fi
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  printf '{"kind":"%s","provenance":"workload","table_id":%s,"rowid":%s,"seq":%s,"checksum":"%s","timestamp_ms":%s}\n' \
    "$kind" "$table_id" "$rowid" "$seq" "$checksum" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
  sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
  seq=$((seq + 1))
  sleep 0.02
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
        match tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await {
            Ok(status) => assert!(status.expect("wait workload child").success()),
            Err(_) => {
                terminate_process_group(self.process_group);
                tokio::time::timeout(Duration::from_secs(5), self.child.wait())
                    .await
                    .expect("terminated workload timeout")
                    .expect("wait terminated workload");
            }
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
        timeout_ms: i32,
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
        let result = self.inner.delete_topics(names, timeout_ms).await?;
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
        let (split_args, schema_version, physical_key_class) = match self {
            Self::Ordinary => (vec!["51", "16"], 2, "primary_version"),
            Self::Hash => (vec!["50", "0", "--bucket", "8"], 3, "hash_primary_version"),
        };
        SplitWorkloadContract {
            point,
            family: point.family(),
            pause_bound_ms: point.pause_bound_ms(),
            operation_bound_ms: point.operation_bound_ms(),
            restart_hosted_ranges: point.restart_hosted_ranges(),
            split_args,
            schema_version,
            physical_key_class,
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

    const fn operation_bound_ms(self) -> u128 {
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
        assert!(point.operation_bound_ms() > point.pause_bound_ms());
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
            Sidecar::None => Sidecar::Parking,
            Sidecar::Parking => Sidecar::Parked,
            Sidecar::Parked => Sidecar::Parking,
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
    assert_eq!(parsed.attempts.len(), 2);
    assert_eq!(parsed.acknowledged.len(), 2);
    assert_eq!(parsed.recovered, 1);
    assert_eq!(parsed.max_ack_gap_ms, 8);
    assert!(!payload_ledger_has_ack_after(&parsed, 20));
    assert!(payload_ledger_has_ack_after(&parsed, 19));
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
    assert_eq!(successor_partition(50, 9), Ok(0));
    assert_eq!(successor_partition(50, 10), Ok(2));
    assert_eq!(successor_partition(50, 20), Ok(2));
    assert_eq!(successor_partition(51, 16), Ok(3));
    assert_eq!(successor_partition(51, 32), Ok(3));
    assert_eq!(successor_partition(51, 15), Ok(2));
    assert!(successor_partition(52, 1).is_err());
}

#[test]
fn payload_ledger_rejects_duplicate_ack_checksum_and_time_regression() {
    let duplicate = r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":13}"#;
    assert!(parse_payload_ledger(duplicate).is_err());
    let checksum = r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"b","timestamp_ms":12}"#;
    assert!(parse_payload_ledger(checksum).is_err());
    let regression = r#"{"kind":"attempt","provenance":"workload","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"ack","provenance":"workload","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":10}"#;
    assert!(parse_payload_ledger(regression).is_err());
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
    assert!(script.contains("table_id=50"));
    assert!(script.contains("table_id=51"));
    assert!(script.contains("kind=attempt"));
    assert!(script.contains("kind=recovered_ack"));
    assert!(script.matches("sync -d").count() >= 2);
    assert!(script.contains("rowid=$((16 + table_seq))"));
    assert!(script.contains("live_ledger50"));
    assert!(script.contains("live_ledger51"));
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
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].endpoint, "127.0.0.1:9092");
    assert_eq!(recorded[0].request, request);
    assert_eq!(recorded[0].response, response);
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
    assert_eq!(
        SplitKillPoint::MarkerClaimReceiptBeforeJournalCas.pause_bound_ms(),
        25_000
    );
    assert_eq!(
        SplitKillPoint::TenantCasBeforeJournalCas.pause_bound_ms(),
        15_000
    );
    assert_eq!(
        SplitKillPoint::RetiringBeforeDelete.pause_bound_ms(),
        15_000
    );
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
    assert_eq!(
        marker_session_action(true, false, false, &SplitOperationPhase::Paused),
        MarkerSessionAction::Keep,
        "Markers response alone must leave the Pending session live"
    );
    assert_eq!(
        marker_session_action(true, true, false, &SplitOperationPhase::Paused),
        MarkerSessionAction::DropAfterCrash,
        "a pre-Prologue crash closes the dead connection without issuing SQL"
    );
    assert_eq!(
        marker_session_action(true, false, true, &SplitOperationPhase::Restored),
        MarkerSessionAction::RollbackAfterPrologue,
        "authenticated Prologue permits an explicit rollback"
    );
}

#[test]
fn marker_rollback_accepts_only_the_post_publication_stale_session_rejection() {
    assert!(is_expected_marker_rollback_rejection(
        "0A000",
        "range map changed; reconnect before issuing another statement"
    ));
    assert!(!is_expected_marker_rollback_rejection(
        "XX000",
        "range map changed; reconnect before issuing another statement"
    ));
    assert!(!is_expected_marker_rollback_rejection(
        "0A000",
        "unrelated feature rejection"
    ));
}

#[test]
fn ordinary_and_hash_workloads_share_kill_mapping_but_not_physical_contract() {
    for point in SplitKillPoint::ALL {
        let ordinary = SplitWorkload::Ordinary.contract(point);
        let hash = SplitWorkload::Hash.contract(point);
        assert_eq!(ordinary.point, hash.point);
        assert_eq!(ordinary.family, hash.family);
        assert_eq!(ordinary.pause_bound_ms, hash.pause_bound_ms);
        assert_eq!(ordinary.operation_bound_ms, hash.operation_bound_ms);
        assert_eq!(ordinary.restart_hosted_ranges, hash.restart_hosted_ranges);
        assert_eq!(ordinary.split_args, ["51", "16"]);
        assert_eq!(hash.split_args, ["50", "0", "--bucket", "8"]);
        assert_ne!(ordinary.schema_version, hash.schema_version);
        assert_eq!(ordinary.physical_key_class, "primary_version");
        assert_eq!(hash.physical_key_class, "hash_primary_version");
    }
}

#[test]
fn split_workload_mode_is_explicit_and_fail_closed() {
    assert_eq!(SplitWorkload::parse(None), Ok(SplitWorkload::Ordinary));
    assert_eq!(
        SplitWorkload::parse(Some("ordinary")),
        Ok(SplitWorkload::Ordinary)
    );
    assert_eq!(SplitWorkload::parse(Some("hash")), Ok(SplitWorkload::Hash));
    assert!(SplitWorkload::parse(Some("")).is_err());
    assert!(SplitWorkload::parse(Some("HASH")).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_child_hash_durable_inspection_covers_pinned_bucket_corpus() {
    if std::env::var_os("CRABKA_G9_HASH_INSPECT").is_none() {
        return;
    }
    let tenant = format!("tg9hi-{}", std::process::id());
    let system = ProcessHarness::start_all_on_zero(&tenant).await;
    let sql = system.sql(0).await;
    let mut ddl = String::new();
    for table in 1..50 {
        ddl.push_str(&format!("CREATE TABLE filler_{table} (id int4);"));
    }
    ddl.push_str("CREATE TABLE hash_probe50 (id int4 NOT NULL) SHARDED BY HASH (id) BUCKETS 16;");
    sql.simple_query(&ddl).await.expect("create hash probe");
    for id in 0..16 {
        sql.simple_query(&format!("INSERT INTO hash_probe50 VALUES ({id})"))
            .await
            .expect("insert pinned hash probe");
    }
    drop(sql);

    let start_key = crabka_pgkv::key::table_prefix(50);
    let mut end_key = start_key.clone();
    *end_key.last_mut().expect("table prefix") += 1;
    let mut buckets = BTreeSet::new();
    for range_id in [0, 1] {
        let response = system
            .inspect_durable_records(crabka_gres_ranges::InspectDurableRecordsReq {
                tenant: tenant.clone(),
                range_id: RangeId::new(range_id),
                generation: 0,
                table_id: 50,
                start_key: start_key.clone(),
                end_key: end_key.clone(),
                max_records: crabka_gres_ranges::MAX_DURABLE_INSPECT_RECORDS,
                max_bytes: crabka_gres_ranges::MAX_DURABLE_INSPECT_BYTES,
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
                    assert!(buckets.insert(bucket), "bucket duplicated across ranges");
                }
                crabka_pgkv::key::KeyClass::System => {}
                class => {
                    panic!("hash durable inspection returned legacy/unexpected class {class:?}")
                }
            }
        }
    }
    assert_eq!(buckets, (0..16).collect());
    system.shutdown().await;
}

fn cli_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/debug/crabka")
}

async fn initiate_split(system: &ProcessHarness, operation_id: &str, workload: SplitWorkload) {
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

async fn assert_static_ids_match_physical_rows(system: &ProcessHarness, table_id: u64) {
    let scan = crabka_gres_ranges::transport::ScanRangeReq {
        range_id: RangeId::new(1),
        table_name: format!("live_ledger{table_id}"),
        interval: crabka_gres_ranges::transport::WireRowInterval {
            start: Some(16),
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
            &system.range_endpoint(1),
            &crabka_gres_ranges::RangeRequest::ScanRange(scan),
        )
        .await
        .expect("direct static-id scan");
    let crabka_gres_ranges::RangeResponse::ScanRange(response) = response else {
        panic!("unexpected direct static-id response {response:?}");
    };
    let mut workload_rows = 0;
    for row in response.rows {
        let (_, _, values) =
            crabka_pgmvcc::version::decode_tuple(&row.tuple).expect("decode static-id tuple");
        let [
            crabka_pgtypes::Datum::Int4(id),
            crabka_pgtypes::Datum::Int4(seq),
            crabka_pgtypes::Datum::Text(_),
        ] = values.as_slice()
        else {
            panic!("unexpected static-id tuple {values:?}");
        };
        assert_eq!(u64::try_from(*id).expect("positive id"), row.rowid);
        assert!(successor_partition(table_id, row.rowid).is_ok());
        if *seq < 1_000_000 {
            workload_rows += 1;
        }
    }
    assert!(workload_rows > 0, "table{table_id} live stream reached r1");
}

async fn assert_pre_split_seed_rows_on_predecessor(system: &ProcessHarness) {
    let r0_locations = direct_payload_locations(system, 0, 50).await;
    let r1_locations = direct_payload_locations(system, 1, 50).await;
    let r0 = direct_payload_rows(system, 0, 50, Some(10), Some(16))
        .await
        .into_iter()
        .collect::<BTreeSet<_>>();
    let r1 = direct_payload_rows(system, 1, 50, Some(10), Some(16))
        .await
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected = (10..16)
        .map(|rowid| PhysicalPayloadRow {
            table_id: 50,
            rowid,
            seq: 1_000_000 + rowid,
            checksum: format!("seed-50-{rowid}"),
        })
        .collect::<BTreeSet<_>>();
    if r1 != expected {
        system
            .preserve_logs(Path::new("target/g8-checkpoint-child-logs"))
            .await;
    }
    assert_eq!(
        r1, expected,
        "pre-split predecessor seed rows; r0={r0:?}; r0_locations={r0_locations:?}; r1_locations={r1_locations:?}"
    );
}

async fn direct_payload_locations(
    system: &ProcessHarness,
    range_id: u32,
    table_id: u64,
) -> Vec<(u64, i32, i32, String)> {
    let scan = crabka_gres_ranges::transport::ScanRangeReq {
        range_id: RangeId::new(range_id),
        table_name: format!("live_ledger{table_id}"),
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
        .expect("direct payload-location scan");
    let crabka_gres_ranges::RangeResponse::ScanRange(response) = response else {
        panic!("unexpected direct payload-location response {response:?}");
    };
    response
        .rows
        .into_iter()
        .filter_map(|row| {
            let (_, _, values) = crabka_pgmvcc::version::decode_tuple(&row.tuple)
                .expect("decode payload-location tuple");
            let [
                crabka_pgtypes::Datum::Int4(id),
                crabka_pgtypes::Datum::Int4(seq),
                crabka_pgtypes::Datum::Text(checksum),
            ] = values.as_slice()
            else {
                panic!("unexpected payload-location tuple {values:?}");
            };
            (10..16)
                .contains(id)
                .then(|| (row.rowid, *id, *seq, checksum.clone()))
        })
        .collect()
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct PhysicalPayloadRow {
    table_id: u64,
    rowid: u64,
    seq: u64,
    checksum: String,
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
    rowid: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct TerminalRangeEvidence {
    range_id: u32,
    end_table_id: Option<u64>,
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

async fn direct_payload_rows(
    system: &ProcessHarness,
    range_id: u32,
    table_id: u64,
    start: Option<u64>,
    end: Option<u64>,
) -> Vec<PhysicalPayloadRow> {
    let scan = crabka_gres_ranges::transport::ScanRangeReq {
        range_id: RangeId::new(range_id),
        table_name: format!("live_ledger{table_id}"),
        interval: crabka_gres_ranges::transport::WireRowInterval { start, end },
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
            assert_eq!(u64::try_from(*id).expect("positive payload id"), row.rowid);
            PhysicalPayloadRow {
                table_id,
                rowid: row.rowid,
                seq: u64::try_from(*seq).expect("positive payload seq"),
                checksum: checksum.clone(),
            }
        })
        .collect()
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
    post_publication_r2_ack: bool,
    post_publication_r3_ack: bool,
    predecessor_topic_absent: bool,
    sentinel_topic: String,
    sentinel_topic_present: bool,
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
    new_source_pid_alive_at_verification: bool,
    old_source_pid_alive: bool,
    new_source_pid_alive: bool,
    old_source_process_group_alive: bool,
    new_source_process_group_alive: bool,
    workload_process_group_alive: bool,
    operation_revision: u64,
    operation_attempts: u32,
    tenant_record_version: u64,
    source_record_version: u64,
    retirement_source_generation: u64,
    retirement_successor_generations: Vec<(u32, u64)>,
    workload_process_reaped: bool,
}

#[derive(Debug, Serialize)]
struct TerminalOperationEvidence {
    manifest_key: String,
    covered_offset: i64,
    barrier_offset: i64,
    tail_sha256: String,
    marker_digest: String,
}

async fn verify_completed_split_case(
    system: &ProcessHarness,
    point: SplitKillPoint,
    operation_id: &str,
    ledger_path: &Path,
    observations: &[ControlObservation],
    delete_ledger: &DeleteLedger,
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
    sentinel_topic: &str,
    pre_kill_predicate: SplitPredicateState,
    journal_receipt_expectations: Vec<JournalReceiptExpectation>,
) -> SplitCrashEvidence {
    let payload_events = std::fs::read_to_string(ledger_path)
        .expect("read reopened payload ledger")
        .lines()
        .map(|line| serde_json::from_str(line).expect("reopen payload event"))
        .collect::<Vec<_>>();
    let ledger = parse_closed_payload_ledger(ledger_path).expect("closed fsynced payload oracle");
    assert!(ledger.recovered >= 1);
    assert!(
        ledger.max_ack_gap_ms <= point.pause_bound_ms(),
        "max ACK gap {}ms exceeded {}ms bound at {point:?}",
        ledger.max_ack_gap_ms,
        point.pause_bound_ms()
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
    let post_publication_r2_ack = ledger.acknowledged.values().any(|event| {
        event.provenance == PayloadProvenance::Workload
            && event.timestamp_ms > publication_ms
            && event.table_id == 50
            && event.rowid.is_some_and(|rowid| rowid >= 16)
    });
    let post_publication_r3_ack = ledger.acknowledged.values().any(|event| {
        event.provenance == PayloadProvenance::Workload
            && event.timestamp_ms > publication_ms
            && event.table_id == 51
            && event.rowid.is_some_and(|rowid| rowid >= 16)
    });
    assert!(post_publication_r2_ack && post_publication_r3_ack);

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
    for row in &expected {
        assert_eq!(successor_partition(row.table_id, row.rowid).is_ok(), true);
    }
    let mut direct_physical_rows = Vec::new();
    for (range_id, table_id) in [(0, 50), (0, 51), (2, 50), (2, 51), (3, 50), (3, 51)] {
        let mut rows = direct_payload_rows(system, range_id, table_id, None, None).await;
        rows.sort();
        direct_physical_rows.push(DirectScanEvidence {
            range_id,
            table_id,
            rows,
        });
    }
    assert_eq!(
        direct_physical_rows
            .iter()
            .flat_map(|scan| scan.rows.iter().cloned())
            .collect::<BTreeSet<_>>(),
        expected
    );

    let sql = system.sql(0).await;
    let mut sql_rows = BTreeSet::new();
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
            sql_rows.insert(PhysicalPayloadRow {
                table_id,
                rowid: u64::try_from(id).expect("positive SQL id"),
                seq: u64::try_from(seq).expect("positive SQL seq"),
                checksum: row.get(2),
            });
        }
    }
    assert_eq!(sql_rows, expected);

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
    assert_eq!((markers[0].key.table_id, markers[0].key.rowid), (52, 1));
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

    let operation = load_operation(system, operation_id).await;
    assert_eq!(operation.phase, SplitOperationPhase::Completed);
    assert_eq!(
        operation.evidence.marker_digest.as_deref(),
        Some(marker_digest.as_str())
    );
    let tenant = load_tenant(system).await;
    let plan = operation.plan.as_ref().expect("sealed completed plan");
    assert_eq!(tenant.ranges, plan.target_layout);
    let r2 = tenant
        .ranges
        .iter()
        .find(|range| range.range_id == 2)
        .expect("r2");
    let r3 = tenant
        .ranges
        .iter()
        .find(|range| range.range_id == 3)
        .expect("r3");
    let r0 = tenant
        .ranges
        .iter()
        .find(|range| range.range_id == 0)
        .expect("r0");
    assert_ne!(r2.endpoint, r3.endpoint);
    assert_eq!((r2.wal_generation, r3.wal_generation), (1, 1));
    let retirement = tenant
        .range_retirements
        .iter()
        .find(|retirement| retirement.operation_id == operation_id)
        .expect("retirement sidecar");
    assert_eq!(retirement.phase, RangeRetirementPhase::Parked);
    assert_eq!(retirement.checkpoint.marker_digest, *marker_digest);
    let terminal_layout = tenant
        .ranges
        .iter()
        .map(|range| TerminalRangeEvidence {
            range_id: range.range_id,
            end_table_id: range.end_key.map(|end| end.table_id),
            end_rowid: range.end_key.map(|end| end.rowid),
            endpoint: range.endpoint.clone(),
            wal_generation: range.wal_generation,
        })
        .collect::<Vec<_>>();

    let mut admin = crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
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
    let predecessor_topic = format!("__gres_wal.{}.r1", system.tenant());
    let predecessor_topic_absent = !topics.contains(&predecessor_topic);
    let sentinel_topic_present = topics.contains(sentinel_topic);
    assert!(predecessor_topic_absent);
    assert!(sentinel_topic_present);
    assert!(topics.contains(&format!("__gres_wal.{}.r0", system.tenant())));
    assert!(topics.contains(&format!("__gres_wal.{}.r2.g0000000001", system.tenant())));
    assert!(topics.contains(&format!("__gres_wal.{}.r3.g0000000001", system.tenant())));
    assert_eq!(delete_ledger.exact_calls, 1);
    assert!(!delete_ledger.unrelated_attempted);
    assert_ne!(old_pid, new_pid);
    assert!(workload_process_reaped);
    assert!(elapsed_ms < point.operation_bound_ms());
    let topology_topics = topics
        .iter()
        .filter(|topic| {
            topic.starts_with(&format!("__gres_wal.{}.", system.tenant()))
                || *topic == sentinel_topic
        })
        .cloned()
        .collect();
    let marker_identity =
        |marker: &crabka_gres_ranges::transport::WireInDoubtMarker| MarkerIdentityEvidence {
            transaction_id: marker.transaction_id,
            table_id: marker.key.table_id,
            rowid: marker.key.rowid,
        };
    let source_markers = markers.iter().map(marker_identity).collect::<Vec<_>>();
    let left_markers = left.iter().map(marker_identity).collect::<Vec<_>>();
    let right_markers = right.iter().map(marker_identity).collect::<Vec<_>>();
    let authenticated_receipts = receipt_replay_evidence(observations);
    let evidence_id = sha256_bytes(
        format!(
            "{}\0{}\0{}\0{}",
            family_name(point.family()),
            point.name(),
            system.tenant(),
            operation_id
        )
        .as_bytes(),
    );
    let old_source_pid_alive = process_exists(old_pid);
    let new_source_pid_alive_at_verification = process_exists(new_pid);
    let workload_process_group_alive = process_group_exists(workload_process_group);
    assert!(!old_source_pid_alive);
    assert!(new_source_pid_alive_at_verification);
    assert!(!process_group_exists(old_source_process_group));
    assert!(process_group_exists(new_source_process_group));
    assert!(!workload_process_group_alive);

    SplitCrashEvidence {
        schema_version: 2,
        evidence_id,
        family: family_name(point.family()),
        case: point.name(),
        tenant_id: system.tenant().to_owned(),
        operation_id: operation_id.to_owned(),
        acknowledged_rows: expected.len(),
        recovered_acknowledgements: ledger.recovered,
        max_ack_gap_ms: ledger.max_ack_gap_ms,
        max_ack_gap_bound_ms: point.pause_bound_ms(),
        operation_elapsed_ms: elapsed_ms,
        operation_bound_ms: point.operation_bound_ms(),
        marker_count: markers.len(),
        left_marker_count: left.len(),
        right_marker_count: right.len(),
        delete_count: delete_ledger.exact_calls,
        old_pid,
        new_pid,
        kill_ms,
        restart_ms,
        publication_ms,
        post_publication_r2_ack,
        post_publication_r3_ack,
        predecessor_topic_absent,
        sentinel_topic: sentinel_topic.to_owned(),
        sentinel_topic_present,
        left_endpoint: r2.endpoint.clone(),
        right_endpoint: r3.endpoint.clone(),
        coordinator_endpoint: r0.endpoint.clone(),
        left_wal_generation: r2.wal_generation,
        right_wal_generation: r3.wal_generation,
        topology_topics,
        payload_events,
        reopened_oracle_rows: expected.iter().cloned().collect(),
        direct_physical_rows,
        sql_union_rows: sql_rows.iter().cloned().collect(),
        source_markers,
        left_markers,
        right_markers,
        marker_response_digest: marker_digest.clone(),
        terminal_operation_evidence: TerminalOperationEvidence {
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
        },
        completed_phase: "completed".into(),
        terminal_layout,
        pre_kill_predicate,
        operation_marker_digest: operation
            .evidence
            .marker_digest
            .clone()
            .expect("operation marker digest"),
        retirement_marker_digest: retirement.checkpoint.marker_digest.clone(),
        authenticated_receipts,
        journal_receipt_expectations,
        delete_attempts: delete_ledger.attempts.clone(),
        unrelated_delete_attempted: delete_ledger.unrelated_attempted,
        old_source_pid: old_pid,
        new_source_pid: new_pid,
        old_source_process_group,
        new_source_process_group,
        workload_process_group,
        new_source_pid_alive_at_verification,
        old_source_pid_alive,
        new_source_pid_alive: true,
        old_source_process_group_alive: process_group_exists(old_source_process_group),
        new_source_process_group_alive: process_group_exists(new_source_process_group),
        workload_process_group_alive,
        operation_revision: operation.revision,
        operation_attempts: operation.attempts,
        tenant_record_version: tenant.record_version,
        source_record_version: plan.source_record_version,
        retirement_source_generation: retirement.source_generation,
        retirement_successor_generations: retirement.successor_ranges.clone(),
        workload_process_reaped,
    }
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
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
    phase: &SplitOperationPhase,
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
    if rollback {
        if let Err(error) = marker.client.simple_query("ROLLBACK").await {
            assert!(
                error.as_db_error().is_some_and(|db| {
                    is_expected_marker_rollback_rejection(db.code().code(), db.message())
                }),
                "release marker session publication guard: {error}"
            );
        }
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
        Ok(_) => {}
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

async fn run_real_split_crash_case(point: SplitKillPoint, workload_mode: SplitWorkload) {
    use std::os::unix::process::CommandExt as _;

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
    let mut marker_session = None;
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
            30_000,
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
        ddl.push_str(&format!("CREATE TABLE filler_{table} (id int4);"));
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
    if point.inject_marker_before_cli() {
        marker_session = Some(
            inject_pending_split_marker(&system)
                .await
                .expect("inject pre-CLI Pending marker"),
        );
        system.clear_commit_fault();
    }
    for rowid in 1..16_i32 {
        for table in [50, 51] {
            let sql = system.sql(0).await;
            sql.simple_query(&format!(
                "INSERT INTO live_ledger{table} VALUES ({rowid}, {}, 'seed-{table}-{rowid}')",
                1_000_000 + rowid
            ))
            .await
            .expect("seed static physical rowid");
        }
    }

    let root = tempfile::tempdir().expect("Split crash workload root");
    let mut ledger_file = tempfile::NamedTempFile::new_in(root.path()).expect("payload ledger");
    for rowid in 1..16_u64 {
        for table_id in [50, 51] {
            let seq = 1_000_000 + rowid;
            let checksum = format!("seed-{table_id}-{rowid}");
            append_payload_event(
                &mut ledger_file,
                &PayloadEvent {
                    kind: PayloadKind::Attempt,
                    provenance: PayloadProvenance::Seed,
                    table_id,
                    rowid: None,
                    seq,
                    checksum: checksum.clone(),
                    timestamp_ms: timestamp_ms(),
                },
            );
            append_payload_event(
                &mut ledger_file,
                &PayloadEvent {
                    kind: PayloadKind::Ack,
                    provenance: PayloadProvenance::Seed,
                    table_id,
                    rowid: Some(rowid),
                    seq,
                    checksum,
                    timestamp_ms: timestamp_ms(),
                },
            );
        }
    }
    let ledger_path = ledger_file.into_temp_path();
    let stop_path = root.path().join("stop");
    let errors_path = root.path().join("errors.log");
    let response_loss = root.path().join("response-loss");
    let mut command = tokio::process::Command::new("bash");
    command
        .args(["-c", split_payload_workload_script()])
        .env("CRABKA_G8_WORKLOAD_STOP", &stop_path)
        .env("CRABKA_G8_WORKLOAD_LEDGER", &ledger_path)
        .env("CRABKA_G8_WORKLOAD_ERRORS", &errors_path)
        .env("CRABKA_G8_RESPONSE_LOSS", &response_loss)
        .env("PGHOST", "127.0.0.1")
        .env("PGPORT", system.stable_sql_port().to_string())
        .env("PGUSER", "alice")
        .env("PGPASSWORD", "process-secret")
        .env("PGDATABASE", system.tenant())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let child = command.spawn().expect("spawn Split workload");
    let process_group = child.id().expect("Split workload PID");
    let mut workload = WorkloadChild::new(child, process_group, stop_path);
    wait_for_payload_acks(&ledger_path, &errors_path, 8).await;
    assert_static_ids_match_physical_rows(&system, 50).await;
    assert_static_ids_match_physical_rows(&system, 51).await;
    assert_pre_split_seed_rows_on_predecessor(&system).await;
    initiate_split(&system, &operation_id, workload_mode).await;

    let tenant_name = TenantName::try_from(system.tenant()).expect("tenant name");
    let mut registry = Registry::connect(system.bootstrap())
        .await
        .expect("registry");
    registry.ensure_topic(1).await.expect("registry topic");
    let faults = Arc::new(OneShotControlFaults::default());
    let mut control: GresControlHandle = Arc::new(BrokerControl {
        registry: Mutex::new(registry),
        faults: Arc::clone(&faults),
    });
    let observations = Arc::new(Mutex::new(Vec::new()));
    let mut mutation = RecordingRangeMutationClient::new(
        MtlsRangeMutationClient::new(system.operator_control_client()),
        Arc::clone(&observations),
    )
    .with_journal_cas_after(receipt_fault_receipt(point), Arc::clone(&faults));
    let predecessor_topic = format!("__gres_wal.{}.r1", system.tenant());
    let delete_ledger = Arc::new(std::sync::Mutex::new(DeleteLedger::default()));
    let mut retirement = CountingRetirementAdmin {
        inner: crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
            .await
            .expect("retirement admin"),
        expected_topic: predecessor_topic.clone(),
        ledger: Arc::clone(&delete_ledger),
        fail_after_delete: false,
    };
    let started = Instant::now();
    let mut killed = false;
    let mut old_pid = 0;
    let mut new_pid = 0;
    let mut old_source_process_group = 0;
    let mut new_source_process_group = 0;
    let mut kill_ms = 0;
    let mut restart_ms = 0;
    let mut publication_ms = 0;
    let mut marker_session_released = false;
    let mut parked_observation_yielded = false;
    let mut last_reported_phase = None;
    let mut pre_kill_predicate = None;
    let mut journal_receipt_expectations = BTreeMap::new();

    loop {
        assert!(started.elapsed().as_millis() < point.operation_bound_ms());
        let operation = load_operation(&system, &operation_id).await;
        if let Some(expectation) = journal_receipt_expectation(&operation) {
            if let Some(previous) = journal_receipt_expectations
                .insert(expectation.operation.clone(), expectation.clone())
            {
                assert_eq!(
                    serde_json::to_value(previous).unwrap(),
                    serde_json::to_value(&expectation).unwrap(),
                    "durable receipt expectation changed before replay"
                );
            }
        }
        if last_reported_phase.as_ref() != Some(&operation.phase) {
            eprintln!(
                "G8_MILESTONE timestamp_ms={} phase={:?}",
                timestamp_ms(),
                operation.phase
            );
            last_reported_phase = Some(operation.phase.clone());
        }
        let tenant = load_tenant(&system).await;
        let obs = observations.lock().await.clone();
        if marker_session.is_some() && !marker_session_released {
            assert!(
                marker_session.is_some(),
                "Pending marker session remains live"
            );
        }
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
            marker_session.is_some(),
            false,
            authenticated_prologue,
            &operation.phase,
        ) == MarkerSessionAction::RollbackAfterPrologue
            && !marker_session_released
        {
            let marker_client = marker_session
                .take()
                .expect("Pending marker session remains live through prologue");
            close_marker_session(marker_client, true).await;
            marker_session_released = true;
            eprintln!(
                "G8_MILESTONE timestamp_ms={} marker_session_rollback",
                timestamp_ms()
            );
        }
        let topic_present = predecessor_topic_present(&mut retirement, &predecessor_topic).await;
        let deletes = delete_ledger.lock().expect("delete ledger").exact_calls;
        let successors_serving = if matches!(
            operation.phase,
            SplitOperationPhase::Restored
                | SplitOperationPhase::Activated
                | SplitOperationPhase::LayoutPublished
                | SplitOperationPhase::Retiring
                | SplitOperationPhase::Resuming
                | SplitOperationPhase::Completed
        ) && observed_receipt(&obs) == Receipt::Prologue
            || matches!(
                operation.phase,
                SplitOperationPhase::Activated
                    | SplitOperationPhase::LayoutPublished
                    | SplitOperationPhase::Retiring
                    | SplitOperationPhase::Resuming
                    | SplitOperationPhase::Completed
            ) {
            verify_target_topology_ready(&mutation, &operation)
                .await
                .is_ok()
        } else {
            false
        };
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
            let before = payload_ack_count(&ledger_path);
            wait_for_payload_acks(&ledger_path, &errors_path, before + 4).await;
        }
        if !killed && point.is_ready(&state) {
            pre_kill_predicate = Some(state.clone());
            wait_for_payload_acks(&ledger_path, &errors_path, 9).await;
            old_pid = system.pid(0);
            old_source_process_group = system.process_group(0);
            kill_ms = timestamp_ms();
            if !point.inject_marker_before_cli() {
                signal_process_group(process_group, "-STOP");
                system.set_commit_fault_for_next_child(timestamp_fault);
            }
            system.kill(0).await;
            if marker_session_action(marker_session.is_some(), true, false, &operation.phase)
                == MarkerSessionAction::DropAfterCrash
            {
                close_marker_session(
                    marker_session
                        .take()
                        .expect("crashed marker session remains owned by harness"),
                    false,
                )
                .await;
                marker_session_released = true;
            }
            system
                .restart_with_hosted_ranges(0, point.restart_hosted_ranges())
                .await;
            new_pid = system.pid(0);
            new_source_process_group = system.process_group(0);
            restart_ms = timestamp_ms();
            assert_ne!(old_pid, new_pid);
            if !point.inject_marker_before_cli() {
                let marker_result = inject_pending_split_marker(&system).await;
                system.clear_commit_fault();
                if marker_result.is_err() {
                    signal_process_group(process_group, "-CONT");
                }
                marker_session = Some(marker_result.expect("inject post-restart Pending marker"));
                signal_process_group(process_group, "-CONT");
            }
            let mut fresh = Registry::connect(system.bootstrap())
                .await
                .expect("registry restart");
            fresh.ensure_topic(1).await.expect("registry topic restart");
            control = Arc::new(BrokerControl {
                registry: Mutex::new(fresh),
                faults: Arc::clone(&faults),
            });
            mutation = RecordingRangeMutationClient::new(
                MtlsRangeMutationClient::new(system.operator_control_client()),
                Arc::clone(&observations),
            )
            .with_journal_cas_after(receipt_fault_receipt(point), Arc::clone(&faults));
            retirement.inner =
                crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
                    .await
                    .expect("retirement admin restart");
            killed = true;
            continue;
        }

        match operation.phase {
            SplitOperationPhase::Activated => {
                verify_target_topology_ready(&mutation, &operation)
                    .await
                    .expect("successor readiness");
                if !killed && point == SplitKillPoint::TenantCasBeforeJournalCas {
                    faults.arm_tenant_cas_ack();
                }
                let _ = reconcile_activated_cutover(&control, &operation).await;
            }
            SplitOperationPhase::Retiring => {
                if !killed && point == SplitKillPoint::DeleteSuccessBeforeSidecarCas {
                    retirement.fail_after_delete = true;
                }
                let _ =
                    reconcile_one_retiring_range_wal(&control, &mut retirement, &tenant_name).await;
                let current = load_operation(&system, &operation_id).await;
                let current_tenant = load_tenant(&system).await;
                let sidecar_parked = current_tenant.range_retirements.iter().any(|retirement| {
                    retirement.operation_id == operation_id
                        && retirement.phase == RangeRetirementPhase::Parked
                });
                if should_yield_parked_observation(parked_observation_yielded, sidecar_parked) {
                    parked_observation_yielded = true;
                    continue;
                }
                reconcile_rpc_with_diagnostics(&system, point, &control, &mutation, &current).await;
            }
            SplitOperationPhase::Completed => {
                assert!(killed, "selected Split boundary was never reached");
                if !payload_ledger_has_ack_after(
                    &parse_closed_payload_ledger(&ledger_path)
                        .expect("open terminal workload ledger"),
                    restart_ms,
                ) {
                    let before = payload_ack_count(&ledger_path);
                    wait_for_payload_acks(&ledger_path, &errors_path, before + 1).await;
                }
                break;
            }
            _ => {
                reconcile_rpc_with_diagnostics(&system, point, &control, &mutation, &operation)
                    .await;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    workload.shutdown().await;
    let workload_process_reaped = !process_group_exists(process_group);
    let delete_snapshot = delete_ledger.lock().expect("delete ledger").clone();
    let preserved_logs = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/g8-split-child-logs")
        .join(point.name());
    system.preserve_logs(&preserved_logs).await;
    let mut evidence = verify_completed_split_case(
        &system,
        point,
        &operation_id,
        &ledger_path,
        &observations.lock().await,
        &delete_snapshot,
        old_pid,
        new_pid,
        old_source_process_group,
        new_source_process_group,
        kill_ms,
        restart_ms,
        publication_ms,
        started.elapsed().as_millis(),
        workload_process_reaped,
        process_group,
        &sentinel_topic,
        pre_kill_predicate.expect("selected pre-kill predicate"),
        journal_receipt_expectations.into_values().collect(),
    )
    .await;
    system.shutdown().await;
    evidence.old_source_pid_alive = process_exists(old_pid);
    evidence.new_source_pid_alive = process_exists(new_pid);
    evidence.old_source_process_group_alive = process_group_exists(old_source_process_group);
    evidence.new_source_process_group_alive = process_group_exists(new_source_process_group);
    evidence.workload_process_group_alive = process_group_exists(process_group);
    assert!(!evidence.old_source_pid_alive);
    assert!(!evidence.new_source_pid_alive);
    assert!(!evidence.old_source_process_group_alive);
    assert!(!evidence.new_source_process_group_alive);
    assert!(!evidence.workload_process_group_alive);
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
    run_real_split_crash_case(point, workload).await;
}

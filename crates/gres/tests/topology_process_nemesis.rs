#![cfg(unix)]

#[path = "../../gres-ranges/tests/harness/process.rs"]
mod process;

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    io::Write as _,
    os::unix::process::CommandExt as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use crabka_gres_control::{
    RangeRetirementPhase, Registry, SplitOperationPhase, SplitOperationRecord, TenantName,
    TenantRecord,
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
    attempts: BTreeMap<i64, usize>,
    retries: BTreeMap<i64, usize>,
    recovered: usize,
    max_ack_gap_ms: u128,
    max_ack_gap_endpoints: Option<(i64, u128, i64, u128)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKillPoint {
    Running,
    Checkpointed,
    PausedBeforeStage,
    PausedAfterStage,
    Restored,
    ActivatedBeforeCutover,
    ActivatedAfterTenantCas,
    LayoutPublished,
    RetiringBeforeDelete,
    RetiringAfterDelete,
    RetiringParked,
    Resuming,
}

#[derive(Debug, Clone, Copy)]
enum PredicateValue {
    Met,
    Unmet,
}

impl PredicateValue {
    const fn is_met(self) -> bool {
        matches!(self, Self::Met)
    }

    const fn toggled(self) -> Self {
        match self {
            Self::Met => Self::Unmet,
            Self::Unmet => Self::Met,
        }
    }
}

impl From<bool> for PredicateValue {
    fn from(value: bool) -> Self {
        if value { Self::Met } else { Self::Unmet }
    }
}

#[derive(Debug, Clone, Copy)]
struct RetirementPredicateState {
    journal_phase: SplitOperationPhase,
    target_layout: PredicateValue,
    target_record_version: PredicateValue,
    retirement_phase: Option<RangeRetirementPhase>,
    predecessor_topic_present: PredicateValue,
    retire_receipt_durable: PredicateValue,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
struct RetirementDeleteLedger {
    exact_delete_calls: usize,
    requested_topics: Vec<String>,
    unrelated_delete_attempted: bool,
    injected_after_delete_errors: usize,
}

#[derive(Debug, Clone)]
struct MarkerObservation {
    endpoint: String,
    request: crabka_gres_ranges::RangeControlReq,
    markers: Vec<crabka_gres_ranges::WireInDoubtMarker>,
    left_markers: Vec<crabka_gres_ranges::WireInDoubtMarker>,
    right_markers: Vec<crabka_gres_ranges::WireInDoubtMarker>,
    digest: String,
}

struct RecordingRangeMutationClient {
    inner: MtlsRangeMutationClient,
    marker_observations: Arc<Mutex<Vec<MarkerObservation>>>,
}

#[async_trait]
impl RangeMutationClient for RecordingRangeMutationClient {
    async fn mutate(
        &self,
        endpoint: &str,
        request: crabka_gres_ranges::RangeControlReq,
    ) -> Result<crabka_gres_ranges::RangeControlResp, SplitReconcileError> {
        let records_markers = matches!(
            &request.operation,
            crabka_gres_ranges::RangeControlOperation::InheritMarkers { .. }
        );
        let response = self.inner.mutate(endpoint, request.clone()).await?;
        if records_markers
            && let crabka_gres_ranges::RangeControlResp::Markers {
                markers,
                left_markers,
                right_markers,
                digest,
            } = &response
        {
            self.marker_observations
                .lock()
                .await
                .push(MarkerObservation {
                    endpoint: endpoint.to_owned(),
                    request,
                    markers: markers.clone(),
                    left_markers: left_markers
                        .clone()
                        .expect("new marker response has left partition"),
                    right_markers: right_markers
                        .clone()
                        .expect("new marker response has right partition"),
                    digest: digest.clone(),
                });
        }
        Ok(response)
    }
}

struct CountingRetirementAdmin {
    inner: crabka_client_admin::AdminClient,
    expected_topic: String,
    ledger: Arc<std::sync::Mutex<RetirementDeleteLedger>>,
    error_after_delete: bool,
}

impl CountingRetirementAdmin {
    fn new(
        inner: crabka_client_admin::AdminClient,
        expected_topic: String,
        ledger: Arc<std::sync::Mutex<RetirementDeleteLedger>>,
    ) -> Self {
        Self {
            inner,
            expected_topic,
            ledger,
            error_after_delete: false,
        }
    }

    fn arm_error_after_delete(&mut self) {
        self.error_after_delete = true;
    }
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
        self.ledger
            .lock()
            .expect("retirement delete ledger")
            .record_delete_request(&self.expected_topic, names)
            .map_err(crabka_client_admin::AdminError::Protocol)?;
        let outcomes = self.inner.delete_topics(names, timeout_ms).await?;
        if self.error_after_delete && outcomes.iter().all(|outcome| outcome.error.is_none()) {
            self.error_after_delete = false;
            self.ledger
                .lock()
                .expect("retirement delete ledger")
                .injected_after_delete_errors += 1;
            return Err(crabka_client_admin::AdminError::Protocol(
                "injected ambiguity after exact predecessor delete".into(),
            ));
        }
        Ok(outcomes)
    }
}

impl RetirementDeleteLedger {
    fn record_delete_request(
        &mut self,
        expected_topic: &str,
        names: &[&str],
    ) -> Result<(), String> {
        if names != [expected_topic] {
            self.unrelated_delete_attempted = true;
            return Err(format!(
                "retirement attempted unrelated topic deletion: {names:?}"
            ));
        }
        self.exact_delete_calls += 1;
        self.requested_topics.push(expected_topic.to_owned());
        Ok(())
    }
}

impl SourceKillPoint {
    fn from_env() -> Self {
        match std::env::var("CRABKA_G8_RETIREMENT_KILL_POINT")
            .or_else(|_| std::env::var("CRABKA_G8_CUTOVER_KILL_POINT"))
            .or_else(|_| std::env::var("CRABKA_G8_SOURCE_KILL_POINT"))
            .as_deref()
            .unwrap_or("paused_after_stage")
        {
            "running" => Self::Running,
            "checkpointed" => Self::Checkpointed,
            "paused_before_stage" => Self::PausedBeforeStage,
            "paused_after_stage" => Self::PausedAfterStage,
            "restored" => Self::Restored,
            "activated_before_cutover" => Self::ActivatedBeforeCutover,
            "activated_after_tenant_cas" => Self::ActivatedAfterTenantCas,
            "layout_published" => Self::LayoutPublished,
            "retiring_before_delete" => Self::RetiringBeforeDelete,
            "retiring_after_delete" => Self::RetiringAfterDelete,
            "retiring_parked" => Self::RetiringParked,
            "resuming" => Self::Resuming,
            other => panic!("unknown source kill point {other}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Checkpointed => "checkpointed",
            Self::PausedBeforeStage => "paused_before_stage",
            Self::PausedAfterStage => "paused_after_stage",
            Self::Restored => "restored",
            Self::ActivatedBeforeCutover => "activated_before_cutover",
            Self::ActivatedAfterTenantCas => "activated_after_tenant_cas",
            Self::LayoutPublished => "layout_published",
            Self::RetiringBeforeDelete => "retiring_before_delete",
            Self::RetiringAfterDelete => "retiring_after_delete",
            Self::RetiringParked => "retiring_parked",
            Self::Resuming => "resuming",
        }
    }

    fn retirement_is_ready(self, state: RetirementPredicateState) -> bool {
        if !state.target_layout.is_met() || !state.target_record_version.is_met() {
            return false;
        }
        match self {
            Self::RetiringBeforeDelete => {
                state.journal_phase == SplitOperationPhase::Retiring
                    && state.retirement_phase == Some(RangeRetirementPhase::Parking)
                    && state.predecessor_topic_present.is_met()
                    && !state.retire_receipt_durable.is_met()
            }
            Self::RetiringAfterDelete => {
                state.journal_phase == SplitOperationPhase::Retiring
                    && state.retirement_phase == Some(RangeRetirementPhase::Parking)
                    && !state.predecessor_topic_present.is_met()
                    && !state.retire_receipt_durable.is_met()
            }
            Self::RetiringParked => {
                state.journal_phase == SplitOperationPhase::Retiring
                    && state.retirement_phase == Some(RangeRetirementPhase::Parked)
                    && !state.predecessor_topic_present.is_met()
                    && !state.retire_receipt_durable.is_met()
            }
            Self::Resuming => {
                state.journal_phase == SplitOperationPhase::Resuming
                    && state.retirement_phase == Some(RangeRetirementPhase::Parked)
                    && !state.predecessor_topic_present.is_met()
                    && state.retire_receipt_durable.is_met()
            }
            _ => false,
        }
    }

    fn is_ready(self, record: &SplitOperationRecord, tenant: &TenantRecord) -> bool {
        let plan = record.plan.as_ref();
        let matching_retirement = tenant.range_retirements.iter().find(|retirement| {
            retirement.operation_id == record.operation_id
                && retirement.source_range_id == record.source_range_id()
                && retirement.source_generation == record.predecessor_generation()
        });
        match self {
            Self::Running => {
                record.phase == SplitOperationPhase::Running
                    && record.evidence == crabka_gres_control::SplitOperationEvidence::default()
            }
            Self::Checkpointed => {
                record.phase == SplitOperationPhase::Checkpointed
                    && record.evidence.manifest_key.is_some()
                    && record.evidence.covered_offset.is_some()
                    && record.evidence.barrier_offset.is_none()
                    && record.evidence.tail_sha256.is_none()
                    && record.evidence.marker_digest.is_none()
            }
            Self::PausedBeforeStage => {
                record.phase == SplitOperationPhase::Paused
                    && record.evidence.manifest_key.is_some()
                    && record.evidence.covered_offset.is_some()
                    && record.evidence.barrier_offset.is_some()
                    && record.evidence.tail_sha256.is_none()
                    && record.evidence.marker_digest.is_none()
            }
            Self::PausedAfterStage => {
                record.phase == SplitOperationPhase::Paused
                    && record.evidence.manifest_key.is_some()
                    && record.evidence.covered_offset.is_some()
                    && record.evidence.barrier_offset.is_some()
                    && record.evidence.tail_sha256.is_some()
                    && record.evidence.marker_digest.is_none()
            }
            Self::Restored => {
                record.phase == SplitOperationPhase::Restored
                    && complete_transfer_evidence(record)
                    && plan.is_some_and(|plan| tenant.ranges == plan.current_layout)
                    && matching_retirement.is_none()
            }
            Self::ActivatedBeforeCutover => {
                record.phase == SplitOperationPhase::Activated
                    && complete_transfer_evidence(record)
                    && plan.is_some_and(|plan| tenant.ranges == plan.current_layout)
                    && matching_retirement.is_none()
            }
            Self::ActivatedAfterTenantCas => {
                record.phase == SplitOperationPhase::Activated
                    && complete_transfer_evidence(record)
                    && plan.is_some_and(|plan| tenant.ranges == plan.target_layout)
                    && matching_retirement
                        .is_some_and(|retirement| retirement.phase == RangeRetirementPhase::Parking)
            }
            Self::LayoutPublished => {
                record.phase == SplitOperationPhase::LayoutPublished
                    && complete_transfer_evidence(record)
                    && plan.is_some_and(|plan| tenant.ranges == plan.target_layout)
                    && matching_retirement
                        .is_some_and(|retirement| retirement.phase == RangeRetirementPhase::Parking)
            }
            Self::RetiringBeforeDelete
            | Self::RetiringAfterDelete
            | Self::RetiringParked
            | Self::Resuming => false,
        }
    }
}

const fn restart_hosted_ranges(point: SourceKillPoint) -> &'static str {
    match point {
        SourceKillPoint::ActivatedBeforeCutover
        | SourceKillPoint::ActivatedAfterTenantCas
        | SourceKillPoint::LayoutPublished
        | SourceKillPoint::RetiringBeforeDelete
        | SourceKillPoint::RetiringAfterDelete
        | SourceKillPoint::RetiringParked
        | SourceKillPoint::Resuming => "r0,r2",
        _ => "r0,r1",
    }
}

fn complete_transfer_evidence(record: &SplitOperationRecord) -> bool {
    record.evidence.manifest_key.is_some()
        && record.evidence.covered_offset.is_some()
        && record.evidence.barrier_offset.is_some()
        && record.evidence.tail_sha256.is_some()
        && record.evidence.marker_digest.is_some()
}

struct KillInjection<'a> {
    ledger_path: &'a Path,
    point: SourceKillPoint,
}

#[derive(Default)]
struct CutoverObservation {
    post_publication_ack_before_retirement: bool,
    ambiguous_cutover_advanced_without_republish: bool,
}

struct RetireReceiptProbes {
    before_kill: bool,
    after_restart: bool,
}

struct KillObservation {
    old_pid: u32,
    new_pid: u32,
    restart_ms: u128,
    pre_kill_ms: u128,
    stage_complete_ms: Option<u128>,
    publication_ms: Option<u128>,
    post_publication_ack_ms: Option<u128>,
    phase: SplitOperationPhase,
    evidence: crabka_gres_control::SplitOperationEvidence,
    cutover: CutoverObservation,
    tenant_layout: &'static str,
    retirement_phase: Option<RangeRetirementPhase>,
    tenant_record_version: u64,
    predecessor_topic_present: Option<bool>,
    delete_ledger: Arc<std::sync::Mutex<RetirementDeleteLedger>>,
    delete_calls_at_kill: usize,
    retire_receipt_probes: RetireReceiptProbes,
}

async fn probe_durable_retire_receipt(
    system: &ProcessHarness,
    client: &dyn RangeMutationClient,
    record: &SplitOperationRecord,
    boundary: &str,
) -> bool {
    let plan = record.plan.as_ref().expect("sealed plan");
    let endpoint = plan
        .current_layout
        .iter()
        .find(|range| range.range_id == record.source_range_id())
        .map(|range| range.endpoint.as_str())
        .expect("source endpoint");
    let request = crabka_gres_ranges::RangeControlReq {
        tenant: record.tenant.as_str().into(),
        range_id: crabka_gres_ranges::RangeId::new(record.source_range_id()),
        generation: record.predecessor_generation(),
        operation_id: record.operation_id.clone(),
        operation: crabka_gres_ranges::RangeControlOperation::RetirePredecessor,
    };
    let response = client
        .mutate(endpoint, request)
        .await
        .expect("probe durable retire receipt");
    assert_eq!(
        response,
        crabka_gres_ranges::RangeControlResp::AlreadyApplied,
        "{boundary} retire receipt probe must replay the durable completed receipt; source log: {}",
        system.log(0)
    );
    true
}

/// Client-side time the workload's ambiguity protocol may spend on top of
/// engine recovery before an acknowledgement can land: a 3s healthy-empty-read
/// streak plus polling and psql round trips. Added to the observed-safe engine
/// ack-gap bounds. A 4s allowance was overshot by 425ms on a CI runner
/// (19425ms against the 15s running/checkpointed engine bound), so this
/// carries a wider margin.
const WORKLOAD_AMBIGUITY_RESOLUTION_MS: u128 = 6_000;

fn parse_ack_ledger(contents: &str) -> Result<AckLedger, String> {
    let mut acknowledgements = BTreeMap::new();
    let mut attempts: BTreeMap<i64, usize> = BTreeMap::new();
    let mut retries: BTreeMap<i64, usize> = BTreeMap::new();
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
        match event.kind.as_str() {
            "attempt" => {
                *attempts.entry(event.seq).or_default() += 1;
            }
            "retry" => {
                if !attempts.contains_key(&event.seq) {
                    return Err(format!(
                        "ambiguity retry without a recorded attempt for sequence {}",
                        event.seq
                    ));
                }
                *retries.entry(event.seq).or_default() += 1;
            }
            "ack" | "recovered_ack" => {
                if !attempts.contains_key(&event.seq) {
                    return Err(format!(
                        "acknowledgement without a recorded attempt for sequence {}",
                        event.seq
                    ));
                }
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
            other => return Err(format!("unknown ledger event kind {other:?}")),
        }
    }
    for (expected, actual) in (0_i64..).zip(acknowledgements.keys().copied()) {
        if actual != expected {
            return Err(format!(
                "acknowledgement sequence is not contiguous: expected {expected}, found {actual}"
            ));
        }
    }
    let max_ack_gap_endpoints = acknowledgements
        .iter()
        .zip(acknowledgements.iter().skip(1))
        .max_by_key(|((_, left), (_, right))| right.saturating_sub(**left))
        .map(|((left_seq, left), (right_seq, right))| (*left_seq, *left, *right_seq, *right));
    let max_ack_gap_ms = max_ack_gap_endpoints
        .map(|(_, left, _, right)| right.saturating_sub(left))
        .unwrap_or_default();
    Ok(AckLedger {
        acknowledgements,
        attempts,
        retries,
        recovered,
        max_ack_gap_ms,
        max_ack_gap_endpoints,
    })
}

/// Explains a final-ledger mismatch per offending sequence, using the client
/// attempt and ambiguity-retry records to distinguish engine double-apply
/// (duplicate rows without any client retry) from a workload grace-window
/// breach (duplicate rows after the client concluded absence and re-INSERTed).
fn describe_ledger_mismatch(
    rows: &[(i64, String)],
    expected: &[(i64, String)],
    ledger: &AckLedger,
) -> String {
    let mut row_counts: BTreeMap<&(i64, String), usize> = BTreeMap::new();
    for row in rows {
        *row_counts.entry(row).or_default() += 1;
    }
    let mut expected_counts: BTreeMap<&(i64, String), usize> = BTreeMap::new();
    for entry in expected {
        *expected_counts.entry(entry).or_default() += 1;
    }
    let mut lines = Vec::new();
    for key in row_counts
        .keys()
        .chain(expected_counts.keys())
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
    {
        let observed = row_counts.get(key).copied().unwrap_or_default();
        let acknowledged = expected_counts.get(key).copied().unwrap_or_default();
        if observed == acknowledged {
            continue;
        }
        let (seq, checksum) = key;
        let attempts = ledger.attempts.get(seq).copied().unwrap_or_default();
        let retries = ledger.retries.get(seq).copied().unwrap_or_default();
        let verdict = if observed < acknowledged {
            "acknowledged write is missing from the database"
        } else if retries == 0 {
            "duplicated without any client retry, implicating the engine"
        } else {
            "duplicated after an ambiguity retry, so the workload concluded absence prematurely"
        };
        lines.push(format!(
            "seq {seq} checksum {checksum}: {observed} database rows vs {acknowledged} \
             acknowledged with {attempts} client attempts and {retries} ambiguity \
             retries ({verdict})"
        ));
    }
    lines.join("; ")
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
        let (status, forced) = if let Ok(status) =
            tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await
        {
            (status.expect("wait workload child"), false)
        } else {
            terminate_process_group(self.process_group);
            let status = tokio::time::timeout(Duration::from_secs(5), self.child.wait())
                .await
                .expect("terminated workload child stop timeout")
                .expect("wait terminated workload child");
            (status, true)
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
    use assert2::assert;
    let ledger = concat!(
        "{\"kind\":\"attempt\",\"seq\":1,\"timestamp_ms\":10}\n",
        "{\"kind\":\"ack\",\"seq\":1,\"timestamp_ms\":11}\n",
        "{\"kind\":\"recovered_ack\",\"seq\":1,\"timestamp_ms\":12}\n",
    );
    assert!(parse_ack_ledger(ledger).is_err());
}

#[test]
fn ack_ledger_rejects_noncontiguous_sequences() {
    use assert2::assert;
    let ledger = concat!(
        "{\"kind\":\"attempt\",\"seq\":0,\"timestamp_ms\":9}\n",
        "{\"kind\":\"ack\",\"seq\":0,\"timestamp_ms\":10}\n",
        "{\"kind\":\"attempt\",\"seq\":2,\"timestamp_ms\":11}\n",
        "{\"kind\":\"ack\",\"seq\":2,\"timestamp_ms\":12}\n",
    );
    assert!(parse_ack_ledger(ledger).is_err());
}

#[test]
fn ack_ledger_rejects_acknowledgement_without_attempt() {
    use assert2::assert;
    let ledger = "{\"kind\":\"ack\",\"seq\":0,\"timestamp_ms\":10}\n";
    assert!(parse_ack_ledger(ledger).is_err());
}

#[test]
fn ack_ledger_rejects_retry_without_attempt() {
    use assert2::assert;
    let ledger = "{\"kind\":\"retry\",\"seq\":0,\"timestamp_ms\":10}\n";
    assert!(parse_ack_ledger(ledger).is_err());
}

#[test]
fn ack_ledger_rejects_unknown_event_kinds() {
    use assert2::assert;
    let ledger = "{\"kind\":\"mystery\",\"seq\":0,\"timestamp_ms\":10}\n";
    assert!(parse_ack_ledger(ledger).is_err());
}

#[test]
fn ack_ledger_counts_attempts_and_ambiguity_retries() {
    use assert2::assert;
    let ledger = parse_ack_ledger(concat!(
        "{\"kind\":\"attempt\",\"seq\":0,\"timestamp_ms\":10}\n",
        "{\"kind\":\"ack\",\"seq\":0,\"timestamp_ms\":11}\n",
        "{\"kind\":\"attempt\",\"seq\":1,\"timestamp_ms\":12}\n",
        "{\"kind\":\"retry\",\"seq\":1,\"timestamp_ms\":13}\n",
        "{\"kind\":\"attempt\",\"seq\":1,\"timestamp_ms\":14}\n",
        "{\"kind\":\"recovered_ack\",\"seq\":1,\"timestamp_ms\":15}\n",
    ))
    .expect("valid retried ledger");
    assert!(ledger.acknowledgements.len() == 2);
    assert!(ledger.recovered == 1);
    assert!(ledger.attempts == BTreeMap::from([(0, 1), (1, 2)]));
    assert!(ledger.retries == BTreeMap::from([(1, 1)]));
}

#[test]
fn ledger_mismatch_distinguishes_engine_duplicates_from_workload_retries() {
    use assert2::assert;
    let ledger = parse_ack_ledger(concat!(
        "{\"kind\":\"attempt\",\"seq\":0,\"timestamp_ms\":10}\n",
        "{\"kind\":\"ack\",\"seq\":0,\"timestamp_ms\":11}\n",
        "{\"kind\":\"attempt\",\"seq\":1,\"timestamp_ms\":12}\n",
        "{\"kind\":\"retry\",\"seq\":1,\"timestamp_ms\":13}\n",
        "{\"kind\":\"attempt\",\"seq\":1,\"timestamp_ms\":14}\n",
        "{\"kind\":\"ack\",\"seq\":1,\"timestamp_ms\":15}\n",
    ))
    .expect("valid retried ledger");
    let expected = vec![(0, "a".to_owned()), (1, "b".to_owned())];
    let engine_duplicate = describe_ledger_mismatch(
        &[
            (0, "a".to_owned()),
            (0, "a".to_owned()),
            (1, "b".to_owned()),
        ],
        &expected,
        &ledger,
    );
    assert!(engine_duplicate.contains("seq 0"));
    assert!(engine_duplicate.contains("implicating the engine"));
    let retry_duplicate = describe_ledger_mismatch(
        &[
            (0, "a".to_owned()),
            (1, "b".to_owned()),
            (1, "b".to_owned()),
        ],
        &expected,
        &ledger,
    );
    assert!(retry_duplicate.contains("seq 1"));
    assert!(retry_duplicate.contains("concluded absence prematurely"));
    let missing = describe_ledger_mismatch(&[(0, "a".to_owned())], &expected, &ledger);
    assert!(missing.contains("missing from the database"));
}

#[test]
fn retirement_kill_predicates_require_exact_durable_state() {
    let exact = [
        (
            SourceKillPoint::RetiringBeforeDelete,
            RetirementPredicateState {
                journal_phase: SplitOperationPhase::Retiring,
                target_layout: PredicateValue::Met,
                target_record_version: PredicateValue::Met,
                retirement_phase: Some(RangeRetirementPhase::Parking),
                predecessor_topic_present: PredicateValue::Met,
                retire_receipt_durable: PredicateValue::Unmet,
            },
        ),
        (
            SourceKillPoint::RetiringAfterDelete,
            RetirementPredicateState {
                journal_phase: SplitOperationPhase::Retiring,
                target_layout: PredicateValue::Met,
                target_record_version: PredicateValue::Met,
                retirement_phase: Some(RangeRetirementPhase::Parking),
                predecessor_topic_present: PredicateValue::Unmet,
                retire_receipt_durable: PredicateValue::Unmet,
            },
        ),
        (
            SourceKillPoint::RetiringParked,
            RetirementPredicateState {
                journal_phase: SplitOperationPhase::Retiring,
                target_layout: PredicateValue::Met,
                target_record_version: PredicateValue::Met,
                retirement_phase: Some(RangeRetirementPhase::Parked),
                predecessor_topic_present: PredicateValue::Unmet,
                retire_receipt_durable: PredicateValue::Unmet,
            },
        ),
        (
            SourceKillPoint::Resuming,
            RetirementPredicateState {
                journal_phase: SplitOperationPhase::Resuming,
                target_layout: PredicateValue::Met,
                target_record_version: PredicateValue::Met,
                retirement_phase: Some(RangeRetirementPhase::Parked),
                predecessor_topic_present: PredicateValue::Unmet,
                retire_receipt_durable: PredicateValue::Met,
            },
        ),
    ];

    for (point, state) in exact {
        assert!(point.retirement_is_ready(state));
        for near_miss in [
            RetirementPredicateState {
                target_layout: PredicateValue::Unmet,
                ..state
            },
            RetirementPredicateState {
                target_record_version: PredicateValue::Unmet,
                ..state
            },
            RetirementPredicateState {
                journal_phase: SplitOperationPhase::Completed,
                ..state
            },
            RetirementPredicateState {
                retirement_phase: None,
                ..state
            },
            RetirementPredicateState {
                predecessor_topic_present: state.predecessor_topic_present.toggled(),
                ..state
            },
            RetirementPredicateState {
                retire_receipt_durable: state.retire_receipt_durable.toggled(),
                ..state
            },
        ] {
            assert!(!point.retirement_is_ready(near_miss));
        }
    }
}

#[test]
fn counting_retirement_admin_rejects_unrelated_delete_requests() {
    let mut ledger = RetirementDeleteLedger::default();
    ledger
        .record_delete_request("__gres_wal.tenant.r1", &["__gres_wal.tenant.r1"])
        .expect("exact predecessor delete");
    assert_eq!(
        ledger,
        RetirementDeleteLedger {
            exact_delete_calls: 1,
            requested_topics: vec!["__gres_wal.tenant.r1".to_owned()],
            unrelated_delete_attempted: false,
            injected_after_delete_errors: 0,
        }
    );

    assert!(
        ledger
            .record_delete_request("__gres_wal.tenant.r1", &["sentinel"])
            .is_err()
    );
    assert_eq!(
        ledger,
        RetirementDeleteLedger {
            exact_delete_calls: 1,
            requested_topics: vec!["__gres_wal.tenant.r1".to_owned()],
            unrelated_delete_attempted: true,
            injected_after_delete_errors: 0,
        }
    );
}

#[test]
fn retirement_restart_uses_authoritative_target_ranges() {
    for (point, expected_ranges) in [
        (SourceKillPoint::RetiringBeforeDelete, "r0,r2"),
        (SourceKillPoint::RetiringAfterDelete, "r0,r2"),
        (SourceKillPoint::RetiringParked, "r0,r2"),
        (SourceKillPoint::Resuming, "r0,r2"),
        (SourceKillPoint::ActivatedBeforeCutover, "r0,r2"),
        (SourceKillPoint::Restored, "r0,r1"),
    ] {
        assert_eq!(restart_hosted_ranges(point), expected_ranges);
    }
}

#[tokio::test]
async fn split_successor_proxies_are_distinct_and_retargeted() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    let mut system = ProcessHarness::start_all_on_zero(&format!(
        "tenant-g8-split-proxies-{:x}-p{:x}",
        timestamp_ms(),
        std::process::id()
    ))
    .await;
    let [r2, r3] = system.split_successor_endpoints();
    assert_ne!(r2, r3);
    system.restart_with_hosted_ranges(0, "r0,r1").await;
    assert_eq!(system.range_endpoint(2), r2);
    assert_eq!(system.range_endpoint(3), r3);
    system.shutdown().await;
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

async fn initiate_split_with_cli(
    system: &ProcessHarness,
    operation_id: &str,
    table: u64,
    rowid: u64,
) {
    let [left_endpoint, right_endpoint] = system.split_successor_endpoints();
    let output = tokio::process::Command::new(cli_binary())
        .args([
            "gres",
            "split",
            "--bootstrap",
            system.bootstrap(),
            system.tenant(),
            &table.to_string(),
            &rowid.to_string(),
            "--operation-id",
            operation_id,
            "--left-range-id",
            "2",
            "--left-endpoint",
            &left_endpoint,
            "--successor-range-id",
            "3",
            "--successor-endpoint",
            &right_endpoint,
            "--successor-wal-generation",
            "1",
        ])
        .output()
        .await
        .expect("run actual Split CLI");
    assert!(
        output.status.success(),
        "Split CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct SplitLedgerRow {
    rowid: u64,
    seq: i32,
    route_key: i32,
    checksum: String,
}

struct SplitFoundationSetup {
    operation_started: Instant,
    operation_id: String,
    system: ProcessHarness,
    sentinel_topic: String,
    acknowledged: Vec<SplitLedgerRow>,
}

async fn prepare_split_foundation() -> SplitFoundationSetup {
    assert!(cli_binary().is_file(), "dedicated CI must build crabka CLI");
    let operation_started = Instant::now();
    let identity = format!("{:x}-p{:x}", timestamp_ms(), std::process::id());
    let operation_id = format!("g8-split-{identity}");
    let system = ProcessHarness::start_all_on_zero(&format!("tenant-g8-split-{identity}")).await;
    let mut registry = Registry::connect(system.bootstrap())
        .await
        .expect("registry");
    assert!(
        registry
            .load_split_operation(system.tenant(), &operation_id)
            .await
            .expect("unique split operation")
            .is_none()
    );
    let sentinel_topic = format!("__gres_g8_split_sentinel.{}", system.tenant());
    let mut sentinel_admin =
        crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
            .await
            .expect("split sentinel admin");
    let outcomes = sentinel_admin
        .create_topics(
            &[crabka_client_admin::CreateTopicSpec {
                name: sentinel_topic.clone(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            30_000,
        )
        .await
        .expect("create split sentinel");
    assert!(outcomes.iter().all(|outcome| outcome.error.is_none()));

    let source = system.sql(0).await;
    source
        .simple_query(
            "CREATE TABLE live_ledger51 (seq int4, route_key int4, checksum text NOT NULL) SHARDED",
        )
        .await
        .expect("create split ledger");
    let mut ack_file = tempfile::NamedTempFile::new().expect("external split ACK ledger");
    for seq in 0..32_i32 {
        let route_key = if seq % 2 == 0 { seq / 2 } else { 16 + seq / 2 };
        let row = SplitLedgerRow {
            rowid: u64::try_from(seq).unwrap() + 1,
            seq,
            route_key,
            checksum: format!("split-{seq:04x}-{route_key:04x}"),
        };
        source
            .execute(
                "INSERT INTO live_ledger51 VALUES ($1, $2, $3)",
                &[&row.seq, &row.route_key, &row.checksum],
            )
            .await
            .expect("acknowledged split source write");
        serde_json::to_writer(&mut ack_file, &row).expect("append split ACK");
        ack_file.write_all(b"\n").expect("terminate split ACK");
        ack_file.as_file().sync_data().expect("fsync split ACK");
    }
    ack_file.flush().expect("flush split ACK ledger");
    ack_file
        .as_file()
        .sync_all()
        .expect("sync split ACK ledger");
    let acknowledged = std::fs::read_to_string(ack_file.path())
        .expect("read back fsynced split ACK ledger")
        .lines()
        .map(|line| serde_json::from_str(line).expect("decode split ACK ledger row"))
        .collect::<Vec<_>>();
    assert_eq!(acknowledged.len(), 32);
    assert!(
        direct_successor_rows(&system, 0, 51, None, None)
            .await
            .is_empty()
    );
    assert_eq!(
        direct_successor_rows(&system, 1, 51, None, None)
            .await
            .iter()
            .map(|row| row.rowid)
            .collect::<Vec<_>>(),
        (1..33_u64).collect::<Vec<_>>()
    );
    SplitFoundationSetup {
        operation_started,
        operation_id,
        system,
        sentinel_topic,
        acknowledged,
    }
}

/// Scan one range directly, bypassing the gateway.
///
/// `routing_table_id` is the suffix the fixture bakes into the relation name to
/// pin its routing slot; the scan RPC addresses the relation by *catalog* id, so
/// the two are resolved against each other here rather than assumed equal.
async fn direct_successor_rows(
    system: &ProcessHarness,
    range_id: u32,
    routing_table_id: u64,
    start: Option<u64>,
    end: Option<u64>,
) -> Vec<SplitLedgerRow> {
    let table_id = system
        .catalog_table_id(&format!("live_ledger{routing_table_id}"))
        .await;
    let scan = crabka_gres_ranges::transport::ScanRangeReq {
        range_id: crabka_gres_ranges::RangeId::new(range_id),
        table_id,
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
    eprintln!("direct r{range_id} ScanRange request: {scan:?}");
    let request = crabka_gres_ranges::RangeRequest::ScanRange(scan);
    let response = system
        .operator_control_client()
        .call(&system.range_endpoint(range_id), &request)
        .await
        .unwrap_or_else(|error| panic!("direct r{range_id} scan: {error}"));
    eprintln!("direct r{range_id} ScanRange response: {response:?}");
    let crabka_gres_ranges::RangeResponse::ScanRange(response) = response else {
        panic!("direct r{range_id} scan returned {response:?}");
    };
    response
        .rows
        .into_iter()
        .map(|row| {
            let (_, _, values) = crabka_pgmvcc::version::decode_tuple(&row.tuple)
                .expect("decode direct split ledger tuple");
            let [
                crabka_pgtypes::Datum::Int4(seq),
                crabka_pgtypes::Datum::Int4(route_key),
                crabka_pgtypes::Datum::Text(checksum),
            ] = values.as_slice()
            else {
                panic!("unexpected direct split ledger tuple {values:?}");
            };
            SplitLedgerRow {
                rowid: row.rowid,
                seq: *seq,
                route_key: *route_key,
                checksum: checksum.clone(),
            }
        })
        .collect()
}

async fn load_operation(bootstrap: &str, tenant: &str, operation_id: &str) -> SplitOperationRecord {
    let mut registry = Registry::connect(bootstrap).await.expect("registry");
    registry
        .load_split_operation(tenant, operation_id)
        .await
        .expect("load operation")
        .expect("journaled operation")
}

async fn load_tenant(bootstrap: &str, tenant: &str) -> TenantRecord {
    let mut registry = Registry::connect(bootstrap).await.expect("registry");
    registry
        .get(tenant)
        .await
        .expect("load tenant")
        .expect("tenant present")
}

async fn retirement_topic_present(admin: &mut dyn RangeRetirementAdmin, topic: &str) -> bool {
    admin
        .metadata(&[topic])
        .await
        .expect("retirement topic metadata")
        .topics
        .iter()
        .find(|entry| entry.name == topic)
        .is_some_and(|entry| entry.error.is_none())
}

struct OperationRestartInput<'a> {
    system: &'a mut ProcessHarness,
    operation_id: &'a str,
    current: &'a SplitOperationRecord,
    current_tenant: &'a TenantRecord,
    injection: &'a KillInjection<'a>,
    predecessor_topic_at_boundary: Option<bool>,
    predecessor_topic: &'a str,
    control: &'a mut GresControlHandle,
    mutation_client: &'a mut RecordingRangeMutationClient,
    retirement_admin: &'a mut CountingRetirementAdmin,
    marker_observations: &'a Arc<Mutex<Vec<MarkerObservation>>>,
    delete_ledger: &'a Arc<std::sync::Mutex<RetirementDeleteLedger>>,
}

async fn restart_operation_source(input: OperationRestartInput<'_>) -> KillObservation {
    wait_for_ack_count(input.injection.ledger_path, 8).await;
    let old_pid = input.system.pid(0);
    let pre_kill_ms = timestamp_ms();
    input.system.kill(0).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    input
        .system
        .restart_with_hosted_ranges(0, restart_hosted_ranges(input.injection.point))
        .await;
    let new_pid = input.system.pid(0);
    assert_ne!(
        old_pid, new_pid,
        "SIGKILL restart must replace the real child"
    );
    let mut fresh_registry = Registry::connect(input.system.bootstrap())
        .await
        .expect("fresh post-kill registry");
    fresh_registry.ensure_topic().await.expect("registry topic");
    *input.control = Arc::new(BrokerControl {
        registry: Mutex::new(fresh_registry),
    });
    *input.mutation_client = RecordingRangeMutationClient {
        inner: MtlsRangeMutationClient::new(input.system.operator_control_client()),
        marker_observations: Arc::clone(input.marker_observations),
    };
    let fresh_admin =
        crabka_client_admin::AdminClient::connect(&[input.system.bootstrap().to_owned()])
            .await
            .expect("fresh retirement admin");
    *input.retirement_admin = CountingRetirementAdmin::new(
        fresh_admin,
        input.predecessor_topic.to_owned(),
        Arc::clone(input.delete_ledger),
    );
    let mut observation = KillObservation {
        old_pid,
        new_pid,
        restart_ms: timestamp_ms(),
        pre_kill_ms,
        stage_complete_ms: None,
        publication_ms: None,
        post_publication_ack_ms: None,
        phase: input.current.phase,
        evidence: input.current.evidence.clone(),
        cutover: CutoverObservation::default(),
        tenant_layout: input
            .current
            .plan
            .as_ref()
            .map(|plan| {
                if input.current_tenant.ranges == plan.target_layout {
                    "target"
                } else {
                    "current"
                }
            })
            .expect("sealed plan"),
        retirement_phase: input
            .current_tenant
            .range_retirements
            .iter()
            .find(|retirement| retirement.operation_id == input.current.operation_id)
            .map(|retirement| retirement.phase),
        tenant_record_version: input.current_tenant.record_version,
        predecessor_topic_present: input.predecessor_topic_at_boundary,
        delete_ledger: Arc::clone(input.delete_ledger),
        delete_calls_at_kill: input
            .delete_ledger
            .lock()
            .expect("delete ledger at kill")
            .exact_delete_calls,
        retire_receipt_probes: RetireReceiptProbes {
            before_kill: input.injection.point == SourceKillPoint::Resuming,
            after_restart: false,
        },
    };
    let restarted_tenant = load_tenant(input.system.bootstrap(), input.system.tenant()).await;
    assert_eq!(
        restarted_tenant.record_version,
        input.current_tenant.record_version
    );
    assert_eq!(restarted_tenant.ranges, input.current_tenant.ranges);
    assert_eq!(
        restarted_tenant.range_retirements,
        input.current_tenant.range_retirements
    );
    if input.injection.point == SourceKillPoint::Resuming {
        let restarted_operation = load_operation(
            input.system.bootstrap(),
            input.system.tenant(),
            input.operation_id,
        )
        .await;
        assert_eq!(restarted_operation.phase, SplitOperationPhase::Resuming);
        observation.retire_receipt_probes.after_restart = probe_durable_retire_receipt(
            input.system,
            input.mutation_client,
            &restarted_operation,
            "after restart",
        )
        .await;
    }
    observation
}

async fn reconcile_activated_operation(
    system: &ProcessHarness,
    operation_id: &str,
    control: &GresControlHandle,
    mutation_client: &RecordingRangeMutationClient,
    current: &SplitOperationRecord,
    restarted_pids: Option<&mut KillObservation>,
) {
    let readiness_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match verify_target_topology_ready(mutation_client, current).await {
            Ok(()) => break,
            Err(error) if Instant::now() < readiness_deadline => {
                let debug_tenant = load_tenant(system.bootstrap(), system.tenant()).await;
                let debug_operation =
                    load_operation(system.bootstrap(), system.tenant(), operation_id).await;
                let plan = debug_operation.plan.as_ref().expect("plan");
                eprintln!(
                    "target readiness retry: {error}; tenant_version={} phase={:?} source_version={} current_match={} target_match={}",
                    debug_tenant.record_version,
                    debug_operation.phase,
                    plan.source_record_version,
                    debug_tenant.ranges == plan.current_layout,
                    debug_tenant.ranges == plan.target_layout,
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("target readiness: {error}"),
        }
    }
    let tenant_before_cutover = load_tenant(system.bootstrap(), system.tenant()).await;
    let reconciled = reconcile_activated_cutover(control, current)
        .await
        .expect("atomic cutover");
    if current
        .plan
        .as_ref()
        .is_some_and(|plan| tenant_before_cutover.ranges == plan.target_layout)
    {
        let tenant_after_cutover = load_tenant(system.bootstrap(), system.tenant()).await;
        assert_eq!(
            tenant_after_cutover.record_version,
            tenant_before_cutover.record_version
        );
        assert_eq!(tenant_after_cutover.ranges, tenant_before_cutover.ranges);
        assert_eq!(
            tenant_after_cutover.range_retirements,
            tenant_before_cutover.range_retirements
        );
        assert_eq!(reconciled.phase, SplitOperationPhase::LayoutPublished);
        if let Some(observation) = restarted_pids {
            observation
                .cutover
                .ambiguous_cutover_advanced_without_republish = true;
        }
    }
}

type DriveOperationResult = (
    SplitOperationRecord,
    Option<KillObservation>,
    Vec<MarkerObservation>,
    RetirementDeleteLedger,
);

async fn finish_drive(
    current: SplitOperationRecord,
    restarted_pids: Option<KillObservation>,
    marker_observations: &Mutex<Vec<MarkerObservation>>,
    delete_ledger: &std::sync::Mutex<RetirementDeleteLedger>,
) -> DriveOperationResult {
    (
        current,
        restarted_pids,
        marker_observations.lock().await.clone(),
        delete_ledger.lock().expect("final delete ledger").clone(),
    )
}

async fn delay_after_transient_reconcile(
    system: &ProcessHarness,
    started: Instant,
    max_operation_duration: Duration,
    error: &SplitReconcileError,
) {
    assert!(
        started.elapsed() < max_operation_duration,
        "operation deadline after transient reconcile error: {error}; source log: {}",
        system.log(0)
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn reconcile_non_special_phase(
    control: &GresControlHandle,
    mutation_client: &RecordingRangeMutationClient,
    current: &SplitOperationRecord,
    system: &ProcessHarness,
    started: Instant,
    max_operation_duration: Duration,
) {
    match reconcile_one_rpc_phase(control, mutation_client, current).await {
        Ok(_) => {}
        Err(
            error @ (SplitReconcileError::Transport(_)
            | SplitReconcileError::Ambiguous(_)
            | SplitReconcileError::Registry(_)),
        ) => {
            delay_after_transient_reconcile(system, started, max_operation_duration, &error).await;
        }
        Err(error) => panic!("non-retryable reconcile error: {error}"),
    }
}

async fn drive_operation(
    system: &mut ProcessHarness,
    operation_id: &str,
    max_operation_duration: std::time::Duration,
    kill_injection: Option<KillInjection<'_>>,
) -> DriveOperationResult {
    let tenant = TenantName::try_from(system.tenant()).expect("tenant");
    let mut registry = Registry::connect(system.bootstrap())
        .await
        .expect("registry");
    registry.ensure_topic().await.expect("registry topic");
    let mut control: GresControlHandle = Arc::new(BrokerControl {
        registry: Mutex::new(registry),
    });
    let marker_observations = Arc::new(Mutex::new(Vec::new()));
    let mut mutation_client = RecordingRangeMutationClient {
        inner: MtlsRangeMutationClient::new(system.operator_control_client()),
        marker_observations: Arc::clone(&marker_observations),
    };
    let predecessor_topic = format!("__gres_wal.{}.r1", system.tenant());
    let delete_ledger = Arc::new(std::sync::Mutex::new(RetirementDeleteLedger::default()));
    let admin = crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
        .await
        .expect("retirement admin");
    let mut retirement_admin =
        CountingRetirementAdmin::new(admin, predecessor_topic.clone(), Arc::clone(&delete_ledger));
    let started = Instant::now();
    let mut restarted_pids = None;
    loop {
        let current = load_operation(system.bootstrap(), system.tenant(), operation_id).await;
        let current_tenant = load_tenant(system.bootstrap(), system.tenant()).await;
        let mut predecessor_topic_at_boundary = None;
        let retirement_ready = if let Some(injection) = kill_injection.as_ref()
            && matches!(
                injection.point,
                SourceKillPoint::RetiringBeforeDelete
                    | SourceKillPoint::RetiringAfterDelete
                    | SourceKillPoint::RetiringParked
                    | SourceKillPoint::Resuming
            ) {
            let plan = current.plan.as_ref().expect("sealed plan");
            let retirement_phase = current_tenant
                .range_retirements
                .iter()
                .find(|retirement| retirement.operation_id == current.operation_id)
                .map(|retirement| retirement.phase);
            let predecessor_topic_present =
                retirement_topic_present(&mut retirement_admin, &predecessor_topic).await;
            predecessor_topic_at_boundary = Some(predecessor_topic_present);
            let retire_receipt_durable = if injection.point == SourceKillPoint::Resuming
                && current.phase == SplitOperationPhase::Resuming
            {
                probe_durable_retire_receipt(system, &mutation_client, &current, "before kill")
                    .await
            } else {
                false
            };
            injection
                .point
                .retirement_is_ready(RetirementPredicateState {
                    journal_phase: current.phase,
                    target_layout: (current_tenant.ranges == plan.target_layout).into(),
                    target_record_version: plan
                        .source_record_version
                        .checked_add(1)
                        .is_some_and(|minimum| current_tenant.record_version >= minimum)
                        .into(),
                    retirement_phase,
                    predecessor_topic_present: predecessor_topic_present.into(),
                    retire_receipt_durable: retire_receipt_durable.into(),
                })
        } else {
            false
        };
        let source_or_cutover_ready = kill_injection
            .as_ref()
            .is_some_and(|injection| injection.point.is_ready(&current, &current_tenant));
        if restarted_pids.is_none() && (retirement_ready || source_or_cutover_ready) {
            let injection = kill_injection.as_ref().expect("checked");
            restarted_pids = Some(
                restart_operation_source(OperationRestartInput {
                    system,
                    operation_id,
                    current: &current,
                    current_tenant: &current_tenant,
                    injection,
                    predecessor_topic_at_boundary,
                    predecessor_topic: &predecessor_topic,
                    control: &mut control,
                    mutation_client: &mut mutation_client,
                    retirement_admin: &mut retirement_admin,
                    marker_observations: &marker_observations,
                    delete_ledger: &delete_ledger,
                })
                .await,
            );
        }
        if current.evidence.tail_sha256.is_some()
            && let Some(observation) = restarted_pids.as_mut()
            && observation.stage_complete_ms.is_none()
        {
            observation.stage_complete_ms = Some(timestamp_ms());
        }
        if matches!(
            current.phase,
            SplitOperationPhase::Activated
                | SplitOperationPhase::LayoutPublished
                | SplitOperationPhase::Retiring
                | SplitOperationPhase::Resuming
        ) && current.plan.as_ref().is_some_and(|plan| {
            current_tenant.ranges == plan.target_layout
                && plan
                    .source_record_version
                    .checked_add(1)
                    .is_some_and(|minimum| current_tenant.record_version >= minimum)
        }) && let (Some(injection), Some(observation)) =
            (kill_injection.as_ref(), restarted_pids.as_mut())
            && !observation.cutover.post_publication_ack_before_retirement
        {
            observation.publication_ms = Some(timestamp_ms());
            let acknowledged = parse_ack_ledger(
                &std::fs::read_to_string(injection.ledger_path)
                    .expect("post-publication acknowledgement ledger"),
            )
            .expect("valid post-publication acknowledgement ledger")
            .acknowledgements
            .len();
            wait_for_ack_count(injection.ledger_path, acknowledged + 1).await;
            observation.cutover.post_publication_ack_before_retirement = true;
            observation.post_publication_ack_ms = parse_ack_ledger(
                &std::fs::read_to_string(injection.ledger_path)
                    .expect("post-publication acknowledgement ledger"),
            )
            .expect("valid post-publication acknowledgement ledger")
            .acknowledgements
            .values()
            .next_back()
            .copied();
        }
        match current.phase {
            SplitOperationPhase::Activated => {
                reconcile_activated_operation(
                    system,
                    operation_id,
                    &control,
                    &mutation_client,
                    &current,
                    restarted_pids.as_mut(),
                )
                .await;
            }
            SplitOperationPhase::Retiring => {
                if kill_injection.as_ref().is_some_and(|injection| {
                    injection.point == SourceKillPoint::RetiringAfterDelete
                        && restarted_pids.is_none()
                }) {
                    retirement_admin.arm_error_after_delete();
                    let error =
                        reconcile_one_retiring_range_wal(&control, &mut retirement_admin, &tenant)
                            .await
                            .expect_err("AfterDelete must stop before sidecar CAS");
                    assert!(error.to_string().contains("injected ambiguity"));
                    continue;
                }
                assert!(
                    reconcile_one_retiring_range_wal(&control, &mut retirement_admin, &tenant)
                        .await
                        .expect("WAL retirement")
                );
                if kill_injection.as_ref().is_some_and(|injection| {
                    injection.point == SourceKillPoint::RetiringParked && restarted_pids.is_none()
                }) {
                    continue;
                }
                let rpc_result = reconcile_one_rpc_phase(&control, &mutation_client, &current)
                    .await
                    .expect("retire predecessor RPC");
                assert_eq!(rpc_result.phase, SplitOperationPhase::Resuming);
            }
            SplitOperationPhase::Completed => {
                assert!(started.elapsed() < max_operation_duration);
                return finish_drive(
                    current,
                    restarted_pids,
                    &marker_observations,
                    &delete_ledger,
                )
                .await;
            }
            _ => {
                reconcile_non_special_phase(
                    &control,
                    &mutation_client,
                    &current,
                    system,
                    started,
                    max_operation_duration,
                )
                .await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_move_cli_operator_and_wal_retirement() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    assert!(cli_binary().is_file(), "dedicated CI must build crabka CLI");
    let identity = format!("{}-p{}", timestamp_ms(), std::process::id());
    let operation_id = format!("g8-live-move-{identity}");
    let mut system =
        ProcessHarness::start_all_on_zero(&format!("tenant-g8-live-move-{identity}")).await;
    let mut ddl = String::new();
    for table in 1..50 {
        write!(&mut ddl, "CREATE TABLE filler_{table} (id int4);").expect("write DDL to string");
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
    initiate_move_with_cli(&system, &operation_id).await;
    let operation_started = Instant::now();
    let completed = tokio::time::timeout(
        Duration::from_secs(75),
        drive_operation(&mut system, &operation_id, Duration::from_secs(75), None),
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
            "tenant_id": system.tenant(),
            "operation_id": operation_id,
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

struct SplitFoundationEvidence<'a> {
    system: &'a ProcessHarness,
    operation_id: &'a str,
    sentinel_topic: &'a str,
    completed: &'a SplitOperationRecord,
    tenant: &'a TenantRecord,
    r2_rows: &'a [SplitLedgerRow],
    r3_rows: &'a [SplitLedgerRow],
    acknowledged: &'a [SplitLedgerRow],
    full_sequences: &'a [SplitLedgerRow],
    marker_observation: &'a MarkerObservation,
    journal_revision: u64,
    journal_digest: &'a str,
    partition_union: &'a [crabka_gres_ranges::WireInDoubtMarker],
    delete_observation: &'a RetirementDeleteLedger,
    operation_elapsed_ms: u128,
}

struct VerifiedSplitMarkers {
    observation: MarkerObservation,
    journal_revision: u64,
    journal_digest: String,
    partition_union: Vec<crabka_gres_ranges::WireInDoubtMarker>,
}

fn verify_split_markers(
    completed: &SplitOperationRecord,
    operation_id: &str,
    marker_observations: &[MarkerObservation],
) -> VerifiedSplitMarkers {
    let [observation] = marker_observations else {
        panic!("expected exactly one authenticated marker response")
    };
    assert_eq!(observation.request.range_id.as_u32(), 1);
    assert_eq!(observation.request.operation_id, operation_id);
    assert_eq!(observation.request.generation, 0);
    let predecessor = completed
        .plan
        .as_ref()
        .expect("sealed Split plan")
        .current_layout
        .iter()
        .find(|range| range.range_id == 1)
        .expect("predecessor r1");
    assert_eq!(observation.endpoint, predecessor.endpoint);
    let crabka_gres_ranges::RangeControlOperation::InheritMarkers {
        journal_revision,
        journal_digest,
    } = &observation.request.operation
    else {
        panic!("recorded non-marker request");
    };
    assert!(*journal_revision > 0);
    assert!(!journal_digest.is_empty());
    assert_eq!(
        observation.digest,
        completed.evidence.marker_digest.as_deref().unwrap()
    );
    let mut partition_union = observation.left_markers.clone();
    partition_union.extend(observation.right_markers.iter().copied());
    assert_eq!(partition_union, observation.markers);
    assert!(observation.left_markers.iter().all(|marker| {
        marker.key.table_id < 51 || (marker.key.table_id == 51 && marker.key.rowid < 16)
    }));
    assert!(observation.right_markers.iter().all(|marker| {
        marker.key.table_id > 51 || (marker.key.table_id == 51 && marker.key.rowid >= 16)
    }));
    assert!(
        observation
            .left_markers
            .iter()
            .all(|left| { observation.right_markers.iter().all(|right| left != right) })
    );
    VerifiedSplitMarkers {
        observation: observation.clone(),
        journal_revision: *journal_revision,
        journal_digest: journal_digest.clone(),
        partition_union,
    }
}

async fn write_split_foundation_evidence(input: SplitFoundationEvidence<'_>) {
    let mut admin =
        crabka_client_admin::AdminClient::connect(&[input.system.bootstrap().to_owned()])
            .await
            .expect("split topic admin");
    let topics = admin
        .metadata(&[])
        .await
        .expect("split topic metadata")
        .topics
        .into_iter()
        .filter(|topic| topic.error.is_none())
        .map(|topic| topic.name)
        .collect::<std::collections::BTreeSet<_>>();
    let predecessor_topic = format!("__gres_wal.{}.r1", input.system.tenant());
    assert!(!topics.contains(&predecessor_topic));
    for topic in [
        format!("__gres_wal.{}.r0", input.system.tenant()),
        format!("__gres_wal.{}.r2.g0000000001", input.system.tenant()),
        format!("__gres_wal.{}.r3.g0000000001", input.system.tenant()),
        input.sentinel_topic.to_owned(),
    ] {
        assert!(topics.contains(&topic));
    }
    let Some(path) = std::env::var_os("CRABKA_G8_SPLIT_EVIDENCE") else {
        return;
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create split evidence directory");
    }
    let plan = input.completed.plan.as_ref().expect("sealed plan");
    let r2 = plan
        .target_layout
        .iter()
        .find(|range| range.range_id == 2)
        .expect("r2 target");
    let r3 = plan
        .target_layout
        .iter()
        .find(|range| range.range_id == 3)
        .expect("r3 target");
    let marker = input.marker_observation;
    let evidence = serde_json::json!({
        "schema_version": 1,
        "operation": "split",
        "tenant_id": input.system.tenant(),
        "operation_id": input.operation_id,
        "phase": "completed",
        "routing_table_id": plan.routing_table_id,
        "split_rowid": 16,
        "target_range_ids": [0, 2, 3],
        "r2": {"endpoint": r2.endpoint, "generation": r2.wal_generation, "serving": input.tenant.ranges.contains(r2), "row_count": input.r2_rows.len(), "cross_side_rows": input.r2_rows.iter().filter(|row| row.rowid >= 16).count()},
        "r3": {"endpoint": r3.endpoint, "generation": r3.wal_generation, "serving": input.tenant.ranges.contains(r3), "row_count": input.r3_rows.len(), "cross_side_rows": input.r3_rows.iter().filter(|row| row.rowid < 16).count()},
        "endpoints_distinct": r2.endpoint != r3.endpoint,
        "acknowledged_rows": input.acknowledged.len(),
        "full_scan_rows": input.full_sequences.len(),
        "full_ack_equality": input.full_sequences == input.acknowledged,
        "marker_partition": {
            "authenticated_endpoint": marker.endpoint,
            "request_range_id": marker.request.range_id.as_u32(),
            "request_generation": marker.request.generation,
            "request_journal_revision": input.journal_revision,
            "request_journal_digest": input.journal_digest,
            "predecessor_count": marker.markers.len(),
            "r2_count": marker.left_markers.len(),
            "r3_count": marker.right_markers.len(),
            "disjoint": marker.left_markers.iter().all(|left| marker.right_markers.iter().all(|right| left != right)),
            "exact_union": input.partition_union == marker.markers,
            "digest": marker.digest,
        },
        "topics": topics,
        "sentinel_topic": input.sentinel_topic,
        "predecessor_topic_absent": !topics.contains(&predecessor_topic),
        "predecessor_delete_count": input.delete_observation.exact_delete_calls,
        "operation_elapsed_ms": input.operation_elapsed_ms,
    });
    std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap())
        .expect("write split evidence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_split_two_successor_foundation() {
    if std::env::var_os("CRABKA_G8_SPLIT_FOUNDATION").is_none() {
        return;
    }
    let SplitFoundationSetup {
        operation_started,
        operation_id,
        mut system,
        sentinel_topic,
        acknowledged,
    } = prepare_split_foundation().await;
    initiate_split_with_cli(&system, &operation_id, 51, 16).await;
    let (completed, _, marker_observations, delete_observation) = tokio::time::timeout(
        Duration::from_secs(90),
        drive_operation(&mut system, &operation_id, Duration::from_secs(90), None),
    )
    .await
    .expect("whole Split operation deadline");
    assert_eq!(completed.phase, SplitOperationPhase::Completed);
    assert_eq!(
        delete_observation,
        RetirementDeleteLedger {
            exact_delete_calls: 1,
            requested_topics: vec![format!("__gres_wal.{}.r1", system.tenant())],
            unrelated_delete_attempted: false,
            injected_after_delete_errors: 0,
        }
    );
    let VerifiedSplitMarkers {
        observation: marker_observation,
        journal_revision,
        journal_digest,
        partition_union,
    } = verify_split_markers(&completed, &operation_id, &marker_observations);
    let tenant = load_tenant(system.bootstrap(), system.tenant()).await;
    assert_eq!(
        tenant
            .ranges
            .iter()
            .map(|range| range.range_id)
            .collect::<Vec<_>>(),
        vec![0, 2, 3]
    );
    let retirement = tenant
        .range_retirements
        .iter()
        .find(|retirement| retirement.operation_id == operation_id)
        .expect("split retirement sidecar");
    assert_eq!(retirement.phase, RangeRetirementPhase::Parked);
    assert_eq!(
        completed.evidence.marker_digest.as_deref(),
        Some(retirement.checkpoint.marker_digest.as_str())
    );

    system.restart_with_hosted_ranges(0, "r0,r2,r3").await;
    let plan = completed
        .plan
        .as_ref()
        .expect("sealed Split operation plan");
    let successor_interval = |range_id| {
        let index = plan
            .target_layout
            .iter()
            .position(|range| range.range_id == range_id)
            .unwrap_or_else(|| panic!("r{range_id} in sealed target layout"));
        let start = index
            .checked_sub(1)
            .and_then(|previous| plan.target_layout[previous].end_key);
        let end = plan.target_layout[index].end_key;
        assert!(start.is_none_or(|key| key.table_id <= plan.routing_table_id));
        assert!(end.is_none_or(|key| key.table_id >= plan.routing_table_id));
        (
            start.and_then(|key| (key.table_id == plan.routing_table_id).then_some(key.rowid)),
            end.and_then(|key| (key.table_id == plan.routing_table_id).then_some(key.rowid)),
        )
    };
    let r2_interval = successor_interval(2);
    let r3_interval = successor_interval(3);

    eprintln!(
        "sealed routing_table_id={}, r2 interval={r2_interval:?}, r3 interval={r3_interval:?}",
        plan.routing_table_id
    );

    let r2_rows = direct_successor_rows(
        &system,
        2,
        plan.routing_table_id,
        r2_interval.0,
        r2_interval.1,
    )
    .await;
    let r3_rows = direct_successor_rows(
        &system,
        3,
        plan.routing_table_id,
        r3_interval.0,
        r3_interval.1,
    )
    .await;
    assert!(!r2_rows.is_empty() && !r3_rows.is_empty());
    assert!(r2_rows.iter().all(|row| {
        r2_interval.0.is_none_or(|start| row.rowid >= start)
            && r2_interval.1.is_none_or(|end| row.rowid < end)
    }));
    assert!(r3_rows.iter().all(|row| {
        r3_interval.0.is_none_or(|start| row.rowid >= start)
            && r3_interval.1.is_none_or(|end| row.rowid < end)
    }));
    let mut successor_rowids = r2_rows
        .iter()
        .chain(&r3_rows)
        .map(|row| row.rowid)
        .collect::<Vec<_>>();
    successor_rowids.sort_unstable();

    assert_eq!(successor_rowids, (1..33_u64).collect::<Vec<_>>());
    let expected_r2 = acknowledged
        .iter()
        .filter(|row| row.rowid < 16)
        .cloned()
        .collect::<Vec<_>>();
    let expected_r3 = acknowledged
        .iter()
        .filter(|row| row.rowid >= 16)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(r2_rows, expected_r2);
    assert_eq!(r3_rows, expected_r3);
    let fresh = system.sql(0).await;
    let full_sequences = fresh
        .query(
            "SELECT seq, route_key, checksum FROM live_ledger51 ORDER BY seq",
            &[],
        )
        .await
        .expect("fresh full split ledger scan")
        .into_iter()
        .enumerate()
        .map(|(index, row)| SplitLedgerRow {
            rowid: u64::try_from(index).unwrap() + 1,
            seq: row.get(0),
            route_key: row.get(1),
            checksum: row.get(2),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        full_sequences,
        acknowledged,
        "post-restart SQL scatter log:\n{}",
        system.log(0)
    );

    write_split_foundation_evidence(SplitFoundationEvidence {
        system: &system,
        operation_id: &operation_id,
        sentinel_topic: &sentinel_topic,
        completed: &completed,
        tenant: &tenant,
        r2_rows: &r2_rows,
        r3_rows: &r3_rows,
        acknowledged: &acknowledged,
        full_sequences: &full_sequences,
        marker_observation: &marker_observation,
        journal_revision,
        journal_digest: &journal_digest,
        partition_union: &partition_union,
        delete_observation: &delete_observation,
        operation_elapsed_ms: operation_started.elapsed().as_millis(),
    })
    .await;
    system.shutdown().await;
}

struct PreparedMoveNemesis {
    system: ProcessHarness,
    operation_id: String,
    sentinel_topic: String,
    _workload_root: tempfile::TempDir,
    ledger_path: PathBuf,
    workload_error_path: PathBuf,
    workload: WorkloadChild,
}

async fn prepare_move_nemesis(kill_point: SourceKillPoint) -> PreparedMoveNemesis {
    let operation_id = format!(
        "g8-{}-{:x}-p{:x}",
        kill_point.name().replace('_', "-"),
        timestamp_ms(),
        std::process::id()
    );
    let system = ProcessHarness::start_all_on_zero(&format!("tenant-{operation_id}")).await;
    assert!(
        Registry::connect(system.bootstrap())
            .await
            .expect("identity registry")
            .load_split_operation(system.tenant(), &operation_id)
            .await
            .expect("check unique operation")
            .is_none(),
        "process nemesis operation identity must be globally fresh"
    );
    let sentinel_topic = format!("__gres_g8_retirement_sentinel.{}", system.tenant());
    let mut sentinel_admin =
        crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
            .await
            .expect("sentinel admin");
    let outcomes = sentinel_admin
        .create_topics(
            &[crabka_client_admin::CreateTopicSpec {
                name: sentinel_topic.clone(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            30_000,
        )
        .await
        .expect("create sentinel topic");
    assert!(outcomes.iter().all(|outcome| outcome.error.is_none()));
    let mut ddl = String::new();
    for table in 1..50 {
        write!(&mut ddl, "CREATE TABLE filler_{table} (id int4);").expect("write DDL to string");
    }
    ddl.push_str("CREATE TABLE live_ledger (id int4, checksum text NOT NULL) SHARDED");
    system
        .sql(0)
        .await
        .simple_query(&ddl)
        .await
        .expect("create sharded workload ledger");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let workload_root = tempfile::tempdir().expect("workload root");
    let ledger_path = workload_root.path().join("acks.jsonl");
    let workload_error_path = workload_root.path().join("workload-errors.log");
    let response_loss_path = workload_root.path().join("response-loss-injected");
    let stop_path = workload_root.path().join("stop");
    let script = r#"
set -u
seq=0
while [[ ! -e "$CRABKA_G8_WORKLOAD_STOP" ]]; do
  checksum=$(printf 'g8-%016x' "$seq")
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  printf '{"kind":"attempt","seq":%s,"timestamp_ms":%s}\n' "$seq" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
  sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
  # The client timeout must exceed every observed-safe ack-gap bound below:
  # abandoning a statement the server may still commit is what creates
  # unresolvable ambiguity, so a statement is only abandoned once the run has
  # already blown its liveness bound. Connection-phase failures stay fast via
  # PGCONNECT_TIMEOUT.
  if timeout 25s psql -X -q -v ON_ERROR_STOP=1 -c "INSERT INTO live_ledger (id, checksum) VALUES ($seq, '$checksum')" >/dev/null 2>>"$CRABKA_G8_WORKLOAD_ERRORS"; then
    if [[ "$seq" -eq 2 && ! -e "$CRABKA_G8_RESPONSE_LOSS" ]]; then touch "$CRABKA_G8_RESPONSE_LOSS"; response_known=false; else response_known=true; fi
  else response_known=false; fi
  if [[ "$response_known" == true ]]; then kind=ack; else
    # Ambiguous outcome: the attempt may still commit server-side. Resolve by
    # polling the row; a read only counts when psql SUCCEEDS, and absence is
    # concluded only after a sustained streak of healthy empty reads, so a
    # still-parked attempt has become visible (or died with its process)
    # before any re-INSERT.
    kind=""
    empty_streak_start=""
    while [[ ! -e "$CRABKA_G8_WORKLOAD_STOP" ]]; do
      if actual=$(timeout 5s psql -X -A -t -q -v ON_ERROR_STOP=1 -c "SELECT checksum FROM live_ledger WHERE id = $seq" 2>>"$CRABKA_G8_WORKLOAD_ERRORS"); then
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
      printf '{"kind":"retry","seq":%s,"timestamp_ms":%s}\n' "$seq" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
      sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
      continue
    fi
  fi
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  printf '{"kind":"%s","seq":%s,"timestamp_ms":%s}\n' "$kind" "$seq" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
  sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
  seq=$((seq + 1)); sleep 0.02
done
"#;
    let mut command = tokio::process::Command::new("bash");
    command
        .args(["-c", script])
        .env("CRABKA_G8_WORKLOAD_LEDGER", &ledger_path)
        .env("CRABKA_G8_WORKLOAD_STOP", &stop_path)
        .env("CRABKA_G8_WORKLOAD_ERRORS", &workload_error_path)
        .env("CRABKA_G8_RESPONSE_LOSS", &response_loss_path)
        .env("PGHOST", "127.0.0.1")
        .env("PGCONNECT_TIMEOUT", "3")
        .env("PGPORT", system.stable_sql_port().to_string())
        .env("PGUSER", "alice")
        .env("PGPASSWORD", process::fixture_password())
        .env("PGDATABASE", system.tenant())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let child = command.spawn().expect("spawn real workload child");
    let process_group = child.id().expect("workload child pid");
    assert!(process_group_exists(process_group));
    PreparedMoveNemesis {
        system,
        operation_id,
        sentinel_topic,
        _workload_root: workload_root,
        ledger_path,
        workload_error_path,
        workload: WorkloadChild {
            child,
            process_group,
            stop_path,
            stopped: false,
        },
    }
}

struct MoveTerminalTopology {
    topic_names: std::collections::BTreeSet<String>,
    delete_ledger: RetirementDeleteLedger,
}

async fn verify_move_terminal_topology(
    system: &ProcessHarness,
    operation_id: &str,
    completed: &SplitOperationRecord,
    restart: &KillObservation,
    kill_point: SourceKillPoint,
    sentinel_topic: &str,
) -> MoveTerminalTopology {
    let durable_tenant = load_tenant(system.bootstrap(), system.tenant()).await;
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
        .find(|retirement| retirement.operation_id == operation_id)
        .expect("exact retirement sidecar");
    assert_eq!(retirement.phase, RangeRetirementPhase::Parked);
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
    for topic in [
        format!("__gres_wal.{}.r2.g0000000001", system.tenant()),
        format!("__gres_wal.{}.r0", system.tenant()),
        sentinel_topic.to_owned(),
    ] {
        assert!(topic_names.contains(&topic));
    }
    let delete_ledger = restart
        .delete_ledger
        .lock()
        .expect("final retirement delete ledger")
        .clone();
    if matches!(
        kill_point,
        SourceKillPoint::RetiringBeforeDelete
            | SourceKillPoint::RetiringAfterDelete
            | SourceKillPoint::RetiringParked
            | SourceKillPoint::Resuming
    ) {
        assert_eq!(
            delete_ledger,
            RetirementDeleteLedger {
                exact_delete_calls: 1,
                requested_topics: vec![format!("__gres_wal.{}.r1", system.tenant())],
                unrelated_delete_attempted: false,
                injected_after_delete_errors: usize::from(
                    kill_point == SourceKillPoint::RetiringAfterDelete
                ),
            }
        );
        assert_eq!(
            restart.predecessor_topic_present,
            Some(kill_point == SourceKillPoint::RetiringBeforeDelete)
        );
        assert_eq!(
            delete_ledger.exact_delete_calls - restart.delete_calls_at_kill,
            usize::from(kill_point == SourceKillPoint::RetiringBeforeDelete)
        );
    }
    MoveTerminalTopology {
        topic_names,
        delete_ledger,
    }
}

struct MoveKillEvidence<'a> {
    system: &'a ProcessHarness,
    operation_id: &'a str,
    kill_point: SourceKillPoint,
    restart: &'a KillObservation,
    ledger: &'a AckLedger,
    acknowledgements_at_completion: usize,
    max_observed_safe_ack_gap_ms: u128,
    operation_elapsed_ms: u128,
    delete_ledger: &'a RetirementDeleteLedger,
    sentinel_topic: &'a str,
    topic_names: &'a std::collections::BTreeSet<String>,
    completed: &'a SplitOperationRecord,
}

fn write_move_kill_evidence(input: &MoveKillEvidence<'_>) {
    let Some(path) = std::env::var_os("CRABKA_G8_KILL_EVIDENCE") else {
        return;
    };
    let restart = input.restart;
    let ledger = input.ledger;
    let delete_ledger = input.delete_ledger;
    let evidence = serde_json::json!({
        "operation": "move",
        "tenant_id": input.system.tenant(),
        "operation_id": input.operation_id,
        "kill_point": input.kill_point.name(),
        "completed": true,
        "old_pid": restart.old_pid,
        "new_pid": restart.new_pid,
        "durable_phase": format!("{:?}", restart.phase),
        "durable_evidence": {
            "manifest_key": restart.evidence.manifest_key,
            "covered_offset": restart.evidence.covered_offset,
            "barrier_offset": restart.evidence.barrier_offset,
            "tail_sha256": restart.evidence.tail_sha256,
            "marker_digest": restart.evidence.marker_digest,
        },
        "acknowledged_rows": ledger.acknowledgements.len(),
        "acknowledgements_at_completion": input.acknowledgements_at_completion,
        "post_completed_ack": ledger.acknowledgements.len() > input.acknowledgements_at_completion,
        "recovered_acknowledgements": ledger.recovered,
        "ambiguity_retries": ledger.retries.values().sum::<usize>(),
        "max_ack_gap_ms": ledger.max_ack_gap_ms,
        "max_ack_gap_bound_ms": input.max_observed_safe_ack_gap_ms,
        "max_ack_gap_endpoints": ledger.max_ack_gap_endpoints.map(|(before_seq, before_ms, after_seq, after_ms)| serde_json::json!({
            "before_seq": before_seq,
            "before_ms": before_ms,
            "after_seq": after_seq,
            "after_ms": after_ms,
        })),
        "phase_timestamps_ms": {
            "pre_kill": restart.pre_kill_ms,
            "restart_ready": restart.restart_ms,
            "stage_complete": restart.stage_complete_ms,
            "publication": restart.publication_ms,
            "post_publication_ack": restart.post_publication_ack_ms,
        },
        "operation_elapsed_ms": input.operation_elapsed_ms,
        "predecessor_wal_retired": true,
        "post_publication_ack_before_retirement": restart.cutover.post_publication_ack_before_retirement,
        "ambiguous_cutover_advanced_without_republish": restart.cutover.ambiguous_cutover_advanced_without_republish,
        "durable_tenant_layout": restart.tenant_layout,
        "durable_retirement_phase": restart.retirement_phase.map(|phase| format!("{phase:?}")),
        "durable_tenant_record_version": restart.tenant_record_version,
        "predecessor_topic_present_at_kill": restart.predecessor_topic_present,
        "exact_predecessor_delete_calls": delete_ledger.exact_delete_calls,
        "delete_calls_at_kill": restart.delete_calls_at_kill,
        "replay_delete_calls": delete_ledger.exact_delete_calls - restart.delete_calls_at_kill,
        "delete_requested_topics": delete_ledger.requested_topics,
        "unrelated_delete_attempted": delete_ledger.unrelated_delete_attempted,
        "injected_after_delete_errors": delete_ledger.injected_after_delete_errors,
        "retire_receipt_probed_before_kill": restart.retire_receipt_probes.before_kill,
        "retire_receipt_probed_after_restart": restart.retire_receipt_probes.after_restart,
        "sentinel_topic": input.sentinel_topic,
        "sentinel_topic_preserved": input.topic_names.contains(input.sentinel_topic),
        "replacement_owner": {"range_id": 2, "generation": 1},
        "marker_digest": input.completed.evidence.marker_digest,
    });
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&evidence).expect("serialize kill evidence"),
    )
    .expect("write kill evidence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_move_source_phase_sigkill_with_exact_ack_ledger() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    let kill_point = SourceKillPoint::from_env();
    let PreparedMoveNemesis {
        mut system,
        operation_id,
        sentinel_topic,
        _workload_root,
        ledger_path,
        workload_error_path,
        mut workload,
    } = prepare_move_nemesis(kill_point).await;
    let case = async {
        wait_for_ack_count(&ledger_path, 8).await;
        initiate_move_with_cli(&system, &operation_id).await;
        assert_eq!(
            load_operation(system.bootstrap(), system.tenant(), &operation_id)
                .await
                .phase,
            SplitOperationPhase::Initiated,
            "CLI must only initiate the fresh operation"
        );
        let operation_started = Instant::now();
        let drive_result = tokio::time::timeout(
            Duration::from_secs(75),
            drive_operation(
                &mut system,
                &operation_id,
                Duration::from_secs(75),
                Some(KillInjection {
                    ledger_path: &ledger_path,
                    point: kill_point,
                }),
            ),
        )
        .await;
        let (completed, restart, _, _) = drive_result.unwrap_or_else(|_| {
            panic!(
                "whole {} Move operation deadline; source log: {}",
                kill_point.name(),
                system.log(0)
            )
        });
        assert_eq!(completed.phase, SplitOperationPhase::Completed);
        let acknowledgements_at_completion = parse_ack_ledger(
            &std::fs::read_to_string(&ledger_path).expect("live acknowledgement ledger"),
        )
        .expect("valid live acknowledgement ledger")
        .acknowledgements
        .len();
        wait_for_ack_count(&ledger_path, acknowledgements_at_completion + 1).await;
        (
            completed,
            restart,
            operation_started.elapsed().as_millis(),
            acknowledgements_at_completion,
        )
    };
    let case = Box::pin(run_with_workload_cleanup(&mut workload, case)).await;
    let Ok((completed, restart, operation_elapsed_ms, acknowledgements_at_completion)) = case
    else {
        panic!(
            "workload case failed; psql errors: {}",
            std::fs::read_to_string(&workload_error_path).unwrap_or_default()
        );
    };
    let restart = restart.expect("configured source-phase SIGKILL occurred");
    let restart_ms = restart.restart_ms;
    assert!(
        restart.cutover.post_publication_ack_before_retirement,
        "a successor-bound write must commit after publication while retirement is pending"
    );
    if kill_point == SourceKillPoint::ActivatedAfterTenantCas {
        assert!(
            restart.cutover.ambiguous_cutover_advanced_without_republish,
            "tenant-CAS ambiguity must advance only the journal"
        );
    }

    let ledger = parse_ack_ledger(
        &std::fs::read_to_string(&ledger_path).expect("read final acknowledgement ledger"),
    )
    .expect("valid final acknowledgement ledger");
    assert!(
        ledger.acknowledgements.len() > acknowledgements_at_completion,
        "at least one write must be acknowledged after Completed is durable"
    );
    let max_observed_safe_ack_gap_ms = WORKLOAD_AMBIGUITY_RESOLUTION_MS
        + match kill_point {
            SourceKillPoint::PausedBeforeStage
            | SourceKillPoint::PausedAfterStage
            | SourceKillPoint::Restored => 20_000,
            SourceKillPoint::Running | SourceKillPoint::Checkpointed => 15_000,
            SourceKillPoint::ActivatedBeforeCutover
            | SourceKillPoint::ActivatedAfterTenantCas
            | SourceKillPoint::LayoutPublished
            | SourceKillPoint::RetiringBeforeDelete
            | SourceKillPoint::RetiringAfterDelete
            | SourceKillPoint::RetiringParked
            | SourceKillPoint::Resuming => 12_000,
        };
    assert!(
        ledger.recovered >= 1,
        "deterministic response loss must recover an ACK"
    );
    assert!(
        ledger.max_ack_gap_ms <= max_observed_safe_ack_gap_ms,
        "maximum acknowledged-write gap {}ms exceeded observed-safe {}ms bound",
        ledger.max_ack_gap_ms,
        max_observed_safe_ack_gap_ms
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
        .unwrap_or_else(|error| {
            panic!(
                "read final workload ledger: {error}; source log: {}",
                system.log(0)
            )
        })
        .into_iter()
        .map(|row| (i64::from(row.get::<_, i32>(0)), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    {
        use assert2::assert;
        assert!(
            rows == expected,
            "database rows must exactly equal durable ACK ledger; {}",
            describe_ledger_mismatch(&rows, &expected, &ledger)
        );
    }

    let MoveTerminalTopology {
        topic_names,
        delete_ledger,
    } = verify_move_terminal_topology(
        &system,
        &operation_id,
        &completed,
        &restart,
        kill_point,
        &sentinel_topic,
    )
    .await;

    write_move_kill_evidence(&MoveKillEvidence {
        system: &system,
        operation_id: &operation_id,
        kill_point,
        restart: &restart,
        ledger: &ledger,
        acknowledgements_at_completion,
        max_observed_safe_ack_gap_ms,
        operation_elapsed_ms,
        delete_ledger: &delete_ledger,
        sentinel_topic: &sentinel_topic,
        topic_names: &topic_names,
        completed: &completed,
    });
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

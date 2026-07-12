use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PayloadKind {
    Attempt,
    Ack,
    RecoveredAck,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PayloadEvent {
    kind: PayloadKind,
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

fn successor_partition(table_id: u64, rowid: u64) -> Result<u32, String> {
    match (table_id, rowid) {
        (50, 16..) => Ok(2),
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
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  kind=attempt
  printf '{"kind":"%s","table_id":%s,"rowid":null,"seq":%s,"checksum":"%s","timestamp_ms":%s}\n' \
    "$kind" "$table_id" "$seq" "$checksum" "$now" >> "$CRABKA_G8_WORKLOAD_LEDGER"
  sync -d "$CRABKA_G8_WORKLOAD_LEDGER"
  if timeout 2s psql -X -q -v ON_ERROR_STOP=1 \
      -c "INSERT INTO $table_name (seq, checksum) VALUES ($seq, '$checksum')" \
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
    actual=$(timeout 2s psql -X -A -t -q -v ON_ERROR_STOP=1 \
      -c "SELECT checksum FROM $table_name WHERE seq = $seq" \
      2>>"$CRABKA_G8_WORKLOAD_ERRORS" || true)
    [[ "$actual" == "$checksum" ]] || continue
    kind=recovered_ack
  fi
  now_raw=$(date +%s%N); now=$((now_raw / 1000000))
  printf '{"kind":"%s","table_id":%s,"rowid":%s,"seq":%s,"checksum":"%s","timestamp_ms":%s}\n' \
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
    assert!(!process_group_exists(process_group));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Family {
    SourceRestore,
    Publication,
    RetirementResume,
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
            Family::SourceRestore => 20_000,
            Family::Publication | Family::RetirementResume => 12_000,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Receipt {
    None,
    Checkpoint,
    Pause,
    Stage,
    Marker,
    Prologue,
    Retire,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    Current,
    Target,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Sidecar {
    None,
    Parking,
    Parked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
            receipt: Receipt::None,
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
                let mut state = Self::source(Phase::Restored, Receipt::Prologue, complete);
                state.successors_serving = true;
                state
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
fn payload_ledger_parses_attempt_ack_and_recovered_ack() {
    let parsed = parse_payload_ledger(
        r#"{"kind":"attempt","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"attempt","table_id":51,"rowid":null,"seq":2,"checksum":"b","timestamp_ms":18}
{"kind":"recovered_ack","table_id":51,"rowid":16,"seq":2,"checksum":"b","timestamp_ms":20}
"#,
    )
    .expect("strict payload ledger");
    assert_eq!(parsed.attempts.len(), 2);
    assert_eq!(parsed.acknowledged.len(), 2);
    assert_eq!(parsed.recovered, 1);
    assert_eq!(parsed.max_ack_gap_ms, 8);
}

#[test]
fn payload_ledger_rejects_incomplete_or_inconsistent_events() {
    assert!(parse_payload_ledger(r#"{"kind":"ack"}"#).is_err());
    assert!(
        parse_payload_ledger(
            r#"{"kind":"ack","table_id":50,"rowid":1,"seq":1,"checksum":"a","timestamp_ms":10}"#,
        )
        .is_err()
    );
}

#[test]
fn payload_ledger_projects_each_table_to_its_sealed_successor() {
    assert_eq!(successor_partition(50, 20), Ok(2));
    assert_eq!(successor_partition(51, 16), Ok(3));
    assert_eq!(successor_partition(51, 32), Ok(3));
    assert!(successor_partition(51, 15).is_err());
    assert!(successor_partition(52, 1).is_err());
}

#[test]
fn payload_ledger_rejects_duplicate_ack_checksum_and_time_regression() {
    let duplicate = r#"{"kind":"attempt","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"ack","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":13}"#;
    assert!(parse_payload_ledger(duplicate).is_err());
    let checksum = r#"{"kind":"attempt","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","table_id":50,"rowid":7,"seq":1,"checksum":"b","timestamp_ms":12}"#;
    assert!(parse_payload_ledger(checksum).is_err());
    let regression = r#"{"kind":"attempt","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"ack","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":10}"#;
    assert!(parse_payload_ledger(regression).is_err());
}

#[test]
fn payload_ledger_is_parsed_only_after_fsync_and_close() {
    let mut file = tempfile::NamedTempFile::new().expect("payload ledger");
    file.write_all(
        br#"{"kind":"attempt","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","table_id":50,"rowid":16,"seq":1,"checksum":"a","timestamp_ms":12}
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

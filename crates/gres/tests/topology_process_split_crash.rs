use std::collections::BTreeSet;

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

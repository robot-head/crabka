#![allow(
    clippy::bool_assert_comparison,
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::struct_excessive_bools
)]

mod crossrange_2pc_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        prepared_versions: u8,
        committed: bool,
        steps: u8,
    }

    struct CrossRangeTwoPcModel {
        stage_idempotent: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        Stage,
        CommitGlobal,
    }

    impl Model for CrossRangeTwoPcModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                prepared_versions: 0,
                committed: false,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 4 {
                return;
            }
            out.extend([Action::Stage, Action::CommitGlobal]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            match action {
                Action::Stage => {
                    if self.stage_idempotent && next.prepared_versions > 0 {
                        return None;
                    }
                    next.prepared_versions += 1;
                }
                Action::CommitGlobal => {
                    if next.prepared_versions == 0 || next.committed {
                        return None;
                    }
                    next.committed = true;
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "one prepared global xid creates at most one committed version",
                |_, state| !state.committed || state.prepared_versions <= 1,
            )]
        }
    }

    #[test]
    fn stage_idempotency_keeps_one_live_version() {
        let checker = CrossRangeTwoPcModel {
            stage_idempotent: true,
        }
        .checker()
        .spawn_bfs()
        .join();

        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    #[test]
    fn non_idempotent_stage_has_teeth() {
        let checker = CrossRangeTwoPcModel {
            stage_idempotent: false,
        }
        .checker()
        .spawn_bfs()
        .join();

        assert!(
            checker
                .discoveries()
                .contains_key("one prepared global xid creates at most one committed version")
        );
    }
}

mod crossrange_2pc_abort_atomicity_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        decision: Option<bool>,
        participant_committed: bool,
        steps: u8,
    }

    struct AbortAtomicityModel {
        abort_is_final: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        DecideAbort,
        DecideCommit,
        ApplyParticipantCommit,
    }

    impl Model for AbortAtomicityModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                decision: None,
                participant_committed: false,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 4 {
                return;
            }
            out.extend([
                Action::DecideAbort,
                Action::DecideCommit,
                Action::ApplyParticipantCommit,
            ]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            match action {
                Action::DecideAbort => {
                    if next.decision.is_some() {
                        return None;
                    }
                    next.decision = Some(false);
                }
                Action::DecideCommit => {
                    if next.decision.is_some() {
                        return None;
                    }
                    next.decision = Some(true);
                }
                Action::ApplyParticipantCommit => {
                    if self.abort_is_final && next.decision != Some(true) {
                        return None;
                    }
                    next.participant_committed = true;
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "abort decision is atomic across participants",
                |_, state| state.decision != Some(false) || !state.participant_committed,
            )]
        }
    }

    #[test]
    fn abort_decision_prevents_participant_commit() {
        let checker = AbortAtomicityModel {
            abort_is_final: true,
        }
        .checker()
        .spawn_bfs()
        .join();

        checker.assert_properties();
    }

    #[test]
    fn ignored_abort_decision_is_caught() {
        let checker = AbortAtomicityModel {
            abort_is_final: false,
        }
        .checker()
        .spawn_bfs()
        .join();

        assert!(
            checker
                .discoveries()
                .contains_key("abort decision is atomic across participants")
        );
    }
}

mod crossrange_2pc_gtm_reuse_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        fenced: bool,
        replayed_next: Option<u8>,
        durable_next: u8,
        mem_next: u8,
        gate_open: bool,
        live_versions: Vec<u8>,
        steps: u8,
    }

    struct GtmReuseModel {
        fence_first: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        Fence,
        ReadEnd,
        ZombieAppendAfterEndRead,
        ReseedAndOpenGate,
        AllocateAndCommit,
    }

    impl State {
        fn has_reused_live_global_xid(&self) -> bool {
            let mut versions = self.live_versions.clone();
            versions.sort_unstable();
            versions.windows(2).any(|window| window[0] == window[1])
        }
    }

    impl Model for GtmReuseModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                fenced: false,
                replayed_next: None,
                durable_next: 1,
                mem_next: 1,
                gate_open: false,
                live_versions: Vec::new(),
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 5 {
                return;
            }
            out.extend([
                Action::Fence,
                Action::ReadEnd,
                Action::ZombieAppendAfterEndRead,
                Action::ReseedAndOpenGate,
                Action::AllocateAndCommit,
            ]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            match action {
                Action::Fence => {
                    if next.fenced {
                        return None;
                    }
                    next.fenced = true;
                }
                Action::ReadEnd => {
                    if self.fence_first && !next.fenced {
                        return None;
                    }
                    if next.replayed_next.is_some() {
                        return None;
                    }
                    next.replayed_next = Some(next.durable_next);
                }
                Action::ZombieAppendAfterEndRead => {
                    if next.replayed_next.is_none() || next.fenced {
                        return None;
                    }
                    let zombie_g = next.durable_next;
                    next.live_versions.push(zombie_g);
                    next.durable_next += 1;
                }
                Action::ReseedAndOpenGate => {
                    let replayed_next = next.replayed_next?;
                    if self.fence_first && !next.fenced {
                        return None;
                    }
                    if next.gate_open {
                        return None;
                    }
                    next.mem_next = replayed_next;
                    next.gate_open = true;
                }
                Action::AllocateAndCommit => {
                    if !next.gate_open {
                        return None;
                    }
                    next.live_versions.push(next.mem_next);
                    next.mem_next += 1;
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "no reused global xid produces two live versions",
                |_, state| !state.has_reused_live_global_xid(),
            )]
        }
    }

    #[test]
    fn fence_first_prevents_zombie_gtm_reuse() {
        let checker = GtmReuseModel { fence_first: true }
            .checker()
            .spawn_bfs()
            .join();

        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    #[test]
    fn end_read_before_fence_reuses_global_xid_and_double_lives() {
        let checker = GtmReuseModel { fence_first: false }
            .checker()
            .spawn_bfs()
            .join();

        assert!(
            checker
                .discoveries()
                .contains_key("no reused global xid produces two live versions")
        );
    }
}

mod crossrange_2pc_settle_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        fenced: bool,
        end_read: bool,
        prepared_durable: bool,
        prepared_replayed: bool,
        gate_open: bool,
        in_doubt_row: bool,
        steps: u8,
    }

    struct SettleModel {
        fence_first: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        Fence,
        ReadEnd,
        ZombieAppendAfterEndRead,
        SettleAndOpenGate,
    }

    impl Model for SettleModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                fenced: false,
                end_read: false,
                prepared_durable: false,
                prepared_replayed: false,
                gate_open: false,
                in_doubt_row: false,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 4 {
                return;
            }
            out.extend([
                Action::Fence,
                Action::ReadEnd,
                Action::ZombieAppendAfterEndRead,
                Action::SettleAndOpenGate,
            ]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            match action {
                Action::Fence => {
                    if next.fenced {
                        return None;
                    }
                    next.fenced = true;
                }
                Action::ReadEnd => {
                    if self.fence_first && !next.fenced {
                        return None;
                    }
                    if next.end_read {
                        return None;
                    }
                    next.prepared_replayed = next.prepared_durable;
                    next.end_read = true;
                }
                Action::ZombieAppendAfterEndRead => {
                    if !next.end_read || next.fenced || next.prepared_durable {
                        return None;
                    }
                    next.prepared_durable = true;
                    next.in_doubt_row = true;
                }
                Action::SettleAndOpenGate => {
                    if !next.end_read || next.gate_open {
                        return None;
                    }
                    if next.prepared_replayed {
                        next.in_doubt_row = false;
                    }
                    next.gate_open = true;
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "gate never opens with an in-doubt row",
                |_, state| !state.gate_open || !state.in_doubt_row,
            )]
        }
    }

    #[test]
    fn fence_first_settles_every_prepared_marker_before_gate_opens() {
        let checker = SettleModel { fence_first: true }
            .checker()
            .spawn_bfs()
            .join();

        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    #[test]
    fn end_read_before_fence_misses_prepared_marker_and_opens_gate() {
        let checker = SettleModel { fence_first: false }
            .checker()
            .spawn_bfs()
            .join();

        assert!(
            checker
                .discoveries()
                .contains_key("gate never opens with an in-doubt row")
        );
    }
}

mod crossrange_2pc_overlap_settle_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        settled_a: bool,
        settled_b: bool,
        gate_open: bool,
        steps: u8,
    }

    struct OverlapSettleModel {
        require_all_markers: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        SettleA,
        SettleB,
        OpenGate,
    }

    impl Model for OverlapSettleModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                settled_a: false,
                settled_b: false,
                gate_open: false,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 3 {
                return;
            }
            out.extend([Action::SettleA, Action::SettleB, Action::OpenGate]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            match action {
                Action::SettleA => next.settled_a = true,
                Action::SettleB => next.settled_b = true,
                Action::OpenGate => {
                    if next.gate_open {
                        return None;
                    }
                    if self.require_all_markers && !(next.settled_a && next.settled_b) {
                        return None;
                    }
                    next.gate_open = true;
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "overlap settle resolves every marker before serving",
                |_, state| !state.gate_open || (state.settled_a && state.settled_b),
            )]
        }
    }

    #[test]
    fn overlap_settle_waits_for_all_markers() {
        let checker = OverlapSettleModel {
            require_all_markers: true,
        }
        .checker()
        .spawn_bfs()
        .join();

        checker.assert_properties();
    }

    #[test]
    fn partial_overlap_settle_is_caught() {
        let checker = OverlapSettleModel {
            require_all_markers: false,
        }
        .checker()
        .spawn_bfs()
        .join();

        assert!(
            checker
                .discoveries()
                .contains_key("overlap settle resolves every marker before serving")
        );
    }
}

mod write_once_decision_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        first_decision: Option<bool>,
        decision: Option<bool>,
        steps: u8,
    }

    struct WriteOnceDecisionModel {
        write_once: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        Commit,
        Abort,
    }

    impl Model for WriteOnceDecisionModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                first_decision: None,
                decision: None,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 2 {
                return;
            }
            out.extend([Action::Commit, Action::Abort]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            let decision = matches!(action, Action::Commit);
            if next.first_decision.is_none() {
                next.first_decision = Some(decision);
            }
            if self.write_once && next.decision.is_some() {
                return None;
            }
            next.decision = Some(decision);
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "first global decision is immutable",
                |_, state| state.first_decision.is_none() || state.decision == state.first_decision,
            )]
        }
    }

    #[test]
    fn first_decision_wins() {
        let checker = WriteOnceDecisionModel { write_once: true }
            .checker()
            .spawn_bfs()
            .join();

        checker.assert_properties();
    }

    #[test]
    fn overwritten_decision_is_caught() {
        let checker = WriteOnceDecisionModel { write_once: false }
            .checker()
            .spawn_bfs()
            .join();

        assert!(
            checker
                .discoveries()
                .contains_key("first global decision is immutable")
        );
    }
}

mod mvcc_write_conflict_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        writer_a_staged: bool,
        writer_b_staged: bool,
        writer_a_committed: bool,
        writer_b_committed: bool,
        steps: u8,
    }

    struct MvccWriteConflictModel {
        lock_conflicts: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        StageA,
        StageB,
        CommitA,
        CommitB,
    }

    impl State {
        fn live_count(&self) -> u8 {
            u8::from(self.writer_a_committed) + u8::from(self.writer_b_committed)
        }
    }

    impl Model for MvccWriteConflictModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                writer_a_staged: false,
                writer_b_staged: false,
                writer_a_committed: false,
                writer_b_committed: false,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 4 {
                return;
            }
            out.extend([
                Action::StageA,
                Action::StageB,
                Action::CommitA,
                Action::CommitB,
            ]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            match action {
                Action::StageA => {
                    if next.writer_a_staged || (self.lock_conflicts && next.writer_b_staged) {
                        return None;
                    }
                    next.writer_a_staged = true;
                }
                Action::StageB => {
                    if next.writer_b_staged || (self.lock_conflicts && next.writer_a_staged) {
                        return None;
                    }
                    next.writer_b_staged = true;
                }
                Action::CommitA => {
                    if !next.writer_a_staged || next.writer_a_committed {
                        return None;
                    }
                    next.writer_a_committed = true;
                }
                Action::CommitB => {
                    if !next.writer_b_staged || next.writer_b_committed {
                        return None;
                    }
                    next.writer_b_committed = true;
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "conflicting writers leave at most one live version",
                |_, state| state.live_count() <= 1,
            )]
        }
    }

    #[test]
    fn write_conflict_blocks_second_live_version() {
        let checker = MvccWriteConflictModel {
            lock_conflicts: true,
        }
        .checker()
        .spawn_bfs()
        .join();

        checker.assert_properties();
    }

    #[test]
    fn missing_write_conflict_is_caught() {
        let checker = MvccWriteConflictModel {
            lock_conflicts: false,
        }
        .checker()
        .spawn_bfs()
        .join();

        assert!(
            checker
                .discoveries()
                .contains_key("conflicting writers leave at most one live version")
        );
    }
}

mod recovery_watermark_model {
    use stateright::{Checker, Model, Property};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        produced_prepared: u8,
        applied_prepared: u8,
        gate_open: bool,
        steps: u8,
    }

    struct RecoveryWatermarkModel {
        require_watermark: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        AppendPrepared,
        ReplayPrepared,
        OpenGate,
    }

    impl Model for RecoveryWatermarkModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                produced_prepared: 0,
                applied_prepared: 0,
                gate_open: false,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= 4 {
                return;
            }
            out.extend([
                Action::AppendPrepared,
                Action::ReplayPrepared,
                Action::OpenGate,
            ]);
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps += 1;
            match action {
                Action::AppendPrepared => {
                    if next.gate_open {
                        return None;
                    }
                    next.produced_prepared += 1;
                }
                Action::ReplayPrepared => {
                    if next.applied_prepared == next.produced_prepared {
                        return None;
                    }
                    next.applied_prepared += 1;
                }
                Action::OpenGate => {
                    if next.gate_open {
                        return None;
                    }
                    if self.require_watermark && next.applied_prepared != next.produced_prepared {
                        return None;
                    }
                    next.gate_open = true;
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![Property::<Self>::always(
                "serving gate waits for the recovery watermark",
                |_, state| !state.gate_open || state.applied_prepared == state.produced_prepared,
            )]
        }
    }

    #[test]
    fn gate_waits_for_recovery_watermark() {
        let checker = RecoveryWatermarkModel {
            require_watermark: true,
        }
        .checker()
        .spawn_bfs()
        .join();

        checker.assert_properties();
    }

    #[test]
    fn early_gate_before_watermark_is_caught() {
        let checker = RecoveryWatermarkModel {
            require_watermark: false,
        }
        .checker()
        .spawn_bfs()
        .join();

        assert!(
            checker
                .discoveries()
                .contains_key("serving gate waits for the recovery watermark")
        );
    }
}

/// Two ranges, each committing single-shard transactions from its own local
/// sequence and publishing a closed timestamp, plus a cross-range snapshot read
/// that reconciles against those watermarks.
///
/// This is the safety proof for the single-shard commit bypass. The instant a
/// commit is stamped from a per-range local sequence rather than the global
/// timestamp source, a cross-range read at `read_ts` could miss it unless the
/// read waits for the range's published closed timestamp to reach `read_ts`.
/// The model encodes the two primitive guarantees the real `LocalSequence`
/// enforces — a commit is allocated from `next` (monotone), and a publish
/// reserves `next` strictly above the target before raising the watermark, so
/// the sequence invariant `next > closed` holds for all time — and checks that
/// reconciling reads never miss a qualifying commit. Flipping the reconcile
/// knob off reintroduces the linearizability bug and the checker witnesses it.
mod single_shard_closed_timestamp_model {
    use stateright::{Checker, Model, Property};

    /// Highest timestamp a range's local sequence will hand out in the model.
    /// Two ranges each committing/publishing up to this bound is enough to reach
    /// the boundary interleaving (read, then a later commit at or below
    /// `read_ts`) that the closed-timestamp discipline must forbid.
    const MAX_TS: u8 = 3;
    /// Exploration-depth guard keeping the state space finite and small.
    const MAX_STEPS: u8 = 10;

    /// One range's local sequence: `next` is the next allocatable timestamp,
    /// `closed` the published closed-timestamp watermark, and `commits` the
    /// commit timestamps of single-shard transactions already durable on it.
    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct Sequence {
        next: u8,
        closed: u8,
        commits: Vec<u8>,
    }

    impl Sequence {
        fn new() -> Self {
            // Seeded at horizon 0: first allocation is 1, watermark starts at 0.
            Self {
                next: 1,
                closed: 0,
                commits: Vec::new(),
            }
        }

        /// Allocate one commit timestamp (mirrors `allocate_transaction_id` /
        /// `allocate_commit_after` collapsed to a single monotone tick).
        fn commit(&mut self) -> Option<u8> {
            if self.next > MAX_TS {
                return None;
            }
            let commit_ts = self.next;
            self.next = self.next.checked_add(1)?;
            self.commits.push(commit_ts);
            Some(commit_ts)
        }

        /// Raise the closed timestamp toward `target`, reserving `next` strictly
        /// above it first (mirrors `publish_closed_timestamp`). Preserves the
        /// `next > closed` invariant the whole bypass rests on.
        fn publish(&mut self, target: u8) -> Option<()> {
            self.next = self.next.max(target.checked_add(1)?);
            self.closed = self.closed.max(target);
            Some(())
        }
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct State {
        a: Sequence,
        b: Sequence,
        /// The cross-range read's snapshot timestamp; `0` before it is chosen.
        read_ts: u8,
        /// Whether the read has already observed range A / range B.
        read_a: bool,
        read_b: bool,
        /// A commit with `commit_ts <= read_ts` became durable on a range the
        /// read had already observed: the read missed a qualifying commit.
        missed: bool,
        steps: u8,
    }

    struct ClosedTimestampModel {
        /// Whether a cross-range read waits for a range's closed timestamp to
        /// reach `read_ts` before observing it. Off reintroduces the bug.
        reconcile_before_read: bool,
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    enum Action {
        StartRead(u8),
        CommitA,
        CommitB,
        PublishA(u8),
        PublishB(u8),
        ReadA,
        ReadB,
    }

    impl Model for ClosedTimestampModel {
        type State = State;
        type Action = Action;

        fn init_states(&self) -> Vec<State> {
            vec![State {
                a: Sequence::new(),
                b: Sequence::new(),
                read_ts: 0,
                read_a: false,
                read_b: false,
                missed: false,
                steps: 0,
            }]
        }

        fn actions(&self, state: &State, out: &mut Vec<Action>) {
            if state.steps >= MAX_STEPS {
                return;
            }
            if state.read_ts == 0 {
                // A cross-range read draws `read_ts` from the global source; it
                // may land anywhere in the domain relative to local commits,
                // including above every timestamp allocated so far.
                for read_ts in 1..=MAX_TS + 1 {
                    out.push(Action::StartRead(read_ts));
                }
            }
            out.push(Action::CommitA);
            out.push(Action::CommitB);
            for target in 1..=MAX_TS {
                out.push(Action::PublishA(target));
                out.push(Action::PublishB(target));
            }
            if state.read_ts != 0 && !state.read_a {
                out.push(Action::ReadA);
            }
            if state.read_ts != 0 && !state.read_b {
                out.push(Action::ReadB);
            }
        }

        fn next_state(&self, state: &State, action: Action) -> Option<State> {
            let mut next = state.clone();
            next.steps = next.steps.checked_add(1)?;
            match action {
                Action::StartRead(read_ts) => {
                    if next.read_ts != 0 {
                        return None;
                    }
                    next.read_ts = read_ts;
                }
                Action::CommitA => {
                    let already_read = next.read_a;
                    let commit_ts = next.a.commit()?;
                    record_commit(&mut next, commit_ts, already_read);
                }
                Action::CommitB => {
                    let already_read = next.read_b;
                    let commit_ts = next.b.commit()?;
                    record_commit(&mut next, commit_ts, already_read);
                }
                Action::PublishA(target) => next.a.publish(target)?,
                Action::PublishB(target) => next.b.publish(target)?,
                Action::ReadA => self.observe_range(&mut next, Range::A)?,
                Action::ReadB => self.observe_range(&mut next, Range::B)?,
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![
                Property::<Self>::always(
                    "cross-range read never misses a single-shard commit at or below read_ts",
                    |_, state| !state.missed,
                ),
                Property::<Self>::sometimes(
                    "a cross-range read observes single-shard commits on both ranges",
                    |_, state| {
                        state.read_a
                            && state.read_b
                            && state.a.commits.iter().any(|&ts| ts <= state.read_ts)
                            && state.b.commits.iter().any(|&ts| ts <= state.read_ts)
                    },
                ),
            ]
        }
    }

    #[derive(Clone, Copy)]
    enum Range {
        A,
        B,
    }

    impl ClosedTimestampModel {
        fn observe_range(&self, state: &mut State, range: Range) -> Option<()> {
            let (sequence, already_read) = match range {
                Range::A => (&state.a, state.read_a),
                Range::B => (&state.b, state.read_b),
            };
            if already_read {
                return None;
            }
            // The reconciliation rule: a range may serve the cross-range read
            // only once its closed timestamp has reached `read_ts`.
            if self.reconcile_before_read && sequence.closed < state.read_ts {
                return None;
            }
            // The read observes exactly the commits with `commit_ts <= read_ts`;
            // anything already durable here is captured. What safety forbids is a
            // *later* qualifying commit, tracked by `record_commit`.
            match range {
                Range::A => state.read_a = true,
                Range::B => state.read_b = true,
            }
            Some(())
        }
    }

    /// Fold a freshly durable commit into the missed-commit witness: if the read
    /// has already observed this range and the commit qualifies for the snapshot
    /// (`commit_ts <= read_ts`), the read missed it.
    fn record_commit(state: &mut State, commit_ts: u8, range_already_read: bool) {
        if range_already_read && state.read_ts != 0 && commit_ts <= state.read_ts {
            state.missed = true;
        }
    }

    #[test]
    fn reconciled_cross_range_read_never_misses_a_single_shard_commit() {
        let checker = ClosedTimestampModel {
            reconcile_before_read: true,
        }
        .checker()
        .spawn_bfs()
        .join();

        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    #[test]
    fn reading_before_closure_misses_a_single_shard_commit() {
        let checker = ClosedTimestampModel {
            reconcile_before_read: false,
        }
        .checker()
        .spawn_bfs()
        .join();

        assert!(checker.discoveries().contains_key(
            "cross-range read never misses a single-shard commit at or below read_ts"
        ));
    }
}

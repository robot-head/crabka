//! The `KRaft` quorum state machine: `on_event(event, log, now) -> Vec<Action>`.

use std::collections::{BTreeMap, BTreeSet};

use crate::kraft::action::{Action, TimerKind};
use crate::kraft::event::{Event, LogEnd};
use crate::kraft::role::{ReplicaProgress, Role};
use crate::kraft::types::{
    LeaderEpoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant,
};

/// The hand-rolled KIP-595 + KIP-996 quorum state machine. Pure and
/// deterministic: it consumes [`Event`]s, reads the log through [`LogView`],
/// takes the current time as an injected [`SimInstant`], and produces a list of
/// [`Action`]s for the caller to execute. It never touches the clock, the wire,
/// or the log bytes directly.
pub struct QuorumStateMachine {
    me: NodeId,
    state: QuorumState,
    role: Role,
    /// Base election timeout in ms; callers vary it per node for liveness.
    election_timeout_ms: u64,
}

impl QuorumStateMachine {
    #[must_use]
    pub fn new(me: NodeId, state: QuorumState, election_timeout_ms: u64) -> Self {
        let observer = !state.voters.contains(me);
        let role = if observer {
            Role::Observer {
                leader_id: None,
                fetch_deadline: SimInstant(0),
            }
        } else {
            Role::default()
        };
        Self {
            me,
            state,
            role,
            election_timeout_ms,
        }
    }

    #[must_use]
    pub fn quorum_state(&self) -> &QuorumState {
        &self.state
    }
    #[must_use]
    pub fn role(&self) -> &Role {
        &self.role
    }
    #[must_use]
    pub fn is_voter(&self) -> bool {
        self.state.voters.contains(self.me)
    }

    #[cfg(test)]
    pub(crate) fn force_epoch(&mut self, e: LeaderEpoch) {
        self.state.leader_epoch = e;
    }

    /// `true` if `candidate_log` is at least as up-to-date as ours
    /// (KIP-595: higher last epoch wins; on tie, higher/equal offset wins).
    fn log_is_up_to_date(log: &dyn LogView, cand: LogEnd) -> bool {
        let my_epoch = log.last_epoch();
        let my_end = log.end_offset();
        cand.last_epoch > my_epoch || (cand.last_epoch == my_epoch && cand.last_offset >= my_end)
    }

    /// The deadline for an election timer armed at `now`.
    fn election_deadline(&self, now: SimInstant) -> SimInstant {
        now.saturating_add_ms(self.election_timeout_ms)
    }

    // `event` is taken by value: it models a consumed input message, and 3b/3c
    // hand ownership to the machine. The current arms happen to only read Copy
    // fields, but that is an implementation detail of slice 3a.
    #[allow(clippy::needless_pass_by_value)]
    pub fn on_event(&mut self, event: Event, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        match event {
            Event::ReceiveVoteRequest {
                from,
                candidate_epoch,
                candidate,
                candidate_log_end,
                pre_vote,
            } => self.handle_vote_request(
                log,
                from,
                candidate_epoch,
                candidate,
                candidate_log_end,
                pre_vote,
                now,
            ),
            Event::ElectionTimeout => self.handle_election_timeout(now),
            Event::ReceiveVoteResponse {
                from,
                epoch,
                vote_granted,
                pre_vote,
            } => self.handle_vote_response(log, from, epoch, vote_granted, pre_vote, now),
            Event::ReceiveBeginQuorumEpoch {
                leader_id,
                leader_epoch,
            } => self.handle_begin_quorum_epoch(leader_id, leader_epoch, now),
            Event::ReceiveEndQuorumEpoch {
                leader_id,
                leader_epoch,
            } => self.handle_end_quorum_epoch(leader_id, leader_epoch, now),
            Event::ReceiveFetch {
                from,
                fetch_epoch,
                fetch_offset,
            } => self.handle_fetch(log, from, fetch_epoch, fetch_offset),
            Event::ReceiveFetchResponse {
                leader_id,
                leader_epoch,
                diverging,
            } => self.handle_fetch_response(leader_id, leader_epoch, diverging, now),
            Event::FetchTimeout => self.handle_fetch_timeout(now),
        }
    }

    /// (Leader side) a follower fetched at `fetch_offset` claiming it last
    /// replicated up to `fetch_epoch`. If the follower's claimed epoch extends
    /// past where that epoch ends in our log, the logs diverged: reply with the
    /// truncation point. Otherwise record its progress and advance the HWM.
    fn handle_fetch(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        fetch_epoch: LeaderEpoch,
        fetch_offset: i64,
    ) -> Vec<Action> {
        // Only a leader tracks follower progress / serves divergence hints.
        if !self.role.is_leader() {
            return Vec::new();
        }
        // Divergence check: if the follower claims to have replicated `fetch_epoch`
        // beyond where that epoch ends in our log, it must truncate.
        if fetch_offset > 0
            && let Some(div_end) = log.end_offset_for_epoch(fetch_epoch)
            && fetch_offset > div_end
        {
            return vec![Action::TruncateTo(LogOffsetMetadata {
                offset: div_end,
                epoch: fetch_epoch,
            })];
        }
        // Consistent: record the follower's fetch offset and recompute the HWM.
        let log_end = log.end_offset();
        if let Role::Leader { replicas, .. } = &mut self.role
            && let Some(progress) = replicas.get_mut(&from)
        {
            progress.fetch_offset = fetch_offset;
        }
        let new_hwm = self.recompute_high_watermark(log_end);
        if let Role::Leader { high_watermark, .. } = &mut self.role
            && new_hwm > *high_watermark
        {
            *high_watermark = new_hwm;
            return vec![Action::AdvanceHighWatermark(new_hwm)];
        }
        Vec::new()
    }

    /// The HWM as the `majority()`-th largest match offset across the leader's
    /// own log end and every follower's acknowledged fetch offset. Never
    /// regresses (the caller only adopts a strictly larger value).
    fn recompute_high_watermark(&self, log_end: i64) -> i64 {
        let Role::Leader { replicas, .. } = &self.role else {
            return 0;
        };
        let mut match_offsets: Vec<i64> = Vec::with_capacity(replicas.len() + 1);
        match_offsets.push(log_end);
        for progress in replicas.values() {
            match_offsets.push(progress.fetch_offset);
        }
        // Sort descending; the majority-th largest sits at index `majority - 1`.
        match_offsets.sort_unstable_by(|a, b| b.cmp(a));
        match_offsets[self.state.majority() - 1]
    }

    /// (Follower side) the leader answered our Fetch. A diverging hint means we
    /// must truncate; otherwise we re-arm the fetch timer and fetch again.
    fn handle_fetch_response(
        &mut self,
        leader_id: NodeId,
        _leader_epoch: LeaderEpoch,
        diverging: Option<LogOffsetMetadata>,
        now: SimInstant,
    ) -> Vec<Action> {
        if let Some(point) = diverging {
            return vec![Action::TruncateTo(point)];
        }
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        vec![
            Action::SendFetch { leader_id },
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            },
        ]
    }

    /// The fetch timer fired: a follower/observer lost contact with the leader.
    /// A voter starts an election; an observer just keeps trying to find a leader.
    fn handle_fetch_timeout(&mut self, now: SimInstant) -> Vec<Action> {
        if self.is_voter() {
            self.start_election(now)
        } else {
            Vec::new()
        }
    }

    /// A leader announced its epoch. If it is at least our current epoch, follow
    /// it (becoming `Follower`/an attached `Observer`); a stale (lower-epoch)
    /// announcement is ignored.
    fn handle_begin_quorum_epoch(
        &mut self,
        leader_id: NodeId,
        leader_epoch: LeaderEpoch,
        now: SimInstant,
    ) -> Vec<Action> {
        if leader_epoch < self.state.leader_epoch {
            return Vec::new();
        }
        self.state.leader_epoch = leader_epoch;
        self.state.leader_id = Some(leader_id);
        self.state.voted_key = None;
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        self.role = if self.is_voter() {
            Role::Follower {
                leader_id,
                fetch_deadline,
            }
        } else {
            Role::Observer {
                leader_id: Some(leader_id),
                fetch_deadline,
            }
        };
        vec![
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            Action::SendFetch { leader_id },
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            },
        ]
    }

    /// A resigning leader asked us to start an election. If it is not stale, a
    /// voter immediately begins a pre-vote round (no waiting for the election
    /// timer); an observer simply detaches and keeps observing.
    fn handle_end_quorum_epoch(
        &mut self,
        _leader_id: NodeId,
        leader_epoch: LeaderEpoch,
        now: SimInstant,
    ) -> Vec<Action> {
        if leader_epoch < self.state.leader_epoch {
            return Vec::new();
        }
        if self.is_voter() {
            self.start_election(now)
        } else {
            let mut actions = Vec::new();
            self.transition_to_unattached(self.state.leader_epoch, now, &mut actions);
            actions
        }
    }

    /// The election timer fired. A voter begins a KIP-996 pre-vote round
    /// (becomes `Prospective`); an observer never elects.
    fn handle_election_timeout(&mut self, now: SimInstant) -> Vec<Action> {
        if !self.is_voter() {
            return Vec::new();
        }
        self.start_election(now)
    }

    /// Shared election-start path (used by `ElectionTimeout` and a resigning
    /// leader's `EndQuorumEpoch`): become `Prospective` and broadcast a
    /// non-binding pre-vote at the *current* epoch (epoch is not bumped until the
    /// pre-vote succeeds).
    fn start_election(&mut self, now: SimInstant) -> Vec<Action> {
        let mut granted = BTreeSet::new();
        granted.insert(self.me);
        let deadline = self.election_deadline(now);
        self.role = Role::Prospective {
            granted,
            election_deadline: deadline,
        };
        let mut actions = vec![
            Action::TransitionedTo(self.role.name()),
            Action::SendVoteRequest {
                epoch: self.state.leader_epoch,
                pre_vote: true,
            },
            Action::ResetTimer {
                kind: TimerKind::Election,
                deadline,
            },
        ];
        // A lone voter wins its own pre-vote immediately.
        if self.tally_prevote_reached_majority() {
            actions.extend(self.promote_to_candidate(now));
        }
        actions
    }

    fn handle_vote_response(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        epoch: LeaderEpoch,
        vote_granted: bool,
        pre_vote: bool,
        now: SimInstant,
    ) -> Vec<Action> {
        // A higher-epoch rejection fences us: step down to that epoch.
        if !vote_granted && epoch > self.state.leader_epoch {
            let mut actions = Vec::new();
            self.transition_to_unattached(epoch, now, &mut actions);
            return actions;
        }
        if !vote_granted {
            return Vec::new();
        }
        match (&mut self.role, pre_vote) {
            (Role::Prospective { granted, .. }, true) => {
                granted.insert(from);
                if self.tally_prevote_reached_majority() {
                    self.promote_to_candidate(now)
                } else {
                    Vec::new()
                }
            }
            (Role::Candidate { granted, .. }, false) if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_candidate_reached_majority() {
                    self.promote_to_leader(log)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    /// Whether the current `Prospective` grant set has reached a quorum.
    fn tally_prevote_reached_majority(&self) -> bool {
        match &self.role {
            Role::Prospective { granted, .. } => granted.len() >= self.state.majority(),
            _ => false,
        }
    }

    /// Whether the current `Candidate` grant set has reached a quorum.
    fn tally_candidate_reached_majority(&self) -> bool {
        match &self.role {
            Role::Candidate { granted, .. } => granted.len() >= self.state.majority(),
            _ => false,
        }
    }

    /// Pre-vote succeeded: bump the epoch, self-vote, and broadcast a real vote.
    fn promote_to_candidate(&mut self, now: SimInstant) -> Vec<Action> {
        self.state.leader_epoch += 1;
        self.state.leader_id = None;
        self.state.voted_key = Some(ReplicaKey {
            id: self.me,
            directory_id: uuid::Uuid::nil(),
        });
        let mut granted = BTreeSet::new();
        granted.insert(self.me);
        let deadline = self.election_deadline(now);
        self.role = Role::Candidate {
            granted,
            election_deadline: deadline,
        };
        let mut actions = vec![
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            Action::SendVoteRequest {
                epoch: self.state.leader_epoch,
                pre_vote: false,
            },
            Action::ResetTimer {
                kind: TimerKind::Election,
                deadline,
            },
        ];
        // A lone voter wins its own election immediately.
        if self.tally_candidate_reached_majority() {
            actions.extend(self.promote_to_leader_inner());
        }
        actions
    }

    /// Real vote succeeded: become leader for the current epoch.
    fn promote_to_leader(&mut self, _log: &dyn LogView) -> Vec<Action> {
        self.promote_to_leader_inner()
    }

    fn promote_to_leader_inner(&mut self) -> Vec<Action> {
        let epoch = self.state.leader_epoch;
        self.state.leader_id = Some(self.me);
        let mut replicas = BTreeMap::new();
        for id in self.state.voters.ids() {
            if id != self.me {
                replicas.insert(id, ReplicaProgress::default());
            }
        }
        self.role = Role::Leader {
            replicas,
            high_watermark: 0,
        };
        vec![
            Action::AppendLeaderChange { epoch },
            Action::SendBeginQuorumEpoch { epoch },
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
        ]
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_vote_request(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        candidate_epoch: LeaderEpoch,
        candidate: NodeId,
        cand_log: LogEnd,
        pre_vote: bool,
        now: SimInstant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        // Fenced: candidate is behind our epoch.
        if candidate_epoch < self.state.leader_epoch {
            actions.push(Action::ReplyVote {
                to: from,
                epoch: self.state.leader_epoch,
                granted: false,
                pre_vote,
            });
            return actions;
        }
        // A standard vote at a higher epoch first advances us to that epoch
        // (Unattached), clearing any prior vote. Pre-vote never changes epoch.
        if !pre_vote && candidate_epoch > self.state.leader_epoch {
            self.transition_to_unattached(candidate_epoch, now, &mut actions);
        }
        let up_to_date = Self::log_is_up_to_date(log, cand_log);
        let granted = if pre_vote {
            // Non-binding: grant if log is up to date and we don't already
            // follow a leader in this (or a higher) epoch.
            up_to_date && self.state.leader_id.is_none()
        } else {
            let not_voted_other = match self.state.voted_key {
                None => true,
                Some(k) => k.id == candidate,
            };
            up_to_date && not_voted_other && self.state.leader_id.is_none()
        };
        if granted && !pre_vote {
            // Binding: persist the vote, become Voted.
            self.state.voted_key = Some(ReplicaKey {
                id: candidate,
                directory_id: uuid::Uuid::nil(),
            });
            self.role = Role::Voted {
                election_deadline: self.election_deadline(now),
            };
            actions.push(Action::PersistQuorumState);
            actions.push(Action::TransitionedTo(self.role.name()));
        }
        actions.push(Action::ReplyVote {
            to: from,
            epoch: self.state.leader_epoch,
            granted,
            pre_vote,
        });
        actions
    }

    fn transition_to_unattached(
        &mut self,
        epoch: LeaderEpoch,
        deadline: SimInstant,
        actions: &mut Vec<Action>,
    ) {
        self.state.leader_epoch = epoch;
        self.state.leader_id = None;
        self.state.voted_key = None;
        self.role = Role::Unattached {
            election_deadline: deadline,
        };
        actions.push(Action::PersistQuorumState);
        actions.push(Action::TransitionedTo("Unattached"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kraft::event::{Event, LogEnd};
    use crate::kraft::types::*;
    use assert2::assert;

    struct FakeLog {
        end: i64,
        last_epoch: LeaderEpoch,
    }
    impl LogView for FakeLog {
        fn end_offset(&self) -> i64 {
            self.end
        }
        fn last_epoch(&self) -> LeaderEpoch {
            self.last_epoch
        }
        fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64> {
            if epoch <= self.last_epoch {
                Some(self.end)
            } else {
                None
            }
        }
    }
    fn voters(ids: &[NodeId]) -> crabka_metadata::voters::VoterSet {
        crabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
            crabka_metadata::voters::Voter {
                id,
                directory_id: uuid::Uuid::nil(),
                endpoints: vec![],
                kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
            }
        }))
    }
    fn machine(me: NodeId, ids: &[NodeId]) -> QuorumStateMachine {
        QuorumStateMachine::new(
            me,
            QuorumState::bootstrap(uuid::Uuid::nil(), voters(ids)),
            1000,
        )
    }

    #[test]
    fn grants_standard_vote_when_log_up_to_date_and_not_voted() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: 2,
                candidate_epoch: 1,
                candidate: 2,
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::ReplyVote {
                to: 2,
                granted: true,
                pre_vote: false,
                ..
            }
        )));
        assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(2)); // binding
    }

    #[test]
    fn denies_standard_vote_when_candidate_log_behind() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 10,
            last_epoch: 2,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: 2,
                candidate_epoch: 2,
                candidate: 2,
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 3,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
        );
    }

    #[test]
    fn pre_vote_grant_is_non_binding() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(
            Event::ReceiveVoteRequest {
                from: 2,
                candidate_epoch: 1,
                candidate: 2,
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: true,
            },
            &log,
            SimInstant(0),
        );
        assert!(m.quorum_state().voted_key.is_none()); // pre-vote does NOT persist
        assert!(m.quorum_state().leader_epoch == 0); // epoch unchanged
    }

    #[test]
    fn denies_standard_vote_when_already_voted_for_other() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        // vote for 2 first
        m.on_event(
            Event::ReceiveVoteRequest {
                from: 2,
                candidate_epoch: 1,
                candidate: 2,
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        // now 3 asks in the same epoch
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: 3,
                candidate_epoch: 1,
                candidate: 3,
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::ReplyVote {
                to: 3,
                granted: false,
                ..
            }
        )));
    }

    #[test]
    fn fenced_when_candidate_epoch_below_current() {
        let mut m = machine(1, &[1, 2, 3]);
        m.force_epoch(5); // test helper
        let log = FakeLog {
            end: 5,
            last_epoch: 5,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: 2,
                candidate_epoch: 3,
                candidate: 2,
                candidate_log_end: LogEnd {
                    last_epoch: 5,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
        );
    }

    #[test]
    fn election_timeout_starts_prevote_prospective() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        assert!(matches!(m.role(), Role::Prospective { .. }));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. }))
        );
        assert!(m.quorum_state().leader_epoch == 0); // pre-vote: epoch not bumped yet
    }

    #[test]
    fn prevote_majority_promotes_to_candidate_and_bumps_epoch() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000)); // Prospective
        // 1 (self) + grant from 2 = majority of 3
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 0,
                vote_granted: true,
                pre_vote: true,
            },
            &log,
            SimInstant(2001),
        );
        assert!(matches!(m.role(), Role::Candidate { .. }));
        assert!(m.quorum_state().leader_epoch == 1);
        assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(1)); // self-vote
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendVoteRequest {
                pre_vote: false,
                epoch: 1
            }
        )));
    }

    #[test]
    fn real_majority_promotes_to_leader_and_appends_leader_change() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 0,
                vote_granted: true,
                pre_vote: true,
            },
            &log,
            SimInstant(2001),
        );
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 1,
                vote_granted: true,
                pre_vote: false,
            },
            &log,
            SimInstant(2002),
        );
        assert!(m.role().is_leader());
        assert!(m.quorum_state().leader_id == Some(1));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::AppendLeaderChange { epoch: 1 }))
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::SendBeginQuorumEpoch { epoch: 1 }))
        );
    }

    #[test]
    fn observer_never_starts_election() {
        let mut m = machine(99, &[1, 2, 3]); // 99 is not a voter
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        assert!(matches!(m.role(), Role::Observer { .. }));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::SendVoteRequest { .. }))
        );
    }

    #[test]
    fn begin_quorum_epoch_makes_us_follower() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: 2,
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert!(matches!(m.role(), Role::Follower { leader_id: 2, .. }));
        assert!(m.quorum_state().leader_epoch == 4);
        assert!(m.quorum_state().leader_id == Some(2));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::SendFetch { leader_id: 2 }))
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistQuorumState))
        );
    }

    #[test]
    fn end_quorum_epoch_triggers_immediate_election() {
        let mut m = machine(1, &[1, 2, 3]);
        // follow leader 2 @ epoch 4 first
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: 2,
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        let actions = m.on_event(
            Event::ReceiveEndQuorumEpoch {
                leader_id: 2,
                leader_epoch: 4,
            },
            &log,
            SimInstant(11),
        );
        // immediately start pre-vote (Prospective), not wait for timeout
        assert!(matches!(m.role(), Role::Prospective { .. }));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. }))
        );
    }

    #[test]
    fn stale_begin_quorum_epoch_ignored() {
        let mut m = machine(1, &[1, 2, 3]);
        m.force_epoch(7);
        let log = FakeLog {
            end: 5,
            last_epoch: 7,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: 2,
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert!(actions.is_empty()); // lower epoch → ignored
        assert!(m.quorum_state().leader_id.is_none());
    }

    #[test]
    fn leader_advances_hwm_at_majority_fetch_offset() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 10,
            last_epoch: 1,
        };
        // drive to leader
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 0,
                vote_granted: true,
                pre_vote: true,
            },
            &log,
            SimInstant(2001),
        );
        m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 1,
                vote_granted: true,
                pre_vote: false,
            },
            &log,
            SimInstant(2002),
        );
        // leader end offset 10. follower 2 fetches at 8, follower 3 at 4.
        let a2 = m.on_event(
            Event::ReceiveFetch {
                from: 2,
                fetch_epoch: 1,
                fetch_offset: 8,
            },
            &log,
            SimInstant(2100),
        );
        // majority of {self=10, 2=8} = 8 → HWM advances to 8
        assert!(
            a2.iter()
                .any(|a| matches!(a, Action::AdvanceHighWatermark(8)))
        );
        let _ = m.on_event(
            Event::ReceiveFetch {
                from: 3,
                fetch_epoch: 1,
                fetch_offset: 4,
            },
            &log,
            SimInstant(2101),
        );
        // sorted match offsets {10,8,4}; majority (2nd highest) = 8 → no regress
        if let Role::Leader { high_watermark, .. } = m.role() {
            assert!(*high_watermark == 8);
        } else {
            panic!()
        }
    }

    #[test]
    fn leader_detects_divergence_and_returns_truncate() {
        // log has last_epoch 2 ending at 10; epoch-1 ended at 5.
        struct L;
        impl LogView for L {
            fn end_offset(&self) -> i64 {
                10
            }
            fn last_epoch(&self) -> LeaderEpoch {
                2
            }
            fn end_offset_for_epoch(&self, e: LeaderEpoch) -> Option<i64> {
                match e {
                    0 => Some(0),
                    1 => Some(5),
                    2 => Some(10),
                    _ => None,
                }
            }
        }
        let mut m = machine(1, &[1, 2, 3]);
        let log = L;
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 0,
                vote_granted: true,
                pre_vote: true,
            },
            &log,
            SimInstant(2001),
        );
        m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 1,
                vote_granted: true,
                pre_vote: false,
            },
            &log,
            SimInstant(2002),
        );
        // follower claims it fetched epoch 1 at offset 8, but epoch 1 ended at 5 → diverged.
        let actions = m.on_event(
            Event::ReceiveFetch {
                from: 2,
                fetch_epoch: 1,
                fetch_offset: 8,
            },
            &log,
            SimInstant(2100),
        );
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::TruncateTo(LogOffsetMetadata {
                offset: 5,
                epoch: 1
            })
        )));
    }

    #[test]
    fn follower_truncates_on_diverging_fetch_response() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 10,
            last_epoch: 2,
        };
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: 2,
                leader_epoch: 3,
            },
            &log,
            SimInstant(10),
        );
        let actions = m.on_event(
            Event::ReceiveFetchResponse {
                leader_id: 2,
                leader_epoch: 3,
                diverging: Some(LogOffsetMetadata {
                    offset: 5,
                    epoch: 1,
                }),
            },
            &log,
            SimInstant(11),
        );
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::TruncateTo(LogOffsetMetadata {
                offset: 5,
                epoch: 1
            })
        )));
    }
}

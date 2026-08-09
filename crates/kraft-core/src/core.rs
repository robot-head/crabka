//! The `KRaft` quorum state machine: `on_event(event, log, now) -> Vec<Action>`.

use std::collections::{BTreeMap, BTreeSet};

use crabka_units::prelude::{Time, TimeExt as _};
use crabka_voters::VoterSet;

use crate::{
    action::{Action, TimerKind},
    event::{Event, LogEnd},
    role::{ReplicaProgress, Role},
    types::{Epoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant},
};

/// Deterministic per-`(node, epoch)` election-timeout jitter in `[0, base_ms)`.
///
/// This is Raft's randomized backoff, made reproducible for the deterministic
/// sims. Different nodes get different spreads, and so does the same node
/// across re-election epochs. Closely-synchronized voters therefore do not arm
/// their election timers in lockstep and split the vote indefinitely.
///
/// Both the pure core and the async engine's initial timer arm call this
/// function, so production self-staggers without per-node config.
#[must_use]
pub fn election_jitter_ms(me: NodeId, epoch: Epoch, base_ms: u64) -> u64 {
    crabka_verified::election_jitter_ms(me.0, epoch, base_ms)
}

/// The hand-rolled KIP-595 + KIP-996 quorum state machine.
///
/// The state machine is pure and deterministic. It consumes [`Event`]s, reads
/// the log through [`LogView`], takes the current time as an injected
/// [`SimInstant`], and produces a list of [`Action`]s for the caller to
/// execute. It never touches the clock, the wire, or the log bytes directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QuorumStateMachine {
    me: NodeId,
    state: QuorumState,
    /// The other side of the one in-flight KIP-853 transition. Requests from
    /// this explicitly adjacent set remain valid until the voter record commits.
    adjacent_voters: Option<VoterSet>,
    role: Role,
    /// Base election timeout, in whole milliseconds.
    ///
    /// This field holds a raw value and not a [`Time`]. Quantities store `f64`,
    /// so a [`Time`] field would cost this struct its `Eq` and `Hash` derives.
    /// The `stateright` model checker needs those derives on every state it
    /// explores. Whole milliseconds is also the domain of the verified jitter
    /// kernel. [`QuorumStateMachine::new`] converts at that boundary.
    election_timeout_ms: u64,
}

#[derive(Clone, Copy)]
struct VoteRequest {
    from: NodeId,
    voter_id: NodeId,
    candidate_epoch: Epoch,
    candidate: NodeId,
    candidate_log_end: LogEnd,
    pre_vote: bool,
}

impl QuorumStateMachine {
    /// `election_timeout` is the base extent of an election timer, before
    /// jitter; callers vary it per node for liveness.
    ///
    /// This constructor rounds the value to whole milliseconds. That is the
    /// domain of both the verified jitter kernel and the [`SimInstant`] clock.
    #[must_use]
    pub fn new(me: NodeId, state: QuorumState, election_timeout: Time) -> Self {
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
            adjacent_voters: None,
            role,
            election_timeout_ms: u64::try_from(election_timeout.millis_i64()).unwrap_or(0),
        }
    }

    #[must_use]
    pub fn quorum_state(&self) -> &QuorumState {
        &self.state
    }
    /// This replica's own node id.
    #[must_use]
    pub fn me(&self) -> NodeId {
        self.me
    }
    #[must_use]
    pub fn role(&self) -> &Role {
        &self.role
    }
    #[must_use]
    pub fn is_voter(&self) -> bool {
        self.state.voters.contains(self.me)
    }

    /// Apply the latest voter set read from the Raft log or a snapshot.
    ///
    /// KIP-853 requires replicas to use an uncommitted `VotersRecord`
    /// immediately. The durable engine owns record history and invokes this
    /// method again with the preceding set if the log is truncated.
    pub fn apply_voter_set(&mut self, voters: VoterSet, now: SimInstant) -> Vec<Action> {
        let was_voter = self.is_voter();
        let leader_id = self.state.leader_id;
        if self.state.voters != voters {
            self.adjacent_voters = Some(self.state.voters.clone());
        }
        self.state.voters = voters;

        if let Role::Leader { replicas, .. } = &mut self.role {
            replicas.retain(|id, _| self.state.voters.contains(*id) && *id != self.me);
            for id in self.state.voters.ids() {
                if id != self.me {
                    replicas.entry(id).or_default();
                }
            }
            return Vec::new();
        }

        match (was_voter, self.is_voter()) {
            (true, false) => {
                let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
                self.role = Role::Observer {
                    leader_id,
                    fetch_deadline,
                };
                let mut actions = vec![Action::TransitionedTo(self.role.name())];
                if let Some(leader_id) = leader_id {
                    actions.push(Action::SendFetch { leader_id });
                    actions.push(Action::ResetTimer {
                        kind: TimerKind::Fetch,
                        deadline: fetch_deadline,
                    });
                }
                actions
            }
            (false, true) => {
                if let Some(leader_id) = leader_id {
                    let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
                    self.role = Role::Follower {
                        leader_id,
                        fetch_deadline,
                    };
                    vec![
                        Action::TransitionedTo(self.role.name()),
                        Action::SendFetch { leader_id },
                        Action::ResetTimer {
                            kind: TimerKind::Fetch,
                            deadline: fetch_deadline,
                        },
                    ]
                } else {
                    let election_deadline = self.election_deadline(now);
                    self.role = Role::Unattached { election_deadline };
                    vec![
                        Action::TransitionedTo(self.role.name()),
                        Action::ResetTimer {
                            kind: TimerKind::Election,
                            deadline: election_deadline,
                        },
                    ]
                }
            }
            _ => Vec::new(),
        }
    }

    /// Apply a committed `KRaftVersionRecord`.
    pub fn set_kraft_version(&mut self, version: u16) {
        self.state.kraft_version = version;
    }

    /// Forget the preceding voter view once the latest voter record commits.
    pub fn commit_voter_set(&mut self) {
        self.adjacent_voters = None;
    }

    fn current_or_adjacent_voter(&self, id: NodeId) -> bool {
        self.state.voters.contains(id)
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| voters.contains(id))
    }

    /// Complete removal of the local leader after the reduced voter set has
    /// committed. Fetch serving continues until the engine invokes this edge.
    pub fn finish_local_leader_removal(&mut self, now: SimInstant) -> Vec<Action> {
        if self.is_voter() || !self.role.is_leader() {
            return Vec::new();
        }
        let epoch = self.state.leader_epoch;
        self.state.leader_id = None;
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        self.role = Role::Observer {
            leader_id: None,
            fetch_deadline,
        };
        vec![
            Action::SendEndQuorumEpoch { epoch },
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
        ]
    }

    #[cfg(test)]
    pub(crate) fn force_epoch(&mut self, e: Epoch) {
        self.state.leader_epoch = e;
    }

    /// `true` if `candidate_log` is at least as up-to-date as ours.
    ///
    /// KIP-595: the higher last epoch wins. On a tie, the higher or equal
    /// offset wins.
    fn log_is_up_to_date(log: &dyn LogView, cand: LogEnd) -> bool {
        crabka_verified::log_is_up_to_date(
            log.last_epoch(),
            log.end_offset(),
            cand.last_epoch,
            cand.last_offset,
        )
    }

    /// The deadline for an election timer armed at `now`.
    ///
    /// This method adds deterministic per-`(node, epoch)` jitter, the standard
    /// Raft randomized backoff made deterministic for the sims. Competing
    /// voters then do not arm their election timers in lockstep. Without the
    /// jitter, a bare majority of in-process or closely-synchronized voters,
    /// for example exactly 2 of a 3-voter set, splits the vote every round.
    /// Both become candidates, both self-vote, and neither reaches a majority.
    /// Elections then livelock until natural skew breaks the tie.
    ///
    /// The whole sum stays in integer milliseconds. The jitter is a verified
    /// integer kernel, and [`SimInstant`] is a coordinate on a millisecond
    /// timeline and not an extent.
    fn election_deadline(&self, now: SimInstant) -> SimInstant {
        now.saturating_add_ms(
            self.election_timeout_ms
                + election_jitter_ms(self.me, self.state.leader_epoch, self.election_timeout_ms),
        )
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, role = self.role.name())
    )]
    pub fn on_event(&mut self, event: Event, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        match event {
            Event::ReceiveVoteRequest {
                from,
                voter_id,
                candidate_epoch,
                candidate,
                candidate_log_end,
                pre_vote,
            } => self.handle_vote_request(
                VoteRequest {
                    from,
                    voter_id,
                    candidate_epoch,
                    candidate,
                    candidate_log_end,
                    pre_vote,
                },
                log,
                now,
            ),
            Event::ElectionTimeout => self.handle_election_timeout(log, now),
            Event::ReceiveVoteResponse {
                from,
                epoch,
                vote_granted,
            } => self.handle_vote_response(log, from, epoch, vote_granted, now),
            Event::ReceiveBeginQuorumEpoch {
                leader_id,
                leader_epoch,
            } => self.handle_begin_quorum_epoch(leader_id, leader_epoch, now),
            Event::ReceiveEndQuorumEpoch {
                leader_id,
                leader_epoch,
            } => self.handle_end_quorum_epoch(log, leader_id, leader_epoch, now),
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
            Event::FetchTimeout => self.handle_fetch_timeout(log, now),
        }
    }

    /// Leader side: a follower fetched at `fetch_offset` and claims that it
    /// last replicated up to `fetch_epoch`.
    ///
    /// If the follower's claimed epoch extends past where that epoch ends in
    /// our log, the logs diverged. This method then replies with the truncation
    /// point. If the logs agree, it records the follower's progress and
    /// advances the HWM.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = from.0, fetch_epoch, fetch_offset)
    )]
    fn handle_fetch(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        fetch_epoch: Epoch,
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
    /// own log end and every follower's acknowledged fetch offset.
    ///
    /// The current leader epoch gates the result (Raft Fig.8 and KIP-595 leader
    /// completeness): the HWM may only advance once a *current-epoch* entry has
    /// been majority-replicated. This method approximates that rule. It requires
    /// the majority offset to be strictly past `epoch_start_offset`, where this
    /// leader's first current-epoch record sits. In every other case the HWM
    /// stays unchanged. The HWM never regresses.
    ///
    /// Full per-offset epoch validation happens against the durable log. The
    /// core tracks `epoch_start_offset` as its in-memory stand-in.
    fn recompute_high_watermark(&self, log_end: i64) -> i64 {
        let Role::Leader {
            replicas,
            high_watermark,
            epoch_start_offset,
        } = &self.role
        else {
            return 0;
        };
        // Clamp inputs into the verified kernel's precondition domain: a
        // follower's acknowledged offset never legitimately exceeds the
        // leader's log end, and the leader's HWM is always within its log.
        // Both are invariants of correct operation; clamping makes them
        // locally evident instead of a distributed assumption.
        let mut follower_offsets: Vec<i64> = replicas
            .values()
            .map(|progress| progress.fetch_offset.min(log_end))
            .collect();
        let new_hwm = if self.is_voter() {
            crabka_verified::recompute_high_watermark(
                log_end,
                &follower_offsets,
                self.state.majority(),
                *epoch_start_offset,
                (*high_watermark).min(log_end),
            )
        } else {
            // A leader removed by its own VotersRecord continues serving Fetch
            // until the record commits, but its local log cannot count toward
            // the new configuration's majority.
            follower_offsets.sort_unstable_by(|a, b| b.cmp(a));
            follower_offsets
                .get(self.state.majority().saturating_sub(1))
                .copied()
                .filter(|offset| *offset > *epoch_start_offset)
                .unwrap_or(*high_watermark)
                .max(*high_watermark)
        };
        debug_assert!(
            new_hwm <= log_end,
            "HWM {new_hwm} must not exceed leader log end {log_end}"
        );
        new_hwm
    }

    /// Follower side: the leader answered our Fetch.
    ///
    /// A diverging hint means that we must truncate. Without a hint, we re-arm
    /// the fetch timer and fetch again.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_id = leader_id.0, diverging = diverging.is_some())
    )]
    fn handle_fetch_response(
        &mut self,
        leader_id: NodeId,
        _leader_epoch: Epoch,
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

    /// The fetch timer fired: a follower or observer lost contact with the leader.
    ///
    /// A voter starts an election. An observer continues to look for a leader.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, is_voter = self.is_voter())
    )]
    fn handle_fetch_timeout(&mut self, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        if self.is_voter() {
            self.start_election(log, now)
        } else {
            Vec::new()
        }
    }

    /// A leader announced its epoch.
    ///
    /// If the epoch is at least our current epoch, we follow that leader and
    /// become a `Follower` or an attached `Observer`. This method ignores a
    /// stale announcement at a lower epoch.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_id = leader_id.0, leader_epoch)
    )]
    fn handle_begin_quorum_epoch(
        &mut self,
        leader_id: NodeId,
        leader_epoch: Epoch,
        now: SimInstant,
    ) -> Vec<Action> {
        // A never-initialized joiner has no membership view yet and discovers
        // its first leader through configured bootstrap endpoints. Once a view
        // exists, accept only the current or explicitly adjacent KIP-853 set.
        let membership_known = !self.state.voters.is_empty()
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| !voters.is_empty());
        if membership_known && !self.current_or_adjacent_voter(leader_id) {
            return Vec::new();
        }
        // Accept a strictly-higher epoch, or an equal epoch only if we do not
        // already know a leader for it (one leader per epoch). Otherwise ignore.
        let accept = leader_epoch > self.state.leader_epoch
            || (leader_epoch == self.state.leader_epoch && self.state.leader_id.is_none());
        if !accept {
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

    /// A resigning leader asked us to start an election.
    ///
    /// If the request is not stale, a voter starts a pre-vote round immediately
    /// and does not wait for the election timer. An observer detaches and
    /// continues to observe.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_epoch)
    )]
    fn handle_end_quorum_epoch(
        &mut self,
        log: &dyn LogView,
        _leader_id: NodeId,
        leader_epoch: Epoch,
        now: SimInstant,
    ) -> Vec<Action> {
        if leader_epoch < self.state.leader_epoch {
            return Vec::new();
        }
        if self.is_voter() {
            self.start_election(log, now)
        } else {
            let mut actions = Vec::new();
            self.transition_to_unattached(self.state.leader_epoch, now, &mut actions);
            actions
        }
    }

    /// The election timer fired.
    ///
    /// A voter starts a KIP-996 pre-vote round and becomes `Prospective`. An
    /// observer never elects.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, is_voter = self.is_voter())
    )]
    fn handle_election_timeout(&mut self, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        if !self.is_voter() {
            return Vec::new();
        }
        self.start_election(log, now)
    }

    /// Shared election-start path for `ElectionTimeout` and for a resigning
    /// leader's `EndQuorumEpoch`.
    ///
    /// This method makes the replica `Prospective` and broadcasts a non-binding
    /// pre-vote at the *current* epoch. The epoch is not bumped until the
    /// pre-vote succeeds.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch)
    )]
    fn start_election(&mut self, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        // Starting a pre-vote round means we have given up on the current leader
        // (our fetch timed out, or the leader resigned). Drop the leader belief:
        // KIP-996 only grants a pre-vote when the voter is no longer following a
        // live leader, and the grant check keys off `leader_id.is_none()`. If we
        // kept `leader_id = Some(old)` here, a `Prospective` voter would refuse to
        // grant pre-votes to an equally-stranded peer, and re-election after the
        // leader is lost would deadlock (no voter can ever clear its stale leader
        // belief without a new leader, which can never be elected). The epoch is
        // unchanged — this is not a step-up to a new epoch, just abandoning the
        // dead leader for the current one.
        self.state.leader_id = None;
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
            actions.extend(self.promote_to_candidate(log, now));
        }
        actions
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = from.0, resp_epoch = epoch, vote_granted, role = self.role.name())
    )]
    fn handle_vote_response(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        epoch: Epoch,
        vote_granted: bool,
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
        // Match the grant to our round by our OWN role + epoch — exactly as
        // Kafka does (its `VoteResponse` carries no pre-vote flag). `Prospective`
        // ⇒ this is a pre-vote grant; `Candidate` ⇒ a real-vote grant. The epoch
        // guard drops a stale grant from a superseded round (e.g. a late pre-vote
        // grant at epoch E arriving after we bumped to E+1 and became Candidate).
        match &mut self.role {
            Role::Prospective { granted, .. } if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_prevote_reached_majority() {
                    self.promote_to_candidate(log, now)
                } else {
                    Vec::new()
                }
            }
            Role::Candidate { granted, .. } if epoch == self.state.leader_epoch => {
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
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch)
    )]
    fn promote_to_candidate(&mut self, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        self.state.leader_epoch = self.state.leader_epoch.saturating_add(1);
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
            actions.extend(self.promote_to_leader_inner(log));
        }
        actions
    }

    /// Real vote succeeded: become leader for the current epoch.
    fn promote_to_leader(&mut self, log: &dyn LogView) -> Vec<Action> {
        self.promote_to_leader_inner(log)
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch)
    )]
    fn promote_to_leader_inner(&mut self, log: &dyn LogView) -> Vec<Action> {
        let epoch = self.state.leader_epoch;
        self.state.leader_id = Some(self.me);
        let mut replicas = BTreeMap::new();
        for id in self.state.voters.ids() {
            if id != self.me {
                replicas.insert(id, ReplicaProgress::default());
            }
        }
        // The leader's `LeaderChange` / first current-epoch record sits at the
        // current log end. The HWM may only advance past this offset (Fig.8).
        let epoch_start_offset = log.end_offset();
        self.role = Role::Leader {
            replicas,
            high_watermark: 0,
            epoch_start_offset,
        };
        vec![
            Action::AppendLeaderChange { epoch },
            Action::SendBeginQuorumEpoch { epoch },
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
        ]
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = request.from.0, voter_id = request.voter_id.0, candidate = request.candidate.0, candidate_epoch = request.candidate_epoch, pre_vote = request.pre_vote)
    )]
    fn handle_vote_request(
        &mut self,
        request: VoteRequest,
        log: &dyn LogView,
        now: SimInstant,
    ) -> Vec<Action> {
        let VoteRequest {
            from,
            voter_id,
            candidate_epoch,
            candidate,
            candidate_log_end: cand_log,
            pre_vote,
        } = request;
        let mut actions = Vec::new();
        // Recipient-targeting check (KIP-595 / `KafkaRaftClient`): a Vote carries
        // the id of the voter it is addressed to. If it targets a different node
        // (a stale/misrouted/forged request), ignore it silently — do not even
        // reply, exactly as the JVM does. Only enforce once the addressing field
        // is meaningful: a `-1`/unset `voter_id` (decoded as 0) and the
        // bootstrap case where we have no voter set yet are not rejected here.
        if voter_id != self.me && voter_id != 0 {
            tracing::warn!(
                addressed_to = voter_id.0,
                me = self.me.0,
                "ignoring Vote addressed to a different voter"
            );
            return Vec::new();
        }
        // Only a member of the local latest set may cast a vote. The candidate
        // can be in either side of the one adjacent KIP-853 transition.
        if !self.is_voter() || !self.current_or_adjacent_voter(candidate) {
            actions.push(Action::ReplyVote {
                to: from,
                epoch: self.state.leader_epoch,
                granted: false,
            });
            return actions;
        }
        // Fenced: candidate is behind our epoch.
        if candidate_epoch < self.state.leader_epoch {
            actions.push(Action::ReplyVote {
                to: from,
                epoch: self.state.leader_epoch,
                granted: false,
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
            let deadline = self.election_deadline(now);
            self.role = Role::Voted {
                election_deadline: deadline,
            };
            actions.push(Action::PersistQuorumState);
            actions.push(Action::TransitionedTo(self.role.name()));
            // Arm the election timer: if the candidate we voted for dies, this
            // node must time out and start its own election (else deadlock).
            actions.push(Action::ResetTimer {
                kind: TimerKind::Election,
                deadline,
            });
        }
        actions.push(Action::ReplyVote {
            to: from,
            epoch: self.state.leader_epoch,
            granted,
        });
        actions
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, from_epoch = self.state.leader_epoch, to_epoch = epoch)
    )]
    fn transition_to_unattached(
        &mut self,
        epoch: Epoch,
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
        // Arm the election timer so a fenced/stepped-down node will eventually
        // re-elect if no leader emerges (without this it would deadlock).
        actions.push(Action::ResetTimer {
            kind: TimerKind::Election,
            deadline,
        });
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::prelude::{millis, secs};

    use super::*;
    use crate::{
        event::{Event, LogEnd},
        types::*,
    };

    struct FakeLog {
        end: i64,
        last_epoch: Epoch,
    }
    impl LogView for FakeLog {
        fn end_offset(&self) -> i64 {
            self.end
        }
        fn last_epoch(&self) -> Epoch {
            self.last_epoch
        }
        fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
            if epoch <= self.last_epoch {
                Some(self.end)
            } else {
                None
            }
        }
    }
    /// A `LogView` whose `end_offset` can change between calls.
    ///
    /// A test can then model a leader that is promoted at a small log end, that
    /// is, a low `epoch_start_offset`, and whose log grows before followers
    /// fetch.
    struct CellLog {
        end: std::cell::Cell<i64>,
        last_epoch: Epoch,
    }
    impl LogView for CellLog {
        fn end_offset(&self) -> i64 {
            self.end.get()
        }
        fn last_epoch(&self) -> Epoch {
            self.last_epoch
        }
        fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
            if epoch <= self.last_epoch {
                Some(self.end.get())
            } else {
                None
            }
        }
    }
    fn voters(ids: &[NodeId]) -> crabka_voters::VoterSet {
        crabka_voters::VoterSet::from_voters(ids.iter().map(|&id| crabka_voters::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![],
            kraft_version: crabka_voters::KRaftVersionRange::default(),
        }))
    }
    fn machine(me: NodeId, ids: &[NodeId]) -> QuorumStateMachine {
        QuorumStateMachine::new(
            me,
            QuorumState::bootstrap(uuid::Uuid::nil(), voters(ids)),
            TEST_ELECTION_TIMEOUT,
        )
    }

    /// The base election timeout for every test machine.
    const TEST_ELECTION_TIMEOUT: Time = secs(1);

    #[test]
    fn grants_standard_vote_when_log_up_to_date_and_not_voted() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(1),
                candidate_epoch: 1,
                candidate: NodeId(2),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::ReplyVote {
                to: NodeId(2),
                granted: true,
                ..
            }
        )));
        assert2::assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(NodeId(2))); // binding
    }

    #[test]
    fn denies_standard_vote_when_candidate_log_behind() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 10,
            last_epoch: 2,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(1),
                candidate_epoch: 2,
                candidate: NodeId(2),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 3,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
        );
    }

    #[test]
    fn pre_vote_grant_is_non_binding() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(1),
                candidate_epoch: 1,
                candidate: NodeId(2),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: true,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!((m.quorum_state().voted_key, m.quorum_state().leader_epoch) == (None, 0));
    }

    #[test]
    fn denies_standard_vote_when_already_voted_for_other() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        // vote for 2 first
        m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(1),
                candidate_epoch: 1,
                candidate: NodeId(2),
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
                from: NodeId(3),
                voter_id: NodeId(1),
                candidate_epoch: 1,
                candidate: NodeId(3),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::ReplyVote {
                to: NodeId(3),
                granted: false,
                ..
            }
        )));
    }

    #[test]
    fn fenced_when_candidate_epoch_below_current() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        m.force_epoch(5); // test helper
        let log = FakeLog {
            end: 5,
            last_epoch: 5,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(1),
                candidate_epoch: 3,
                candidate: NodeId(2),
                candidate_log_end: LogEnd {
                    last_epoch: 5,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
        );
    }

    #[test]
    fn election_timeout_starts_prevote_prospective() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
        assert2::assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. }))
        );
        check!(m.quorum_state().leader_epoch == 0); // pre-vote: epoch not bumped yet
    }

    #[test]
    fn prevote_majority_promotes_to_candidate_and_bumps_epoch() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000)); // Prospective
        // 1 (self) + grant from 2 = majority of 3
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        check!(
            (
                matches!(m.role(), Role::Candidate { .. }),
                m.quorum_state().leader_epoch,
                m.quorum_state().voted_key.map(|k| k.id),
            ) == (true, 1, Some(NodeId(1)))
        );
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendVoteRequest {
                pre_vote: false,
                epoch: 1
            }
        )));
    }

    #[test]
    fn real_majority_promotes_to_leader_and_appends_leader_change() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 1,
                vote_granted: true,
            },
            &log,
            SimInstant(2002),
        );
        check!(
            (
                m.role().is_leader(),
                m.quorum_state().leader_id,
                actions
                    .iter()
                    .any(|a| matches!(a, Action::AppendLeaderChange { epoch: 1 })),
                actions
                    .iter()
                    .any(|a| matches!(a, Action::SendBeginQuorumEpoch { epoch: 1 })),
            ) == (true, Some(NodeId(1)), true, true)
        );
    }

    #[test]
    fn observer_never_starts_election() {
        let mut m = machine(NodeId(99), &[NodeId(1), NodeId(2), NodeId(3)]); // 99 is not a voter
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        assert2::assert!(matches!(m.role(), Role::Observer { .. }));
        assert2::assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::SendVoteRequest { .. }))
        );
    }

    #[test]
    fn begin_quorum_epoch_makes_us_follower() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(
            (
                matches!(
                    m.role(),
                    Role::Follower {
                        leader_id: NodeId(2),
                        ..
                    }
                ),
                m.quorum_state().leader_epoch,
                m.quorum_state().leader_id,
                actions.iter().any(|a| matches!(
                    a,
                    Action::SendFetch {
                        leader_id: NodeId(2)
                    }
                )),
                actions
                    .iter()
                    .any(|a| matches!(a, Action::PersistQuorumState)),
            ) == (true, 4, Some(NodeId(2)), true, true)
        );
    }

    #[test]
    fn end_quorum_epoch_triggers_immediate_election() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        // follow leader 2 @ epoch 4 first
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        let actions = m.on_event(
            Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(11),
        );
        // immediately start pre-vote (Prospective), not wait for timeout
        assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
        assert2::assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. }))
        );
    }

    #[test]
    fn stale_begin_quorum_epoch_ignored() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        m.force_epoch(7);
        let log = FakeLog {
            end: 5,
            last_epoch: 7,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert2::assert!((actions.is_empty(), m.quorum_state().leader_id) == (true, None));
    }

    #[test]
    fn leader_advances_hwm_at_majority_fetch_offset() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        // The log end is 0 at promotion so the leader's `epoch_start_offset` is
        // 0; the leader-completeness gate then permits advancing the HWM to any
        // majority offset > 0. After promotion the log grows to end 10, which is
        // what followers replicate against.
        let log = CellLog {
            end: std::cell::Cell::new(0),
            last_epoch: 0,
        };
        // drive to leader (epoch_start_offset captured as end_offset() == 0)
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 1,
                vote_granted: true,
            },
            &log,
            SimInstant(2002),
        );
        assert2::assert!(matches!(
            m.role(),
            Role::Leader {
                epoch_start_offset: 0,
                ..
            }
        ));
        // Leader's log now ends at 10. follower 2 fetches at 8, follower 3 at 4.
        log.end.set(10);
        let a2 = m.on_event(
            Event::ReceiveFetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 8,
            },
            &log,
            SimInstant(2100),
        );
        // majority of {self=10, 2=8} = 8, and 8 > epoch_start_offset 0 → advances
        assert2::assert!(
            a2.iter()
                .any(|a| matches!(a, Action::AdvanceHighWatermark(8)))
        );
        let _ = m.on_event(
            Event::ReceiveFetch {
                from: NodeId(3),
                fetch_epoch: 1,
                fetch_offset: 4,
            },
            &log,
            SimInstant(2101),
        );
        // sorted match offsets {10,8,4}; majority (2nd highest) = 8 → no regress
        if let Role::Leader { high_watermark, .. } = m.role() {
            assert2::assert!(*high_watermark == 8);
        } else {
            panic!()
        }
    }

    #[test]
    fn leader_hwm_does_not_regress_on_reordered_stale_fetch() {
        // A follower's recorded `fetch_offset` can fall (a reordered/stale fetch,
        // or a legitimate post-truncation re-fetch), dropping the `majority()`-th
        // match offset below the current HWM. `recompute_high_watermark` must
        // clamp to the existing HWM (never regress) rather than return a lower
        // value — historically a lower return tripped a debug-only assertion.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = CellLog {
            end: std::cell::Cell::new(0),
            last_epoch: 0,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 1,
                vote_granted: true,
            },
            &log,
            SimInstant(2002),
        );
        // Leader log ends at 10; follower 2 fetches at 8 → HWM advances to 8.
        log.end.set(10);
        m.on_event(
            Event::ReceiveFetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 8,
            },
            &log,
            SimInstant(2100),
        );
        if let Role::Leader { high_watermark, .. } = m.role() {
            assert2::assert!(*high_watermark == 8);
        } else {
            panic!("expected leader")
        }
        // Now follower 2 sends a STALE lower fetch (offset 2 < its prior 8).
        // match offsets {self=10, 2=2, 3=0} → majority (2nd highest) = 2, which is
        // below the current HWM 8. The HWM must hold at 8, and no spurious
        // AdvanceHighWatermark may be emitted. (Pre-clamp this panicked.)
        let a = m.on_event(
            Event::ReceiveFetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 2,
            },
            &log,
            SimInstant(2101),
        );
        assert2::assert!(
            !a.iter()
                .any(|x| matches!(x, Action::AdvanceHighWatermark(_)))
        );
        if let Role::Leader { high_watermark, .. } = m.role() {
            assert2::assert!(*high_watermark == 8);
        } else {
            panic!("expected leader")
        }
    }

    #[test]
    fn leader_holds_hwm_for_prior_epoch_entries_until_current_epoch_committed() {
        // Leader-completeness (Raft Fig.8): a leader promoted at log end 10
        // (epoch_start_offset = 10) must NOT advance the HWM to a majority
        // offset that only covers prior-epoch entries (8 < 10).
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 10,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 1,
                vote_granted: true,
            },
            &log,
            SimInstant(2002),
        );
        assert2::assert!(matches!(
            m.role(),
            Role::Leader {
                epoch_start_offset: 10,
                ..
            }
        ));
        // follower 2 fetches at 8: majority of {10, 8} = 8, but 8 <= 10 → hold.
        let a2 = m.on_event(
            Event::ReceiveFetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 8,
            },
            &log,
            SimInstant(2100),
        );
        assert2::assert!(
            !a2.iter()
                .any(|a| matches!(a, Action::AdvanceHighWatermark(_)))
        );
        if let Role::Leader { high_watermark, .. } = m.role() {
            assert2::assert!(*high_watermark == 0);
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
            fn last_epoch(&self) -> Epoch {
                2
            }
            fn end_offset_for_epoch(&self, e: Epoch) -> Option<i64> {
                match e {
                    0 => Some(0),
                    1 => Some(5),
                    2 => Some(10),
                    _ => None,
                }
            }
        }
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = L;
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 1,
                vote_granted: true,
            },
            &log,
            SimInstant(2002),
        );
        // follower claims it fetched epoch 1 at offset 8, but epoch 1 ended at 5 → diverged.
        let actions = m.on_event(
            Event::ReceiveFetch {
                from: NodeId(2),
                fetch_epoch: 1,
                fetch_offset: 8,
            },
            &log,
            SimInstant(2100),
        );
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::TruncateTo(LogOffsetMetadata {
                offset: 5,
                epoch: 1
            })
        )));
    }

    #[test]
    fn prospective_counts_grant_with_no_wire_prevote_signal() {
        // A JVM voter's `VoteResponse` carries no pre-vote flag. The candidate
        // must still count the grant as a PRE-VOTE because it is Prospective —
        // this is the KIP-996 interop fix (was dropped by the old echo-tag path).
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000)); // → Prospective, epoch 0
        assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        // Pre-vote majority (self + 2) → promote to Candidate and bump the epoch.
        assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
        check!(m.quorum_state().leader_epoch == 1);
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendVoteRequest {
                pre_vote: false,
                epoch: 1
            }
        )));
    }

    #[test]
    fn stale_prevote_grant_ignored_after_promotion() {
        // A late pre-vote grant at the old epoch must not be miscounted toward
        // the real election once we have promoted to Candidate at epoch+1.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        ); // → Candidate @ epoch 1
        assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
        // A duplicate/late pre-vote grant still tagged epoch 0 arrives.
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(3),
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2002),
        );
        // Epoch guard (0 != 1) drops it: we stay Candidate, do NOT become leader.
        check!(
            (
                matches!(m.role(), Role::Candidate { .. }),
                m.role().is_leader(),
                actions.is_empty()
            ) == (true, false, true)
        );
        // The ignored stale grant must not have entered the real-vote tally:
        // after promotion the Candidate's grant set holds only our self-vote.
        if let Role::Candidate { granted, .. } = m.role() {
            assert2::assert!((granted.len(), granted.contains(&NodeId(3))) == (1, false));
        } else {
            panic!("expected Candidate");
        }
    }

    #[test]
    fn begin_quorum_epoch_from_adjacent_voter_view_is_accepted() {
        // KIP-853: a newly elected leader may be absent from our temporarily
        // stale local voter view. Adopt the higher epoch and fetch its log.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let _ = m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(99)]), SimInstant(0));
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(99), // not a voter
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(
            (
                m.quorum_state().leader_id,
                m.quorum_state().leader_epoch,
                matches!(m.role(), Role::Follower { .. }),
                actions.iter().any(|action| matches!(
                    action,
                    Action::SendFetch {
                        leader_id: NodeId(99)
                    }
                )),
            ) == (Some(NodeId(99)), 4, true, true)
        );
    }

    #[test]
    fn begin_quorum_epoch_from_voter_leader_still_accepted() {
        // C-2 must not break the legitimate path: a voter leader is adopted.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2), // a real voter
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert2::assert!(matches!(
            m.role(),
            Role::Follower {
                leader_id: NodeId(2),
                ..
            }
        ));
        check!(m.quorum_state().leader_id == Some(NodeId(2)));
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendFetch {
                leader_id: NodeId(2)
            }
        )));
    }

    #[test]
    fn vote_from_adjacent_voter_view_is_granted_when_up_to_date() {
        // KIP-853 permits an up-to-date candidate from an adjacent voter view;
        // only the local latest set determines whether this replica may vote.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let _ = m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(99)]), SimInstant(0));
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(99),
                voter_id: NodeId(1), // addressed to us
                candidate_epoch: 1,
                candidate: NodeId(99), // not a voter
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!(
            m.quorum_state()
                .voted_key
                .is_some_and(|key| key.id == NodeId(99))
        );
        assert2::assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::ReplyVote { granted: true, .. }))
        );
    }

    #[test]
    fn vote_addressed_to_other_voter_rejected() {
        // C-2: a Vote addressed (voter_id) to a different node than us is
        // ignored, even if the candidate is a legitimate voter.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(3), // addressed to node 3, not us (node 1)
                candidate_epoch: 1,
                candidate: NodeId(2),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!((actions.is_empty(), m.quorum_state().voted_key) == (true, None));
    }

    #[test]
    fn vote_from_voter_addressed_to_us_still_granted() {
        // C-2 must not break the legitimate path: a voter candidate addressing
        // us is still granted.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(1), // addressed to us
                candidate_epoch: 1,
                candidate: NodeId(2),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::ReplyVote {
                to: NodeId(2),
                granted: true,
                ..
            }
        )));
        assert2::assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(NodeId(2)));
    }

    #[test]
    fn follower_truncates_on_diverging_fetch_response() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 10,
            last_epoch: 2,
        };
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 3,
            },
            &log,
            SimInstant(10),
        );
        let actions = m.on_event(
            Event::ReceiveFetchResponse {
                leader_id: NodeId(2),
                leader_epoch: 3,
                diverging: Some(LogOffsetMetadata {
                    offset: 5,
                    epoch: 1,
                }),
            },
            &log,
            SimInstant(11),
        );
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::TruncateTo(LogOffsetMetadata {
                offset: 5,
                epoch: 1
            })
        )));
    }

    #[test]
    fn election_deadline_is_the_configured_extent_plus_integer_jitter() {
        // The constructor takes an extent, but the armed deadline must land on
        // exactly `now + base_ms + jitter_ms` — the same integer timeline the
        // verified jitter kernel and `SimInstant` are defined over. A rounding
        // shift here would change which node wins a deterministic election.
        for (name, timeout, base_ms) in [
            ("whole seconds", secs(1), 1000u64),
            ("sub-second extent", millis(250), 250),
        ] {
            let mut m = QuorumStateMachine::new(
                NodeId(1),
                QuorumState::bootstrap(
                    uuid::Uuid::nil(),
                    voters(&[NodeId(1), NodeId(2), NodeId(3)]),
                ),
                timeout,
            );
            let log = FakeLog {
                end: 5,
                last_epoch: 1,
            };
            let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
            let armed = actions.iter().find_map(|a| match a {
                Action::ResetTimer {
                    kind: TimerKind::Election,
                    deadline,
                } => Some(*deadline),
                _ => None,
            });
            let expected = SimInstant(2000 + base_ms + election_jitter_ms(NodeId(1), 0, base_ms));
            check!(armed == Some(expected), "case {name}");
        }
    }

    #[test]
    fn election_jitter_is_deterministic_hash_in_range() {
        // Pin the exact deterministic jitter so a constant-return regression
        // (no jitter at all → split-vote livelock) is caught. The values are the
        // integer hash of (node, epoch) mod base_ms; they are non-zero and
        // node-dependent, so both "always 0" and "always 1" are distinguished.
        for (name, node, epoch, base_ms, expected) in [
            ("first node", NodeId(1), 0, 1000, 485),
            ("different node", NodeId(2), 0, 1000, 354),
            ("next epoch", NodeId(1), 1, 1000, 446),
            ("zero base", NodeId(1), 0, 0, 0),
        ] {
            check!(
                election_jitter_ms(node, epoch, base_ms) == expected,
                "case {name}"
            );
        }
    }
}

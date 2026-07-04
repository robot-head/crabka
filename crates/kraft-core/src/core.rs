//! The `KRaft` quorum state machine: `on_event(event, log, now) -> Vec<Action>`.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    action::{Action, TimerKind},
    event::{Event, LogEnd},
    role::{ReplicaProgress, Role},
    types::{Epoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant},
};

/// Deterministic per-`(node, epoch)` election-timeout jitter in
/// `[0, base_ms)` — Raft's randomized backoff, made reproducible for the
/// deterministic sims. Different nodes (and the same node across re-election
/// epochs) get different spreads, so closely-synchronized voters don't arm their
/// election timers in lockstep and split the vote indefinitely. Shared by the
/// pure core and the async engine's initial timer arm so production self-staggers
/// without per-node config.
#[must_use]
pub fn election_jitter_ms(me: NodeId, epoch: Epoch, base_ms: u64) -> u64 {
    if base_ms == 0 {
        return 0;
    }
    // Cheap integer hash of (node id, epoch); avoids any RNG so the sims stay
    // deterministic.
    let mix = me.0.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(epoch).wrapping_mul(0xD1B5_4A32_D192_ED03);
    mix % base_ms
}

/// The hand-rolled KIP-595 + KIP-996 quorum state machine. Pure and
/// deterministic: it consumes [`Event`]s, reads the log through [`LogView`],
/// takes the current time as an injected [`SimInstant`], and produces a list of
/// [`Action`]s for the caller to execute. It never touches the clock, the wire,
/// or the log bytes directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

    #[cfg(test)]
    pub(crate) fn force_epoch(&mut self, e: Epoch) {
        self.state.leader_epoch = e;
    }

    /// `true` if `candidate_log` is at least as up-to-date as ours
    /// (KIP-595: higher last epoch wins; on tie, higher/equal offset wins).
    fn log_is_up_to_date(log: &dyn LogView, cand: LogEnd) -> bool {
        let my_epoch = log.last_epoch();
        let my_end = log.end_offset();
        cand.last_epoch > my_epoch || (cand.last_epoch == my_epoch && cand.last_offset >= my_end)
    }

    /// The deadline for an election timer armed at `now`. Adds deterministic
    /// per-`(node, epoch)` jitter (standard Raft randomized backoff, made
    /// deterministic for the sims) so competing voters do not arm their election
    /// timers in lockstep. Without this, a bare majority of in-process /
    /// closely-synchronized voters (e.g. exactly 2 of a 3-voter set) splits the
    /// vote every round — both become candidates, self-vote, and neither reaches
    /// majority — livelocking elections until natural skew breaks the tie.
    fn election_deadline(&self, now: SimInstant) -> SimInstant {
        now.saturating_add_ms(
            self.election_timeout_ms
                + election_jitter_ms(self.me, self.state.leader_epoch, self.election_timeout_ms),
        )
    }

    // `event` is taken by value: it models a consumed input message and hands
    // ownership to the machine. The current arms happen to only read Copy
    // fields, but future events can carry owned records/snapshots.
    #[allow(clippy::needless_pass_by_value)]
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
                log,
                from,
                voter_id,
                candidate_epoch,
                candidate,
                candidate_log_end,
                pre_vote,
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

    /// (Leader side) a follower fetched at `fetch_offset` claiming it last
    /// replicated up to `fetch_epoch`. If the follower's claimed epoch extends
    /// past where that epoch ends in our log, the logs diverged: reply with the
    /// truncation point. Otherwise record its progress and advance the HWM.
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
    /// own log end and every follower's acknowledged fetch offset, gated on the
    /// current leader epoch (Raft Fig.8 / KIP-595 leader completeness): the HWM
    /// may only advance once a *current-epoch* entry has been majority-replicated.
    /// We approximate that here by requiring the majority offset to be strictly
    /// past `epoch_start_offset` (where this leader's first current-epoch record
    /// sits). Otherwise the HWM is left unchanged. Never regresses.
    ///
    /// Full per-offset epoch validation happens against the durable log; the
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
        let mut match_offsets: Vec<i64> = Vec::with_capacity(replicas.len() + 1);
        match_offsets.push(log_end);
        for progress in replicas.values() {
            match_offsets.push(progress.fetch_offset);
        }
        // Sort descending; the majority-th largest sits at index `majority - 1`.
        match_offsets.sort_unstable_by(|a, b| b.cmp(a));
        let majority_offset = match_offsets[self.state.majority() - 1];
        // Leader-completeness gate: only commit once the current-epoch entry at
        // `epoch_start_offset` is itself majority-replicated. Until then, hold.
        let gated = if majority_offset > *epoch_start_offset {
            majority_offset
        } else {
            *high_watermark
        };
        // Monotonicity is intrinsic: the HWM never regresses. The
        // `majority()`-th offset can drop below the current HWM if a follower's
        // recorded `fetch_offset` legitimately falls (e.g. it truncated a
        // divergent suffix, or — in tests/models — a reordered stale fetch
        // arrives). Clamping here keeps the contract a property of this function
        // rather than of every caller's guard.
        let new_hwm = gated.max(*high_watermark);
        debug_assert!(
            new_hwm <= log_end,
            "HWM {new_hwm} must not exceed leader log end {log_end}"
        );
        new_hwm
    }

    /// (Follower side) the leader answered our Fetch. A diverging hint means we
    /// must truncate; otherwise we re-arm the fetch timer and fetch again.
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

    /// The fetch timer fired: a follower/observer lost contact with the leader.
    /// A voter starts an election; an observer just keeps trying to find a leader.
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

    /// A leader announced its epoch. If it is at least our current epoch, follow
    /// it (becoming `Follower`/an attached `Observer`); a stale (lower-epoch)
    /// announcement is ignored.
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
        // KIP-595 / leadership-hijack defense: only adopt a leader that belongs
        // to our current applied voter set. A peer with network access must not
        // be able to install an arbitrary `leader_id` we will then fetch from
        // and replicate metadata from. Guard on a NON-EMPTY voter set so a
        // bootstrapping node that has not yet learned its voters is unaffected
        // (it has no basis to reject), and so KIP-853 add/remove-voter — which
        // legitimately changes the set — is honored by reading the *current*
        // voter set rather than a stale view.
        if !self.state.voters.is_empty() && !self.state.voters.contains(leader_id) {
            tracing::warn!(
                rejected_leader = leader_id.0,
                leader_epoch,
                "rejecting BeginQuorumEpoch from non-voter leader (not in current voter set)"
            );
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

    /// A resigning leader asked us to start an election. If it is not stale, a
    /// voter immediately begins a pre-vote round (no waiting for the election
    /// timer); an observer simply detaches and keeps observing.
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

    /// The election timer fired. A voter begins a KIP-996 pre-vote round
    /// (becomes `Prospective`); an observer never elects.
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

    /// Shared election-start path (used by `ElectionTimeout` and a resigning
    /// leader's `EndQuorumEpoch`): become `Prospective` and broadcast a
    /// non-binding pre-vote at the *current* epoch (epoch is not bumped until the
    /// pre-vote succeeds).
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

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = from.0, voter_id = voter_id.0, candidate = candidate.0, candidate_epoch, pre_vote)
    )]
    fn handle_vote_request(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        voter_id: NodeId,
        candidate_epoch: Epoch,
        candidate: NodeId,
        cand_log: LogEnd,
        pre_vote: bool,
        now: SimInstant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        // Recipient-targeting check (KIP-595 / `KafkaRaftClient`): a Vote carries
        // the id of the voter it is addressed to. If it targets a different node
        // (a stale/misrouted/forged request), ignore it silently — do not even
        // reply, exactly as the JVM does. Only enforce once the addressing field
        // is meaningful: a `-1`/unset `voter_id` (decoded as 0) and the
        // bootstrap case where we have no voter set yet are not rejected here.
        if voter_id != self.me && voter_id != NodeId(0) {
            tracing::warn!(
                addressed_to = voter_id.0,
                me = self.me.0,
                "ignoring Vote addressed to a different voter"
            );
            return Vec::new();
        }
        // Leadership-hijack defense: only consider a vote from a candidate that
        // belongs to our current applied voter set. Guarded on a NON-EMPTY voter
        // set so a bootstrapping node is unaffected, and read from the current
        // set so KIP-853 reconfiguration is honored. A non-voter candidate is
        // ignored (no reply), mirroring the JVM which drops votes from replicas
        // it does not recognize as voters.
        if !self.state.voters.is_empty() && !self.state.voters.contains(candidate) {
            tracing::warn!(
                candidate = candidate.0,
                candidate_epoch,
                "ignoring Vote from non-voter candidate (not in current voter set)"
            );
            return Vec::new();
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
    /// A `LogView` whose `end_offset` can change between calls, so a test can
    /// model a leader promoted at a small log end (low `epoch_start_offset`)
    /// and then growing before followers fetch.
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
            1000,
        )
    }

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
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::ReplyVote {
                to: NodeId(2),
                granted: true,
                ..
            }
        )));
        assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(NodeId(2))); // binding
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
        assert!(
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
        assert!(m.quorum_state().voted_key.is_none()); // pre-vote does NOT persist
        assert!(m.quorum_state().leader_epoch == 0); // epoch unchanged
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
        assert!(actions.iter().any(|a| matches!(
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
        assert!(
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
        assert!(matches!(m.role(), Role::Prospective { .. }));
        assert!(
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
        assert!(matches!(m.role(), Role::Candidate { .. }));
        check!(m.quorum_state().leader_epoch == 1);
        check!(m.quorum_state().voted_key.map(|k| k.id) == Some(NodeId(1))); // self-vote
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
        check!(m.role().is_leader());
        check!(m.quorum_state().leader_id == Some(NodeId(1)));
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
        let mut m = machine(NodeId(99), &[NodeId(1), NodeId(2), NodeId(3)]); // 99 is not a voter
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
        assert!(matches!(
            m.role(),
            Role::Follower {
                leader_id: NodeId(2),
                ..
            }
        ));
        check!(m.quorum_state().leader_epoch == 4);
        check!(m.quorum_state().leader_id == Some(NodeId(2)));
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendFetch {
                leader_id: NodeId(2)
            }
        )));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PersistQuorumState))
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
        assert!(matches!(m.role(), Role::Prospective { .. }));
        assert!(
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
        assert!(actions.is_empty()); // lower epoch → ignored
        assert!(m.quorum_state().leader_id.is_none());
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
        assert!(matches!(
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
        assert!(
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
            assert!(*high_watermark == 8);
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
            assert!(*high_watermark == 8);
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
        assert!(
            !a.iter()
                .any(|x| matches!(x, Action::AdvanceHighWatermark(_)))
        );
        if let Role::Leader { high_watermark, .. } = m.role() {
            assert!(*high_watermark == 8);
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
        assert!(matches!(
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
        assert!(
            !a2.iter()
                .any(|a| matches!(a, Action::AdvanceHighWatermark(_)))
        );
        if let Role::Leader { high_watermark, .. } = m.role() {
            assert!(*high_watermark == 0);
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
        assert!(actions.iter().any(|a| matches!(
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
        assert!(matches!(m.role(), Role::Prospective { .. }));
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
        assert!(matches!(m.role(), Role::Candidate { .. }));
        check!(m.quorum_state().leader_epoch == 1);
        assert!(actions.iter().any(|a| matches!(
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
        assert!(matches!(m.role(), Role::Candidate { .. }));
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
        assert!(matches!(m.role(), Role::Candidate { .. }));
        check!(!m.role().is_leader());
        check!(actions.is_empty());
        // The ignored stale grant must not have entered the real-vote tally:
        // after promotion the Candidate's grant set holds only our self-vote.
        if let Role::Candidate { granted, .. } = m.role() {
            assert!(granted.len() == 1);
            assert!(!granted.contains(&NodeId(3)));
        } else {
            panic!("expected Candidate");
        }
    }

    #[test]
    fn begin_quorum_epoch_from_non_voter_leader_rejected() {
        // C-2: a BeginQuorumEpoch claiming a leader_id that is not in our
        // (non-empty) voter set must NOT be adopted — no leader installed, no
        // role transition, no actions.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
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
        check!(actions.is_empty());
        check!(m.quorum_state().leader_id.is_none());
        check!(m.quorum_state().leader_epoch == 0); // epoch not advanced
        assert!(!matches!(m.role(), Role::Follower { .. }));
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
        assert!(matches!(
            m.role(),
            Role::Follower {
                leader_id: NodeId(2),
                ..
            }
        ));
        check!(m.quorum_state().leader_id == Some(NodeId(2)));
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendFetch {
                leader_id: NodeId(2)
            }
        )));
    }

    #[test]
    fn vote_from_non_voter_candidate_not_granted() {
        // C-2: a Vote whose candidate is not in our (non-empty) voter set is
        // ignored — no reply, no vote recorded.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
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
        assert!(actions.is_empty());
        assert!(m.quorum_state().voted_key.is_none());
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
        assert!(actions.is_empty());
        assert!(m.quorum_state().voted_key.is_none());
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
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::ReplyVote {
                to: NodeId(2),
                granted: true,
                ..
            }
        )));
        assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(NodeId(2)));
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
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::TruncateTo(LogOffsetMetadata {
                offset: 5,
                epoch: 1
            })
        )));
    }

    #[test]
    fn election_jitter_is_deterministic_hash_in_range() {
        // Pin the exact deterministic jitter so a constant-return regression
        // (no jitter at all → split-vote livelock) is caught. The values are the
        // integer hash of (node, epoch) mod base_ms; they are non-zero and
        // node-dependent, so both "always 0" and "always 1" are distinguished.
        check!(election_jitter_ms(NodeId(1), 0, 1000) == 485);
        check!(election_jitter_ms(NodeId(2), 0, 1000) == 354); // different node → different spread
        check!(election_jitter_ms(NodeId(1), 1, 1000) == 446); // same node, next epoch → re-spread
        // Jitter must always stay strictly inside [0, base_ms).
        check!(election_jitter_ms(NodeId(1), 0, 1000) < 1000);
        // A zero base disables jitter entirely (guard branch): returns 0, not 1.
        check!(election_jitter_ms(NodeId(1), 0, 0) == 0);
    }
}

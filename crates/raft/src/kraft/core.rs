//! The `KRaft` quorum state machine: `on_event(event, log, now) -> Vec<Action>`.

use crate::kraft::action::Action;
use crate::kraft::event::{Event, LogEnd};
use crate::kraft::role::Role;
use crate::kraft::types::{LeaderEpoch, LogView, NodeId, QuorumState, ReplicaKey, SimInstant};

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
            // remaining arms added in Tasks 4–6
            _ => Vec::new(),
        }
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
}

//! Exhaustive stateright model of producer-side failover routing.
//!
//! The model checks the sender contract at the client/server seam: stale
//! metadata and a dead cached leader cannot make a prepared batch wedge forever.
//! The batch either reaches the freshly elected leader quickly or fails within a
//! bounded retry budget, and retry preserves producer identity.

#![allow(clippy::struct_excessive_bools)]

use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

use stateright::{Checker, Model, Property};

const BROKERS: [i32; 3] = [0, 1, 2];
const INITIAL_LEADER: i32 = 0;
const MAX_STALE_REFRESHES: u8 = 2;
const MAX_SENDS: u8 = 5;
const MAX_REFRESHES: u8 = 4;
const MAX_STEPS_AFTER_FAILOVER: u8 = 8;
const MAX_DEPTH: usize = 32;
const MAX_STATES: usize = 250_000;
const CHECK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Batch {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Outcome {
    Pending,
    Acked,
    Failed,
}

type StateProjection = (
    (i32, i32, Option<i32>, u8, u8, bool),
    (bool, u8, u8, u8, u8, u8, bool),
    (Outcome, Batch, Batch, bool),
);

#[derive(Clone, Debug)]
struct State {
    actual_leader: i32,
    cached_leader: i32,
    leader_hint: Option<i32>,
    live: u8,
    stale_refreshes_left: u8,
    refresh_needed: bool,
    failover_started: bool,
    steps_after_failover: u8,
    sends: u8,
    resends: u8,
    refreshes: u8,
    stale_leader_sends: u8,
    leader_hint_adopted: bool,
    outcome: Outcome,
    batch: Batch,
    // Immutable identity of the prepared batch; resend transitions must not rewrite it.
    prepared_identity: Batch,
    identity_preserved: bool,
}

impl State {
    fn broker_live(&self, broker: i32) -> bool {
        self.live & (1u8 << broker) != 0
    }

    fn terminal(&self) -> bool {
        self.outcome != Outcome::Pending
    }

    fn proj(&self) -> StateProjection {
        (
            (
                self.actual_leader,
                self.cached_leader,
                self.leader_hint,
                self.live,
                self.stale_refreshes_left,
                self.refresh_needed,
            ),
            (
                self.failover_started,
                self.steps_after_failover,
                self.sends,
                self.resends,
                self.refreshes,
                self.stale_leader_sends,
                self.leader_hint_adopted,
            ),
            (
                self.outcome,
                self.batch.clone(),
                self.prepared_identity.clone(),
                self.identity_preserved,
            ),
        )
    }
}

impl PartialEq for State {
    fn eq(&self, other: &Self) -> bool {
        self.proj() == other.proj()
    }
}

impl Eq for State {}

impl Hash for State {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.proj().hash(state);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Act {
    KillCachedLeader,
    ElectNewLeader(i32),
    Send,
    AdoptLeaderHint(i32),
    RefreshMetadata,
    ExpireBudget,
}

#[derive(Clone, Debug)]
struct ClientFailoverModel;

impl Model for ClientFailoverModel {
    type State = State;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            actual_leader: INITIAL_LEADER,
            cached_leader: INITIAL_LEADER,
            leader_hint: None,
            live: 0b111,
            stale_refreshes_left: MAX_STALE_REFRESHES,
            refresh_needed: false,
            failover_started: false,
            steps_after_failover: 0,
            sends: 0,
            resends: 0,
            refreshes: 0,
            stale_leader_sends: 0,
            leader_hint_adopted: false,
            outcome: Outcome::Pending,
            batch: Batch {
                producer_id: 1,
                producer_epoch: 0,
                base_sequence: 0,
            },
            prepared_identity: Batch {
                producer_id: 1,
                producer_epoch: 0,
                base_sequence: 0,
            },
            identity_preserved: true,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        if s.terminal() {
            return;
        }
        if !s.failover_started && s.broker_live(s.cached_leader) {
            acts.push(Act::KillCachedLeader);
        }
        for broker in BROKERS {
            if broker != s.actual_leader && s.broker_live(broker) {
                acts.push(Act::ElectNewLeader(broker));
            }
        }
        if !s.refresh_needed && s.sends < MAX_SENDS {
            acts.push(Act::Send);
        }
        if let Some(broker) = s.leader_hint
            && s.refresh_needed
            && s.broker_live(broker)
        {
            acts.push(Act::AdoptLeaderHint(broker));
        }
        if s.refresh_needed && s.refreshes < MAX_REFRESHES {
            acts.push(Act::RefreshMetadata);
        }
        if s.sends >= MAX_SENDS || s.refreshes >= MAX_REFRESHES {
            acts.push(Act::ExpireBudget);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        if s.failover_started && !s.terminal() {
            s.steps_after_failover = s.steps_after_failover.saturating_add(1);
            if s.steps_after_failover > MAX_STEPS_AFTER_FAILOVER {
                s.outcome = Outcome::Failed;
                return Some(s);
            }
        }

        match action {
            Act::KillCachedLeader => {
                s.live &= !(1u8 << s.cached_leader);
                s.failover_started = true;
            }
            Act::ElectNewLeader(broker) => {
                if !s.broker_live(broker) {
                    return None;
                }
                s.actual_leader = broker;
                s.failover_started = true;
            }
            Act::Send => {
                if s.refresh_needed {
                    return None;
                }
                if s.sends > 0 {
                    s.resends = s.resends.saturating_add(1);
                }
                s.sends = s.sends.saturating_add(1);
                let target = s.cached_leader;
                if !s.broker_live(target) {
                    s.stale_leader_sends = s.stale_leader_sends.saturating_add(1);
                    s.refresh_needed = true;
                } else if target == s.actual_leader {
                    s.outcome = Outcome::Acked;
                } else {
                    s.leader_hint = Some(s.actual_leader);
                    s.refresh_needed = true;
                }
                s.identity_preserved = s.batch == s.prepared_identity;
            }
            Act::AdoptLeaderHint(broker) => {
                if s.leader_hint != Some(broker) || !s.broker_live(broker) {
                    return None;
                }
                s.cached_leader = broker;
                s.leader_hint = None;
                s.refresh_needed = false;
                s.leader_hint_adopted = true;
            }
            Act::RefreshMetadata => {
                s.refreshes = s.refreshes.saturating_add(1);
                if s.stale_refreshes_left > 0 {
                    s.stale_refreshes_left -= 1;
                    s.refresh_needed = true;
                } else if s.broker_live(s.actual_leader) {
                    s.cached_leader = s.actual_leader;
                    s.leader_hint = None;
                    s.refresh_needed = false;
                } else {
                    s.leader_hint = None;
                    s.refresh_needed = true;
                }
            }
            Act::ExpireBudget => {
                s.outcome = Outcome::Failed;
            }
        }

        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("identity_preserved", |_, s: &State| s.identity_preserved),
            Property::always("send_attempts_capped", |_, s: &State| s.sends <= MAX_SENDS),
            Property::always("refreshes_capped", |_, s: &State| {
                s.refreshes <= MAX_REFRESHES
            }),
            Property::always("quick_recovery_or_failure", |_, s: &State| {
                !s.failover_started
                    || s.terminal()
                    || s.steps_after_failover <= MAX_STEPS_AFTER_FAILOVER
            }),
            Property::always("bounded_dead_leader_churn", |_, s: &State| {
                s.stale_leader_sends <= 2
            }),
            Property::sometimes("stale_metadata_observed", |_, s: &State| {
                s.refreshes > 0 && s.stale_refreshes_left < MAX_STALE_REFRESHES
            }),
            Property::sometimes("dead_leader_attempted", |_, s: &State| {
                s.stale_leader_sends > 0
            }),
            Property::sometimes("acked_after_failover", |_, s: &State| {
                s.failover_started && s.outcome == Outcome::Acked
            }),
            Property::sometimes("budget_expiry_reachable", |_, s: &State| {
                s.outcome == Outcome::Failed
            }),
            Property::sometimes("leader_hint_fast_reroute", |_, s: &State| {
                s.leader_hint_adopted && s.refreshes == 0 && s.outcome == Outcome::Acked
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.sends <= MAX_SENDS
            && s.refreshes <= MAX_REFRESHES
            && (s.terminal() || s.steps_after_failover <= MAX_STEPS_AFTER_FAILOVER + 1)
            && s.stale_refreshes_left <= MAX_STALE_REFRESHES
    }
}

fn run_model() {
    let checker = ClientFailoverModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[client_failover] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    checker.assert_properties();
}

#[test]
fn client_failover_recovers_or_fails_boundedly() {
    run_model();
}

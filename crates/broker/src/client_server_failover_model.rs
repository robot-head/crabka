//! Bounded stateright model composing producer-client failover routing with the
//! broker-side idempotent-producer dedup check.

use std::time::Duration;

use crate::producer_state::{Decision, ProducerEntry, check_pure};
use stateright::{Checker, Model, Property};

const NB: usize = 3;
const INITIAL_LEADER: usize = 0;
const PRODUCER_ID: i64 = 1;
const PRODUCER_EPOCH: i16 = 0;
const BASE_SEQUENCE: i32 = 0;
const BASE_OFFSET: i64 = 0;
const MAX_LOG_LEN: usize = 1;
const MAX_HWM: u8 = 1;
const MAX_DEPTH: usize = 36;
const MAX_STATES: usize = 400_000;
const CHECK_TIMEOUT: Duration = Duration::from_secs(45);
const WITNESS_DUPLICATE_RESPONSE: u8 = 1 << 0;
const WITNESS_FAILOVER: u8 = 1 << 1;
const WITNESS_RETRY: u8 = 1 << 2;
const WITNESS_RETRY_AFTER_FAILOVER: u8 = 1 << 3;
const WITNESS_PREPARED_RETRY: u8 = 1 << 4;
const WITNESS_ACKED_BEFORE_FAILOVER: u8 = 1 << 5;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ClientServerFailoverModel;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct LogBatch {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct AcceptedBatch {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct ProducerEntryProjection {
    epoch: i16,
    last_sequence: i32,
    base_offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum BatchState {
    Empty,
    Prepared,
    Appended,
    Acked,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Witnesses(u8);

impl Witnesses {
    fn mark(&mut self, bit: u8) {
        self.0 |= bit;
    }

    fn seen(self, bit: u8) -> bool {
        self.0 & bit != 0
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct FailoverState {
    logs: [[Option<LogBatch>; MAX_LOG_LEN]; NB],
    leader: usize,
    live: u8,
    hwm: u8,
    cached_leader: usize,
    refresh_needed: bool,
    batch: BatchState,
    next_sequence: i32,
    accepted: Option<AcceptedBatch>,
    producer_entry: Option<ProducerEntryProjection>,
    acked_offset: Option<i64>,
    witnesses: Witnesses,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Action {
    ClientSend,
    ClientRetry,
    Replicate(usize),
    AdvanceHwm,
    AckCommitted,
    KillLeader,
    ElectClean(usize),
    RefreshMetadata,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SendKind {
    Send,
    Retry,
}

impl LogBatch {
    fn initial() -> Self {
        Self {
            producer_id: PRODUCER_ID,
            producer_epoch: PRODUCER_EPOCH,
            base_sequence: BASE_SEQUENCE,
            offset: BASE_OFFSET,
        }
    }
}

impl ProducerEntryProjection {
    fn as_entry(self) -> ProducerEntry {
        ProducerEntry {
            epoch: self.epoch,
            last_sequence: self.last_sequence,
            last_offset: self.base_offset,
            base_offset: self.base_offset,
            last_timestamp: 0,
            last_activity_ms: 0,
        }
    }
}

impl FailoverState {
    fn live(&self, broker: usize) -> bool {
        self.live & (1 << broker) != 0
    }

    fn live_count(&self) -> u32 {
        self.live.count_ones()
    }

    fn log_len(&self, broker: usize) -> usize {
        self.logs[broker].iter().flatten().count()
    }

    fn log_contains_base(&self, broker: usize) -> bool {
        self.logs[broker]
            .iter()
            .flatten()
            .any(|batch| batch.producer_id == PRODUCER_ID && batch.base_sequence == BASE_SEQUENCE)
    }

    fn contains_hwm_prefix(&self, broker: usize) -> bool {
        self.hwm == 0 || self.log_len(broker) >= usize::from(self.hwm)
    }

    fn hwm_prefix_replicated(&self) -> bool {
        self.live(self.leader)
            && self.log_len(self.leader) > 0
            && (0..NB)
                .filter(|broker| self.live(*broker) && self.logs[*broker] == self.logs[self.leader])
                .count()
                >= 2
    }

    fn producer_entry(&self) -> Option<ProducerEntry> {
        self.producer_entry.map(ProducerEntryProjection::as_entry)
    }

    fn producer_entry_for_broker(&self, broker: usize) -> Option<ProducerEntryProjection> {
        let batch = self.logs[broker].iter().flatten().next()?;
        if batch.producer_id != PRODUCER_ID {
            return None;
        }
        Some(ProducerEntryProjection {
            epoch: batch.producer_epoch,
            last_sequence: batch.base_sequence,
            base_offset: batch.offset,
        })
    }

    fn refresh_leader_producer_entry(&mut self) {
        self.producer_entry = self.producer_entry_for_broker(self.leader);
    }

    fn leader_contains_accepted(&self) -> bool {
        let Some(accepted) = self.accepted else {
            return false;
        };
        self.logs[self.leader].iter().flatten().any(|batch| {
            batch.producer_id == accepted.producer_id
                && batch.producer_epoch == accepted.producer_epoch
                && batch.base_sequence == accepted.base_sequence
                && batch.offset == accepted.offset
        })
    }

    fn can_ack_committed(&self) -> bool {
        self.batch == BatchState::Appended
            && self.acked_offset.is_none()
            && self.hwm == 1
            && self.live(self.leader)
            && self.leader_contains_accepted()
    }

    fn mark_failover(&mut self) {
        if self.acked_offset.is_some() {
            self.witnesses.mark(WITNESS_ACKED_BEFORE_FAILOVER);
        }
        self.witnesses.mark(WITNESS_FAILOVER);
    }

    fn apply_client_send(&self, kind: SendKind) -> Option<Self> {
        let mut s = self.clone();
        let base_sequence = match s.batch {
            BatchState::Empty => {
                if kind == SendKind::Retry {
                    return None;
                }
                s.batch = BatchState::Prepared;
                s.next_sequence
            }
            BatchState::Prepared => {
                if kind == SendKind::Retry {
                    s.witnesses.mark(WITNESS_RETRY);
                    s.witnesses.mark(WITNESS_PREPARED_RETRY);
                    if s.witnesses.seen(WITNESS_FAILOVER) {
                        s.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                    }
                }
                BASE_SEQUENCE
            }
            BatchState::Appended | BatchState::Acked => {
                if kind == SendKind::Send {
                    return None;
                }
                s.witnesses.mark(WITNESS_RETRY);
                if s.witnesses.seen(WITNESS_FAILOVER) {
                    s.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                }
                BASE_SEQUENCE
            }
            BatchState::Failed => return None,
        };

        if s.cached_leader != s.leader || !s.live(s.cached_leader) {
            s.refresh_needed = true;
            return Some(s);
        }

        s.refresh_leader_producer_entry();
        let entry = s.producer_entry();
        match check_pure(entry.as_ref(), PRODUCER_EPOCH, base_sequence) {
            Decision::Append => {
                if s.log_len(s.leader) >= MAX_LOG_LEN || (s.hwm > 0 && s.accepted.is_some()) {
                    return None;
                }
                let batch = LogBatch::initial();
                s.logs[s.leader][0] = Some(batch);
                s.accepted = Some(AcceptedBatch {
                    producer_id: PRODUCER_ID,
                    producer_epoch: PRODUCER_EPOCH,
                    base_sequence,
                    offset: BASE_OFFSET,
                });
                s.producer_entry = Some(ProducerEntryProjection {
                    epoch: PRODUCER_EPOCH,
                    last_sequence: base_sequence,
                    base_offset: BASE_OFFSET,
                });
                s.batch = BatchState::Appended;
                s.next_sequence = base_sequence + 1;
                Some(s)
            }
            Decision::Duplicate { base_offset } => {
                let accepted = s.accepted?;
                if accepted.producer_id != PRODUCER_ID
                    || accepted.producer_epoch != PRODUCER_EPOCH
                    || accepted.base_sequence != base_sequence
                    || accepted.offset != base_offset
                {
                    return None;
                }
                if s.batch != BatchState::Acked || !s.leader_contains_accepted() {
                    return None;
                }
                s.acked_offset = Some(base_offset);
                s.witnesses.mark(WITNESS_DUPLICATE_RESPONSE);
                Some(s)
            }
            Decision::OutOfOrder | Decision::Fenced => {
                s.batch = BatchState::Failed;
                Some(s)
            }
        }
    }
}

impl Model for ClientServerFailoverModel {
    type State = FailoverState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![FailoverState {
            logs: [[None; MAX_LOG_LEN]; NB],
            leader: INITIAL_LEADER,
            live: (1 << NB) - 1,
            hwm: 0,
            cached_leader: INITIAL_LEADER,
            refresh_needed: false,
            batch: BatchState::Empty,
            next_sequence: BASE_SEQUENCE,
            accepted: None,
            producer_entry: None,
            acked_offset: None,
            witnesses: Witnesses(0),
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if matches!(s.batch, BatchState::Empty | BatchState::Prepared) {
            actions.push(Action::ClientSend);
        }
        if matches!(
            s.batch,
            BatchState::Prepared | BatchState::Appended | BatchState::Acked
        ) {
            actions.push(Action::ClientRetry);
        }
        if s.live(s.leader) {
            for broker in 0..NB {
                if broker != s.leader
                    && s.live(broker)
                    && s.log_len(s.leader) > 0
                    && s.logs[broker] != s.logs[s.leader]
                {
                    actions.push(Action::Replicate(broker));
                }
            }
        }
        if s.hwm == 0 && s.hwm_prefix_replicated() {
            actions.push(Action::AdvanceHwm);
        }
        if s.can_ack_committed() {
            actions.push(Action::AckCommitted);
        }
        if s.live(s.leader) && s.live_count() > 1 {
            actions.push(Action::KillLeader);
        }
        for broker in 0..NB {
            if broker != s.leader && s.live(broker) && s.contains_hwm_prefix(broker) {
                actions.push(Action::ElectClean(broker));
            }
        }
        if s.refresh_needed || s.cached_leader != s.leader || !s.live(s.cached_leader) {
            actions.push(Action::RefreshMetadata);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::ClientSend => last.apply_client_send(SendKind::Send),
            Action::ClientRetry => last.apply_client_send(SendKind::Retry),
            Action::Replicate(follower) => {
                if follower == s.leader || !s.live(s.leader) || !s.live(follower) {
                    return None;
                }
                let leader_log = s.logs[s.leader];
                if s.log_len(s.leader) == 0 || s.logs[follower] == leader_log {
                    return None;
                }
                s.logs[follower] = leader_log;
                Some(s)
            }
            Action::AdvanceHwm => {
                if !s.live(s.leader) || s.hwm != 0 || !s.hwm_prefix_replicated() {
                    return None;
                }
                s.hwm = 1;
                Some(s)
            }
            Action::AckCommitted => {
                if !s.can_ack_committed() {
                    return None;
                }
                s.acked_offset = Some(BASE_OFFSET);
                s.batch = BatchState::Acked;
                Some(s)
            }
            Action::KillLeader => {
                if !s.live(s.leader) || s.live_count() <= 1 {
                    return None;
                }
                s.live &= !(1 << s.leader);
                s.refresh_needed = true;
                s.mark_failover();
                Some(s)
            }
            Action::ElectClean(follower) => {
                if follower == s.leader || !s.live(follower) || !s.contains_hwm_prefix(follower) {
                    return None;
                }
                s.leader = follower;
                s.refresh_needed = s.cached_leader != s.leader || !s.live(s.cached_leader);
                s.refresh_leader_producer_entry();
                s.mark_failover();
                Some(s)
            }
            Action::RefreshMetadata => {
                if !s.live(s.leader) {
                    return None;
                }
                s.cached_leader = s.leader;
                s.refresh_needed = false;
                s.refresh_leader_producer_entry();
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("acked_all_durable", |_, s: &FailoverState| {
                s.acked_offset.is_none_or(|offset| {
                    offset >= i64::from(s.hwm)
                        || s.logs[s.leader].iter().flatten().any(|batch| {
                            batch.producer_id == PRODUCER_ID
                                && batch.offset == offset
                                && batch.base_sequence == BASE_SEQUENCE
                        })
                })
            }),
            Property::always("ack_requires_hwm", |_, s: &FailoverState| {
                s.acked_offset.is_none_or(|offset| {
                    s.hwm == 1
                        && s.logs[s.leader].iter().flatten().any(|batch| {
                            batch.producer_id == PRODUCER_ID
                                && batch.offset == offset
                                && batch.base_sequence == BASE_SEQUENCE
                        })
                })
            }),
            Property::always("no_duplicate_append", |_, s: &FailoverState| {
                s.logs.iter().all(|log| {
                    log.iter()
                        .flatten()
                        .filter(|batch| {
                            batch.producer_id == PRODUCER_ID && batch.base_sequence == BASE_SEQUENCE
                        })
                        .count()
                        <= 1
                })
            }),
            Property::always("no_sequence_skip_on_reroute", |_, s: &FailoverState| {
                matches!(s.next_sequence, BASE_SEQUENCE..=1)
            }),
            Property::always(
                "clean_leader_contains_hwm_prefix",
                |_, s: &FailoverState| s.log_len(s.leader) >= usize::from(s.hwm),
            ),
            Property::sometimes("ack_before_failover", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_ACKED_BEFORE_FAILOVER)
            }),
            Property::sometimes("retry_after_failover", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_RETRY_AFTER_FAILOVER)
            }),
            Property::sometimes("prepared_retry", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_PREPARED_RETRY)
            }),
            Property::sometimes("duplicate_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_DUPLICATE_RESPONSE)
            }),
            Property::sometimes("clean_failover_preserves_ack", |_, s: &FailoverState| {
                s.leader != INITIAL_LEADER
                    && s.live(s.leader)
                    && s.hwm == 1
                    && s.acked_offset == Some(BASE_OFFSET)
                    && s.log_contains_base(s.leader)
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.logs
            .iter()
            .all(|log| log.iter().flatten().count() <= MAX_LOG_LEN)
            && s.hwm <= MAX_HWM
            && s.next_sequence <= BASE_SEQUENCE + 1
            && usize::from(s.live) < (1 << NB)
    }
}

fn run_model() {
    let checker = ClientServerFailoverModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[client_server_failover] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "client_server_failover depth cap hit"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "client_server_failover truncated"
    );
    checker.assert_properties();
}

#[test]
fn client_server_failover_preserves_acked_batch() {
    run_model();
}

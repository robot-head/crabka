//! Bounded stateright model composing producer-client failover routing with the
//! broker-side idempotent-producer dedup check.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use crate::producer_state::{Decision, ProducerEntry, check_pure};

const NB: usize = 3;
const INITIAL_LEADER: usize = 0;
const PRODUCER_ID: i64 = 1;
const PRODUCER_EPOCH: i16 = 0;
const BASE_SEQUENCE: i32 = 0;
const BASE_OFFSET: i64 = 0;
const MAX_LOG_LEN: usize = 1;
const MAX_HWM: u8 = 1;
const MAX_DEPTH: usize = 36;
const MAX_STATES: usize = 120_000;
const CHECK_TIMEOUT: Duration = Duration::from_secs(45);
const WITNESS_DUPLICATE_RESPONSE: u16 = 1 << 0;
const WITNESS_FAILOVER: u16 = 1 << 1;
const WITNESS_RETRY: u16 = 1 << 2;
const WITNESS_RETRY_AFTER_FAILOVER: u16 = 1 << 3;
const WITNESS_PREPARED_RETRY: u16 = 1 << 4;
const WITNESS_ACKED_BEFORE_FAILOVER: u16 = 1 << 5;
const WITNESS_NOT_LEADER: u16 = 1 << 8;
const WITNESS_TIMED_OUT_UNKNOWN: u16 = 1 << 9;
const WITNESS_APPENDED_UNACKED: u16 = 1 << 10;
const WITNESS_DUPLICATE_AFTER_UNKNOWN: u16 = 1 << 11;
const WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER: u16 = 1 << 12;
const MAX_SEND_ATTEMPTS: u8 = 4;
const MAX_METADATA_REFRESHES: u8 = 4;

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
enum ProduceResult {
    NotLeader,
    TimedOutUnknown,
    AppendedUnacked,
    Acked,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum RequestOutcome {
    NotLeader,
    AppendedUnacked,
    TimedOutUnknown,
    Duplicate,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Witnesses(u16);

impl Witnesses {
    fn mark(&mut self, bit: u16) {
        self.0 |= bit;
    }

    fn seen(self, bit: u16) -> bool {
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
    last_result: Option<ProduceResult>,
    send_attempts: u8,
    metadata_refreshes: u8,
    witnesses: Witnesses,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Action {
    ClientSend(RequestOutcome),
    ClientRetry(RequestOutcome),
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

    fn cached_leader_current(&self) -> bool {
        self.cached_leader == self.leader && self.live(self.cached_leader)
    }

    fn can_try_duplicate(&self) -> bool {
        self.accepted.is_some() && self.cached_leader_current() && self.leader_contains_accepted()
    }

    fn mark_failover(&mut self) {
        if self.acked_offset.is_some() {
            self.witnesses.mark(WITNESS_ACKED_BEFORE_FAILOVER);
        }
        self.witnesses.mark(WITNESS_FAILOVER);
    }

    fn request_base_sequence(&mut self, kind: SendKind) -> Option<i32> {
        let base_sequence = match self.batch {
            BatchState::Empty => {
                if kind == SendKind::Retry {
                    return None;
                }
                BASE_SEQUENCE
            }
            BatchState::Prepared => {
                if kind == SendKind::Retry {
                    self.witnesses.mark(WITNESS_RETRY);
                    self.witnesses.mark(WITNESS_PREPARED_RETRY);
                    if self.witnesses.seen(WITNESS_FAILOVER) {
                        self.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                    }
                }
                BASE_SEQUENCE
            }
            BatchState::Appended | BatchState::Acked => {
                if kind == SendKind::Send {
                    return None;
                }
                self.witnesses.mark(WITNESS_RETRY);
                if self.witnesses.seen(WITNESS_FAILOVER) {
                    self.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                }
                BASE_SEQUENCE
            }
            BatchState::Failed => return None,
        };
        Some(base_sequence)
    }

    fn apply_client_send(&self, kind: SendKind, outcome: RequestOutcome) -> Option<Self> {
        if self.send_attempts >= MAX_SEND_ATTEMPTS {
            return None;
        }

        let mut s = self.clone();
        let base_sequence = s.request_base_sequence(kind)?;
        s.send_attempts = s.send_attempts.saturating_add(1);
        if s.batch == BatchState::Empty {
            s.batch = BatchState::Prepared;
        }

        if !s.cached_leader_current() {
            if outcome != RequestOutcome::NotLeader {
                return None;
            }
            s.refresh_needed = true;
            s.last_result = Some(ProduceResult::NotLeader);
            s.witnesses.mark(WITNESS_NOT_LEADER);
            return Some(s);
        } else if outcome == RequestOutcome::NotLeader {
            return None;
        }

        s.refresh_leader_producer_entry();
        let entry = s.producer_entry();
        match check_pure(entry.as_ref(), PRODUCER_EPOCH, base_sequence, 0) {
            Decision::Append => {
                if !matches!(
                    outcome,
                    RequestOutcome::AppendedUnacked | RequestOutcome::TimedOutUnknown
                ) {
                    return None;
                }
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
                match outcome {
                    RequestOutcome::AppendedUnacked => {
                        s.last_result = Some(ProduceResult::AppendedUnacked);
                        s.witnesses.mark(WITNESS_APPENDED_UNACKED);
                    }
                    RequestOutcome::TimedOutUnknown => {
                        s.last_result = Some(ProduceResult::TimedOutUnknown);
                        s.witnesses.mark(WITNESS_TIMED_OUT_UNKNOWN);
                    }
                    RequestOutcome::NotLeader | RequestOutcome::Duplicate => return None,
                }
                Some(s)
            }
            Decision::Duplicate { base_offset } => {
                if outcome != RequestOutcome::Duplicate {
                    return None;
                }
                let accepted = s.accepted?;
                if accepted.producer_id != PRODUCER_ID
                    || accepted.producer_epoch != PRODUCER_EPOCH
                    || accepted.base_sequence != base_sequence
                    || accepted.offset != base_offset
                {
                    return None;
                }
                if !s.leader_contains_accepted() {
                    return None;
                }
                s.witnesses.mark(WITNESS_DUPLICATE_RESPONSE);
                if self.last_result == Some(ProduceResult::TimedOutUnknown) {
                    s.witnesses.mark(WITNESS_DUPLICATE_AFTER_UNKNOWN);
                    if self.witnesses.seen(WITNESS_FAILOVER) {
                        s.witnesses.mark(WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER);
                    }
                }
                if s.hwm == 1 {
                    s.acked_offset = Some(base_offset);
                    s.batch = BatchState::Acked;
                    s.last_result = Some(ProduceResult::Acked);
                } else {
                    s.batch = BatchState::Appended;
                    s.last_result = Some(ProduceResult::AppendedUnacked);
                    s.witnesses.mark(WITNESS_APPENDED_UNACKED);
                }
                Some(s)
            }
            Decision::OutOfOrder | Decision::Fenced => {
                s.batch = BatchState::Failed;
                Some(s)
            }
        }
    }
}

impl ClientServerFailoverModel {
    fn safety_properties() -> Vec<Property<Self>> {
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
                "not_leader_before_append_preserves_sequence",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::NotLeader)
                        || s.accepted.is_some()
                        || s.next_sequence == BASE_SEQUENCE
                },
            ),
            Property::always(
                "appended_unacked_not_acknowledged",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::AppendedUnacked)
                        || s.acked_offset.is_none()
                },
            ),
            Property::always(
                "acked_result_requires_committed_leader",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::Acked)
                        || (s.hwm == 1
                            && s.acked_offset == Some(BASE_OFFSET)
                            && s.leader_contains_accepted())
                },
            ),
            Property::always(
                "unknown_timeout_records_acceptance",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::TimedOutUnknown)
                        || (s.accepted
                            == Some(AcceptedBatch {
                                producer_id: PRODUCER_ID,
                                producer_epoch: PRODUCER_EPOCH,
                                base_sequence: BASE_SEQUENCE,
                                offset: BASE_OFFSET,
                            })
                            && s.next_sequence == BASE_SEQUENCE + 1)
                },
            ),
            Property::always("send_attempts_capped", |_, s: &FailoverState| {
                s.send_attempts <= MAX_SEND_ATTEMPTS
            }),
            Property::always("metadata_refreshes_capped", |_, s: &FailoverState| {
                s.metadata_refreshes <= MAX_METADATA_REFRESHES
            }),
            Property::always(
                "clean_leader_contains_hwm_prefix",
                |_, s: &FailoverState| s.log_len(s.leader) >= usize::from(s.hwm),
            ),
        ]
    }

    fn witness_properties() -> Vec<Property<Self>> {
        vec![
            Property::sometimes("not_leader_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_NOT_LEADER)
            }),
            Property::sometimes("timed_out_unknown_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_TIMED_OUT_UNKNOWN)
            }),
            Property::sometimes("appended_unacked_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_APPENDED_UNACKED)
            }),
            Property::sometimes("duplicate_after_unknown", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_DUPLICATE_AFTER_UNKNOWN)
            }),
            Property::sometimes("unknown_retry_after_failover", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER)
            }),
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
            last_result: None,
            send_attempts: 0,
            metadata_refreshes: 0,
            witnesses: Witnesses(0),
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        let routed = s.cached_leader_current();
        if s.send_attempts < MAX_SEND_ATTEMPTS && s.batch == BatchState::Empty {
            if routed {
                actions.push(Action::ClientSend(RequestOutcome::AppendedUnacked));
                actions.push(Action::ClientSend(RequestOutcome::TimedOutUnknown));
            } else {
                actions.push(Action::ClientSend(RequestOutcome::NotLeader));
            }
        }
        if s.send_attempts < MAX_SEND_ATTEMPTS
            && matches!(
                s.batch,
                BatchState::Prepared | BatchState::Appended | BatchState::Acked
            )
        {
            if routed {
                if s.can_try_duplicate() {
                    actions.push(Action::ClientRetry(RequestOutcome::Duplicate));
                } else {
                    actions.push(Action::ClientRetry(RequestOutcome::AppendedUnacked));
                    actions.push(Action::ClientRetry(RequestOutcome::TimedOutUnknown));
                }
            } else {
                actions.push(Action::ClientRetry(RequestOutcome::NotLeader));
            }
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
            if broker != s.leader
                && s.live(broker)
                && s.contains_hwm_prefix(broker)
                && (!s.witnesses.seen(WITNESS_FAILOVER) || !s.live(s.leader))
            {
                actions.push(Action::ElectClean(broker));
            }
        }
        if (s.refresh_needed || s.cached_leader != s.leader || !s.live(s.cached_leader))
            && s.metadata_refreshes < MAX_METADATA_REFRESHES
        {
            actions.push(Action::RefreshMetadata);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::ClientSend(outcome) => last.apply_client_send(SendKind::Send, outcome),
            Action::ClientRetry(outcome) => last.apply_client_send(SendKind::Retry, outcome),
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
                s.last_result = Some(ProduceResult::Acked);
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
                if s.metadata_refreshes >= MAX_METADATA_REFRESHES {
                    return None;
                }
                s.metadata_refreshes = s.metadata_refreshes.saturating_add(1);
                s.cached_leader = s.leader;
                s.refresh_needed = false;
                s.refresh_leader_producer_entry();
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = Self::safety_properties();
        properties.extend(Self::witness_properties());
        properties
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.logs
            .iter()
            .all(|log| log.iter().flatten().count() <= MAX_LOG_LEN)
            && s.hwm <= MAX_HWM
            && s.next_sequence <= BASE_SEQUENCE + 1
            && s.send_attempts <= MAX_SEND_ATTEMPTS
            && s.metadata_refreshes <= MAX_METADATA_REFRESHES
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

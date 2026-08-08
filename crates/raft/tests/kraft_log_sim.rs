//! Slice 3b headline acceptance: the slice-3a consensus core drives a real,
//! on-disk [`KraftLog`] in the deterministic multi-node simulation.
//!
//! This reuses the exact `Sim` scheduler and action translation from the shared
//! [`sim_harness`] module, which is the same code path that `kraft_sim.rs`
//! validates over the in-memory fake. It plugs in a [`KraftBackedLog`] instead:
//! one `KraftLog` per node, each in its own `tempfile::tempdir()`. The test
//! exercises election, pull-replication, and divergence-truncation against
//! genuine byte-level log I/O, and the asserts compare the committed bytes and
//! not only the offsets.

mod sim_harness;

use std::cell::RefCell;

use crabka_ids::{NodeId, Offset};
use crabka_protocol::records::{Attributes, Record, RecordBatch};
use crabka_raft::kraft::{
    KraftLog,
    types::{Epoch, LogView},
};
use crabka_units::prelude::{ByteSize, gibibytes};
use sim_harness::{Sim, SimNodeLog};

/// A read budget larger than any log this simulation builds, so every read
/// returns the whole log.
const UNBOUNDED_READ: ByteSize = gibibytes(1);

// --------------------------------------------------------------------------
// A real-KraftLog-backed per-node log for the simulation harness.
// --------------------------------------------------------------------------

/// Wraps a real [`KraftLog`] so that it satisfies the [`SimNodeLog`] trait of
/// the harness. Each node owns its own tempdir, which stays alive for the
/// lifetime of the log.
struct KraftBackedLog {
    log: KraftLog,
    /// Keeps the per-node temp directory alive. It drops with the log.
    _dir: tempfile::TempDir,
}

impl KraftBackedLog {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open KraftLog");
        Self { log, _dir: dir }
    }

    /// All batches currently in the log, decoded. This simulation puts one
    /// record in each batch, so there is one batch per offset.
    fn decoded(&self) -> Vec<RecordBatch> {
        let end = self.log.log_end_offset();
        if end <= 0 {
            return Vec::new();
        }
        self.log
            .read_decoded(Offset(0), UNBOUNDED_READ)
            .expect("read_decoded")
    }
}

/// Builds a single-record batch stamped with `epoch`. The log assigns
/// `base_offset` on `append`, which is the leader path, or `append_at` pins it,
/// which is the follower path. The value here is therefore only a placeholder.
fn make_batch(epoch: Epoch, value: &[u8]) -> RecordBatch {
    let epoch_i32 = i32::try_from(epoch).expect("epoch fits in i32");
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: epoch_i32,
        attributes: Attributes::default(),
        last_offset_delta: 0,
        base_timestamp: 0,
        max_timestamp: 0,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: Some(bytes::Bytes::copy_from_slice(value)),
            headers: Vec::new(),
        }],
    }
}

impl LogView for KraftBackedLog {
    fn end_offset(&self) -> i64 {
        LogView::end_offset(&self.log)
    }
    fn last_epoch(&self) -> Epoch {
        LogView::last_epoch(&self.log)
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
        LogView::end_offset_for_epoch(&self.log, epoch)
    }
}

impl SimNodeLog for KraftBackedLog {
    fn append_in_epoch(&mut self, epoch: Epoch, count: usize) {
        // Leader path: one real single-record batch per record, each stamped
        // with the leader's current epoch. The log assigns sequential offsets.
        // A per-node monotonic counter keeps record values distinct so the
        // on-disk bytes are meaningful (and identical across replicas, since
        // followers copy the leader's exact batches).
        for _ in 0..count {
            let seq = NEXT_VALUE.with(|c| {
                let v = *c.borrow();
                *c.borrow_mut() = v + 1;
                v
            });
            let value = seq.to_be_bytes();
            let mut batch = make_batch(epoch, &value);
            self.log.append(&mut batch).expect("append");
        }
    }

    fn truncate_to(&mut self, offset: i64) {
        // The `SimNodeLog` seam speaks raw `i64`; wrap into the `KraftLog`
        // offset domain.
        let offset = Offset(offset);
        if offset < self.log.log_end_offset() {
            self.log.truncate_to(offset).expect("truncate_to");
        }
    }

    fn advance_hwm(&mut self, hwm: i64) {
        // Gate committed reads on the consensus HWM (monotonic, clamped to the
        // local log end by `KraftLog`). The seam is raw `i64`; wrap it.
        self.log.advance_hwm(Offset(hwm));
    }

    fn replicate_from(&mut self, leader: &Self) {
        let leader_batches = leader.decoded();
        let mut follower_batches = self.decoded();

        // Find the first offset where the follower diverges from the leader
        // (different leader epoch, or different record bytes). The follower must
        // truncate everything at/after that point before it can append-at the
        // leader's suffix.
        let mut common = 0usize;
        while common < leader_batches.len() && common < follower_batches.len() {
            let lb = &leader_batches[common];
            let fb = &follower_batches[common];
            if lb.partition_leader_epoch != fb.partition_leader_epoch || lb.records != fb.records {
                break;
            }
            common += 1;
        }

        // Truncate any follower suffix the leader does not share (conflicting
        // epoch tail, or a longer stale follower). Real on-disk truncation.
        if common < follower_batches.len() {
            let truncate_at = i64::try_from(common).expect("offset fits in i64");
            self.log
                .truncate_to(Offset(truncate_at))
                .expect("truncate_to");
            follower_batches.truncate(common);
        }

        // Append the suffix the follower is missing, preserving the leader's
        // exact bytes + epoch at the leader-assigned offset.
        for (next_offset, lb) in
            (self.log.log_end_offset().0..).zip(leader_batches.iter().skip(follower_batches.len()))
        {
            let mut batch = lb.clone();
            self.log
                .append_at(&mut batch, Offset(next_offset))
                .expect("append_at");
        }
    }

    fn record_count(&self) -> usize {
        usize::try_from(self.log.log_end_offset().0).expect("log end fits in usize")
    }
}

thread_local! {
    /// Monotonic record-value counter, one for each test thread, so that
    /// appended records carry distinct, deterministic payloads. `cargo test`
    /// runs each test on its own thread, so this counter resets between tests
    /// with no cross-talk.
    static NEXT_VALUE: RefCell<u64> = const { RefCell::new(0) };
}

/// A cluster whose nodes use real on-disk `KraftLog` instances.
fn new_with_kraft_log(voter_ids: &[NodeId]) -> Sim<KraftBackedLog> {
    Sim::new_with(voter_ids, |_id| KraftBackedLog::new())
}

/// The committed bytes of the log of `node`: the verbatim `.log` bytes for
/// every batch below the node's high watermark, served through the real
/// `read_committed` path.
///
/// The harness advances each node's HWM to the consensus HWM. It advances a
/// leader through `AdvanceHighWatermark`, and a follower on fetch. This is
/// therefore the byte-exact convergence target.
fn committed_bytes(sim: &Sim<KraftBackedLog>, node: NodeId) -> bytes::Bytes {
    let log = &sim.node_log(node).log;
    let raw = log
        .read_committed(Offset(0), UNBOUNDED_READ)
        .expect("read_committed");
    raw.bytes
}

/// Decoded committed batches of the log of `node`, up to the consensus HWM.
fn committed_batches(sim: &Sim<KraftBackedLog>, node: NodeId, hwm: i64) -> Vec<RecordBatch> {
    let log = &sim.node_log(node).log;
    log.read_decoded(Offset(0), UNBOUNDED_READ)
        .expect("read_decoded")
        .into_iter()
        .filter(|b| b.base_offset < hwm)
        .collect()
}

use assert2::check;

#[test]
fn voters_logs_byte_identical_up_to_hwm_over_real_log() {
    let mut sim = new_with_kraft_log(&[NodeId(1), NodeId(2), NodeId(3)]);
    sim.run_until_stable(10_000);
    assert2::assert!(sim.leaders().len() == 1);
    let leader = sim.leaders()[0];

    sim.leader_append(leader, 5); // 5 real batches stamped in the leader's epoch
    sim.run_until_stable(10_000);

    let hwm = sim.leader_high_watermark(leader);
    assert2::assert!(hwm >= 5);

    // Every voter's committed batches are byte-identical to the leader's.
    let leader_committed = committed_batches(&sim, leader, hwm);
    assert2::assert!(!leader_committed.is_empty());
    for v in sim.voters() {
        let voter_committed = committed_batches(&sim, v, hwm);
        assert2::assert!(voter_committed == leader_committed);
    }

    // The raw committed bytes also agree across all voters (true byte-exactness:
    // same encoded v2 batches, same offsets, same epochs).
    let leader_bytes = committed_bytes(&sim, leader);
    assert2::assert!(!leader_bytes.is_empty());
    for v in sim.voters() {
        assert2::assert!(committed_bytes(&sim, v) == leader_bytes);
    }
}

#[test]
fn follower_truncates_real_log_on_divergence_then_reconverges() {
    let mut sim = new_with_kraft_log(&[NodeId(1), NodeId(2), NodeId(3)]);
    sim.run_until_stable(10_000);
    assert2::assert!(sim.leaders().len() == 1);
    let leader = sim.leaders()[0];

    // Get the leader to commit some real data first.
    sim.leader_append(leader, 3);
    sim.run_until_stable(10_000);
    let leader_end = sim.log_end_offset(leader);

    // Give a follower a conflicting-epoch tail the leader does not have: two
    // records stamped at a strictly higher epoch (7) than the leader holds at
    // those offsets. This is the classic uncommitted-tail-from-a-dead-leader
    // case — the follower must truncate it on disk before it can re-replicate
    // the leader's authoritative log.
    let f = sim.voters().into_iter().find(|&v| v != leader).unwrap();
    let before = sim.log_end_offset(f);
    sim.inject_conflicting_tail(f, 7, 2);
    assert2::assert!(sim.log_end_offset(f) == before + 2);
    assert2::assert!(sim.log_end_offset(f) > leader_end);
    // The conflicting tail bumped the follower's last epoch above the leader's.
    assert2::assert!(LogView::last_epoch(sim.node_log(f)) == 7);

    // The leader produces more authoritative data. When the divergent follower
    // pulls it on its next fetch, it must first truncate its conflicting tail
    // (real on-disk truncation) before it can append the leader's batches —
    // exactly the KRaft divergence-then-reconverge flow.
    sim.leader_append(leader, 2);
    // Force the divergent follower to re-establish contact and re-run its fetch
    // loop: partition it (so the cluster quiesces without it) then heal it. On
    // reconnect it fetches, the leader detects the conflicting epoch-7 tail, and
    // the follower truncates on disk before re-replicating the leader's suffix.
    sim.partition(f);
    sim.run_until_stable(10_000);
    sim.heal(f);
    sim.run_until_stable(10_000);

    // The follower's KraftLog was truncated on disk (its end offset dropped back
    // to the leader's authoritative length) and re-replicated to match exactly:
    // same record bytes at the same offsets, byte-identical committed log.
    assert2::assert!(sim.log_end_offset(f) == sim.log_end_offset(leader));
    let hwm = sim.leader_high_watermark(leader);
    check!(
        committed_batches(&sim, f, hwm) == committed_batches(&sim, leader, hwm),
        "follower {f} did not re-converge to the leader after truncation"
    );
    check!(
        committed_bytes(&sim, f) == committed_bytes(&sim, leader),
        "follower {f} committed bytes did not re-converge to the leader"
    );

    // The follower's leader-epoch metadata must also roll back: after the
    // conflicting epoch-7 tail is truncated from disk, `last_epoch()` must no
    // longer report the stale higher epoch and must match the leader's. This is
    // guaranteed by `Log::truncate_to` now truncating the leader-epoch
    // checkpoint (mirrors Kafka's `LeaderEpochFileCache.truncateFromEnd`).
    check!(
        LogView::last_epoch(sim.node_log(f)) == LogView::last_epoch(sim.node_log(leader)),
        "follower {f} last_epoch should match the leader after truncation (no stale epoch 7)"
    );
}

#[test]
fn hwm_agrees_and_never_exceeds_any_voter_log_end() {
    let mut sim = new_with_kraft_log(&[NodeId(1), NodeId(2), NodeId(3)]);
    sim.run_until_stable(10_000);
    assert2::assert!(sim.leaders().len() == 1);
    let leader = sim.leaders()[0];

    sim.leader_append(leader, 3);
    sim.run_until_stable(10_000);

    let hwm = sim.leader_high_watermark(leader);
    assert2::assert!(hwm >= 3);
    for v in sim.voters() {
        assert2::assert!(hwm <= sim.log_end_offset(v));
    }
}

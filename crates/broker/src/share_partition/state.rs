//! In-memory share-partition acquisition state machine (KIP-932).
//!
//! This is the pure core the share-partition leader drives. It owns no I/O: it
//! tracks per-offset delivery state over the live window
//! `[start_offset, end_offset)` (SPSO..SPEO) as a list of contiguous
//! `InFlightBatch` runs and answers `acquire` / `acknowledge` /
//! `expire_locks` queries. Bytes, logs, locks, and persistence live elsewhere.
//!
//! Delivery-state codes match Kafka's on-the-wire values
//! (`DS_AVAILABLE=0`, `DS_ACQUIRED=1`, `DS_ACKNOWLEDGED=2`, `DS_ARCHIVED=4`).
//! `Acquired` is transient and is persisted back as `Available(0)`.

use std::time::{Duration, Instant};

use crabka_log::Offset;

use crate::share_coordinator::persistence::StateBatch;

/// Saturating `i64 -> i32` for record counts (offset ranges never realistically
/// overflow `i32`, but the counter type is `i32` to match the persister).
fn clamp_i32(n: i64) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

pub const DS_AVAILABLE: i8 = 0;
pub const DS_ACQUIRED: i8 = 1;
pub const DS_ACKNOWLEDGED: i8 = 2;
pub const DS_ARCHIVED: i8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordState {
    Available,
    Acquired,
    Acknowledged,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AckType {
    Gap,
    Accept,
    Release,
    Reject,
}

impl AckType {
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Gap),
            1 => Some(Self::Accept),
            2 => Some(Self::Release),
            3 => Some(Self::Reject),
            _ => None,
        }
    }
}

/// A contiguous run of offsets acquired by a single `acquire` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredRange {
    pub first: Offset,
    pub last: Offset,
    pub delivery_count: i16,
}

/// One contiguous run of offsets `[first_offset, last_offset]` sharing the same
/// delivery state and delivery count. Lock fields are only meaningful while
/// `state == RecordState::Acquired`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InFlightBatch {
    first_offset: Offset,
    last_offset: Offset,
    state: RecordState,
    delivery_count: i16,
    acquired_by: Option<String>,
    lock_deadline: Option<Instant>,
}

impl InFlightBatch {
    fn len(&self) -> i64 {
        self.last_offset.0 - self.first_offset.0 + 1
    }
}

/// The in-memory acquisition state for one share partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AcquisitionState {
    /// Share-partition start offset (SPSO): the lowest offset not yet
    /// terminally acknowledged/archived.
    pub start_offset: Offset,
    /// Share-partition end offset (SPEO): one past the highest materialized
    /// offset. Equals `start_offset` when the window is empty.
    pub end_offset: Offset,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub dirty: bool,
    /// Count of offsets that have reached a terminal state (Acknowledged or
    /// Archived) since `new`/`load_from`. This is the persister's
    /// `delivery_complete_count`.
    delivery_complete_count: i32,
    batches: Vec<InFlightBatch>,
}

impl AcquisitionState {
    #[must_use]
    pub fn new(start_offset: Offset) -> Self {
        Self {
            start_offset,
            end_offset: start_offset,
            state_epoch: 0,
            leader_epoch: 0,
            dirty: false,
            delivery_complete_count: 0,
            batches: Vec::new(),
        }
    }

    /// Extend the live window with freshly produced records.
    ///
    /// If no `Available` records remain in the window and the log has advanced
    /// past `end_offset`, append one `Available` batch spanning
    /// `[end_offset, min(hwm-1, end_offset + max_inflight - 1)]` and advance
    /// `end_offset`. `max_inflight` caps how many records may be in flight at
    /// once (approximated as a record count).
    pub fn materialize(&mut self, hwm: Offset, max_inflight: i32) {
        let has_available = self
            .batches
            .iter()
            .any(|b| b.state == RecordState::Available);
        if has_available || self.end_offset >= hwm {
            return;
        }
        let max_inflight = i64::from(max_inflight.max(1));
        let last = (hwm - 1).min(self.end_offset + max_inflight - 1);
        if last < self.end_offset {
            return;
        }
        self.batches.push(InFlightBatch {
            first_offset: self.end_offset,
            last_offset: last,
            state: RecordState::Available,
            delivery_count: 0,
            acquired_by: None,
            lock_deadline: None,
        });
        self.end_offset = last + 1;
        self.coalesce();
    }

    /// Acquire up to `max_records` Available records for `member`, walking the
    /// window from `start_offset`.
    ///
    /// Available batches whose `delivery_count >= max_attempts` are poison
    /// pills: they are Archived (not handed out) and the SPSO advances past
    /// them. Available batches under the limit transition to Acquired (split if
    /// they would exceed `max_records`), get `acquired_by`/`lock_deadline` set,
    /// `delivery_count += 1`, and are collected into the returned ranges.
    /// Walking stops once `max_records` are acquired.
    ///
    /// `max_bytes` is accepted for API symmetry but is approximated here by the
    /// record count (`max_records`); byte limits are enforced at the handler's
    /// log-read step, not in this pure machine.
    pub fn acquire(
        &mut self,
        member: &str,
        max_records: i32,
        _max_bytes: i32,
        now: Instant,
        lock_dur: Duration,
        max_attempts: i16,
    ) -> Vec<AcquiredRange> {
        let mut acquired = Vec::new();
        let mut remaining = i64::from(max_records.max(0));
        let mut i = 0;
        let mut any_change = false;
        while i < self.batches.len() {
            if remaining == 0 {
                break;
            }
            if self.batches[i].state != RecordState::Available {
                i += 1;
                continue;
            }
            // Poison pill: archive without handing out.
            if self.batches[i].delivery_count >= max_attempts {
                let n = clamp_i32(self.batches[i].len());
                self.batches[i].state = RecordState::Archived;
                self.batches[i].acquired_by = None;
                self.batches[i].lock_deadline = None;
                self.delivery_complete_count += n;
                any_change = true;
                i += 1;
                continue;
            }
            // Split if the Available run exceeds the remaining budget.
            let avail_len = self.batches[i].len();
            if avail_len > remaining {
                let split_at = self.batches[i].first_offset + remaining;
                self.split_at(i, split_at);
            }
            let b = &mut self.batches[i];
            b.state = RecordState::Acquired;
            b.delivery_count += 1;
            b.acquired_by = Some(member.to_string());
            b.lock_deadline = Some(now + lock_dur);
            acquired.push(AcquiredRange {
                first: b.first_offset,
                last: b.last_offset,
                delivery_count: b.delivery_count,
            });
            remaining -= b.len();
            any_change = true;
            i += 1;
        }
        if any_change {
            self.dirty = true;
            self.advance_spso();
        }
        acquired
    }

    /// Acknowledge the offset range `[first, last]` previously acquired by
    /// `member`.
    ///
    /// The whole range must currently be `Acquired` by `member`, else
    /// `Err(INVALID_RECORD_STATE)`. The range is split off into its own batches
    /// at the boundaries, then: `Accept` -> Acknowledged, `Release` ->
    /// Available (lock/owner cleared, `delivery_count` retained for redelivery),
    /// `Reject`/`Gap` -> Archived. SPSO is advanced over any new terminal
    /// prefix and the state is marked dirty.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn acknowledge(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        ack: AckType,
        _now: Instant,
    ) -> Result<(), i16> {
        if first > last {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // Validate the entire range is Acquired by this member.
        if !self.range_acquired_by(member, first, last) {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // Carve the range out at its boundaries.
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        for b in &mut self.batches {
            if b.first_offset < first || b.last_offset > last {
                continue;
            }
            if b.state != RecordState::Acquired {
                continue;
            }
            let n = clamp_i32(b.len());
            match ack {
                AckType::Accept => {
                    b.state = RecordState::Acknowledged;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    self.delivery_complete_count += n;
                }
                AckType::Release => {
                    b.state = RecordState::Available;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    // delivery_count retained: next acquire redelivers at +1.
                }
                AckType::Reject | AckType::Gap => {
                    b.state = RecordState::Archived;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    self.delivery_complete_count += n;
                }
            }
        }
        self.dirty = true;
        self.advance_spso();
        Ok(())
    }

    /// Renew (extend) the acquisition lock on the range `[first, last]` held by
    /// `member`, resetting each covered Acquired batch's `lock_deadline` to
    /// `now + lock_dur` (KIP-932 RENEW acknowledgement).
    ///
    /// The whole range must currently be `Acquired` by `member`, else
    /// `Err(INVALID_RECORD_STATE)`. State, owner, and `delivery_count` are all
    /// preserved (only the deadline moves) and SPSO is NOT advanced — renew
    /// keeps records in flight. Marks the state dirty.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn renew(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        now: Instant,
        lock_dur: Duration,
    ) -> Result<(), i16> {
        if first > last {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // The entire range must be Acquired by this member.
        if !self.range_acquired_by(member, first, last) {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // Carve the range out at its boundaries, then extend each covered lock.
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        let new_deadline = now + lock_dur;
        for b in &mut self.batches {
            if b.first_offset < first || b.last_offset > last {
                continue;
            }
            if b.state != RecordState::Acquired {
                continue;
            }
            b.lock_deadline = Some(new_deadline);
        }
        self.dirty = true;
        Ok(())
    }

    /// Revert any Acquired batch whose lock has expired back to Available,
    /// clearing the lock/owner but retaining `delivery_count` (so the next
    /// acquire counts as a redelivery). Marks dirty if anything changed.
    pub fn expire_locks(&mut self, now: Instant) {
        let mut changed = false;
        for b in &mut self.batches {
            if b.state == RecordState::Acquired
                && let Some(deadline) = b.lock_deadline
                && now >= deadline
            {
                b.state = RecordState::Available;
                b.acquired_by = None;
                b.lock_deadline = None;
                changed = true;
            }
        }
        if changed {
            self.dirty = true;
            self.coalesce();
        }
    }

    /// True iff every offset in `[first, last]` is currently Acquired by
    /// `member`.
    fn range_acquired_by(&self, member: &str, first: Offset, last: Offset) -> bool {
        let mut cursor = first;
        for b in &self.batches {
            if b.last_offset < first || b.first_offset > last {
                continue;
            }
            // The covered batches must be contiguous from `first`.
            if b.first_offset > cursor {
                return false;
            }
            if b.state != RecordState::Acquired || b.acquired_by.as_deref() != Some(member) {
                return false;
            }
            cursor = b.last_offset + 1;
            if cursor > last {
                break;
            }
        }
        cursor > last
    }

    /// Split the batch at index `i` so that `split` becomes the first offset of
    /// a new trailing batch. No-op if `split` is at a boundary.
    fn split_at(&mut self, i: usize, split: Offset) {
        let b = &self.batches[i];
        if split <= b.first_offset || split > b.last_offset {
            return;
        }
        let tail = InFlightBatch {
            first_offset: split,
            last_offset: b.last_offset,
            state: b.state,
            delivery_count: b.delivery_count,
            acquired_by: b.acquired_by.clone(),
            lock_deadline: b.lock_deadline,
        };
        self.batches[i].last_offset = split - 1;
        self.batches.insert(i + 1, tail);
    }

    /// Split whichever batch contains the boundary so `offset` is a batch
    /// `first_offset`. No-op when `offset` already lands on a boundary or is
    /// outside the window.
    fn split_at_offset(&mut self, offset: Offset) {
        if let Some(i) = self.batches.iter().position(|b| {
            b.first_offset
                .0
                .checked_add(1)
                .is_some_and(|first_split_offset| {
                    (first_split_offset..=b.last_offset.0).contains(&offset.0)
                })
        }) {
            self.split_at(i, offset);
        }
    }

    /// Advance SPSO over any terminal (Acknowledged/Archived) prefix, dropping
    /// those batches, then coalesce adjacent same-state neighbors.
    fn advance_spso(&mut self) {
        while let Some(b) = self.batches.first() {
            if b.first_offset == self.start_offset
                && matches!(b.state, RecordState::Acknowledged | RecordState::Archived)
            {
                self.start_offset = b.last_offset + 1;
                self.batches.remove(0);
            } else {
                break;
            }
        }
        if self.end_offset < self.start_offset {
            self.end_offset = self.start_offset;
        }
        self.coalesce();
    }

    /// Merge adjacent batches that share the same delivery state, delivery
    /// count, and acquisition (owner + deadline). Keeps the batch list compact.
    fn coalesce(&mut self) {
        let mut i = 0;
        while i + 1 < self.batches.len() {
            let mergeable = {
                let a = &self.batches[i];
                let b = &self.batches[i + 1];
                a.last_offset + 1 == b.first_offset
                    && a.state == b.state
                    && a.delivery_count == b.delivery_count
                    && a.acquired_by == b.acquired_by
                    && a.lock_deadline == b.lock_deadline
            };
            if mergeable {
                let next_last = self.batches[i + 1].last_offset;
                self.batches[i].last_offset = next_last;
                self.batches.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }

    /// Project the live window into persistable batches.
    ///
    /// Returns `(start_offset, delivery_complete_count, batches)` for
    /// `[start_offset, end_offset)`. Transient `Acquired` records are persisted
    /// as `Available(0)` (a leader that crashes and reloads re-offers them).
    /// Acknowledged/Archived batches are emitted with their terminal codes.
    #[must_use]
    pub fn to_persist_batches(&self) -> (Offset, i32, Vec<StateBatch>) {
        let mut out = Vec::with_capacity(self.batches.len());
        for b in &self.batches {
            let delivery_state = match b.state {
                RecordState::Available | RecordState::Acquired => DS_AVAILABLE,
                RecordState::Acknowledged => DS_ACKNOWLEDGED,
                RecordState::Archived => DS_ARCHIVED,
            };
            out.push(StateBatch {
                first_offset: b.first_offset,
                last_offset: b.last_offset,
                delivery_state,
                delivery_count: b.delivery_count,
            });
        }
        (self.start_offset, self.delivery_complete_count, out)
    }

    /// Cumulative count of offsets that have reached a terminal state
    /// (Acknowledged or Archived) — the persister's `delivery_complete_count`.
    /// Currently only consulted by the state-machine tests; the value also
    /// flows out through [`Self::to_persist_batches`].
    #[cfg(test)]
    #[must_use]
    pub(crate) fn delivery_complete_count(&self) -> i32 {
        self.delivery_complete_count
    }

    /// Number of in-flight batches currently in `Acquired` state. Test-only.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub(crate) fn count_acquired_batches(&self) -> i32 {
        i32::try_from(
            self.batches
                .iter()
                .filter(|b| b.state == RecordState::Acquired)
                .count(),
        )
        .unwrap_or(i32::MAX)
    }

    /// Rebuild the machine from persisted state. SPSO is restored to
    /// `start_offset`, the cumulative `delivery_complete_count` is restored
    /// (so the consumer-lag accounting survives a leader change), batches are
    /// rehydrated (persisted `Acquired(1)` maps to `Available` since locks
    /// don't survive a leader change), and `end_offset` is set to
    /// `max(last_offset)+1` or `start_offset` when empty.
    pub fn load_from(
        &mut self,
        start_offset: Offset,
        state_epoch: i32,
        leader_epoch: i32,
        delivery_complete_count: i32,
        batches: &[StateBatch],
    ) {
        self.start_offset = start_offset;
        self.state_epoch = state_epoch;
        self.leader_epoch = leader_epoch;
        self.dirty = false;
        self.delivery_complete_count = delivery_complete_count;
        self.batches = batches
            .iter()
            .map(|sb| {
                // Persisted Acquired(1) maps to Available: locks don't survive a
                // leader change, so re-offer those records.
                let state = match sb.delivery_state {
                    DS_ACKNOWLEDGED => RecordState::Acknowledged,
                    DS_ARCHIVED => RecordState::Archived,
                    _ => RecordState::Available,
                };
                InFlightBatch {
                    first_offset: sb.first_offset,
                    last_offset: sb.last_offset,
                    state,
                    delivery_count: sb.delivery_count,
                    acquired_by: None,
                    lock_deadline: None,
                }
            })
            .collect();
        self.end_offset = self
            .batches
            .iter()
            .map(|b| b.last_offset + 1)
            .max()
            .unwrap_or(start_offset)
            .max(start_offset);
        self.coalesce();
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    const LOCK: Duration = Duration::from_secs(30);

    #[test]
    fn acquire_then_accept_advances_spso() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100); // [0,4] Available
        let acq = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(0),
                last: Offset(4),
                delivery_count: 1
            }]
        );
        s.acknowledge("m1", Offset(0), Offset(4), AckType::Accept, t0())
            .unwrap();
        assert!(s.start_offset == 5);
    }

    #[test]
    fn release_redelivers_with_incremented_count() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Release, t0())
            .unwrap();
        let acq2 = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(acq2[0].delivery_count == 2);
        // Released records stay in the window; SPSO did not advance.
        assert!(s.start_offset == 0);
    }

    #[test]
    fn delivery_limit_archives_poison_pill() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(1), 100);
        for _ in 0..2 {
            // max_attempts = 2
            let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 2);
            s.expire_locks(t0() + Duration::from_secs(31));
        }
        let acq = s.acquire("m1", 10, i32::MAX, t0() + Duration::from_secs(62), LOCK, 2);
        assert!(acq.is_empty()); // archived, not redelivered
        assert!(s.start_offset == 1); // SPSO advanced past the poison pill
    }

    #[test]
    fn partial_acknowledge_splits_a_batch() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100); // [0,9] Available
        let acq = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(acq.len() == 1);
        // Accept only [0,3]; [4,9] remain Acquired.
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, t0())
            .unwrap();
        assert!(s.start_offset == 4);
        // The remaining acquired range can still be acknowledged.
        s.acknowledge("m1", Offset(4), Offset(9), AckType::Accept, t0())
            .unwrap();
        assert!(s.start_offset == 10);
    }

    #[test]
    fn expire_locks_reverts_to_available() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(4), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        // Before expiry: re-acquire finds nothing (all Acquired).
        let none = s.acquire("m2", 10, i32::MAX, t0(), LOCK, 5);
        assert!(none.is_empty());
        s.expire_locks(t0() + Duration::from_secs(31));
        // Now another member can acquire; redelivery bumps the count.
        let acq = s.acquire("m2", 10, i32::MAX, t0() + Duration::from_secs(31), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(0),
                last: Offset(3),
                delivery_count: 2
            }]
        );
        assert!(s.start_offset == 0);
    }

    #[test]
    fn reject_archives_and_advances_spso() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Reject, t0())
            .unwrap();
        assert!(s.start_offset == 3); // archived prefix dropped
        let acq = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(acq.is_empty()); // nothing left
    }

    #[test]
    fn gap_archives() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(2), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Gap, t0())
            .unwrap();
        assert!(s.start_offset == 2);
        let (_start, dcc, batches) = s.to_persist_batches();
        assert!(batches.is_empty()); // archived prefix dropped from window
        assert!(dcc == 2); // both offsets reached a terminal state
    }

    #[test]
    fn to_persist_batches_maps_acquired_to_available() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        let (start, dcc, batches) = s.to_persist_batches();
        check!(start == 0);
        check!(dcc == 0); // nothing terminal yet
        // Acquired persists as Available(0) but retains its delivery_count.
        check!(
            batches
                == vec![StateBatch {
                    first_offset: Offset(0),
                    last_offset: Offset(4),
                    delivery_state: DS_AVAILABLE,
                    delivery_count: 1
                }]
        );
    }

    #[test]
    fn load_from_round_trip() {
        // Build a state, acquire part of it, persist, reload into a fresh one.
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        let _ = s.acquire("m1", 4, i32::MAX, t0(), LOCK, 5); // [0,3] Acquired, [4,9] Available
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, t0())
            .unwrap(); // SPSO -> 4
        let (start, _dcc, batches) = s.to_persist_batches();
        assert!(start == 4);

        let mut reloaded = AcquisitionState::new(Offset(0));
        reloaded.load_from(start, 7, 3, 0, &batches);
        check!(reloaded.start_offset == 4);
        check!(reloaded.end_offset == 10);
        check!(reloaded.state_epoch == 7);
        check!(reloaded.leader_epoch == 3);
        check!(!reloaded.dirty);
        // The remaining records are Available again and re-acquirable.
        let acq = reloaded.acquire("m2", 100, i32::MAX, t0(), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(4),
                last: Offset(9),
                delivery_count: 1
            }]
        );
    }

    #[test]
    fn acknowledge_wrong_member_is_invalid_record_state() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        let err = s.acknowledge("m2", Offset(0), Offset(2), AckType::Accept, t0());
        assert!(err == Err(crate::codes::INVALID_RECORD_STATE));
    }

    #[test]
    fn materialize_respects_max_inflight() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(100), 10); // hwm far ahead, but cap at 10 in flight
        assert!(s.end_offset == 10);
        let acq = s.acquire("m1", 100, i32::MAX, t0(), LOCK, 5);
        assert!(acq[0].first == 0);
        assert!(acq[0].last == 9);
    }

    #[test]
    fn acquire_splits_at_max_records() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        let acq = s.acquire("m1", 4, i32::MAX, t0(), LOCK, 5);
        assert!(acq.len() == 1);
        assert!(acq[0].first == 0 && acq[0].last == 3);
        // The remaining [4,9] is still Available.
        let acq2 = s.acquire("m2", 100, i32::MAX, t0(), LOCK, 5);
        assert!(acq2[0].first == 4 && acq2[0].last == 9);
    }

    #[test]
    fn load_from_restores_delivery_complete_count() {
        // F3: the cumulative delivery-complete count must survive a reload so
        // consumer-lag accounting is preserved across a leader change.
        let mut s = AcquisitionState::new(Offset(4));
        s.load_from(Offset(4), 0, 0, 5, &[]);
        assert!(s.delivery_complete_count() == 5);
        // It round-trips back out through the persist projection.
        let (_start, dcc, _batches) = s.to_persist_batches();
        assert!(dcc == 5);
    }

    #[test]
    fn renew_extends_lock_keeping_acquired() {
        // F1: acquire with a short lock, renew with a longer one. An
        // `expire_locks` at the ORIGINAL deadline must not release the records.
        let t0 = Instant::now();
        let short = Duration::from_secs(10);
        let long = Duration::from_mins(1);

        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(4), 100);
        let acq = s.acquire("m1", 10, i32::MAX, t0, short, 5);
        assert!(acq.len() == 1);
        let original_deadline = t0 + short;

        // Renew extends the lock well past the original deadline.
        s.renew("m1", Offset(0), Offset(3), t0, long).unwrap();
        assert!(s.dirty);

        // Sweeping at the original deadline must NOT release the renewed lock.
        s.expire_locks(original_deadline);
        // Still Acquired by m1 -> a different member acquires nothing.
        let none = s.acquire("m2", 10, i32::MAX, original_deadline, short, 5);
        assert!(none.is_empty());
        // And m1 can still acknowledge it (proves it stayed Acquired by m1).
        s.acknowledge(
            "m1",
            Offset(0),
            Offset(3),
            AckType::Accept,
            original_deadline,
        )
        .unwrap();
        assert!(s.start_offset == 4);
    }

    #[test]
    fn renew_on_unacquired_range_is_invalid_record_state() {
        let t0 = Instant::now();
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0, LOCK, 5);
        // Wrong member.
        let err = s.renew("m2", Offset(0), Offset(2), t0, LOCK);
        assert!(err == Err(crate::codes::INVALID_RECORD_STATE));

        // Non-Acquired range: release [0,2] back to Available, then renew fails.
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Release, t0)
            .unwrap();
        let err2 = s.renew("m1", Offset(0), Offset(2), t0, LOCK);
        assert!(err2 == Err(crate::codes::INVALID_RECORD_STATE));
    }
}

#[cfg(test)]
#[path = "state_model.rs"]
mod state_model;

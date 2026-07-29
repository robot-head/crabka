//! Deterministic range-0 tail over committed WAL frames.

use std::sync::{Arc, Mutex};

use crabka_pgkv::{Kv, KvError, WriteOp};
use tokio::sync::watch;

use crate::swappable_kv::SwappableKv;

const GRW1_VERSION: u8 = 1;

/// A committed range-0 WAL frame ready for local catalog application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range0Frame {
    /// Kafka offset that carried this frame.
    pub offset: i64,
    /// Ordered write batch from the WAL frame.
    pub ops: Vec<WriteOp>,
}

impl Range0Frame {
    /// Build a committed range-0 frame from decoded operations.
    #[must_use]
    pub fn new(offset: i64, ops: Vec<WriteOp>) -> Self {
        Self { offset, ops }
    }

    /// Decode the current `GRW1` frame payload.
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn decode(offset: i64, bytes: &[u8]) -> Result<Self, Range0TailError> {
        let mut reader = FrameReader { bytes, at: 0 };
        let version = reader.u8()?;
        if version != GRW1_VERSION {
            return Err(Range0TailError::Frame(format!(
                "unknown range-0 frame version {version}"
            )));
        }

        let _journal_seq = reader.u64()?;
        let op_count = reader.u32()?;
        let mut ops = Vec::with_capacity(usize::try_from(op_count).expect("u32 fits usize"));
        for _ in 0..op_count {
            let tag = reader.u8()?;
            let key = reader.chunk()?.to_vec();
            let op = match tag {
                0 => WriteOp::Put {
                    key,
                    value: reader.chunk()?.to_vec(),
                },
                1 => WriteOp::Delete { key },
                other => return Err(Range0TailError::Frame(format!("unknown op tag {other}"))),
            };
            ops.push(op);
        }

        if reader.at != bytes.len() {
            return Err(Range0TailError::Frame("trailing bytes".to_owned()));
        }

        Ok(Self { offset, ops })
    }
}

/// A sink for the content of every committed range-0 frame.
///
/// The tail exists to keep a local catalog store in step with the range-0 log,
/// so it publishes only an applied offset. Records that are deliberately never
/// written to the KV — cross-node `NOTIFY` being the first — are visible
/// nowhere else, hence this seam.
///
/// `observe` runs inline on the apply path, before the ops reach the store and
/// before the applied offset advances. An implementation must therefore never
/// block: a slow observer stalls catalog application and every read barrier
/// waiting on it. Deciding what to do with a full downstream queue belongs to
/// the observer, which is why this is an installed hook rather than a broadcast
/// channel with a buffering policy of its own.
pub trait Range0FrameObserver: Send + Sync {
    /// Called with every committed frame's ops before they reach the store.
    fn observe(&self, ops: &[WriteOp]);
}

/// Local range-0 tail state and offset observable.
#[derive(Clone)]
pub struct Range0Tail {
    store: Arc<SwappableKv>,
    applied_offset_tx: watch::Sender<i64>,
    _applied_offset_rx: watch::Receiver<i64>,
    observer: Arc<Mutex<Option<Arc<dyn Range0FrameObserver>>>>,
}

impl Range0Tail {
    /// Create a deterministic tail over an already-open local store.
    #[must_use]
    pub fn new(store: Arc<dyn Kv>) -> Self {
        Self::from_applied_offset(store, -1)
    }

    /// Create a tail after a checkpoint has restored state through `applied_offset`.
    ///
    /// Callers must only use this after the checkpoint is durably restored; publishing a
    /// covered offset before its data is present would make a read barrier unsafe.
    #[must_use]
    pub fn from_checkpoint(store: Arc<dyn Kv>, applied_offset: i64) -> Self {
        Self::from_applied_offset(store, applied_offset)
    }

    fn from_applied_offset(store: Arc<dyn Kv>, applied_offset: i64) -> Self {
        let (applied_offset_tx, applied_offset_rx) = watch::channel(applied_offset);
        Self {
            store: Arc::new(SwappableKv::new(store)),
            applied_offset_tx,
            _applied_offset_rx: applied_offset_rx,
            observer: Arc::new(Mutex::new(None)),
        }
    }

    /// Install the observer that sees every committed frame's ops.
    ///
    /// Takes `&self` rather than `&mut self` on purpose: a `Range0Tail` is
    /// cloned to hand out (the substrate follower's `tail()` returns a clone),
    /// and the handle a caller holds is rarely the one that applies frames. The
    /// hook is stored behind a shared cell exactly like the offset sender, so
    /// installing on any clone is visible to all of them.
    ///
    /// A second call replaces the first; there is one observer, not a list.
    ///
    /// # Panics
    ///
    /// Panics if the observer cell was poisoned by a panicking observer.
    pub fn set_frame_observer(&self, observer: Arc<dyn Range0FrameObserver>) {
        *self.observer.lock().expect("range-0 frame observer cell") = Some(observer);
    }

    fn frame_observer(&self) -> Option<Arc<dyn Range0FrameObserver>> {
        self.observer
            .lock()
            .expect("range-0 frame observer cell")
            .clone()
    }

    /// Apply a committed frame with G-2 merge rules and publish its offset.
    ///
    /// The installed [`Range0FrameObserver`] sees the frame's ops first: it is
    /// the only view of records the merge rules drop instead of storing.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn apply_committed(&self, frame: &Range0Frame) -> Result<(), Range0TailError> {
        if frame.offset <= *self.applied_offset_tx.borrow() {
            return Err(Range0TailError::NonMonotoneOffset {
                current: *self.applied_offset_tx.borrow(),
                incoming: frame.offset,
            });
        }

        if let Some(observer) = self.frame_observer() {
            observer.observe(&frame.ops);
        }

        apply_merge_rules(self.store.as_ref(), &frame.ops)?;
        self.applied_offset_tx
            .send(frame.offset)
            .map_err(|_| Range0TailError::Closed)
    }

    /// Observe the latest range-0 offset this tail has applied.
    #[must_use]
    pub fn subscribe_applied_offset(&self) -> watch::Receiver<i64> {
        self.applied_offset_tx.subscribe()
    }

    /// Return the latest applied range-0 offset.
    #[must_use]
    pub fn applied_offset(&self) -> i64 {
        *self.applied_offset_tx.borrow()
    }

    /// Return the store that receives committed range-0 frames.
    ///
    /// Consumers that use this tail for catalog reads must use this exact handle;
    /// a separate catalog store could let a barrier certify unrelated state.
    ///
    /// The handle is stable for the tail's whole life. It is a
    /// [`SwappableKv`], so a caller that captures it once — the read-only
    /// range-0 replica does exactly that, at construction — keeps reading the
    /// live store across a [`Range0Tail::reset_to_checkpoint`].
    #[must_use]
    pub fn store_handle(&self) -> Arc<dyn Kv> {
        Arc::clone(&self.store) as Arc<dyn Kv>
    }

    /// Adopt a store rebuilt from a checkpoint that covers `covered_offset`.
    ///
    /// This is the recovery path for a follower whose WAL was trimmed past the
    /// offset it still needed: the committed frames it was waiting for no
    /// longer exist, so the only way forward is a fresh restore. `store` must
    /// be a *different* store that already holds the restored state through
    /// `covered_offset` — restoring in place would expose partially rebuilt
    /// state to readers the barrier has already released.
    ///
    /// Ordering is load-bearing. The store is installed *before* the offset is
    /// published, because a reader gated on the barrier waits for
    /// `applied_offset >= target` and only then reads through
    /// [`Range0Tail::store_handle`]. Publishing first would let such a reader
    /// wake on an offset whose data lives in a store it cannot see yet.
    ///
    /// Frames between the old applied offset and `covered_offset` are never
    /// replayed. The catalog stays correct — the checkpoint is a superset of
    /// what they wrote — but anything derived from *observing* frames is lost
    /// for that window. The one such consumer today is the
    /// [`Range0FrameObserver`] hook that delivers cross-node `NOTIFY` records,
    /// so notifications committed in the skipped window are dropped. That is
    /// at-most-once delivery, and it matches `PostgreSQL`, where a listener that
    /// loses its connection loses the notifications sent while it was away.
    ///
    /// # Errors
    ///
    /// Returns [`Range0TailError::NonMonotoneOffset`] when `covered_offset`
    /// does not advance the applied offset — the offset this tail publishes
    /// only ever moves forward — or [`Range0TailError::Closed`] when the
    /// observable is closed.
    pub fn reset_to_checkpoint(
        &self,
        store: Arc<dyn Kv>,
        covered_offset: i64,
    ) -> Result<(), Range0TailError> {
        let current = self.applied_offset();
        if covered_offset <= current {
            return Err(Range0TailError::NonMonotoneOffset {
                current,
                incoming: covered_offset,
            });
        }

        self.store.swap(store);
        self.applied_offset_tx
            .send(covered_offset)
            .map_err(|_| Range0TailError::Closed)
    }

    /// Wait until the local tail has applied at least `target_offset`.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn wait_until_applied(&self, target_offset: i64) -> Result<(), Range0TailError> {
        if self.applied_offset() >= target_offset {
            return Ok(());
        }

        let mut rx = self.subscribe_applied_offset();
        while rx.changed().await.is_ok() {
            if *rx.borrow_and_update() >= target_offset {
                return Ok(());
            }
        }

        Err(Range0TailError::Closed)
    }
}

/// Start a range-0 tail seam. Live broker consumption is supplied by substrate.
#[must_use]
pub fn spawn(_bootstrap: impl Into<String>, _tenant: TenantName, store: Arc<dyn Kv>) -> Range0Tail {
    Range0Tail::new(store)
}

use crate::TenantName;

/// Errors applying or observing the range-0 tail.
#[derive(Debug, thiserror::Error)]
pub enum Range0TailError {
    /// A frame payload could not be parsed.
    #[error("malformed range-0 frame: {0}")]
    Frame(String),
    /// Local KV application failed.
    #[error(transparent)]
    Kv(#[from] KvError),
    /// The observable tail was closed.
    #[error("range-0 tail observable is closed")]
    Closed,
    /// The committed stream moved backwards or replayed an old offset.
    #[error("range-0 committed offsets must increase: current {current}, incoming {incoming}")]
    NonMonotoneOffset { current: i64, incoming: i64 },
}

fn apply_merge_rules(kv: &dyn Kv, ops: &[WriteOp]) -> Result<(), KvError> {
    range0_tail_merge::apply_frame(kv, ops)
}

mod range0_tail_merge {
    use std::collections::{HashMap, HashSet};

    use crabka_pgkv::{Kv, KvError, WriteOp, is_notify_op, key};
    use crabka_pgmvcc::clog;

    pub(super) fn apply_frame(kv: &dyn Kv, ops: &[WriteOp]) -> Result<(), KvError> {
        let mut counters: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut decided: HashSet<Vec<u8>> = HashSet::new();
        let mut adjusted = Vec::with_capacity(ops.len());

        for op in ops {
            // Notify records ride the range-0 log purely as a transport; the frame
            // observer has already seen them. They must never reach the KV: a
            // checkpoint snapshots the whole range-0 store and the WAL topic never
            // expires by time, so one stored notify record would be baked into
            // every later checkpoint and restored by every follower that
            // bootstraps from it. Dropping the op does not stall the read barrier —
            // `apply_committed` advances the applied offset whatever the batch
            // contained.
            if is_notify_op(op) {
                continue;
            }
            match op {
                WriteOp::Put { key, value } if is_counter_key(key) => {
                    push_counter_op(kv, &mut counters, &mut adjusted, key, value)?;
                }
                WriteOp::Put { key, value } if is_clog_key(key) => {
                    push_clog_op(kv, &mut decided, &mut adjusted, key, value)?;
                }
                other => adjusted.push(other.clone()),
            }
        }

        kv.write_batch(&adjusted)
    }

    fn push_counter_op(
        kv: &dyn Kv,
        counters: &mut HashMap<Vec<u8>, u64>,
        adjusted: &mut Vec<WriteOp>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), KvError> {
        let incoming = u64_be(value);
        let current = match counters.get(key) {
            Some(value) => *value,
            None => kv.get(key)?.as_deref().map_or(0, u64_be),
        };
        let merged = incoming.max(current);
        counters.insert(key.to_vec(), merged);
        adjusted.push(WriteOp::Put {
            key: key.to_vec(),
            value: merged.to_be_bytes().to_vec(),
        });
        Ok(())
    }

    fn push_clog_op(
        kv: &dyn Kv,
        decided: &mut HashSet<Vec<u8>>,
        adjusted: &mut Vec<WriteOp>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), KvError> {
        let already_terminal =
            decided.contains(key) || kv.get(key)?.as_deref().is_some_and(clog::is_terminal);
        if already_terminal {
            return Ok(());
        }

        if clog::is_terminal(value) {
            decided.insert(key.to_vec());
        }
        adjusted.push(WriteOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn is_counter_key(key: &[u8]) -> bool {
        key == b"/0/meta/max_ts"
            || key == key::next_xid_key().as_slice()
            || key.starts_with(&key::seq_prefix())
    }

    fn is_clog_key(key: &[u8]) -> bool {
        key.starts_with(&key::clog_prefix())
    }

    fn u64_be(bytes: &[u8]) -> u64 {
        let mut buf = [0_u8; 8];
        let n = bytes.len().min(8);
        buf[8 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
        u64::from_be_bytes(buf)
    }
}

struct FrameReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> FrameReader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Range0TailError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| Range0TailError::Frame("truncated frame".to_owned()))?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, Range0TailError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, Range0TailError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, Range0TailError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn chunk(&mut self) -> Result<&'a [u8], Range0TailError> {
        let len = self.u32()?;
        self.take(usize::try_from(len).expect("u32 fits usize"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv, WriteOp, key};
    use crabka_pgmvcc::clog::{self, XidStatus};

    use super::*;

    /// Records every frame the tail hands to the observer seam.
    #[derive(Default)]
    struct RecordingObserver {
        frames: Mutex<Vec<Vec<WriteOp>>>,
    }

    impl RecordingObserver {
        fn frames(&self) -> Vec<Vec<WriteOp>> {
            self.frames.lock().expect("observer mutex").clone()
        }
    }

    impl Range0FrameObserver for RecordingObserver {
        fn observe(&self, ops: &[WriteOp]) {
            self.frames
                .lock()
                .expect("observer mutex")
                .push(ops.to_vec());
        }
    }

    fn put(key: Vec<u8>, value: &[u8]) -> WriteOp {
        WriteOp::Put {
            key,
            value: value.to_vec(),
        }
    }

    #[test]
    fn tail_applies_frames_and_publishes_offsets() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store.clone());
        let mut applied = tail.subscribe_applied_offset();

        tail.apply_committed(&Range0Frame::new(
            4,
            vec![WriteOp::Put {
                key: b"catalog/table".to_vec(),
                value: b"created".to_vec(),
            }],
        ))
        .expect("apply");

        assert!(store.get(b"catalog/table").expect("get") == Some(b"created".to_vec()));
        assert!(*applied.borrow_and_update() == 4);
    }

    #[test]
    fn tail_uses_g2_counter_merge_rules() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store.clone());

        tail.apply_committed(&Range0Frame::new(
            1,
            vec![WriteOp::Put {
                key: key::next_xid_key(),
                value: 9_u64.to_be_bytes().to_vec(),
            }],
        ))
        .expect("apply first");
        tail.apply_committed(&Range0Frame::new(
            2,
            vec![WriteOp::Put {
                key: key::next_xid_key(),
                value: 7_u64.to_be_bytes().to_vec(),
            }],
        ))
        .expect("apply second");

        assert!(
            store.get(&key::next_xid_key()).expect("get") == Some(9_u64.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn an_installed_observer_sees_every_frames_ops() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store);
        let observer = Arc::new(RecordingObserver::default());
        tail.set_frame_observer(Arc::clone(&observer) as Arc<dyn Range0FrameObserver>);

        let first = vec![put(b"catalog/a".to_vec(), b"1")];
        let second = vec![
            put(key::notify_key(7), b"record"),
            WriteOp::Delete {
                key: b"catalog/a".to_vec(),
            },
        ];
        tail.apply_committed(&Range0Frame::new(1, first.clone()))
            .expect("apply first");
        tail.apply_committed(&Range0Frame::new(2, second.clone()))
            .expect("apply second");

        // The observer sees frames whole and in order, including the notify ops
        // the merge rules go on to drop.
        assert!(observer.frames() == vec![first, second]);
    }

    #[test]
    fn an_observer_installed_on_one_handle_sees_another_handles_frames() {
        let store = Arc::new(MemKv::default());
        let applying = Range0Tail::new(store);
        let handed_out = applying.clone();
        let observer = Arc::new(RecordingObserver::default());
        handed_out.set_frame_observer(Arc::clone(&observer) as Arc<dyn Range0FrameObserver>);

        let ops = vec![put(b"catalog/a".to_vec(), b"1")];
        applying
            .apply_committed(&Range0Frame::new(1, ops.clone()))
            .expect("apply");

        assert!(observer.frames() == vec![ops]);
    }

    #[test]
    fn a_frame_of_only_notify_ops_still_advances_the_applied_offset() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(Arc::clone(&store) as Arc<dyn Kv>);
        let mut applied = tail.subscribe_applied_offset();

        tail.apply_committed(&Range0Frame::new(
            9,
            vec![
                put(key::notify_key(1), b"first"),
                put(key::notify_key(2), b"second"),
            ],
        ))
        .expect("apply");

        assert!(*applied.borrow_and_update() == 9);
        assert!(tail.applied_offset() == 9);
        // A read barrier waiting on this offset is released.
        assert!(store.get(&key::notify_key(1)).expect("get") == None);
    }

    #[test]
    fn notify_ops_never_reach_the_store_while_their_batch_mates_do() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(Arc::clone(&store) as Arc<dyn Kv>);

        // A pre-existing key under a notify delete proves deletes are dropped
        // too, not just puts.
        store
            .write_batch(&[put(key::notify_key(3), b"stale")])
            .expect("seed");
        tail.apply_committed(&Range0Frame::new(
            1,
            vec![
                put(key::notify_key(4), b"record"),
                put(b"catalog/table".to_vec(), b"created"),
                WriteOp::Delete {
                    key: key::notify_key(3),
                },
            ],
        ))
        .expect("apply");

        assert!(store.get(&key::notify_key(4)).expect("get") == None);
        assert!(store.get(&key::notify_key(3)).expect("get") == Some(b"stale".to_vec()));
        assert!(store.get(b"catalog/table").expect("get") == Some(b"created".to_vec()));
    }

    #[test]
    fn a_checkpoint_reset_is_visible_through_a_handle_captured_before_it() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(Arc::clone(&store) as Arc<dyn Kv>);
        // The read-only replica captures this handle once, at construction.
        let catalog_kv = tail.store_handle();
        tail.apply_committed(&Range0Frame::new(
            3,
            vec![put(b"catalog/before".to_vec(), b"old")],
        ))
        .expect("apply");

        let rebuilt = Arc::new(MemKv::default());
        rebuilt
            .write_batch(&[put(b"catalog/restored".to_vec(), b"checkpoint")])
            .expect("restore");
        tail.reset_to_checkpoint(Arc::clone(&rebuilt) as Arc<dyn Kv>, 9)
            .expect("reset");

        assert!(catalog_kv.get(b"catalog/restored").expect("get") == Some(b"checkpoint".to_vec()));
        assert!(catalog_kv.get(b"catalog/before").expect("get") == None);
        assert!(tail.applied_offset() == 9);
        // Later frames land in the adopted store, not the abandoned one.
        tail.apply_committed(&Range0Frame::new(
            10,
            vec![put(b"catalog/after".to_vec(), b"new")],
        ))
        .expect("apply after reset");
        assert!(rebuilt.get(b"catalog/after").expect("get") == Some(b"new".to_vec()));
        assert!(store.get(b"catalog/after").expect("get") == None);
    }

    #[tokio::test]
    async fn a_reader_released_by_the_reset_offset_sees_the_adopted_store() {
        let tail = Range0Tail::new(Arc::new(MemKv::default()));
        let catalog_kv = tail.store_handle();
        let waiter = {
            let tail = tail.clone();
            tokio::spawn(async move {
                tail.wait_until_applied(7).await.expect("barrier");
                catalog_kv.get(b"catalog/restored").expect("get")
            })
        };
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!waiter.is_finished());

        let rebuilt = Arc::new(MemKv::default());
        rebuilt
            .write_batch(&[put(b"catalog/restored".to_vec(), b"checkpoint")])
            .expect("restore");
        tail.reset_to_checkpoint(rebuilt, 7).expect("reset");

        let read = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter completes")
            .expect("join");
        assert!(read == Some(b"checkpoint".to_vec()));
    }

    #[test]
    fn a_checkpoint_reset_never_moves_the_applied_offset_backwards() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(Arc::clone(&store) as Arc<dyn Kv>);
        tail.apply_committed(&Range0Frame::new(
            5,
            vec![put(b"catalog/live".to_vec(), b"kept")],
        ))
        .expect("apply");

        let stale = Arc::new(MemKv::default());
        let rejected = tail.reset_to_checkpoint(Arc::clone(&stale) as Arc<dyn Kv>, 5);

        assert!(let Err(Range0TailError::NonMonotoneOffset { current: 5, incoming: 5 }) = rejected);
        assert!(tail.applied_offset() == 5);
        // The rejected reset must not have swapped the store either.
        assert!(tail.store_handle().get(b"catalog/live").expect("get") == Some(b"kept".to_vec()));
    }

    #[test]
    fn merge_rules_survive_a_notify_op_in_the_same_batch() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(Arc::clone(&store) as Arc<dyn Kv>);

        tail.apply_committed(&Range0Frame::new(
            1,
            vec![
                put(key::next_xid_key(), &9_u64.to_be_bytes()),
                clog::put_op(5, XidStatus::Committed),
            ],
        ))
        .expect("apply first");
        tail.apply_committed(&Range0Frame::new(
            2,
            vec![
                put(key::notify_key(1), b"record"),
                // A regressing counter still loses and a terminal clog decision
                // is still write-once, with a notify op interleaved.
                put(key::next_xid_key(), &7_u64.to_be_bytes()),
                clog::put_op(5, XidStatus::Aborted),
            ],
        ))
        .expect("apply second");

        assert!(
            store.get(&key::next_xid_key()).expect("get") == Some(9_u64.to_be_bytes().to_vec())
        );
        assert!(clog::get(store.as_ref(), 5).expect("clog") == XidStatus::Committed);
        assert!(store.get(&key::notify_key(1)).expect("get") == None);
    }
}

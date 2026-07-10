//! Deterministic range-0 tail over committed WAL frames.

use std::sync::Arc;

use crabka_pgkv::{Kv, KvError, WriteOp};
use tokio::sync::watch;

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

/// Local range-0 tail state and offset observable.
#[derive(Clone)]
pub struct Range0Tail {
    store: Arc<dyn Kv>,
    applied_offset_tx: watch::Sender<i64>,
    _applied_offset_rx: watch::Receiver<i64>,
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
            store,
            applied_offset_tx,
            _applied_offset_rx: applied_offset_rx,
        }
    }

    /// Apply a committed frame with G-2 merge rules and publish its offset.
    pub fn apply_committed(&self, frame: &Range0Frame) -> Result<(), Range0TailError> {
        if frame.offset <= *self.applied_offset_tx.borrow() {
            return Err(Range0TailError::NonMonotoneOffset {
                current: *self.applied_offset_tx.borrow(),
                incoming: frame.offset,
            });
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
    #[must_use]
    pub fn store_handle(&self) -> Arc<dyn Kv> {
        Arc::clone(&self.store)
    }

    /// Wait until the local tail has applied at least `target_offset`.
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

    use crabka_pgkv::{Kv, KvError, WriteOp, key};
    use crabka_pgmvcc::clog;

    pub(super) fn apply_frame(kv: &dyn Kv, ops: &[WriteOp]) -> Result<(), KvError> {
        let mut counters: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut decided: HashSet<Vec<u8>> = HashSet::new();
        let mut adjusted = Vec::with_capacity(ops.len());

        for op in ops {
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
    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv, WriteOp, key};

    use super::*;

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
}

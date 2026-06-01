//! Pure reassembly state machine for a follower fetching a KIP-630 snapshot
//! over `FetchSnapshot` (api key 59). IO-free: the async engine
//! (`controller.rs`) owns the transport and applies the [`SnapshotFetchStep`]
//! this returns. Chunks must arrive in order (position == bytes received so
//! far); any mismatch or a changed snapshot id aborts the transfer so the
//! engine restarts cleanly against the current leader.

use bytes::{Bytes, BytesMut};

use crate::kraft::types::NodeId;

/// Snapshot identity: (`end_offset` exclusive, epoch). Matches KIP-630 `SnapshotId`.
pub type SnapshotId = (i64, i32);

/// In-flight reassembly of one snapshot from one leader.
#[derive(Debug)]
pub struct SnapshotFetchState {
    pub snapshot_id: SnapshotId,
    pub leader_id: NodeId,
    buf: BytesMut,
    size: Option<i64>,
}

/// What the engine should do after feeding a chunk in.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotFetchStep {
    /// Request the next byte range starting at `next_position`.
    Continue { next_position: i64 },
    /// All bytes received; holds the assembled snapshot.
    Complete(Bytes),
    /// Abort: id mismatch / out-of-order / leader change. Engine discards
    /// this state and falls back to a normal Fetch.
    Restart,
}

impl SnapshotFetchState {
    #[must_use]
    pub fn new(snapshot_id: SnapshotId, leader_id: NodeId) -> Self {
        Self {
            snapshot_id,
            leader_id,
            buf: BytesMut::new(),
            size: None,
        }
    }

    /// The byte position to request next (bytes received so far).
    #[must_use]
    pub fn next_position(&self) -> i64 {
        i64::try_from(self.buf.len()).unwrap_or(i64::MAX)
    }

    /// Feed one response chunk. `id`/`size`/`position` come from the
    /// `FetchSnapshot` response; `chunk` is `unaligned_records` bytes.
    pub fn on_chunk(
        &mut self,
        id: SnapshotId,
        size: i64,
        position: i64,
        chunk: &[u8],
    ) -> SnapshotFetchStep {
        if id != self.snapshot_id || position != self.next_position() || size < 0 {
            return SnapshotFetchStep::Restart;
        }
        match self.size {
            Some(s) if s != size => return SnapshotFetchStep::Restart,
            _ => self.size = Some(size),
        }
        self.buf.extend_from_slice(chunk);
        if self.next_position() >= size {
            SnapshotFetchStep::Complete(self.buf.split().freeze())
        } else {
            SnapshotFetchStep::Continue {
                next_position: self.next_position(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn assembles_in_order_chunks_to_complete() {
        let mut s = SnapshotFetchState::new((10, 1), 2);
        assert!(s.next_position() == 0);
        let step = s.on_chunk((10, 1), 6, 0, b"abc");
        assert!(step == SnapshotFetchStep::Continue { next_position: 3 });
        let step = s.on_chunk((10, 1), 6, 3, b"def");
        match step {
            SnapshotFetchStep::Complete(b) => assert!(b.as_ref() == b"abcdef"),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn out_of_order_position_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), 2);
        let _ = s.on_chunk((10, 1), 6, 0, b"abc");
        assert!(s.on_chunk((10, 1), 6, 99, b"def") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn mismatched_id_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), 2);
        assert!(s.on_chunk((11, 1), 6, 0, b"abc") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn single_chunk_completes() {
        let mut s = SnapshotFetchState::new((5, 0), 1);
        match s.on_chunk((5, 0), 3, 0, b"xyz") {
            SnapshotFetchStep::Complete(b) => assert!(b.as_ref() == b"xyz"),
            other => panic!("expected Complete, got {other:?}"),
        }
    }
}

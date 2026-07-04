//! Pure reassembly state machine for a follower fetching a KIP-630 snapshot
//! over `FetchSnapshot` (api key 59). IO-free: the async engine
//! (`controller.rs`) owns the transport and applies the [`SnapshotFetchStep`]
//! this returns. Chunks must arrive in order (position == bytes received so
//! far); any mismatch or a changed snapshot id aborts the transfer so the
//! engine restarts cleanly against the current leader.

use bytes::{Bytes, BytesMut};

use crate::types::NodeId;

/// Snapshot identity: (`end_offset` exclusive, epoch). Matches KIP-630 `SnapshotId`.
pub type SnapshotId = (i64, i32);

/// Hard ceiling on the total bytes a follower will reassemble for one snapshot.
///
/// The `__cluster_metadata` snapshot is bounded by the size of cluster metadata
/// (topics, partitions, ACLs, configs, broker registrations) — realistically
/// tens of MiB even for very large clusters. 1 GiB is generous-but-finite: no
/// legitimate cluster reaches it, while it caps the memory a peer the follower
/// believes is the leader can force it to allocate (denial-of-service finding
/// M-3). A peer declaring (or streaming) more than this aborts the transfer.
const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024 * 1024;

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
        // `size < 0` is rejected first, so the `as u64` cast below is exact and
        // never wraps; the declared total must also stay under the hard cap so a
        // hostile leader cannot make us reassemble unbounded bytes (finding M-3).
        if id != self.snapshot_id
            || position != self.next_position()
            || size < 0
            || size.cast_unsigned() > MAX_SNAPSHOT_BYTES as u64
        {
            return SnapshotFetchStep::Restart;
        }
        match self.size {
            Some(s) if s != size => return SnapshotFetchStep::Restart,
            _ => self.size = Some(size),
        }
        // Reject chunk overshoot: a well-behaved leader never sends bytes beyond
        // its declared `size`. Without this a leader could declare a tiny `size`
        // yet stream a giant chunk, pushing `buf` past `size` and still
        // Completing. With this guard `buf` is bounded by `min(size, cap)`.
        // `size - position >= 0` here (position == buf.len() <= size, checked
        // each call), so the comparison is well-defined.
        if i64::try_from(chunk.len()).unwrap_or(i64::MAX) > size - position {
            return SnapshotFetchStep::Restart;
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
    use assert2::assert;

    use super::*;

    #[test]
    fn assembles_in_order_chunks_to_complete() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
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
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        let _ = s.on_chunk((10, 1), 6, 0, b"abc");
        assert!(s.on_chunk((10, 1), 6, 99, b"def") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn mismatched_id_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        assert!(s.on_chunk((11, 1), 6, 0, b"abc") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn chunk_overshooting_declared_size_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        // Leader declares size 3 but streams 5 bytes in one chunk.
        assert!(s.on_chunk((10, 1), 3, 0, b"abcde") == SnapshotFetchStep::Restart);
        // buf must not have been blown past the declared size.
        assert!(s.next_position() == 0);
    }

    #[test]
    fn declared_size_over_cap_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        let too_big = i64::try_from(MAX_SNAPSHOT_BYTES).unwrap() + 1;
        assert!(s.on_chunk((10, 1), too_big, 0, b"abc") == SnapshotFetchStep::Restart);
        assert!(s.next_position() == 0);
    }

    #[test]
    fn single_chunk_completes() {
        let mut s = SnapshotFetchState::new((5, 0), NodeId(1));
        match s.on_chunk((5, 0), 3, 0, b"xyz") {
            SnapshotFetchStep::Complete(b) => assert!(b.as_ref() == b"xyz"),
            other => panic!("expected Complete, got {other:?}"),
        }
    }
}

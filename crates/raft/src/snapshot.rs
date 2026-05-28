//! KIP-630 metadata snapshot artifact: `<offset>-<epoch>.checkpoint`.
//!
//! Slice S1: the self-contained format layer — image ⇄ record sequence,
//! the `.checkpoint` filename grammar, and the canonical on-disk bytes
//! (header/data/footer Kafka `RecordBatch`es). Later slices (S2/S5) wire
//! these into snapshot generation, log truncation, and `FetchSnapshot`
//! serving, so not every helper is consumed yet.

#![allow(dead_code)]

/// Identifies a snapshot by the log position it covers: `end_offset` is
/// the offset of the last record contained in the snapshot, and `epoch`
/// is the leader epoch at that offset. The on-disk artifact is named
/// `<end_offset>-<epoch>.checkpoint` with both fields zero-padded so
/// lexical sort matches numeric sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotId {
    pub end_offset: i64,
    pub epoch: i32,
}

impl SnapshotId {
    pub(crate) fn file_name(self) -> String {
        format!("{:020}-{:010}.checkpoint", self.end_offset, self.epoch)
    }

    pub(crate) fn parse(name: &str) -> Option<Self> {
        let stem = name.strip_suffix(".checkpoint")?;
        let (off, ep) = stem.split_once('-')?;
        Some(Self {
            end_offset: off.parse().ok()?,
            epoch: ep.parse().ok()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_id_name_round_trips() {
        let id = SnapshotId {
            end_offset: 1847,
            epoch: 3,
        };
        assert_eq!(id.file_name(), "00000000000000001847-0000000003.checkpoint");
        assert_eq!(SnapshotId::parse(&id.file_name()), Some(id));
    }
}

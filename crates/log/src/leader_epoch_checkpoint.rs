//! Per-partition `.leader-epoch-checkpoint` file. Two-column text
//! format matching Apache Kafka exactly:
//!
//! ```text
//!   0          <-- header version
//!   <n>        <-- row count
//!   <epoch_0> <start_offset_0>
//!   <epoch_1> <start_offset_1>
//!   ...
//! ```
//!
//! Byte layout is preserved so `kafka-dump-log` can read our files.

#![allow(dead_code)]

use std::fmt::Write as _;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use tracing::instrument;

use crate::error::LogError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EpochEntry {
    pub epoch: i32,
    pub start_offset: i64,
}

#[derive(Debug)]
pub struct LeaderEpochCheckpoint {
    path: PathBuf,
    entries: Vec<EpochEntry>,
}

/// Kafka sentinel: "no leader epoch information".
pub const UNDEFINED_EPOCH: i32 = -1;
/// Kafka sentinel: "no offset".
pub const UNDEFINED_OFFSET: i64 = -1;

impl LeaderEpochCheckpoint {
    /// Open (or recover) the checkpoint at `path`. Missing file → empty.
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let entries = match fs::read_to_string(&path) {
            Ok(s) => Self::parse(&s)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(LogError::Io(e)),
        };
        tracing::Span::current().record("entries", entries.len());
        Ok(Self { path, entries })
    }

    fn parse(s: &str) -> Result<Vec<EpochEntry>, LogError> {
        let mut lines = s.lines();
        let _version = lines.next();
        let count: usize = lines
            .next()
            .and_then(|l| l.trim().parse().ok())
            .unwrap_or(0);
        // Do NOT pre-size from the untrusted `count`: a corrupt or hostile
        // checkpoint (local dir, or bytes restored from tiered storage) could
        // declare a huge count and trigger a multi-GB allocation before the
        // bounded `lines.take(count)` loop ever runs. `count` is used only to
        // bound the number of rows read; the Vec grows as entries are parsed.
        // Matches Kafka's CheckpointFile, which reads entries line-by-line.
        let mut out = Vec::new();
        for line in lines.take(count) {
            let mut parts = line.split_whitespace();
            let epoch = parts
                .next()
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| LogError::Corrupt(format!("bad checkpoint row: {line:?}")))?;
            let start_offset = parts
                .next()
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| LogError::Corrupt(format!("bad checkpoint row: {line:?}")))?;
            out.push(EpochEntry {
                epoch,
                start_offset,
            });
        }
        Ok(out)
    }

    /// Append `(epoch, start_offset)`. Idempotent: re-appending an entry
    /// with the same epoch is a no-op (keeps the earliest recorded
    /// `start_offset`). Rewrites the file atomically.
    #[instrument(level = "debug", skip(self), fields(epoch, start_offset), err)]
    pub fn append(&mut self, epoch: i32, start_offset: i64) -> Result<(), LogError> {
        if append_to(&mut self.entries, epoch, start_offset) {
            self.flush()?;
        }
        Ok(())
    }

    /// Remove epoch entries that begin at or after `end_offset` (mirrors Kafka's
    /// LeaderEpochFileCache.truncateFromEnd). Persists if anything changed.
    #[instrument(level = "debug", skip(self), fields(end_offset), err)]
    pub fn truncate_from_end(&mut self, end_offset: i64) -> Result<(), LogError> {
        let before = self.entries.len();
        truncate_to(&mut self.entries, end_offset);
        if self.entries.len() != before {
            self.flush()?;
        }
        Ok(())
    }

    /// Drop every recorded epoch (mirrors Kafka's
    /// `LeaderEpochFileCache.clearAndFlush`, invoked by
    /// `LocalLog.truncateFullyAndStartAt`). Used by [`Log::reset_to`]: once the
    /// log has been emptied, no offset has a backing record, so no epoch may be
    /// advertised. Persists the now-empty file only when something was removed.
    #[instrument(level = "debug", skip(self), fields(cleared = self.entries.len()), err)]
    pub fn clear(&mut self) -> Result<(), LogError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        self.entries.clear();
        self.flush()
    }

    fn flush(&self) -> Result<(), LogError> {
        let mut s = String::new();
        s.push_str("0\n");
        let _ = writeln!(s, "{}", self.entries.len());
        for e in &self.entries {
            let _ = writeln!(s, "{} {}", e.epoch, e.start_offset);
        }
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp).map_err(LogError::Io)?;
            f.write_all(s.as_bytes()).map_err(LogError::Io)?;
            f.sync_data().map_err(LogError::Io)?;
        }
        fs::rename(&tmp, &self.path).map_err(LogError::Io)?;
        Ok(())
    }

    /// End offset of `epoch` = `start_offset` of the next-larger recorded
    /// epoch, or `log_end_offset` if `epoch` is the current epoch.
    /// Returns -1 (`UNDEFINED_OFFSET`) if `epoch` is unknown.
    #[must_use]
    pub fn end_offset_for_epoch(&self, epoch: i32, log_end_offset: i64) -> i64 {
        if !self.entries.iter().any(|e| e.epoch == epoch) {
            return -1;
        }
        // End of `epoch` is the start of the next-larger epoch. Higher
        // epochs always carry higher start offsets, so the minimum start
        // among epochs `> epoch` is that next epoch's start; if `epoch` is
        // the latest, there is none and the end is the log end. No clone or
        // sort — a single linear pass.
        self.entries
            .iter()
            .filter(|e| e.epoch > epoch)
            .map(|e| e.start_offset)
            .min()
            .unwrap_or(log_end_offset)
    }

    /// Floor lookup: return the epoch of the entry whose `start_offset` is the
    /// greatest value `<= offset`, i.e. the leader epoch that owned `offset`.
    ///
    /// Returns `None` when the checkpoint has no entries, or when `offset`
    /// precedes the first entry's `start_offset` (the offset predates any
    /// recorded epoch boundary).
    ///
    /// Since entries are stored in increasing `start_offset` order (by
    /// construction: `append` always writes the epoch that is current, which
    /// has a `start_offset` >= every prior entry), this is a single linear
    /// scan from the back — equivalent to finding the last entry with
    /// `start_offset <= offset`.
    #[must_use]
    pub fn epoch_for_offset(&self, offset: i64) -> Option<i32> {
        self.entries
            .iter()
            .filter(|e| e.start_offset <= offset)
            .max_by_key(|e| e.start_offset)
            .map(|e| e.epoch)
    }

    /// Kafka `LeaderEpochFileCache.endOffsetFor`. Returns
    /// `(found_epoch, end_offset)` — the epoch the requested offset range
    /// actually belongs to on this log, and the first offset *after* that
    /// epoch. Used to detect follower/consumer log divergence (KIP-320):
    ///
    ///  - `requested == UNDEFINED_EPOCH`            → `(UNDEFINED_EPOCH, log_end_offset)`
    ///  - `requested == latest recorded epoch`      → `(requested, log_end_offset)`
    ///  - `requested` above all recorded epochs     → `(UNDEFINED_EPOCH, log_end_offset)`
    ///  - `requested` below all recorded epochs     → `(requested, first_recorded_start)`
    ///  - otherwise (gap or exact older match)      → `(floor_epoch, next_epoch_start)`
    ///
    /// where `floor_epoch` is the largest recorded epoch `<= requested`.
    /// `end_offset` is always a valid truncation target (`>= 0`).
    #[must_use]
    pub fn epoch_and_offset_for(&self, requested_epoch: i32, log_end_offset: i64) -> (i32, i64) {
        epoch_and_offset_for_entries(&self.entries, requested_epoch, log_end_offset)
    }

    #[must_use]
    pub fn latest_epoch(&self) -> Option<i32> {
        self.entries.iter().map(|e| e.epoch).max()
    }

    #[must_use]
    pub fn entries(&self) -> &[EpochEntry] {
        &self.entries
    }
}

/// Pure core of [`LeaderEpochCheckpoint::epoch_and_offset_for`] over a raw slice,
/// so it can be exhaustively + property-tested without a file. The method
/// delegates to this. See `leader_epoch_model.rs` for the divergence-safety model.
#[must_use]
pub fn epoch_and_offset_for_entries(
    entries: &[EpochEntry],
    requested_epoch: i32,
    log_end_offset: i64,
) -> (i32, i64) {
    if requested_epoch == UNDEFINED_EPOCH {
        return (UNDEFINED_EPOCH, log_end_offset);
    }
    if entries.iter().map(|e| e.epoch).max() == Some(requested_epoch) {
        return (requested_epoch, log_end_offset);
    }
    // Smallest recorded epoch strictly greater than `requested`.
    let higher = entries
        .iter()
        .filter(|e| e.epoch > requested_epoch)
        .min_by_key(|e| e.epoch);
    match higher {
        // `requested` is in the future relative to this log.
        None => (UNDEFINED_EPOCH, log_end_offset),
        Some(next) => {
            // Largest recorded epoch <= requested (the floor).
            let floor = entries
                .iter()
                .filter(|e| e.epoch <= requested_epoch)
                .map(|e| e.epoch)
                .max();
            match floor {
                Some(f) => (f, next.start_offset),
                // `requested` is below the first recorded epoch.
                None => (requested_epoch, next.start_offset),
            }
        }
    }
}

/// Pure core of [`LeaderEpochCheckpoint::append`]: idempotent push-if-absent.
/// Returns `true` if a new entry was added (so the caller knows to flush).
pub(crate) fn append_to(entries: &mut Vec<EpochEntry>, epoch: i32, start_offset: i64) -> bool {
    if entries.iter().any(|e| e.epoch == epoch) {
        return false;
    }
    entries.push(EpochEntry {
        epoch,
        start_offset,
    });
    true
}

/// Pure core of [`LeaderEpochCheckpoint::truncate_from_end`]: drop entries that
/// begin at or after `end_offset`.
pub(crate) fn truncate_to(entries: &mut Vec<EpochEntry>, end_offset: i64) {
    entries.retain(|e| e.start_offset < end_offset);
}

#[cfg(test)]
#[path = "leader_epoch_model.rs"]
mod leader_epoch_model;

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use tempfile::TempDir;

    fn fresh() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("leader-epoch-checkpoint");
        (dir, path)
    }

    #[test]
    fn round_trip_byte_compat_format() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();

        let s = std::fs::read_to_string(&path).unwrap();
        assert!(s == "0\n3\n0 0\n1 50\n2 100\n");
    }

    #[test]
    fn append_preserves_existing_rows() {
        let (_d, path) = fresh();
        {
            let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
            c.append(0, 0).unwrap();
        }
        let mut c2 = LeaderEpochCheckpoint::open(path).unwrap();
        c2.append(1, 50).unwrap();
        assert!(c2.entries().len() == 2);
    }

    #[test]
    fn append_idempotent_for_same_epoch() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(0, 999).unwrap(); // ignored; epoch 0 already recorded
        assert!(
            c.entries()
                == &[EpochEntry {
                    epoch: 0,
                    start_offset: 0
                }]
        );
    }

    #[test]
    fn end_offset_for_current_epoch_returns_log_end_offset() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        assert!(c.end_offset_for_epoch(1, 100) == 100);
    }

    #[test]
    fn end_offset_for_older_epoch_returns_next_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();
        assert!(c.end_offset_for_epoch(0, 200) == 50);
        assert!(c.end_offset_for_epoch(1, 200) == 100);
    }

    #[test]
    fn end_offset_for_unknown_epoch_returns_undefined() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        assert!(c.end_offset_for_epoch(7, 200) == -1);
    }

    #[test]
    fn truncate_from_end_removes_entries_at_or_after_end_offset() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(1, 0).unwrap();
        c.append(7, 4).unwrap();
        c.truncate_from_end(4).unwrap();
        assert!(c.latest_epoch() == Some(1));
        // Epoch 7 began at offset 4 (>= end_offset), so it is gone.
        assert!(c.end_offset_for_epoch(7, 4) == -1);
        // Epoch 1 survives; its end is now the log end (4).
        assert!(c.end_offset_for_epoch(1, 4) == 4);
    }

    #[test]
    fn clear_removes_all_entries_and_persists_empty() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.append(1, 0).unwrap();
        c.append(2, 50).unwrap();
        c.clear().unwrap();
        assert!(c.entries().is_empty());
        assert!(c.latest_epoch() == None);
        // Persisted: a reopen sees no entries.
        let reopened = LeaderEpochCheckpoint::open(path).unwrap();
        assert!(reopened.entries().is_empty());
    }

    #[test]
    fn clear_on_empty_cache_skips_flush_and_writes_no_file() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.clear().unwrap();
        assert!(c.entries().is_empty());
        // The early-return skips the flush for an already-empty cache, so no
        // checkpoint file is written. A forced-`false` empty-guard would flush
        // an empty file here instead.
        assert!(!path.exists());
    }

    #[test]
    fn missing_file_yields_empty() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert!(c.entries().is_empty());
        assert!(c.latest_epoch() == None);
    }

    #[test]
    fn absurd_declared_count_does_not_over_allocate() {
        // Hostile/corrupt checkpoint: header declares billions of rows but only
        // one actual entry line follows. Parsing must not pre-size a giant Vec;
        // it should grow to fit the real rows and return just those.
        let s = "0\n9999999999999\n3 42\n";
        let entries = LeaderEpochCheckpoint::parse(s).unwrap();
        assert!(
            entries
                == [EpochEntry {
                    epoch: 3,
                    start_offset: 42,
                }],
            "only the one real row is parsed despite the absurd declared count"
        );
        // `lines.take(count)` bounds reads to the available lines, so capacity
        // stays at the grown size, not the untrusted billions.
        assert!(entries.capacity() < 9_999_999_999_999);
    }

    // ── epoch_for_offset ──────────────────────────────────────────────────────

    #[test]
    fn epoch_for_offset_empty_returns_none() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert!(c.epoch_for_offset(0) == None, "empty checkpoint → None");
        assert!(c.epoch_for_offset(100) == None, "empty checkpoint → None");
    }

    #[test]
    fn epoch_for_offset_before_first_entry_returns_none() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        // Epoch 0 starts at offset 10 (first entry does not start at 0).
        c.append(0, 10).unwrap();
        c.append(1, 50).unwrap();
        assert!(
            c.epoch_for_offset(9) == None,
            "offset before first entry's start_offset → None"
        );
    }

    #[test]
    fn epoch_for_offset_within_epoch_range() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();
        // Offsets 0–49 belong to epoch 0.
        assert!(c.epoch_for_offset(0) == Some(0), "start of epoch 0");
        assert!(c.epoch_for_offset(25) == Some(0), "middle of epoch 0");
        assert!(
            c.epoch_for_offset(49) == Some(0),
            "last offset before epoch 1"
        );
        // Offsets 50–99 belong to epoch 1.
        assert!(c.epoch_for_offset(50) == Some(1), "start of epoch 1");
        assert!(c.epoch_for_offset(75) == Some(1), "middle of epoch 1");
        assert!(
            c.epoch_for_offset(99) == Some(1),
            "last offset before epoch 2"
        );
    }

    #[test]
    fn epoch_for_offset_at_epoch_boundary() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        // Offset exactly at epoch 1's start_offset → belongs to epoch 1.
        assert!(
            c.epoch_for_offset(50) == Some(1),
            "boundary offset belongs to the epoch that starts there"
        );
    }

    #[test]
    fn epoch_for_offset_past_last_entry() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        // Any offset >= 50 that extends beyond the last known epoch → epoch 1
        // (the current / latest epoch owns all subsequent offsets).
        assert!(
            c.epoch_for_offset(100) == Some(1),
            "offset past last entry → last epoch"
        );
        assert!(
            c.epoch_for_offset(999) == Some(1),
            "far past last entry → last epoch"
        );
    }

    #[test]
    fn epoch_for_offset_single_entry_at_zero() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        assert!(c.epoch_for_offset(0) == Some(0));
        assert!(c.epoch_for_offset(1000) == Some(0));
    }

    // ── epoch_and_offset_for (KIP-320) ────────────────────────────────────────

    #[test]
    fn epoch_and_offset_latest_returns_pair_at_log_end() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        // Requested == latest recorded epoch → (epoch, log_end_offset).
        assert!(c.epoch_and_offset_for(1, 100) == (1, 100));
    }

    #[test]
    fn epoch_and_offset_older_returns_floor_epoch_and_next_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();
        // Recorded older epoch → (epoch, start of next epoch).
        assert!(c.epoch_and_offset_for(0, 200) == (0, 50));
        assert!(c.epoch_and_offset_for(1, 200) == (1, 100));
    }

    #[test]
    fn epoch_and_offset_gap_uses_floor_epoch() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(5, 100).unwrap();
        // Requested epoch 3 is not recorded; floor is epoch 0, next start 100.
        assert!(c.epoch_and_offset_for(3, 200) == (0, 100));
    }

    #[test]
    fn epoch_and_offset_future_epoch_is_undefined_at_log_end() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        // Requested epoch above everything recorded → (UNDEFINED, log_end).
        assert!(c.epoch_and_offset_for(7, 100) == (UNDEFINED_EPOCH, 100));
    }

    #[test]
    fn epoch_and_offset_below_all_returns_requested_and_first_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(3, 30).unwrap();
        c.append(4, 40).unwrap();
        // Requested epoch below the first recorded epoch.
        assert!(c.epoch_and_offset_for(1, 100) == (1, 30));
    }

    #[test]
    fn epoch_and_offset_empty_cache_is_undefined_at_log_end() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert!(c.epoch_and_offset_for(0, 9) == (UNDEFINED_EPOCH, 9));
    }
}

#[cfg(test)]
mod fuzz {
    use proptest::prelude::*;

    use super::{EpochEntry, UNDEFINED_EPOCH, append_to, epoch_and_offset_for_entries};

    /// Fold random `(epoch_gap, offset_jump)` steps into a strictly-increasing
    /// leader epoch-history (gaps allowed, mirroring how `append` builds one).
    fn leader_history(steps: &[(i32, i64)]) -> Vec<EpochEntry> {
        let mut v: Vec<EpochEntry> = vec![];
        let (mut le, mut lo) = (-1i32, -1i64);
        for &(de, doff) in steps {
            let e = le + 1 + de.rem_euclid(3); // epoch gap 1..=3
            let o = lo + 1 + doff.rem_euclid(1000); // offset jump 1..=1000
            append_to(&mut v, e, o);
            le = e;
            lo = o;
        }
        v
    }

    proptest! {
        /// Large-N randomized leader epoch-histories + requested epoch + follower
        /// log-end, asserting the same KIP-101/320 truncation contract the
        /// exhaustive `leader_epoch_model` checks, at a scale the BFS can't reach
        /// (histories up to 20 entries, epochs to ~60, offsets to ~20000).
        #[test]
        fn truncation_contract_holds(
            steps in proptest::collection::vec((0i32..10, 0i64..1000), 0..20usize),
            requested in -1i32..70,
            dleo in 0i64..2000,
        ) {
            let leader = leader_history(&steps);
            let last_off = leader.last().map_or(0, |e| e.start_offset);
            // Follower log end is at or past the last epoch boundary.
            let leo = last_off + 1 + dleo;
            let (found, trunc) = epoch_and_offset_for_entries(&leader, requested, leo);
            let latest = leader.iter().map(|e| e.epoch).max();

            // Always a valid truncation target.
            prop_assert!(trunc >= 0, "truncation target {} < 0", trunc);
            // The resolved epoch never exceeds the requested epoch.
            prop_assert!(
                found <= requested,
                "found_epoch {} > requested {}",
                found,
                requested
            );

            if let Some(entry) = leader.iter().find(|e| e.epoch == requested) {
                // Committed-prefix-preserved: never truncate below the start of
                // an epoch the leader and follower agree on.
                prop_assert!(
                    trunc >= entry.start_offset,
                    "truncation {} dropped agreed epoch {} (starts at {})",
                    trunc,
                    requested,
                    entry.start_offset
                );
                if latest == Some(requested) {
                    // Current epoch → keep up to the follower's log end.
                    prop_assert_eq!(found, requested);
                    prop_assert_eq!(trunc, leo, "latest epoch keeps up to log end");
                } else {
                    // Older agreed epoch → truncate to the next leader epoch's
                    // start, dropping the divergent higher-epoch suffix.
                    let next_start = leader
                        .iter()
                        .filter(|e| e.epoch > requested)
                        .map(|e| e.start_offset)
                        .min()
                        .expect("a non-latest recorded epoch has a higher epoch");
                    prop_assert_eq!(found, requested);
                    prop_assert_eq!(
                        trunc,
                        next_start,
                        "older epoch truncates to next epoch start"
                    );
                    prop_assert!(trunc <= leo, "truncation {} above log end {}", trunc, leo);
                }
            } else if requested == UNDEFINED_EPOCH {
                // No last epoch → no truncation this round.
                prop_assert_eq!(found, UNDEFINED_EPOCH);
                prop_assert_eq!(trunc, leo);
            }
        }
    }
}

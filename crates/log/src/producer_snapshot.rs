//! Kafka-compatible producer-state snapshot encoding and recovery.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write as _,
    path::{Path, PathBuf},
};

use bytes::{BufMut as _, BytesMut};
use crabka_ids::{Offset, ProducerId};

use crate::{LogError, name};

const VERSION: i16 = 1;
const HEADER_LEN: usize = 6;
const ENTRY_LEN: usize = 46;

type SnapshotState = HashMap<ProducerId, ProducerSnapshotEntry>;
type LoadedSnapshot = (Offset, SnapshotState);

/// Durable producer metadata stored in Kafka's `.snapshot` sidecar format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSnapshotEntry {
    pub producer_id: ProducerId,
    pub producer_epoch: i16,
    pub last_sequence: i32,
    pub last_offset: Offset,
    pub offset_delta: i32,
    pub timestamp: i64,
    pub coordinator_epoch: i32,
    pub current_txn_first_offset: Option<Offset>,
}

impl ProducerSnapshotEntry {
    pub(crate) fn empty(producer_id: ProducerId, producer_epoch: i16) -> Self {
        Self {
            producer_id,
            producer_epoch,
            last_sequence: -1,
            last_offset: Offset(-1),
            offset_delta: 0,
            timestamp: -1,
            coordinator_epoch: -1,
            current_txn_first_offset: None,
        }
    }
}

pub(crate) fn path(dir: &Path, offset: Offset) -> PathBuf {
    name::producer_snapshot_path(dir, offset.0)
}

pub(crate) fn list(dir: &Path) -> Result<Vec<(Offset, PathBuf)>, LogError> {
    let mut snapshots = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".snapshot") else {
            continue;
        };
        if stem.len() != name::FILENAME_DIGITS {
            continue;
        }
        let Ok(offset) = stem.parse::<i64>() else {
            continue;
        };
        snapshots.push((Offset(offset), entry.path()));
    }
    snapshots.sort_unstable_by_key(|(offset, _)| *offset);
    Ok(snapshots)
}

pub(crate) fn latest_at_or_before(
    dir: &Path,
    end: Offset,
) -> Result<Option<LoadedSnapshot>, LogError> {
    for (offset, path) in list(dir)?.into_iter().rev() {
        if offset > end {
            fs::remove_file(path)?;
            continue;
        }
        match read(&path) {
            Ok(entries) => return Ok(Some((offset, entries))),
            Err(LogError::Corrupt(_)) => {
                // Match Kafka recovery: discard a corrupt newest snapshot and
                // retry the preceding one instead of failing the partition.
                let _ = fs::remove_file(path);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

pub(crate) fn remove_after(dir: &Path, offset: Offset) -> Result<(), LogError> {
    for (snapshot_offset, path) in list(dir)? {
        if snapshot_offset > offset {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

pub(crate) fn remove_all(dir: &Path) -> Result<(), LogError> {
    for (_, path) in list(dir)? {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub(crate) fn write(
    dir: &Path,
    offset: Offset,
    entries: &HashMap<ProducerId, ProducerSnapshotEntry>,
) -> Result<PathBuf, LogError> {
    let destination = path(dir, offset);
    if destination.exists() {
        return Ok(destination);
    }

    let bytes = encode(entries)?;
    let temporary = destination.with_extension("snapshot.tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, &destination)?;
    Ok(destination)
}

fn encode(entries: &HashMap<ProducerId, ProducerSnapshotEntry>) -> Result<Vec<u8>, LogError> {
    let count = i32::try_from(entries.len())
        .map_err(|_| LogError::InvalidArgument("too many producer snapshot entries".into()))?;
    let capacity = HEADER_LEN
        .checked_add(4)
        .and_then(|size| size.checked_add(entries.len().checked_mul(ENTRY_LEN)?))
        .ok_or_else(|| LogError::InvalidArgument("producer snapshot size overflow".into()))?;
    let mut buffer = BytesMut::with_capacity(capacity);
    buffer.put_i16(VERSION);
    buffer.put_u32(0);
    buffer.put_i32(count);

    let mut ordered: Vec<_> = entries.values().copied().collect();
    ordered.sort_unstable_by_key(|entry| entry.producer_id);
    for entry in ordered {
        buffer.put_i64(entry.producer_id.get());
        buffer.put_i16(entry.producer_epoch);
        buffer.put_i32(entry.last_sequence);
        buffer.put_i64(entry.last_offset.0);
        buffer.put_i32(entry.offset_delta);
        buffer.put_i64(entry.timestamp);
        buffer.put_i32(entry.coordinator_epoch);
        buffer.put_i64(entry.current_txn_first_offset.map_or(-1, |offset| offset.0));
    }

    let crc = crc32c::crc32c(&buffer[HEADER_LEN..]);
    buffer[2..6].copy_from_slice(&crc.to_be_bytes());
    Ok(buffer.to_vec())
}

fn read(path: &Path) -> Result<HashMap<ProducerId, ProducerSnapshotEntry>, LogError> {
    let bytes = fs::read(path)?;
    if bytes.len() < HEADER_LEN + 4 {
        return Err(corrupt(path, "file is shorter than the snapshot header"));
    }
    let version = i16::from_be_bytes([bytes[0], bytes[1]]);
    if version != VERSION {
        return Err(corrupt(path, &format!("unsupported version {version}")));
    }
    let stored_crc = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
    let computed_crc = crc32c::crc32c(&bytes[HEADER_LEN..]);
    if stored_crc != computed_crc {
        return Err(corrupt(path, "CRC32C mismatch"));
    }

    let count = i32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    let count = usize::try_from(count).map_err(|_| corrupt(path, "negative entry count"))?;
    let expected = HEADER_LEN
        .checked_add(4)
        .and_then(|size| size.checked_add(count.checked_mul(ENTRY_LEN)?))
        .ok_or_else(|| corrupt(path, "entry count overflows file size"))?;
    if bytes.len() != expected {
        return Err(corrupt(path, "entry count does not match file length"));
    }

    let mut entries = HashMap::with_capacity(count);
    let mut cursor = HEADER_LEN + 4;
    for _ in 0..count {
        let producer_id = ProducerId(take_i64(&bytes, &mut cursor));
        let producer_epoch = take_i16(&bytes, &mut cursor);
        let last_sequence = take_i32(&bytes, &mut cursor);
        let last_offset = Offset(take_i64(&bytes, &mut cursor));
        let offset_delta = take_i32(&bytes, &mut cursor);
        let timestamp = take_i64(&bytes, &mut cursor);
        let coordinator_epoch = take_i32(&bytes, &mut cursor);
        let txn_offset = take_i64(&bytes, &mut cursor);
        if producer_id.is_none()
            || producer_epoch < 0
            || offset_delta < 0
            || txn_offset < -1
            || (last_offset.0 < 0 && last_sequence >= 0)
            || (last_offset.0 >= 0
                && (last_sequence < 0 || last_offset.0 < i64::from(offset_delta)))
        {
            return Err(corrupt(path, "entry contains an invalid producer state"));
        }
        let current_txn_first_offset = (txn_offset >= 0).then_some(Offset(txn_offset));
        let entry = ProducerSnapshotEntry {
            producer_id,
            producer_epoch,
            last_sequence,
            last_offset,
            offset_delta,
            timestamp,
            coordinator_epoch,
            current_txn_first_offset,
        };
        if entries.insert(producer_id, entry).is_some() {
            return Err(corrupt(path, "duplicate producer id"));
        }
    }
    Ok(entries)
}

fn corrupt(path: &Path, reason: &str) -> LogError {
    LogError::Corrupt(format!("producer snapshot {}: {reason}", path.display()))
}

fn take_i16(bytes: &[u8], cursor: &mut usize) -> i16 {
    let value = i16::from_be_bytes([bytes[*cursor], bytes[*cursor + 1]]);
    *cursor += 2;
    value
}

fn take_i32(bytes: &[u8], cursor: &mut usize) -> i32 {
    let value = i32::from_be_bytes(bytes[*cursor..*cursor + 4].try_into().expect("four bytes"));
    *cursor += 4;
    value
}

fn take_i64(bytes: &[u8], cursor: &mut usize) -> i64 {
    let value = i64::from_be_bytes(bytes[*cursor..*cursor + 8].try_into().expect("eight bytes"));
    *cursor += 8;
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry() -> ProducerSnapshotEntry {
        ProducerSnapshotEntry {
            producer_id: ProducerId(42),
            producer_epoch: 3,
            last_sequence: 9,
            last_offset: Offset(101),
            offset_delta: 4,
            timestamp: 1_234_567,
            coordinator_epoch: 8,
            current_txn_first_offset: Some(Offset(99)),
        }
    }

    fn sample() -> HashMap<ProducerId, ProducerSnapshotEntry> {
        let entry = sample_entry();
        HashMap::from([(entry.producer_id, entry)])
    }

    fn assert_rejected(entry: ProducerSnapshotEntry) {
        let dir = tempfile::tempdir().unwrap();
        let entries = HashMap::from([(entry.producer_id, entry)]);
        let path = write(dir.path(), Offset(1), &entries).unwrap();
        assert2::assert!(matches!(read(&path), Err(LogError::Corrupt(_))));
    }

    #[test]
    fn kafka_v1_snapshot_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), Offset(102), &sample()).unwrap();
        assert2::assert!(read(&path).unwrap() == sample());
    }

    #[test]
    fn empty_entry_uses_kafka_marker_sentinels() {
        let entry = ProducerSnapshotEntry::empty(ProducerId(7), 0);
        assert2::assert!(
            entry
                == ProducerSnapshotEntry {
                    producer_id: ProducerId(7),
                    producer_epoch: 0,
                    last_sequence: -1,
                    last_offset: Offset(-1),
                    offset_delta: 0,
                    timestamp: -1,
                    coordinator_epoch: -1,
                    current_txn_first_offset: None,
                }
        );
    }

    #[test]
    fn encoding_is_ordered_and_uses_minus_one_for_no_transaction() {
        let mut entries = sample();
        let marker = ProducerSnapshotEntry::empty(ProducerId(7), 0);
        entries.insert(marker.producer_id, marker);

        let bytes = encode(&entries).unwrap();
        assert2::assert!(bytes.len() == HEADER_LEN + 4 + 2 * ENTRY_LEN);
        assert2::assert!(i32::from_be_bytes(bytes[6..10].try_into().unwrap()) == 2);
        assert2::assert!(i64::from_be_bytes(bytes[10..18].try_into().unwrap()) == 7);
        assert2::assert!(i64::from_be_bytes(bytes[48..56].try_into().unwrap()) == -1);
        assert2::assert!(i64::from_be_bytes(bytes[56..64].try_into().unwrap()) == 42);
        assert2::assert!(i64::from_be_bytes(bytes[94..102].try_into().unwrap()) == 99);
    }

    #[test]
    fn empty_snapshot_has_exact_header_length_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), Offset(0), &HashMap::new()).unwrap();
        assert2::assert!(fs::metadata(&path).unwrap().len() == 10);
        assert2::assert!(read(&path).unwrap().is_empty());
    }

    #[test]
    fn every_short_header_length_is_rejected_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.snapshot");
        for length in 0..10 {
            fs::write(&path, vec![0; length]).unwrap();
            assert2::assert!(matches!(read(&path), Err(LogError::Corrupt(_))));
        }
    }

    #[test]
    fn crc_covers_entry_count_and_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), Offset(102), &sample()).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[9] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert2::assert!(matches!(read(&path), Err(LogError::Corrupt(_))));
    }

    #[test]
    fn rejects_unknown_version_and_truncated_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), Offset(102), &sample()).unwrap();
        let mut unknown = fs::read(&path).unwrap();
        unknown[1] = 2;
        fs::write(&path, unknown).unwrap();
        assert2::assert!(matches!(read(&path), Err(LogError::Corrupt(_))));

        fs::remove_file(&path).unwrap();
        let path = write(dir.path(), Offset(102), &sample()).unwrap();
        let mut truncated = fs::read(&path).unwrap();
        truncated.pop();
        fs::write(&path, truncated).unwrap();
        assert2::assert!(matches!(read(&path), Err(LogError::Corrupt(_))));
    }

    #[test]
    fn latest_snapshot_honors_inclusive_end_and_removes_future_files() {
        let dir = tempfile::tempdir().unwrap();
        for offset in [1, 2, 3] {
            write(dir.path(), Offset(offset), &sample()).unwrap();
        }

        let (offset, entries) = latest_at_or_before(dir.path(), Offset(2)).unwrap().unwrap();
        assert2::assert!(offset == Offset(2));
        assert2::assert!(entries == sample());
        assert2::assert!(path(dir.path(), Offset(1)).exists());
        assert2::assert!(path(dir.path(), Offset(2)).exists());
        assert2::assert!(!path(dir.path(), Offset(3)).exists());
    }

    #[test]
    fn corrupt_latest_snapshot_is_removed_and_previous_snapshot_is_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let previous = write(dir.path(), Offset(1), &sample()).unwrap();
        let corrupt = write(dir.path(), Offset(2), &sample()).unwrap();
        fs::write(&corrupt, b"broken").unwrap();

        let (offset, entries) = latest_at_or_before(dir.path(), Offset(2)).unwrap().unwrap();
        assert2::assert!(offset == Offset(1));
        assert2::assert!(entries == sample());
        assert2::assert!(previous.exists());
        assert2::assert!(!corrupt.exists());
    }

    #[test]
    fn remove_after_keeps_the_inclusive_boundary() {
        let dir = tempfile::tempdir().unwrap();
        for offset in [1, 2, 3] {
            write(dir.path(), Offset(offset), &sample()).unwrap();
        }

        remove_after(dir.path(), Offset(2)).unwrap();
        assert2::assert!(path(dir.path(), Offset(1)).exists());
        assert2::assert!(path(dir.path(), Offset(2)).exists());
        assert2::assert!(!path(dir.path(), Offset(3)).exists());
    }

    #[test]
    fn remove_all_removes_every_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        for offset in [1, 2] {
            write(dir.path(), Offset(offset), &sample()).unwrap();
        }

        remove_all(dir.path()).unwrap();
        assert2::assert!(list(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn rejects_each_invalid_entry_field_independently() {
        let mut entry = sample_entry();
        entry.producer_id = ProducerId(-1);
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.producer_epoch = -1;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.offset_delta = -1;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.current_txn_first_offset = Some(Offset(-2));
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.last_offset = Offset(-1);
        entry.last_sequence = 0;
        entry.offset_delta = 0;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.last_offset = Offset(0);
        entry.last_sequence = -1;
        entry.offset_delta = 0;
        assert_rejected(entry);

        let mut entry = sample_entry();
        entry.last_offset = Offset(0);
        entry.last_sequence = 0;
        entry.offset_delta = 1;
        assert_rejected(entry);
    }

    #[test]
    fn accepts_every_valid_entry_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let marker = ProducerSnapshotEntry::empty(ProducerId(7), 0);
        let data = ProducerSnapshotEntry {
            producer_id: ProducerId(8),
            producer_epoch: 0,
            last_sequence: 0,
            last_offset: Offset(0),
            offset_delta: 0,
            timestamp: 0,
            coordinator_epoch: 0,
            current_txn_first_offset: None,
        };
        let entries = HashMap::from([(marker.producer_id, marker), (data.producer_id, data)]);
        let path = write(dir.path(), Offset(1), &entries).unwrap();
        assert2::assert!(read(&path).unwrap() == entries);
    }
}

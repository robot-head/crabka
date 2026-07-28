//! Durable-across-process-crash local spool for the AU-5 degraded path.
//!
//! Holds exactly the chained audit records not yet written to the topic, in
//! order. Appends are flushed to the OS page cache (not `fsync`'d), and a torn
//! tail frame from a crash mid-append is healed on `open`; so this survives a
//! process crash, but an OS/power loss between append and replay can still lose
//! not-yet-replayed records. (Real `fsync` durability is a prerequisite for the
//! future fail-closed mode — tracked separately.)
//!
//! Frame: `[u32 len][record]`; record: `[u8 class_tag][u32 value_len]
//! [value][u32 header_count]([u32 klen][k][u32 vlen][v])*`. Synchronous
//! `std::fs` (degraded, low-frequency path); a truncated tail frame is treated
//! as end-of-data.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, ByteSizeExt as _},
};

use crate::{
    chain::{chain_hash, from_hex32},
    event::AuditEventClass,
    ids::{MaxSpoolBytes, RecordCount, Seq, SpoolBytes},
    sink::{AuditError, AuditRecord, HEADER_PREV_HASH, HEADER_SEQ},
};

const SPOOL_FILE: &str = "audit.spool";

fn io<E: std::fmt::Display>(e: E) -> AuditError {
    AuditError::Io(e.to_string())
}

/// Append-only durable spool file.
///
/// The byte cap and the running total are held as raw [`MaxSpoolBytes`] /
/// [`SpoolBytes`] newtypes: both are accumulated and compared exactly, which a
/// `f64`-backed [`ByteSize`] cannot promise. The quantity is the boundary type —
/// [`Spool::open`] takes one and [`Spool::size`] hands one back.
#[derive(Debug)]
pub struct Spool {
    path: PathBuf,
    file: File,
    max_bytes: MaxSpoolBytes,
    bytes: SpoolBytes,
    count: RecordCount,
}

impl Spool {
    /// Open (creating the dir + file if needed) and recover existing contents.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), max_size = %max_size.human(), count = tracing::field::Empty, bytes = tracing::field::Empty),
        err
    )]
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub fn open(dir: &Path, max_size: ByteSize) -> Result<Self, AuditError> {
        std::fs::create_dir_all(dir).map_err(io)?;
        let path = dir.join(SPOOL_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(io)?;
        let mut s = Self {
            path,
            file,
            max_bytes: MaxSpoolBytes(max_size.bytes_u64()),
            bytes: SpoolBytes(0),
            count: RecordCount(0),
        };
        let (records, valid_bytes) = s.scan()?;
        let physical = s.file.metadata().map_err(io)?.len();
        if valid_bytes.0 < physical {
            s.file.set_len(valid_bytes.0).map_err(io)?;
            tracing::warn!(
                physical,
                valid_bytes = valid_bytes.0,
                "audit spool: truncated torn tail frame on open"
            );
        }
        s.count = RecordCount(u64::try_from(records.len()).unwrap_or(u64::MAX));
        s.bytes = valid_bytes;
        let span = tracing::Span::current();
        span.record("count", s.count.0);
        span.record("bytes", s.bytes.0);
        Ok(s)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count.0 == 0
    }

    #[must_use]
    pub fn count(&self) -> RecordCount {
        self.count
    }

    /// How much the spool currently holds.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        ByteSize::from_bytes(self.bytes.0)
    }

    /// Append a record. Returns `Ok(false)` if it would exceed the configured
    /// cap.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?record.class, value_bytes = record.value.len(), count = self.count.0),
        err
    )]
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub fn append(&mut self, record: &AuditRecord) -> Result<bool, AuditError> {
        let frame = encode_frame(record);
        let frame_len = SpoolBytes(u64::try_from(frame.len()).unwrap_or(u64::MAX));
        if (self.bytes + frame_len).0 > self.max_bytes.0 {
            return Ok(false);
        }
        self.file.seek(SeekFrom::End(0)).map_err(io)?;
        self.file.write_all(&frame).map_err(io)?;
        self.file.flush().map_err(io)?;
        self.bytes += frame_len;
        self.count.0 += 1;
        Ok(true)
    }

    /// Scan the spool file, returning decoded records and the byte offset
    /// immediately after the last complete, successfully-decoded frame (the
    /// "logical length"). A truncated or corrupt tail frame is treated as
    /// end-of-data; `valid_bytes` points to just before that torn frame.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(records = tracing::field::Empty, valid_bytes = tracing::field::Empty),
        err
    )]
    fn scan(&self) -> Result<(Vec<AuditRecord>, SpoolBytes), AuditError> {
        let mut buf = Vec::new();
        {
            let mut f = File::open(&self.path).map_err(io)?;
            f.read_to_end(&mut buf).map_err(io)?;
        }
        let mut out = Vec::new();
        let mut cur: &[u8] = &buf;
        let mut valid_bytes = SpoolBytes(0);
        while cur.len() >= 4 {
            let len =
                usize::try_from(u32::from_be_bytes([cur[0], cur[1], cur[2], cur[3]])).unwrap_or(0);
            if cur.len() < 4 + len {
                break; // truncated tail frame
            }
            match decode_record(&cur[4..4 + len]) {
                Some(rec) => {
                    out.push(rec);
                    valid_bytes += SpoolBytes(u64::try_from(4 + len).unwrap_or(0));
                }
                None => break, // corrupt frame: stop (visible)
            }
            cur = &cur[4 + len..];
        }
        let span = tracing::Span::current();
        span.record("records", out.len());
        span.record("valid_bytes", valid_bytes.0);
        Ok((out, valid_bytes))
    }

    /// Read every record from the start of the spool, in order.
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub fn read_all(&self) -> Result<Vec<AuditRecord>, AuditError> {
        Ok(self.scan()?.0)
    }

    /// Replace the spool contents with exactly `remaining`, atomically.
    #[tracing::instrument(level = "debug", skip_all, fields(remaining = remaining.len()), err)]
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub fn rewrite(&mut self, remaining: &[AuditRecord]) -> Result<(), AuditError> {
        let tmp = self.path.with_extension("spool.tmp");
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .map_err(io)?;
            let mut bytes = SpoolBytes(0);
            for rec in remaining {
                let frame = encode_frame(rec);
                f.write_all(&frame).map_err(io)?;
                bytes += SpoolBytes(u64::try_from(frame.len()).unwrap_or(u64::MAX));
            }
            f.flush().map_err(io)?;
            self.bytes = bytes;
        }
        std::fs::rename(&tmp, &self.path).map_err(io)?;
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(io)?;
        self.count = RecordCount(u64::try_from(remaining.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Clear the spool.
    #[tracing::instrument(level = "debug", skip_all, fields(count = self.count.0, bytes = self.bytes.0), err)]
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub fn truncate(&mut self) -> Result<(), AuditError> {
        self.file.set_len(0).map_err(io)?;
        self.file.seek(SeekFrom::Start(0)).map_err(io)?;
        self.file.flush().map_err(io)?;
        self.bytes = SpoolBytes(0);
        self.count = RecordCount(0);
        Ok(())
    }

    /// `(next_seq, head)` implied by the last chained (non-checkpoint) record,
    /// or `None` if the spool has no chained records.
    #[tracing::instrument(level = "debug", skip_all, fields(count = self.count.0), err)]
    /// # Errors
    /// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
    pub fn resume_point(&self) -> Result<Option<(u64, [u8; 32])>, AuditError> {
        let records = self.read_all()?;
        Ok(records
            .iter()
            .rev()
            .find_map(resume_from_record)
            .map(|(seq, head)| (seq.0, head)))
    }
}

/// Compute `(next_seq, head_after)` from a single chained record's headers +
/// value. Returns `None` for checkpoints or records missing chain headers.
fn resume_from_record(rec: &AuditRecord) -> Option<(Seq, [u8; 32])> {
    if rec.class == AuditEventClass::Checkpoint {
        return None;
    }
    let mut seq: Option<u64> = None;
    let mut prev: Option<[u8; 32]> = None;
    for (k, v) in &rec.headers {
        if k == HEADER_SEQ {
            seq = std::str::from_utf8(v).ok().and_then(|s| s.parse().ok());
        } else if k == HEADER_PREV_HASH {
            prev = std::str::from_utf8(v).ok().and_then(from_hex32);
        }
    }
    let (seq, prev) = (seq?, prev?);
    let head = chain_hash(&prev, seq, &rec.value);
    Some((Seq(seq + 1), head))
}

fn encode_frame(record: &AuditRecord) -> Vec<u8> {
    let body = encode_record(record);
    let len = u32::try_from(body.len()).expect("audit record fits u32");
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn encode_record(record: &AuditRecord) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(record.class.tag());
    put_bytes(&mut b, &record.value);
    let hc = u32::try_from(record.headers.len()).expect("header count fits u32");
    b.extend_from_slice(&hc.to_be_bytes());
    for (k, v) in &record.headers {
        put_bytes(&mut b, k.as_bytes());
        put_bytes(&mut b, v);
    }
    b
}

fn put_bytes(b: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("field fits u32");
    b.extend_from_slice(&len.to_be_bytes());
    b.extend_from_slice(bytes);
}

fn decode_record(mut b: &[u8]) -> Option<AuditRecord> {
    let class = AuditEventClass::from_tag(*b.first()?)?;
    b = &b[1..];
    let value = take_bytes(&mut b)?;
    let hc = usize::try_from(take_u32(&mut b)?).unwrap_or(0);
    let mut headers = Vec::with_capacity(hc);
    for _ in 0..hc {
        let k = take_bytes(&mut b)?;
        let v = take_bytes(&mut b)?;
        headers.push((String::from_utf8(k).ok()?, v));
    }
    Some(AuditRecord {
        class,
        value,
        headers,
    })
}

fn take_u32(b: &mut &[u8]) -> Option<u32> {
    if b.len() < 4 {
        return None;
    }
    let n = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    *b = &b[4..];
    Some(n)
}

fn take_bytes(b: &mut &[u8]) -> Option<Vec<u8>> {
    let len = usize::try_from(take_u32(b)?).unwrap_or(0);
    if b.len() < len {
        return None;
    }
    let out = b[..len].to_vec();
    *b = &b[len..];
    Some(out)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::prelude::{ByteSize, ByteSizeExt as _, mebibytes};

    use super::*;
    use crate::{
        chain::{GENESIS_HEAD, chain_hash, to_hex},
        event::AuditEventClass,
        sink::{AuditRecord, HEADER_PREV_HASH, HEADER_SEQ},
    };

    /// Cap large enough that no test hits it by accident.
    const ROOMY_CAP: ByteSize = mebibytes(1);

    fn chained_record(seq: u64, prev: &[u8; 32], value: &[u8]) -> AuditRecord {
        let mut r = AuditRecord {
            class: AuditEventClass::ApplicationLifecycle,
            value: value.to_vec(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        r.push_chain_headers(seq, prev);
        r
    }

    #[test]
    fn append_then_read_round_trips_records_with_headers() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.is_empty());
        let r0 = chained_record(0, &GENESIS_HEAD, b"{\"i\":0}");
        let r1 = chained_record(1, &chain_hash(&GENESIS_HEAD, 0, b"{\"i\":0}"), b"{\"i\":1}");
        check!((s.append(&r0).unwrap(), s.append(&r1).unwrap()) == (true, true));
        let read = s.read_all().unwrap();
        check!((s.count().0, s.is_empty(), read) == (2, false, vec![r0.clone(), r1.clone()]));
    }

    #[test]
    fn overflow_is_rejected_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        // tiny cap: first record fits, second does not
        let r = chained_record(0, &GENESIS_HEAD, b"0123456789");
        let one = {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r).unwrap();
            s.size()
        };
        let dir2 = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir2.path(), one).unwrap(); // exactly one record fits
        check!(s.append(&r).unwrap()); // accepted
        check!(!s.append(&r).unwrap()); // rejected (would exceed the cap)
        check!((s.count().0, s.read_all().unwrap().len()) == (1, 1)); // not corrupted
    }

    #[test]
    fn rewrite_keeps_only_remainder_and_truncate_clears() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"a");
        let r1 = chained_record(1, &GENESIS_HEAD, b"b");
        let r2 = chained_record(2, &GENESIS_HEAD, b"c");
        s.append(&r0).unwrap();
        s.append(&r1).unwrap();
        s.append(&r2).unwrap();
        s.rewrite(&[r1.clone(), r2.clone()]).unwrap(); // drop r0 (replayed)
        check!(s.size() > ByteSize::ZERO); // `*=` mutant would leave bytes at 0
        check!((s.count().0, s.read_all().unwrap()) == (2, vec![r1.clone(), r2.clone()]));
        s.truncate().unwrap();
        check!((s.is_empty(), s.read_all().unwrap().is_empty()) == (true, true));
    }

    #[test]
    fn append_accepts_record_that_exactly_fills_to_max() {
        let probe = tempfile::tempdir().unwrap();
        let r = chained_record(0, &GENESIS_HEAD, b"payload");
        let one = {
            let mut s = Spool::open(probe.path(), ROOMY_CAP).unwrap();
            s.append(&r).unwrap();
            s.size()
        };
        // Cap at exactly two records: the 2nd append fills to max and MUST be
        // accepted (bytes + frame == max, not >). The `+ -> *` mutant computes
        // bytes * frame, which exceeds max and would wrongly reject.
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), one * 2.0).unwrap();
        check!(s.append(&r).unwrap());
        check!(s.append(&r).unwrap());
        check!(s.count() == 2);
    }

    #[test]
    fn size_reports_the_bytes_actually_on_disk() {
        // Anchors the `SpoolBytes` -> `ByteSize` accessor to the real file
        // length, so a scale slip at that seam cannot hide behind a matching
        // slip at the `open` seam.
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        s.append(&chained_record(0, &GENESIS_HEAD, b"payload"))
            .unwrap();
        let on_disk = std::fs::metadata(dir.path().join(SPOOL_FILE))
            .unwrap()
            .len();
        check!(s.size() == ByteSize::from_bytes(on_disk));
    }

    #[test]
    fn resume_point_is_from_last_chained_record_skipping_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let prev0 = GENESIS_HEAD;
        let r0 = chained_record(0, &prev0, b"v0");
        let head0 = chain_hash(&prev0, 0, b"v0");
        let r1 = chained_record(1, &head0, b"v1");
        let head1 = chain_hash(&head0, 1, b"v1");
        // a checkpoint record (no seq/prev_hash) must be skipped
        let cp = AuditRecord {
            class: AuditEventClass::Checkpoint,
            value: b"{\"type\":\"checkpoint\"}".to_vec(),
            headers: vec![("event_class".into(), b"checkpoint".to_vec())],
        };
        s.append(&r0).unwrap();
        s.append(&r1).unwrap();
        s.append(&cp).unwrap();
        let (next_seq, head) = s.resume_point().unwrap().unwrap();
        // after seq 1; the hex projection also checks r1's chain math.
        check!((next_seq, head, to_hex(&head)) == (2, head1, to_hex(&head1)));
        let _ = (HEADER_SEQ, HEADER_PREV_HASH); // used by impl
    }

    #[test]
    fn open_heals_torn_tail_frame() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"good");
        {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r0).unwrap();
        }
        // Simulate a crash mid-append: a length prefix claiming 100 bytes, only 3 follow.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join("audit.spool"))
                .unwrap();
            f.write_all(&100u32.to_be_bytes()).unwrap();
            f.write_all(b"abc").unwrap();
        }
        // Reopen heals the torn tail; the good record survives and appends continue contiguously.
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        assert2::check!((s.count().0, s.read_all().unwrap()) == (1, vec![r0.clone()]));
        let r1 = chained_record(1, &GENESIS_HEAD, b"more");
        assert2::check!(s.append(&r1).unwrap());
        assert2::check!(s.read_all().unwrap() == vec![r0, r1]);
    }

    #[test]
    fn reopen_recovers_existing_contents() {
        let dir = tempfile::tempdir().unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"x");
        {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r0).unwrap();
        }
        let s2 = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!((s2.count().0, s2.read_all().unwrap()) == (1, vec![r0]));
    }
}

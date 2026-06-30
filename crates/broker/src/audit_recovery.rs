//! Recover the audit hash-chain position from this broker's audit partition.

use crabka_audit::chain::{chain_hash, from_hex32};
use crabka_audit::{EVENT_CLASS_CHECKPOINT, HEADER_PREV_HASH, HEADER_SEQ};

use crate::partition::Partition;

/// Read the tail of `partition` and return `(next_seq, head)` implied by the
/// last chained (non-checkpoint) record, or `None` if there are none.
#[must_use]
pub(crate) fn recover_from_partition_tail(partition: &Partition) -> Option<(u64, [u8; 32])> {
    let leo = partition.log_end_offset();
    if leo <= 0 {
        return None;
    }
    // Read a bounded tail window (audit records are small).  4096 offsets
    // comfortably exceeds the worst-case run of consecutive checkpoints
    // between chained records; the 1 MiB byte cap keeps the read cheap.
    let start = tail_window_start(leo);
    let out = partition.read_log(start, 1 << 20).ok()?;
    let mut last: Option<(u64, [u8; 32])> = None;
    for batch in &out.batches {
        for rec in &batch.records {
            // Skip checkpoint records (they don't advance the chained seq).
            if header_bytes(rec, "event_class").as_deref()
                == Some(EVENT_CLASS_CHECKPOINT.as_bytes())
            {
                continue;
            }
            let seq = header_str(rec, HEADER_SEQ).and_then(|s| s.parse::<u64>().ok());
            let prev = header_str(rec, HEADER_PREV_HASH).and_then(|s| from_hex32(&s));
            let value: &[u8] = rec
                .value
                .as_ref()
                .map(std::convert::AsRef::as_ref)
                .unwrap_or_default();
            if let (Some(seq), Some(prev)) = (seq, prev) {
                last = Some((seq + 1, chain_hash(&prev, seq, value)));
            }
        }
    }
    last
}

fn header_bytes(rec: &crabka_protocol::records::Record, key: &str) -> Option<Vec<u8>> {
    rec.headers
        .iter()
        .find(|h| h.key == key)
        .and_then(|h| h.value.as_ref().map(|b| b.to_vec()))
}

fn header_str(rec: &crabka_protocol::records::Record, key: &str) -> Option<String> {
    header_bytes(rec, key).and_then(|v| std::str::from_utf8(&v).ok().map(str::to_owned))
}

fn tail_window_start(log_end_offset: i64) -> i64 {
    (log_end_offset - 4096).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::Bytes;
    use crabka_audit::chain::{GENESIS_HEAD, to_hex};
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::{Record, RecordBatch, RecordHeader};
    use std::sync::atomic::{AtomicI32, AtomicU64};
    use std::sync::{Arc, Mutex};
    use tokio::sync::{Notify, mpsc};

    fn test_partition() -> (Partition, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel(1);
        let writer = tokio::spawn(async {});
        let p = Partition {
            topic: "__audit".into(),
            partition_id: 0,
            log_dir: Arc::new(arc_swap::ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify: Arc::new(Notify::new()),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        (p, dir)
    }

    fn header(key: &str, value: impl Into<Bytes>) -> RecordHeader {
        RecordHeader {
            key: key.to_string(),
            value: Some(value.into()),
        }
    }

    fn chained_record(seq: u64, prev: &[u8; 32], value: &'static [u8]) -> Record {
        Record {
            value: Some(Bytes::from_static(value)),
            headers: vec![
                header(HEADER_SEQ, seq.to_string()),
                header(HEADER_PREV_HASH, to_hex(prev)),
            ],
            ..Default::default()
        }
    }

    fn checkpoint_record() -> Record {
        Record {
            value: Some(Bytes::from_static(b"checkpoint")),
            headers: vec![header("event_class", EVENT_CLASS_CHECKPOINT)],
            ..Default::default()
        }
    }

    fn append_records(partition: &Partition, mut records: Vec<Record>) {
        for (i, rec) in records.iter_mut().enumerate() {
            rec.offset_delta = i32::try_from(i).expect("offset delta fits");
        }
        let mut batch = RecordBatch {
            last_offset_delta: i32::try_from(records.len() - 1).expect("last offset fits"),
            records,
            ..Default::default()
        };
        partition
            .log
            .lock()
            .expect("log lock")
            .append(&mut batch)
            .expect("append");
    }

    #[test]
    fn tail_window_start_keeps_only_last_4096_offsets() {
        assert!(tail_window_start(0) == 0);
        assert!(tail_window_start(4096) == 0);
        assert!(tail_window_start(4097) == 1);
        assert!(tail_window_start(8192) == 4096);
    }

    #[tokio::test]
    async fn recover_empty_partition_returns_none() {
        let (partition, _td) = test_partition();

        assert!(recover_from_partition_tail(&partition).is_none());
    }

    #[tokio::test]
    async fn recover_returns_next_sequence_and_chain_head_from_tail_record() {
        let (partition, _td) = test_partition();
        let seq = 0x0102_0304_0506_0708;
        let value = b"tail-value";
        append_records(&partition, vec![chained_record(seq, &GENESIS_HEAD, value)]);

        let recovered = recover_from_partition_tail(&partition).expect("tail record");

        assert!(recovered.0 == seq + 1);
        assert!(recovered.1 == chain_hash(&GENESIS_HEAD, seq, value));
    }

    #[tokio::test]
    async fn recover_skips_checkpoints_and_malformed_records() {
        let (partition, _td) = test_partition();
        let first = chained_record(0, &GENESIS_HEAD, b"first");
        let first_head = chain_hash(&GENESIS_HEAD, 0, b"first");
        let malformed = Record {
            value: Some(Bytes::from_static(b"bad")),
            headers: vec![header(HEADER_SEQ, "not-a-number")],
            ..Default::default()
        };
        let second = chained_record(1, &first_head, b"second");
        let second_head = chain_hash(&first_head, 1, b"second");
        append_records(
            &partition,
            vec![first, checkpoint_record(), malformed, second],
        );

        let recovered = recover_from_partition_tail(&partition).expect("last chained record");

        assert!(recovered == (2, second_head));
    }
}

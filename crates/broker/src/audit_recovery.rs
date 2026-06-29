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

    #[test]
    fn tail_window_start_keeps_only_last_4096_offsets() {
        assert!(tail_window_start(0) == 0);
        assert!(tail_window_start(4096) == 0);
        assert!(tail_window_start(4097) == 1);
        assert!(tail_window_start(8192) == 4096);
    }
}

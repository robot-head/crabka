//! Replay application with the engine's merge semantics.
//!
//! A strictly ordered single-writer journal still needs two non-LWW rules,
//! mirrored from the donor's replicated state machine. Counter keys max-merge,
//! because sessions fold counter ops at allocation time and journal order can
//! therefore carry non-monotone values. Clog keys are write-once, and the first
//! terminal decision wins, because an abort race can journal two decisions for
//! one xid.

use std::collections::{HashMap, HashSet};

use crabka_pgkv::{Kv, KvError, WriteOp, is_notify_op, key};
use crabka_pgmvcc::clog;

use crate::telemetry;

/// Apply one journaled batch to `kv` with max-merge counters and write-once
/// clog, and fold duplicates within the batch.
///
/// This function drops cross-node `NOTIFY` records. They travel on the WAL so
/// followers can observe them in flight, but a record that reached the KV would
/// become part of every later checkpoint. The drop is invisible to everything
/// else, because the caller accounts for offsets and journal sequences per
/// frame.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn apply_frame(kv: &dyn Kv, ops: &[WriteOp]) -> Result<(), KvError> {
    // A manual span rather than `#[instrument]`: the attribute macro only
    // accepts a literal `target`, which would fork the target name from
    // `telemetry::WAL_TARGET`.
    let span = telemetry::apply_span(ops.len());
    let _entered = span.enter();
    let mut counters: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut decided: HashSet<Vec<u8>> = HashSet::new();
    let mut adjusted = Vec::with_capacity(ops.len());

    for op in ops {
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
    let incoming = counter_value(key, value)?;
    let current = match counters.get(key) {
        Some(v) => *v,
        None => kv
            .get(key)?
            .as_deref()
            .map_or(Ok(0), |stored| counter_value(key, stored))?,
    };
    let merged = incoming.max(current);
    counters.insert(key.to_vec(), merged);
    adjusted.push(WriteOp::Put {
        key: key.to_vec(),
        value: merged.to_be_bytes().to_vec(),
    });
    Ok(())
}

fn counter_value(key: &[u8], value: &[u8]) -> Result<u64, KvError> {
    if key == crabka_gres_ranges::tso::MAX_TS_KEY {
        return strict_u64_be(value, "range-0 TSO horizon");
    }

    Ok(u64_be(value))
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

/// True for the `next_xid` counter, the range-0 TSO horizon, and any per-table sequence key.
fn is_counter_key(k: &[u8]) -> bool {
    k == key::next_xid_key().as_slice()
        || k == crabka_gres_ranges::tso::MAX_TS_KEY
        || k.starts_with(&key::seq_prefix())
}

/// True for any clog (`pg_xact`) status key.
fn is_clog_key(k: &[u8]) -> bool {
    k.starts_with(&key::clog_prefix())
}

/// Decode an 8-byte big-endian counter value; shorter/absent decodes as 0.
fn u64_be(bytes: &[u8]) -> u64 {
    let mut buf = [0_u8; 8];
    let n = bytes.len().min(8);
    buf[8 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    u64::from_be_bytes(buf)
}

fn strict_u64_be(bytes: &[u8], name: &str) -> Result<u64, KvError> {
    let value: [u8; 8] = bytes.try_into().map_err(|_| {
        KvError::CorruptRow(format!(
            "{name} must be exactly 8 bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(value))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv, WriteOp, key};
    use crabka_pgmvcc::clog;

    use super::*;

    fn u64_be_vec(v: u64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    #[test]
    fn counter_keys_max_merge_across_frames() {
        let kv = MemKv::default();

        apply_frame(
            &kv,
            &[WriteOp::Put {
                key: key::next_xid_key(),
                value: u64_be_vec(7),
            }],
        )
        .expect("apply");
        apply_frame(
            &kv,
            &[WriteOp::Put {
                key: key::next_xid_key(),
                value: u64_be_vec(6),
            }],
        )
        .expect("apply");

        assert!(kv.get(&key::next_xid_key()).expect("get") == Some(u64_be_vec(7)));
    }

    #[test]
    fn counter_keys_fold_within_one_frame() {
        let kv = MemKv::default();

        apply_frame(
            &kv,
            &[
                WriteOp::Put {
                    key: key::next_xid_key(),
                    value: u64_be_vec(9),
                },
                WriteOp::Put {
                    key: key::next_xid_key(),
                    value: u64_be_vec(8),
                },
            ],
        )
        .expect("apply");

        assert!(kv.get(&key::next_xid_key()).expect("get") == Some(u64_be_vec(9)));
    }

    #[test]
    fn seq_keys_max_merge() {
        let kv = MemKv::default();
        let seq_key = key::seq_key(7);

        apply_frame(
            &kv,
            &[
                WriteOp::Put {
                    key: seq_key.clone(),
                    value: u64_be_vec(3),
                },
                WriteOp::Put {
                    key: seq_key.clone(),
                    value: u64_be_vec(5),
                },
            ],
        )
        .expect("apply");

        assert!(kv.get(&seq_key).expect("get") == Some(u64_be_vec(5)));
    }

    #[test]
    fn range_zero_tso_horizon_max_merges() {
        let kv = MemKv::default();

        apply_frame(
            &kv,
            &[WriteOp::Put {
                key: crabka_gres_ranges::tso::MAX_TS_KEY.to_vec(),
                value: u64_be_vec(12),
            }],
        )
        .expect("apply");
        apply_frame(
            &kv,
            &[WriteOp::Put {
                key: crabka_gres_ranges::tso::MAX_TS_KEY.to_vec(),
                value: u64_be_vec(7),
            }],
        )
        .expect("apply");

        assert!(kv.get(crabka_gres_ranges::tso::MAX_TS_KEY).expect("get") == Some(u64_be_vec(12)));
    }

    #[test]
    fn range_zero_tso_horizon_rejects_malformed_values() {
        let kv = MemKv::default();

        let error = apply_frame(
            &kv,
            &[WriteOp::Put {
                key: crabka_gres_ranges::tso::MAX_TS_KEY.to_vec(),
                value: vec![1, 2, 3],
            }],
        )
        .expect_err("malformed horizon");

        assert!(matches!(error, KvError::CorruptRow(_)));
        assert!(error.to_string().contains("exactly 8 bytes"));
        assert!(
            kv.get(crabka_gres_ranges::tso::MAX_TS_KEY)
                .expect("get")
                .is_none()
        );
    }

    #[test]
    fn clog_first_terminal_decision_wins() {
        let kv = MemKv::default();

        apply_frame(&kv, &[clog::put_op(11, clog::XidStatus::Aborted)]).expect("apply");
        apply_frame(&kv, &[clog::put_op(11, clog::XidStatus::Committed)]).expect("apply");
        let stored = kv.get(&key::clog_key(11)).expect("get").expect("present");

        assert!(clog::is_terminal(&stored));
        assert!(stored == terminal_bytes(clog::XidStatus::Aborted));
    }

    #[test]
    fn plain_keys_are_last_writer_wins() {
        let kv = MemKv::default();

        apply_frame(
            &kv,
            &[WriteOp::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            }],
        )
        .expect("apply");
        apply_frame(
            &kv,
            &[WriteOp::Put {
                key: b"a".to_vec(),
                value: b"2".to_vec(),
            }],
        )
        .expect("apply");
        apply_frame(&kv, &[WriteOp::Delete { key: b"a".to_vec() }]).expect("apply");

        assert!(kv.get(b"a").expect("get").is_none());
    }

    /// Notify records must never reach the store, because a checkpoint would
    /// make them permanent. Every other op in the same batch applies as usual.
    #[test]
    fn notify_ops_are_dropped_without_disturbing_the_rest_of_the_batch() {
        let kv = MemKv::default();
        let row = key::row_key(7, 1);
        let record = crabka_pgkv::NotifyRecord {
            origin: "node-a".into(),
            process_id: 99,
            channel: "c".into(),
            payload: "p".into(),
        };

        apply_frame(
            &kv,
            &[
                WriteOp::Put {
                    key: row.clone(),
                    value: b"row".to_vec(),
                },
                WriteOp::Put {
                    key: key::next_xid_key(),
                    value: u64_be_vec(4),
                },
                WriteOp::Put {
                    key: key::notify_key(1),
                    value: record.encode(),
                },
                clog::put_op(11, clog::XidStatus::Committed),
                WriteOp::Put {
                    key: key::notify_key(2),
                    value: record.encode(),
                },
                WriteOp::Put {
                    key: key::next_xid_key(),
                    value: u64_be_vec(3),
                },
            ],
        )
        .expect("apply");

        assert!(kv.get(&row).expect("get") == Some(b"row".to_vec()));
        assert!(kv.get(&key::next_xid_key()).expect("get") == Some(u64_be_vec(4)));
        assert!(
            kv.get(&key::clog_key(11)).expect("get")
                == Some(terminal_bytes(clog::XidStatus::Committed))
        );
        assert!(kv.get(&key::notify_key(1)).expect("get").is_none());
        assert!(kv.get(&key::notify_key(2)).expect("get").is_none());
        assert!(
            kv.scan_prefix(&key::notify_prefix())
                .expect("scan")
                .is_empty()
        );
    }

    /// The filter covers every write shape, so a stray delete or conditional
    /// put in the notify namespace cannot restore it either.
    #[test]
    fn notify_ops_are_dropped_in_every_write_shape() {
        let kv = MemKv::default();

        apply_frame(
            &kv,
            &[
                WriteOp::Delete {
                    key: key::notify_key(1),
                },
                WriteOp::ConditionalPut {
                    key: key::notify_key(2),
                    expected: None,
                    value: b"x".to_vec(),
                },
                WriteOp::Put {
                    key: key::notify_prefix(),
                    value: b"x".to_vec(),
                },
            ],
        )
        .expect("apply");

        assert!(
            kv.scan_prefix(&key::notify_prefix())
                .expect("scan")
                .is_empty()
        );
    }

    fn terminal_bytes(status: clog::XidStatus) -> Vec<u8> {
        match clog::put_op(0, status) {
            WriteOp::Put { value, .. } => value,
            WriteOp::ConditionalPut { .. } | WriteOp::Delete { .. } => {
                unreachable!("put_op only emits Put")
            }
        }
    }
}

//! Replay application with the engine's merge semantics.
//!
//! A strictly ordered single-writer journal still needs two non-LWW rules
//! (mirrored from the donor's replicated state machine): counter keys
//! max-merge because sessions fold counter ops at allocation time, so journal
//! order can carry non-monotone values; clog keys are write-once with the
//! first terminal decision winning, because an abort race can journal two
//! decisions for one xid.

use std::collections::{HashMap, HashSet};

use crabka_pgkv::{Kv, KvError, WriteOp, key};
use crabka_pgmvcc::clog;

/// Apply one journaled batch to `kv` with max-merge counters and write-once
/// clog, folding duplicates within the batch.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn apply_frame(kv: &dyn Kv, ops: &[WriteOp]) -> Result<(), KvError> {
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

    fn terminal_bytes(status: clog::XidStatus) -> Vec<u8> {
        match clog::put_op(0, status) {
            WriteOp::Put { value, .. } => value,
            WriteOp::ConditionalPut { .. } | WriteOp::Delete { .. } => {
                unreachable!("put_op only emits Put")
            }
        }
    }
}

//! Pure recovery replay over committed `GRW1` frame bytes.

use crabka_pgkv::{Kv, WriteOp};

use crate::{
    apply::apply_frame,
    checkpoint::CheckpointFilter,
    error::SubstrateError,
    frame::{BARRIER_SEQ, WalFrame},
    transfer::TableTransferSelector,
};

/// A committed WAL record fetched from the tenant topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayItem {
    /// Kafka offset carrying `bytes`.
    pub offset: i64,
    /// Encoded [`WalFrame`] bytes.
    pub bytes: Vec<u8>,
}

/// Result of replaying committed WAL frames into a local read model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// Next non-barrier journal sequence a successor writer must use.
    pub next_journal_seq: u64,
}

/// Decode and apply committed frames, stopping at this generation's barrier.
///
/// Barriers never consume journal sequence numbers. Foreign barriers before
/// `own_barrier_offset` are skipped; the barrier at or after `own_barrier_offset`
/// terminates replay.
pub fn replay_committed_frames(
    kv: &dyn Kv,
    frames: impl IntoIterator<Item = ReplayItem>,
    own_barrier_offset: i64,
) -> Result<ReplayOutcome, SubstrateError> {
    replay_committed_frames_from(kv, frames, own_barrier_offset, 0, 0)
}

/// Decode and apply committed frames starting at `replay_start_offset` with an
/// already-restored expected journal sequence.
pub fn replay_committed_frames_from(
    kv: &dyn Kv,
    frames: impl IntoIterator<Item = ReplayItem>,
    own_barrier_offset: i64,
    replay_start_offset: i64,
    first_expected_journal_seq: u64,
) -> Result<ReplayOutcome, SubstrateError> {
    replay_committed_frames_from_with_filter(
        kv,
        frames,
        own_barrier_offset,
        replay_start_offset,
        first_expected_journal_seq,
        None,
    )
}

/// Decode committed frames while applying only mutations inside `filter`.
pub fn replay_committed_frames_from_filtered(
    kv: &dyn Kv,
    frames: impl IntoIterator<Item = ReplayItem>,
    own_barrier_offset: i64,
    replay_start_offset: i64,
    first_expected_journal_seq: u64,
    filter: CheckpointFilter,
) -> Result<ReplayOutcome, SubstrateError> {
    replay_committed_frames_from_with_filter(
        kv,
        frames,
        own_barrier_offset,
        replay_start_offset,
        first_expected_journal_seq,
        Some(filter),
    )
}

/// Decode committed frames while applying a stateful table-transfer closure.
///
/// Unlike a range filter, selecting an MVCC tuple changes the selector state:
/// CLOG operations later in the WAL for its xmin/xmax become required input.
pub fn replay_committed_frames_from_table_transfer(
    kv: &dyn Kv,
    frames: impl IntoIterator<Item = ReplayItem>,
    own_barrier_offset: i64,
    replay_start_offset: i64,
    first_expected_journal_seq: u64,
    selector: &mut TableTransferSelector,
) -> Result<ReplayOutcome, SubstrateError> {
    let mut expected = first_expected_journal_seq;
    for item in frames {
        if item.offset < replay_start_offset {
            continue;
        }
        let frame = WalFrame::decode(&item.bytes)?;
        if frame.journal_seq == BARRIER_SEQ {
            if item.offset >= own_barrier_offset {
                return Ok(ReplayOutcome {
                    next_journal_seq: expected,
                });
            }
            continue;
        }
        if frame.journal_seq != expected {
            return Err(SubstrateError::SequenceGap {
                expected,
                found: frame.journal_seq,
                offset: item.offset,
            });
        }
        let selected = frame
            .ops
            .iter()
            .try_fold(Vec::new(), |mut ops, operation| {
                if let Some(operation) = selector.select_tail_op(operation)? {
                    ops.push(operation);
                }
                Ok::<_, SubstrateError>(ops)
            })?;
        apply_frame(kv, &selected)?;
        expected = expected.checked_add(1).ok_or_else(|| {
            SubstrateError::Frame("journal sequence exhausted before barrier".into())
        })?;
    }
    Err(SubstrateError::Unavailable(
        "replay reached end before recovery barrier".into(),
    ))
}

fn replay_committed_frames_from_with_filter(
    kv: &dyn Kv,
    frames: impl IntoIterator<Item = ReplayItem>,
    own_barrier_offset: i64,
    replay_start_offset: i64,
    first_expected_journal_seq: u64,
    filter: Option<CheckpointFilter>,
) -> Result<ReplayOutcome, SubstrateError> {
    let mut expected = first_expected_journal_seq;

    for item in frames {
        if item.offset < replay_start_offset {
            continue;
        }
        let frame = WalFrame::decode(&item.bytes)?;
        if frame.journal_seq == BARRIER_SEQ {
            if item.offset >= own_barrier_offset {
                return Ok(ReplayOutcome {
                    next_journal_seq: expected,
                });
            }
            continue;
        }

        if frame.journal_seq != expected {
            return Err(SubstrateError::SequenceGap {
                expected,
                found: frame.journal_seq,
                offset: item.offset,
            });
        }

        let filtered_ops;
        let ops = match filter {
            Some(ref filter) => {
                filtered_ops = filter_write_ops(&frame.ops, filter)?;
                filtered_ops.as_slice()
            }
            None => frame.ops.as_slice(),
        };
        apply_frame(kv, ops)?;
        expected = expected.checked_add(1).ok_or_else(|| {
            SubstrateError::Frame("journal sequence exhausted before barrier".into())
        })?;
    }

    Err(SubstrateError::Unavailable(
        "replay reached end before recovery barrier".into(),
    ))
}

fn filter_write_ops(
    ops: &[WriteOp],
    filter: &CheckpointFilter,
) -> Result<Vec<WriteOp>, SubstrateError> {
    ops.iter()
        .filter_map(|op| match op {
            WriteOp::Put { key, .. }
            | WriteOp::ConditionalPut { key, .. }
            | WriteOp::Delete { key } => match filter.contains_key(key) {
                Ok(true) => Some(Ok(op.clone())),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_gres_ranges::{RangeKey, TableId};
    use crabka_pgkv::{Kv, MemKv, WriteOp};

    use super::*;

    #[test]
    fn replay_applies_committed_frames_until_own_barrier() {
        let kv = MemKv::default();
        let frames = vec![
            item(
                0,
                &WalFrame {
                    journal_seq: 0,
                    ops: vec![WriteOp::Put {
                        key: b"a".to_vec(),
                        value: b"1".to_vec(),
                    }],
                },
            ),
            item(
                1,
                &WalFrame {
                    journal_seq: BARRIER_SEQ,
                    ops: Vec::new(),
                },
            ),
        ];

        let outcome = replay_committed_frames(&kv, frames, 1).expect("replay");

        assert!(outcome.next_journal_seq == 1);
        assert!(kv.get(b"a").expect("get") == Some(b"1".to_vec()));
    }

    #[test]
    fn replay_skips_foreign_barriers() {
        let kv = MemKv::default();
        let frames = vec![
            item(
                0,
                &WalFrame {
                    journal_seq: BARRIER_SEQ,
                    ops: Vec::new(),
                },
            ),
            item(
                1,
                &WalFrame {
                    journal_seq: 0,
                    ops: vec![WriteOp::Put {
                        key: b"survives".to_vec(),
                        value: b"yes".to_vec(),
                    }],
                },
            ),
            item(
                2,
                &WalFrame {
                    journal_seq: BARRIER_SEQ,
                    ops: Vec::new(),
                },
            ),
        ];

        let outcome = replay_committed_frames(&kv, frames, 2).expect("replay");

        assert!(outcome.next_journal_seq == 1);
        assert!(kv.get(b"survives").expect("get") == Some(b"yes".to_vec()));
    }

    #[test]
    fn replay_rejects_sequence_gaps() {
        let kv = MemKv::default();
        let frames = vec![item(
            9,
            &WalFrame {
                journal_seq: 1,
                ops: Vec::new(),
            },
        )];

        let error = replay_committed_frames(&kv, frames, 10).expect_err("gap");

        assert!(matches!(
            error,
            SubstrateError::SequenceGap {
                expected: 0,
                found: 1,
                offset: 9
            }
        ));
    }

    #[test]
    fn seeded_replay_decouples_generation_offsets_from_journal_sequence() {
        let frames = |journal_seq| {
            vec![
                item(
                    0,
                    &WalFrame {
                        journal_seq,
                        ops: vec![WriteOp::Put {
                            key: b"continued".to_vec(),
                            value: b"yes".to_vec(),
                        }],
                    },
                ),
                item(
                    1,
                    &WalFrame {
                        journal_seq: BARRIER_SEQ,
                        ops: Vec::new(),
                    },
                ),
            ]
        };
        let kv = MemKv::default();
        let outcome = replay_committed_frames_from(&kv, frames(42), 1, 0, 42)
            .expect("generation offset zero continues journal sequence 42");
        assert_eq!(outcome.next_journal_seq, 43);
        assert_eq!(kv.get(b"continued").expect("get"), Some(b"yes".to_vec()));

        for found in [41, 43] {
            let error = replay_committed_frames_from(&MemKv::default(), frames(found), 1, 0, 42)
                .expect_err("non-exact replay seed must fail");
            assert!(matches!(
                error,
                SubstrateError::SequenceGap {
                    expected: 42,
                    found: actual,
                    offset: 0,
                } if actual == found
            ));
        }
    }

    #[test]
    fn replay_reports_missing_own_barrier() {
        let kv = MemKv::default();

        let error = replay_committed_frames(&kv, Vec::new(), 0).expect_err("missing barrier");

        assert!(matches!(error, SubstrateError::Unavailable(_)));
    }

    #[test]
    fn filtered_replay_skips_predecessor_owned_keys() {
        let kv = MemKv::default();
        let predecessor_key = crabka_pgkv::key::row_key(7, 10);
        let successor_key = crabka_pgkv::key::row_key(7, 20);
        let frames = vec![
            item(
                0,
                &WalFrame {
                    journal_seq: 0,
                    ops: vec![
                        WriteOp::Put {
                            key: predecessor_key.clone(),
                            value: b"predecessor".to_vec(),
                        },
                        WriteOp::Put {
                            key: successor_key.clone(),
                            value: b"successor".to_vec(),
                        },
                    ],
                },
            ),
            item(
                1,
                &WalFrame {
                    journal_seq: BARRIER_SEQ,
                    ops: Vec::new(),
                },
            ),
        ];
        let filter = CheckpointFilter::new(
            RangeKey::new(TableId::new(7), 20),
            Some(RangeKey::new(TableId::new(7), 30)),
        )
        .expect("filter")
        .with_physical_to_logical(std::collections::BTreeMap::from([(
            TableId::new(7),
            TableId::new(7),
        )]));

        let outcome = replay_committed_frames_from_filtered(&kv, frames, 1, 0, 0, filter)
            .expect("filtered replay");

        assert!(outcome.next_journal_seq == 1);
        assert!(kv.get(&predecessor_key).expect("get predecessor").is_none());
        assert!(kv.get(&successor_key).expect("get successor") == Some(b"successor".to_vec()));
    }

    fn item(offset: i64, frame: &WalFrame) -> ReplayItem {
        ReplayItem {
            offset,
            bytes: frame.encode(),
        }
    }
}

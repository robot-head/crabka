//! Pure recovery replay over committed `GRW1` frame bytes.

use std::borrow::Cow;

use crabka_pgkv::{Kv, WriteOp, is_notify_op};

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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn replay_committed_frames(
    kv: &dyn Kv,
    frames: impl IntoIterator<Item = ReplayItem>,
    own_barrier_offset: i64,
) -> Result<ReplayOutcome, SubstrateError> {
    replay_committed_frames_from(kv, frames, own_barrier_offset, 0, 0)
}

/// Decode and apply committed frames starting at `replay_start_offset` with an
/// already-restored expected journal sequence.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn replay_committed_frames_from_filtered(
    kv: &dyn Kv,
    frames: impl IntoIterator<Item = ReplayItem>,
    own_barrier_offset: i64,
    replay_start_offset: i64,
    first_expected_journal_seq: u64,
    filter: &CheckpointFilter,
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
        let replayable = without_notify_ops(&frame.ops);
        let selected = replayable
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
    filter: Option<&CheckpointFilter>,
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

        let replayable = without_notify_ops(&frame.ops);
        let filtered_ops;
        let ops = match filter {
            Some(filter) => {
                filtered_ops = filter_write_ops(&replayable, filter)?;
                filtered_ops.as_slice()
            }
            None => replayable.as_ref(),
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

/// Drop the WAL-only cross-node `NOTIFY` records from one frame.
///
/// Recovery rebuilds durable state, and notify records are not durable state:
/// they are in-flight fan-out that must never reach the KV (a checkpoint would
/// make them permanent) and that no listener exists to receive during recovery.
/// Dropping them here also keeps them away from the range and table-transfer
/// selectors, which have no notion of the namespace. Frames without notify
/// records — every frame outside a `NOTIFY` — are borrowed, not copied.
fn without_notify_ops(ops: &[WriteOp]) -> Cow<'_, [WriteOp]> {
    if ops.iter().any(is_notify_op) {
        Cow::Owned(
            ops.iter()
                .filter(|op| !is_notify_op(op))
                .cloned()
                .collect::<Vec<_>>(),
        )
    } else {
        Cow::Borrowed(ops)
    }
}

fn filter_write_ops(
    ops: &[WriteOp],
    filter: &CheckpointFilter,
) -> Result<Vec<WriteOp>, SubstrateError> {
    ops.iter()
        .filter_map(|op| match op {
            WriteOp::Put { key, value } => match filter.filter_pair(key, value) {
                Ok(Some(value)) => Some(Ok(WriteOp::Put {
                    key: key.clone(),
                    value,
                })),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            },
            WriteOp::ConditionalPut {
                key,
                expected,
                value,
            } => match filter.filter_pair(key, value).and_then(|value| {
                let expected = expected
                    .as_deref()
                    .map(|expected| filter.filter_pair(key, expected))
                    .transpose()?
                    .flatten();
                match (value, expected) {
                    (None, None) => Ok(None),
                    (Some(value), expected) => Ok(Some(WriteOp::ConditionalPut {
                        key: key.clone(),
                        expected,
                        value,
                    })),
                    (None, Some(_)) => Err(SubstrateError::Checkpoint(
                        "timestamp descriptor transition drops successor ownership".into(),
                    )),
                }
            }) {
                Ok(Some(op)) => Some(Ok(op)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            },
            WriteOp::Delete { key } => match filter.contains_pair(key, None) {
                Ok(true) => Some(Ok(op.clone())),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use crabka_gres_ranges::{RangeKey, TableId};
    use crabka_pgkv::{Kv, MemKv, WriteOp};

    use super::*;

    #[test]
    fn filtered_tail_rehomes_timestamp_descriptor_and_intent() {
        let filter = CheckpointFilter::new(RangeKey::new(TableId::new(51), 16), None)
            .unwrap()
            .with_physical_to_logical(BTreeMap::from([(TableId::new(1), TableId::new(52))]))
            .with_target_range(crabka_gres_ranges::RangeId::new(3));
        let start_ts = crabka_pgexec::TimestampTransactionId::new(9).unwrap();
        let mut descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(start_ts, 10, vec![1]);
        descriptor
            .acknowledge_operations(
                1,
                &[crabka_pgexec::TimestampTxnOperation {
                    range_id: 1,
                    table_id: 1,
                    bucket: None,
                    rowid: 1,
                    delete: false,
                }],
            )
            .unwrap();
        let mut intent_key = b"\0\0\0\0meta/ts_intent/".to_vec();
        intent_key.extend_from_slice(&1_u32.to_be_bytes());
        intent_key.extend_from_slice(&1_u64.to_be_bytes());
        intent_key.extend_from_slice(&9_u64.to_be_bytes());
        let mut identity = vec![0; 24];
        identity[16..20].copy_from_slice(&1_u32.to_be_bytes());
        identity[20..24].copy_from_slice(&1_u32.to_be_bytes());
        let ops = filter_write_ops(
            &[
                crabka_pgexec::timestamp_txn_descriptor_op(&descriptor),
                WriteOp::Put {
                    key: intent_key,
                    value: identity,
                },
            ],
            &filter,
        )
        .unwrap();
        let WriteOp::Put { value, .. } = &ops[0] else {
            panic!("descriptor put")
        };
        let rewritten =
            crabka_pgexec::decode_timestamp_txn_descriptor_value(start_ts, value).unwrap();
        assert_eq!(rewritten.participants, vec![3]);
        assert_eq!(rewritten.prepared, vec![3]);
        assert_eq!(rewritten.operations[0].range_id, 3);
        let WriteOp::Put { value, .. } = &ops[1] else {
            panic!("intent put")
        };
        assert_eq!(&value[16..20], &3_u32.to_be_bytes());
        assert_eq!(&value[20..24], &3_u32.to_be_bytes());
    }

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

        let outcome = replay_committed_frames_from_filtered(&kv, frames, 1, 0, 0, &filter)
            .expect("filtered replay");

        assert!(outcome.next_journal_seq == 1);
        assert!(kv.get(&predecessor_key).expect("get predecessor").is_none());
        assert!(kv.get(&successor_key).expect("get successor") == Some(b"successor".to_vec()));
    }

    fn notify_record() -> Vec<u8> {
        crabka_pgkv::NotifyRecord {
            origin: "node-a".into(),
            process_id: 7,
            channel: "c".into(),
            payload: "p".into(),
        }
        .encode()
    }

    /// A mixed frame replayed during recovery must rebuild everything except
    /// the notify records, which are in-flight fan-out rather than state.
    #[test]
    fn replay_never_persists_notify_records() {
        let kv = MemKv::default();
        let row = crabka_pgkv::key::row_key(7, 1);
        let frames = vec![
            item(
                0,
                &WalFrame {
                    journal_seq: 0,
                    ops: vec![
                        WriteOp::Put {
                            key: row.clone(),
                            value: b"row".to_vec(),
                        },
                        WriteOp::Put {
                            key: crabka_pgkv::key::notify_key(1),
                            value: notify_record(),
                        },
                        WriteOp::Put {
                            key: crabka_pgkv::key::next_xid_key(),
                            value: 9_u64.to_be_bytes().to_vec(),
                        },
                        WriteOp::Put {
                            key: crabka_pgkv::key::notify_key(2),
                            value: notify_record(),
                        },
                        crabka_pgmvcc::clog::put_op(11, crabka_pgmvcc::clog::XidStatus::Committed),
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

        let outcome = replay_committed_frames(&kv, frames, 1).expect("replay");

        assert!(outcome.next_journal_seq == 1);
        assert!(kv.get(&row).expect("get") == Some(b"row".to_vec()));
        assert!(
            kv.get(&crabka_pgkv::key::next_xid_key()).expect("get")
                == Some(9_u64.to_be_bytes().to_vec())
        );
        assert!(
            kv.get(&crabka_pgkv::key::clog_key(11))
                .expect("get")
                .is_some()
        );
        assert!(
            kv.scan_prefix(&crabka_pgkv::key::notify_prefix())
                .expect("scan")
                .is_empty()
        );
    }

    /// The range-filtered and table-transfer recovery paths drop notify records
    /// before their selectors ever see a key from a namespace they do not know.
    #[test]
    fn filtered_and_table_transfer_replay_never_persist_notify_records() {
        let successor_key = crabka_pgkv::key::row_key(7, 20);
        let frames = || {
            vec![
                item(
                    0,
                    &WalFrame {
                        journal_seq: 0,
                        ops: vec![
                            WriteOp::Put {
                                key: crabka_pgkv::key::notify_key(1),
                                value: notify_record(),
                            },
                            WriteOp::Put {
                                key: successor_key.clone(),
                                value: b"successor".to_vec(),
                            },
                            WriteOp::Delete {
                                key: crabka_pgkv::key::notify_key(2),
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
            ]
        };
        let filter = CheckpointFilter::new(
            RangeKey::new(TableId::new(7), 20),
            Some(RangeKey::new(TableId::new(7), 30)),
        )
        .expect("filter")
        .with_physical_to_logical(BTreeMap::from([(TableId::new(7), TableId::new(7))]));

        let filtered_kv = MemKv::default();
        let outcome =
            replay_committed_frames_from_filtered(&filtered_kv, frames(), 1, 0, 0, &filter)
                .expect("filtered replay");
        assert!(outcome.next_journal_seq == 1);
        assert!(filtered_kv.get(&successor_key).expect("get") == Some(b"successor".to_vec()));
        assert!(
            filtered_kv
                .scan_prefix(&crabka_pgkv::key::notify_prefix())
                .expect("scan")
                .is_empty()
        );

        let transfer_kv = MemKv::default();
        let mut selector = TableTransferSelector::new(7).expect("selector");
        let transfer_frames = vec![
            item(
                0,
                &WalFrame {
                    journal_seq: 0,
                    ops: vec![
                        WriteOp::Put {
                            key: crabka_pgkv::key::notify_key(1),
                            value: notify_record(),
                        },
                        WriteOp::Put {
                            key: crabka_pgkv::key::seq_key(7),
                            value: 5_u64.to_be_bytes().to_vec(),
                        },
                        WriteOp::Delete {
                            key: crabka_pgkv::key::notify_key(2),
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
        let outcome = replay_committed_frames_from_table_transfer(
            &transfer_kv,
            transfer_frames,
            1,
            0,
            0,
            &mut selector,
        )
        .expect("table transfer replay");
        assert!(outcome.next_journal_seq == 1);
        assert!(
            transfer_kv.get(&crabka_pgkv::key::seq_key(7)).expect("get")
                == Some(5_u64.to_be_bytes().to_vec())
        );
        assert!(
            transfer_kv
                .scan_prefix(&crabka_pgkv::key::notify_prefix())
                .expect("scan")
                .is_empty()
        );
    }

    fn item(offset: i64, frame: &WalFrame) -> ReplayItem {
        ReplayItem {
            offset,
            bytes: frame.encode(),
        }
    }
}

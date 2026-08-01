//! Durable selection of one ordinary table's local transfer closure.

use std::collections::{BTreeMap, BTreeSet};

use crabka_pgkv::{
    KvPair, WriteOp,
    key::{self, KeyClass},
};
use crabka_pgmvcc::{
    FROZEN_XID, INVALID_XID,
    clog::{self, XidStatus},
    version,
};
use sha2::{Digest, Sha256};

use crate::error::SubstrateError;

/// Reproducible identity of the sole table selected for transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableTransferIdentity {
    /// The local table id.
    pub table_id: u32,
}

/// Counts and digest of materialized transfer input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTransferStats {
    /// Selected KV pairs.
    pub pairs: u64,
    /// Selected MVCC tuple versions.
    pub tuple_versions: u64,
    /// Selected CLOG entries.
    pub clog_entries: u64,
    /// Selected sequence entries.
    pub sequence_entries: u64,
    /// SHA-256 of ordered `[key length][key][value length][value]` pairs.
    pub checksum: String,
}

/// Deterministic checkpoint selection output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTransferMaterialization {
    /// Transfer identity.
    pub identity: TableTransferIdentity,
    /// Key-ordered selected pairs.
    pub pairs: Vec<KvPair>,
    /// Selection counts and checksum.
    pub stats: TableTransferStats,
}

/// Stateful selector shared by checkpoint materialization and ordered WAL tail replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTransferSelector {
    identity: TableTransferIdentity,
    referenced_xids: BTreeSet<u64>,
}

impl TableTransferSelector {
    /// Select an ordinary, unsharded table.
    ///
    /// Table ID zero is reserved for system data and cannot be transferred as
    /// an ordinary table.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn new(table_id: u32) -> Result<Self, SubstrateError> {
        if table_id == 0 {
            return Err(unsupported("system table ID 0"));
        }
        Ok(Self {
            identity: TableTransferIdentity { table_id },
            referenced_xids: BTreeSet::new(),
        })
    }

    /// Return the immutable selection identity.
    #[must_use]
    pub const fn identity(&self) -> TableTransferIdentity {
        self.identity
    }

    /// Materialize the table's primary versions, rowid sequence, and referenced
    /// CLOG closure from one consistent checkpoint snapshot.
    ///
    /// The resulting selector retains the XID closure for subsequent in-order
    /// tail replay.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn materialize_checkpoint(
        &mut self,
        pairs: impl IntoIterator<Item = KvPair>,
    ) -> Result<TableTransferMaterialization, SubstrateError> {
        let mut staged = self.clone();
        let materialization = staged.materialize_checkpoint_inner(pairs)?;
        *self = staged;
        Ok(materialization)
    }

    fn materialize_checkpoint_inner(
        &mut self,
        pairs: impl IntoIterator<Item = KvPair>,
    ) -> Result<TableTransferMaterialization, SubstrateError> {
        let mut tuples = Vec::new();
        let mut sequence = Vec::new();
        let mut clog_pairs = BTreeMap::new();

        for (key_bytes, value) in pairs {
            match key::classify_key(&key_bytes) {
                KeyClass::PrimaryVersion { table_id, .. } if table_id == self.identity.table_id => {
                    self.record_tuple_dependencies(&value)?;
                    tuples.push((key_bytes, value));
                }
                KeyClass::HashPrimaryRow { table_id, .. }
                | KeyClass::HashPrimaryVersion { table_id, .. }
                    if table_id == self.identity.table_id =>
                {
                    return Err(unsupported("hash-sharded primary key"));
                }
                KeyClass::SecondaryIndex { table_id, .. } if table_id == self.identity.table_id => {
                    return Err(unsupported("local secondary index key"));
                }
                KeyClass::PrimaryRow { table_id, .. } if table_id == self.identity.table_id => {
                    return Err(unsupported("non-MVCC primary row key"));
                }
                KeyClass::Sequence { table_id } if table_id == self.identity.table_id => {
                    sequence.push((key_bytes, value));
                }
                KeyClass::Clog { xid } => {
                    clog_pairs.insert(xid, (key_bytes, value));
                }
                _ => {}
            }
        }

        let mut selected =
            Vec::with_capacity(tuples.len() + sequence.len() + self.referenced_xids.len());
        selected.extend(tuples);
        selected.extend(sequence);
        for xid in &self.referenced_xids {
            let pair = clog_pairs.get(xid).ok_or_else(|| {
                SubstrateError::Checkpoint(format!(
                    "table transfer missing CLOG entry for xid {xid}"
                ))
            })?;
            reject_in_doubt_clog(*xid, &pair.1)?;
            selected.push(pair.clone());
        }
        selected.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(materialization(self.identity, selected))
    }

    /// Select one WAL operation in frame order.
    ///
    /// A newly retained tuple adds its XID dependencies before later operations,
    /// so later CLOG outcomes for those XIDs are retained. Earlier CLOG writes
    /// are intentionally not retroactively selected; WAL replay is in-order.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn select_tail_op(
        &mut self,
        operation: &WriteOp,
    ) -> Result<Option<WriteOp>, SubstrateError> {
        let key_bytes = operation_key(operation);
        match key::classify_key(key_bytes) {
            KeyClass::PrimaryVersion { table_id, .. } if table_id == self.identity.table_id => {
                if let Some(value) = operation_value(operation) {
                    self.record_tuple_dependencies(value)?;
                }
                Ok(Some(operation.clone()))
            }
            KeyClass::HashPrimaryRow { table_id, .. }
            | KeyClass::HashPrimaryVersion { table_id, .. }
                if table_id == self.identity.table_id =>
            {
                Err(unsupported("hash-sharded primary key"))
            }
            KeyClass::SecondaryIndex { table_id, .. } if table_id == self.identity.table_id => {
                Err(unsupported("local secondary index key"))
            }
            KeyClass::PrimaryRow { table_id, .. } if table_id == self.identity.table_id => {
                Err(unsupported("non-MVCC primary row key"))
            }
            KeyClass::Sequence { table_id } if table_id == self.identity.table_id => {
                Ok(Some(operation.clone()))
            }
            KeyClass::Clog { xid } if self.referenced_xids.contains(&xid) => {
                let value = operation_value(operation)
                    .ok_or_else(|| unsupported(&format!("CLOG delete for xid {xid}")))?;
                reject_in_doubt_clog(xid, value)?;
                Ok(Some(operation.clone()))
            }
            _ => Ok(None),
        }
    }

    fn record_tuple_dependencies(&mut self, value: &[u8]) -> Result<(), SubstrateError> {
        let (xmin, xmax, _) = version::decode_tuple(value)?;
        for xid in [xmin, xmax] {
            if xid != INVALID_XID && xid != FROZEN_XID {
                self.referenced_xids.insert(xid);
            }
        }
        Ok(())
    }
}

fn operation_key(operation: &WriteOp) -> &[u8] {
    match operation {
        WriteOp::Put { key, .. }
        | WriteOp::ConditionalPut { key, .. }
        | WriteOp::Delete { key } => key,
    }
}

fn operation_value(operation: &WriteOp) -> Option<&[u8]> {
    match operation {
        WriteOp::Put { value, .. } | WriteOp::ConditionalPut { value, .. } => Some(value),
        WriteOp::Delete { .. } => None,
    }
}

fn reject_in_doubt_clog(xid: u64, value: &[u8]) -> Result<(), SubstrateError> {
    match clog::decode(value)? {
        XidStatus::Committed | XidStatus::Aborted => Ok(()),
        XidStatus::InProgress => Err(unsupported(&format!("in-doubt CLOG xid {xid}"))),
        XidStatus::Prepared(_) => Err(unsupported(&format!("prepared CLOG xid {xid}"))),
    }
}

fn unsupported(detail: &str) -> SubstrateError {
    SubstrateError::Checkpoint(format!("table transfer does not support {detail}"))
}

fn materialization(
    identity: TableTransferIdentity,
    pairs: Vec<KvPair>,
) -> TableTransferMaterialization {
    let mut digest = Sha256::new();
    let mut tuples = 0_u64;
    let mut clogs = 0_u64;
    let mut sequences = 0_u64;
    for (key_bytes, value) in &pairs {
        digest.update(
            u64::try_from(key_bytes.len())
                .expect("usize fits u64")
                .to_be_bytes(),
        );
        digest.update(key_bytes);
        digest.update(
            u64::try_from(value.len())
                .expect("usize fits u64")
                .to_be_bytes(),
        );
        digest.update(value);
        match key::classify_key(key_bytes) {
            KeyClass::PrimaryVersion { .. } => tuples += 1,
            KeyClass::Clog { .. } => clogs += 1,
            KeyClass::Sequence { .. } => sequences += 1,
            _ => {}
        }
    }
    TableTransferMaterialization {
        identity,
        stats: TableTransferStats {
            pairs: u64::try_from(pairs.len()).expect("usize fits u64"),
            tuple_versions: tuples,
            clog_entries: clogs,
            sequence_entries: sequences,
            checksum: hex::encode(digest.finalize()),
        },
        pairs,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::put_op,
        visibility::{Snapshot, satisfies_mvcc},
    };
    use crabka_pgtypes::Datum;

    use super::*;
    use crate::{
        ReplayItem, RestoreTail, WalFrame,
        checkpoint::{
            CheckpointSnapshot, DEFAULT_PART_MAX_SIZE, InMemoryCheckpointStore,
            restore_latest_table_transfer_and_replay_tail, write_checkpoint,
        },
        frame::BARRIER_SEQ,
    };

    #[tokio::test]
    async fn checkpoint_and_tail_materialize_a_durable_mvcc_closure() {
        let table = 7;
        let (source, [old, updated, deleted]) = checkpoint_source(table);

        let objects = InMemoryCheckpointStore::shared();
        write_checkpoint(
            objects.as_ref(),
            "transfer",
            &source,
            CheckpointSnapshot {
                covered_offset: 1,
                journal_seq: 2,
                producer_epoch: 0,
                wal_generation: 0,
                garbage_horizon_xid: 0,
            },
            DEFAULT_PART_MAX_SIZE,
        )
        .await
        .expect("checkpoint");
        let tail_tuple = version::version_key_xid(table, 3, 9);
        let tail = vec![
            replay_item(
                2,
                2,
                vec![WriteOp::Put {
                    key: tail_tuple.clone(),
                    value: version::encode_tuple(9, 0, &[Datum::Int4(4)]),
                }],
            ),
            replay_item(3, 3, vec![put_op(9, XidStatus::Committed)]),
            barrier(4),
        ];
        let target = MemKv::default();
        let mut selector = TableTransferSelector::new(table).expect("selector");

        let (restored, replay) = restore_latest_table_transfer_and_replay_tail(
            objects.as_ref(),
            "transfer",
            &target,
            RestoreTail {
                current_generation: 0,
                log_start: Some(0),
                committed_frames: tail,
                barrier_offset: 4,
            },
            &mut selector,
        )
        .await
        .expect("transfer restore");

        assert!(restored.stats.tuple_versions == 3);
        assert!(restored.stats.clog_entries == 4);
        assert!(restored.stats.sequence_entries == 1);
        assert!(replay.next_journal_seq == 4);
        assert!(target.get(&old).expect("old") == source.get(&old).expect("source old"));
        assert!(
            target.get(&updated).expect("updated") == source.get(&updated).expect("source updated")
        );
        assert!(
            target.get(&deleted).expect("deleted") == source.get(&deleted).expect("source deleted")
        );
        assert!(target.get(&tail_tuple).expect("tail tuple").is_some());
        assert!(target.get(&key::clog_key(9)).expect("tail clog").is_some());
        assert!(
            target.get(&key::seq_key(table)).expect("sequence")
                == Some(9_u64.to_be_bytes().to_vec())
        );
        assert!(
            target
                .get(&key::catalog_key(
                    crabka_pgcatalog::PUBLIC_SCHEMA,
                    "table_7"
                ))
                .expect("catalog")
                .is_none()
        );
        assert!(
            target
                .get(&version::version_key_xid(8, 1, 5))
                .expect("other")
                .is_none()
        );
        let visible = satisfies_mvcc(
            6,
            0,
            &Snapshot {
                xmin: 1,
                xmax: 10,
                xip: Vec::new(),
            },
            None,
            |xid| clog::get(&target, xid),
        )
        .expect("visible");
        assert!(visible);
    }

    fn checkpoint_source(table: u32) -> (MemKv, [Vec<u8>; 3]) {
        let source = MemKv::default();
        let old = version::version_key_xid(table, 1, 5);
        let updated = version::version_key_xid(table, 1, 6);
        let deleted = version::version_key_xid(table, 2, 7);
        for (key_bytes, value) in [
            (old.clone(), version::encode_tuple(5, 6, &[Datum::Int4(1)])),
            (
                updated.clone(),
                version::encode_tuple(6, 0, &[Datum::Int4(2)]),
            ),
            (
                deleted.clone(),
                version::encode_tuple(7, 8, &[Datum::Int4(3)]),
            ),
            (key::seq_key(table), 9_u64.to_be_bytes().to_vec()),
            (
                version::version_key_xid(8, 1, 5),
                version::encode_tuple(5, 0, &[Datum::Int4(99)]),
            ),
            (
                key::catalog_key(crabka_pgcatalog::PUBLIC_SCHEMA, "table_7"),
                b"catalog".to_vec(),
            ),
        ] {
            source.put(key_bytes, value).expect("source pair");
        }
        for xid in [5, 6, 7, 8] {
            let WriteOp::Put {
                key: clog_key,
                value,
            } = put_op(xid, XidStatus::Committed)
            else {
                unreachable!()
            };
            source.put(clog_key, value).expect("clog");
        }
        (source, [old, updated, deleted])
    }

    #[tokio::test]
    async fn transfer_requires_a_durable_checkpoint_source() {
        let target = MemKv::default();
        let mut selector = TableTransferSelector::new(7).expect("selector");
        let error = restore_latest_table_transfer_and_replay_tail(
            InMemoryCheckpointStore::shared().as_ref(),
            "missing",
            &target,
            RestoreTail {
                current_generation: 0,
                log_start: None,
                committed_frames: vec![barrier(0)],
                barrier_offset: 0,
            },
            &mut selector,
        )
        .await
        .expect_err("no source");
        assert!(matches!(error, SubstrateError::Unavailable(_)));
    }

    #[test]
    fn rejects_local_index_hash_and_in_doubt_inputs() {
        let tuple = version::encode_tuple(5, 0, &[Datum::Int4(1)]);
        for pair in [
            (key::secondary_index_prefix(7, 1), b"index".to_vec()),
            (version::hash_version_key_xid(7, 1, 1, 5), tuple.clone()),
        ] {
            assert!(
                TableTransferSelector::new(7)
                    .expect("selector")
                    .materialize_checkpoint(vec![pair])
                    .is_err()
            );
        }
        let mut selector = TableTransferSelector::new(7).expect("selector");
        assert!(
            selector
                .materialize_checkpoint(vec![
                    (version::version_key_xid(7, 1, 5), tuple),
                    (key::clog_key(5), vec![3, 0, 0, 0, 0, 0, 0, 0, 1]),
                ])
                .is_err()
        );
        selector
            .materialize_checkpoint(vec![
                (
                    version::version_key_xid(7, 1, 5),
                    version::encode_tuple(5, 0, &[Datum::Int4(1)]),
                ),
                (key::clog_key(5), vec![1]),
            ])
            .expect("committed closure");
        let in_doubt = selector.select_tail_op(&put_op(5, XidStatus::InProgress));
        assert!(in_doubt.is_err());
    }

    #[test]
    fn selector_rejects_system_table_id() {
        assert!(TableTransferSelector::new(0).is_err());
    }

    #[test]
    fn referenced_clog_delete_in_tail_fails_closed() {
        let mut selector = TableTransferSelector::new(7).expect("selector");
        let tuple = version::version_key_xid(7, 1, 5);
        selector
            .materialize_checkpoint(vec![
                (tuple, version::encode_tuple(5, 0, &[Datum::Int4(1)])),
                (key::clog_key(5), vec![1]),
            ])
            .expect("checkpoint closure");
        let target = MemKv::default();
        target
            .put(key::clog_key(5), vec![1])
            .expect("seed CLOG closure");

        let error = crate::replay_committed_frames_from_table_transfer(
            &target,
            vec![replay_item(
                0,
                0,
                vec![WriteOp::Delete {
                    key: key::clog_key(5),
                }],
            )],
            1,
            0,
            0,
            &mut selector,
        )
        .expect_err("required CLOG delete must be rejected");

        assert!(matches!(error, SubstrateError::Checkpoint(_)));
        assert!(target.get(&key::clog_key(5)).expect("CLOG") == Some(vec![1]));
    }

    fn replay_item(offset: i64, journal_seq: u64, ops: Vec<WriteOp>) -> ReplayItem {
        ReplayItem {
            offset,
            bytes: WalFrame { journal_seq, ops }.encode(),
        }
    }

    fn barrier(offset: i64) -> ReplayItem {
        replay_item(offset, BARRIER_SEQ, Vec::new())
    }
}

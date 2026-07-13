//! Checkpoint part wire format.

use crabka_gres_ranges::{RangeKey, RowInterval, TableId};
use crabka_pgkv::key;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::{error::SubstrateError, frame::Reader};

/// One key/value pair carried by a checkpoint part.
pub type PartPayload = (Vec<u8>, Vec<u8>);

/// Row-key interval used to ingest a checkpoint subset during range restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFilter {
    start: RangeKey,
    end: Option<RangeKey>,
    physical_to_logical: BTreeMap<TableId, TableId>,
    owns_structural: Option<bool>,
    target_range: Option<u32>,
}

impl CheckpointFilter {
    /// Build a non-empty half-open row-key interval `[start, end)`.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] when the exclusive end is not after the start.
    pub fn new(start: RangeKey, end: Option<RangeKey>) -> Result<Self, SubstrateError> {
        if end.is_some_and(|end| end <= start) {
            return Err(SubstrateError::Checkpoint(
                "checkpoint filter end must be greater than start".into(),
            ));
        }

        Ok(Self {
            start,
            end,
            physical_to_logical: BTreeMap::new(),
            owns_structural: None,
            target_range: None,
        })
    }

    /// Translate physical catalog table ids before applying the logical range interval.
    #[must_use]
    pub fn with_physical_to_logical(mut self, mapping: BTreeMap<TableId, TableId>) -> Self {
        self.physical_to_logical = mapping;
        self
    }

    /// Assign non-row metadata to exactly one successor in a partition.
    #[must_use]
    pub fn with_structural_ownership(mut self, owns_structural: bool) -> Self {
        self.owns_structural = Some(owns_structural);
        self
    }

    /// Name the successor range used when rehoming copied timestamp descriptors.
    #[must_use]
    pub fn with_target_range(mut self, range_id: crabka_gres_ranges::RangeId) -> Self {
        self.target_range = Some(range_id.as_u32());
        self
    }

    /// Select and, when necessary, rewrite one checkpoint pair for this successor.
    pub fn filter_pair(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, SubstrateError> {
        if key.starts_with(b"\0\0\0\0meta/ts_intent/") {
            if !self.contains_pair(key, Some(value))? {
                return Ok(None);
            }
            let target = self.target_range.ok_or_else(|| {
                SubstrateError::Checkpoint("timestamp intent filter lacks target range".into())
            })?;
            let mut value: [u8; 24] = value.try_into().map_err(|_| {
                SubstrateError::Checkpoint("malformed timestamp intent identity".into())
            })?;
            value[16..20].copy_from_slice(&target.to_be_bytes());
            value[20..24].copy_from_slice(&target.to_be_bytes());
            return Ok(Some(value.to_vec()));
        }
        if !key.starts_with(b"\0\0\0\0meta/ts_txn/") {
            return self
                .contains_pair(key, Some(value))
                .map(|selected| selected.then(|| value.to_vec()));
        }
        let start_ts = timestamp_descriptor_start(key)?;
        let mut descriptor = crabka_pgexec::decode_timestamp_txn_descriptor_value(start_ts, value)
            .map_err(|error| {
                SubstrateError::Checkpoint(format!("malformed timestamp descriptor: {error}"))
            })?;
        let target_range = self.target_range.ok_or_else(|| {
            SubstrateError::Checkpoint("timestamp descriptor filter lacks target range".into())
        })?;
        let mut selected_source_ranges = std::collections::BTreeSet::new();
        let mut selected_operations = Vec::new();
        for mut operation in descriptor.operations {
            let logical = self.logical_table(operation.table_id)?;
            let key = operation.bucket.map_or_else(
                || RangeKey::new(logical, operation.rowid),
                |bucket| RangeKey::hash(logical, bucket, operation.rowid),
            );
            if self.contains_range_key(key) {
                selected_source_ranges.insert(operation.range_id);
                operation.range_id = target_range;
                selected_operations.push(operation);
            }
        }
        descriptor.operations = selected_operations;
        if descriptor.operations.is_empty() {
            return Ok(None);
        }
        let prepared = selected_source_ranges
            .iter()
            .all(|range| descriptor.prepared.contains(range));
        descriptor.participants = vec![target_range];
        descriptor.prepared = prepared.then_some(target_range).into_iter().collect();
        let crabka_pgkv::WriteOp::Put { value, .. } =
            crabka_pgexec::timestamp_txn_descriptor_op(&descriptor)
        else {
            unreachable!()
        };
        Ok(Some(value))
    }

    /// Build a table-local row interval filter.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] when the interval is empty or inverted.
    pub fn for_table_interval(
        table_id: TableId,
        interval: RowInterval,
    ) -> Result<Self, SubstrateError> {
        let start = RangeKey::new(table_id, interval.start.unwrap_or(0));
        let end = interval.end.map(|rowid| RangeKey::new(table_id, rowid));
        Self::new(start, end)
    }

    /// Return true when a KV key belongs to this row interval.
    #[must_use]
    pub fn contains_key(&self, bytes: &[u8]) -> Result<bool, SubstrateError> {
        self.contains_pair(bytes, None)
    }

    /// Return true when a KV pair belongs to this interval, including timestamp metadata whose
    /// ownership is encoded in its key or descriptor value.
    pub fn contains_pair(
        &self,
        bytes: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, SubstrateError> {
        if let Some(key) = self.timestamp_metadata_key(bytes, value)? {
            return Ok(self.contains_range_key(key));
        }
        if bytes.starts_with(b"\0\0\0\0meta/ts_txn/") {
            let start_ts = timestamp_descriptor_start(bytes)?;
            return match value {
                Some(value) => self.timestamp_descriptor_belongs(start_ts, value),
                None => Ok(true),
            };
        }
        // Non-row substrate metadata has no range key. Assign it exactly once
        // to the interval beginning at the global minimum (the r0 successor),
        // so two adjacent successor restores form a disjoint full partition.
        let owns_structural = self.owns_structural.unwrap_or(self.start == RangeKey::MIN);
        let row_key = match key::classify_key(bytes) {
            key::KeyClass::PrimaryRow { table_id, rowid }
            | key::KeyClass::PrimaryVersion {
                table_id, rowid, ..
            } => RangeKey::new(self.logical_table(table_id)?, rowid),
            key::KeyClass::HashPrimaryRow {
                table_id,
                bucket,
                rowid,
            }
            | key::KeyClass::HashPrimaryVersion {
                table_id,
                bucket,
                rowid,
                ..
            } => RangeKey::hash(self.logical_table(table_id)?, bucket, rowid),
            key::KeyClass::Sequence { table_id } => {
                RangeKey::table_start(self.logical_table(table_id)?)
            }
            key::KeyClass::Clog { .. }
            | key::KeyClass::SecondaryIndex { .. }
            | key::KeyClass::System
            | key::KeyClass::Unknown => return Ok(owns_structural),
        };
        Ok(self.contains_range_key(row_key))
    }

    fn contains_range_key(&self, key: RangeKey) -> bool {
        key >= self.start && self.end.is_none_or(|end| key < end)
    }

    fn timestamp_metadata_key(
        &self,
        bytes: &[u8],
        _value: Option<&[u8]>,
    ) -> Result<Option<RangeKey>, SubstrateError> {
        const INTENT: &[u8] = b"\0\0\0\0meta/ts_intent/";
        const PREWRITE: &[u8] = b"\0\0\0\0meta/ts_prewrite/";
        let (tail, suffix_len) = if let Some(tail) = bytes.strip_prefix(INTENT) {
            if !matches!(tail.len(), 20 | 25) {
                return Err(SubstrateError::Checkpoint(
                    "malformed timestamp intent metadata key".into(),
                ));
            }
            (tail, 8)
        } else if let Some(tail) = bytes.strip_prefix(PREWRITE) {
            if !matches!(tail.len(), 12 | 17) {
                return Err(SubstrateError::Checkpoint(
                    "malformed timestamp prewrite metadata key".into(),
                ));
            }
            (tail, 0)
        } else {
            return Ok(None);
        };
        let physical = u32::from_be_bytes(tail[..4].try_into().expect("4-byte table id"));
        let logical = self.logical_table(physical)?;
        let row_tail = &tail[..tail.len() - suffix_len];
        let key = match row_tail.len() {
            12 => {
                let rowid = u64::from_be_bytes(row_tail[4..12].try_into().expect("8-byte row id"));
                RangeKey::new(logical, rowid)
            }
            17 if row_tail[4] == 1 => {
                let bucket = u32::from_be_bytes(row_tail[5..9].try_into().expect("4-byte bucket"));
                let rowid = u64::from_be_bytes(row_tail[9..17].try_into().expect("8-byte row id"));
                RangeKey::hash(logical, bucket, rowid)
            }
            _ => {
                return Err(SubstrateError::Checkpoint(
                    "malformed timestamp metadata bucket tag".into(),
                ));
            }
        };
        Ok(Some(key))
    }

    fn timestamp_descriptor_belongs(
        &self,
        start_ts: crabka_pgexec::TimestampTransactionId,
        value: &[u8],
    ) -> Result<bool, SubstrateError> {
        let descriptor = crabka_pgexec::decode_timestamp_txn_descriptor_value(start_ts, value)
            .map_err(|error| {
                SubstrateError::Checkpoint(format!("malformed timestamp descriptor: {error}"))
            })?;
        descriptor
            .operations
            .into_iter()
            .try_fold(false, |owned, operation| {
                let logical = self.logical_table(operation.table_id)?;
                let key = operation.bucket.map_or_else(
                    || RangeKey::new(logical, operation.rowid),
                    |bucket| RangeKey::hash(logical, bucket, operation.rowid),
                );
                Ok(owned || self.contains_range_key(key))
            })
    }

    fn logical_table(&self, physical_id: u32) -> Result<TableId, SubstrateError> {
        let physical = TableId::new(u64::from(physical_id));
        self.physical_to_logical
            .get(&physical)
            .copied()
            .ok_or(SubstrateError::UnmappedPhysicalTable(physical_id))
    }
}

fn timestamp_descriptor_start(
    bytes: &[u8],
) -> Result<crabka_pgexec::TimestampTransactionId, SubstrateError> {
    const PREFIX: &[u8] = b"\0\0\0\0meta/ts_txn/";
    let raw = bytes.strip_prefix(PREFIX).expect("prefix checked");
    let raw: [u8; 8] = raw
        .try_into()
        .map_err(|_| SubstrateError::Checkpoint("malformed timestamp descriptor key".into()))?;
    crabka_pgexec::TimestampTransactionId::new(u64::from_be_bytes(raw))
        .map_err(|_| SubstrateError::Checkpoint("zero timestamp descriptor key".into()))
}

/// A checkpoint part containing key-ordered KV snapshot chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointPart {
    /// KV pairs in snapshot key order.
    pub pairs: Vec<PartPayload>,
}

impl CheckpointPart {
    /// Build a part from already ordered pairs.
    #[must_use]
    pub fn new(pairs: Vec<PartPayload>) -> Self {
        Self { pairs }
    }

    /// Encode the part as repeated `[u32 klen][key][u32 vlen][value]` entries.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        for (key, value) in &self.pairs {
            push_chunk(&mut out, key);
            push_chunk(&mut out, value);
        }
        out
    }

    /// Decode a bounds-checked part object.
    pub fn decode(bytes: &[u8]) -> Result<Self, SubstrateError> {
        let mut reader = Reader { bytes, at: 0 };
        let mut pairs = Vec::new();
        while reader.at < bytes.len() {
            let key = reader.chunk()?.to_vec();
            let value = reader.chunk()?.to_vec();
            pairs.push((key, value));
        }
        Ok(Self { pairs })
    }

    /// Return the encoded byte length.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        self.pairs
            .iter()
            .map(|(key, value)| 8 + key.len() + value.len())
            .sum()
    }

    /// Return the SHA-256 digest of the encoded part as lowercase hexadecimal.
    #[must_use]
    pub fn sha256_hex(&self) -> String {
        sha256_hex(&self.encode())
    }

    /// Split key/value pairs into encoded parts capped by the target size when possible.
    pub fn split_at_target_size(
        pairs: impl IntoIterator<Item = PartPayload>,
        part_max_bytes: usize,
    ) -> Result<Vec<Self>, SubstrateError> {
        if part_max_bytes < 8 {
            return Err(SubstrateError::Checkpoint(
                "part_max_bytes must fit one empty key/value pair".into(),
            ));
        }

        let mut parts = Vec::new();
        let mut current = Vec::new();
        let mut current_len = 0_usize;
        for pair in pairs {
            let pair_len = pair_encoded_len(&pair)?;
            if !current.is_empty() && current_len + pair_len > part_max_bytes {
                parts.push(Self::new(std::mem::take(&mut current)));
                current_len = 0;
            }
            current_len += pair_len;
            current.push(pair);
        }

        if !current.is_empty() {
            parts.push(Self::new(current));
        }
        Ok(parts)
    }
}

/// Return the SHA-256 digest of raw part bytes as lowercase hexadecimal.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn pair_encoded_len((key, value): &PartPayload) -> Result<usize, SubstrateError> {
    let body_len = key
        .len()
        .checked_add(value.len())
        .and_then(|len| len.checked_add(8))
        .ok_or_else(|| SubstrateError::Checkpoint("part pair length overflow".into()))?;

    u32::try_from(key.len())
        .and_then(|_| u32::try_from(value.len()))
        .map_err(|_| SubstrateError::Checkpoint("part key/value length exceeds u32".into()))?;
    Ok(body_len)
}

fn push_chunk(out: &mut Vec<u8>, chunk: &[u8]) {
    out.extend_from_slice(
        &u32::try_from(chunk.len())
            .expect("checkpoint chunk fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(chunk);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use proptest::prelude::*;

    use super::*;

    fn descriptor_pair(operations: &[(u32, u64)]) -> (Vec<u8>, Vec<u8>) {
        let start_ts = crabka_pgexec::TimestampTransactionId::new(9).unwrap();
        let mut descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(start_ts, 10, vec![1]);
        let operations = operations
            .iter()
            .map(|(table_id, rowid)| crabka_pgexec::TimestampTxnOperation {
                range_id: 1,
                table_id: *table_id,
                bucket: None,
                rowid: *rowid,
                delete: false,
            })
            .collect::<Vec<_>>();
        descriptor
            .acknowledge_operations(1, &operations)
            .expect("descriptor operations");
        let crabka_pgkv::WriteOp::Put { key, value } =
            crabka_pgexec::timestamp_txn_descriptor_op(&descriptor)
        else {
            unreachable!()
        };
        (key, value)
    }

    fn split_filters() -> (CheckpointFilter, CheckpointFilter) {
        let mapping = BTreeMap::from([
            (TableId::new(1), TableId::new(52)),
            (TableId::new(2), TableId::new(50)),
        ]);
        let left = CheckpointFilter::new(
            RangeKey::new(TableId::new(50), 10),
            Some(RangeKey::new(TableId::new(51), 16)),
        )
        .unwrap()
        .with_physical_to_logical(mapping.clone())
        .with_structural_ownership(true)
        .with_target_range(crabka_gres_ranges::RangeId::new(2));
        let right = CheckpointFilter::new(RangeKey::new(TableId::new(51), 16), None)
            .unwrap()
            .with_physical_to_logical(mapping)
            .with_structural_ownership(false)
            .with_target_range(crabka_gres_ranges::RangeId::new(3));
        (left, right)
    }

    #[test]
    fn timestamp_metadata_routes_by_embedded_physical_row_key() {
        let (left, right) = split_filters();
        let mut intent = b"\0\0\0\0meta/ts_intent/".to_vec();
        intent.extend_from_slice(&1_u32.to_be_bytes());
        intent.extend_from_slice(&1_u64.to_be_bytes());
        intent.extend_from_slice(&9_u64.to_be_bytes());
        let mut identity = vec![0; 24];
        identity[16..20].copy_from_slice(&1_u32.to_be_bytes());
        identity[20..24].copy_from_slice(&1_u32.to_be_bytes());
        assert!(left.filter_pair(&intent, &identity).unwrap().is_none());
        let rewritten = right
            .filter_pair(&intent, &identity)
            .unwrap()
            .expect("right intent");
        assert_eq!(&rewritten[16..20], &3_u32.to_be_bytes());
        assert_eq!(&rewritten[20..24], &3_u32.to_be_bytes());
    }

    #[test]
    fn hash_timestamp_metadata_routes_by_embedded_bucket_and_preserves_bucket_zero() {
        let mapping = BTreeMap::from([(TableId::new(1), TableId::new(10))]);
        let left = CheckpointFilter::new(
            RangeKey::hash(TableId::new(10), 0, 0),
            Some(RangeKey::hash(TableId::new(10), 8, 0)),
        )
        .unwrap()
        .with_physical_to_logical(mapping.clone())
        .with_target_range(crabka_gres_ranges::RangeId::new(2));
        let right = CheckpointFilter::new(RangeKey::hash(TableId::new(10), 8, 0), None)
            .unwrap()
            .with_physical_to_logical(mapping)
            .with_target_range(crabka_gres_ranges::RangeId::new(3));

        let metadata_key = |prefix: &[u8], bucket: u32, start_ts: Option<u64>| {
            let mut key = prefix.to_vec();
            key.extend_from_slice(&1_u32.to_be_bytes());
            key.push(1);
            key.extend_from_slice(&bucket.to_be_bytes());
            key.extend_from_slice(&7_u64.to_be_bytes());
            if let Some(start_ts) = start_ts {
                key.extend_from_slice(&start_ts.to_be_bytes());
            }
            key
        };
        let mut identity = vec![0; 24];
        identity[16..20].copy_from_slice(&1_u32.to_be_bytes());
        identity[20..24].copy_from_slice(&1_u32.to_be_bytes());
        let bucket_zero = metadata_key(b"\0\0\0\0meta/ts_intent/", 0, Some(9));
        let bucket_fifteen = metadata_key(b"\0\0\0\0meta/ts_prewrite/", 15, None);

        assert!(left.contains_pair(&bucket_zero, Some(&identity)).unwrap());
        assert!(!right.contains_pair(&bucket_zero, Some(&identity)).unwrap());
        assert!(!left.contains_pair(&bucket_fifteen, Some(&[])).unwrap());
        assert!(right.contains_pair(&bucket_fifteen, Some(&[])).unwrap());
    }

    #[test]
    fn timestamp_descriptor_is_duplicated_for_cross_partition_operations() {
        let (left, right) = split_filters();
        let right_only = descriptor_pair(&[(1, 1)]);
        assert!(
            !left
                .contains_pair(&right_only.0, Some(&right_only.1))
                .unwrap()
        );
        assert!(
            right
                .contains_pair(&right_only.0, Some(&right_only.1))
                .unwrap()
        );

        let (cross_key, cross_value) = descriptor_pair(&[(2, 12), (1, 1)]);
        let start_ts = crabka_pgexec::TimestampTransactionId::new(9).unwrap();
        let mut committed =
            crabka_pgexec::decode_timestamp_txn_descriptor_value(start_ts, &cross_value).unwrap();
        committed
            .decide(crabka_pgexec::PrimaryTxnDecision::Committed(
                crabka_pgexec::CommitTimestamp::after_start(start_ts, 10).unwrap(),
            ))
            .unwrap();
        let crabka_pgkv::WriteOp::Put {
            value: cross_value, ..
        } = crabka_pgexec::timestamp_txn_descriptor_op(&committed)
        else {
            unreachable!()
        };
        let cross = (cross_key, cross_value);
        assert!(left.contains_pair(&cross.0, Some(&cross.1)).unwrap());
        assert!(right.contains_pair(&cross.0, Some(&cross.1)).unwrap());

        let left_value = left
            .filter_pair(&cross.0, &cross.1)
            .unwrap()
            .expect("left descriptor");
        let right_value = right
            .filter_pair(&cross.0, &cross.1)
            .unwrap()
            .expect("right descriptor");
        let left_descriptor =
            crabka_pgexec::decode_timestamp_txn_descriptor_value(start_ts, &left_value).unwrap();
        let right_descriptor =
            crabka_pgexec::decode_timestamp_txn_descriptor_value(start_ts, &right_value).unwrap();
        assert_eq!(left_descriptor.participants, vec![2]);
        assert_eq!(right_descriptor.participants, vec![3]);
        assert!(left_descriptor.operations.iter().all(|op| op.range_id == 2));
        assert!(
            right_descriptor
                .operations
                .iter()
                .all(|op| op.range_id == 3)
        );
        assert_eq!(left_descriptor.decision, right_descriptor.decision);
    }

    #[test]
    fn timestamp_descriptor_delete_broadcasts_to_both_successors() {
        let (left, right) = split_filters();
        let (key, _) = descriptor_pair(&[(1, 1)]);
        assert!(left.contains_pair(&key, None).unwrap());
        assert!(right.contains_pair(&key, None).unwrap());
        assert!(
            left.contains_pair(b"\0\0\0\0meta/ts_txn/bad", None)
                .is_err()
        );
    }

    #[test]
    fn timestamp_metadata_filtering_fails_closed() {
        let (_, right) = split_filters();
        let malformed = b"\0\0\0\0meta/ts_prewrite/short";
        assert!(right.contains_pair(malformed, Some(&[])).is_err());
        let unknown = descriptor_pair(&[(99, 1)]);
        assert!(matches!(
            right.contains_pair(&unknown.0, Some(&unknown.1)),
            Err(SubstrateError::UnmappedPhysicalTable(99))
        ));
        assert!(
            right
                .contains_pair(b"\0\0\0\0meta/ts_txn/bad", Some(&[]))
                .is_err()
        );
    }

    #[test]
    fn rejects_truncated_value() {
        let bytes = CheckpointPart::new(vec![(b"k".to_vec(), b"value".to_vec())]).encode();

        assert!(CheckpointPart::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn filtered_restore_rejects_unmapped_row_and_sequence_tables() {
        let filter = CheckpointFilter::new(RangeKey::MIN, None).expect("filter");
        for key in [
            crabka_pgkv::key::row_key(41, 7),
            crabka_pgkv::key::seq_key(42),
        ] {
            assert!(matches!(
                filter.contains_key(&key),
                Err(SubstrateError::UnmappedPhysicalTable(41 | 42))
            ));
        }
    }

    #[test]
    fn filtered_restore_uses_explicit_physical_to_logical_mapping() {
        let filter = CheckpointFilter::new(
            RangeKey::new(TableId::new(10), 0),
            Some(RangeKey::new(TableId::new(20), 0)),
        )
        .expect("filter")
        .with_physical_to_logical(BTreeMap::from([
            (TableId::new(41), TableId::new(10)),
            (TableId::new(42), TableId::new(30)),
        ]));
        assert!(
            filter
                .contains_key(&crabka_pgkv::key::row_key(41, 7))
                .unwrap()
        );
        assert!(!filter.contains_key(&crabka_pgkv::key::seq_key(42)).unwrap());
    }

    #[test]
    fn splits_parts_at_target_size() {
        let pairs = vec![
            (b"a".to_vec(), b"1111".to_vec()),
            (b"b".to_vec(), b"2222".to_vec()),
            (b"c".to_vec(), b"3333".to_vec()),
        ];

        let parts = CheckpointPart::split_at_target_size(pairs.clone(), 13).expect("split");

        assert!(parts.len() == 3);
        assert!(
            parts
                .into_iter()
                .flat_map(|part| part.pairs)
                .collect::<Vec<_>>()
                == pairs
        );
    }

    proptest! {
        #[test]
        fn prop_round_trips_parts(pairs in proptest::collection::vec(pair_strategy(), 0..64)) {
            let part = CheckpointPart::new(pairs);

            prop_assert_eq!(CheckpointPart::decode(&part.encode()).expect("decode"), part);
        }

        #[test]
        fn prop_split_round_trips_all_pairs(
            pairs in proptest::collection::vec(pair_strategy(), 0..64),
            part_max_bytes in 8_usize..256,
        ) {
            let parts = CheckpointPart::split_at_target_size(pairs.clone(), part_max_bytes).expect("split");
            let decoded = parts
                .into_iter()
                .flat_map(|part| CheckpointPart::decode(&part.encode()).expect("decode").pairs)
                .collect::<Vec<_>>();

            prop_assert_eq!(decoded, pairs);
        }
    }

    fn pair_strategy() -> impl Strategy<Value = PartPayload> {
        (
            proptest::collection::vec(any::<u8>(), 0..32),
            proptest::collection::vec(any::<u8>(), 0..128),
        )
    }
}

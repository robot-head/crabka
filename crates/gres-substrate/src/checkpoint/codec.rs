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
    pub fn contains_key(&self, bytes: &[u8]) -> bool {
        // Non-row substrate metadata has no range key. Assign it exactly once
        // to the interval beginning at the global minimum (the r0 successor),
        // so two adjacent successor restores form a disjoint full partition.
        let owns_structural = self.owns_structural.unwrap_or(self.start == RangeKey::MIN);
        let row_key = match key::classify_key(bytes) {
            key::KeyClass::PrimaryRow { table_id, rowid }
            | key::KeyClass::PrimaryVersion {
                table_id, rowid, ..
            } => RangeKey::new(self.logical_table(table_id), rowid),
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
            } => RangeKey::hash(self.logical_table(table_id), bucket, rowid),
            key::KeyClass::Sequence { table_id } => {
                RangeKey::table_start(self.logical_table(table_id))
            }
            key::KeyClass::Clog { .. }
            | key::KeyClass::SecondaryIndex { .. }
            | key::KeyClass::System
            | key::KeyClass::Unknown => return owns_structural,
        };
        if row_key < self.start {
            return false;
        };

        self.end.is_none_or(|end| row_key < end)
    }

    fn logical_table(&self, physical: u32) -> TableId {
        let physical = TableId::new(u64::from(physical));
        self.physical_to_logical
            .get(&physical)
            .copied()
            .unwrap_or(physical)
    }
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

    #[test]
    fn rejects_truncated_value() {
        let bytes = CheckpointPart::new(vec![(b"k".to_vec(), b"value".to_vec())]).encode();

        assert!(CheckpointPart::decode(&bytes[..bytes.len() - 1]).is_err());
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

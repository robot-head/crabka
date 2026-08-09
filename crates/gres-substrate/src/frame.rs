//! `GRW1` record framing: one frame per `Committer` batch.
//!
//! Layout (all integers big-endian):
//! `[version: u8][journal_seq: u64][op_count: u32]` then per op
//! `[tag: u8]` (0 = Put, 1 = Delete) `[klen: u32][key][vlen: u32][value]`
//! (`vlen`/`value` present only for Put). The parser bounds-checks every wire
//! length against the remaining buffer before any allocation.

use crabka_pgkv::WriteOp;

use crate::error::SubstrateError;

/// Current (only) frame version.
pub const GRW1_VERSION: u8 = 1;

/// Reserved journal sequence for recovery barriers.
pub const BARRIER_SEQ: u64 = u64::MAX;

/// One journaled `Committer` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    /// Monotone per-generation sequence. It is a replay tripwire, not a protocol.
    pub journal_seq: u64,
    /// The batch, in engine order.
    pub ops: Vec<WriteOp>,
}

impl WalFrame {
    /// Serialize to the `GRW1` byte layout.
    #[must_use]
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        out.push(GRW1_VERSION);
        out.extend_from_slice(&self.journal_seq.to_be_bytes());
        out.extend_from_slice(
            &u32::try_from(self.ops.len())
                .expect("op count fits u32")
                .to_be_bytes(),
        );
        for op in &self.ops {
            match op {
                WriteOp::Put { key, value } => {
                    out.push(0);
                    push_chunk(&mut out, key);
                    push_chunk(&mut out, value);
                }
                WriteOp::ConditionalPut {
                    key,
                    expected,
                    value,
                } => {
                    out.push(2);
                    push_chunk(&mut out, key);
                    match expected {
                        Some(expected) => {
                            out.push(1);
                            push_chunk(&mut out, expected);
                        }
                        None => out.push(0),
                    }
                    push_chunk(&mut out, value);
                }
                WriteOp::Delete { key } => {
                    out.push(1);
                    push_chunk(&mut out, key);
                }
            }
        }
        out
    }

    /// Parse a `GRW1` frame. This function validates every length before use.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn decode(bytes: &[u8]) -> Result<Self, SubstrateError> {
        let mut reader = Reader { bytes, at: 0 };
        let version = reader.u8()?;
        if version != GRW1_VERSION {
            return Err(SubstrateError::Frame(format!("unknown version {version}")));
        }

        let journal_seq = reader.u64()?;
        let op_count = reader.u32()?;
        let mut ops = Vec::new();
        for _ in 0..op_count {
            let tag = reader.u8()?;
            let key = reader.chunk()?.to_vec();
            let op = match tag {
                0 => WriteOp::Put {
                    key,
                    value: reader.chunk()?.to_vec(),
                },
                1 => WriteOp::Delete { key },
                2 => {
                    let expected = match reader.u8()? {
                        0 => None,
                        1 => Some(reader.chunk()?.to_vec()),
                        other => {
                            return Err(SubstrateError::Frame(format!(
                                "unknown conditional expectation tag {other}"
                            )));
                        }
                    };
                    WriteOp::ConditionalPut {
                        key,
                        expected,
                        value: reader.chunk()?.to_vec(),
                    }
                }
                other => return Err(SubstrateError::Frame(format!("unknown op tag {other}"))),
            };
            ops.push(op);
        }

        if reader.at != bytes.len() {
            return Err(SubstrateError::Frame("trailing bytes".into()));
        }

        Ok(Self { journal_seq, ops })
    }

    /// The encoded frame size in bytes.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        13 + self
            .ops
            .iter()
            .map(|op| match op {
                WriteOp::Put { key, value } => 9 + key.len() + value.len(),
                WriteOp::ConditionalPut {
                    key,
                    expected,
                    value,
                } => 14 + key.len() + expected.as_ref().map_or(0, Vec::len) + value.len(),
                WriteOp::Delete { key } => 5 + key.len(),
            })
            .sum::<usize>()
    }
}

fn push_chunk(out: &mut Vec<u8>, chunk: &[u8]) {
    out.extend_from_slice(
        &u32::try_from(chunk.len())
            .expect("chunk fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(chunk);
}

pub(crate) struct Reader<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) at: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], SubstrateError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| SubstrateError::Frame("truncated frame".into()))?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, SubstrateError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, SubstrateError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, SubstrateError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    pub(crate) fn chunk(&mut self) -> Result<&'a [u8], SubstrateError> {
        let len = self.u32()?;
        self.take(usize::try_from(len).expect("u32 fits usize"))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::WriteOp;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn round_trips_a_mixed_batch() {
        let frame = WalFrame {
            journal_seq: 42,
            ops: vec![
                WriteOp::Put {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
                WriteOp::Delete {
                    key: b"k2".to_vec(),
                },
            ],
        };

        let decoded = WalFrame::decode(&frame.encode()).expect("decode");

        assert!(decoded == frame);
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = WalFrame {
            journal_seq: 0,
            ops: vec![],
        }
        .encode();
        bytes[0] = 99;

        assert!(WalFrame::decode(&bytes).is_err());
    }

    #[test]
    fn rejects_truncated_frame() {
        let bytes = WalFrame {
            journal_seq: 7,
            ops: vec![WriteOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        }
        .encode();

        assert!(WalFrame::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn reserves_barrier_sequence() {
        let barrier = WalFrame {
            journal_seq: BARRIER_SEQ,
            ops: Vec::new(),
        };

        let decoded = WalFrame::decode(&barrier.encode()).expect("decode");

        assert!(decoded == barrier);
    }

    proptest! {
        #[test]
        fn prop_round_trip(seq in any::<u64>(), ops in proptest::collection::vec(op_strategy(), 0..32)) {
            let frame = WalFrame { journal_seq: seq, ops };

            prop_assert_eq!(WalFrame::decode(&frame.encode()).expect("decode"), frame);
        }
    }

    fn op_strategy() -> impl Strategy<Value = WriteOp> {
        prop_oneof![
            (
                proptest::collection::vec(any::<u8>(), 0..64),
                proptest::collection::vec(any::<u8>(), 0..256),
            )
                .prop_map(|(key, value)| WriteOp::Put { key, value }),
            proptest::collection::vec(any::<u8>(), 0..64).prop_map(|key| WriteOp::Delete { key }),
        ]
    }
}

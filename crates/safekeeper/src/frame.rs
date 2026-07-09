//! Internal `PGW1` WAL frame encoding and contiguity checks.
//!
//! Each Kafka record payload is one complete WAL frame. The Kafka record
//! boundary delimits the frame, so the frame body does not carry a length:
//! `b"PGW1" | start_lsn:u64le | wal_bytes...`.

use bytes::Bytes;
use thiserror::Error;

use crate::Lsn;

const WAL_FRAME_MAGIC: &[u8; 4] = b"PGW1";
const WAL_FRAME_HEADER_LEN: usize = WAL_FRAME_MAGIC.len() + size_of::<u64>();

/// A WAL payload beginning at a specific `PostgreSQL` LSN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    /// LSN of the first byte in `payload`.
    pub lsn: Lsn,
    /// Contiguous WAL bytes beginning at `lsn`.
    pub payload: Bytes,
}

impl WalFrame {
    /// Creates a WAL frame after enforcing the non-empty payload invariant.
    pub fn new(lsn: Lsn, payload: impl Into<Bytes>) -> Result<Self, WalFrameError> {
        let payload = payload.into();
        if payload.is_empty() {
            return Err(WalFrameError::EmptyPayload { lsn });
        }

        Ok(Self { lsn, payload })
    }

    /// Encodes this frame as `b"PGW1" | start_lsn:u64le | payload`.
    pub fn encode(&self) -> Result<Vec<u8>, WalFrameError> {
        let frame_len = WAL_FRAME_HEADER_LEN.checked_add(self.payload.len()).ok_or(
            WalFrameError::PayloadTooLarge {
                lsn: self.lsn,
                len: self.payload.len(),
            },
        )?;
        let mut encoded = Vec::with_capacity(frame_len);
        encoded.extend_from_slice(WAL_FRAME_MAGIC);
        encoded.extend_from_slice(&self.lsn.value().to_le_bytes());
        encoded.extend_from_slice(&self.payload);
        Ok(encoded)
    }

    /// Decodes a frame produced by [`Self::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, WalFrameError> {
        if bytes.len() < WAL_FRAME_HEADER_LEN {
            return Err(WalFrameError::TruncatedHeader {
                needed: WAL_FRAME_HEADER_LEN,
                got: bytes.len(),
            });
        }

        if &bytes[..WAL_FRAME_MAGIC.len()] != WAL_FRAME_MAGIC {
            let mut got = [0_u8; WAL_FRAME_MAGIC.len()];
            got.copy_from_slice(&bytes[..WAL_FRAME_MAGIC.len()]);
            return Err(WalFrameError::InvalidMagic { got });
        }

        let lsn = Lsn(read_u64_le(bytes, WAL_FRAME_MAGIC.len()));
        Self::new(lsn, Bytes::copy_from_slice(&bytes[WAL_FRAME_HEADER_LEN..]))
    }
}

/// Enforces contiguous WAL frame ingestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunker {
    next_lsn: Lsn,
}

impl Chunker {
    /// Creates a chunker that expects the next frame at `next_lsn`.
    #[must_use]
    pub const fn new(next_lsn: Lsn) -> Self {
        Self { next_lsn }
    }

    /// Returns the LSN expected for the next accepted frame.
    #[must_use]
    pub const fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Accepts `frame` only when it begins exactly at [`Self::next_lsn`].
    pub fn accept(&mut self, frame: &WalFrame) -> Result<(), WalFrameError> {
        if frame.payload.is_empty() {
            return Err(WalFrameError::EmptyPayload { lsn: frame.lsn });
        }

        if frame.lsn > self.next_lsn {
            return Err(WalFrameError::Gap {
                expected: self.next_lsn,
                got: frame.lsn,
            });
        }

        if frame.lsn < self.next_lsn {
            return Err(WalFrameError::Overlap {
                expected: self.next_lsn,
                got: frame.lsn,
            });
        }

        let bytes_accepted =
            u64::try_from(frame.payload.len()).map_err(|_| WalFrameError::PayloadTooLarge {
                lsn: frame.lsn,
                len: frame.payload.len(),
            })?;
        let next_value = self.next_lsn.value().checked_add(bytes_accepted).ok_or(
            WalFrameError::LsnOverflow {
                lsn: self.next_lsn,
                len: frame.payload.len(),
            },
        )?;
        self.next_lsn = Lsn(next_value);
        Ok(())
    }
}

/// Errors returned by internal WAL frame parsing and contiguity checks.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum WalFrameError {
    /// The frame header is incomplete.
    #[error("WAL frame header is truncated: needed {needed} bytes, got {got}")]
    TruncatedHeader {
        /// Header bytes needed.
        needed: usize,
        /// Bytes available.
        got: usize,
    },

    /// The frame did not begin with the `PGW1` magic bytes.
    #[error("WAL frame magic is invalid: expected PGW1, got {got:?}")]
    InvalidMagic {
        /// First four bytes in the frame.
        got: [u8; 4],
    },

    /// WAL frames must carry at least one byte.
    #[error("WAL frame at {lsn} has an empty payload")]
    EmptyPayload {
        /// Frame LSN.
        lsn: Lsn,
    },

    /// The payload cannot be represented by the frame format or target platform.
    #[error("WAL frame at {lsn} payload is too large: {len} bytes")]
    PayloadTooLarge {
        /// Frame LSN.
        lsn: Lsn,
        /// Payload length.
        len: usize,
    },

    /// The frame starts after the expected LSN.
    #[error("WAL frame gap: expected {expected}, got {got}")]
    Gap {
        /// Expected frame LSN.
        expected: Lsn,
        /// Actual frame LSN.
        got: Lsn,
    },

    /// The frame starts before the expected LSN.
    #[error("WAL frame overlap: expected {expected}, got {got}")]
    Overlap {
        /// Expected frame LSN.
        expected: Lsn,
        /// Actual frame LSN.
        got: Lsn,
    },

    /// Advancing by the payload length would overflow the LSN space.
    #[error("WAL frame at {lsn} with {len} payload bytes overflows LSN space")]
    LsnOverflow {
        /// Frame LSN.
        lsn: Lsn,
        /// Payload length.
        len: usize,
    },
}

fn read_u64_le(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn frame_round_trips_through_pgw1_record_payload() {
        let frame = WalFrame::new(Lsn(0x16_0000_0010), Bytes::from_static(b"wal-bytes"));
        assert!(let Ok(frame) = frame);

        let encoded = frame.encode();
        assert!(let Ok(encoded) = encoded);
        assert!(&encoded[..4] == b"PGW1");
        assert!(
            u64::from_le_bytes(encoded[4..12].try_into().expect("lsn bytes")) == frame.lsn.value()
        );
        assert!(
            u64::from_be_bytes(encoded[4..12].try_into().expect("lsn bytes")) != frame.lsn.value()
        );
        assert!(&encoded[12..] == frame.payload.as_ref());

        let decoded = WalFrame::decode(&encoded);

        assert!(decoded == Ok(frame));
    }

    #[test]
    fn decode_rejects_invalid_magic_before_reading_payload() {
        let bytes = b"BAD!\x10\0\0\0\0\0\0\0wal";

        let decoded = WalFrame::decode(bytes);

        assert!(decoded == Err(WalFrameError::InvalidMagic { got: *b"BAD!" }));
    }

    #[test]
    fn decode_rejects_truncated_header() {
        let decoded = WalFrame::decode(b"PGW1\x10");

        assert!(
            decoded
                == Err(WalFrameError::TruncatedHeader {
                    needed: WAL_FRAME_HEADER_LEN,
                    got: 5
                })
        );
    }

    #[test]
    fn decode_rejects_header_without_payload() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"PGW1");
        bytes.extend_from_slice(&42_u64.to_le_bytes());

        let decoded = WalFrame::decode(&bytes);

        assert!(decoded == Err(WalFrameError::EmptyPayload { lsn: Lsn(42) }));
    }

    #[test]
    fn chunker_accepts_contiguous_frames_and_advances_next_lsn() {
        let mut chunker = Chunker::new(Lsn(10));
        let first = WalFrame::new(Lsn(10), Bytes::from_static(b"abc"));
        let second = WalFrame::new(Lsn(13), Bytes::from_static(b"de"));
        assert!(let Ok(first) = first);
        assert!(let Ok(second) = second);

        assert!(chunker.accept(&first) == Ok(()));
        assert!(chunker.accept(&second) == Ok(()));

        assert!(chunker.next_lsn() == Lsn(15));
    }

    #[test]
    fn chunker_rejects_gap() {
        let mut chunker = Chunker::new(Lsn(10));
        let frame = WalFrame::new(Lsn(11), Bytes::from_static(b"x"));
        assert!(let Ok(frame) = frame);

        assert!(
            chunker.accept(&frame)
                == Err(WalFrameError::Gap {
                    expected: Lsn(10),
                    got: Lsn(11)
                })
        );
    }

    #[test]
    fn chunker_rejects_overlap() {
        let mut chunker = Chunker::new(Lsn(10));
        let frame = WalFrame::new(Lsn(9), Bytes::from_static(b"x"));
        assert!(let Ok(frame) = frame);

        assert!(
            chunker.accept(&frame)
                == Err(WalFrameError::Overlap {
                    expected: Lsn(10),
                    got: Lsn(9)
                })
        );
    }

    #[test]
    fn empty_payload_is_rejected() {
        let frame = WalFrame::new(Lsn(10), Bytes::new());

        assert!(frame == Err(WalFrameError::EmptyPayload { lsn: Lsn(10) }));
    }
}

//! Pure `PostgreSQL` `CopyBoth` streaming replication message codec.

use bytes::Bytes;
use thiserror::Error;

use crate::Lsn;

const XLOG_DATA_TAG: u8 = b'w';
const PRIMARY_KEEPALIVE_TAG: u8 = b'k';
const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';
const XLOG_DATA_HEADER_LEN: usize = 25;
const PRIMARY_KEEPALIVE_LEN: usize = 18;
const STANDBY_STATUS_UPDATE_LEN: usize = 34;

/// A parsed `CopyBoth` message used by `PostgreSQL` physical replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyBothMessage {
    /// WAL bytes streamed from the primary.
    XLogData(XLogData),
    /// Primary keepalive message.
    PrimaryKeepalive(PrimaryKeepalive),
    /// Standby status update message.
    StandbyStatusUpdate(StandbyStatusUpdate),
}

impl CopyBothMessage {
    /// Parses a `CopyBoth` data payload.
    pub fn parse(bytes: &[u8]) -> Result<Self, CopyBothError> {
        let (&tag, _) = bytes.split_first().ok_or(CopyBothError::EmptyMessage)?;
        match tag {
            XLOG_DATA_TAG => XLogData::parse(bytes).map(Self::XLogData),
            PRIMARY_KEEPALIVE_TAG => PrimaryKeepalive::parse(bytes).map(Self::PrimaryKeepalive),
            STANDBY_STATUS_UPDATE_TAG => {
                StandbyStatusUpdate::parse(bytes).map(Self::StandbyStatusUpdate)
            }
            other => Err(CopyBothError::UnknownMessageTag { tag: other }),
        }
    }

    /// Encodes this message as a `CopyBoth` data payload.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::XLogData(message) => message.encode(),
            Self::PrimaryKeepalive(message) => message.encode(),
            Self::StandbyStatusUpdate(message) => message.encode(),
        }
    }
}

/// WAL bytes streamed from the primary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XLogData {
    /// LSN of the first WAL byte in `data`.
    pub wal_start: Lsn,
    /// Server WAL end when this message was sent.
    pub wal_end: Lsn,
    /// Server send timestamp in `PostgreSQL`'s replication protocol epoch.
    pub send_time: i64,
    /// WAL bytes carried by this message.
    pub data: Bytes,
}

impl XLogData {
    /// Encodes this `XLogData` message.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(XLOG_DATA_HEADER_LEN + self.data.len());
        encoded.push(XLOG_DATA_TAG);
        push_lsn(&mut encoded, self.wal_start);
        push_lsn(&mut encoded, self.wal_end);
        push_i64(&mut encoded, self.send_time);
        encoded.extend_from_slice(&self.data);
        encoded
    }

    fn parse(bytes: &[u8]) -> Result<Self, CopyBothError> {
        require_exact_or_more(bytes, XLOG_DATA_HEADER_LEN, XLOG_DATA_TAG)?;
        Ok(Self {
            wal_start: Lsn(read_u64(bytes, 1)),
            wal_end: Lsn(read_u64(bytes, 9)),
            send_time: read_i64(bytes, 17),
            data: Bytes::copy_from_slice(&bytes[XLOG_DATA_HEADER_LEN..]),
        })
    }
}

/// Keepalive sent by the primary while streaming WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryKeepalive {
    /// Server WAL end when this message was sent.
    pub wal_end: Lsn,
    /// Server send timestamp in `PostgreSQL`'s replication protocol epoch.
    pub send_time: i64,
    /// Whether the primary requests an immediate reply.
    pub reply_requested: bool,
}

impl PrimaryKeepalive {
    /// Encodes this primary keepalive message.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(PRIMARY_KEEPALIVE_LEN);
        encoded.push(PRIMARY_KEEPALIVE_TAG);
        push_lsn(&mut encoded, self.wal_end);
        push_i64(&mut encoded, self.send_time);
        push_bool(&mut encoded, self.reply_requested);
        encoded
    }

    fn parse(bytes: &[u8]) -> Result<Self, CopyBothError> {
        require_exact_len(bytes, PRIMARY_KEEPALIVE_LEN, PRIMARY_KEEPALIVE_TAG)?;
        Ok(Self {
            wal_end: Lsn(read_u64(bytes, 1)),
            send_time: read_i64(bytes, 9),
            reply_requested: read_bool(bytes, 17)?,
        })
    }
}

/// Status update sent by the standby.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandbyStatusUpdate {
    /// Last WAL byte written to disk by the standby.
    pub write_lsn: Lsn,
    /// Last WAL byte flushed to disk by the standby.
    pub flush_lsn: Lsn,
    /// Last WAL byte applied by the standby.
    pub apply_lsn: Lsn,
    /// Standby timestamp in `PostgreSQL`'s replication protocol epoch.
    pub client_time: i64,
    /// Whether the standby asks for an immediate response.
    pub reply_requested: bool,
}

impl StandbyStatusUpdate {
    /// Encodes this standby status update message.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(STANDBY_STATUS_UPDATE_LEN);
        encoded.push(STANDBY_STATUS_UPDATE_TAG);
        push_lsn(&mut encoded, self.write_lsn);
        push_lsn(&mut encoded, self.flush_lsn);
        push_lsn(&mut encoded, self.apply_lsn);
        push_i64(&mut encoded, self.client_time);
        push_bool(&mut encoded, self.reply_requested);
        encoded
    }

    fn parse(bytes: &[u8]) -> Result<Self, CopyBothError> {
        require_exact_len(bytes, STANDBY_STATUS_UPDATE_LEN, STANDBY_STATUS_UPDATE_TAG)?;
        Ok(Self {
            write_lsn: Lsn(read_u64(bytes, 1)),
            flush_lsn: Lsn(read_u64(bytes, 9)),
            apply_lsn: Lsn(read_u64(bytes, 17)),
            client_time: read_i64(bytes, 25),
            reply_requested: read_bool(bytes, 33)?,
        })
    }
}

/// Errors returned when `CopyBoth` payloads cannot be parsed.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum CopyBothError {
    /// No message tag was present.
    #[error("CopyBoth message is empty")]
    EmptyMessage,

    /// The message tag is not one of the supported `PostgreSQL` replication tags.
    #[error("unknown CopyBoth message tag 0x{tag:02X}")]
    UnknownMessageTag {
        /// Raw tag byte.
        tag: u8,
    },

    /// The message did not contain enough bytes for its fixed fields.
    #[error("CopyBoth message tag 0x{tag:02X} is truncated: needed {needed} bytes, got {got}")]
    TruncatedMessage {
        /// Raw tag byte.
        tag: u8,
        /// Minimum bytes needed.
        needed: usize,
        /// Bytes available.
        got: usize,
    },

    /// A fixed-length message had trailing bytes.
    #[error(
        "CopyBoth message tag 0x{tag:02X} has invalid length: expected {expected} bytes, got {got}"
    )]
    InvalidLength {
        /// Raw tag byte.
        tag: u8,
        /// Expected exact byte count.
        expected: usize,
        /// Actual byte count.
        got: usize,
    },

    /// A protocol boolean was neither zero nor one.
    #[error("CopyBoth message has invalid boolean byte 0x{value:02X} at offset {offset}")]
    InvalidBoolean {
        /// Offset of the boolean byte.
        offset: usize,
        /// Raw boolean byte.
        value: u8,
    },
}

fn require_exact_or_more(bytes: &[u8], needed: usize, tag: u8) -> Result<(), CopyBothError> {
    if bytes.len() >= needed {
        return Ok(());
    }

    Err(CopyBothError::TruncatedMessage {
        tag,
        needed,
        got: bytes.len(),
    })
}

fn require_exact_len(bytes: &[u8], expected: usize, tag: u8) -> Result<(), CopyBothError> {
    require_exact_or_more(bytes, expected, tag)?;
    if bytes.len() == expected {
        return Ok(());
    }

    Err(CopyBothError::InvalidLength {
        tag,
        expected,
        got: bytes.len(),
    })
}

fn push_bool(bytes: &mut Vec<u8>, value: bool) {
    bytes.push(u8::from(value));
}

fn push_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_lsn(bytes: &mut Vec<u8>, lsn: Lsn) {
    bytes.extend_from_slice(&lsn.value().to_be_bytes());
}

fn read_bool(bytes: &[u8], offset: usize) -> Result<bool, CopyBothError> {
    match bytes[offset] {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(CopyBothError::InvalidBoolean { offset, value }),
    }
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(read_array(bytes, offset))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(read_array(bytes, offset))
}

fn read_array(bytes: &[u8], offset: usize) -> [u8; 8] {
    [
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ]
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn xlog_data_round_trips_through_copyboth_encoding() {
        let message = CopyBothMessage::XLogData(XLogData {
            wal_start: Lsn(11),
            wal_end: Lsn(15),
            send_time: 123_456,
            data: Bytes::from_static(b"wal"),
        });

        let encoded = message.encode();
        let decoded = CopyBothMessage::parse(&encoded);

        assert!(decoded == Ok(message));
    }

    #[test]
    fn primary_keepalive_round_trips_through_copyboth_encoding() {
        let message = CopyBothMessage::PrimaryKeepalive(PrimaryKeepalive {
            wal_end: Lsn(15),
            send_time: 123_456,
            reply_requested: true,
        });

        let encoded = message.encode();
        let decoded = CopyBothMessage::parse(&encoded);

        assert!(decoded == Ok(message));
    }

    #[test]
    fn standby_status_update_round_trips_through_copyboth_encoding() {
        let message = CopyBothMessage::StandbyStatusUpdate(StandbyStatusUpdate {
            write_lsn: Lsn(11),
            flush_lsn: Lsn(12),
            apply_lsn: Lsn(13),
            client_time: 123_456,
            reply_requested: false,
        });

        let encoded = message.encode();
        let decoded = CopyBothMessage::parse(&encoded);

        assert!(decoded == Ok(message));
    }
}

//! The WAL-only cross-node `NOTIFY` record.
//!
//! The coordinator appends a `NOTIFY` to the range-0 WAL as one
//! [`WriteOp::Put`] per notification, keyed by [`key::notify_key`]. Followers
//! observe the frame as it streams past and re-inject the notifications into
//! their local bus. **No apply site ever writes the record to the KV**,
//! because range-0 checkpoints snapshot the whole KV and the WAL topic never
//! expires by time. [`is_notify_op`] is that filter, and every apply site
//! calls it.
//!
//! Layout (all integers big-endian):
//! `[version: u8][olen: u32][origin][process_id: i32][clen: u32][channel]`
//! `[plen: u32][payload]`. Strings are UTF-8. The decoder bounds-checks every
//! length against the remaining buffer before it uses that length, so a
//! corrupt record read off the log fails to decode. It does not panic and it
//! does not over-allocate.

use crate::{KvError, WriteOp, key};

/// Current (only) notify-record format version.
pub const NOTIFY_RECORD_VERSION: u8 = 1;

/// One cross-node notification as it travels through the range-0 WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyRecord {
    /// Per-node identity of the publisher. It is a cheap dedup safety net, so
    /// a node that observes its own record can ignore it.
    pub origin: String,
    /// The *originating* backend pid. `PostgreSQL` reports the notifying
    /// backend's pid to every listener, so it must survive the hop. The
    /// receiving node must not restamp it.
    pub process_id: i32,
    /// Channel name (`NOTIFY <channel>`).
    pub channel: String,
    /// Notification payload; empty when `NOTIFY` carried none.
    pub payload: String,
}

impl NotifyRecord {
    /// Serializes to the record byte layout.
    ///
    /// # Panics
    ///
    /// Panics when a field exceeds the format's 4 GiB length limit. Callers
    /// validate channel and payload against `PostgreSQL`'s much smaller limits
    /// before publishing.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(13 + self.origin.len() + self.channel.len() + self.payload.len());
        out.push(NOTIFY_RECORD_VERSION);
        push_chunk(&mut out, self.origin.as_bytes());
        out.extend_from_slice(&self.process_id.to_be_bytes());
        push_chunk(&mut out, self.channel.as_bytes());
        push_chunk(&mut out, self.payload.as_bytes());
        out
    }

    /// Parses a record. The decoder validates every length before use.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::CorruptRow`] for an unknown version, a truncated or
    /// over-long length prefix, non-UTF-8 text, or trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, KvError> {
        let mut reader = Reader { bytes, at: 0 };
        let version = reader.u8()?;
        if version != NOTIFY_RECORD_VERSION {
            return Err(KvError::CorruptRow(format!(
                "unknown notify record version {version}"
            )));
        }

        let origin = reader.text("origin")?;
        let process_id = i32::from_be_bytes(reader.four()?);
        let channel = reader.text("channel")?;
        let payload = reader.text("payload")?;
        if reader.at != bytes.len() {
            return Err(KvError::CorruptRow(
                "trailing bytes after notify record".into(),
            ));
        }

        Ok(Self {
            origin,
            process_id,
            channel,
            payload,
        })
    }
}

/// True when `op` touches the WAL-only notify keyspace.
///
/// This is the never-persist predicate. Every site that applies WAL ops to a
/// KV must drop the ops that this predicate selects. If it does not, a
/// notification becomes permanent catalog state through the next checkpoint.
/// The predicate matches the whole `/0/notify/` namespace, not just
/// well-formed keys, so nothing under it can ever land in the store.
#[must_use]
pub fn is_notify_op(op: &WriteOp) -> bool {
    let op_key = match op {
        WriteOp::Put { key, .. }
        | WriteOp::ConditionalPut { key, .. }
        | WriteOp::Delete { key } => key,
    };
    key::is_notify_key(op_key)
}

fn push_chunk(out: &mut Vec<u8>, chunk: &[u8]) {
    out.extend_from_slice(
        &u32::try_from(chunk.len())
            .expect("notify record field fits u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(chunk);
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], KvError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| KvError::CorruptRow("truncated notify record".into()))?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, KvError> {
        Ok(self.take(1)?[0])
    }

    /// Reads four bytes as a fixed array. The read is fallible, so nothing in
    /// the decoder can panic on input from the log.
    fn four(&mut self) -> Result<[u8; 4], KvError> {
        self.take(4)?
            .try_into()
            .map_err(|_| KvError::CorruptRow("truncated notify record".into()))
    }

    fn u32(&mut self) -> Result<u32, KvError> {
        Ok(u32::from_be_bytes(self.four()?))
    }

    fn text(&mut self, field: &str) -> Result<String, KvError> {
        let len = usize::try_from(self.u32()?)
            .map_err(|_| KvError::CorruptRow(format!("notify {field} length exceeds usize")))?;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| KvError::CorruptRow(format!("notify {field} is not UTF-8")))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// `PostgreSQL`'s channel-name limit (`NAMEDATALEN - 1`). It is mirrored
    /// here so the tests exercise the codec at the largest value a publisher
    /// can produce.
    const MAX_CHANNEL_BYTES: usize = 63;
    /// `PostgreSQL`'s payload limit (`NOTIFY_PAYLOAD_MAX_LENGTH - 1`).
    /// `PostgreSQL` rejects 8000, so 7999 is the largest a publisher can
    /// produce.
    const MAX_PAYLOAD_BYTES: usize = 7999;

    fn sample() -> NotifyRecord {
        NotifyRecord {
            origin: "node-a:7433".into(),
            process_id: 4242,
            channel: "chan".into(),
            payload: "hello".into(),
        }
    }

    #[test]
    fn records_round_trip() {
        let cases = [
            ("ordinary", sample()),
            (
                "empty payload",
                NotifyRecord {
                    payload: String::new(),
                    ..sample()
                },
            ),
            (
                "empty origin",
                NotifyRecord {
                    origin: String::new(),
                    ..sample()
                },
            ),
            (
                "maximum channel and payload",
                NotifyRecord {
                    channel: "c".repeat(MAX_CHANNEL_BYTES),
                    payload: "x".repeat(MAX_PAYLOAD_BYTES),
                    ..sample()
                },
            ),
            (
                "non-ascii utf-8",
                NotifyRecord {
                    origin: "nœud-α".into(),
                    channel: "канал".into(),
                    payload: "日本語 🐿 payload".into(),
                    ..sample()
                },
            ),
            (
                "negative process id",
                NotifyRecord {
                    process_id: -1,
                    ..sample()
                },
            ),
            (
                "extreme process ids",
                NotifyRecord {
                    process_id: i32::MIN,
                    ..sample()
                },
            ),
            (
                "maximum process id",
                NotifyRecord {
                    process_id: i32::MAX,
                    ..sample()
                },
            ),
        ];

        for (name, record) in cases {
            let decoded = NotifyRecord::decode(&record.encode()).expect("decode");
            assert!(decoded == record, "{name}");
        }
    }

    #[test]
    fn distinct_records_do_not_share_an_encoding() {
        let base = sample().encode();

        for other in [
            NotifyRecord {
                origin: "node-b:7433".into(),
                ..sample()
            },
            NotifyRecord {
                process_id: 4243,
                ..sample()
            },
            NotifyRecord {
                channel: "other".into(),
                ..sample()
            },
            NotifyRecord {
                payload: "goodbye".into(),
                ..sample()
            },
            // Field boundaries are explicit: shifting a byte across them changes
            // the encoding rather than aliasing onto the same bytes.
            NotifyRecord {
                channel: "cha".into(),
                payload: "nhello".into(),
                ..sample()
            },
        ] {
            assert!(other.encode() != base, "{other:?}");
        }
    }

    #[test]
    fn corrupt_input_is_rejected_without_panicking() {
        let valid = sample().encode();
        let truncations =
            (0..valid.len()).map(|n| (format!("truncated to {n}"), valid[..n].to_vec()));

        let mut bad_version = valid.clone();
        bad_version[0] = 2;

        let mut huge_origin_len = valid.clone();
        huge_origin_len[1..5].copy_from_slice(&u32::MAX.to_be_bytes());

        let mut huge_channel_len = valid.clone();
        let channel_len_at = 1 + 4 + sample().origin.len() + 4;
        huge_channel_len[channel_len_at..channel_len_at + 4]
            .copy_from_slice(&u32::MAX.to_be_bytes());

        let mut invalid_utf8 = valid.clone();
        let origin_at = 5;
        invalid_utf8[origin_at] = 0xFF;

        let mut trailing = valid.clone();
        trailing.push(0);

        let mut overlapping_origin_len = valid.clone();
        overlapping_origin_len[1..5].copy_from_slice(
            &u32::try_from(valid.len())
                .expect("length fits u32")
                .to_be_bytes(),
        );

        let named = [
            ("empty".to_string(), Vec::new()),
            ("unknown version".to_string(), bad_version),
            ("over-long origin length".to_string(), huge_origin_len),
            ("over-long channel length".to_string(), huge_channel_len),
            ("non-utf8 origin".to_string(), invalid_utf8),
            ("trailing bytes".to_string(), trailing),
            (
                "origin length swallows the record".to_string(),
                overlapping_origin_len,
            ),
        ];

        for (name, bytes) in named.into_iter().chain(truncations) {
            let error = NotifyRecord::decode(&bytes).expect_err(&name);
            assert!(matches!(error, KvError::CorruptRow(_)), "{name}");
        }
    }

    #[test]
    fn notify_ops_are_recognised_in_every_write_shape() {
        let notify = key::notify_key(9);
        let ordinary = key::row_key(7, 9);

        let cases = [
            (
                true,
                WriteOp::Put {
                    key: notify.clone(),
                    value: sample().encode(),
                },
            ),
            (
                true,
                WriteOp::Delete {
                    key: notify.clone(),
                },
            ),
            (
                true,
                WriteOp::ConditionalPut {
                    key: notify,
                    expected: None,
                    value: sample().encode(),
                },
            ),
            (
                false,
                WriteOp::Put {
                    key: ordinary.clone(),
                    value: b"row".to_vec(),
                },
            ),
            (false, WriteOp::Delete { key: ordinary }),
            (
                false,
                WriteOp::Put {
                    key: key::clog_key(3),
                    value: b"c".to_vec(),
                },
            ),
        ];

        for (expected, op) in cases {
            assert!(is_notify_op(&op) == expected, "{op:?}");
        }
    }
}

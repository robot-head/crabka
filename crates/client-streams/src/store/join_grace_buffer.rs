//! Stream-side buffer for the KIP-923 stream–table join grace period. Holds
//! incoming stream records `(key, value)` keyed by `(timestamp, seqnum)` so
//! out-of-order records buffer and `drain_due` returns them in ascending
//! `(ts, seq)` order once stream-time passes `ts + grace`. Keeps EVERY record —
//! a stream is not a changelog (two records with the same key at different
//! timestamps must BOTH buffer; there is no replace-by-key).
//!
//! Changelog format (Crabka-internal — there is no JVM byte-exact golden for this
//! buffer, so it only needs to be self-consistent for Crabka's own clean-slate
//! restore):
//! - Changelog KEY  = `(ts, seq)` encoded big-endian (8 bytes ts ‖ 4 bytes seq).
//! - Changelog VALUE = `(key bytes, value bytes)` length-prefixed, or `None`
//!   (a tombstone logged when an entry is drained).
use std::{any::Any, collections::BTreeMap};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{processor::serde::Serde, store::api::StateStore};

/// Encode a `(ts, seq)` buffer id as the changelog KEY: 8-byte big-endian `ts`
/// followed by 4-byte big-endian `seq` (so byte order matches `(ts, seq)` order).
fn encode_id(id: (i64, u32)) -> Bytes {
    let mut buf = Vec::with_capacity(12);
    buf.extend_from_slice(&id.0.to_be_bytes());
    buf.extend_from_slice(&id.1.to_be_bytes());
    Bytes::from(buf)
}

/// Decode a changelog KEY back into `(ts, seq)`.
fn decode_id(bytes: &[u8]) -> (i64, u32) {
    let ts = i64::from_be_bytes(bytes[0..8].try_into().expect("8-byte ts prefix"));
    let seq = u32::from_be_bytes(bytes[8..12].try_into().expect("4-byte seq suffix"));
    (ts, seq)
}

/// Encode `(key bytes, value bytes)` as the changelog VALUE: a 4-byte big-endian
/// key length, the key bytes, then the value bytes.
fn encode_payload(kb: &[u8], vb: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(4 + kb.len() + vb.len());
    buf.extend_from_slice(
        &u32::try_from(kb.len())
            .expect("key len fits u32")
            .to_be_bytes(),
    );
    buf.extend_from_slice(kb);
    buf.extend_from_slice(vb);
    Bytes::from(buf)
}

/// Decode a changelog VALUE back into `(key bytes, value bytes)`.
fn decode_payload(bytes: &[u8]) -> (Bytes, Bytes) {
    let klen = u32::from_be_bytes(bytes[0..4].try_into().expect("4-byte key len")) as usize;
    let kb = Bytes::copy_from_slice(&bytes[4..4 + klen]);
    let vb = Bytes::copy_from_slice(&bytes[4 + klen..]);
    (kb, vb)
}

/// Typed stream-side grace buffer. `put` appends `(key, value)` at `ts` under a
/// fresh `(ts, seq)` id; `drain_due(threshold)` pops every entry with
/// `ts <= threshold` in ascending `(ts, seq)` order (logging a tombstone per
/// popped entry).
pub struct JoinGraceBufferStore<K, V> {
    name: String,
    changelog_topic: String,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    /// `(ts, seq)` -> `(key bytes, value bytes)`; `seq` disambiguates equal ts and
    /// preserves insertion order among records with the same timestamp.
    buffer: BTreeMap<(i64, u32), (Bytes, Bytes)>,
    seq: u32,
    /// Append-only changelog: `Some` = buffered, `None` = drained tombstone.
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> JoinGraceBufferStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            key_serde,
            value_serde,
            buffer: BTreeMap::new(),
            seq: 0,
            changelog: Vec::new(),
            logging: true,
        }
    }

    /// The public ctor used by tests + the DSL. The grace buffer has no pluggable
    /// backend, so `in_memory` and `new` are the same body.
    #[must_use]
    #[allow(dead_code)] // test-only ctor
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(name, key_serde, value_serde, changelog_topic)
    }

    /// Buffer a stream record `(key, value)` at `ts`. Always appends a NEW entry
    /// (no replace-by-key): a stream is not a changelog.
    // async to mirror the suppress store's `put` (trait-async there); the
    // grace-flush processor awaits it like every other store op.
    #[allow(clippy::unused_async)]
    pub async fn put(&mut self, key: K, value: V, ts: i64) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let vb = self.value_serde.serialize(&self.changelog_topic, &value);
        let id = (ts, self.seq);
        self.seq = self.seq.wrapping_add(1);
        if self.logging {
            self.changelog
                .push((encode_id(id), Some(encode_payload(&kb, &vb))));
        }
        self.buffer.insert(id, (kb, vb));
    }

    /// Pop every entry with `ts <= threshold`, in ascending `(ts, seq)` order, as
    /// `(key, value, ts)`. Logs a tombstone per popped entry (if logging).
    #[allow(clippy::unused_async)]
    pub async fn drain_due(&mut self, threshold: i64) -> Vec<(K, V, i64)> {
        let ids: Vec<(i64, u32)> = self
            .buffer
            .range(..=(threshold, u32::MAX))
            .map(|(k, _)| *k)
            .collect();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let (kb, vb) = self.buffer.remove(&id).expect("present");
            if self.logging {
                self.changelog.push((encode_id(id), None));
            }
            let key = self
                .key_serde
                .deserialize(&self.changelog_topic, &kb)
                .expect("grace buffer key deserialize");
            let value = self
                .value_serde
                .deserialize(&self.changelog_topic, &vb)
                .expect("grace buffer value deserialize");
            out.push((key, value, id.0));
        }
        out
    }

    /// Number of buffered records.
    #[allow(dead_code)] // test-only accessor
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Paired with [`len`](Self::len) for `clippy::len_without_is_empty`.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[async_trait]
impl<K: 'static, V: 'static> StateStore for JoinGraceBufferStore<K, V> {
    fn name(&self) -> &str {
        &self.name
    }
    async fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn changelog_topic(&self) -> &str {
        &self.changelog_topic
    }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> {
        std::mem::take(&mut self.changelog)
    }
    async fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        // Restore is silent: rebuild the buffer WITHOUT pushing to `self.changelog`.
        let id = decode_id(&key);
        // Keep `seq` ahead of every restored id so post-restore `put`s never
        // collide with a recovered slot at the same timestamp.
        self.seq = self.seq.max(id.1.wrapping_add(1));
        match value {
            Some(v) => {
                let (kb, vb) = decode_payload(&v);
                self.buffer.insert(id, (kb, vb));
            }
            None => {
                self.buffer.remove(&id);
            }
        }
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
    async fn clear(&mut self) {
        // No pluggable backend: wipe the in-memory buffer + the changelog buffer
        // (re-restore replays the committed log).
        self.buffer.clear();
        self.seq = 0;
        self.changelog.clear();
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    #[tokio::test]
    async fn buffers_and_drains_in_ts_order() {
        let mut s = JoinGraceBufferStore::<String, i64>::in_memory(
            "jb".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "jb-cl".into(),
        );
        s.put("a".into(), 2, 200).await;
        s.put("a".into(), 1, 100).await; // out-of-order, same key
        s.put("b".into(), 3, 150).await;
        let due = s.drain_due(150).await; // everything ts <= 150, ascending (ts, seq)
        assert_eq!(due, vec![("a".into(), 1, 100), ("b".into(), 3, 150)]);
        let rest = s.drain_due(i64::MAX).await; // remaining ts=200
        assert_eq!(rest, vec![("a".into(), 2, 200)]);
    }

    #[tokio::test]
    async fn keeps_every_record_no_replace_by_key() {
        let mut s = JoinGraceBufferStore::<String, i64>::in_memory(
            "jb".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "jb-cl".into(),
        );
        // Same key, same ts, different values — both must buffer.
        s.put("k".into(), 1, 100).await;
        s.put("k".into(), 2, 100).await;
        check!(s.len() == 2);
        let due = s.drain_due(100).await;
        check!(due == vec![("k".into(), 1, 100), ("k".into(), 2, 100)]);
        check!(s.is_empty());
    }

    #[tokio::test]
    async fn changelog_records_puts_then_tombstones_on_drain() {
        let mut s = JoinGraceBufferStore::<String, i64>::in_memory(
            "jb".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "jb-cl".into(),
        );
        s.put("a".into(), 1, 100).await;
        s.put("b".into(), 2, 200).await;
        let cl = s.take_changelog();
        check!(cl.len() == 2);
        check!(cl.iter().all(|(_, v)| v.is_some()));
        let _ = s.drain_due(100).await;
        let cl = s.take_changelog();
        check!(cl.len() == 1);
        check!(cl[0].1.is_none());
    }

    #[tokio::test]
    async fn apply_changelog_round_trips_then_tombstone_removes() {
        // Source store buffers two records, out-of-order.
        let mut src = JoinGraceBufferStore::<String, i64>::in_memory(
            "jb".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "jb-cl".into(),
        );
        src.put("a".into(), 9, 200).await;
        src.put("b".into(), 7, 100).await;
        let cl = src.take_changelog();
        check!(cl.len() == 2);

        // Restore into a FRESH store — apply_changelog must be silent.
        let mut dst = JoinGraceBufferStore::<String, i64>::in_memory(
            "jb".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "jb-cl".into(),
        );
        for (k, v) in cl {
            dst.apply_changelog(k, v).await;
        }
        check!(dst.len() == 2);
        check!(dst.take_changelog().is_empty());
        // Drains in restored (ts, seq) order: b@100 then a@200.
        let out = dst.drain_due(i64::MAX).await;
        check!(out == vec![("b".into(), 7, 100), ("a".into(), 9, 200)]);
    }
}

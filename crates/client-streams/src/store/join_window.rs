//! The retainDuplicates window store for stream-stream joins.
//!
//! It uses composite `WindowKeySchema` keys with a per-store incrementing
//! seqnum, and it stores RAW values with no `ValueAndTimestamp` wrap. `fetch`
//! returns every duplicate in a range.
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    processor::serde::Serde,
    store::{
        api::StateStore,
        byte::{ByteKeyValueStore, InMemoryBytes},
        window_schema::{key_bytes_of, store_key, window_start_of},
    },
};

/// A typed windowed store that retains duplicates.
///
/// Every `put` with the same `(key, timestamp)` builds a distinct composite key
/// from an incrementing seqnum, so the duplicates coexist in the backend. The
/// store keeps the values RAW, with no `ValueAndTimestamp` wrap. The
/// stream-stream join processors use it.
#[async_trait]
pub trait JoinWindowStore<K: Send + Sync, V: Send>: StateStore {
    async fn put(&mut self, key: K, timestamp: i64, value: V);
    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)>;
}

pub struct JoinWindowBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
    seqnum: u32,
}

impl<K: 'static, V: 'static> JoinWindowBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        backend: Box<dyn ByteKeyValueStore>,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            backend,
            key_serde,
            value_serde,
            changelog: Vec::new(),
            logging: true,
            seqnum: 0,
        }
    }

    #[must_use]
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(
            name,
            Box::new(InMemoryBytes::default()),
            key_serde,
            value_serde,
            changelog_topic,
        )
    }

    fn next_seqnum(&mut self) -> u32 {
        let s = self.seqnum;
        self.seqnum = (self.seqnum + 1) & 0x7FFF_FFFF;
        s
    }
}

#[async_trait]
impl<K: 'static, V: 'static> StateStore for JoinWindowBytesStore<K, V> {
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
        match value {
            Some(v) => self.backend.put(key, v).await,
            None => {
                self.backend.delete(&key).await;
            }
        }
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
    async fn clear(&mut self) {
        self.backend.clear().await;
        self.changelog.clear();
        self.seqnum = 0;
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> JoinWindowStore<K, V>
    for JoinWindowBytesStore<K, V>
{
    async fn put(&mut self, key: K, timestamp: i64, value: V) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let seq = self.next_seqnum();
        let sk = store_key(&kb, timestamp, seq);
        let raw = self.value_serde.serialize(&self.changelog_topic, &value); // RAW — no ValueAndTimestamp wrap
        self.backend.put(sk.clone(), raw.clone()).await;
        if self.logging {
            self.changelog.push((sk, Some(raw)));
        }
    }

    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        // Clamp to 0: negative timestamps encode to 0xFF… in BE, which sorts
        // after all positive entries in the lexicographic BTreeMap backend.
        // Kafka's segment-scoped range fetch does the same (`Math.max(0, timeFrom)`).
        let lo = store_key(&kb, time_from.max(0), 0);
        let hi = store_key(&kb, time_to.saturating_add(1), 0);
        let mut out = Vec::new();
        for (k, raw) in self.backend.range(&lo, &hi).await {
            // guard prefix collisions: only return entries whose inner key matches
            if key_bytes_of(&k) != kb.as_ref() {
                continue;
            }
            out.push((
                window_start_of(&k),
                self.value_serde
                    .deserialize(&self.changelog_topic, &raw)
                    .expect("join window value deserialize"),
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::StringSerde;

    #[tokio::test]
    async fn put_keeps_duplicates_and_fetch_returns_all() {
        let mut s = JoinWindowBytesStore::<String, String>::in_memory(
            "j".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "app-j-changelog".into(),
        );
        s.put("k".into(), 5, "a".into()).await;
        s.put("k".into(), 5, "b".into()).await; // SAME (key, ts) → kept (seqnum increments)
        s.put("k".into(), 7, "c".into()).await;
        assert_eq!(
            s.fetch(&"k".to_string(), 5, 7).await,
            vec![
                (5, "a".to_string()),
                (5, "b".to_string()),
                (7, "c".to_string())
            ]
        );
        assert_eq!(
            s.fetch(&"k".to_string(), 5, 5).await,
            vec![(5, "a".to_string()), (5, "b".to_string())]
        );
        assert_eq!(s.take_changelog().len(), 3);
    }
}

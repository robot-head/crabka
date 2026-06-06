//! Session store over the byte backend — `SessionKeySchema` keys (`key‖end‖start`),
//! raw aggregate values. Third typed store beside `KeyValueBytesStore` and
//! `WindowBytesStore`. Supports the JVM session-merge fetch (`find_sessions`).
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::session_schema::{
    session_end_of, session_key, session_key_bytes_of, session_start_of,
};

/// Typed session store keyed by `(K, start, end)`. `find_sessions` returns the
/// merge candidates for a record: sessions whose `[start, end]` overlaps the
/// inactivity gap window `[earliest_end, latest_start]`.
#[async_trait]
pub trait SessionStore<K: Send + Sync, V: Send>: StateStore {
    /// Sessions for `key` with `end >= earliest_end && start <= latest_start`,
    /// returned as `(start, end, value)` in store order (end asc, then start asc).
    async fn find_sessions(
        &self,
        key: &K,
        earliest_end: i64,
        latest_start: i64,
    ) -> Vec<(i64, i64, V)>;
    async fn put(&mut self, key: K, start: i64, end: i64, value: V);
    async fn remove(&mut self, key: &K, start: i64, end: i64);
}

pub struct SessionBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> SessionBytesStore<K, V> {
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
}

#[async_trait]
impl<K: 'static, V: 'static> StateStore for SessionBytesStore<K, V> {
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
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> SessionStore<K, V> for SessionBytesStore<K, V> {
    async fn find_sessions(
        &self,
        key: &K,
        earliest_end: i64,
        latest_start: i64,
    ) -> Vec<(i64, i64, V)> {
        let kb = self.key_serde.serialize(key);
        // Lower bound: smallest qualifying end (clamped to 0 — stored ends are
        // non-negative epoch millis; a negative earliest_end means "all qualify").
        let lo = session_key(&kb, 0, earliest_end.max(0));
        // Upper bound: past every entry for this key prefix.
        let hi = session_key(&kb, i64::MAX, i64::MAX);
        let mut out = Vec::new();
        for (k, raw) in self.backend.range(&lo, &hi).await {
            if session_key_bytes_of(&k) != kb.as_ref() {
                continue; // guard prefix collisions with a different key
            }
            let end = session_end_of(&k);
            let start = session_start_of(&k);
            if end >= earliest_end && start <= latest_start {
                out.push((
                    start,
                    end,
                    self.value_serde
                        .deserialize(&raw)
                        .expect("session value deserialize"),
                ));
            }
        }
        out
    }

    async fn put(&mut self, key: K, start: i64, end: i64, value: V) {
        let kb = self.key_serde.serialize(&key);
        let sk = session_key(&kb, start, end);
        let raw = self.value_serde.serialize(&value);
        self.backend.put(sk.clone(), raw.clone()).await;
        if self.logging {
            self.changelog.push((sk, Some(raw)));
        }
    }

    async fn remove(&mut self, key: &K, start: i64, end: i64) {
        let kb = self.key_serde.serialize(key);
        let sk = session_key(&kb, start, end);
        self.backend.delete(&sk).await;
        if self.logging {
            self.changelog.push((sk, None));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    fn store() -> SessionBytesStore<String, i64> {
        SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-s-changelog".into(),
        )
    }

    #[tokio::test]
    async fn put_find_remove_and_changelog() {
        let mut s = store();
        s.put("k".to_string(), 0, 10, 1).await; // session [0,10]
        s.put("k".to_string(), 50, 60, 2).await; // session [50,60]
        // gap=20 around ts=15 → earliest_end=15-20=-5, latest_start=15+20=35 →
        // only [0,10] qualifies (end 10 >= -5, start 0 <= 35); [50,60] start 50 > 35.
        let found = s.find_sessions(&"k".to_string(), -5, 35).await;
        assert_eq!(found, vec![(0, 10, 1)]);
        // remove [0,10]
        s.remove(&"k".to_string(), 0, 10).await;
        assert_eq!(s.find_sessions(&"k".to_string(), -5, 35).await, vec![]);
        // changelog: put, put, remove → 3 entries (last is a tombstone)
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 3);
        assert!(cl[2].1.is_none());
    }

    #[tokio::test]
    async fn find_sessions_returns_store_order_end_then_start() {
        let mut s = store();
        s.put("k".to_string(), 0, 30, 1).await;
        s.put("k".to_string(), 0, 10, 2).await;
        // both qualify for earliest_end=0, latest_start=100; store order = end asc.
        let found = s.find_sessions(&"k".to_string(), 0, 100).await;
        assert_eq!(found, vec![(0, 10, 2), (0, 30, 1)]);
    }

    #[tokio::test]
    async fn other_key_prefix_is_not_returned() {
        let mut s = store();
        s.put("k".to_string(), 0, 10, 1).await;
        s.put("kk".to_string(), 0, 10, 9).await; // longer key sharing the "k" prefix
        let found = s.find_sessions(&"k".to_string(), 0, 100).await;
        assert_eq!(found, vec![(0, 10, 1)]);
    }

    #[tokio::test]
    async fn restore_via_changelog_rebuilds_sessions() {
        let mut s = store();
        s.put("k".to_string(), 0, 10, 1).await;
        s.put("k".to_string(), 50, 60, 2).await;
        s.remove(&"k".to_string(), 0, 10).await; // a tombstone in the changelog
        let cl = s.take_changelog();
        // Clean-slate restore: replay the changelog into a fresh store.
        let mut s2 = store();
        for (k, v) in cl {
            s2.apply_changelog(k, v).await;
        }
        // [0,10] was removed; only [50,60] survives.
        assert_eq!(
            s2.find_sessions(&"k".to_string(), 0, 100).await,
            vec![(50, 60, 2)]
        );
    }
}

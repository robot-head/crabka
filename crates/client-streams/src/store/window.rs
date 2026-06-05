//! Window store over the byte backend: composite `WindowKeySchema` keys +
//! `ValueAndTimestamp` values. A second typed store beside `KeyValueBytesStore`.
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::window_schema::{
    key_bytes_of, store_key, unwrap_value, window_start_of, wrap_value,
};

/// Typed windowed store keyed by `(K, windowStart)`, holding `V` + a record
/// timestamp. `fetch_single` returns `(storedTs, V)` so the aggregator can compute
/// `newTs = max(recordTs, storedTs)`.
#[async_trait]
pub trait WindowStore<K: Send + Sync, V: Send>: StateStore {
    async fn fetch_single(&self, key: &K, window_start: i64) -> Option<(i64, V)>;
    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)>;
    async fn put(&mut self, key: K, window_start: i64, value: V, record_ts: i64);
}

pub struct WindowBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> WindowBytesStore<K, V> {
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
impl<K: 'static, V: 'static> StateStore for WindowBytesStore<K, V> {
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
impl<K: Send + Sync + 'static, V: Send + 'static> WindowStore<K, V> for WindowBytesStore<K, V> {
    async fn fetch_single(&self, key: &K, window_start: i64) -> Option<(i64, V)> {
        let kb = self.key_serde.serialize(key);
        let sk = store_key(&kb, window_start);
        let wrapped = self.backend.get(&sk).await?;
        let (ts, raw) = unwrap_value(&wrapped);
        Some((
            ts,
            self.value_serde
                .deserialize(raw)
                .expect("window value deserialize"),
        ))
    }

    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)> {
        let kb = self.key_serde.serialize(key);
        let lo = store_key(&kb, time_from);
        let hi = store_key(&kb, time_to.saturating_add(1));
        let mut out = Vec::new();
        for (k, wrapped) in self.backend.range(&lo, &hi).await {
            // guard prefix collisions: only return entries whose inner key matches
            if key_bytes_of(&k) != kb.as_ref() {
                continue;
            }
            let (_ts, raw) = unwrap_value(&wrapped);
            out.push((
                window_start_of(&k),
                self.value_serde
                    .deserialize(raw)
                    .expect("window value deserialize"),
            ));
        }
        out
    }

    async fn put(&mut self, key: K, window_start: i64, value: V, record_ts: i64) {
        let kb = self.key_serde.serialize(&key);
        let sk = store_key(&kb, window_start);
        let raw = self.value_serde.serialize(&value);
        let wrapped = wrap_value(record_ts, &raw);
        self.backend.put(sk.clone(), wrapped.clone()).await;
        if self.logging {
            self.changelog.push((sk, Some(wrapped)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    #[tokio::test]
    async fn put_fetch_single_and_range() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
        );
        s.put("k".to_string(), 0, 1, 5).await;
        s.put("k".to_string(), 0, 2, 7).await;
        s.put("k".to_string(), 10, 9, 11).await;
        assert_eq!(s.fetch_single(&"k".to_string(), 0).await, Some((7, 2)));
        assert_eq!(s.fetch_single(&"k".to_string(), 10).await, Some((11, 9)));
        assert_eq!(s.fetch_single(&"k".to_string(), 99).await, None);
        assert_eq!(
            s.fetch(&"k".to_string(), 0, 10).await,
            vec![(0, 2), (10, 9)]
        );
        assert_eq!(s.take_changelog().len(), 3);
    }
}

//! Versioned key-value store (KIP-889). Each key maps to a chain of versions
//! keyed by `validFrom` (epoch millis); a version's value is `Some` (live) or
//! `None` (tombstone). `get` returns the latest; `get_as_of(t)` returns the
//! version valid at `t`. History older than `history_retention` (relative to the
//! max observed timestamp) is expired. Changelog VALUE = `validFrom:8B ‖ value`
//! (`versioned_schema`); the raw key is the changelog key (JVM-exact).
use std::any::Any;
use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;
use crate::store::versioned_schema::{unwrap_versioned, wrap_versioned};

/// A single resolved version: its value, the timestamp it became valid, and the
/// timestamp the next version superseded it (`None` = still the latest, ∞).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedRecord<V> {
    pub value: V,
    pub valid_from: i64,
    pub valid_to: Option<i64>,
}

/// Typed versioned store surface.
#[async_trait]
pub trait VersionedKeyValueStore<K: Send + Sync, V: Send>: StateStore {
    /// Insert a version at `validFrom = timestamp`. `value == None` is a
    /// tombstone version. Out-of-order timestamps are inserted mid-chain; the
    /// latest pointer only advances when `timestamp >=` the current max.
    /// A timestamp older than the retention horizon is dropped.
    async fn put(&mut self, key: K, value: Option<V>, timestamp: i64);
    /// Insert a tombstone version at `timestamp`; returns the record that was
    /// valid at `timestamp` immediately before the delete (if any live value).
    async fn delete(&mut self, key: &K, timestamp: i64) -> Option<VersionedRecord<V>>;
    /// The latest live version, or `None` if absent / latest is a tombstone.
    async fn get(&self, key: &K) -> Option<VersionedRecord<V>>;
    /// The version valid at `as_of`, or `None` if absent / that version is a
    /// tombstone / `as_of` predates the oldest retained version.
    async fn get_as_of(&self, key: &K, as_of: i64) -> Option<VersionedRecord<V>>;
}

pub struct VersionedBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    history_retention_ms: i64,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    // key bytes -> (validFrom -> Some(value bytes) | None tombstone)
    chains: BTreeMap<Bytes, BTreeMap<i64, Option<Bytes>>>,
    observed_stream_time: i64,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> VersionedBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        history_retention_ms: i64,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            history_retention_ms,
            key_serde,
            value_serde,
            chains: BTreeMap::new(),
            observed_stream_time: i64::MIN,
            changelog: Vec::new(),
            logging: true,
        }
    }

    #[must_use]
    pub fn in_memory(
        name: String,
        history_retention_ms: i64,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(
            name,
            history_retention_ms,
            key_serde,
            value_serde,
            changelog_topic,
        )
    }

    /// The retention horizon: versions whose `valid_to` is strictly below this
    /// are unreachable and may be evicted; a put below it is dropped.
    fn horizon(&self) -> i64 {
        self.observed_stream_time
            .saturating_sub(self.history_retention_ms)
    }

    /// Insert raw (already-serialized) version bytes into a chain, applying
    /// retention. Shared by `put`/`delete` (logging on) and restore (logging off).
    fn insert_raw(&mut self, key: Bytes, valid_from: i64, value: Option<Bytes>) -> bool {
        self.observed_stream_time = self.observed_stream_time.max(valid_from);
        // Compute horizon after updating stream time (horizon depends on it).
        let h = self.horizon();
        // KIP-889 out-of-bounds drop: if an existing later version already
        // supersedes this one entirely below the horizon, it is not retained.
        // Checked before `entry()` so a dropped put for a brand-new key does not
        // leave an orphan empty chain.
        if let Some(chain) = self.chains.get(&key)
            && let Some(next_above) = chain.range((valid_from + 1)..).next().map(|(t, _)| *t)
            && next_above <= h
        {
            return false;
        }
        let chain = self.chains.entry(key).or_default();
        chain.insert(valid_from, value);
        // Evict versions whose successor is at/below the horizon (their validTo
        // makes them unreachable). The latest version (no successor) is never a
        // left edge here, so the chain always retains at least the new version.
        let times: Vec<i64> = chain.keys().copied().collect();
        for w in times.windows(2) {
            let (from, next) = (w[0], w[1]);
            if next <= h {
                chain.remove(&from);
            }
        }
        true
    }
}

#[async_trait]
impl<K: Send + 'static, V: Send + 'static> StateStore for VersionedBytesStore<K, V> {
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
        // A bare null changelog value cannot carry a timestamp; with the
        // value-packed encoding it never occurs. Ignore it defensively and
        // reconstruct the version chain from each `ts ‖ value` entry.
        if let Some(wrapped) = value {
            let (ts, inner) = unwrap_versioned(&wrapped);
            self.insert_raw(key, ts, inner);
        }
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
    fn as_iq(&self) -> Option<&dyn crate::store::iq::IqQueryable> {
        Some(self)
    }
    async fn clear(&mut self) {
        self.chains.clear();
        self.changelog.clear();
        self.observed_stream_time = i64::MIN;
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> VersionedKeyValueStore<K, V>
    for VersionedBytesStore<K, V>
{
    async fn put(&mut self, key: K, value: Option<V>, timestamp: i64) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let vb = value
            .as_ref()
            .map(|v| self.value_serde.serialize(&self.changelog_topic, v));
        let inserted = self.insert_raw(kb.clone(), timestamp, vb.clone());
        if inserted && self.logging {
            let wrapped = wrap_versioned(timestamp, vb.as_deref());
            self.changelog.push((kb, Some(wrapped)));
        }
    }

    async fn delete(&mut self, key: &K, timestamp: i64) -> Option<VersionedRecord<V>> {
        let prev = self.get_as_of(key, timestamp).await;
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let inserted = self.insert_raw(kb.clone(), timestamp, None);
        if inserted && self.logging {
            let wrapped = wrap_versioned(timestamp, None);
            self.changelog.push((kb, Some(wrapped)));
        }
        prev
    }

    async fn get(&self, key: &K) -> Option<VersionedRecord<V>> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let chain = self.chains.get(&kb)?;
        let (&valid_from, value) = chain.iter().next_back()?;
        let raw = value.as_ref()?; // latest is a tombstone => None
        Some(VersionedRecord {
            value: self
                .value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("versioned value deserialize"),
            valid_from,
            valid_to: None,
        })
    }

    async fn get_as_of(&self, key: &K, as_of: i64) -> Option<VersionedRecord<V>> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let chain = self.chains.get(&kb)?;
        let (&valid_from, value) = chain.range(..=as_of).next_back()?;
        let raw = value.as_ref()?; // that version is a tombstone => None
        let valid_to = chain.range((as_of + 1)..).next().map(|(t, _)| *t);
        Some(VersionedRecord {
            value: self
                .value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("versioned value deserialize"),
            valid_from,
            valid_to,
        })
    }
}

// Holds only `Box<dyn Serde<_>>` + byte buffers → `Send + Sync` for any K/V.
#[async_trait]
impl<K: 'static, V: 'static> crate::store::iq::IqQueryable for VersionedBytesStore<K, V> {
    fn kind(&self) -> crate::store::iq::StoreKind {
        crate::store::iq::StoreKind::Versioned
    }
    async fn iq_versioned_get(&self, key: &[u8]) -> Option<(i64, Option<i64>, Bytes)> {
        let chain = self.chains.get(key)?;
        let (&vf, value) = chain.iter().next_back()?;
        let raw = value.as_ref()?;
        Some((vf, None, raw.clone()))
    }
    async fn iq_versioned_get_as_of(
        &self,
        key: &[u8],
        as_of: i64,
    ) -> Option<(i64, Option<i64>, Bytes)> {
        let chain = self.chains.get(key)?;
        let (&vf, value) = chain.range(..=as_of).next_back()?;
        let raw = value.as_ref()?;
        let vt = chain.range((as_of + 1)..).next().map(|(t, _)| *t);
        Some((vf, vt, raw.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    fn store(retention: i64) -> VersionedBytesStore<String, i64> {
        VersionedBytesStore::in_memory(
            "v".into(),
            retention,
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-v-changelog".into(),
        )
    }

    #[tokio::test]
    async fn latest_and_as_of() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await;
        assert_eq!(s.get(&"k".into()).await.map(|r| r.value), Some(20));
        let r = s.get_as_of(&"k".into(), 150).await.unwrap();
        assert_eq!((r.value, r.valid_from, r.valid_to), (10, 100, Some(200)));
        assert_eq!(s.get_as_of(&"k".into(), 50).await, None);
    }

    #[tokio::test]
    async fn out_of_order_does_not_clobber_latest() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(20), 200).await;
        s.put("k".into(), Some(10), 100).await;
        assert_eq!(s.get(&"k".into()).await.map(|r| r.value), Some(20));
        assert_eq!(
            s.get_as_of(&"k".into(), 150).await.map(|r| r.value),
            Some(10)
        );
    }

    #[tokio::test]
    async fn tombstone_hides_latest_but_keeps_history() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(10), 100).await;
        let prev = s.delete(&"k".into(), 200).await;
        assert_eq!(prev.map(|r| r.value), Some(10));
        assert_eq!(s.get(&"k".into()).await, None);
        assert_eq!(
            s.get_as_of(&"k".into(), 150).await.map(|r| r.value),
            Some(10)
        );
    }

    #[tokio::test]
    async fn retention_drops_old_put_and_evicts_history() {
        let mut s = store(50);
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await;
        s.put("k".into(), Some(5), 40).await;
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 2);
        assert_eq!(s.get_as_of(&"k".into(), 40).await, None);
    }

    #[tokio::test]
    async fn changelog_roundtrip_restores_chain() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(10), 100).await;
        s.delete(&"k".into(), 150).await;
        s.put("k".into(), Some(30), 300).await;
        let cl = s.take_changelog();
        let mut r = store(1_000_000);
        r.set_logging(false);
        for (k, v) in cl {
            r.apply_changelog(k, v).await;
        }
        assert!(r.take_changelog().is_empty());
        assert_eq!(r.get(&"k".into()).await.map(|x| x.value), Some(30));
        assert_eq!(
            r.get_as_of(&"k".into(), 120).await.map(|x| x.value),
            Some(10)
        );
        assert_eq!(r.get_as_of(&"k".into(), 160).await, None);
    }
}

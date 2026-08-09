//! KIP-213 subscription store: `CombinedKey<KO,K>` to
//! `ValueAndTimestamp<SubscriptionWrapper>`, over the pluggable byte backend.
//!
//! A prefix range by foreign key drives the right-table-change re-emit. The store
//! is changelog-backed with a compact policy, and restore is a replay.
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(test)]
use crate::store::byte::InMemoryBytes;
use crate::{
    dsl::processors::fk::{
        combined_key::{combined_key, foreign_prefix, range_upper, split_combined_key},
        subscription::SubscriptionWrapper,
    },
    store::{
        api::StateStore,
        byte::ByteKeyValueStore,
        window_schema::{unwrap_value, wrap_value},
    },
};

/// Subscription store keyed by `combined_key(fk, pk)`, holding
/// `SubscriptionWrapper` bytes wrapped in a `ValueAndTimestamp`.
/// `range_by_foreign` scans every primary key subscribed to a given foreign key,
/// and that scan drives the right-table re-emit.
pub(crate) struct SubscriptionBytesStore {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl SubscriptionBytesStore {
    pub(crate) fn new(
        name: String,
        backend: Box<dyn ByteKeyValueStore>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            backend,
            changelog: Vec::new(),
            logging: true,
        }
    }

    #[cfg(test)]
    pub(crate) fn in_memory(name: String, changelog_topic: String) -> Self {
        Self::new(name, Box::new(InMemoryBytes::default()), changelog_topic)
    }

    pub(crate) async fn put(
        &mut self,
        fk: &[u8],
        pk: &[u8],
        w: &SubscriptionWrapper,
        record_ts: i64,
    ) {
        let key = combined_key(fk, pk);
        let val = wrap_value(record_ts, &w.serialize());
        self.backend.put(key.clone(), val.clone()).await;
        if self.logging {
            self.changelog.push((key, Some(val)));
        }
    }

    /// Point lookup of a single `(fk, pk)` subscription. The processors drive the
    /// store only with `put`, `delete`, and `range_by_foreign`. `get` backs the
    /// unit test.
    #[cfg(test)]
    pub(crate) async fn get(&self, fk: &[u8], pk: &[u8]) -> Option<SubscriptionWrapper> {
        let raw = self.backend.get(&combined_key(fk, pk)).await?;
        let (_ts, w) = unwrap_value(&raw);
        Some(SubscriptionWrapper::deserialize(w))
    }

    pub(crate) async fn delete(&mut self, fk: &[u8], pk: &[u8]) -> Option<SubscriptionWrapper> {
        let key = combined_key(fk, pk);
        let prev = self.backend.delete(&key).await.map(|raw| {
            let (_ts, w) = unwrap_value(&raw);
            SubscriptionWrapper::deserialize(w)
        });
        if self.logging {
            self.changelog.push((key, None));
        }
        prev
    }

    /// Every `(primaryKeyBytes, wrapper)` subscribed to `fk`, in stored key order.
    pub(crate) async fn range_by_foreign(&self, fk: &[u8]) -> Vec<(Bytes, SubscriptionWrapper)> {
        let lo = foreign_prefix(fk);
        let hi = range_upper(fk);
        let mut out = Vec::new();
        for (k, raw) in self.backend.range(&lo, &hi).await {
            let (gfk, gpk) = split_combined_key(&k);
            if gfk != fk {
                continue; // guard prefix collisions with a different foreign key
            }
            let (_ts, w) = unwrap_value(&raw);
            out.push((
                Bytes::copy_from_slice(gpk),
                SubscriptionWrapper::deserialize(w),
            ));
        }
        out
    }
}

#[async_trait]
impl StateStore for SubscriptionBytesStore {
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
    }
    // No `as_iq` override: the subscription store is internal (not user-queryable),
    // so it keeps the trait default `None`.
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::dsl::processors::fk::subscription::{Instruction, SubscriptionWrapper};

    fn wrapper(pk: &str) -> SubscriptionWrapper {
        SubscriptionWrapper {
            instruction: Instruction::PropagateOnlyIfFkValAvailable,
            hash: Some(vec![7u8; 16]),
            primary_key: Bytes::copy_from_slice(pk.as_bytes()),
            primary_partition: 0,
        }
    }

    #[tokio::test]
    async fn put_get_range_by_foreign_and_changelog() {
        let mut s = SubscriptionBytesStore::in_memory("sub".into(), "app-sub-changelog".into());
        s.put(b"FK1", b"pk1", &wrapper("pk1"), 10).await;
        s.put(b"FK1", b"pk2", &wrapper("pk2"), 11).await;
        s.put(b"FK2", b"pk9", &wrapper("pk9"), 12).await;
        assert_eq!(
            s.get(b"FK1", b"pk1").await.unwrap().primary_key,
            Bytes::from_static(b"pk1")
        );
        let subs = s.range_by_foreign(b"FK1").await;
        let pks: Vec<&[u8]> = subs.iter().map(|(_pk, w)| w.primary_key.as_ref()).collect();
        assert_eq!(pks, vec![b"pk1".as_ref(), b"pk2".as_ref()]);
        assert!(s.delete(b"FK1", b"pk1").await.is_some());
        assert_eq!(s.range_by_foreign(b"FK1").await.len(), 1);
        assert_eq!(s.take_changelog().len(), 4); // 3 puts + 1 delete (tombstone)
    }
}

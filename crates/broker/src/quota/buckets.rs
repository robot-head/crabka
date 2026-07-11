//! Per-broker cache of `TokenBucket`s, one per (`quota_key`, `entity_key`) pair.

use std::sync::Arc;

use crabka_metadata::EntityKey;
use dashmap::DashMap;

use crate::throttle::TokenBucket;

#[derive(Debug, Default)]
pub struct QuotaBuckets {
    /// Keyed by (`quota_key`, canonical entity key). One bucket per
    /// (`quota_type`, entity) pair, lazy-allocated on first lookup.
    buckets: DashMap<(String, EntityKey), Arc<TokenBucket>>,
}

impl QuotaBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    /// Get or lazily create a bucket for `(quota_key, entity_key)`,
    /// initializing it to `initial_rate` if newly created.
    #[must_use]
    pub fn get_or_create(
        &self,
        quota_key: &str,
        entity_key: &EntityKey,
        initial_rate: u64,
    ) -> Arc<TokenBucket> {
        if let Some(b) = self
            .buckets
            .get(&(quota_key.to_string(), entity_key.clone()))
        {
            return b.clone();
        }
        let b = Arc::new(TokenBucket::new());
        b.set_rate(initial_rate);
        let entry = self
            .buckets
            .entry((quota_key.to_string(), entity_key.clone()))
            .or_insert_with(|| b.clone());
        entry.clone()
    }

    /// Iterate every (`quota_key`, `entity_key`, bucket) — used by the
    /// refresh task to push new rates after an image change.
    pub fn iter(&self) -> impl Iterator<Item = ((String, EntityKey), Arc<TokenBucket>)> + '_ {
        self.buckets
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
    }

    #[cfg(test)]
    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn key(user: &str) -> EntityKey {
        vec![("user".into(), Some(user.into()))]
    }

    #[test]
    fn get_or_create_returns_new_bucket_first_time() {
        let buckets = QuotaBuckets::new();
        let b = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        assert2::assert!(b.rate() == 1024);
        assert2::assert!(buckets.len() == 1);
    }

    #[test]
    fn get_or_create_returns_existing_bucket_second_time() {
        let buckets = QuotaBuckets::new();
        let b1 = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let b2 = buckets.get_or_create("producer_byte_rate", &key("alice"), 4096);
        // Same Arc — initial_rate on second call is ignored.
        check!(Arc::ptr_eq(&b1, &b2));
        check!(b1.rate() == 1024);
        check!(buckets.len() == 1);
    }

    #[test]
    fn different_quota_keys_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let _ = buckets.get_or_create("consumer_byte_rate", &key("alice"), 2048);
        assert2::assert!(buckets.len() == 2);
    }

    #[test]
    fn different_entities_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let _ = buckets.get_or_create("producer_byte_rate", &key("bob"), 2048);
        assert2::assert!(buckets.len() == 2);
    }
}

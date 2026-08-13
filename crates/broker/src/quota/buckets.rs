//! Per-broker cache of `TokenBucket`s, one per (`quota_key`, `entity_key`) pair.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use crabka_metadata::EntityKey;
use dashmap::DashMap;

use crate::throttle::TokenBucket;

#[derive(Debug, Default)]
pub struct QuotaBuckets {
    /// Keyed by (`quota_key`, canonical entity key). There is one bucket for
    /// each (`quota_type`, entity) pair, allocated lazily on the first
    /// lookup.
    buckets: DashMap<(String, EntityKey), Arc<TokenBucket>>,
    controller_mutations: DashMap<EntityKey, Arc<Mutex<ControllerMutationBucket>>>,
}

#[derive(Debug)]
pub(super) struct ControllerMutationBucket {
    pub(super) rate: f64,
    pub(super) window_secs: f64,
    pub(super) tokens: f64,
    pub(super) updated_at: Instant,
}

impl QuotaBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
            controller_mutations: DashMap::new(),
        }
    }

    /// Returns the bucket for `(quota_key, entity_key)`, and creates it
    /// lazily if it does not exist. A new bucket starts at `initial_rate`.
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
        b.set_byte_rate(super::bucket_rate(initial_rate));
        let entry = self
            .buckets
            .entry((quota_key.to_string(), entity_key.clone()))
            .or_insert_with(|| b.clone());
        entry.clone()
    }

    /// Iterates over every (`quota_key`, `entity_key`, bucket) triple. The
    /// refresh task uses it to push new rates after an image change.
    pub fn iter(&self) -> impl Iterator<Item = ((String, EntityKey), Arc<TokenBucket>)> + '_ {
        self.buckets
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
    }

    pub(super) fn controller_mutation_bucket(
        &self,
        entity_key: &EntityKey,
        rate: f64,
        window_secs: f64,
    ) -> Arc<Mutex<ControllerMutationBucket>> {
        self.controller_mutations
            .entry(entity_key.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(ControllerMutationBucket {
                    rate,
                    window_secs,
                    tokens: rate * window_secs,
                    updated_at: Instant::now(),
                }))
            })
            .clone()
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
    use assert2::{assert, check};

    use super::{super::bucket_rate, *};

    fn key(user: &str) -> EntityKey {
        vec![("user".into(), Some(user.into()))]
    }

    #[test]
    fn get_or_create_returns_new_bucket_first_time() {
        let buckets = QuotaBuckets::new();
        let b = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        assert!(b.byte_rate() == bucket_rate(1024));
        assert!(buckets.len() == 1);
    }

    #[test]
    fn get_or_create_returns_existing_bucket_second_time() {
        let buckets = QuotaBuckets::new();
        let b1 = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let b2 = buckets.get_or_create("producer_byte_rate", &key("alice"), 4096);
        // Same Arc — initial_rate on second call is ignored.
        check!(Arc::ptr_eq(&b1, &b2));
        check!(b1.byte_rate() == bucket_rate(1024));
        check!(buckets.len() == 1);
    }

    #[test]
    fn different_quota_keys_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let _ = buckets.get_or_create("consumer_byte_rate", &key("alice"), 2048);
        assert!(buckets.len() == 2);
    }

    #[test]
    fn different_entities_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let _ = buckets.get_or_create("producer_byte_rate", &key("bob"), 2048);
        assert!(buckets.len() == 2);
    }
}

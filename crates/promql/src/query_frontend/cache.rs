use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use crabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};

use super::{FrontendRangeQuery, QueryShard};
use crate::{PromqlError, QueryResult};

/// The identity of one cached sub-range result.
///
/// The step stays a raw millisecond integer here. The key is a `BTreeMap` key
/// and an object-store path component. Both need the `Ord`/`Eq` that a
/// `f64`-backed [`Time`](crabka_units::Time) cannot supply.
/// [`RangeCacheKey::new`] does the conversion.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct RangeCacheKey {
    tenant: String,
    query: String,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    shard: Option<QueryShard>,
}

impl RangeCacheKey {
    fn new(tenant: &str, query: &FrontendRangeQuery) -> Self {
        Self {
            tenant: tenant.to_string(),
            query: query.query.clone(),
            start_ms: query.start_ms,
            end_ms: query.end_ms,
            step_ms: query.step.millis_i64(),
            shard: query.shard,
        }
    }
}

/// Wall-clock source for cache-entry age checks.
///
/// The trait exists so that tests can advance time deterministically with
/// [`ManualClock`] instead of a sleep. Production uses [`SystemClock`].
pub trait Clock: Send + Sync {
    /// Current time as Unix-epoch milliseconds.
    fn now_epoch_millis(&self) -> i64;
}

/// Default wall clock backed by [`std::time::SystemTime`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_millis(&self) -> i64 {
        let now = std::time::SystemTime::now();
        match now.duration_since(std::time::UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
            // Clock is before the Unix epoch; treat as time zero.
            Err(_) => 0,
        }
    }
}

/// Returns `true` if `inserted_epoch_millis` is older than `ttl`.
///
/// The age is measured against `now_epoch_millis`. A `None` TTL never expires.
fn entry_is_expired(ttl: Option<Time>, inserted_epoch_millis: i64, now_epoch_millis: i64) -> bool {
    let Some(ttl) = ttl else {
        return false;
    };
    let age = Time::from_millis(now_epoch_millis.saturating_sub(inserted_epoch_millis));
    age > ttl
}

/// In-memory range-result cache for query-frontend fan-out responses.
///
/// The backing store is small and swappable on purpose. Production wiring can
/// replace it with an object-store or topic-backed implementation and keep the
/// key contract that the tests cover here.
///
/// Each entry carries an insertion timestamp from the configured internal clock.
/// When [`QueryFrontendCache::with_ttl`] sets a TTL, a `get` for an entry older
/// than the TTL evicts that entry and reports a miss. With no TTL, the default,
/// entries never expire.
pub struct QueryFrontendCache {
    pub(super) range_results: Mutex<BTreeMap<RangeCacheKey, (i64, QueryResult)>>,
    ttl: Option<Time>,
    clock: Arc<dyn Clock>,
}

impl Default for QueryFrontendCache {
    fn default() -> Self {
        Self {
            range_results: Mutex::new(BTreeMap::new()),
            ttl: None,
            clock: Arc::new(SystemClock),
        }
    }
}

impl QueryFrontendCache {
    /// Builds a cache that expires entries older than `ttl`.
    #[must_use]
    pub fn with_ttl(ttl: Time) -> Self {
        Self {
            ttl: Some(ttl),
            ..Self::default()
        }
    }

    /// Overrides the wall clock, mainly for deterministic tests.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[must_use]
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn get(&self, tenant: &str, query: &FrontendRangeQuery) -> Option<QueryResult> {
        let key = RangeCacheKey::new(tenant, query);
        let mut entries = self
            .range_results
            .lock()
            .expect("query frontend cache poisoned");
        let (inserted, result) = entries.get(&key)?;
        if entry_is_expired(self.ttl, *inserted, self.clock.now_epoch_millis()) {
            entries.remove(&key);
            return None;
        }
        Some(result.clone())
    }

    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn insert(&self, tenant: &str, query: &FrontendRangeQuery, result: QueryResult) {
        let inserted = self.clock.now_epoch_millis();
        self.range_results
            .lock()
            .expect("query frontend cache poisoned")
            .insert(RangeCacheKey::new(tenant, query), (inserted, result));
    }
}

#[async_trait]
pub trait RangeQueryCache: Send + Sync {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError>;

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError>;
}

#[async_trait]
impl RangeQueryCache for QueryFrontendCache {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        Ok(QueryFrontendCache::get(self, tenant, query))
    }

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        QueryFrontendCache::insert(self, tenant, query, result);
        Ok(())
    }
}

/// Cached object-store payload: the range result and its store timestamp.
///
/// The timestamp is the wall-clock instant of the store operation. A reader
/// enforces a TTL from it and does not depend on the object-store
/// `last_modified` metadata.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredRangeResult {
    stored_at_ms: i64,
    result: QueryResult,
}

/// Object-store backed range-result cache for query-frontend fan-out responses.
///
/// Each cached object embeds the epoch-millis instant of its store operation.
/// When [`ObjectStoreQueryFrontendCache::with_ttl`] sets a TTL, a `get` for an
/// object older than the TTL reports a miss. That `get` also deletes the stale
/// object on a best-effort basis.
pub struct ObjectStoreQueryFrontendCache {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    ttl: Option<Time>,
    clock: Arc<dyn Clock>,
}

impl ObjectStoreQueryFrontendCache {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            store,
            prefix: normalize_cache_prefix(&prefix),
            ttl: None,
            clock: Arc::new(SystemClock),
        }
    }

    /// Expires cached objects older than `ttl`.
    #[must_use]
    pub fn with_ttl(mut self, ttl: Time) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Overrides the wall clock, mainly for deterministic tests.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        <Self as RangeQueryCache>::get(self, tenant, query).await
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        <Self as RangeQueryCache>::insert(self, tenant, query, result).await
    }

    fn path(&self, tenant: &str, query: &FrontendRangeQuery) -> Path {
        Path::from(format!(
            "{}/{}.json",
            self.prefix,
            range_cache_key_object_name(tenant, query)
        ))
    }
}

#[async_trait]
impl RangeQueryCache for ObjectStoreQueryFrontendCache {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        let path = self.path(tenant, query);
        let bytes = match self.store.get(&path).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| cache_store_error(&error))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(cache_store_error(&error)),
        };
        let stored: StoredRangeResult = serde_json::from_slice(&bytes).map_err(|error| {
            PromqlError::Store(format!("query frontend cache decode failed: {error}"))
        })?;
        if entry_is_expired(self.ttl, stored.stored_at_ms, self.clock.now_epoch_millis()) {
            // Best-effort eviction; a delete failure must not fail the read.
            let _ = self.store.delete(&path).await;
            return Ok(None);
        }
        Ok(Some(stored.result))
    }

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        let path = self.path(tenant, query);
        let stored = StoredRangeResult {
            stored_at_ms: self.clock.now_epoch_millis(),
            result,
        };
        let bytes = serde_json::to_vec(&stored).map_err(|error| {
            PromqlError::Store(format!("query frontend cache encode failed: {error}"))
        })?;
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(|error| cache_store_error(&error))?;
        Ok(())
    }
}

fn normalize_cache_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        "query-cache".to_string()
    } else {
        trimmed.to_string()
    }
}

fn cache_store_error(error: &object_store::Error) -> PromqlError {
    PromqlError::Store(format!("query frontend cache object-store error: {error}"))
}

fn range_cache_key_object_name(tenant: &str, query: &FrontendRangeQuery) -> String {
    let mut key = String::new();
    append_hex_component(&mut key, tenant.as_bytes());
    key.push('/');
    append_hex_component(&mut key, query.query.as_bytes());
    let shard = query
        .shard
        .map_or_else(|| "none".to_string(), QueryShard::selector_value);
    let _ = write!(
        key,
        "/{}-{}-{}-{}-{}",
        query.start_ms,
        query.end_ms,
        query.step.millis_i64(),
        query.shard.map_or(0, |shard| shard.index),
        query.shard.map_or(0, |shard| shard.total)
    );
    key.push('-');
    append_hex_component(&mut key, shard.as_bytes());
    key
}

fn append_hex_component(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

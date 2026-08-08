//! Byte-level read surface for Interactive Queries.
//!
//! The supervisor calls these methods through `&dyn StateStore::as_iq()` to serve
//! `KafkaStreams::*_store` queries, and it never learns `K` or `V`. The typed
//! view owns the serialization and deserialization. All reads take `&self`, and
//! only key and value **bytes** cross this trait.

use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

/// Which kind of store a query targets. Public so it can appear in `IqError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    KeyValue,
    Window,
    Session,
    Versioned,
}

/// A typed `IQv2` query lowered to the store boundary.
///
/// Keys travel as `Box<dyn Any + Send + Sync>`, which holds the raw `K`. The
/// concrete store downcasts to its own `K`, serializes with its own key serde,
/// runs the op, and returns the typed result boxed as `Box<dyn Any + Send>`.
/// That result is an `Option<V>`, a `Vec<(K,V)>`, or a similar type.
///
/// Time bounds are plain `i64`, and the ordering and bound choices are flags. No
/// serde and no `K` or `V` appear here. That is the whole point of the
/// byte-level boundary.
pub enum Iq2Query {
    /// `KeyQuery`: single key. Result: `Option<V>`.
    Key { key: Box<dyn Any + Send + Sync> },
    /// `RangeQuery`: a `None` bound is unbounded on that side. Result:
    /// `Vec<(K,V)>`.
    Range {
        lo: Option<Box<dyn Any + Send + Sync>>,
        hi: Option<Box<dyn Any + Send + Sync>>,
        descending: bool,
    },
    /// `WindowKeyQuery`: one key, window starts in `[from_ts, to_ts]`.
    /// Result: `Vec<(i64 /*windowStart*/, V)>`, ascending by start.
    WindowKey {
        key: Box<dyn Any + Send + Sync>,
        from_ts: i64,
        to_ts: i64,
    },
    /// `WindowRangeQuery`: key range by window-start range. A `None` bound is
    /// unbounded on that side. Result: `Vec<((K, i64 /*windowStart*/), V)>`,
    /// ascending by (key bytes, windowStart).
    WindowRange {
        lo: Option<Box<dyn Any + Send + Sync>>,
        hi: Option<Box<dyn Any + Send + Sync>>,
        from_ts: i64,
        to_ts: i64,
    },
    /// `VersionedKeyQuery` (KIP-960): one key. `as_of = None` gives the latest
    /// live version, and `Some(t)` gives the version valid at `t`. Result:
    /// `Option<VersionedRecord<V>>`.
    VersionedKey {
        key: Box<dyn Any + Send + Sync>,
        as_of: Option<i64>,
    },
    /// `MultiVersionedKeyQuery` (KIP-968): one key, and every version whose
    /// validity `[valid_from, valid_to)` overlaps `[from_ts, to_ts]`. A `None`
    /// bound is unbounded on that side. The order is ascending by `valid_from`,
    /// unless `descending` is set. Result: `Vec<VersionedRecord<V>>`.
    MultiVersionedKey {
        key: Box<dyn Any + Send + Sync>,
        from_ts: Option<i64>,
        to_ts: Option<i64>,
        descending: bool,
    },
}

/// Why a store could not run an `IQv2` query. The runtime maps these variants
/// into the public `FailureReason`, together with its own conditions:
/// rebalancing, not-up-to-bound, and not-active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Iq2Failure {
    /// This store kind has no handler for the requested query variant.
    UnknownQueryType,
    /// A key `Box<dyn Any>` did not downcast to this store's `K`.
    KeyTypeMismatch,
}

/// Byte-level IQ reads. The three materialized `*Bytes` stores implement this
/// trait. The default methods return empty results, so a non-matching store kind
/// never produces wrong data. The supervisor's `kind()` check catches that case
/// earlier.
#[doc(hidden)]
#[async_trait]
pub trait IqQueryable: Send + Sync {
    fn kind(&self) -> StoreKind;

    async fn iq_kv_get(&self, _key: &[u8]) -> Option<Bytes> {
        None
    }
    /// Inclusive `[lo, hi]` in memcmp order.
    async fn iq_kv_range(&self, _lo: &[u8], _hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        Vec::new()
    }
    async fn iq_kv_all(&self) -> Vec<(Bytes, Bytes)> {
        Vec::new()
    }
    async fn iq_kv_approx_count(&self) -> u64 {
        0
    }

    async fn iq_window_fetch_single(&self, _key: &[u8], _window_start: i64) -> Option<Bytes> {
        None
    }
    /// Ascending by window start, inclusive `[time_from, time_to]`.
    async fn iq_window_fetch(
        &self,
        _key: &[u8],
        _time_from: i64,
        _time_to: i64,
    ) -> Vec<(i64, Bytes)> {
        Vec::new()
    }

    /// All sessions for `key`, store order. Tuple is `(start, end)`.
    async fn iq_session_fetch_key(&self, _key: &[u8]) -> Vec<((i64, i64), Bytes)> {
        Vec::new()
    }

    /// Latest live version: `(validFrom, validTo=None, valueBytes)`.
    async fn iq_versioned_get(&self, _key: &[u8]) -> Option<(i64, Option<i64>, Bytes)> {
        None
    }
    /// Version valid at `as_of`: `(validFrom, validTo, valueBytes)`.
    async fn iq_versioned_get_as_of(
        &self,
        _key: &[u8],
        _as_of: i64,
    ) -> Option<(i64, Option<i64>, Bytes)> {
        None
    }

    /// `IQv2` entry point. The store downcasts the keys, serializes and
    /// deserializes with its own serdes, runs the op, and returns the typed
    /// result boxed. The default is that this store kind handles no `IQv2` query
    /// variant.
    async fn iq2_execute(&self, _query: &Iq2Query) -> Result<Box<dyn Any + Send>, Iq2Failure> {
        Err(Iq2Failure::UnknownQueryType)
    }
}

#[cfg(test)]
mod tests {
    use crabka_units::prelude::*;

    use super::*;
    use crate::{
        processor::serde::{I64Serde, Serde, StringSerde},
        store::{
            api::{KeyValueStore, StateStore},
            kv::KeyValueBytesStore,
            session::{SessionBytesStore, SessionStore},
            window::{WindowBytesStore, WindowStore},
        },
    };

    #[tokio::test]
    async fn kv_get_range_all_count_inclusive() {
        let mut s = KeyValueBytesStore::<String, i64>::in_memory(
            "c".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "c-changelog".into(),
        );
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            s.put(k.into(), v).await;
        }
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        assert_eq!(q.iq_kv_get(b"b").await, Some(I64Serde.serialize("t", &2)));
        assert_eq!(q.iq_kv_get(b"z").await, None);
        let r = q.iq_kv_range(b"a", b"b").await; // inclusive => a,b
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, bytes::Bytes::from_static(b"a"));
        assert_eq!(r[1].0, bytes::Bytes::from_static(b"b"));
        assert!(q.iq_kv_range(b"c", b"a").await.is_empty()); // lo>hi => empty
        assert_eq!(q.iq_kv_all().await.len(), 3);
        assert_eq!(q.iq_kv_approx_count().await, 3);
    }

    #[tokio::test]
    async fn window_fetch_point_and_range() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            secs(1),
        );
        s.put("k".into(), 0, 10, 5).await;
        s.put("k".into(), 1000, 20, 1005).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        assert_eq!(
            q.iq_window_fetch_single(b"k", 0).await,
            Some(I64Serde.serialize("t", &10))
        );
        assert_eq!(q.iq_window_fetch_single(b"k", 500).await, None);
        let r = q.iq_window_fetch(b"k", 0, 1000).await;
        assert_eq!(r.iter().map(|(t, _)| *t).collect::<Vec<_>>(), vec![0, 1000]);
    }

    #[tokio::test]
    async fn versioned_get_latest_and_as_of() {
        use crate::store::versioned::{VersionedBytesStore, VersionedKeyValueStore};
        let mut s = VersionedBytesStore::<String, i64>::in_memory(
            "v".into(),
            secs(1_000),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "v-changelog".into(),
        );
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        assert_eq!(q.kind(), StoreKind::Versioned);
        let (vf, vt, raw) = q.iq_versioned_get(b"k").await.unwrap();
        assert_eq!((vf, vt), (200, None));
        assert_eq!(raw, I64Serde.serialize("t", &20));
        let (vf2, vt2, raw2) = q.iq_versioned_get_as_of(b"k", 150).await.unwrap();
        assert_eq!((vf2, vt2), (100, Some(200)));
        assert_eq!(raw2, I64Serde.serialize("t", &10));
        assert_eq!(q.iq_versioned_get_as_of(b"k", 50).await, None);
    }

    #[tokio::test]
    async fn session_fetch_key_carries_start_end() {
        let mut s = SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "s-changelog".into(),
        );
        s.put("k".into(), 0, 10, 1).await;
        s.put("k".into(), 20, 30, 2).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        let r = q.iq_session_fetch_key(b"k").await;
        let windows: Vec<(i64, i64)> = r.iter().map(|((st, en), _)| (*st, *en)).collect();
        assert!(windows.contains(&(0, 10)) && windows.contains(&(20, 30)));
    }

    #[tokio::test]
    async fn iq2_execute_default_is_unknown_query_type() {
        use super::{Iq2Failure, Iq2Query};
        // A session store has no `IQv2` handler — default impl must reject.
        let s = SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "s-changelog".into(),
        );
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        let query = Iq2Query::Key {
            key: Box::new("k".to_string()),
        };
        assert_eq!(
            q.iq2_execute(&query).await.err(),
            Some(Iq2Failure::UnknownQueryType)
        );

        // Versioned variants also hit the default (a session store has no handler).
        let mv = Iq2Query::MultiVersionedKey {
            key: Box::new("k".to_string()),
            from_ts: None,
            to_ts: None,
            descending: false,
        };
        assert_eq!(
            q.iq2_execute(&mv).await.err(),
            Some(Iq2Failure::UnknownQueryType)
        );
    }
}

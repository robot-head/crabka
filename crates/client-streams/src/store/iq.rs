//! Byte-level read surface for Interactive Queries. The supervisor calls these
//! through `&dyn StateStore::as_iq()` to serve `KafkaStreams::*_store` queries
//! without knowing `K`/`V` — the typed view owns (de)serialization. All reads
//! are `&self`; only key/value **bytes** cross this trait.

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

/// A typed `IQv2` query lowered to the store boundary. Keys travel as
/// `Box<dyn Any + Send + Sync>` (the raw `K`); the concrete store downcasts to
/// its own `K`, serializes with its own key serde, runs the op, and returns the
/// typed result (`Option<V>`, `Vec<(K,V)>`, …) boxed as `Box<dyn Any + Send>`.
///
/// Time bounds are plain `i64`; ordering/bound choices are flags. No serde and
/// no `K`/`V` appear here — that is the whole point of the byte-level boundary.
pub enum Iq2Query {
    /// `KeyQuery` — single key. Result: `Option<V>`.
    Key { key: Box<dyn Any + Send + Sync> },
    /// `RangeQuery` — `None` bound = unbounded that side. Result: `Vec<(K,V)>`.
    Range {
        lo: Option<Box<dyn Any + Send + Sync>>,
        hi: Option<Box<dyn Any + Send + Sync>>,
        descending: bool,
    },
    /// `WindowKeyQuery` — one key, window starts in `[from_ts, to_ts]`.
    /// Result: `Vec<(i64 /*windowStart*/, V)>`, ascending by start.
    WindowKey {
        key: Box<dyn Any + Send + Sync>,
        from_ts: i64,
        to_ts: i64,
    },
    /// `WindowRangeQuery` — key range × window-start range. `None` bound =
    /// unbounded that side. Result: `Vec<((K, i64 /*windowStart*/), V)>`,
    /// ascending by (key bytes, windowStart).
    WindowRange {
        lo: Option<Box<dyn Any + Send + Sync>>,
        hi: Option<Box<dyn Any + Send + Sync>>,
        from_ts: i64,
        to_ts: i64,
    },
    /// `VersionedKeyQuery` (KIP-960) — one key; `as_of = None` ⇒ latest live
    /// version, `Some(t)` ⇒ the version valid at `t`. Result:
    /// `Option<VersionedRecord<V>>`.
    VersionedKey {
        key: Box<dyn Any + Send + Sync>,
        as_of: Option<i64>,
    },
    /// `MultiVersionedKeyQuery` (KIP-968) — one key; every version whose
    /// validity `[valid_from, valid_to)` overlaps `[from_ts, to_ts]` (`None`
    /// bound = unbounded that side), ascending by `valid_from` unless
    /// `descending`. Result: `Vec<VersionedRecord<V>>`.
    MultiVersionedKey {
        key: Box<dyn Any + Send + Sync>,
        from_ts: Option<i64>,
        to_ts: Option<i64>,
        descending: bool,
    },
}

/// Why a store could not execute an `IQv2` query. The runtime maps these (plus its
/// own conditions: rebalancing, not-up-to-bound, not-active) into the public
/// `FailureReason`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Iq2Failure {
    /// This store kind has no handler for the requested query variant.
    UnknownQueryType,
    /// A key `Box<dyn Any>` did not downcast to this store's `K`.
    KeyTypeMismatch,
}

/// Byte-level IQ reads. Implemented by the three materialized `*Bytes` stores.
/// Default methods return empties so a non-matching store kind (caught earlier
/// by the supervisor's `kind()` check) never produces wrong data.
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

    /// `IQv2` entry point. The store downcasts keys, (de)serializes with its own
    /// serdes, runs the op, and returns the typed result boxed. Default: this
    /// store kind handles no `IQv2` query variant.
    async fn iq2_execute(&self, _query: &Iq2Query) -> Result<Box<dyn Any + Send>, Iq2Failure> {
        Err(Iq2Failure::UnknownQueryType)
    }
}

#[cfg(test)]
mod tests {
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
        let hits = (q.iq_kv_get(b"b").await, q.iq_kv_get(b"z").await);
        let r = q.iq_kv_range(b"a", b"b").await; // inclusive => a,b
        let range_keys = r.iter().map(|(key, _)| key.as_ref()).collect::<Vec<_>>();
        let reverse_range = q.iq_kv_range(b"c", b"a").await;
        let counts = (q.iq_kv_all().await.len(), q.iq_kv_approx_count().await);
        assert_eq!(hits, (Some(I64Serde.serialize("t", &2)), None));
        assert_eq!(range_keys, vec![b"a".as_slice(), b"b".as_slice()]);
        assert!(reverse_range.is_empty());
        assert_eq!(counts, (3, 3));
    }

    #[tokio::test]
    async fn window_fetch_point_and_range() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            1000,
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
            1_000_000,
            Box::new(StringSerde),
            Box::new(I64Serde),
            "v-changelog".into(),
        );
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        assert_eq!(q.kind(), StoreKind::Versioned);
        assert_eq!(
            q.iq_versioned_get(b"k").await,
            Some((200, None, I64Serde.serialize("t", &20)))
        );
        assert_eq!(
            q.iq_versioned_get_as_of(b"k", 150).await,
            Some((100, Some(200), I64Serde.serialize("t", &10)))
        );
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

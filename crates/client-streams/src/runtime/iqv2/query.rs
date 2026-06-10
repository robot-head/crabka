//! `IQv2` query objects. Each builder lowers (serde-free) to a
//! `store::iq::Iq2Query`; the store supplies the actual serdes.

use std::any::Any;
use std::marker::PhantomData;

use crate::store::iq::{Iq2Query, StoreKind};
use crate::store::versioned::VersionedRecord;

mod sealed {
    pub trait Sealed {}
}

/// A typed `IQv2` query. `Result` is the type `KafkaStreams::query` returns per
/// partition. Sealed: only the in-crate query types implement it.
pub trait Query: sealed::Sealed {
    /// What a successful `QueryResult` carries.
    type Result: 'static;
    #[doc(hidden)]
    fn store_kind(&self) -> StoreKind;
    #[doc(hidden)]
    fn lower(self) -> Iq2Query;
}

/// Single-key lookup. Result: `Option<V>`.
pub struct KeyQuery<K, V> {
    key: K,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> KeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self {
            key,
            _v: PhantomData,
        }
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for KeyQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for KeyQuery<K, V> {
    type Result = Option<V>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::KeyValue
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::Key {
            key: Box::new(self.key),
        }
    }
}

/// Key-range scan. `None` bound = unbounded that side. Result: `Vec<(K,V)>`.
pub struct RangeQuery<K, V> {
    lo: Option<K>,
    hi: Option<K>,
    descending: bool,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> RangeQuery<K, V> {
    #[must_use]
    pub fn with_range(lo: K, hi: K) -> Self {
        Self {
            lo: Some(lo),
            hi: Some(hi),
            descending: false,
            _v: PhantomData,
        }
    }
    #[must_use]
    pub fn with_lower_bound(lo: K) -> Self {
        Self {
            lo: Some(lo),
            hi: None,
            descending: false,
            _v: PhantomData,
        }
    }
    #[must_use]
    pub fn with_upper_bound(hi: K) -> Self {
        Self {
            lo: None,
            hi: Some(hi),
            descending: false,
            _v: PhantomData,
        }
    }
    #[must_use]
    pub fn with_no_bounds() -> Self {
        Self {
            lo: None,
            hi: None,
            descending: false,
            _v: PhantomData,
        }
    }
    #[must_use]
    pub fn with_ascending_keys(mut self) -> Self {
        self.descending = false;
        self
    }
    #[must_use]
    pub fn with_descending_keys(mut self) -> Self {
        self.descending = true;
        self
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for RangeQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for RangeQuery<K, V> {
    type Result = Vec<(K, V)>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::KeyValue
    }
    fn lower(self) -> Iq2Query {
        let bx = |k: K| -> Box<dyn Any + Send + Sync> { Box::new(k) };
        Iq2Query::Range {
            lo: self.lo.map(bx),
            hi: self.hi.map(bx),
            descending: self.descending,
        }
    }
}

/// One key, window starts in `[from_time, to_time]`. Result: `Vec<(i64, V)>`.
pub struct WindowKeyQuery<K, V> {
    key: K,
    from_ts: i64,
    to_ts: i64,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> WindowKeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self {
            key,
            from_ts: i64::MIN,
            to_ts: i64::MAX,
            _v: PhantomData,
        }
    }
    #[must_use]
    pub fn from_time(mut self, t: i64) -> Self {
        self.from_ts = t;
        self
    }
    #[must_use]
    pub fn to_time(mut self, t: i64) -> Self {
        self.to_ts = t;
        self
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for WindowKeyQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for WindowKeyQuery<K, V> {
    type Result = Vec<(i64, V)>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Window
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::WindowKey {
            key: Box::new(self.key),
            from_ts: self.from_ts,
            to_ts: self.to_ts,
        }
    }
}

/// Key range × window-start range. Result: `Vec<((K, i64), V)>`.
pub struct WindowRangeQuery<K, V> {
    lo: Option<K>,
    hi: Option<K>,
    from_ts: i64,
    to_ts: i64,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> WindowRangeQuery<K, V> {
    #[must_use]
    pub fn with_key_range(lo: K, hi: K) -> Self {
        Self {
            lo: Some(lo),
            hi: Some(hi),
            from_ts: i64::MIN,
            to_ts: i64::MAX,
            _v: PhantomData,
        }
    }
    #[must_use]
    pub fn with_all_keys() -> Self {
        Self {
            lo: None,
            hi: None,
            from_ts: i64::MIN,
            to_ts: i64::MAX,
            _v: PhantomData,
        }
    }
    #[must_use]
    pub fn from_time(mut self, t: i64) -> Self {
        self.from_ts = t;
        self
    }
    #[must_use]
    pub fn to_time(mut self, t: i64) -> Self {
        self.to_ts = t;
        self
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for WindowRangeQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for WindowRangeQuery<K, V> {
    type Result = Vec<((K, i64), V)>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Window
    }
    fn lower(self) -> Iq2Query {
        let bx = |k: K| -> Box<dyn Any + Send + Sync> { Box::new(k) };
        Iq2Query::WindowRange {
            lo: self.lo.map(bx),
            hi: self.hi.map(bx),
            from_ts: self.from_ts,
            to_ts: self.to_ts,
        }
    }
}

/// Single versioned-key lookup (KIP-960). `as_of = None` ⇒ latest live version.
/// Result: `Option<VersionedRecord<V>>`.
pub struct VersionedKeyQuery<K, V> {
    key: K,
    as_of: Option<i64>,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> VersionedKeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self { key, as_of: None, _v: PhantomData }
    }
    #[must_use]
    pub fn as_of(mut self, timestamp: i64) -> Self {
        self.as_of = Some(timestamp);
        self
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for VersionedKeyQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for VersionedKeyQuery<K, V> {
    type Result = Option<VersionedRecord<V>>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Versioned
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::VersionedKey { key: Box::new(self.key), as_of: self.as_of }
    }
}

/// All versions of a key whose validity overlaps `[from_time, to_time]`
/// (KIP-968). `None` bound = unbounded that side; ascending by `valid_from`
/// unless `with_descending_timestamps()`. Result: `Vec<VersionedRecord<V>>`.
pub struct MultiVersionedKeyQuery<K, V> {
    key: K,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
    descending: bool,
    _v: PhantomData<fn() -> V>,
}
impl<K, V> MultiVersionedKeyQuery<K, V> {
    #[must_use]
    pub fn with_key(key: K) -> Self {
        Self { key, from_ts: None, to_ts: None, descending: false, _v: PhantomData }
    }
    #[must_use]
    pub fn from_time(mut self, t: i64) -> Self {
        self.from_ts = Some(t);
        self
    }
    #[must_use]
    pub fn to_time(mut self, t: i64) -> Self {
        self.to_ts = Some(t);
        self
    }
    #[must_use]
    pub fn with_ascending_timestamps(mut self) -> Self {
        self.descending = false;
        self
    }
    #[must_use]
    pub fn with_descending_timestamps(mut self) -> Self {
        self.descending = true;
        self
    }
}
impl<K: Send + Sync + 'static, V: 'static> sealed::Sealed for MultiVersionedKeyQuery<K, V> {}
impl<K: Send + Sync + 'static, V: 'static> Query for MultiVersionedKeyQuery<K, V> {
    type Result = Vec<VersionedRecord<V>>;
    fn store_kind(&self) -> StoreKind {
        StoreKind::Versioned
    }
    fn lower(self) -> Iq2Query {
        Iq2Query::MultiVersionedKey {
            key: Box::new(self.key),
            from_ts: self.from_ts,
            to_ts: self.to_ts,
            descending: self.descending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowering_picks_the_right_variant_and_kind() {
        let kq = KeyQuery::<String, i64>::with_key("a".into());
        assert_eq!(kq.store_kind(), StoreKind::KeyValue);
        assert!(matches!(kq.lower(), Iq2Query::Key { .. }));

        let rq = RangeQuery::<String, i64>::with_lower_bound("a".into()).with_descending_keys();
        assert!(matches!(
            rq.lower(),
            Iq2Query::Range {
                lo: Some(_),
                hi: None,
                descending: true
            }
        ));

        let wk = WindowKeyQuery::<String, i64>::with_key("a".into())
            .from_time(0)
            .to_time(9);
        assert_eq!(wk.store_kind(), StoreKind::Window);
        assert!(matches!(
            wk.lower(),
            Iq2Query::WindowKey {
                from_ts: 0,
                to_ts: 9,
                ..
            }
        ));

        let wr = WindowRangeQuery::<String, i64>::with_all_keys();
        assert!(matches!(
            wr.lower(),
            Iq2Query::WindowRange {
                lo: None,
                hi: None,
                ..
            }
        ));
    }

    #[test]
    fn versioned_queries_lower_correctly() {
        let vk = VersionedKeyQuery::<String, i64>::with_key("k".into()).as_of(250);
        assert_eq!(vk.store_kind(), StoreKind::Versioned);
        assert!(matches!(vk.lower(), Iq2Query::VersionedKey { as_of: Some(250), .. }));

        let vk_latest = VersionedKeyQuery::<String, i64>::with_key("k".into());
        assert!(matches!(vk_latest.lower(), Iq2Query::VersionedKey { as_of: None, .. }));

        let mv = MultiVersionedKeyQuery::<String, i64>::with_key("k".into())
            .from_time(150)
            .to_time(250)
            .with_descending_timestamps();
        assert_eq!(mv.store_kind(), StoreKind::Versioned);
        assert!(matches!(
            mv.lower(),
            Iq2Query::MultiVersionedKey { from_ts: Some(150), to_ts: Some(250), descending: true, .. }
        ));

        let mv_all = MultiVersionedKeyQuery::<String, i64>::with_key("k".into());
        assert!(matches!(
            mv_all.lower(),
            Iq2Query::MultiVersionedKey { from_ts: None, to_ts: None, descending: false, .. }
        ));
    }
}

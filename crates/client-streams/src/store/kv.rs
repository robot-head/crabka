//! `KeyValueBytesStore<K,V>`: the single typed store the registry holds and
//! downcasts to. Serde + changelog-buffer logic over a pluggable `ByteKeyValueStore`.
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::{KeyValueStore, StateStore};
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};

pub struct KeyValueBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> KeyValueBytesStore<K, V> {
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

    /// Convenience constructor for tests: an in-memory-backed store.
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
impl<K: Send + 'static, V: Send + 'static> StateStore for KeyValueBytesStore<K, V> {
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
    fn as_iq(&self) -> Option<&dyn crate::store::iq::IqQueryable> {
        Some(self)
    }
    async fn clear(&mut self) {
        self.backend.clear().await;
        self.changelog.clear();
    }
}

// The store struct holds only `Box<dyn Serde<_>>` + byte buffers (no bare `K`/`V`
// fields), so it is `Send + Sync` for *any* `K`/`V` — no `Sync` bound needed here.
// `K: Send + V: Send` are required so `Box<Vec<(K,V)>>` / `Box<Option<V>>` can be
// returned as `Box<dyn Any + Send>` from `iq2_execute`.

/// Intermediate representation for `iq2_execute`: keys already serialized to bytes,
/// no reference to `Iq2Query` (which is not `Sync`) held across any await point.
enum Iq2Prepared {
    Key(bytes::Bytes),
    Range {
        lo_b: Option<bytes::Bytes>,
        hi_b: Option<bytes::Bytes>,
        descending: bool,
    },
    Unknown,
}

impl<K: Send + 'static, V: Send + 'static> KeyValueBytesStore<K, V> {
    /// Synchronously extract and serialize keys from an `Iq2Query`. Called before
    /// any `.await` so `query` (non-`Sync`) is dropped before the async phase.
    fn iq2_prepare(
        &self,
        query: &crate::store::iq::Iq2Query,
    ) -> Result<Iq2Prepared, crate::store::iq::Iq2Failure> {
        use crate::store::iq::{Iq2Failure, Iq2Query};
        let ser = |b: &dyn Any| -> Result<bytes::Bytes, Iq2Failure> {
            let k = b.downcast_ref::<K>().ok_or(Iq2Failure::KeyTypeMismatch)?;
            Ok(self.key_serde.serialize(&self.changelog_topic, k))
        };
        match query {
            Iq2Query::Key { key } => Ok(Iq2Prepared::Key(ser(&**key)?)),
            Iq2Query::Range { lo, hi, descending } => {
                let lo_b = match lo {
                    Some(b) => Some(ser(&**b)?),
                    None => None,
                };
                let hi_b = match hi {
                    Some(b) => Some(ser(&**b)?),
                    None => None,
                };
                Ok(Iq2Prepared::Range {
                    lo_b,
                    hi_b,
                    descending: *descending,
                })
            }
            _ => Ok(Iq2Prepared::Unknown),
        }
    }
}

#[async_trait::async_trait]
impl<K: Send + 'static, V: Send + 'static> crate::store::iq::IqQueryable
    for KeyValueBytesStore<K, V>
{
    fn kind(&self) -> crate::store::iq::StoreKind {
        crate::store::iq::StoreKind::KeyValue
    }
    async fn iq_kv_get(&self, key: &[u8]) -> Option<bytes::Bytes> {
        self.backend.get(key).await
    }
    async fn iq_kv_range(&self, lo: &[u8], hi: &[u8]) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        // JVM `range` is inclusive `[lo, hi]`; the byte backend is half-open
        // `[lo, hi)`. `hi ++ 0x00` is the least key strictly greater than `hi`,
        // so `[lo, hi ++ 0x00)` == inclusive `[lo, hi]`.
        let mut hi_succ = hi.to_vec();
        hi_succ.push(0);
        self.backend.range(lo, &hi_succ).await
    }
    async fn iq_kv_all(&self) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        self.backend.scan_all().await
    }
    async fn iq_kv_approx_count(&self) -> u64 {
        self.backend.approx_len().await
    }

    async fn iq2_execute(
        &self,
        query: &crate::store::iq::Iq2Query,
    ) -> Result<Box<dyn Any + Send>, crate::store::iq::Iq2Failure> {
        use crate::store::iq::Iq2Failure;

        // Serialize all keys synchronously via the helper so `query`
        // (non-`Sync`) is fully consumed before any `.await`.
        let prepared = self.iq2_prepare(query)?;

        match prepared {
            Iq2Prepared::Key(kb) => {
                let out: Option<V> = self.backend.get(&kb).await.map(|vb| {
                    self.value_serde
                        .deserialize(&self.changelog_topic, &vb)
                        .expect("iqv2 kv value deserialize")
                });
                Ok(Box::new(out))
            }
            Iq2Prepared::Range {
                lo_b,
                hi_b,
                descending,
            } => {
                let mut rows: Vec<(K, V)> = Vec::new();
                for (kb, vb) in self.backend.scan_all().await {
                    if let Some(l) = &lo_b {
                        if kb.as_ref() < l.as_ref() {
                            continue;
                        }
                    }
                    if let Some(h) = &hi_b {
                        if kb.as_ref() > h.as_ref() {
                            continue;
                        }
                    }
                    rows.push((
                        self.key_serde
                            .deserialize(&self.changelog_topic, &kb)
                            .expect("iqv2 kv range key deserialize"),
                        self.value_serde
                            .deserialize(&self.changelog_topic, &vb)
                            .expect("iqv2 kv range value deserialize"),
                    ));
                }
                if descending {
                    rows.reverse();
                }
                Ok(Box::new(rows))
            }
            Iq2Prepared::Unknown => Err(Iq2Failure::UnknownQueryType),
        }
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> KeyValueStore<K, V> for KeyValueBytesStore<K, V> {
    async fn get(&self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        self.backend.get(&kb).await.map(|vb| {
            self.value_serde
                .deserialize(&self.changelog_topic, &vb)
                .expect("store value deserialize")
        })
    }
    async fn put(&mut self, key: K, value: V) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let vb = self.value_serde.serialize(&self.changelog_topic, &value);
        self.backend.put(kb.clone(), vb.clone()).await;
        if self.logging {
            self.changelog.push((kb, Some(vb)));
        }
    }
    async fn delete(&mut self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let prev = self.backend.delete(&kb).await.map(|vb| {
            self.value_serde
                .deserialize(&self.changelog_topic, &vb)
                .expect("store value deserialize")
        });
        if self.logging {
            self.changelog.push((kb, None));
        }
        prev
    }
    async fn range(&self, lo: &K, hi: &K) -> Vec<(K, V)> {
        let lo_b = self.key_serde.serialize(&self.changelog_topic, lo);
        let hi_b = self.key_serde.serialize(&self.changelog_topic, hi);
        self.backend
            .range(&lo_b, &hi_b)
            .await
            .into_iter()
            .map(|(kb, vb)| {
                (
                    self.key_serde
                        .deserialize(&self.changelog_topic, &kb)
                        .expect("kv range key deserialize"),
                    self.value_serde
                        .deserialize(&self.changelog_topic, &vb)
                        .expect("kv range value deserialize"),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};
    use assert2::check;

    fn store() -> KeyValueBytesStore<String, i64> {
        KeyValueBytesStore::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "s-changelog".into(),
        )
    }

    #[tokio::test]
    async fn put_get_delete_and_changelog_buffer() {
        let mut s = store();
        s.put("a".into(), 1).await;
        s.put("a".into(), 2).await;
        check!(s.get(&"a".to_string()).await == Some(2));
        check!(s.delete(&"a".to_string()).await == Some(2));
        check!(s.get(&"a".to_string()).await == None);
        let cl = s.take_changelog();
        check!(cl.len() == 3);
        check!(cl[2].1.is_none());
        check!(s.take_changelog().is_empty());
    }

    #[tokio::test]
    async fn range_returns_ordered_half_open() {
        use crate::processor::serde::BytesSerde;
        use bytes::Bytes;
        let mut s = KeyValueBytesStore::<Bytes, Bytes>::in_memory(
            "r".into(),
            Box::new(BytesSerde),
            Box::new(BytesSerde),
            "r-cl".into(),
        );
        s.put(Bytes::from_static(&[1, 0]), Bytes::from_static(b"a"))
            .await;
        s.put(Bytes::from_static(&[1, 5]), Bytes::from_static(b"b"))
            .await;
        s.put(Bytes::from_static(&[2, 0]), Bytes::from_static(b"c"))
            .await;
        let r = s
            .range(&Bytes::from_static(&[1, 0]), &Bytes::from_static(&[2, 0]))
            .await; // [lo, hi)
        assert_eq!(
            r,
            vec![
                (Bytes::from_static(&[1, 0]), Bytes::from_static(b"a")),
                (Bytes::from_static(&[1, 5]), Bytes::from_static(b"b")),
            ]
        );
    }

    #[tokio::test]
    async fn iq2_key_and_range() {
        use crate::store::iq::{Iq2Query, IqQueryable};
        let mut s = store();
        s.put("a".into(), 1).await;
        s.put("b".into(), 2).await;
        s.put("c".into(), 3).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();

        // KeyQuery hit / miss.
        let got = q
            .iq2_execute(&Iq2Query::Key {
                key: Box::new("b".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(*got.downcast::<Option<i64>>().unwrap(), Some(2));
        let miss = q
            .iq2_execute(&Iq2Query::Key {
                key: Box::new("z".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(*miss.downcast::<Option<i64>>().unwrap(), None);

        // RangeQuery inclusive [a,b] ascending.
        let r = q
            .iq2_execute(&Iq2Query::Range {
                lo: Some(Box::new("a".to_string())),
                hi: Some(Box::new("b".to_string())),
                descending: false,
            })
            .await
            .unwrap();
        assert_eq!(
            *r.downcast::<Vec<(String, i64)>>().unwrap(),
            vec![("a".to_string(), 1), ("b".to_string(), 2)]
        );

        // Unbounded both sides, descending → all, reversed.
        let all_desc = q
            .iq2_execute(&Iq2Query::Range {
                lo: None,
                hi: None,
                descending: true,
            })
            .await
            .unwrap();
        assert_eq!(
            *all_desc.downcast::<Vec<(String, i64)>>().unwrap(),
            vec![
                ("c".to_string(), 3),
                ("b".to_string(), 2),
                ("a".to_string(), 1)
            ]
        );

        // Wrong key type → KeyTypeMismatch.
        use crate::store::iq::Iq2Failure;
        let bad = q
            .iq2_execute(&Iq2Query::Key {
                key: Box::new(7_i64),
            })
            .await;
        assert_eq!(bad.err(), Some(Iq2Failure::KeyTypeMismatch));
    }

    #[tokio::test]
    async fn apply_changelog_restores_without_re_logging() {
        let mut s = store();
        s.apply_changelog(
            b"k".to_vec().into(),
            Some(bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 7])),
        )
        .await;
        check!(s.get(&"k".to_string()).await == Some(7));
        check!(s.take_changelog().is_empty());
        s.apply_changelog(b"k".to_vec().into(), None).await;
        check!(s.get(&"k".to_string()).await == None);
    }
}

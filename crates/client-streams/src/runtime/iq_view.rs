//! Typed, read-only composite views over local state stores (Interactive
//! Queries). Each view owns its Serdes and round-trips byte-level `IqRequest`s
//! to the supervisor; results are eagerly materialized `Vec`s (one intentional
//! divergence from the JVM's lazy `KeyValueIterator`).

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::{
    dsl::windows::{Window, Windowed},
    error::StreamsClientError,
    processor::serde::Serde,
    runtime::iq::{IqError, IqOp, IqPayload, IqRequest},
    store::iq::StoreKind,
};

/// Round-trip one op to the supervisor. Shared by all three views.
async fn query(
    tx: &mpsc::Sender<IqRequest>,
    store: &str,
    kind: StoreKind,
    op: IqOp,
) -> Result<IqPayload, StreamsClientError> {
    let (reply, rx) = oneshot::channel();
    tx.send(IqRequest {
        store: store.to_string(),
        kind,
        op,
        reply,
    })
    .await
    .map_err(|_| StreamsClientError::InteractiveQuery(IqError::RebalanceInProgress))?;
    rx.await
        .map_err(|_| StreamsClientError::InteractiveQuery(IqError::RebalanceInProgress))?
        .map_err(StreamsClientError::InteractiveQuery)
}

fn deser<T: 'static>(
    topic: &str,
    serde: &dyn Serde<T>,
    bytes: &[u8],
) -> Result<T, StreamsClientError> {
    serde
        .deserialize(topic, bytes)
        .map_err(|e| StreamsClientError::Runtime(format!("iq deserialize: {e}")))
}

pub(crate) fn unexpected(p: &IqPayload) -> StreamsClientError {
    StreamsClientError::Runtime(format!("iq: unexpected payload {p:?}"))
}

/// Validate a store exists locally + has the requested kind (eager, in accessors).
#[tracing::instrument(
    name = "streams.iq.validate",
    level = "debug",
    skip_all,
    fields(store = %store, kind = ?kind),
    err,
)]
pub(crate) async fn validate(
    tx: &mpsc::Sender<IqRequest>,
    store: &str,
    kind: StoreKind,
) -> Result<(), StreamsClientError> {
    match query(tx, store, kind, IqOp::Validate).await? {
        IqPayload::Validated => Ok(()),
        other => Err(unexpected(&other)),
    }
}

/// Read-only composite KV store view (Interactive Queries).
pub struct ReadOnlyKeyValueStore<K, V> {
    pub(crate) tx: mpsc::Sender<IqRequest>,
    pub(crate) store: String,
    pub(crate) key_serde: Box<dyn Serde<K>>,
    pub(crate) value_serde: Box<dyn Serde<V>>,
}

impl<K: 'static, V: 'static> ReadOnlyKeyValueStore<K, V> {
    /// Value for `key`, or `None` if absent.
    pub async fn get(&self, key: &K) -> Result<Option<V>, StreamsClientError> {
        let kb = self.key_serde.serialize(&self.store, key);
        match query(
            &self.tx,
            &self.store,
            StoreKind::KeyValue,
            IqOp::KvGet { key: kb },
        )
        .await?
        {
            IqPayload::Value(Some(vb)) => Ok(Some(deser(&self.store, &*self.value_serde, &vb)?)),
            IqPayload::Value(None) => Ok(None),
            other => Err(unexpected(&other)),
        }
    }

    /// Inclusive `[lo, hi]` range, ascending memcmp key order.
    pub async fn range(&self, lo: &K, hi: &K) -> Result<Vec<(K, V)>, StreamsClientError> {
        let lo_b = self.key_serde.serialize(&self.store, lo);
        let hi_b = self.key_serde.serialize(&self.store, hi);
        match query(
            &self.tx,
            &self.store,
            StoreKind::KeyValue,
            IqOp::KvRange { lo: lo_b, hi: hi_b },
        )
        .await?
        {
            IqPayload::Entries(pairs) => self.decode_pairs(pairs),
            other => Err(unexpected(&other)),
        }
    }

    /// Every entry.
    pub async fn all(&self) -> Result<Vec<(K, V)>, StreamsClientError> {
        match query(&self.tx, &self.store, StoreKind::KeyValue, IqOp::KvAll).await? {
            IqPayload::Entries(pairs) => self.decode_pairs(pairs),
            other => Err(unexpected(&other)),
        }
    }

    /// Approximate entry count (exact for in-memory; summed across partitions).
    pub async fn approximate_num_entries(&self) -> Result<u64, StreamsClientError> {
        match query(
            &self.tx,
            &self.store,
            StoreKind::KeyValue,
            IqOp::KvApproxCount,
        )
        .await?
        {
            IqPayload::Count(n) => Ok(n),
            other => Err(unexpected(&other)),
        }
    }

    fn decode_pairs(&self, pairs: Vec<(Bytes, Bytes)>) -> Result<Vec<(K, V)>, StreamsClientError> {
        pairs
            .into_iter()
            .map(|(kb, vb)| {
                Ok((
                    deser(&self.store, &*self.key_serde, &kb)?,
                    deser(&self.store, &*self.value_serde, &vb)?,
                ))
            })
            .collect()
    }
}

/// Read-only composite window store view. `fetch` yields `(windowStart, V)`.
pub struct ReadOnlyWindowStore<K, V> {
    pub(crate) tx: mpsc::Sender<IqRequest>,
    pub(crate) store: String,
    pub(crate) key_serde: Box<dyn Serde<K>>,
    pub(crate) value_serde: Box<dyn Serde<V>>,
}

impl<K: 'static, V: 'static> ReadOnlyWindowStore<K, V> {
    /// Value of the window for `key` starting exactly at `window_start`, else `None`.
    pub async fn fetch_single(
        &self,
        key: &K,
        window_start: i64,
    ) -> Result<Option<V>, StreamsClientError> {
        let kb = self.key_serde.serialize(&self.store, key);
        match query(
            &self.tx,
            &self.store,
            StoreKind::Window,
            IqOp::WindowFetchSingle {
                key: kb,
                window_start,
            },
        )
        .await?
        {
            IqPayload::Value(Some(vb)) => Ok(Some(deser(&self.store, &*self.value_serde, &vb)?)),
            IqPayload::Value(None) => Ok(None),
            other => Err(unexpected(&other)),
        }
    }

    /// Windows for `key` with start in inclusive `[time_from, time_to]`,
    /// ascending by start. Each item is `(windowStart, value)`.
    pub async fn fetch(
        &self,
        key: &K,
        time_from: i64,
        time_to: i64,
    ) -> Result<Vec<(i64, V)>, StreamsClientError> {
        let kb = self.key_serde.serialize(&self.store, key);
        match query(
            &self.tx,
            &self.store,
            StoreKind::Window,
            IqOp::WindowFetch {
                key: kb,
                time_from,
                time_to,
            },
        )
        .await?
        {
            IqPayload::WindowEntries(rows) => rows
                .into_iter()
                .map(|(t, vb)| Ok((t, deser(&self.store, &*self.value_serde, &vb)?)))
                .collect(),
            other => Err(unexpected(&other)),
        }
    }
}

/// Read-only composite session store view. `fetch` yields each session as a
/// `Windowed<K>` (key + `[start, end]`) with its value.
pub struct ReadOnlySessionStore<K, V> {
    pub(crate) tx: mpsc::Sender<IqRequest>,
    pub(crate) store: String,
    pub(crate) key_serde: Box<dyn Serde<K>>,
    pub(crate) value_serde: Box<dyn Serde<V>>,
}

impl<K: 'static, V: 'static> ReadOnlySessionStore<K, V> {
    /// All sessions for `key`, in store order.
    pub async fn fetch(&self, key: &K) -> Result<Vec<(Windowed<K>, V)>, StreamsClientError> {
        let kb = self.key_serde.serialize(&self.store, key);
        match query(
            &self.tx,
            &self.store,
            StoreKind::Session,
            IqOp::SessionFetchKey { key: kb },
        )
        .await?
        {
            IqPayload::SessionEntries(rows) => rows
                .into_iter()
                .map(|((start, end), vb)| {
                    // Re-deserialize the key per row (avoids a `K: Clone` bound).
                    let k = deser(
                        &self.store,
                        &*self.key_serde,
                        &self.key_serde.serialize(&self.store, key),
                    )?;
                    Ok((
                        Windowed {
                            key: k,
                            window: Window { start, end },
                        },
                        deser(&self.store, &*self.value_serde, &vb)?,
                    ))
                })
                .collect(),
            other => Err(unexpected(&other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        processor::serde::{I64Serde, StringSerde},
        runtime::iq::answer_iq,
        store::{api::KeyValueStore, kv::KeyValueBytesStore, registry::StoreRegistry},
    };

    /// Spawn a tiny servicer over one registry; returns the sender the views use.
    pub(super) fn servicer(reg: StoreRegistry) -> mpsc::Sender<IqRequest> {
        let (tx, mut rx) = mpsc::channel::<IqRequest>(16);
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let matching = reg.iq_get(&req.store).into_iter().collect::<Vec<_>>();
                let res = answer_iq(matching, req.kind, &req.op, &req.store, true).await;
                let _ = req.reply.send(res);
            }
        });
        tx
    }

    async fn kv_registry() -> StoreRegistry {
        let mut s = KeyValueBytesStore::<String, i64>::in_memory(
            "counts".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "counts-changelog".into(),
        );
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            s.put(k.into(), v).await;
        }
        let mut reg = StoreRegistry::default();
        reg.insert(Box::new(s));
        reg
    }

    #[tokio::test]
    async fn kv_view_get_range_all_count() {
        let tx = servicer(kv_registry().await);
        let view = ReadOnlyKeyValueStore::<String, i64> {
            tx,
            store: "counts".into(),
            key_serde: Box::new(StringSerde),
            value_serde: Box::new(I64Serde),
        };
        let hits = (
            view.get(&"b".to_string()).await.unwrap(),
            view.get(&"z".to_string()).await.unwrap(),
        );
        let r = view
            .range(&"a".to_string(), &"b".to_string())
            .await
            .unwrap();
        let counts = (
            view.all().await.unwrap().len(),
            view.approximate_num_entries().await.unwrap(),
        );
        assert_eq!(
            (hits, r, counts),
            (
                (Some(2), None),
                vec![("a".to_string(), 1), ("b".to_string(), 2)],
                (3, 3)
            )
        );
    }

    async fn window_registry() -> StoreRegistry {
        use crate::store::window::{WindowBytesStore, WindowStore};
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "wc".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "wc-changelog".into(),
            1000,
        );
        s.put("k".into(), 0, 10, 5).await;
        s.put("k".into(), 1000, 20, 1005).await;
        let mut reg = StoreRegistry::default();
        reg.insert(Box::new(s));
        reg
    }

    #[tokio::test]
    async fn window_view_fetch() {
        let tx = servicer(window_registry().await);
        let view = ReadOnlyWindowStore::<String, i64> {
            tx,
            store: "wc".into(),
            key_serde: Box::new(StringSerde),
            value_serde: Box::new(I64Serde),
        };
        assert_eq!(
            view.fetch_single(&"k".to_string(), 0).await.unwrap(),
            Some(10)
        );
        assert_eq!(view.fetch_single(&"k".to_string(), 5).await.unwrap(), None);
        let r = view.fetch(&"k".to_string(), 0, 1000).await.unwrap();
        assert_eq!(r, vec![(0, 10), (1000, 20)]);
    }

    async fn session_registry() -> StoreRegistry {
        use crate::store::session::{SessionBytesStore, SessionStore};
        let mut s = SessionBytesStore::<String, i64>::in_memory(
            "sc".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "sc-changelog".into(),
        );
        s.put("k".into(), 0, 10, 1).await;
        s.put("k".into(), 20, 30, 2).await;
        let mut reg = StoreRegistry::default();
        reg.insert(Box::new(s));
        reg
    }

    #[tokio::test]
    async fn session_view_fetch() {
        use crate::dsl::windows::Window;
        let tx = servicer(session_registry().await);
        let view = ReadOnlySessionStore::<String, i64> {
            tx,
            store: "sc".into(),
            key_serde: Box::new(StringSerde),
            value_serde: Box::new(I64Serde),
        };
        let rows = view.fetch(&"k".to_string()).await.unwrap();
        let mut got: Vec<(Window, i64)> = rows.into_iter().map(|(w, v)| (w.window, v)).collect();
        got.sort_by_key(|(window, _)| window.start);
        assert_eq!(
            got,
            vec![
                (Window { start: 0, end: 10 }, 1),
                (Window { start: 20, end: 30 }, 2),
            ]
        );
    }
}

//! The interactive-query channel protocol.
//!
//! The `KafkaStreams` handle sends byte-level `IqRequest`s to the supervisor
//! task. The supervisor resolves them against the local stores with `answer_iq`
//! and replies on a `oneshot`.

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::store::iq::{IqQueryable, StoreKind};

/// A byte-level query operation. It carries no `K` and no `V`, because the typed
/// view serializes and deserializes.
#[derive(Debug)]
pub(crate) enum IqOp {
    Validate,
    KvGet {
        key: Bytes,
    },
    KvRange {
        lo: Bytes,
        hi: Bytes,
    },
    KvAll,
    KvApproxCount,
    WindowFetchSingle {
        key: Bytes,
        window_start: i64,
    },
    WindowFetch {
        key: Bytes,
        time_from: i64,
        time_to: i64,
    },
    SessionFetchKey {
        key: Bytes,
    },
}

/// A byte-level query result.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IqPayload {
    Validated,
    Value(Option<Bytes>),
    Entries(Vec<(Bytes, Bytes)>),
    WindowEntries(Vec<(i64, Bytes)>),
    SessionEntries(Vec<((i64, i64), Bytes)>),
    Count(u64),
}

/// Why an interactive query failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IqError {
    #[error("state store {0:?} is not assigned to this instance")]
    StoreNotFound(String),
    #[error("state store {name:?} is a {found:?} store, not {requested:?}")]
    WrongStoreKind {
        name: String,
        found: StoreKind,
        requested: StoreKind,
    },
    #[error("a rebalance is in progress; retry the query")]
    RebalanceInProgress,
}

/// One query addressed to the supervisor.
pub(crate) struct IqRequest {
    pub store: String,
    pub kind: StoreKind,
    pub op: IqOp,
    pub reply: oneshot::Sender<Result<IqPayload, IqError>>,
}

/// Resolve one operation against every local store named `store`, which gives a
/// composite result across the partitions.
///
/// `matching` is the set of `IqQueryable` views for that name on this instance.
/// `any_tasks` separates two cases: the instance is rebalancing and has no tasks
/// at all, or the instance has tasks but none of them hosts this store.
pub(crate) async fn answer_iq(
    matching: Vec<&dyn IqQueryable>,
    kind: StoreKind,
    op: &IqOp,
    store: &str,
    any_tasks: bool,
) -> Result<IqPayload, IqError> {
    if matching.is_empty() {
        return Err(if any_tasks {
            IqError::StoreNotFound(store.to_string())
        } else {
            IqError::RebalanceInProgress
        });
    }
    let found = matching[0].kind();
    if found != kind {
        return Err(IqError::WrongStoreKind {
            name: store.to_string(),
            found,
            requested: kind,
        });
    }
    Ok(match op {
        IqOp::Validate => IqPayload::Validated,
        IqOp::KvGet { key } => {
            let mut hit = None;
            for s in &matching {
                if let Some(v) = s.iq_kv_get(key).await {
                    hit = Some(v);
                    break;
                }
            }
            IqPayload::Value(hit)
        }
        IqOp::KvRange { lo, hi } => {
            let mut out = Vec::new();
            for s in &matching {
                out.extend(s.iq_kv_range(lo, hi).await);
            }
            IqPayload::Entries(out)
        }
        IqOp::KvAll => {
            let mut out = Vec::new();
            for s in &matching {
                out.extend(s.iq_kv_all().await);
            }
            IqPayload::Entries(out)
        }
        IqOp::KvApproxCount => {
            let mut n = 0;
            for s in &matching {
                n += s.iq_kv_approx_count().await;
            }
            IqPayload::Count(n)
        }
        IqOp::WindowFetchSingle { key, window_start } => {
            let mut hit = None;
            for s in &matching {
                if let Some(v) = s.iq_window_fetch_single(key, *window_start).await {
                    hit = Some(v);
                    break;
                }
            }
            IqPayload::Value(hit)
        }
        IqOp::WindowFetch {
            key,
            time_from,
            time_to,
        } => {
            let mut out = Vec::new();
            for s in &matching {
                out.extend(s.iq_window_fetch(key, *time_from, *time_to).await);
            }
            IqPayload::WindowEntries(out)
        }
        IqOp::SessionFetchKey { key } => {
            let mut out = Vec::new();
            for s in &matching {
                out.extend(s.iq_session_fetch_key(key).await);
            }
            IqPayload::SessionEntries(out)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        processor::serde::{I64Serde, Serde, StringSerde},
        store::{
            api::{KeyValueStore, StateStore},
            kv::KeyValueBytesStore,
        },
    };

    #[tokio::test]
    async fn answer_kv_get_validate_wrongkind_notfound() {
        let mut s = KeyValueBytesStore::<String, i64>::in_memory(
            "c".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "c-changelog".into(),
        );
        s.put("x".into(), 7).await;
        let q = s.as_iq().unwrap();

        assert_eq!(
            answer_iq(vec![q], StoreKind::KeyValue, &IqOp::Validate, "c", true).await,
            Ok(IqPayload::Validated)
        );
        let got = answer_iq(
            vec![q],
            StoreKind::KeyValue,
            &IqOp::KvGet {
                key: StringSerde.serialize("t", &"x".to_string()),
            },
            "c",
            true,
        )
        .await;
        assert_eq!(got, Ok(IqPayload::Value(Some(I64Serde.serialize("t", &7)))));
        assert!(matches!(
            answer_iq(vec![q], StoreKind::Window, &IqOp::Validate, "c", true).await,
            Err(IqError::WrongStoreKind { .. })
        ));
        assert_eq!(
            answer_iq(
                vec![],
                StoreKind::KeyValue,
                &IqOp::Validate,
                "missing",
                true
            )
            .await,
            Err(IqError::StoreNotFound("missing".into()))
        );
        assert_eq!(
            answer_iq(
                vec![],
                StoreKind::KeyValue,
                &IqOp::Validate,
                "missing",
                false
            )
            .await,
            Err(IqError::RebalanceInProgress)
        );
    }
}

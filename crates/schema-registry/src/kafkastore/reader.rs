//! Group-less reader: tails `_schemas` partition 0 over a dedicated connection,
//! folds records into the shared store, and publishes the last-applied offset
//! (for read-your-writes). Mirrors remote-storage-topic's `partition_fetch_loop`.

use std::net::ToSocketAddrs;
use std::sync::Arc;

use crabka_client_core::{Connection, ConnectionOptions, fetch_partition};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use parking_lot::RwLock;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::RegistryConfig;
use crate::kafkastore::record::SchemaRecord;
use crate::store::StoreState;

/// Shared state + offset watch returned by [`spawn`].
pub struct StoreReader {
    pub store: Arc<RwLock<StoreState>>,
    pub applied_rx: watch::Receiver<i64>,
}

/// Apply one decoded record to the store. Returns nothing; idempotent for
/// SCHEMA records. Extracted for unit testing.
pub fn apply_record(store: &RwLock<StoreState>, rec: SchemaRecord) {
    match rec {
        SchemaRecord::Schema(k, v) => store.write().apply_schema(&k, &v),
        SchemaRecord::Config(k, v) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.set_subject_compat(&subj, v.compatibility_level),
                None => s.set_global_compat(v.compatibility_level),
            }
        }
        SchemaRecord::Noop | SchemaRecord::Unknown => {}
    }
}

/// Spawn the reader. Returns the shared store + an offset watch immediately; the
/// background task runs until `cancel` fires.
#[must_use]
pub fn spawn(cfg: &RegistryConfig, topic_id: WireUuid, cancel: CancellationToken) -> StoreReader {
    let store = Arc::new(RwLock::new(StoreState::default()));
    let (applied_tx, applied_rx) = watch::channel(-1_i64);
    let topic = cfg.schemas_topic.clone();
    let bootstrap = cfg.bootstrap.clone();
    let client_id = format!("{}-reader", cfg.client_id);
    let store_bg = store.clone();

    tokio::spawn(async move {
        let Some(addr) = bootstrap
            .split(',')
            .next()
            .and_then(|b| b.trim().to_socket_addrs().ok())
            .and_then(|mut a| a.next())
        else {
            tracing::error!(%bootstrap, "store reader: bad bootstrap addr");
            return;
        };
        let opts = ConnectionOptions {
            client_id,
            ..Default::default()
        };
        let conn = match Connection::connect_with_options(addr, opts).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "store reader: connect failed");
                return;
            }
        };
        let mut next = 0_i64;
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    conn.close();
                    return;
                }
                res = fetch_partition(&conn, &topic, topic_id, 0, next, 500, 1 << 20) => {
                    match res {
                        Ok(records) => {
                            for r in records {
                                if r.offset < next {
                                    continue;
                                }
                                let key = r.key.as_deref().unwrap_or_default();
                                apply_record(
                                    &store_bg,
                                    SchemaRecord::decode(key, r.value.as_deref()),
                                );
                                next = r.offset + 1;
                                let _ = applied_tx.send(r.offset);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "store reader: fetch error; backing off");
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        }
                    }
                }
            }
        }
    });

    StoreReader { store, applied_rx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafkastore::record::{SchemaKey, SchemaValue};

    #[test]
    fn apply_record_folds_schema_and_ignores_noop() {
        let store = RwLock::new(StoreState::default());
        let k = SchemaKey::new("av-value", 1);
        let v = SchemaValue {
            subject: "av-value".into(),
            version: 1,
            id: 1,
            schema_type: None,
            references: vec![],
            schema: "{\"type\":\"int\"}".into(),
            deleted: false,
        };
        apply_record(&store, SchemaRecord::Schema(k, v));
        apply_record(&store, SchemaRecord::Noop);
        assert_eq!(store.read().versions("av-value").unwrap(), vec![1]);
        assert_eq!(
            store.read().schema_by_id(1).unwrap().1,
            "{\"type\":\"int\"}"
        );
    }
}

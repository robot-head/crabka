//! Group-less reader: tails `_schemas` partition 0 over a dedicated connection,
//! folds records into the shared store, and publishes the last-applied offset
//! (for read-your-writes). Mirrors remote-storage-topic's `partition_fetch_loop`.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use crabka_client_core::{
    ClientError, ClientSecurity, Connection, ConnectionOptions, fetch_partition,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchErrorAction {
    Reconnect,
    RetrySameConnection,
}

fn fetch_error_action(e: &ClientError) -> FetchErrorAction {
    match e {
        ClientError::Connect { .. }
        | ClientError::Disconnected
        | ClientError::Timeout(_)
        | ClientError::Io(_) => FetchErrorAction::Reconnect,
        _ => FetchErrorAction::RetrySameConnection,
    }
}

fn resolve_bootstrap_addr(bootstrap: &str) -> Option<SocketAddr> {
    bootstrap
        .split(',')
        .filter_map(|b| b.trim().to_socket_addrs().ok())
        .find_map(|mut addrs| addrs.next())
}

async fn sleep_or_cancel(cancel: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

/// Apply one decoded record to the store. Returns nothing; idempotent for
/// SCHEMA records. Extracted for unit testing.
pub fn apply_record(store: &RwLock<StoreState>, rec: SchemaRecord) {
    match rec {
        SchemaRecord::Schema(k, v) => store.write().apply_schema(&k, &v),
        SchemaRecord::Tombstone(k) => {
            store
                .write()
                .permanent_delete_version(&k.subject, k.version);
        }
        SchemaRecord::DeleteSubject(k, _v) => {
            store.write().soft_delete_subject(&k.subject);
        }
        SchemaRecord::Mode(k, Some(v)) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.set_subject_mode(&subj, v.mode),
                None => s.set_global_mode(v.mode),
            }
        }
        SchemaRecord::Mode(k, None) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.clear_subject_mode(&subj),
                None => s.clear_global_mode(),
            }
        }
        SchemaRecord::Config(k, Some(v)) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.set_subject_compat(&subj, v.compatibility_level),
                None => s.set_global_compat(v.compatibility_level),
            }
        }
        SchemaRecord::Config(k, None) => {
            // CONFIG tombstone: clear the per-subject override so the subject
            // reverts to the global level.
            // Global CONFIG tombstones (k.subject = None) are ignored: there is
            // no DELETE /config endpoint in our REST surface, so we never emit
            // them, and ignoring them is safe if an external SR sends one.
            if let Some(subj) = k.subject {
                store.write().clear_subject_compat(&subj);
            }
        }
        SchemaRecord::Noop | SchemaRecord::Unknown => {}
    }
}

/// Spawn the reader. Returns the shared store + an offset watch immediately; the
/// background task runs until `cancel` fires.
#[must_use]
pub fn spawn(
    cfg: &RegistryConfig,
    topic_id: WireUuid,
    security: Option<ClientSecurity>,
    cancel: CancellationToken,
) -> StoreReader {
    let store = Arc::new(RwLock::new(StoreState::default()));
    let (applied_tx, applied_rx) = watch::channel(-1_i64);
    let topic = cfg.schemas_topic.clone();
    let bootstrap = cfg.bootstrap.clone();
    let client_id = format!("{}-reader", cfg.client_id);
    let store_bg = store.clone();

    tokio::spawn(async move {
        let opts = ConnectionOptions {
            client_id,
            security: security.map(Box::new),
            ..Default::default()
        };
        let mut next = 0_i64;
        loop {
            let Some(addr) = resolve_bootstrap_addr(&bootstrap) else {
                tracing::error!(%bootstrap, "store reader: bad bootstrap addr; backing off");
                if sleep_or_cancel(&cancel, Duration::from_millis(250)).await {
                    return;
                }
                continue;
            };
            let conn = match Connection::connect_with_options(addr, opts.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "store reader: connect failed; backing off");
                    if sleep_or_cancel(&cancel, Duration::from_millis(250)).await {
                        return;
                    }
                    continue;
                }
            };

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
                                let action = fetch_error_action(&e);
                                tracing::warn!(
                                    error = %e,
                                    action = ?action,
                                    "store reader: fetch error; backing off"
                                );
                                if action == FetchErrorAction::Reconnect {
                                    conn.close();
                                    if sleep_or_cancel(&cancel, Duration::from_millis(250)).await {
                                        return;
                                    }
                                    break;
                                }
                                if sleep_or_cancel(&cancel, Duration::from_millis(250)).await {
                                    return;
                                }
                            }
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
    use crabka_client_core::ClientError;
    use std::io;
    use std::time::Duration;

    #[test]
    fn apply_record_folds_schema_and_ignores_noop() {
        let store = RwLock::new(StoreState::default());
        let k = SchemaKey::new("av-value", 1);
        let v = SchemaValue {
            subject: "av-value".into(),
            version: 1,
            id: 1,
            schema_type: None,
            message_type: None,
            references: vec![],
            schema: "{\"type\":\"int\"}".into(),
            deleted: false,
        };
        apply_record(&store, SchemaRecord::Schema(k, v));
        apply_record(&store, SchemaRecord::Noop);
        assert_eq!(store.read().versions("av-value", false).unwrap(), vec![1]);
        assert_eq!(
            store.read().schema_by_id(1, false).unwrap().1,
            "{\"type\":\"int\"}"
        );
    }

    #[test]
    fn fetch_transport_errors_force_reader_reconnect() {
        assert_eq!(
            fetch_error_action(&ClientError::Disconnected),
            FetchErrorAction::Reconnect
        );
        assert_eq!(
            fetch_error_action(&ClientError::Timeout(Duration::from_millis(1))),
            FetchErrorAction::Reconnect
        );
        assert_eq!(
            fetch_error_action(&ClientError::Connect {
                addr: "127.0.0.1:9092".parse().unwrap(),
                source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
            }),
            FetchErrorAction::Reconnect
        );
        assert_eq!(
            fetch_error_action(&ClientError::Io(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "reset",
            ))),
            FetchErrorAction::Reconnect
        );
        assert_eq!(
            fetch_error_action(&ClientError::Server { error_code: 6 }),
            FetchErrorAction::RetrySameConnection
        );
    }

    #[test]
    fn apply_record_handles_mode_delete_tombstone() {
        use crate::kafkastore::record::{DeleteSubjectKey, DeleteSubjectValue, ModeKey, ModeValue};
        let store = RwLock::new(StoreState::default());
        let v = SchemaValue {
            subject: "s".into(),
            version: 1,
            id: 1,
            schema_type: None,
            message_type: None,
            references: vec![],
            schema: "{\"type\":\"int\"}".into(),
            deleted: false,
        };
        apply_record(&store, SchemaRecord::Schema(SchemaKey::new("s", 1), v));
        // global mode set then clear (Mode(None) -> clear_global_mode)
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: None,
                    magic: 0,
                },
                Some(ModeValue {
                    mode: "READONLY".into(),
                }),
            ),
        );
        assert_eq!(store.read().global_mode(), "READONLY");
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: None,
                    magic: 0,
                },
                None,
            ),
        );
        assert_eq!(store.read().global_mode(), "READWRITE");
        // subject mode set then clear
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: Some("s".into()),
                    magic: 0,
                },
                Some(ModeValue {
                    mode: "IMPORT".into(),
                }),
            ),
        );
        assert_eq!(store.read().subject_mode("s"), Some("IMPORT"));
        apply_record(
            &store,
            SchemaRecord::Mode(
                ModeKey {
                    keytype: "MODE".into(),
                    subject: Some("s".into()),
                    magic: 0,
                },
                None,
            ),
        );
        assert_eq!(store.read().subject_mode("s"), None);
        // soft-delete the subject, then permanently delete its version via a tombstone
        apply_record(
            &store,
            SchemaRecord::DeleteSubject(
                DeleteSubjectKey {
                    keytype: "DELETE_SUBJECT".into(),
                    subject: "s".into(),
                    magic: 0,
                },
                DeleteSubjectValue {
                    subject: "s".into(),
                    version: 1,
                },
            ),
        );
        assert!(store.read().versions("s", false).is_none());
        apply_record(&store, SchemaRecord::Tombstone(SchemaKey::new("s", 1)));
        assert!(store.read().versions("s", true).is_none());
    }
}

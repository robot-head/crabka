//! Broker-side heartbeat client. It sends `BrokerHeartbeat` to the
//! controller leader at every configured `heartbeat_interval`. It finds the
//! current controller in the metadata image, and it retries after transient
//! errors.

use std::sync::Arc;

use crabka_client_core::ConnectionOptions;
use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_security::ListenerProtocol;
use crabka_units::{Time, convert::TimeExt as _, fmt::Human as _, millis, secs};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) struct Config {
    pub broker_id: i32,
    pub interval: Time,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub shutdown: CancellationToken,
    /// Shared inter-broker dialer that reaches the controller leader.
    /// It runs TLS / SASL when the inter-broker listener needs them.
    /// If not, it uses plain TCP.
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub inter_broker_listener_protocol: ListenerProtocol,
    pub inter_broker_listener_name: String,
    /// When `true`, the client stamps `want_shut_down=true` on outbound
    /// `BrokerHeartbeat` requests.
    /// [`crate::BrokerHandle::controlled_shutdown`] drives this flag.
    pub want_shutdown: tokio::sync::watch::Receiver<bool>,
    /// The client sets this to `true` when the controller responds with
    /// `should_shut_down=true`. The caller of `controlled_shutdown`
    /// awaits this flag.
    pub should_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    /// Per-log-dir health registry. Each heartbeat reports the offline dirs
    /// to the controller as `offline_log_dirs` UUIDs (KIP-858).
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// Stable per-log-dir UUIDs, to translate offline dir paths to ids.
    pub log_dir_ids: crate::log_dir_id::LogDirIds,
    /// All configured log dirs. When every one of them is offline, the
    /// broker shuts itself down (KIP-112).
    pub all_log_dirs: Vec<std::path::PathBuf>,
    /// The broker cancels this when all dirs go offline. This stops
    /// replication and materialization against dead disks before teardown.
    pub supervisor_shutdown: tokio_util::sync::CancellationToken,
}

/// UUIDs of the currently-offline log dirs, for the heartbeat's `offline_log_dirs`.
fn offline_dir_uuids(
    status: &crate::log_dir_status::LogDirRegistry,
    ids: &crate::log_dir_id::LogDirIds,
) -> Vec<crabka_protocol::primitives::uuid::Uuid> {
    status
        .offline()
        .into_iter()
        .filter_map(|(path, _reason)| ids.id_for(&path))
        .map(|u| crabka_protocol::primitives::uuid::Uuid(*u.as_bytes()))
        .collect()
}

/// True when every configured log dir is offline. The broker then shuts itself
/// down.
fn all_dirs_offline(
    all_log_dirs: &[std::path::PathBuf],
    status: &crate::log_dir_status::LogDirRegistry,
) -> bool {
    !all_log_dirs.is_empty() && all_log_dirs.iter().all(|d| status.is_offline(d))
}

/// Returns `true` when every configured log dir is currently offline.
fn all_log_dirs_offline(cfg: &Config) -> bool {
    all_dirs_offline(&cfg.all_log_dirs, &cfg.log_dir_status)
}

fn heartbeat_rpc_timeout(interval: Time) -> Time {
    (interval * 2.0).max(millis(500)).min(secs(1))
}

fn heartbeat_connection_options(broker_id: i32, interval: Time) -> ConnectionOptions {
    let timeout = heartbeat_rpc_timeout(interval);
    ConnectionOptions {
        client_id: format!("crabka-broker-{broker_id}-heartbeat"),
        connect_timeout: timeout,
        request_timeout: timeout,
        ..ConnectionOptions::default()
    }
}

fn heartbeat_request(
    broker_id: i32,
    broker_epoch: i64,
    current_metadata_offset: i64,
    want_shut_down: bool,
    offline_log_dirs: Vec<crabka_protocol::primitives::uuid::Uuid>,
) -> BrokerHeartbeatRequest {
    BrokerHeartbeatRequest {
        broker_id,
        broker_epoch,
        current_metadata_offset,
        want_fence: false,
        want_shut_down,
        offline_log_dirs,
        ..Default::default()
    }
}

/// Triggers the KIP-112 self-shutdown. It latches `should_shutdown` and
/// cancels the supervisor. Every early-exit path calls it, so the check is not
/// accidentally skipped when the controller is temporarily unreachable.
fn trigger_all_dirs_offline_shutdown(cfg: &mut Config, reason: &str) {
    tracing::error!(
        reason,
        "all log dirs offline — initiating broker self-shutdown (KIP-112)"
    );
    let _ = cfg.should_shutdown.send(true);
    cfg.supervisor_shutdown.cancel();
}

pub(crate) async fn run(mut cfg: Config) {
    let mut tick = tokio::time::interval(cfg.interval.to_std());
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
        }
        // KIP-112 check: even if we cannot reach the controller, self-shutdown
        // must fire as long as every log dir is offline.
        if all_log_dirs_offline(&cfg) {
            trigger_all_dirs_offline_shutdown(&mut cfg, "detected before controller resolution");
            return;
        }
        // Resolve current controller leader's listen address from
        // the metadata image (via brokers() iteration), or skip this
        // tick if not yet known.
        let leader_id = *cfg.controller.watch_leader().borrow();
        let Some(leader_id) = leader_id else {
            debug!("heartbeat: no controller leader yet");
            continue;
        };
        let image = cfg.controller.current_image();
        let Some(broker_epoch) = image.broker_epoch(crabka_raft::NodeId(
            u64::try_from(cfg.broker_id).unwrap_or(u64::MAX),
        )) else {
            debug!(
                broker_id = cfg.broker_id,
                "heartbeat: broker registration not in metadata image yet"
            );
            continue;
        };
        let Some(broker_rec) = image.broker(leader_id) else {
            debug!("heartbeat: controller leader not in metadata image yet");
            continue;
        };
        // Prefer the inter-broker listener's endpoint when available;
        // fall back to the legacy top-level host/port. Mirrors the
        // resolution in the replicator supervisor.
        let (host, port) = broker_rec
            .endpoints
            .iter()
            .find(|e| e.name == cfg.inter_broker_listener_name)
            .map_or_else(
                || (broker_rec.host.clone(), broker_rec.port),
                |e| (e.host.clone(), e.port),
            );
        let opts = heartbeat_connection_options(cfg.broker_id, cfg.interval);
        let rpc_timeout = heartbeat_rpc_timeout(cfg.interval);
        let client_res = tokio::time::timeout(
            rpc_timeout.to_std(),
            cfg.inter_broker_client.connect_as_connection(
                &host,
                port,
                cfg.inter_broker_listener_protocol,
                "localhost",
                opts,
            ),
        )
        .await;
        let client = match client_res {
            Ok(Ok(client)) => client,
            Ok(Err(error)) => {
                debug!(%error, "heartbeat: connect failed");
                continue;
            }
            Err(_) => {
                debug!(
                    rpc_timeout = %rpc_timeout.human(),
                    "heartbeat: connect timed out"
                );
                continue;
            }
        };
        let want_shut_down = *cfg.want_shutdown.borrow_and_update();
        let offline_log_dirs = offline_dir_uuids(&cfg.log_dir_status, &cfg.log_dir_ids);
        let resp = tokio::time::timeout(
            rpc_timeout.to_std(),
            client.send(heartbeat_request(
                cfg.broker_id,
                broker_epoch,
                cfg.controller.current_metadata_offset(),
                want_shut_down,
                offline_log_dirs,
            )),
        )
        .await;
        match resp {
            Ok(Ok(r)) => {
                if r.error_code != crate::codes::NONE {
                    warn!(
                        error_code = r.error_code,
                        "heartbeat rejected by controller"
                    );
                    continue;
                }
                if r.should_shut_down {
                    // Latch true; never flip back. The
                    // `BrokerHandle::controlled_shutdown` waiter is
                    // single-shot.
                    let _ = cfg.should_shutdown.send(true);
                }
            }
            Ok(Err(e)) => warn!(error = %e, "heartbeat send failed"),
            Err(_) => warn!(
                rpc_timeout = %rpc_timeout.human(),
                "heartbeat send timed out"
            ),
        }

        // KIP-112: re-check after the heartbeat round-trip. This covers the
        // window where dirs went offline *during* the connect/send. The
        // top-of-tick check already handles dirs that were offline before
        // leader resolution; this one handles the same-tick race.
        if all_log_dirs_offline(&cfg) {
            trigger_all_dirs_offline_shutdown(&mut cfg, "detected after heartbeat send");
            // Returning stops heartbeats; if shutdown drags, the controller's
            // session timeout fences this broker independently.
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn offline_dir_uuids_maps_offline_paths() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let paths = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let ids = crate::log_dir_id::LogDirIds::resolve(&paths);
        let status = crate::log_dir_status::LogDirRegistry::probe(&paths);

        // Initially no dirs are offline.
        assert!(offline_dir_uuids(&status, &ids).is_empty());

        // Mark dir `a` as offline.
        status.mark_offline(a.path(), "test");
        let result = offline_dir_uuids(&status, &ids);
        assert!(result.len() == 1);
        let expected_id = ids.id_for(a.path()).unwrap();
        assert!(result[0].0 == *expected_id.as_bytes());
    }

    #[test]
    fn all_dirs_offline_true_only_when_every_dir_offline() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let paths = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let status = crate::log_dir_status::LogDirRegistry::probe(&paths);

        // Empty all_log_dirs: always false.
        assert!(!all_dirs_offline(&[], &status));

        // No dirs offline yet.
        assert!(!all_dirs_offline(&paths, &status));

        // Only `a` offline: still false.
        status.mark_offline(a.path(), "disk error");
        assert!(!all_dirs_offline(&paths, &status));

        // Both offline: true.
        status.mark_offline(b.path(), "disk error");
        assert!(all_dirs_offline(&paths, &status));
    }

    #[test]
    fn heartbeat_rpc_timeout_tracks_interval_with_bounds() {
        for (interval, want) in [
            (millis(50), millis(500)),
            (millis(500), secs(1)),
            (secs(5), secs(1)),
        ] {
            assert!(heartbeat_rpc_timeout(interval) == want, "{interval:?}");
        }
    }

    #[test]
    fn heartbeat_connection_options_use_bounded_rpc_timeout() {
        use assert2::check;
        let opts = heartbeat_connection_options(9, millis(500));

        check!(opts.client_id == "crabka-broker-9-heartbeat");
        check!(opts.connect_timeout == secs(1));
        check!(opts.request_timeout == secs(1));
    }

    #[test]
    fn heartbeat_request_reports_registration_and_applied_metadata() {
        let offline = crabka_protocol::primitives::uuid::Uuid([7; 16]);
        let req = heartbeat_request(3, 41, 47, true, vec![offline]);

        assert!(req.broker_id == 3);
        assert!(req.broker_epoch == 41);
        assert!(req.current_metadata_offset == 47);
        assert!(!req.want_fence);
        assert!(req.want_shut_down);
        assert!(req.offline_log_dirs == vec![offline]);
    }
}

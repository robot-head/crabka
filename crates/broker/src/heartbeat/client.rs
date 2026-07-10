//! Broker-side heartbeat client. Sends `BrokerHeartbeat` to the
//! controller leader every `heartbeat_interval_ms`. Discovers the
//! current controller via the metadata image; retries on transient
//! errors.

#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use crabka_client_core::ConnectionOptions;
use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_security::ListenerProtocol;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) struct Config {
    pub broker_id: i32,
    pub interval: Duration,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub shutdown: CancellationToken,
    /// Shared inter-broker dialer used to reach the controller leader.
    /// Runs TLS / SASL when the inter-broker listener requires them,
    /// otherwise falls back to plain TCP.
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub inter_broker_listener_protocol: ListenerProtocol,
    pub inter_broker_listener_name: String,
    /// When `true`, stamp `want_shut_down=true` on outbound
    /// `BrokerHeartbeat` requests. Driven by
    /// [`crate::BrokerHandle::controlled_shutdown`].
    pub want_shutdown: tokio::sync::watch::Receiver<bool>,
    /// Set to `true` when the controller responds with
    /// `should_shut_down=true`. The caller of `controlled_shutdown`
    /// awaits this flag.
    pub should_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    /// Per-log-dir health registry; offline dirs are reported to the
    /// controller as `offline_log_dirs` UUIDs each heartbeat (KIP-858).
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// Stable per-log-dir UUIDs, to translate offline dir paths → ids.
    pub log_dir_ids: crate::log_dir_id::LogDirIds,
    /// All configured log dirs; when every one is offline the broker
    /// self-shuts-down (KIP-112).
    pub all_log_dirs: Vec<std::path::PathBuf>,
    /// Cancelled on all-dirs-offline to stop replication/materialization
    /// against dead disks before teardown.
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

/// True when every configured log dir is offline (→ broker self-shutdown).
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

fn heartbeat_rpc_timeout(interval: Duration) -> Duration {
    interval
        .saturating_mul(2)
        .max(Duration::from_millis(500))
        .min(Duration::from_secs(1))
}

fn heartbeat_connection_options(broker_id: i32, interval: Duration) -> ConnectionOptions {
    let timeout = heartbeat_rpc_timeout(interval);
    ConnectionOptions {
        client_id: format!("crabka-broker-{broker_id}-heartbeat"),
        connect_timeout: timeout,
        request_timeout: timeout,
        ..ConnectionOptions::default()
    }
}

/// Trigger the KIP-112 self-shutdown: latch `should_shutdown` and cancel
/// the supervisor. Called from every early-exit path so the check is not
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
    let mut tick = tokio::time::interval(cfg.interval);
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
            rpc_timeout,
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
                    timeout_ms = rpc_timeout.as_millis(),
                    "heartbeat: connect timed out"
                );
                continue;
            }
        };
        let want_shut_down = *cfg.want_shutdown.borrow_and_update();
        let offline_log_dirs = offline_dir_uuids(&cfg.log_dir_status, &cfg.log_dir_ids);
        let resp = tokio::time::timeout(
            rpc_timeout,
            client.send(BrokerHeartbeatRequest {
                broker_id: cfg.broker_id,
                broker_epoch: 0,
                current_metadata_offset: 0,
                want_fence: false,
                want_shut_down,
                offline_log_dirs,
                ..Default::default()
            }),
        )
        .await;
        match resp {
            Ok(Ok(r)) => {
                if r.should_shut_down {
                    // Latch true; never flip back. The
                    // `BrokerHandle::controlled_shutdown` waiter is
                    // single-shot.
                    let _ = cfg.should_shutdown.send(true);
                }
            }
            Ok(Err(e)) => warn!(error = %e, "heartbeat send failed"),
            Err(_) => warn!(
                timeout_ms = rpc_timeout.as_millis(),
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
        assert2::assert!(offline_dir_uuids(&status, &ids).is_empty());

        // Mark dir `a` as offline.
        status.mark_offline(a.path(), "test");
        let result = offline_dir_uuids(&status, &ids);
        let expected_id = ids.id_for(a.path()).unwrap();
        assert2::assert!((result.len(), result[0].0) == (1, *expected_id.as_bytes()));
    }

    #[test]
    fn all_dirs_offline_true_only_when_every_dir_offline() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let paths = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let status = crate::log_dir_status::LogDirRegistry::probe(&paths);

        // Empty all_log_dirs: always false.
        assert2::assert!(!all_dirs_offline(&[], &status));

        // No dirs offline yet.
        assert2::assert!(!all_dirs_offline(&paths, &status));

        // Only `a` offline: still false.
        status.mark_offline(a.path(), "disk error");
        assert2::assert!(!all_dirs_offline(&paths, &status));

        // Both offline: true.
        status.mark_offline(b.path(), "disk error");
        assert2::assert!(all_dirs_offline(&paths, &status));
    }

    #[test]
    fn heartbeat_rpc_timeout_tracks_interval_with_bounds() {
        for (interval, want) in [
            (Duration::from_millis(50), Duration::from_millis(500)),
            (Duration::from_millis(500), Duration::from_secs(1)),
            (Duration::from_secs(5), Duration::from_secs(1)),
        ] {
            assert2::assert!(heartbeat_rpc_timeout(interval) == want);
        }
    }

    #[test]
    fn heartbeat_connection_options_use_bounded_rpc_timeout() {
        use assert2::check;
        let opts = heartbeat_connection_options(9, Duration::from_millis(500));

        check!(
            (
                opts.client_id.as_str(),
                opts.connect_timeout,
                opts.request_timeout
            ) == (
                "crabka-broker-9-heartbeat",
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
        );
    }
}

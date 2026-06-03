//! Broker-side heartbeat client. Sends `BrokerHeartbeat` to the
//! controller leader every `heartbeat_interval_ms`. Discovers the
//! current controller via the metadata image; retries on transient
//! errors.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

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

/// Returns `true` when every configured log dir is currently offline.
fn all_log_dirs_offline(cfg: &Config) -> bool {
    !cfg.all_log_dirs.is_empty()
        && cfg
            .all_log_dirs
            .iter()
            .all(|d| cfg.log_dir_status.is_offline(d))
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
        let opts = ConnectionOptions {
            client_id: format!("crabka-broker-{}-heartbeat", cfg.broker_id),
            ..ConnectionOptions::default()
        };
        let client_res = cfg
            .inter_broker_client
            .connect_as_connection(
                &host,
                port,
                cfg.inter_broker_listener_protocol,
                "localhost",
                opts,
            )
            .await;
        let Ok(client) = client_res else {
            debug!("heartbeat: connect failed");
            continue;
        };
        let want_shut_down = *cfg.want_shutdown.borrow_and_update();
        let offline_log_dirs: Vec<crabka_protocol::primitives::uuid::Uuid> = cfg
            .log_dir_status
            .offline()
            .into_iter()
            .filter_map(|(path, _reason)| cfg.log_dir_ids.id_for(&path))
            .map(|u| crabka_protocol::primitives::uuid::Uuid(*u.as_bytes()))
            .collect();
        let resp = client
            .send(BrokerHeartbeatRequest {
                broker_id: cfg.broker_id,
                broker_epoch: 0,
                current_metadata_offset: 0,
                want_fence: false,
                want_shut_down,
                offline_log_dirs,
                ..Default::default()
            })
            .await;
        match resp {
            Ok(r) => {
                if r.should_shut_down {
                    // Latch true; never flip back. The
                    // `BrokerHandle::controlled_shutdown` waiter is
                    // single-shot.
                    let _ = cfg.should_shutdown.send(true);
                }
            }
            Err(e) => warn!(error = %e, "heartbeat send failed"),
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

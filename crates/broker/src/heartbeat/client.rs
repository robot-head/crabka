//! Broker-side heartbeat client. Sends `BrokerHeartbeat` to the
//! controller leader every `heartbeat_interval_ms`. Discovers the
//! current controller via the metadata image; retries on transient
//! errors.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use crabka_client_core::ConnectionOptions;
use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_raft::ControllerHandle;
use crabka_security::ListenerProtocol;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) struct Config {
    pub broker_id: i32,
    pub interval: Duration,
    pub controller: Arc<ControllerHandle>,
    pub shutdown: CancellationToken,
    /// Shared inter-broker dialer used to reach the controller leader.
    /// Runs TLS / SASL when the inter-broker listener requires them,
    /// otherwise falls back to plain TCP.
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub inter_broker_listener_protocol: ListenerProtocol,
    pub inter_broker_listener_name: String,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(cfg.interval);
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
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
        let resp = client
            .send(BrokerHeartbeatRequest {
                broker_id: cfg.broker_id,
                broker_epoch: 0,
                current_metadata_offset: 0,
                want_fence: false,
                want_shut_down: false,
                ..Default::default()
            })
            .await;
        if let Err(e) = resp {
            warn!(error = %e, "heartbeat send failed");
        }
    }
}

//! Broker-side heartbeat client. Sends `BrokerHeartbeat` to the
//! controller leader every `heartbeat_interval_ms`. Discovers the
//! current controller via the metadata image; retries on transient
//! errors.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_raft::ControllerHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) struct Config {
    pub broker_id: i32,
    pub interval: Duration,
    pub controller: Arc<ControllerHandle>,
    pub shutdown: CancellationToken,
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
        let addr = format!("{}:{}", broker_rec.host, broker_rec.port);
        let client_res = crabka_client_core::Client::builder()
            .bootstrap(addr)
            .client_id(format!("crabka-broker-{}-heartbeat", cfg.broker_id))
            .build()
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

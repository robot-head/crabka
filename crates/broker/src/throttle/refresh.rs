//! Background task that subscribes to `MetadataImage` changes and
//! updates the throttle bucket rates. Runs unconditionally on every
//! broker; the bucket itself handles the unthrottled fast path.

use std::sync::Arc;

use async_trait::async_trait;
use crabka_metadata::{MetadataImage, NodeId, ThrottleKind};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::ThrottleState;

#[async_trait]
pub trait ImageWatcher: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
}

pub async fn run(
    controller: Arc<dyn ImageWatcher>,
    node_id: NodeId,
    throttle: Arc<ThrottleState>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    // Apply initial state.
    apply_image(&controller.current_image(), node_id, &throttle);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                info!("throttle refresh task shutting down");
                return;
            }
            r = watcher.changed() => {
                if r.is_err() {
                    info!("throttle refresh task: image channel closed");
                    return;
                }
            }
        }
        apply_image(&controller.current_image(), node_id, &throttle);
    }
}

fn apply_image(image: &MetadataImage, node_id: NodeId, throttle: &ThrottleState) {
    let leader_rate = image
        .broker_throttle_rate(node_id, ThrottleKind::Leader)
        .unwrap_or(0);
    let follower_rate = image
        .broker_throttle_rate(node_id, ThrottleKind::Follower)
        .unwrap_or(0);
    if throttle.leader_out.rate() != leader_rate {
        debug!(node_id, leader_rate, "throttle: leader-out rate update");
        throttle.leader_out.set_rate(leader_rate);
    }
    if throttle.follower_in.rate() != follower_rate {
        debug!(node_id, follower_rate, "throttle: follower-in rate update");
        throttle.follower_in.set_rate(follower_rate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{BrokerConfigRecord, MetadataRecord};
    use uuid::Uuid;

    #[test]
    fn apply_image_sets_rates() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "follower.replication.throttled.rate".into(),
            config_value: Some("1024".into()),
        }));
        let throttle = ThrottleState::new();
        apply_image(&img, 1, &throttle);
        assert!(throttle.leader_out.rate() == 2048);
        assert!(throttle.follower_in.rate() == 1024);
    }

    #[test]
    fn apply_image_resets_to_zero_when_config_deleted() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        let throttle = ThrottleState::new();
        apply_image(&img, 1, &throttle);
        assert!(throttle.leader_out.rate() == 2048);
        // Delete the config.
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: None,
        }));
        apply_image(&img, 1, &throttle);
        assert!(throttle.leader_out.rate() == 0);
    }
}

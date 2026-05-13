//! Per-leader-partition ISR maintenance. Compares each follower's
//! last-fetch time vs `replica_lag_time_max_ms` and proposes
//! `AlterPartition` shrink/expand to the controller leader.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use crabka_raft::{ControllerHandle, NodeId};
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::partition::Partition;

pub(crate) struct Config {
    pub node_id: NodeId,
    pub partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub controller: Arc<ControllerHandle>,
    pub replica_lag_time_max: Duration,
    pub broker_id: i32,
    pub shutdown: CancellationToken,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
        }
        // Snapshot the keys to avoid holding the DashMap iterator across awaits.
        let keys: Vec<(String, i32)> = cfg
            .partitions
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for key in keys {
            let Some(part) = cfg.partitions.get(&key).map(|e| e.value().clone()) else {
                continue;
            };
            if part.current_leader.load(std::sync::atomic::Ordering::Acquire) != cfg.node_id {
                continue;
            }
            let Some((new_isr, leader_epoch)) =
                compute_proposal(&part, cfg.replica_lag_time_max).await
            else {
                continue;
            };
            if let Err(e) = send_alter_partition(
                &cfg.controller,
                cfg.broker_id,
                &key.0,
                key.1,
                new_isr,
                leader_epoch,
            )
            .await
            {
                warn!(topic = %key.0, partition = key.1, error = %e,
                    "AlterPartition propose failed");
            }
        }
    }
}

/// Returns `Some((new_isr, leader_epoch))` if the ISR should change,
/// else `None`.
async fn compute_proposal(
    part: &Partition,
    lag_max: Duration,
) -> Option<(Vec<NodeId>, i32)> {
    let st = part.replica_state.lock().await;
    let now = Instant::now();
    let mut new_isr: Vec<NodeId> = st.isr.iter().copied().collect();
    // Sort for deterministic comparisons later.
    new_isr.sort_unstable();
    // Shrink: drop followers lagging > lag_max.
    new_isr.retain(|n| {
        st.per_follower
            .get(n)
            .is_none_or(|stats| now.duration_since(stats.last_fetch) <= lag_max)
    });
    // Expand: add followers in per_follower not in current ISR that have
    // been recently caught up.
    for (n, stats) in &st.per_follower {
        if !st.isr.contains(n)
            && now.duration_since(stats.last_caught_up) <= lag_max
            && !new_isr.contains(n)
        {
            new_isr.push(*n);
        }
    }
    new_isr.sort_unstable();
    let mut current_isr: Vec<NodeId> = st.isr.iter().copied().collect();
    current_isr.sort_unstable();
    let no_change = new_isr == current_isr;
    if no_change {
        None
    } else {
        Some((new_isr, st.current_leader_epoch))
    }
}

async fn send_alter_partition(
    controller: &Arc<ControllerHandle>,
    broker_id: i32,
    topic: &str,
    partition: i32,
    new_isr: Vec<NodeId>,
    leader_epoch: i32,
) -> Result<(), String> {
    use crabka_protocol::owned::alter_partition_request::{
        AlterPartitionRequest, PartitionData, TopicData,
    };

    // Look up the controller leader's address via metadata image.
    let leader_id = *controller.watch_leader().borrow();
    let Some(leader_id) = leader_id else {
        return Err("no controller leader".into());
    };
    let image = controller.current_image();
    let Some(broker_rec) = image.broker(leader_id) else {
        return Err("controller leader not in image".into());
    };
    let addr = format!("{}:{}", broker_rec.host, broker_rec.port);

    // Look up topic_id from the metadata image and convert to the protocol Uuid type.
    let topic_id = {
        let raw: [u8; 16] = image
            .topic(topic)
            .map_or([0u8; 16], |t| *t.topic_id.as_bytes());
        crabka_protocol::primitives::uuid::Uuid(raw)
    };

    // The wire format uses new_isr: Vec<i32> (versions 2–3 both send it;
    // version 3 adds new_isr_with_epochs but we send version 2 for simplicity).
    let new_isr_i32: Vec<i32> = new_isr
        .iter()
        .map(|n| i32::try_from(*n).unwrap_or(i32::MAX))
        .collect();

    let req = AlterPartitionRequest {
        broker_id,
        broker_epoch: -1,
        topics: vec![TopicData {
            topic_id,
            partitions: vec![PartitionData {
                partition_index: partition,
                leader_epoch,
                new_isr: new_isr_i32,
                new_isr_with_epochs: Vec::new(),
                leader_recovery_state: 0,
                partition_epoch: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let client = crabka_client_core::Client::builder()
        .bootstrap(addr)
        .client_id(format!("crabka-broker-{broker_id}-isr"))
        .build()
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let _resp = client.send(req).await.map_err(|e| format!("send: {e}"))?;
    debug!(
        topic = topic,
        partition = partition,
        new_isr_len = new_isr.len(),
        "AlterPartition proposed"
    );
    Ok(())
}

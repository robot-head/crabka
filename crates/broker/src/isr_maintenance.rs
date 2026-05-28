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
    /// Bumped on each proposed shrink / expand.
    pub metrics: crate::metrics::BrokerMetrics,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
        }
        // Snapshot the keys to avoid holding the DashMap iterator across awaits.
        let keys: Vec<(String, i32)> = cfg.partitions.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            let Some(part) = cfg.partitions.get(&key).map(|e| e.value().clone()) else {
                continue;
            };
            if part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                != cfg.node_id
            {
                continue;
            }
            let Some((new_isr, leader_epoch)) =
                compute_proposal(&part, cfg.replica_lag_time_max).await
            else {
                continue;
            };
            // Classify the proposal as shrink/expand by
            // comparing membership against the pre-proposal ISR.
            // `compute_proposal` already filtered for "actually
            // changed", so at least one of these bumps fires.
            let prev_isr: std::collections::HashSet<NodeId> = {
                let st = part.replica_state.lock().await;
                st.isr.iter().copied().collect()
            };
            let next_isr: std::collections::HashSet<NodeId> = new_isr.iter().copied().collect();
            if prev_isr.difference(&next_isr).next().is_some() {
                cfg.metrics.isr_shrinks_total.inc();
            }
            if next_isr.difference(&prev_isr).next().is_some() {
                cfg.metrics.isr_expands_total.inc();
            }
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
async fn compute_proposal(part: &Partition, lag_max: Duration) -> Option<(Vec<NodeId>, i32)> {
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
        AlterPartitionRequest, BrokerState, PartitionData, TopicData,
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

    // `new_isr` is the v2 field (versions 2 only on the wire).
    // `new_isr_with_epochs` is the v3 field; the client negotiates MAX_VERSION
    // (= 3), so we must populate both so that whichever version is selected
    // carries the correct ISR.  The handler side reads `new_isr_with_epochs`
    // when `new_isr` is empty (i.e. version 3).  Broker epochs are unknown
    // at this call site so we send -1 (the standard "unknown epoch" sentinel).
    let new_isr_i32: Vec<i32> = new_isr
        .iter()
        .map(|n| i32::try_from(*n).unwrap_or(i32::MAX))
        .collect();
    let new_isr_with_epochs: Vec<BrokerState> = new_isr_i32
        .iter()
        .map(|&bid| BrokerState {
            broker_id: bid,
            broker_epoch: -1,
            ..Default::default()
        })
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
                new_isr_with_epochs,
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

    let resp = client.send(req).await.map_err(|e| format!("send: {e}"))?;
    // Log the global error code and per-partition error codes so failures
    // are visible (previously _resp was discarded, hiding non-zero codes).
    let global_err = resp.error_code;
    let part_err = resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map_or(0, |p| p.error_code);
    if global_err != 0 || part_err != 0 {
        warn!(
            topic = topic,
            partition = partition,
            new_isr_len = new_isr.len(),
            global_error_code = global_err,
            partition_error_code = part_err,
            "AlterPartition rejected by controller"
        );
        return Err(format!(
            "AlterPartition rejected: global={global_err} partition={part_err}"
        ));
    }
    debug!(
        topic = topic,
        partition = partition,
        new_isr_len = new_isr.len(),
        "AlterPartition proposed"
    );
    Ok(())
}

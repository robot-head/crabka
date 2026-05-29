//! Per-leader-partition ISR maintenance. Compares each follower's
//! last-fetch time vs `replica_lag_time_max_ms` and proposes
//! `AlterPartition` shrink/expand to the controller leader.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use crabka_raft::NodeId;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::partition::Partition;
use crate::partition_registry::PartitionRegistry;

pub(crate) struct Config {
    pub node_id: NodeId,
    pub partitions: Arc<PartitionRegistry>,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub replica_lag_time_max: Duration,
    pub broker_id: i32,
    pub shutdown: CancellationToken,
    /// Bumped on each proposed shrink / expand.
    pub metrics: crate::metrics::BrokerMetrics,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    // Reused across ticks to avoid re-allocating the snapshot Vec each second.
    // Holds cheap `Arc<Partition>` clones (no String allocation, no second
    // registry lookup). Cleared and refilled each tick.
    let mut snapshot: Vec<Arc<Partition>> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
        }
        // Snapshot the partition values as cheap Arc clones in a single
        // iteration, then iterate the owned `Vec` so we never hold a shard
        // guard across a yield point.
        snapshot.clear();
        snapshot.extend(cfg.partitions.arcs());
        for part in snapshot.drain(..) {
            if part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                != cfg.node_id
            {
                continue;
            }
            let Some(proposal) = compute_proposal(&part, cfg.replica_lag_time_max).await else {
                continue;
            };
            // Classify the proposal as shrink/expand using the ISRs captured
            // inside `compute_proposal`'s single lock scope. `compute_proposal`
            // already filtered for "actually changed", so at least one of these
            // bumps fires. Reusing its captured `prev_isr` avoids a second
            // `replica_state` lock and closes the TOCTOU window where the ISR
            // could change between the two acquisitions.
            let prev_isr: std::collections::HashSet<NodeId> =
                proposal.prev_isr.iter().copied().collect();
            let next_isr: std::collections::HashSet<NodeId> =
                proposal.new_isr.iter().copied().collect();
            if prev_isr.difference(&next_isr).next().is_some() {
                cfg.metrics.isr_shrinks_total.inc();
            }
            if next_isr.difference(&prev_isr).next().is_some() {
                cfg.metrics.isr_expands_total.inc();
            }
            if let Err(e) = send_alter_partition(
                &cfg.controller,
                cfg.broker_id,
                &part.topic,
                part.partition_id,
                proposal.new_isr,
                proposal.leader_epoch,
            )
            .await
            {
                warn!(topic = %part.topic, partition = part.partition_id, error = %e,
                    "AlterPartition propose failed");
            }
        }
    }
}

/// A computed ISR change proposal. All fields are captured within
/// `compute_proposal`'s single `replica_state` lock scope so the caller
/// can classify shrink/expand and submit the proposal without re-locking
/// (and without a TOCTOU window where the ISR shifts between locks).
struct Proposal {
    /// The pre-proposal ISR (sorted), used by the caller for shrink/expand
    /// metric classification.
    prev_isr: Vec<NodeId>,
    /// The proposed new ISR (sorted). Guaranteed `!= prev_isr`.
    new_isr: Vec<NodeId>,
    /// Leader epoch to stamp on the `AlterPartition` request.
    leader_epoch: i32,
}

/// Returns `Some(Proposal)` if the ISR should change, else `None`.
async fn compute_proposal(part: &Partition, lag_max: Duration) -> Option<Proposal> {
    let st = part.replica_state.lock().await;
    let now = Instant::now();
    // Capture the pre-proposal ISR (sorted) once, inside this lock scope.
    let mut prev_isr: Vec<NodeId> = st.isr.iter().copied().collect();
    prev_isr.sort_unstable();
    let mut new_isr: Vec<NodeId> = prev_isr.clone();
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
    let no_change = new_isr == prev_isr;
    if no_change {
        None
    } else {
        Some(Proposal {
            prev_isr,
            new_isr,
            leader_epoch: st.current_leader_epoch,
        })
    }
}

async fn send_alter_partition(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
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

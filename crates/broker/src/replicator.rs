//! Per-(topic, partition) replication task. Issues standard Kafka `Fetch`
//! requests against the partition's leader (with `replica_id` set to the
//! local broker's `node_id`), appending each returned batch to the local
//! `crabka-log`. Handles `OFFSET_OUT_OF_RANGE` by truncating local log to
//! 0 and restarting; `NOT_LEADER_FOR_PARTITION` by returning so the
//! supervisor's next reconcile re-evaluates.

// `log_config` is a conventional field name; the "ends with struct name" lint
// is a false positive here.
#![allow(clippy::struct_field_names)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crabka_client_core::{ClientError, Connection, ConnectionOptions};
use crabka_log::{Log, LogConfig};
use crabka_protocol::owned::fetch_request::{
    FetchPartition, FetchRequest, FetchTopic, ReplicaState,
};
use crabka_protocol::owned::fetch_response::FetchResponse;
use crabka_protocol::owned::offset_for_leader_epoch_request::{
    OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_raft::NodeId;
use crabka_security::ListenerProtocol;

use crate::broker::spawn_partition;
use crate::codes;
use crate::partition::Partition;
use crate::throttle::{ThrottleState, TopicThrottle};

const FETCH_MAX_BYTES: i32 = 1 << 20;
const FETCH_MAX_WAIT_MS: i32 = 500;
const FETCH_MIN_BYTES: i32 = 1;

/// Configuration handed to a single replicator task.
pub(crate) struct Config {
    pub node_id: NodeId,
    pub topic: String,
    /// Wire-format `topic_id` for the partition. Required so the
    /// `Fetch` request can populate the v13+ wire field — at v ≥ 13
    /// Kafka drops `FetchTopic.topic` in favour of `topic_id` (KIP-516),
    /// and the leader's handler resolves topic-name purely via
    /// `topic_id`. If we send `WireUuid::ZERO` here the leader returns
    /// `UNKNOWN_TOPIC_OR_PARTITION` for every fetch.
    pub topic_id: WireUuid,
    pub partition: i32,
    pub leader_node_id: NodeId,
    /// Leader's `host` portion from the metadata image (the inter-broker
    /// endpoint when available, otherwise the legacy broker host).
    pub leader_host: String,
    pub leader_port: u16,
    pub partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub log_dirs: Vec<PathBuf>,
    pub log_config: LogConfig,
    pub client_id: String,
    pub shutdown: CancellationToken,
    /// Shared outbound dialer. Connects through TLS + SASL when the
    /// inter-broker listener requires them; falls back to raw TCP for
    /// PLAINTEXT.
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub inter_broker_listener_protocol: ListenerProtocol,
    /// KIP-73: broker-wide throttle state. The follower-in bucket gates
    /// outbound Fetch bytes when this partition is throttled.
    pub throttle_state: Arc<ThrottleState>,
    /// Controller handle used to read the current metadata image each
    /// Fetch round (for `follower.replication.throttled.replicas` lookup).
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    /// KIP-113 runtime offline-dir registry. Forwarded into
    /// `spawn_partition` so the per-partition writer can flip the
    /// owning dir offline on a segment-write / fsync failure.
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// Broker-wide metrics handle so the replicator can
    /// increment `replication_bytes_in` after a successful follower-
    /// side append.
    pub metrics: crate::metrics::BrokerMetrics,
}

/// Entry point: drive a single (topic, partition) replication loop until
/// cancelled.
pub(crate) async fn run(cfg: Config) {
    info!(
        topic = %cfg.topic,
        partition = cfg.partition,
        leader_node_id = cfg.leader_node_id,
        "replicator.started"
    );

    // First-run materialization of the local on-disk partition.
    if let Err(e) = ensure_local_partition(&cfg) {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
            "replicator failed to open local partition; aborting");
        return;
    }

    if let Err(e) = run_inner(&cfg).await {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
            "replicator stopped on unrecoverable error");
    }

    info!(topic = %cfg.topic, partition = cfg.partition, "replicator.stopped");
}

/// Build (or recover) the on-disk `Partition` for this follower, inserting
/// it into the broker's shared `partitions` map. Idempotent.
fn ensure_local_partition(cfg: &Config) -> Result<(), String> {
    if cfg
        .partitions
        .contains_key(&(cfg.topic.clone(), cfg.partition))
    {
        return Ok(());
    }
    let dir = crate::log_dir::place_partition_dir(&cfg.log_dirs, &cfg.topic, cfg.partition);
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let log = Log::open(&dir, cfg.log_config.clone()).map_err(|e| format!("Log::open: {e}"))?;
    let owning_dir = dir
        .parent()
        .expect("placed partition dir always has a parent log.dir")
        .to_path_buf();
    let part = spawn_partition(
        cfg.topic.clone(),
        cfg.partition,
        owning_dir,
        log,
        cfg.log_dir_status.clone(),
    );
    cfg.partitions
        .insert((cfg.topic.clone(), cfg.partition), part);
    Ok(())
}

async fn run_inner(cfg: &Config) -> Result<(), String> {
    let mut client = connect_with_backoff(cfg).await?;

    loop {
        if cfg.shutdown.is_cancelled() {
            return Ok(());
        }

        // Read the local log's next offset so the leader knows where to
        // resume from. Cheap: takes the partition's log mutex briefly.
        let fetch_offset = {
            let entry = cfg
                .partitions
                .get(&(cfg.topic.clone(), cfg.partition))
                .ok_or_else(|| "local partition missing".to_string())?;
            entry.value().log_end_offset()
        };

        // KIP-73: follower-side throttle. Check the current metadata image
        // to see if this (partition, node) pair is in the follower throttled
        // replicas list. If so, cap the request size via the follower-in
        // token bucket.
        //
        // The replicator already issues one Fetch per (topic, partition), so
        // throttled-partition Fetch isolation is free — no need to split
        // requests here. We set `partition_max_bytes` on the single partition
        // in the request to the bucket-granted amount.
        let partition_max_bytes_cap = {
            let image = cfg.controller.current_image();
            let throttle = TopicThrottle::for_topic(&image, &cfg.topic);
            let throttled = throttle.follower.contains(cfg.partition, cfg.node_id);
            if throttled && cfg.throttle_state.follower_in.rate() > 0 {
                let granted = cfg
                    .throttle_state
                    .follower_in
                    .try_consume(u64::try_from(FETCH_MAX_BYTES).unwrap_or(0));
                if granted == 0 {
                    tracing::debug!(
                        topic = %cfg.topic,
                        partition = cfg.partition,
                        "follower throttle: skip fetch this round (bucket exhausted)"
                    );
                    // Bucket exhausted — yield and retry next loop iteration.
                    tokio::select! {
                        () = cfg.shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(Duration::from_millis(100)) => {}
                    }
                    continue;
                }
                i32::try_from(granted).unwrap_or(FETCH_MAX_BYTES)
            } else {
                FETCH_MAX_BYTES
            }
        };

        let req = build_fetch_request(cfg, fetch_offset, partition_max_bytes_cap);

        let send = tokio::select! {
            () = cfg.shutdown.cancelled() => return Ok(()),
            r = client.send(req) => r,
        };

        let resp: FetchResponse = match send {
            Ok(r) => r,
            // Transport / framing failure: drop the client and reconnect.
            Err(ClientError::Disconnected | ClientError::Io(_)) => {
                client = connect_with_backoff(cfg).await?;
                continue;
            }
            Err(e) => {
                warn!(error = %e,
                    "replicator: client.send unexpected error; retrying after backoff");
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                client = connect_with_backoff(cfg).await?;
                continue;
            }
        };

        match handle_response(&resp, cfg).await {
            LoopAction::Continue => {}
            LoopAction::StopNotLeader => {
                info!(topic = %cfg.topic, partition = cfg.partition,
                    "replicator.not_leader; supervisor will re-evaluate");
                return Ok(());
            }
        }
    }
}

/// Build a single-partition Fetch request for the (topic, partition) this
/// replicator is responsible for. `replica_id` is set to the local broker
/// so the leader treats this as a follower fetch rather than a consumer
/// fetch (Kafka's high-watermark semantics differ between the two).
///
/// KIP-101: `current_leader_epoch` is included so the leader can detect
/// stale or fenced replicas and return `FENCED_LEADER_EPOCH` or
/// `UNKNOWN_LEADER_EPOCH` when appropriate.
///
/// `partition_max_bytes_cap` is the KIP-73 follower-throttle cap for
/// `partition_max_bytes`. Pass `FETCH_MAX_BYTES` when unthrottled.
fn build_fetch_request(
    cfg: &Config,
    fetch_offset: i64,
    partition_max_bytes_cap: i32,
) -> FetchRequest {
    let leader_epoch = cfg
        .partitions
        .get(&(cfg.topic.clone(), cfg.partition))
        .map_or(-1, |entry| {
            entry
                .value()
                .current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire)
        });
    // `replica_id` is the wire field on Fetch v0-14. KIP-903 (Kafka 3.5) moved
    // it into a tagged `replica_state` struct on v15+; the codegen serializes
    // whichever the negotiated version requires. Populate BOTH so the request
    // is correct regardless of which version the leader negotiates.
    let rid = i32::try_from(cfg.node_id).unwrap_or(-1);
    FetchRequest {
        replica_id: rid,
        replica_state: ReplicaState {
            replica_id: rid,
            ..ReplicaState::default()
        },
        max_wait_ms: FETCH_MAX_WAIT_MS,
        min_bytes: FETCH_MIN_BYTES,
        max_bytes: FETCH_MAX_BYTES,
        topics: vec![FetchTopic {
            topic: cfg.topic.clone(),
            topic_id: cfg.topic_id,
            partitions: vec![FetchPartition {
                partition: cfg.partition,
                fetch_offset,
                current_leader_epoch: leader_epoch,
                partition_max_bytes: partition_max_bytes_cap,
                ..FetchPartition::default()
            }],
            ..FetchTopic::default()
        }],
        ..FetchRequest::default()
    }
}

/// Outcome of one fetch round.
enum LoopAction {
    Continue,
    StopNotLeader,
}

async fn handle_response(resp: &FetchResponse, cfg: &Config) -> LoopAction {
    // The replicator only ever requests one (topic, partition) per Fetch.
    // Match by either `topic` (v ≤ 12) or `topic_id` (v ≥ 13) so that
    // when the negotiated wire format drops the topic-name field
    // (KIP-516) we still find our partition. Without this fallback
    // every fetch silently no-ops at v ≥ 13 because `t.topic == ""`.
    let Some(part_resp) = resp
        .responses
        .iter()
        .find(|t| {
            t.topic == cfg.topic || (cfg.topic_id != WireUuid::ZERO && t.topic_id == cfg.topic_id)
        })
        .and_then(|t| {
            t.partitions
                .iter()
                .find(|p| p.partition_index == cfg.partition)
        })
    else {
        return LoopAction::Continue;
    };

    match part_resp.error_code {
        codes::NONE => {
            let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition)) else {
                warn!(topic = %cfg.topic, partition = cfg.partition,
                    "replicator: local partition vanished between fetches");
                return LoopAction::Continue;
            };
            if let Some(batch) = part_resp.records.as_ref().and_then(|p| p.as_v2()) {
                // Capture byte count before the move into replicate_batch
                // so the metrics update only fires on a successful append.
                let batch_bytes = batch.encoded_len();
                if let Err(e) = entry.value().replicate_batch(batch.clone()).await {
                    warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
                        "replicator: replicate_batch failed");
                } else {
                    cfg.metrics.record_replication_in(
                        &cfg.topic,
                        cfg.partition,
                        u64::try_from(batch_bytes).unwrap_or(0),
                    );
                }
            }
            // KIP-392: record the leader's high watermark so consumer reads
            // served from this follower are bounded correctly. Done on every
            // successful response, including empty ones.
            entry
                .value()
                .set_follower_hw(part_resp.high_watermark)
                .await;
            LoopAction::Continue
        }
        codes::OFFSET_OUT_OF_RANGE => {
            // The leader reports its current `log_start_offset` in the
            // partition response. We MUST reset our local log to that
            // value, not to 0: the leader may have moved its log_start
            // forward past records this follower never saw (retention
            // happened, etc.), and re-fetching from 0 would just bounce
            // off the same `OFFSET_OUT_OF_RANGE` forever. `reset_to`
            // drops every existing segment and creates a fresh active
            // segment at `leader_log_start`, after which the next loop
            // iteration's `log_end_offset()` equals `leader_log_start`
            // and the fetch lands inside the leader's retained range.
            let leader_log_start = part_resp.log_start_offset;
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition,
                leader_log_start,
                "replicator.out_of_range; resetting local log to leader log_start"
            );
            if let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition))
                && let Err(e) = entry.value().reset_to(leader_log_start).await
            {
                warn!(error = %e, "replicator: reset_to(leader_log_start) failed");
            }
            LoopAction::Continue
        }
        codes::UNKNOWN_TOPIC_OR_PARTITION => {
            // Leader hasn't materialized its side yet
            // (CreateTopics-vs-replicator race).
            tokio::time::sleep(Duration::from_millis(100)).await;
            LoopAction::Continue
        }
        codes::NOT_LEADER_OR_FOLLOWER => LoopAction::StopNotLeader,
        codes::FENCED_LEADER_EPOCH | codes::UNKNOWN_LEADER_EPOCH => {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition,
                error_code = part_resp.error_code,
                "replicator: fenced/unknown leader epoch; calling OffsetForLeaderEpoch"
            );
            let _ = handle_epoch_fence(cfg).await;
            LoopAction::Continue
        }
        other => {
            warn!(
                error_code = other,
                "replicator: unexpected fetch error_code"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
            LoopAction::Continue
        }
    }
}

/// On `FENCED_LEADER_EPOCH` or `UNKNOWN_LEADER_EPOCH`, call
/// `OffsetForLeaderEpoch` against the leader to find the truncation
/// point, then truncate our local log to align with the leader's epoch
/// history.
///
/// KIP-101: the follower sends our current `leader_epoch`; the leader
/// replies with `end_offset` = the first offset of the next epoch,
/// which is the safe truncation point.
async fn handle_epoch_fence(cfg: &Config) -> Result<(), String> {
    let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition)) else {
        return Ok(());
    };
    let our_epoch = entry
        .value()
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    drop(entry);

    let opts = ConnectionOptions {
        client_id: cfg.client_id.clone(),
        ..ConnectionOptions::default()
    };
    let client = cfg
        .inter_broker_client
        .connect_as_connection(
            &cfg.leader_host,
            cfg.leader_port,
            cfg.inter_broker_listener_protocol,
            "localhost",
            opts,
        )
        .await
        .map_err(|e| format!("handle_epoch_fence: connect: {e}"))?;

    let req = OffsetForLeaderEpochRequest {
        replica_id: i32::try_from(cfg.node_id).unwrap_or(-1),
        topics: vec![OffsetForLeaderTopic {
            topic: cfg.topic.clone(),
            partitions: vec![OffsetForLeaderPartition {
                partition: cfg.partition,
                current_leader_epoch: our_epoch,
                leader_epoch: our_epoch,
                ..OffsetForLeaderPartition::default()
            }],
            ..OffsetForLeaderTopic::default()
        }],
        ..OffsetForLeaderEpochRequest::default()
    };

    let resp = client
        .send(req)
        .await
        .map_err(|e| format!("handle_epoch_fence: send: {e}"))?;

    // Find our (topic, partition) in the response.
    let Some(end_offset) = resp
        .topics
        .iter()
        .find(|t| t.topic == cfg.topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == cfg.partition))
        .map(|p| p.end_offset)
    else {
        return Ok(());
    };

    let Some(part_entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition)) else {
        return Ok(());
    };
    let part = part_entry.value().clone();
    drop(part_entry);

    if end_offset >= 0 {
        // Truncate to the epoch boundary.
        if let Err(e) = part.truncate_to(end_offset).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition,
                end_offset,
                error = %e,
                "handle_epoch_fence: truncate_to failed"
            );
        } else {
            info!(
                topic = %cfg.topic,
                partition = cfg.partition,
                end_offset,
                "handle_epoch_fence: truncated to epoch boundary"
            );
        }
    } else {
        // end_offset == -1 (UNDEFINED_OFFSET): no epoch info available;
        // reset to 0 as a safe fallback.
        if let Err(e) = part.reset_to(0).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition,
                error = %e,
                "handle_epoch_fence: reset_to(0) failed"
            );
        } else {
            info!(
                topic = %cfg.topic,
                partition = cfg.partition,
                "handle_epoch_fence: reset to 0 (undefined epoch boundary)"
            );
        }
    }

    Ok(())
}

/// Open a [`Connection`] against the partition's leader, retrying with
/// exponential backoff (capped at 5s). Returns `Err` only if shutdown is
/// requested while we were waiting.
///
/// Routes through the shared [`InterBrokerClient`] so TLS + SASL are run
/// when the inter-broker listener demands them, and falls back to plain
/// TCP for `ListenerProtocol::Plaintext`.
async fn connect_with_backoff(cfg: &Config) -> Result<Connection, String> {
    let mut delay = Duration::from_millis(100);
    let cap = Duration::from_secs(5);
    loop {
        let opts = ConnectionOptions {
            client_id: cfg.client_id.clone(),
            ..ConnectionOptions::default()
        };
        let attempt = cfg.inter_broker_client.connect_as_connection(
            &cfg.leader_host,
            cfg.leader_port,
            cfg.inter_broker_listener_protocol,
            "localhost",
            opts,
        );
        let result = tokio::select! {
            () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
            r = attempt => r,
        };
        match result {
            Ok(c) => return Ok(c),
            Err(e) => {
                warn!(
                    host = %cfg.leader_host, port = cfg.leader_port, error = %e,
                    "replicator: connect failed; retrying after {:?}", delay
                );
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
                    () = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(cap);
            }
        }
    }
}

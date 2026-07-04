//! Per-(topic, partition) replication task. Issues standard Kafka `Fetch`
//! requests against the partition's leader (with `replica_id` set to the
//! local broker's `node_id`), appending each returned batch to the local
//! `crabka-log`. Handles `OFFSET_OUT_OF_RANGE` by truncating local log to
//! 0 and restarting; `NOT_LEADER_FOR_PARTITION` by returning so the
//! supervisor's next reconcile re-evaluates.

// `log_config` is a conventional field name; the "ends with struct name" lint
// is a false positive here.
#![allow(clippy::struct_field_names)]

use std::{path::PathBuf, sync::Arc, time::Duration};

use crabka_client_core::{ClientError, Connection, ConnectionOptions};
use crabka_ids::PartitionIndex;
use crabka_log::{Log, LogConfig, Offset};
use crabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
        fetch_response::FetchResponse,
        offset_for_leader_epoch_request::{
            OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};
use crabka_raft::NodeId;
use crabka_security::ListenerProtocol;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    broker::spawn_partition,
    codes,
    partition_registry::PartitionRegistry,
    throttle::{ThrottleState, TopicThrottle},
};

const FETCH_MAX_BYTES: i32 = 1 << 20;
const FETCH_MAX_WAIT_MS: i32 = 500;
const FETCH_MIN_BYTES: i32 = 1;

/// Sleep before re-checking the KIP-73 follower-in token bucket after it
/// refused the fetch (bucket exhausted this round).
const THROTTLE_EXHAUSTED_BACKOFF: Duration = Duration::from_millis(100);
/// Backoff before reconnecting after an unexpected (non-transport)
/// `client.send` error.
const SEND_ERROR_BACKOFF: Duration = Duration::from_secs(1);
/// Retry delay while the leader hasn't materialized its side of the
/// partition yet (`CreateTopics`-vs-replicator race).
const UNKNOWN_TOPIC_RETRY_DELAY: Duration = Duration::from_millis(100);
/// Backoff between fetch rounds after a fenced/unknown leader epoch, so a
/// persistent fence doesn't hot-spin fetch → `OffsetForLeaderEpoch`.
const EPOCH_FENCE_BACKOFF: Duration = Duration::from_millis(200);
/// Backoff after an unexpected fetch `error_code` before the next round.
const UNEXPECTED_ERROR_BACKOFF: Duration = Duration::from_millis(500);
/// First delay of the leader-connect exponential backoff.
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(100);
/// Ceiling for the leader-connect exponential backoff.
const RECONNECT_DELAY_CAP: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchThrottleDecision {
    Fetch(i32),
    Sleep,
}

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
    pub partition: PartitionIndex,
    pub leader_node_id: NodeId,
    /// Leader's `host` portion from the metadata image (the inter-broker
    /// endpoint when available, otherwise the legacy broker host).
    pub leader_host: String,
    pub leader_port: u16,
    pub partitions: Arc<PartitionRegistry>,
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
    /// Broker-wide idempotent/transactional producer-sequence tracker.
    /// Forwarded into `spawn_partition` (via `ensure_local_partition`)
    /// so the per-partition writer's `Compact` handler can snapshot
    /// active producers for KIP-534 `RETAIN_EMPTY`.
    pub producer_state: Arc<crate::producer_state::ProducerState>,
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
        partition = cfg.partition.get(),
        leader_node_id = cfg.leader_node_id.0,
        "replicator.started"
    );

    // First-run materialization of the local on-disk partition.
    if let Err(e) = ensure_local_partition(&cfg) {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator failed to open local partition; aborting");
        return;
    }

    if let Err(e) = run_inner(&cfg).await {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator stopped on unrecoverable error");
    }

    info!(topic = %cfg.topic, partition = cfg.partition.get(), "replicator.stopped");
}

/// Build (or recover) the on-disk `Partition` for this follower, inserting
/// it into the broker's shared `partitions` map. Idempotent.
fn ensure_local_partition(cfg: &Config) -> Result<(), String> {
    // `materialize_if_vacant` runs the build under the per-key lock, so two
    // concurrent replicators for the same partition can never both build it.
    cfg.partitions
        .materialize_if_vacant(&cfg.topic, cfg.partition, || {
            let dir =
                crate::log_dir::place_partition_dir(&cfg.log_dirs, &cfg.topic, cfg.partition.get());
            std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
            let log =
                Log::open(&dir, cfg.log_config.clone()).map_err(|e| format!("Log::open: {e}"))?;
            let owning_dir = dir
                .parent()
                .expect("placed partition dir always has a parent log.dir")
                .to_path_buf();
            Ok(spawn_partition(
                cfg.topic.clone(),
                cfg.partition,
                owning_dir,
                log,
                cfg.log_dir_status.clone(),
                cfg.producer_state.clone(),
            ))
        })
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
                .get(&cfg.topic, cfg.partition)
                .ok_or_else(|| "local partition missing".to_string())?;
            entry.log_end_offset()
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
        let partition_max_bytes_cap = match follower_partition_fetch_cap(cfg) {
            FetchThrottleDecision::Fetch(cap) => cap,
            FetchThrottleDecision::Sleep => {
                tracing::debug!(
                    topic = %cfg.topic,
                    partition = cfg.partition.get(),
                    "follower throttle: skip fetch this round (bucket exhausted)"
                );
                // Bucket exhausted — yield and retry next loop iteration.
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(THROTTLE_EXHAUSTED_BACKOFF) => {}
                }
                continue;
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
                    () = tokio::time::sleep(SEND_ERROR_BACKOFF) => {}
                }
                client = connect_with_backoff(cfg).await?;
                continue;
            }
        };

        match handle_response(resp, cfg).await {
            LoopAction::Continue => {}
            LoopAction::StopNotLeader => {
                info!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator.not_leader; supervisor will re-evaluate");
                return Ok(());
            }
        }
    }
}

fn follower_partition_fetch_cap(cfg: &Config) -> FetchThrottleDecision {
    let image = cfg.controller.current_image();
    let throttle = TopicThrottle::for_topic(&image, &cfg.topic);
    let throttled = throttle.follower.contains(cfg.partition.get(), cfg.node_id);
    if !throttled || cfg.throttle_state.follower_in.rate() == 0 {
        return FetchThrottleDecision::Fetch(FETCH_MAX_BYTES);
    }

    let granted = cfg
        .throttle_state
        .follower_in
        .try_consume(u64::try_from(FETCH_MAX_BYTES).unwrap_or(0));
    if granted == 0 {
        FetchThrottleDecision::Sleep
    } else {
        FetchThrottleDecision::Fetch(i32::try_from(granted).unwrap_or(FETCH_MAX_BYTES))
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
    fetch_offset: Offset,
    partition_max_bytes_cap: i32,
) -> FetchRequest {
    let leader_epoch = cfg
        .partitions
        .get(&cfg.topic, cfg.partition)
        .map_or(-1, |entry| {
            entry
                .current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire)
        });
    // KIP-320: the leader epoch of our last appended record. Sent so the
    // leader can detect divergence in-band and answer with `diverging_epoch`.
    let last_fetched_epoch = cfg
        .partitions
        .get(&cfg.topic, cfg.partition)
        .and_then(|entry| {
            let log = entry.log.lock().expect("log mutex poisoned");
            log.epoch_checkpoint().latest_epoch()
        })
        // Unwrap the log-layer `LeaderEpoch` into the raw wire `last_fetched_epoch`.
        .map_or(-1, |e| e.0);
    // `replica_id` is the wire field on Fetch v0-14. KIP-903 (Kafka 3.5) moved
    // it into a tagged `replica_state` struct on v15+; the codegen serializes
    // whichever the negotiated version requires. Populate BOTH so the request
    // is correct regardless of which version the leader negotiates.
    let rid = i32::try_from(cfg.node_id.0).unwrap_or(-1);
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
                partition: cfg.partition.get(),
                // Unwrap the `Offset` into the wire `i64` field.
                fetch_offset: fetch_offset.0,
                current_leader_epoch: leader_epoch,
                last_fetched_epoch,
                partition_max_bytes: partition_max_bytes_cap,
                ..FetchPartition::default()
            }],
            ..FetchTopic::default()
        }],
        ..FetchRequest::default()
    }
}

/// Outcome of one fetch round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoopAction {
    Continue,
    StopNotLeader,
}

/// `true` if the committed metadata image now lists THIS broker as the leader of
/// the partition this replicator follows.
///
/// A follower-replicator's cancellation on a leadership change is cooperative —
/// the run loop only checks the shutdown token between fetches — so an in-flight
/// Fetch response can still be processed after this broker has been promoted to
/// leader. Truncating/resetting our own log from such a stale response would
/// drop the new leader's freshly-appended (possibly acknowledged) data, both
/// stalling `acks=all` produces and silently losing records. Callers consult
/// this immediately before any truncation and stop the replicator instead.
fn became_partition_leader(cfg: &Config) -> bool {
    cfg.controller
        .current_image()
        .partition(&cfg.topic, cfg.partition.get())
        .map(|pr| pr.leader)
        == Some(cfg.node_id)
}

#[allow(clippy::too_many_lines)] // KIP-320 in-band truncation + KIP-101 epoch fence add match arms
async fn handle_response(mut resp: FetchResponse, cfg: &Config) -> LoopAction {
    // The replicator only ever requests one (topic, partition) per Fetch.
    // Match by either `topic` (v ≤ 12) or `topic_id` (v ≥ 13) so that
    // when the negotiated wire format drops the topic-name field
    // (KIP-516) we still find our partition. Without this fallback
    // every fetch silently no-ops at v ≥ 13 because `t.topic == ""`.
    //
    // Take `resp` BY VALUE and resolve the matching partition by *mutable*
    // reference so the record batches can be moved out (via `records.take()`)
    // and handed to the writer without a deep clone per batch.
    let Some(part_resp) = resp
        .responses
        .iter_mut()
        .find(|t| {
            t.topic == cfg.topic || (cfg.topic_id != WireUuid::ZERO && t.topic_id == cfg.topic_id)
        })
        .and_then(|t| {
            t.partitions
                .iter_mut()
                .find(|p| p.partition_index == cfg.partition)
        })
    else {
        return LoopAction::Continue;
    };

    match part_resp.error_code {
        codes::NONE => {
            // KIP-320: an in-band divergence signal. The leader served no
            // records and told us the epoch/offset our log must truncate to.
            // `EpochEndOffset` defaults to (epoch:-1, end_offset:-1); a
            // populated `end_offset >= 0` means "truncate here".
            if part_resp.diverging_epoch.end_offset >= 0 {
                // Stale-response guard: never truncate from a Fetch response if
                // we have since become this partition's leader (see
                // `became_partition_leader`).
                if became_partition_leader(cfg) {
                    warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                        "replicator: skipping diverging_epoch truncation — this broker is now the partition leader (stale fetch response)");
                    return LoopAction::StopNotLeader;
                }
                let end_offset = part_resp.diverging_epoch.end_offset;
                if let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) {
                    // Wrap the wire `i64` into `Offset` for the log-layer call.
                    match part.truncate_to(Offset(end_offset)).await {
                        Ok(()) => {
                            // Drop idempotent-producer dedup entries for the
                            // truncated tail, or a retried batch deduplicates
                            // against an offset the log no longer holds and its
                            // acks=all HW gate stalls forever (failover stall).
                            cfg.producer_state
                                .truncate(&cfg.topic, cfg.partition, end_offset)
                                .await;
                            info!(
                                topic = %cfg.topic,
                                partition = cfg.partition.get(),
                                end_offset,
                                "replicator: truncated to diverging_epoch (KIP-320 in-band)"
                            );
                        }
                        Err(e) => warn!(
                            topic = %cfg.topic,
                            partition = cfg.partition.get(),
                            end_offset,
                            error = %e,
                            "replicator: truncate_to(diverging_epoch) failed"
                        ),
                    }
                }
                return LoopAction::Continue;
            }

            let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: local partition vanished between fetches");
                return LoopAction::Continue;
            };
            // Move the parsed v2 batches out of the owned response so each
            // batch can be handed to the writer BY VALUE — no per-batch deep
            // clone. `take()` leaves `None` behind; the response is dropped at
            // the end of this call so nothing is read from `records` again.
            // `Raw`/`Legacy` payloads were never processed here (the old
            // `as_v2()` returned `None` for them), so they are ignored.
            if let Some(RecordsPayload::V2(batches)) = part_resp.records.take() {
                for batch in batches {
                    // Capture byte count before the move into replicate_batch
                    // so the metrics update only fires on a successful append.
                    // PERF: `encoded_len()` is computed here for the metric and
                    // again inside the append path; threading a single
                    // computation through would save the re-walk, but that
                    // touches the writer API (cross-file) so it's left as-is.
                    let batch_bytes = batch.encoded_len();
                    if let Err(e) = part.replicate_batch(batch).await {
                        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition.get(),
                            "replicator: replicate_batch failed");
                        break;
                    }
                    cfg.metrics.record_replication_in(
                        &cfg.topic,
                        cfg.partition.get(),
                        u64::try_from(batch_bytes).unwrap_or(0),
                    );
                }
            }
            // KIP-392: record the leader's high watermark so consumer reads
            // served from this follower are bounded correctly. Done on every
            // successful response, including empty ones. Wrap the wire `i64`.
            part.set_follower_hw(Offset(part_resp.high_watermark)).await;
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
            // Stale-response guard: never reset from a Fetch response if we have
            // since become this partition's leader (see `became_partition_leader`).
            if became_partition_leader(cfg) {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: skipping out_of_range reset — this broker is now the partition leader (stale fetch response)");
                return LoopAction::StopNotLeader;
            }
            let leader_log_start = part_resp.log_start_offset;
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                leader_log_start,
                "replicator.out_of_range; resetting local log to leader log_start"
            );
            if let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) {
                // Wrap the wire `i64` into `Offset` for the log-layer call.
                match part.reset_to(Offset(leader_log_start)).await {
                    Ok(()) => {
                        // The log restarts empty at leader_log_start; drop
                        // idempotent-producer dedup entries at/above it so a
                        // retried batch re-appends instead of stalling its
                        // acks=all HW gate against a vanished offset.
                        cfg.producer_state
                            .truncate(&cfg.topic, cfg.partition, leader_log_start)
                            .await;
                    }
                    Err(e) => {
                        warn!(error = %e, "replicator: reset_to(leader_log_start) failed");
                    }
                }
            }
            LoopAction::Continue
        }
        codes::UNKNOWN_TOPIC_OR_PARTITION => {
            // Leader hasn't materialized its side yet
            // (CreateTopics-vs-replicator race).
            tokio::time::sleep(UNKNOWN_TOPIC_RETRY_DELAY).await;
            LoopAction::Continue
        }
        codes::NOT_LEADER_OR_FOLLOWER => LoopAction::StopNotLeader,
        codes::FENCED_LEADER_EPOCH | codes::UNKNOWN_LEADER_EPOCH => {
            // Stale-response guard: if we have become this partition's leader,
            // a fenced response from our former leader means this
            // follower-replicator is stale — STOP it. Without this it neither
            // truncates (the `became_partition_leader` guard in
            // `handle_epoch_fence` skips that) nor stops, so it hot-loops the
            // Fetch at ~full CPU, starving metadata propagation and the
            // cooperative cancellation that would otherwise retire it — the
            // broker then never becomes ready and crashloops.
            if became_partition_leader(cfg) {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: stopping on fenced epoch — this broker is now the partition leader");
                return LoopAction::StopNotLeader;
            }
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                error_code = part_resp.error_code,
                "replicator: fenced/unknown leader epoch; calling OffsetForLeaderEpoch"
            );
            let _ = handle_epoch_fence(cfg).await;
            // Back off before re-fetching so a persistent fence (e.g. our
            // leader_epoch hasn't caught up to the new leader's yet) doesn't
            // hot-spin the CPU between fetch and fence.
            tokio::select! {
                () = cfg.shutdown.cancelled() => return LoopAction::StopNotLeader,
                () = tokio::time::sleep(EPOCH_FENCE_BACKOFF) => {}
            }
            LoopAction::Continue
        }
        other => {
            warn!(
                error_code = other,
                "replicator: unexpected fetch error_code"
            );
            tokio::time::sleep(UNEXPECTED_ERROR_BACKOFF).await;
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
// The `end_offset >= 0` truncate-vs-reset branch is only reachable after a live
// leader connection returns an `OffsetForLeaderEpoch` response; the whole
// function is inter-broker IO (connect, send, then `part.truncate_to` /
// `part.reset_to`) with no pure seam. Exercised by the live-replication suite.
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    name = "replicator_handle_epoch_fence",
    level = "info",
    skip_all,
    fields(topic = %cfg.topic, partition = cfg.partition.get()),
    err,
)]
async fn handle_epoch_fence(cfg: &Config) -> Result<(), String> {
    let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return Ok(());
    };
    let our_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    drop(part);

    let opts = connection_options(&cfg.client_id);
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

    let req = build_offset_for_leader_epoch_request(cfg, our_epoch);

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

    let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return Ok(());
    };

    // Stale-response guard: never truncate/reset from an OffsetForLeaderEpoch
    // response if we have since become this partition's leader (see
    // `became_partition_leader`).
    if became_partition_leader(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping epoch-fence truncation — this broker is now the partition leader (stale response)");
        return Ok(());
    }

    if end_offset >= 0 {
        // Truncate to the epoch boundary. Wrap the wire `i64` into `Offset`.
        if let Err(e) = part.truncate_to(Offset(end_offset)).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                end_offset,
                error = %e,
                "handle_epoch_fence: truncate_to failed"
            );
        } else {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, end_offset)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                end_offset,
                "handle_epoch_fence: truncated to epoch boundary"
            );
        }
    } else {
        // end_offset == -1 (UNDEFINED_OFFSET): no epoch info available;
        // reset to 0 as a safe fallback.
        if let Err(e) = part.reset_to(Offset(0)).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                error = %e,
                "handle_epoch_fence: reset_to(0) failed"
            );
        } else {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, 0)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                "handle_epoch_fence: reset to 0 (undefined epoch boundary)"
            );
        }
    }

    Ok(())
}

fn connection_options(client_id: &str) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_string(),
        ..ConnectionOptions::default()
    }
}

fn build_offset_for_leader_epoch_request(
    cfg: &Config,
    our_epoch: i32,
) -> OffsetForLeaderEpochRequest {
    OffsetForLeaderEpochRequest {
        replica_id: i32::try_from(cfg.node_id.0).unwrap_or(-1),
        topics: vec![OffsetForLeaderTopic {
            topic: cfg.topic.clone(),
            partitions: vec![OffsetForLeaderPartition {
                partition: cfg.partition.get(),
                current_leader_epoch: our_epoch,
                leader_epoch: our_epoch,
                ..OffsetForLeaderPartition::default()
            }],
            ..OffsetForLeaderTopic::default()
        }],
        ..OffsetForLeaderEpochRequest::default()
    }
}

/// Open a [`Connection`] against the partition's leader, retrying with
/// exponential backoff (capped at [`RECONNECT_DELAY_CAP`]). Returns `Err`
/// only if shutdown is requested while we were waiting.
///
/// Routes through the shared [`InterBrokerClient`] so TLS + SASL are run
/// when the inter-broker listener demands them, and falls back to plain
/// TCP for `ListenerProtocol::Plaintext`.
async fn connect_with_backoff(cfg: &Config) -> Result<Connection, String> {
    let mut delay = RECONNECT_INITIAL_DELAY;
    let cap = RECONNECT_DELAY_CAP;
    loop {
        let opts = connection_options(&cfg.client_id);
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
                delay = next_reconnect_delay(delay, cap);
            }
        }
    }
}

fn next_reconnect_delay(delay: Duration, cap: Duration) -> Duration {
    (delay * 2).min(cap)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        net::SocketAddr,
    };

    use assert2::assert;
    use crabka_metadata::{
        MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
    };
    use crabka_protocol::owned::fetch_response::{
        EpochEndOffset, FetchableTopicResponse, PartitionData,
    };
    use crabka_raft::{
        AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
        UpdateVoter,
    };
    use tokio::sync::watch;

    use super::*;

    const TOPIC: &str = "orders";
    const PARTITION: i32 = 0;
    const NODE_ID: NodeId = NodeId(2);
    const LEADER_ID: NodeId = NodeId(1);
    const WIRE_TOPIC_ID: WireUuid = WireUuid([7; 16]);

    struct StaticMetadataSource {
        image: Arc<MetadataImage>,
        image_rx: watch::Receiver<Arc<MetadataImage>>,
        leader_rx: watch::Receiver<Option<NodeId>>,
    }

    impl StaticMetadataSource {
        fn new(image: MetadataImage) -> Self {
            let image = Arc::new(image);
            let (_image_tx, image_rx) = watch::channel(image.clone());
            let (_leader_tx, leader_rx) = watch::channel(None);
            Self {
                image,
                image_rx,
                leader_rx,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for StaticMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            self.image_rx.clone()
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_rx.clone()
        }

        fn quorum_state(&self) -> QuorumState {
            QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: None,
                voters: Vec::new(),
                voter_nodes: BTreeMap::new(),
                per_voter_matched_index: BTreeMap::new(),
            }
        }

        async fn submit_change(&self, _records: Vec<MetadataRecord>) -> Result<(), RaftError> {
            panic!("unused in replicator tests")
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            panic!("unused in replicator tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            panic!("unused in replicator tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            SocketAddr::from(([127, 0, 0, 1], 0))
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            panic!("unused in replicator tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("unused in replicator tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("unused in replicator tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("unused in replicator tests")
        }

        async fn cancel(&self) {}
    }

    fn image_with_leader(leader: NodeId) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.into(),
            topic_id: uuid::Uuid::from_bytes(WIRE_TOPIC_ID.0),
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: TOPIC.into(),
            partition: PARTITION,
            leader,
            replicas: vec![LEADER_ID, NODE_ID],
            isr: vec![LEADER_ID, NODE_ID],
            leader_epoch: crabka_metadata::LeaderEpoch(4),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: Vec::new(),
            partition_epoch: 0,
        }));
        image
    }

    fn image_with_follower_throttle(value: &str) -> MetadataImage {
        let mut image = image_with_leader(LEADER_ID);
        let mut overrides = BTreeMap::new();
        overrides.insert(
            crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY.to_string(),
            value.to_string(),
        );
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides,
        }));
        image
    }

    fn test_config(image: MetadataImage) -> (Config, tempfile::TempDir) {
        let log_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            node_id: NODE_ID,
            topic: TOPIC.into(),
            topic_id: WIRE_TOPIC_ID,
            partition: PartitionIndex(PARTITION),
            leader_node_id: LEADER_ID,
            leader_host: "127.0.0.1".into(),
            leader_port: 9,
            partitions: Arc::new(PartitionRegistry::new()),
            log_dirs: vec![log_dir.path().to_path_buf()],
            log_config: LogConfig::default(),
            client_id: "replica-test".into(),
            shutdown: CancellationToken::new(),
            inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(
                None, None,
            )),
            inter_broker_listener_protocol: ListenerProtocol::Plaintext,
            throttle_state: Arc::new(ThrottleState::new()),
            controller: Arc::new(StaticMetadataSource::new(image)),
            log_dir_status: crate::log_dir_status::LogDirRegistry::default(),
            producer_state: Arc::new(crate::producer_state::ProducerState::new()),
            metrics: crate::metrics::BrokerMetrics::default(),
        };
        (cfg, log_dir)
    }

    fn fetch_response(topic: &str, topic_id: WireUuid, part: PartitionData) -> FetchResponse {
        FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: topic.into(),
                topic_id,
                partitions: vec![part],
                ..FetchableTopicResponse::default()
            }],
            ..FetchResponse::default()
        }
    }

    fn partition_response(partition_index: i32, error_code: i16) -> PartitionData {
        PartitionData {
            partition_index,
            error_code,
            ..PartitionData::default()
        }
    }

    #[test]
    fn fetch_max_bytes_is_one_mebibyte() {
        assert!(FETCH_MAX_BYTES == 1_048_576);
    }

    #[test]
    fn build_fetch_request_populates_replica_and_partition_fields() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));

        let req = build_fetch_request(&cfg, Offset(123), 456);

        let rid = i32::try_from(NODE_ID.0).unwrap();
        let expected = FetchRequest {
            replica_id: rid,
            max_wait_ms: FETCH_MAX_WAIT_MS,
            min_bytes: FETCH_MIN_BYTES,
            max_bytes: FETCH_MAX_BYTES,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopic {
                topic: TOPIC.into(),
                topic_id: WIRE_TOPIC_ID,
                partitions: vec![FetchPartition {
                    partition: PARTITION,
                    current_leader_epoch: -1,
                    fetch_offset: 123,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 456,
                    replica_directory_id: WireUuid::ZERO,
                    high_watermark: i64::MAX,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            forgotten_topics_data: Vec::new(),
            rack_id: String::new(),
            cluster_id: None,
            replica_state: ReplicaState {
                replica_id: rid,
                replica_epoch: -1,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(req == expected);
    }

    #[test]
    fn build_fetch_request_uses_negative_replica_sentinel_when_node_id_overflows() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);

        let req = build_fetch_request(&cfg, Offset(0), FETCH_MAX_BYTES);

        assert!(req.replica_id == -1);
        assert!(req.replica_state.replica_id == -1);
    }

    #[test]
    fn offset_epoch_request_and_connection_options_preserve_identity_fields() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let opts = connection_options(&cfg.client_id);
        assert!(opts.client_id == "replica-test");

        let req = build_offset_for_leader_epoch_request(&cfg, 7);
        let expected = OffsetForLeaderEpochRequest {
            replica_id: i32::try_from(NODE_ID.0).unwrap(),
            topics: vec![OffsetForLeaderTopic {
                topic: TOPIC.into(),
                partitions: vec![OffsetForLeaderPartition {
                    partition: PARTITION,
                    current_leader_epoch: 7,
                    leader_epoch: 7,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(req == expected);
    }

    #[test]
    fn offset_epoch_request_uses_negative_replica_sentinel_when_node_id_overflows() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);

        let req = build_offset_for_leader_epoch_request(&cfg, 7);

        assert!(req.replica_id == -1);
    }

    #[test]
    fn next_reconnect_delay_doubles_until_cap() {
        let cases = [
            // Doubles below the cap.
            (
                Duration::from_millis(100),
                Duration::from_secs(5),
                Duration::from_millis(200),
            ),
            // Clamps at the cap.
            (
                Duration::from_secs(4),
                Duration::from_secs(5),
                Duration::from_secs(5),
            ),
        ];
        for (current, cap, want) in cases {
            assert!(
                next_reconnect_delay(current, cap) == want,
                "current {current:?} cap {cap:?}"
            );
        }
    }

    #[test]
    fn became_partition_leader_reflects_current_metadata_leader() {
        let cases = [(NODE_ID, true), (LEADER_ID, false)];
        for (leader, want) in cases {
            let (cfg, _log_dir) = test_config(image_with_leader(leader));
            assert!(
                became_partition_leader(&cfg) == want,
                "metadata leader {leader}"
            );
        }
    }

    #[test]
    fn follower_partition_fetch_cap_ignores_unthrottled_partitions() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.throttle_state.follower_in.set_rate_with_burst(1234, 0);

        assert!(
            follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Fetch(FETCH_MAX_BYTES)
        );
    }

    #[test]
    fn follower_partition_fetch_cap_ignores_zero_rate_throttle() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));

        assert!(
            follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Fetch(FETCH_MAX_BYTES)
        );
    }

    #[test]
    fn follower_partition_fetch_cap_sleeps_when_throttled_bucket_is_empty() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state.follower_in.set_rate_with_burst(1024, 0);

        assert!(follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Sleep);
    }

    #[test]
    fn follower_partition_fetch_cap_uses_granted_bucket_size() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_rate_with_burst(1234, 1234);

        assert!(follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Fetch(1234));
    }

    #[tokio::test]
    async fn handle_response_matches_fetch_topic_by_name() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            TOPIC,
            WireUuid::ZERO,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg).await == LoopAction::StopNotLeader);
    }

    #[tokio::test]
    async fn handle_response_matches_fetch_topic_by_topic_id() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            "",
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg).await == LoopAction::StopNotLeader);
    }

    #[tokio::test]
    async fn handle_response_ignores_other_partition_index() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION + 1, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg).await == LoopAction::Continue);
    }

    #[tokio::test]
    async fn handle_response_stops_on_diverging_epoch_after_local_promotion() {
        let (cfg, _log_dir) = test_config(image_with_leader(NODE_ID));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                diverging_epoch: EpochEndOffset {
                    epoch: 4,
                    end_offset: 0,
                    ..EpochEndOffset::default()
                },
                ..PartitionData::default()
            },
        );

        assert!(handle_response(resp, &cfg).await == LoopAction::StopNotLeader);
    }

    #[tokio::test]
    async fn handle_response_stops_on_out_of_range_after_local_promotion() {
        let (cfg, _log_dir) = test_config(image_with_leader(NODE_ID));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::OFFSET_OUT_OF_RANGE),
        );

        assert!(handle_response(resp, &cfg).await == LoopAction::StopNotLeader);
    }

    #[tokio::test]
    async fn run_materializes_local_partition_before_observing_cancelled_shutdown() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.shutdown.cancel();
        let partitions = cfg.partitions.clone();

        run(cfg).await;

        assert!(partitions.contains(TOPIC, PartitionIndex(PARTITION)));
    }

    #[tokio::test]
    async fn run_inner_reports_cancelled_before_first_connection() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.shutdown.cancel();

        let err = run_inner(&cfg).await.unwrap_err();

        assert!(err == "cancelled");
    }

    #[tokio::test]
    async fn handle_epoch_fence_surfaces_connection_failure_for_local_partition() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        cfg.leader_port = listener.local_addr().unwrap().port();
        drop(listener);

        let err = handle_epoch_fence(&cfg).await.unwrap_err();

        assert!(
            err.contains("handle_epoch_fence: connect"),
            "unexpected error: {err}"
        );
    }
}

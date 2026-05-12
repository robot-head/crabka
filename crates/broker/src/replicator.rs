//! Per-(topic, partition) replication task. Issues standard Kafka `Fetch`
//! requests against the partition's leader (with `replica_id` set to the
//! local broker's `node_id`), appending each returned batch to the local
//! `crabka-log`. Handles `OFFSET_OUT_OF_RANGE` by truncating local log to
//! 0 and restarting; `NOT_LEADER_FOR_PARTITION` by returning so the
//! supervisor's next reconcile re-evaluates.

// `node_id` and `client_id` on `Config` are consumed by the supervisor in
// Task 8 / Broker::start in Task 10; keep the allow until then.
#![allow(dead_code)]
// `log_config` is a conventional field name; the "ends with struct name" lint
// is a false positive here.
#![allow(clippy::struct_field_names)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crabka_client_core::{Client, ClientError};
use crabka_log::{Log, LogConfig};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::fetch_response::FetchResponse;
use crabka_raft::NodeId;

use crate::broker::spawn_partition;
use crate::codes;
use crate::partition::Partition;

const FETCH_MAX_BYTES: i32 = 1 << 20;
const FETCH_MAX_WAIT_MS: i32 = 500;
const FETCH_MIN_BYTES: i32 = 1;

/// Configuration handed to a single replicator task.
pub(crate) struct Config {
    pub node_id: NodeId,
    pub topic: String,
    pub partition: i32,
    pub leader_node_id: NodeId,
    pub leader_addr: String,
    pub partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub log_dir: PathBuf,
    pub log_config: LogConfig,
    pub client_id: String,
    pub shutdown: CancellationToken,
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
    let dir = cfg.log_dir.join(format!("{}-{}", cfg.topic, cfg.partition));
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let log = Log::open(&dir, cfg.log_config.clone()).map_err(|e| format!("Log::open: {e}"))?;
    let part = spawn_partition(cfg.topic.clone(), cfg.partition, log);
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

        let req = build_fetch_request(cfg, fetch_offset);

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
fn build_fetch_request(cfg: &Config, fetch_offset: i64) -> FetchRequest {
    FetchRequest {
        replica_id: i32::try_from(cfg.node_id).unwrap_or(-1),
        max_wait_ms: FETCH_MAX_WAIT_MS,
        min_bytes: FETCH_MIN_BYTES,
        max_bytes: FETCH_MAX_BYTES,
        topics: vec![FetchTopic {
            topic: cfg.topic.clone(),
            partitions: vec![FetchPartition {
                partition: cfg.partition,
                fetch_offset,
                partition_max_bytes: FETCH_MAX_BYTES,
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
    // Slice 8 only ever requests one (topic, partition) per Fetch.
    let Some(part_resp) = resp
        .responses
        .iter()
        .find(|t| t.topic == cfg.topic)
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
            if let Some(batch) = &part_resp.records {
                let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition)) else {
                    warn!(topic = %cfg.topic, partition = cfg.partition,
                        "replicator: local partition vanished between fetches");
                    return LoopAction::Continue;
                };
                if let Err(e) = entry.value().replicate_batch(batch.clone()).await {
                    warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
                        "replicator: replicate_batch failed");
                }
            }
            LoopAction::Continue
        }
        codes::OFFSET_OUT_OF_RANGE => {
            warn!(topic = %cfg.topic, partition = cfg.partition,
                "replicator.out_of_range; truncating local log to 0");
            if let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition))
                && let Err(e) = entry.value().truncate_to(0).await
            {
                warn!(error = %e, "replicator: truncate_to(0) failed");
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

/// Open a [`Client`] against the partition's leader, retrying with
/// exponential backoff (capped at 5s). Returns `Err` only if shutdown is
/// requested while we were waiting.
async fn connect_with_backoff(cfg: &Config) -> Result<Client, String> {
    let mut delay = Duration::from_millis(100);
    let cap = Duration::from_secs(5);
    loop {
        let attempt = Client::builder()
            .bootstrap(cfg.leader_addr.clone())
            .client_id(cfg.client_id.clone())
            .build();
        let result = tokio::select! {
            () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
            r = attempt => r,
        };
        match result {
            Ok(c) => return Ok(c),
            Err(e) => {
                warn!(addr = %cfg.leader_addr, error = %e,
                    "replicator: connect failed; retrying after {:?}", delay);
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
                    () = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(cap);
            }
        }
    }
}

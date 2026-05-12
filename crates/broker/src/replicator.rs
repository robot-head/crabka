//! Per-(topic, partition) replication task. Issues standard Kafka `Fetch`
//! requests against the partition's leader (with `replica_id` set to the
//! local broker's `node_id`), appending each returned batch to the local
//! `crabka-log`. Handles `OFFSET_OUT_OF_RANGE` by truncating local log to
//! 0 and restarting; `NOT_LEADER_FOR_PARTITION` by returning so the
//! supervisor's next reconcile re-evaluates.

// Tasks 6-7 will wire these in; Task 10 spawns from Broker::start.
#![allow(dead_code)]
// `log_config` is a conventional field name; the "ends with struct name" lint
// is a false positive here.
#![allow(clippy::struct_field_names)]
// `ensure_local_partition` and `run_inner` are stubs — Task 6 adds await
// points; keeping them async avoids a call-site change later.
#![allow(clippy::unused_async)]

use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crabka_log::{Log, LogConfig};
use crabka_raft::NodeId;

use crate::broker::spawn_partition;
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
    if let Err(e) = ensure_local_partition(&cfg).await {
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
async fn ensure_local_partition(cfg: &Config) -> Result<(), String> {
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

async fn run_inner(_cfg: &Config) -> Result<(), String> {
    // Stub — Tasks 6-8 fill this in.
    Ok(())
}

//! Pull-based durable WAL follower for one diskless shard.

use std::{
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use crabka_client_core::{ClientError, Connection, ConnectionOptions};
use crabka_ids::Offset;
use crabka_log::{Log, LogConfig};
use crabka_protocol::{
    owned::fetch_response::{FetchResponse, PartitionData},
    records::RecordsPayload,
};
use crabka_raft::NodeId;
use crabka_security::ListenerProtocol;
use crabka_units::{
    Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    fmt::Human as _,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{
    engine::{split_batches, sync_replica},
    log_view::ShardLog,
    registry::ShardId,
    wire::{QuorumGroup, fetch_request},
};
use crate::{codes, config::ReplicationRuntimeConfig};

const DURABLE_OFFSET_FILE: &str = "wal-durable-offset.checkpoint";
const DURABLE_OFFSET_BACKUP_FILE: &str = "wal-durable-offset.checkpoint.bak";

pub(crate) struct Config {
    pub(crate) node_id: NodeId,
    pub(crate) topic: String,
    pub(crate) shard: ShardId,
    pub(crate) leader_node_id: NodeId,
    pub(crate) leader_epoch: i32,
    pub(crate) leader_host: String,
    pub(crate) leader_port: u16,
    pub(crate) log_dirs: Vec<PathBuf>,
    pub(crate) storage: LogConfig,
    pub(crate) client_id: String,
    pub(crate) shutdown: CancellationToken,
    pub(crate) inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub(crate) inter_broker_listener_protocol: ListenerProtocol,
    pub(crate) inter_broker_server_name: String,
    pub(crate) replication: ReplicationRuntimeConfig,
}

#[derive(Debug)]
struct FollowerLog {
    log: ShardLog,
    durable_offset_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
struct DurableRange {
    start: Offset,
    end: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalFetchFrontiers {
    start: Offset,
    end: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchProgress {
    Idle,
    Advanced,
}

impl FollowerLog {
    fn open(config: &Config) -> Result<Self, crate::BrokerError> {
        let dir = config
            .log_dirs
            .iter()
            .map(|root| voter_dir(root, &config.topic, config.shard, config.node_id))
            .find(|candidate| candidate.exists())
            .map_or_else(
                || {
                    let partition_dir = crate::log_dir::place_partition_dir(
                        &config.log_dirs,
                        &config.topic,
                        config.shard.partition.0,
                    );
                    partition_dir
                        .parent()
                        .map(|root| voter_dir(root, &config.topic, config.shard, config.node_id))
                        .ok_or_else(|| {
                            crate::BrokerError::Replication("WAL log dir has no parent".into())
                        })
                },
                Ok,
            )?;
        Self::open_at(dir, &config.storage)
    }

    fn open_at(dir: PathBuf, storage: &LogConfig) -> Result<Self, crate::BrokerError> {
        let mut log_config = storage.clone();
        log_config.validate_on_open = true;
        let durable_offset_path = dir.join(DURABLE_OFFSET_FILE);
        let mut log = Log::open(dir, log_config)?;
        recover_durable_offset(&mut log, &durable_offset_path)?;
        Ok(Self {
            log: ShardLog::new(Arc::new(std::sync::Mutex::new(log))),
            durable_offset_path,
        })
    }

    #[cfg(test)]
    fn for_log(log: Log) -> Self {
        let durable_offset_path = log.dir().join(DURABLE_OFFSET_FILE);
        write_durable_offset(
            &durable_offset_path,
            DurableRange {
                start: log.log_start_offset(),
                end: log.log_end_offset(),
            },
        )
        .unwrap();
        Self {
            log: ShardLog::new(Arc::new(std::sync::Mutex::new(log))),
            durable_offset_path,
        }
    }

    fn end_offset(&self) -> Offset {
        self.log.lock().log_end_offset()
    }

    fn start_offset(&self) -> Offset {
        self.log.lock().log_start_offset()
    }

    async fn trim_to(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        if offset <= self.start_offset() {
            return Ok(());
        }
        let log = self.log.clone();
        let durable_offset_path = self.durable_offset_path.clone();
        run_blocking(move || {
            let mut log = log.lock();
            log.trim_to_offset(offset)?;
            log.sync()?;
            write_durable_offset(
                &durable_offset_path,
                DurableRange {
                    start: log.log_start_offset(),
                    end: log.log_end_offset(),
                },
            )?;
            Ok(())
        })
        .await
    }

    async fn reset_to(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        let log = self.log.clone();
        let durable_offset_path = self.durable_offset_path.clone();
        run_blocking(move || {
            let mut log = log.lock();
            log.reset_to(offset)?;
            log.sync()?;
            write_durable_offset(
                &durable_offset_path,
                DurableRange {
                    start: offset,
                    end: offset,
                },
            )?;
            Ok(())
        })
        .await
    }

    async fn append(
        &self,
        requested: Offset,
        leader_end: Offset,
        records: Option<RecordsPayload>,
    ) -> Result<Offset, crate::BrokerError> {
        if self.end_offset() != requested {
            return Err(crate::BrokerError::Replication(format!(
                "WAL follower moved from requested offset {} to {}",
                requested.0,
                self.end_offset().0
            )));
        }
        let Some(records) = records else {
            return Ok(requested);
        };
        let mut encoded = BytesMut::with_capacity(records.payload_len());
        records.encode_to(&mut encoded).map_err(|error| {
            crate::BrokerError::Replication(format!("encode WAL fetch: {error}"))
        })?;
        let bytes: Bytes = encoded.freeze();
        let batches = split_batches(&bytes)?;
        let mut expected = requested;
        for batch in &batches {
            if batch.base_offset != expected {
                return Err(crate::BrokerError::Replication(format!(
                    "WAL fetch is not contiguous at {}, got {}",
                    expected.0, batch.base_offset.0
                )));
            }
            expected = Offset(batch.last_offset.0.checked_add(1).ok_or_else(|| {
                crate::BrokerError::Replication("WAL fetch offset overflow".into())
            })?);
        }
        if expected.cmp(&leader_end).is_gt() {
            return Err(crate::BrokerError::Replication(format!(
                "WAL fetch ends at {}, beyond leader LEO {}",
                expected.0, leader_end.0
            )));
        }
        sync_replica(self.log.clone(), &batches).await?;
        let actual = self.end_offset();
        if actual != expected {
            return Err(crate::BrokerError::Replication(format!(
                "WAL follower ended at {}, expected {}",
                actual.0, expected.0
            )));
        }
        let durable_offset_path = self.durable_offset_path.clone();
        let start = self.start_offset();
        run_blocking(move || {
            write_durable_offset(&durable_offset_path, DurableRange { start, end: actual })?;
            Ok(())
        })
        .await?;
        Ok(actual)
    }
}

fn voter_dir(root: &Path, topic: &str, shard: ShardId, node_id: NodeId) -> PathBuf {
    super::shard_dir(root, topic, Some(shard.topic_id), shard.partition)
        .join(format!("voter-{}", node_id.0))
}

/// Copy this broker's checkpointed follower prefix into a newly promoted
/// partition log. The follower directory remains intact so a crash during or
/// after hydration can retry from the same durable source.
pub(crate) fn hydrate_on_promotion(
    log_dirs: &[PathBuf],
    topic: &str,
    shard: ShardId,
    node_id: NodeId,
    storage: &LogConfig,
    destination: &mut Log,
) -> Result<Option<Offset>, crate::BrokerError> {
    let Some(dir) = log_dirs
        .iter()
        .map(|root| voter_dir(root, topic, shard, node_id))
        .find(|candidate| candidate.exists())
    else {
        return Ok(None);
    };
    let follower = FollowerLog::open_at(dir, storage)?;
    let source_start = follower.start_offset();
    let source_end = follower.end_offset();
    let destination_start = destination.log_start_offset();
    let destination_end = destination.log_end_offset();

    if destination_start == destination_end && destination_end < source_end {
        destination.reset_to(source_start)?;
    } else {
        let overlap_start = source_start.max(destination_start);
        let overlap_end = source_end.min(destination_end);
        if overlap_start < overlap_end {
            let source =
                super::engine::read_batches_exact(&follower.log, overlap_start, overlap_end)?;
            let current =
                super::engine::read_log_batches_exact(destination, overlap_start, overlap_end)?;
            if source.len() != current.len()
                || source.iter().zip(&current).any(|(source, current)| {
                    source.base_offset != current.base_offset
                        || source.last_offset != current.last_offset
                        || source.verbatim.bytes != current.verbatim.bytes
                })
            {
                return Err(crate::BrokerError::Replication(format!(
                    "promoted WAL follower diverges from canonical log in {}..{}",
                    overlap_start.0, overlap_end.0
                )));
            }
        } else if destination_end < source_start {
            return Err(crate::BrokerError::Replication(format!(
                "promoted WAL follower starts at {}, after canonical LEO {}",
                source_start.0, destination_end.0
            )));
        }
    }

    if destination.log_end_offset() < source_end {
        let batches = super::engine::read_batches_exact(
            &follower.log,
            destination.log_end_offset(),
            source_end,
        )?;
        for batch in batches {
            destination.append_verbatim_at(&batch.verbatim, batch.base_offset)?;
        }
    }
    if destination.log_end_offset() < source_end {
        return Err(crate::BrokerError::Replication(format!(
            "promoted WAL hydration ended at {}, before durable follower LEO {}",
            destination.log_end_offset().0,
            source_end.0
        )));
    }
    destination.sync()?;
    Ok(Some(source_end))
}

fn recover_durable_offset(log: &mut Log, path: &Path) -> Result<(), crate::BrokerError> {
    let backup = path.with_file_name(DURABLE_OFFSET_BACKUP_FILE);
    let checkpoint = if path.exists() {
        Some(path)
    } else if backup.exists() {
        Some(backup.as_path())
    } else {
        None
    };
    let durable = checkpoint.map_or_else(
        || {
            Ok(DurableRange {
                start: log.log_start_offset(),
                end: log.log_start_offset(),
            })
        },
        |checkpoint| {
            let value = std::fs::read_to_string(checkpoint)?;
            let offsets = value
                .split_ascii_whitespace()
                .map(str::parse::<i64>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    crate::BrokerError::Replication(format!(
                        "decode WAL durable offsets {}: {error}",
                        checkpoint.display()
                    ))
                })?;
            match offsets.as_slice() {
                [start, end] => Ok(DurableRange {
                    start: Offset(*start),
                    end: Offset(*end),
                }),
                _ => Err(crate::BrokerError::Replication(format!(
                    "decode WAL durable offsets {}: expected two offsets",
                    checkpoint.display()
                ))),
            }
        },
    )?;
    let start = log.log_start_offset();
    let end = log.log_end_offset();
    let (true, true) = (
        (start..=end).contains(&durable.start),
        (durable.start..=end).contains(&durable.end),
    ) else {
        return Err(crate::BrokerError::Replication(format!(
            "WAL durable range {}..{} is outside recovered range {}..{}",
            durable.start.0, durable.end.0, start.0, end.0
        )));
    };
    log.truncate_to(durable.end)?;
    log.trim_to_offset(durable.start)?;
    log.sync()?;
    write_durable_offset(path, durable)?;
    Ok(())
}

fn write_durable_offset(path: &Path, durable: DurableRange) -> Result<(), crate::BrokerError> {
    let temporary = path.with_extension("checkpoint.tmp");
    let backup = path.with_file_name(DURABLE_OFFSET_BACKUP_FILE);
    let mut file = std::fs::File::create(&temporary)?;
    writeln!(file, "{} {}", durable.start.0, durable.end.0)?;
    file.sync_all()?;
    drop(file);
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    if path.exists() {
        std::fs::rename(path, &backup)?;
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        restore_durable_offset_backup(path, &backup);
        return Err(error.into());
    }
    if backup.exists() {
        std::fs::remove_file(backup)?;
    }
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn restore_durable_offset_backup(path: &Path, backup: &Path) {
    let (Ok(false), Ok(true)) = (path.try_exists(), backup.try_exists()) else {
        return;
    };
    let _ = std::fs::rename(backup, path);
}

pub(crate) async fn run(config: Config) {
    info!(
        topic = %config.topic,
        partition = config.shard.partition.0,
        leader = config.leader_node_id.0,
        "diskless WAL follower started"
    );
    loop {
        if config.shutdown.is_cancelled() {
            return;
        }
        match FollowerLog::open(&config) {
            Ok(follower) => {
                if let Err(error) = run_inner(&config, &follower).await {
                    if config.shutdown.is_cancelled() {
                        return;
                    }
                    warn!(error = %error, "diskless WAL follower stopped; retrying");
                }
            }
            Err(error) => {
                warn!(error = %error, "diskless WAL follower could not open its log; retrying");
            }
        }
        if sleep_or_cancel(
            &config.shutdown,
            config.replication.unexpected_error_backoff,
        )
        .await
        .is_err()
        {
            return;
        }
    }
}

async fn run_inner(config: &Config, follower: &FollowerLog) -> Result<(), String> {
    let mut connection = connect_with_backoff(config).await?;
    loop {
        let requested = follower.end_offset();
        let mut request = fetch_request(
            QuorumGroup::diskless_wal(config.shard.topic_id, config.shard.partition),
            config.node_id,
            config.leader_epoch,
            requested.0,
            config.replication.fetch_max,
        );
        request.max_wait_ms =
            i32::try_from(config.replication.fetch_max_wait.millis_i64_trunc().max(0))
                .unwrap_or(i32::MAX);
        request.min_bytes = config.replication.fetch_min.bytes_i32();
        let response: FetchResponse = tokio::select! {
            () = config.shutdown.cancelled() => return Ok(()),
            response = connection.send(request) => match response {
                Ok(response) => response,
                Err(ClientError::Disconnected | ClientError::Io(_)) => {
                    connection = connect_with_backoff(config).await?;
                    continue;
                }
                Err(error) => {
                    warn!(error = %error, "diskless WAL fetch failed; reconnecting");
                    sleep_or_cancel(&config.shutdown, config.replication.send_error_backoff)
                        .await?;
                    connection = connect_with_backoff(config).await?;
                    continue;
                }
            },
        };
        let Some(partition) = response_partition(response, config.shard) else {
            sleep_or_cancel(
                &config.shutdown,
                config.replication.unexpected_error_backoff,
            )
            .await?;
            continue;
        };
        match partition.error_code {
            codes::NONE => {
                let frontiers = validate_fetch_frontiers(&partition)?;
                follower
                    .trim_to(frontiers.start)
                    .await
                    .map_err(|error| error.to_string())?;
                let appended = follower
                    .append(requested, frontiers.end, partition.records)
                    .await
                    .map_err(|error| error.to_string())?;
                match fetch_progress(requested, appended)? {
                    FetchProgress::Idle => {
                        sleep_or_cancel(
                            &config.shutdown,
                            config.replication.throttle_exhausted_backoff,
                        )
                        .await?;
                    }
                    FetchProgress::Advanced => {}
                }
            }
            codes::OFFSET_OUT_OF_RANGE => {
                let reset = validate_reset_offset(&partition)?;
                follower
                    .reset_to(reset)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            codes::UNKNOWN_TOPIC_OR_PARTITION => {
                sleep_or_cancel(
                    &config.shutdown,
                    config.replication.unknown_topic_retry_delay,
                )
                .await?;
            }
            codes::NOT_LEADER_OR_FOLLOWER
            | codes::FENCED_LEADER_EPOCH
            | codes::UNKNOWN_LEADER_EPOCH => return Ok(()),
            error_code => {
                warn!(
                    error_code,
                    "diskless WAL follower received an unexpected error"
                );
                sleep_or_cancel(
                    &config.shutdown,
                    config.replication.unexpected_error_backoff,
                )
                .await?;
            }
        }
    }
}

fn validate_fetch_frontiers(partition: &PartitionData) -> Result<WalFetchFrontiers, String> {
    let start = Offset(partition.log_start_offset);
    let high_watermark = Offset(partition.high_watermark);
    let end = Offset(partition.last_stable_offset);
    let (true, true) = (
        (Offset(0)..=end).contains(&start),
        (start..=end).contains(&high_watermark),
    ) else {
        return Err("leader returned invalid WAL frontiers".into());
    };
    Ok(WalFetchFrontiers { start, end })
}

fn validate_reset_offset(partition: &PartitionData) -> Result<Offset, String> {
    let start = Offset(partition.log_start_offset);
    let end = Offset(partition.last_stable_offset);
    let true = (Offset(0)..=end).contains(&start) else {
        return Err("leader returned invalid WAL reset offset".into());
    };
    Ok(start)
}

fn fetch_progress(requested: Offset, appended: Offset) -> Result<FetchProgress, String> {
    match appended.cmp(&requested) {
        std::cmp::Ordering::Less => Err(format!(
            "WAL follower regressed from requested offset {} to {}",
            requested.0, appended.0
        )),
        std::cmp::Ordering::Equal => Ok(FetchProgress::Idle),
        std::cmp::Ordering::Greater => Ok(FetchProgress::Advanced),
    }
}

fn response_partition(response: FetchResponse, shard: ShardId) -> Option<PartitionData> {
    let topic_id = crabka_protocol::primitives::uuid::Uuid(*shard.topic_id.as_bytes());
    response
        .responses
        .into_iter()
        .find(|topic| topic.topic_id == topic_id)?
        .partitions
        .into_iter()
        .find(|partition| partition.partition_index == shard.partition)
}

async fn connect_with_backoff(config: &Config) -> Result<Connection, String> {
    let mut delay = config.replication.reconnect_initial_delay;
    loop {
        let attempt = config.inter_broker_client.connect_as_connection(
            &config.leader_host,
            config.leader_port,
            config.inter_broker_listener_protocol,
            &config.inter_broker_server_name,
            follower_connection_options(&config.client_id),
        );
        let result = tokio::select! {
            () = config.shutdown.cancelled() => return Err("cancelled".into()),
            result = attempt => result,
        };
        match result {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                warn!(
                    host = %config.leader_host,
                    port = config.leader_port,
                    error = %error,
                    "diskless WAL follower connect failed; retrying after {}",
                    delay.human()
                );
                sleep_or_cancel(&config.shutdown, delay).await?;
                delay = next_reconnect_delay(delay, config.replication.reconnect_delay_cap);
            }
        }
    }
}

fn follower_connection_options(client_id: &str) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_owned(),
        ..ConnectionOptions::default()
    }
}

fn next_reconnect_delay(delay: Time, cap: Time) -> Time {
    (delay + delay).min(cap)
}

async fn sleep_or_cancel(shutdown: &CancellationToken, delay: Time) -> Result<(), String> {
    tokio::select! {
        () = shutdown.cancelled() => Err("cancelled".into()),
        () = tokio::time::sleep(delay.to_std()) => Ok(()),
    }
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, crate::BrokerError> + Send + 'static,
) -> Result<T, crate::BrokerError> {
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(operation)
    } else {
        tokio::task::spawn_blocking(operation)
            .await
            .map_err(|error| {
                crate::partition_writer::storage_failure_error(
                    "WAL follower storage task panicked",
                    error,
                )
            })?
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use assert2::assert;
    use crabka_ids::PartitionIndex;
    use crabka_log::LogConfig;
    use crabka_protocol::records::{Record, RecordBatch};
    use crabka_units::{mebibytes, millis, secs};

    use super::*;
    use crate::wal::{WalStore as _, quorum::registry::WalShardRegistry};

    fn test_config(root: &Path, shutdown: CancellationToken) -> Config {
        Config {
            node_id: NodeId(2),
            topic: "diskless".into(),
            shard: ShardId {
                topic_id: uuid::Uuid::from_u128(99),
                partition: PartitionIndex(0),
            },
            leader_node_id: NodeId(1),
            leader_epoch: 7,
            leader_host: "127.0.0.1".into(),
            leader_port: 0,
            log_dirs: vec![root.to_path_buf()],
            storage: LogConfig::default(),
            client_id: "wal-follower-test".into(),
            shutdown,
            inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(
                None, None,
            )),
            inter_broker_listener_protocol: ListenerProtocol::Plaintext,
            inter_broker_server_name: "localhost".into(),
            replication: ReplicationRuntimeConfig::default(),
        }
    }

    fn frontiers(start: i64, high_watermark: i64, end: i64) -> PartitionData {
        PartitionData {
            log_start_offset: start,
            high_watermark,
            last_stable_offset: end,
            ..Default::default()
        }
    }

    #[test]
    fn fetch_frontiers_require_an_ordered_nonnegative_range() {
        assert!(
            validate_fetch_frontiers(&frontiers(5, 6, 7))
                == Ok(WalFetchFrontiers {
                    start: Offset(5),
                    end: Offset(7),
                })
        );
        for invalid in [
            frontiers(-1, 0, 1),
            frontiers(0, -1, 1),
            frontiers(0, 2, 1),
            frontiers(2, 2, 1),
            frontiers(0, 0, -1),
        ] {
            assert!(validate_fetch_frontiers(&invalid).is_err());
        }
    }

    #[test]
    fn reset_offset_must_fall_inside_the_leader_log() {
        assert!(validate_reset_offset(&frontiers(5, 5, 7)) == Ok(Offset(5)));
        for invalid in [frontiers(-1, 0, 7), frontiers(8, 8, 7), frontiers(0, 0, -1)] {
            assert!(validate_reset_offset(&invalid).is_err());
        }
    }

    #[test]
    fn fetch_progress_distinguishes_idle_advance_and_regression() {
        assert!(fetch_progress(Offset(4), Offset(4)) == Ok(FetchProgress::Idle));
        assert!(fetch_progress(Offset(4), Offset(5)) == Ok(FetchProgress::Advanced));
        assert!(fetch_progress(Offset(4), Offset(3)).is_err());
    }

    #[test]
    fn follower_connection_and_backoff_policy_preserve_runtime_values() {
        let options = follower_connection_options("wal-client");
        assert!(options.client_id == "wal-client");
        assert!(next_reconnect_delay(millis(100), secs(1)) == millis(200));
        assert!(next_reconnect_delay(millis(600), secs(1)) == secs(1));
        assert!(next_reconnect_delay(secs(1), secs(1)) == secs(1));
    }

    #[test]
    fn durable_offset_backup_is_restored_only_when_primary_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DURABLE_OFFSET_FILE);
        let backup = dir.path().join(DURABLE_OFFSET_BACKUP_FILE);
        std::fs::write(&backup, "0 4\n").unwrap();

        restore_durable_offset_backup(&path, &backup);

        assert!(std::fs::read_to_string(&path).unwrap() == "0 4\n");
        assert!(!backup.exists());
        std::fs::write(&backup, "0 3\n").unwrap();

        restore_durable_offset_backup(&path, &backup);

        assert!(std::fs::read_to_string(&path).unwrap() == "0 4\n");
        assert!(std::fs::read_to_string(&backup).unwrap() == "0 3\n");
    }

    #[tokio::test(start_paused = true)]
    async fn follower_sleep_completes_on_delay_or_cancellation() {
        let shutdown = CancellationToken::new();
        assert!(sleep_or_cancel(&shutdown, millis(10)).await.is_ok());

        shutdown.cancel();
        assert!(sleep_or_cancel(&shutdown, secs(1)).await.is_err());
    }

    #[tokio::test]
    async fn follower_run_retries_until_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let shutdown = CancellationToken::new();
        let config = test_config(dir.path(), shutdown.clone());
        let mut task = tokio::spawn(run(config));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut task)
                .await
                .is_err()
        );
        shutdown.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn follower_inner_loop_propagates_connect_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let config = test_config(dir.path(), shutdown);
        let follower = FollowerLog::for_log(
            Log::open(dir.path().join("follower"), LogConfig::default()).unwrap(),
        );

        let error = run_inner(&config, &follower).await.unwrap_err();

        assert!(error == "cancelled");
    }

    #[tokio::test]
    async fn follower_appends_and_syncs_a_contiguous_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let batch = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let end = follower
            .append(Offset(0), Offset(1), Some(RecordsPayload::V2(vec![batch])))
            .await
            .unwrap();

        assert_eq!(end, Offset(1));
        drop(follower);
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(reopened.log_end_offset(), Offset(1));
    }

    #[tokio::test]
    async fn follower_rejects_a_gap_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let batch = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let error = follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(vec![batch])))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("not contiguous"));
        assert_eq!(follower.end_offset(), Offset(0));
    }

    #[tokio::test]
    async fn follower_accepts_a_partial_fetch_and_rejects_a_leader_overrun() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let first = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let end = follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(vec![first])))
            .await
            .unwrap();

        assert!(end == Offset(1));
        let beyond_leader = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        let error = follower
            .append(
                Offset(1),
                Offset(1),
                Some(RecordsPayload::V2(vec![beyond_leader])),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("beyond leader LEO"));
        assert!(follower.end_offset() == Offset(1));
    }

    #[tokio::test]
    async fn follower_reset_persists_the_leader_log_start() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());

        follower.reset_to(Offset(7)).await.unwrap();

        assert_eq!(follower.end_offset(), Offset(7));
        drop(follower);
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(reopened.log_start_offset(), Offset(7));
        assert_eq!(reopened.log_end_offset(), Offset(7));
    }

    #[tokio::test]
    async fn follower_trim_persists_the_leader_log_start() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let batches = (0..2)
            .map(|base_offset| RecordBatch {
                base_offset,
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .collect();
        follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(batches)))
            .await
            .unwrap();

        follower.trim_to(Offset(1)).await.unwrap();

        assert_eq!(follower.start_offset(), Offset(1));
        assert_eq!(follower.end_offset(), Offset(2));
        drop(follower);
        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        recover_durable_offset(&mut reopened, &dir.path().join(DURABLE_OFFSET_FILE)).unwrap();
        assert_eq!(reopened.log_start_offset(), Offset(1));
        assert_eq!(reopened.log_end_offset(), Offset(2));
    }

    #[test]
    fn follower_recovery_discards_a_suffix_beyond_the_durable_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = dir.path().join(DURABLE_OFFSET_FILE);
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut durable = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        log.append(&mut durable).unwrap();
        log.sync().unwrap();
        write_durable_offset(
            &checkpoint,
            DurableRange {
                start: Offset(0),
                end: Offset(1),
            },
        )
        .unwrap();
        let mut uncertain = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        log.append(&mut uncertain).unwrap();
        assert_eq!(log.log_end_offset(), Offset(2));
        drop(log);

        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        recover_durable_offset(&mut reopened, &checkpoint).unwrap();

        assert_eq!(reopened.log_end_offset(), Offset(1));
        assert_eq!(std::fs::read_to_string(checkpoint).unwrap().trim(), "0 1");
    }

    #[test]
    fn promotion_hydrates_exact_checkpointed_bytes_without_regression() {
        let root = tempfile::tempdir().unwrap();
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(101),
            partition: PartitionIndex(0),
        };
        let follower_dir = voter_dir(root.path(), "diskless", shard, NodeId(2));
        let mut follower = Log::open(&follower_dir, LogConfig::default()).unwrap();
        let mut durable = RecordBatch {
            records: vec![
                Record {
                    value: Some(Bytes::from_static(b"a")),
                    ..Record::default()
                },
                Record {
                    offset_delta: 1,
                    value: Some(Bytes::from_static(b"b")),
                    ..Record::default()
                },
            ],
            last_offset_delta: 1,
            ..RecordBatch::default()
        };
        follower.append(&mut durable).unwrap();
        follower.sync().unwrap();
        write_durable_offset(
            &follower_dir.join(DURABLE_OFFSET_FILE),
            DurableRange {
                start: Offset(0),
                end: Offset(2),
            },
        )
        .unwrap();
        let mut uncertain = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"uncertain")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        follower.append(&mut uncertain).unwrap();
        follower.sync().unwrap();
        drop(follower);

        let destination_dir = crate::log_dir::partition_dir(root.path(), "diskless", 0);
        let mut destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
        assert!(
            hydrate_on_promotion(
                &[root.path().to_path_buf()],
                "diskless",
                shard,
                NodeId(2),
                &LogConfig::default(),
                &mut destination,
            )
            .unwrap()
                == Some(Offset(2))
        );
        assert!(destination.log_end_offset() == Offset(2));
        let source = Log::open(&follower_dir, LogConfig::default()).unwrap();
        assert!(source.log_end_offset() == Offset(2));
        assert!(
            source
                .read_raw(Offset(0), Offset(2), mebibytes(1))
                .unwrap()
                .bytes
                == destination
                    .read_raw(Offset(0), Offset(2), mebibytes(1))
                    .unwrap()
                    .bytes
        );

        let mut newer = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"newer")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        destination.append(&mut newer).unwrap();
        destination.sync().unwrap();
        assert!(
            hydrate_on_promotion(
                &[root.path().to_path_buf()],
                "diskless",
                shard,
                NodeId(2),
                &LogConfig::default(),
                &mut destination,
            )
            .unwrap()
                == Some(Offset(2))
        );
        assert!(destination.log_end_offset() == Offset(3));
        assert!(follower_dir.exists());
    }

    #[test]
    fn promotion_retries_after_reopening_a_partial_destination() {
        let root = tempfile::tempdir().unwrap();
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(102),
            partition: PartitionIndex(0),
        };
        let follower_dir = voter_dir(root.path(), "diskless", shard, NodeId(2));
        let mut follower = Log::open(&follower_dir, LogConfig::default()).unwrap();
        let mut first = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"first")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        let mut second = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"second")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        follower.append(&mut first).unwrap();
        follower.append(&mut second).unwrap();
        follower.sync().unwrap();
        write_durable_offset(
            &follower_dir.join(DURABLE_OFFSET_FILE),
            DurableRange {
                start: Offset(0),
                end: Offset(2),
            },
        )
        .unwrap();

        let destination_dir = crate::log_dir::partition_dir(root.path(), "diskless", 0);
        {
            let mut partial = Log::open(&destination_dir, LogConfig::default()).unwrap();
            let prefix =
                super::super::engine::read_log_batches_exact(&follower, Offset(0), Offset(1))
                    .unwrap();
            partial
                .append_verbatim_at(&prefix[0].verbatim, prefix[0].base_offset)
                .unwrap();
            partial.sync().unwrap();
        }

        // Model a process restart after only the first durable batch was
        // adopted. Reopening the canonical directory and retrying hydration
        // must retain the exact prefix and append the missing durable tail.
        let mut reopened = Log::open(&destination_dir, LogConfig::default()).unwrap();
        assert!(
            hydrate_on_promotion(
                &[root.path().to_path_buf()],
                "diskless",
                shard,
                NodeId(2),
                &LogConfig::default(),
                &mut reopened,
            )
            .unwrap()
                == Some(Offset(2))
        );
        assert!(reopened.log_end_offset() == Offset(2));
        assert!(
            follower
                .read_raw(Offset(0), Offset(2), mebibytes(1))
                .unwrap()
                .bytes
                == reopened
                    .read_raw(Offset(0), Offset(2), mebibytes(1))
                    .unwrap()
                    .bytes
        );
    }

    #[test]
    fn follower_recovery_rejects_incomplete_and_invalid_durable_ranges() {
        for (checkpoint_value, expected_error) in [
            ("1\n", "expected two offsets"),
            ("-1 0\n", "outside recovered range"),
            ("1 0\n", "outside recovered range"),
            ("0 2\n", "outside recovered range"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let checkpoint = dir.path().join(DURABLE_OFFSET_FILE);
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut batch = RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            };
            log.append(&mut batch).unwrap();
            log.sync().unwrap();
            std::fs::write(&checkpoint, checkpoint_value).unwrap();

            let error = recover_durable_offset(&mut log, &checkpoint).unwrap_err();

            assert!(
                error.to_string().contains(expected_error),
                "checkpoint {checkpoint_value:?}: {error}"
            );
            assert!(log.log_start_offset() == Offset(0));
            assert!(log.log_end_offset() == Offset(1));
        }
    }

    #[tokio::test]
    async fn follower_fsync_ack_releases_the_leader_quorum_wait() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(leader_dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = Arc::new(
            super::super::QuorumWalStore::for_distributed_partition(
                uuid::Uuid::from_u128(42),
                PartitionIndex(0),
                source.clone(),
                None,
                3,
            )
            .unwrap(),
        );
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(42),
            partition: PartitionIndex(0),
        };
        let registry = WalShardRegistry::new(NodeId(1));
        registry.replace_placements(&HashMap::from([(
            shard,
            vec![NodeId(1), NodeId(2), NodeId(3)],
        )]));
        registry.insert(shard, store.engine());
        let follower =
            FollowerLog::for_log(Log::open(follower_dir.path(), LogConfig::default()).unwrap());
        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        source.lock().unwrap().append(&mut batch).unwrap();
        let leo = source.lock().unwrap().log_end_offset();
        let syncing = Arc::clone(&store);
        let sync = tokio::spawn(async move { syncing.sync_durable(leo).await });

        let request = fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            NodeId(2),
            0,
            0,
            crabka_units::mebibytes(1),
        );
        let response = registry.route_fetch_request(&request).unwrap().unwrap();
        let partition = response_partition(response, shard).unwrap();
        let follower_end = follower
            .append(
                Offset(0),
                Offset(partition.last_stable_offset),
                partition.records,
            )
            .await
            .unwrap();
        assert_eq!(follower_end, leo);

        let acknowledgement = fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            NodeId(2),
            0,
            follower_end.0,
            crabka_units::mebibytes(1),
        );
        registry
            .route_fetch_request(&acknowledgement)
            .unwrap()
            .unwrap();

        assert_eq!(sync.await.unwrap().unwrap(), leo);
        assert_eq!(store.engine().durable_watermark(), leo);
    }
}

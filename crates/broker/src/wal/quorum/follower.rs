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

impl FollowerLog {
    fn open(config: &Config) -> Result<Self, crate::BrokerError> {
        let voter_dir = |root: &std::path::Path| {
            super::shard_dir(
                root,
                &config.topic,
                Some(config.shard.topic_id),
                config.shard.partition,
            )
            .join(format!("voter-{}", config.node_id.0))
        };
        let dir = config
            .log_dirs
            .iter()
            .map(|root| voter_dir(root))
            .find(|candidate| candidate.exists())
            .map_or_else(
                || {
                    let partition_dir = crate::log_dir::place_partition_dir(
                        &config.log_dirs,
                        &config.topic,
                        config.shard.partition.0,
                    );
                    partition_dir.parent().map(voter_dir).ok_or_else(|| {
                        crate::BrokerError::Replication("WAL log dir has no parent".into())
                    })
                },
                Ok,
            )?;
        let mut log_config = config.storage.clone();
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
        if expected > leader_end {
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
                [end] => Ok(DurableRange {
                    start: log.log_start_offset(),
                    end: Offset(*end),
                }),
                [start, end] => Ok(DurableRange {
                    start: Offset(*start),
                    end: Offset(*end),
                }),
                _ => Err(crate::BrokerError::Replication(format!(
                    "decode WAL durable offsets {}: expected one or two offsets",
                    checkpoint.display()
                ))),
            }
        },
    )?;
    let start = log.log_start_offset();
    let end = log.log_end_offset();
    if durable.start < start || durable.start > durable.end || durable.end > end {
        return Err(crate::BrokerError::Replication(format!(
            "WAL durable range {}..{} is outside recovered range {}..{}",
            durable.start.0, durable.end.0, start.0, end.0
        )));
    }
    if durable.end < end {
        log.truncate_to(durable.end)?;
    }
    if durable.start > start {
        log.trim_to_offset(durable.start)?;
    }
    if durable.end < end || durable.start > start {
        log.sync()?;
    }
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
        if backup.exists() && !path.exists() {
            let _ = std::fs::rename(&backup, path);
        }
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
                if let Err(error) = run_inner(&config, &follower).await
                    && !config.shutdown.is_cancelled()
                {
                    warn!(error = %error, "diskless WAL follower stopped; retrying");
                }
            }
            Err(error) => {
                warn!(error = %error, "diskless WAL follower could not open its log; retrying");
            }
        }
        if sleep_or_cancel(&config, config.replication.unexpected_error_backoff)
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
                    sleep_or_cancel(config, config.replication.send_error_backoff).await?;
                    connection = connect_with_backoff(config).await?;
                    continue;
                }
            },
        };
        let Some(partition) = response_partition(response, config.shard) else {
            sleep_or_cancel(config, config.replication.unexpected_error_backoff).await?;
            continue;
        };
        match partition.error_code {
            codes::NONE => {
                let leader_end = Offset(partition.last_stable_offset);
                let leader_start = Offset(partition.log_start_offset);
                if leader_start.0 < 0
                    || leader_end.0 < 0
                    || partition.high_watermark > leader_end.0
                    || partition.log_start_offset > leader_end.0
                {
                    return Err("leader returned invalid WAL frontiers".into());
                }
                follower
                    .trim_to(leader_start)
                    .await
                    .map_err(|error| error.to_string())?;
                let appended = follower
                    .append(requested, leader_end, partition.records)
                    .await
                    .map_err(|error| error.to_string())?;
                if appended == requested {
                    sleep_or_cancel(config, config.replication.throttle_exhausted_backoff).await?;
                }
            }
            codes::OFFSET_OUT_OF_RANGE => {
                if partition.log_start_offset < 0
                    || partition.log_start_offset > partition.last_stable_offset
                {
                    return Err("leader returned invalid WAL reset offset".into());
                }
                follower
                    .reset_to(Offset(partition.log_start_offset))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            codes::UNKNOWN_TOPIC_OR_PARTITION => {
                sleep_or_cancel(config, config.replication.unknown_topic_retry_delay).await?;
            }
            codes::NOT_LEADER_OR_FOLLOWER
            | codes::FENCED_LEADER_EPOCH
            | codes::UNKNOWN_LEADER_EPOCH => return Ok(()),
            error_code => {
                warn!(
                    error_code,
                    "diskless WAL follower received an unexpected error"
                );
                sleep_or_cancel(config, config.replication.unexpected_error_backoff).await?;
            }
        }
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
            ConnectionOptions {
                client_id: config.client_id.clone(),
                ..ConnectionOptions::default()
            },
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
                sleep_or_cancel(config, delay).await?;
                let doubled = delay * 2.0;
                delay = if doubled > config.replication.reconnect_delay_cap {
                    config.replication.reconnect_delay_cap
                } else {
                    doubled
                };
            }
        }
    }
}

async fn sleep_or_cancel(config: &Config, delay: Time) -> Result<(), String> {
    tokio::select! {
        () = config.shutdown.cancelled() => Err("cancelled".into()),
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

    use crabka_ids::PartitionIndex;
    use crabka_log::LogConfig;
    use crabka_protocol::records::{Record, RecordBatch};

    use super::*;
    use crate::wal::{WalStore as _, quorum::registry::WalShardRegistry};

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

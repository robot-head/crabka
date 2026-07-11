//! Barrier-based recovery primitives.

use std::sync::Arc;

use crabka_client_admin::AdminClient;
use crabka_client_core::{Connection, ConnectionOptions, security::ClientSecurity};
use crabka_client_producer::{Acks, Producer};
use crabka_gres_ranges::{RangeId, TenantName};
use crabka_pgkv::{Kv, RestoreKv};
use crabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::sync::Mutex;

use crate::{
    checkpoint::{CheckpointStore, CheckpointWalPruner, restore_latest},
    error::SubstrateError,
    frame::{BARRIER_SEQ, WalFrame},
    replay::{ReplayItem, ReplayOutcome, replay_committed_frames, replay_committed_frames_from},
    topic::{ensure_wal_topic_for_range, transactional_id_for_range, wal_topic_for_range},
    writer::{
        FenceLease, GroupCommitAck, GroupCommitRequest, TransactionalWalWriter, WalAppendAck,
        WriterGeneration,
    },
};

const READ_COMMITTED: i8 = 1;
const PARTITION: i32 = 0;
const FETCH_MAX_WAIT_MS: i32 = 100;
const FETCH_MAX_BYTES: i32 = 1_048_576;
const EMPTY_FETCH_RETRIES: usize = 100;
const OFFSET_OUT_OF_RANGE: i16 = 1;

/// Live Kafka recovery output.
pub struct LiveRecovered {
    /// Fenced transactional producer, ready for the WAL writer.
    pub producer: Arc<Producer>,
    /// Generation token for the engine-facing seams.
    pub generation: WriterGeneration,
    /// Next non-barrier journal sequence to write.
    pub next_journal_seq: u64,
    /// Offset covered by the recovery barrier.
    pub barrier_offset: i64,
}

/// Range-selected live Kafka recovery input.
#[derive(Debug, Clone)]
pub struct LiveRecoveryConfig {
    /// Broker bootstrap address list.
    pub bootstrap: String,
    /// Tenant recovered by this compute.
    pub tenant: TenantName,
    /// Range recovered by this compute.
    pub range: RangeId,
    /// Optional Kafka client security configuration.
    pub security: Option<ClientSecurity>,
    /// Optional checkpoint object store used to seed recovery before WAL tail replay.
    pub checkpoints: Option<LiveRecoveryCheckpoints>,
}

/// Checkpoint inputs for live recovery.
#[derive(Clone)]
pub struct LiveRecoveryCheckpoints {
    /// Durable checkpoint objects for this tenant/range.
    pub store: Arc<dyn CheckpointStore>,
}

impl std::fmt::Debug for LiveRecoveryCheckpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveRecoveryCheckpoints")
            .finish_non_exhaustive()
    }
}

impl LiveRecoveryConfig {
    /// Build a live recovery config for a tenant range.
    #[must_use]
    pub fn new(
        bootstrap: impl Into<String>,
        tenant: TenantName,
        range: RangeId,
        security: Option<ClientSecurity>,
    ) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            tenant,
            range,
            security,
            checkpoints: None,
        }
    }

    /// Use checkpoint restore before committed WAL tail replay.
    #[must_use]
    pub fn with_checkpoints(mut self, store: Arc<dyn CheckpointStore>) -> Self {
        self.checkpoints = Some(LiveRecoveryCheckpoints { store });
        self
    }

    /// WAL topic selected for replay and barrier writes.
    #[must_use]
    pub fn wal_topic(&self) -> String {
        wal_topic_for_range(&self.tenant, self.range)
    }

    /// Transactional producer id selected for fencing this range.
    #[must_use]
    pub fn transactional_id(&self) -> String {
        transactional_id_for_range(&self.tenant, self.range)
    }

    /// Checkpoint namespace selected for this tenant range.
    #[must_use]
    pub fn checkpoint_namespace(&self) -> String {
        format!("{}/r{}", self.tenant, self.range.as_u32())
    }

    fn client_id(&self) -> String {
        format!("crabka-gres-{}-r{}", self.tenant, self.range)
    }
}

/// Recover a tenant from a live Kafka-backed substrate WAL.
pub async fn recover_live(
    bootstrap: &str,
    tenant: &str,
    security: Option<ClientSecurity>,
    store: &dyn Kv,
) -> Result<LiveRecovered, SubstrateError> {
    let tenant = TenantName::parse(tenant)
        .map_err(|error| SubstrateError::Unavailable(format!("tenant name: {error}")))?;
    recover_live_for_range(
        LiveRecoveryConfig::new(bootstrap, tenant, RangeId::COORDINATOR, security),
        store,
    )
    .await
}

/// Recover a tenant range from a live Kafka-backed substrate WAL.
pub async fn recover_live_for_range(
    config: LiveRecoveryConfig,
    store: &dyn Kv,
) -> Result<LiveRecovered, SubstrateError> {
    if config.checkpoints.is_some() {
        return Err(SubstrateError::Unavailable(
            "checkpoint restore requires a restore-capable KV store".into(),
        ));
    }
    recover_live_for_range_inner(config, store, None).await
}

/// Recover a tenant range from a live Kafka-backed substrate WAL, restoring a checkpoint first if configured.
pub async fn recover_live_for_range_with_restore(
    config: LiveRecoveryConfig,
    store: &dyn RestoreKv,
) -> Result<LiveRecovered, SubstrateError> {
    recover_live_for_range_inner(config, store, Some(store)).await
}

/// Read the committed WAL tail in the exact interval `(after_offset, barrier_offset]`.
///
/// The reader uses Kafka `READ_COMMITTED`, omits Kafka control batches and
/// tombstones exactly as recovery does, and fails rather than returning an
/// unbounded or incomplete interval.
///
/// # Errors
///
/// Returns an error when the requested interval is invalid, the topic cannot
/// be resolved, or the committed barrier cannot be read.
pub async fn read_live_committed_tail(
    config: &LiveRecoveryConfig,
    after_offset: i64,
    barrier_offset: i64,
) -> Result<Vec<ReplayItem>, SubstrateError> {
    if after_offset >= barrier_offset {
        return Err(SubstrateError::Unavailable(format!(
            "bounded WAL tail requires after offset {after_offset} below barrier {barrier_offset}"
        )));
    }
    let bootstrap_addrs = parse_bootstrap_addrs(&config.bootstrap)?;
    let topic = config.wal_topic();
    let mut admin = AdminClient::connect_secured(&bootstrap_addrs, config.security.clone())
        .await
        .map_err(|error| SubstrateError::Unavailable(format!("admin connect: {error}")))?;
    let topic_uuid = resolve_topic_uuid(&mut admin, &topic).await?;
    let reader = KafkaCommittedWalReader::new(
        bootstrap_addrs,
        topic,
        topic_uuid,
        barrier_offset,
        config.security.clone(),
    );
    bounded_committed_tail(&reader, after_offset, barrier_offset).await
}

async fn recover_live_for_range_inner(
    config: LiveRecoveryConfig,
    store: &dyn Kv,
    restore_store: Option<&dyn RestoreKv>,
) -> Result<LiveRecovered, SubstrateError> {
    let bootstrap_addrs = parse_bootstrap_addrs(&config.bootstrap)?;
    let mut admin = AdminClient::connect_secured(&bootstrap_addrs, config.security.clone())
        .await
        .map_err(|error| SubstrateError::Unavailable(format!("admin connect: {error}")))?;
    let topic = ensure_wal_topic_for_range(&mut admin, &config.tenant, config.range).await?;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap_addrs[0].clone())
            .client_id(config.client_id())
            .acks(Acks::All)
            .transactional_id(config.transactional_id())
            .maybe_security(config.security.clone())
            .build()
            .await
            .map_err(|error| SubstrateError::Unavailable(format!("producer build: {error}")))?,
    );
    producer
        .init_transactions()
        .await
        .map_err(|error| SubstrateError::Unavailable(format!("init transactions: {error}")))?;

    let barrier_writer = crate::writer::ProducerWalWriter::new(producer.clone(), topic.clone());
    let barrier = barrier_writer
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![WalFrame {
                journal_seq: BARRIER_SEQ,
                ops: Vec::new(),
            }],
        })
        .await?
        .frames
        .into_iter()
        .next()
        .ok_or_else(|| SubstrateError::Unavailable("barrier did not produce an ack".into()))?;

    let topic_uuid = resolve_topic_uuid(&mut admin, &topic).await?;
    let checkpoint_namespace = config.checkpoint_namespace();
    let reader = KafkaCommittedWalReader::new(
        bootstrap_addrs,
        topic,
        topic_uuid,
        barrier.offset,
        config.security,
    );
    let recovery_barrier = RecoveryBarrier {
        generation: WriterGeneration(0),
        offset: barrier.offset,
    };
    let outcome = recover_store_after_barrier(
        store,
        restore_store,
        config.checkpoints.as_ref(),
        &checkpoint_namespace,
        &reader,
        &recovery_barrier,
    )
    .await?;

    Ok(LiveRecovered {
        producer,
        generation: WriterGeneration(0),
        next_journal_seq: outcome.next_journal_seq,
        barrier_offset: barrier.offset,
    })
}

async fn recover_store_after_barrier(
    kv: &dyn Kv,
    restore_kv: Option<&dyn RestoreKv>,
    checkpoints: Option<&LiveRecoveryCheckpoints>,
    tenant: &str,
    reader: &dyn CommittedWalReader,
    barrier: &RecoveryBarrier,
) -> Result<ReplayOutcome, SubstrateError> {
    let Some(checkpoints) = checkpoints else {
        return replay_committed_frames(kv, reader.committed_from_start().await?, barrier.offset);
    };
    let restore_kv = restore_kv.ok_or_else(|| {
        SubstrateError::Unavailable("checkpoint restore requires a restore-capable KV store".into())
    })?;
    let log_start = reader.log_start_offset().await?;
    let restored = restore_latest(
        checkpoints.store.as_ref(),
        tenant,
        restore_kv,
        barrier.generation.0,
        log_start,
    )
    .await?;
    if let (None, Some(log_start)) = (restored, log_start)
        && log_start > 0
    {
        return Err(SubstrateError::Checkpoint(format!(
            "no valid checkpoint covers retained WAL starting at {log_start}"
        )));
    }
    let (replay_start, expected) = restored.map_or((0, 0), |restored| {
        if restored.wal_generation == barrier.generation.0 {
            (
                restored.covered_offset.saturating_add(1),
                restored.journal_seq,
            )
        } else {
            (0, 0)
        }
    });
    replay_committed_frames_from(
        kv,
        reader.committed_from(replay_start).await?,
        barrier.offset,
        replay_start,
        expected,
    )
}

fn parse_bootstrap_addrs(bootstrap: &str) -> Result<Vec<String>, SubstrateError> {
    let addrs: Vec<_> = bootstrap
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if addrs.is_empty() {
        return Err(SubstrateError::Unavailable(
            "substrate bootstrap address list is empty".into(),
        ));
    }
    Ok(addrs)
}

async fn resolve_topic_uuid(
    admin: &mut AdminClient,
    topic: &str,
) -> Result<WireUuid, SubstrateError> {
    let metadata = admin
        .metadata(&[topic])
        .await
        .map_err(|error| SubstrateError::Unavailable(format!("metadata: {error}")))?;
    let entry = metadata
        .topics
        .into_iter()
        .find(|entry| entry.name == topic)
        .ok_or_else(|| SubstrateError::Unavailable(format!("topic {topic} missing in metadata")))?;
    if let Some(error) = entry.error {
        return Err(SubstrateError::Unavailable(format!(
            "metadata for topic {topic}: {} ({})",
            error.name, error.code
        )));
    }
    Ok(entry
        .topic_id
        .map_or(WireUuid::ZERO, |uuid| WireUuid(uuid.into_bytes())))
}

struct KafkaCommittedWalReader {
    bootstrap_addrs: Vec<String>,
    topic: String,
    topic_uuid: WireUuid,
    barrier_offset: i64,
    security: Option<ClientSecurity>,
}

impl KafkaCommittedWalReader {
    fn new(
        bootstrap_addrs: Vec<String>,
        topic: String,
        topic_uuid: WireUuid,
        barrier_offset: i64,
        security: Option<ClientSecurity>,
    ) -> Self {
        Self {
            bootstrap_addrs,
            topic,
            topic_uuid,
            barrier_offset,
            security,
        }
    }

    async fn open_connection(&self) -> Result<Connection, SubstrateError> {
        let host_port = self.bootstrap_addrs.first().ok_or_else(|| {
            SubstrateError::Unavailable("substrate bootstrap address list is empty".into())
        })?;
        let mut addrs = tokio::net::lookup_host(host_port).await.map_err(|error| {
            SubstrateError::Unavailable(format!("DNS lookup {host_port}: {error}"))
        })?;
        let addr = addrs
            .next()
            .ok_or_else(|| SubstrateError::Unavailable(format!("no addresses for {host_port}")))?;
        Connection::connect_with_options(
            addr,
            ConnectionOptions {
                client_id: "crabka-gres-substrate-replay".to_string(),
                connect_timeout: std::time::Duration::from_secs(10),
                request_timeout: std::time::Duration::from_secs(30),
                security: self.security.clone().map(Box::new),
            },
        )
        .await
        .map_err(|error| SubstrateError::Unavailable(format!("connect to {host_port}: {error}")))
    }
}

#[async_trait::async_trait]
impl CommittedWalReader for KafkaCommittedWalReader {
    async fn committed_from(&self, start_offset: i64) -> Result<Vec<ReplayItem>, SubstrateError> {
        let conn = self.open_connection().await?;
        let mut items = Vec::new();
        let mut next_offset = start_offset;
        let mut empty_fetches = 0_usize;
        loop {
            let fetched = self.fetch_partition(&conn, next_offset).await?;

            if fetched.records.is_empty() {
                if fetched.next_offset > next_offset {
                    next_offset = fetched.next_offset;
                    empty_fetches = 0;
                    continue;
                }
                empty_fetches += 1;
                if empty_fetches > EMPTY_FETCH_RETRIES {
                    return Err(SubstrateError::Unavailable(
                        "replay could not read recovery barrier before retry limit".into(),
                    ));
                }
                continue;
            }
            empty_fetches = 0;

            for record in fetched.records {
                if record.offset >= next_offset {
                    next_offset = record.offset + 1;
                }
                let offset = record.offset;
                items.push(record);
                if offset >= self.barrier_offset {
                    return Ok(items);
                }
            }
        }
    }

    async fn log_start_offset(&self) -> Result<Option<i64>, SubstrateError> {
        let conn = self.open_connection().await?;
        let fetched = self.fetch_partition_log_start(&conn).await?;
        Ok((fetched.log_start_offset >= 0).then_some(fetched.log_start_offset))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FetchedWalPartition {
    log_start_offset: i64,
    next_offset: i64,
    records: Vec<ReplayItem>,
}

impl KafkaCommittedWalReader {
    async fn fetch_partition(
        &self,
        conn: &Connection,
        fetch_offset: i64,
    ) -> Result<FetchedWalPartition, SubstrateError> {
        let response: FetchResponse = conn
            .send(build_fetch_request(
                &self.topic,
                self.topic_uuid,
                fetch_offset,
            ))
            .await
            .map_err(|error| {
                SubstrateError::Unavailable(format!(
                    "fetch {} partition {PARTITION} offset {fetch_offset}: {error}",
                    self.topic
                ))
            })?;
        decode_fetch_response(&response, FetchDecodeMode::ReplayRecords)
    }

    async fn fetch_partition_log_start(
        &self,
        conn: &Connection,
    ) -> Result<FetchedWalPartition, SubstrateError> {
        let response: FetchResponse = conn
            .send(build_fetch_request(&self.topic, self.topic_uuid, 0))
            .await
            .map_err(|error| {
                SubstrateError::Unavailable(format!(
                    "fetch {} partition {PARTITION} log start: {error}",
                    self.topic
                ))
            })?;
        decode_fetch_response(&response, FetchDecodeMode::LogStart)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchDecodeMode {
    ReplayRecords,
    LogStart,
}

fn build_fetch_request(topic: &str, topic_id: WireUuid, fetch_offset: i64) -> FetchRequest {
    FetchRequest {
        max_wait_ms: FETCH_MAX_WAIT_MS,
        min_bytes: 1,
        max_bytes: 50 * 1024 * 1024,
        isolation_level: READ_COMMITTED,
        topics: vec![FetchTopic {
            topic: topic.to_owned(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: PARTITION,
                fetch_offset,
                partition_max_bytes: FETCH_MAX_BYTES,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn decode_fetch_response(
    response: &FetchResponse,
    mode: FetchDecodeMode,
) -> Result<FetchedWalPartition, SubstrateError> {
    for topic in &response.responses {
        for partition in &topic.partitions {
            if partition.partition_index != PARTITION {
                continue;
            }
            if partition.error_code != 0 {
                if mode == FetchDecodeMode::LogStart && partition.error_code == OFFSET_OUT_OF_RANGE
                {
                    return Ok(FetchedWalPartition {
                        log_start_offset: partition.log_start_offset,
                        next_offset: partition.log_start_offset,
                        records: Vec::new(),
                    });
                }
                return Err(SubstrateError::Unavailable(format!(
                    "fetch partition {PARTITION} error code {}",
                    partition.error_code
                )));
            }
            let (records, next_offset) = decode_replay_items(partition);
            return Ok(FetchedWalPartition {
                log_start_offset: partition.log_start_offset,
                next_offset,
                records,
            });
        }
    }
    Err(SubstrateError::Unavailable(format!(
        "fetch response missing partition {PARTITION}"
    )))
}

fn decode_replay_items(
    partition: &crabka_protocol::owned::fetch_response::PartitionData,
) -> (Vec<ReplayItem>, i64) {
    let Some(payload) = &partition.records else {
        return (Vec::new(), partition.log_start_offset.max(0));
    };
    let Some(batches) = payload.as_v2() else {
        return (Vec::new(), partition.log_start_offset.max(0));
    };
    let mut aborted: std::collections::VecDeque<(i64, i64)> = partition
        .aborted_transactions
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|transaction| (transaction.first_offset, transaction.producer_id))
        .collect::<Vec<_>>()
        .into();
    aborted.make_contiguous().sort_unstable();
    let mut aborted_producers = std::collections::HashSet::new();
    let mut records = Vec::new();
    let mut next_offset = partition.log_start_offset.max(0);
    for batch in batches {
        next_offset = next_offset.max(
            batch
                .base_offset
                .saturating_add(i64::from(batch.last_offset_delta))
                .saturating_add(1),
        );
        while let Some(&(first_offset, producer_id)) = aborted.front() {
            if first_offset > batch.base_offset {
                break;
            }
            aborted_producers.insert(producer_id);
            aborted.pop_front();
        }
        if batch.attributes.is_control_batch() {
            aborted_producers.remove(&batch.producer_id);
            continue;
        }
        if batch.attributes.is_transactional() && aborted_producers.contains(&batch.producer_id) {
            continue;
        }
        records.extend(batch.records.iter().filter_map(|record| {
            record.value.as_ref().map(|value| ReplayItem {
                offset: batch.base_offset + i64::from(record.offset_delta),
                bytes: value.to_vec(),
            })
        }));
    }
    records.sort_by_key(|record| record.offset);
    (records, next_offset)
}

/// Result of fencing predecessors with a committed recovery barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryBarrier {
    /// New writer generation that owns the tenant after fencing.
    pub generation: WriterGeneration,
    /// Offset where the barrier committed.
    pub offset: i64,
}

/// Fence-first recovery seam.
#[async_trait::async_trait]
pub trait RecoveryFencer: Send + Sync {
    /// Commit a barrier under a new writer generation before replay starts.
    async fn fence_with_barrier(&self) -> Result<RecoveryBarrier, SubstrateError>;
}

/// `READ_COMMITTED` replay seam. Implementations must not model `ListOffsets` as a
/// stable end; recovery terminates on the barrier offset returned by fencing.
#[async_trait::async_trait]
pub trait CommittedWalReader: Send + Sync {
    /// Return committed WAL records starting at `start_offset`.
    async fn committed_from(&self, start_offset: i64) -> Result<Vec<ReplayItem>, SubstrateError>;

    /// Return committed WAL records from the beginning of the topic.
    async fn committed_from_start(&self) -> Result<Vec<ReplayItem>, SubstrateError> {
        self.committed_from(0).await
    }

    /// Return the earliest retained WAL offset when known.
    async fn log_start_offset(&self) -> Result<Option<i64>, SubstrateError> {
        Ok(None)
    }
}

/// Read a finite committed WAL interval `(after_offset, barrier_offset]`.
///
/// This shared boundary parser keeps test and live readers honest: records
/// beyond the supplied barrier are never returned, and a missing barrier is a
/// hard error rather than a partial transfer tail.
///
/// # Errors
///
/// Returns an error for an invalid interval or when the reader does not return
/// the inclusive barrier record.
pub async fn bounded_committed_tail(
    reader: &dyn CommittedWalReader,
    after_offset: i64,
    barrier_offset: i64,
) -> Result<Vec<ReplayItem>, SubstrateError> {
    if after_offset >= barrier_offset {
        return Err(SubstrateError::Unavailable(format!(
            "bounded WAL tail requires after offset {after_offset} below barrier {barrier_offset}"
        )));
    }
    let start_offset = after_offset.checked_add(1).ok_or_else(|| {
        SubstrateError::Unavailable("bounded WAL tail start offset overflowed".into())
    })?;
    let records = reader.committed_from(start_offset).await?;
    let barrier_index = records
        .iter()
        .position(|record| record.offset == barrier_offset)
        .ok_or_else(|| {
            SubstrateError::Unavailable(format!(
                "committed WAL tail did not contain barrier offset {barrier_offset}"
            ))
        })?;
    Ok(records.into_iter().take(barrier_index + 1).collect())
}

/// Fence, replay committed records through the new barrier, and return the next sequence.
pub async fn recover_after_barrier(
    kv: &dyn Kv,
    fencer: &dyn RecoveryFencer,
    reader: &dyn CommittedWalReader,
) -> Result<(RecoveryBarrier, ReplayOutcome), SubstrateError> {
    let barrier = fencer.fence_with_barrier().await?;
    let items = reader.committed_from_start().await?;
    let outcome = replay_committed_frames(kv, items, barrier.offset)?;
    Ok((barrier, outcome))
}

#[derive(Debug, Clone)]
struct StoredWalRecord {
    offset: i64,
    bytes: Vec<u8>,
}

/// In-memory WAL test double with generation fencing and committed-only reads.
#[derive(Debug, Default)]
pub struct InMemoryWalLog {
    state: Mutex<InMemoryWalState>,
}

#[derive(Debug, Default)]
struct InMemoryWalState {
    current_generation: u64,
    log_start_offset: i64,
    next_offset: i64,
    committed: Vec<StoredWalRecord>,
    unacked: Vec<StoredWalRecord>,
}

impl InMemoryWalLog {
    /// Build a shared in-memory WAL log.
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Append records that are not visible to committed replay.
    pub async fn append_unacked(
        &self,
        generation: WriterGeneration,
        frames: &[WalFrame],
    ) -> Result<(), SubstrateError> {
        let mut state = self.state.lock().await;
        if generation.0 != state.current_generation {
            return Err(SubstrateError::Fenced);
        }
        for frame in frames {
            let offset = state.allocate_offset()?;
            state.unacked.push(StoredWalRecord {
                offset,
                bytes: frame.encode(),
            });
        }
        Ok(())
    }

    /// Return the current writer generation for deterministic prologue tests.
    pub async fn current_generation(&self) -> WriterGeneration {
        let state = self.state.lock().await;
        WriterGeneration(state.current_generation)
    }

    /// Return the next offset that will be allocated by this test WAL.
    pub async fn next_offset(&self) -> i64 {
        self.state.lock().await.next_offset
    }

    /// Return the earliest retained committed WAL offset.
    pub async fn earliest_retained_offset(&self) -> i64 {
        self.state.lock().await.log_start_offset
    }
}

#[async_trait::async_trait]
impl TransactionalWalWriter for InMemoryWalLog {
    async fn commit_group(
        &self,
        request: GroupCommitRequest,
    ) -> Result<GroupCommitAck, SubstrateError> {
        let mut state = self.state.lock().await;
        if request.generation.0 != state.current_generation {
            return Err(SubstrateError::Fenced);
        }
        let mut acks = Vec::with_capacity(request.frames.len());
        for frame in request.frames {
            let offset = state.allocate_offset()?;
            state.committed.push(StoredWalRecord {
                offset,
                bytes: frame.encode(),
            });
            acks.push(WalAppendAck {
                offset,
                journal_seq: frame.journal_seq,
            });
        }
        Ok(GroupCommitAck { frames: acks })
    }
}

#[async_trait::async_trait]
impl RecoveryFencer for InMemoryWalLog {
    async fn fence_with_barrier(&self) -> Result<RecoveryBarrier, SubstrateError> {
        let mut state = self.state.lock().await;
        state.current_generation = state
            .current_generation
            .checked_add(1)
            .ok_or_else(|| SubstrateError::Frame("writer generation exhausted".into()))?;
        state.unacked.clear();
        let generation = WriterGeneration(state.current_generation);
        let offset = state.allocate_offset()?;
        state.committed.push(StoredWalRecord {
            offset,
            bytes: WalFrame {
                journal_seq: BARRIER_SEQ,
                ops: Vec::new(),
            }
            .encode(),
        });
        Ok(RecoveryBarrier { generation, offset })
    }
}

#[async_trait::async_trait]
impl CommittedWalReader for InMemoryWalLog {
    async fn committed_from(&self, start_offset: i64) -> Result<Vec<ReplayItem>, SubstrateError> {
        let state = self.state.lock().await;
        let retained_start = start_offset.max(state.log_start_offset);
        Ok(state
            .committed
            .iter()
            .filter(|record| record.offset >= retained_start)
            .map(|record| ReplayItem {
                offset: record.offset,
                bytes: record.bytes.clone(),
            })
            .collect())
    }

    async fn log_start_offset(&self) -> Result<Option<i64>, SubstrateError> {
        Ok(Some(self.state.lock().await.log_start_offset))
    }
}

#[async_trait::async_trait]
impl CheckpointWalPruner for InMemoryWalLog {
    async fn delete_records(
        &self,
        ops: &[crabka_client_admin::DeleteRecordsOp],
    ) -> Result<(), SubstrateError> {
        let mut state = self.state.lock().await;
        for op in ops {
            if op.offset < 0 {
                return Err(SubstrateError::Checkpoint(format!(
                    "cannot prune WAL to negative offset {}",
                    op.offset
                )));
            }
            state.log_start_offset = state.log_start_offset.max(op.offset);
        }
        let log_start_offset = state.log_start_offset;
        state
            .committed
            .retain(|record| record.offset >= log_start_offset);
        Ok(())
    }
}

#[async_trait::async_trait]
impl FenceLease for InMemoryWalLog {
    async fn assert_current(&self, generation: WriterGeneration) -> Result<(), SubstrateError> {
        let state = self.state.lock().await;
        if generation.0 != state.current_generation {
            return Err(SubstrateError::Fenced);
        }
        Ok(())
    }
}

impl InMemoryWalState {
    fn allocate_offset(&mut self) -> Result<i64, SubstrateError> {
        let offset = self.next_offset;
        self.next_offset = self
            .next_offset
            .checked_add(1)
            .ok_or_else(|| SubstrateError::Frame("WAL offset exhausted".into()))?;
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use assert2::assert;
    use crabka_gres_ranges::{RangeId, TenantName};
    use crabka_pgkv::{Kv, KvError, MemKv, WriteOp, key};
    use crabka_pgmvcc::clog;
    use crabka_protocol::owned::fetch_response::{FetchableTopicResponse, PartitionData};

    use super::*;
    use crate::checkpoint::{
        DEFAULT_PART_MAX_BYTES, InMemoryCheckpointStore, Manifest, write_checkpoint,
    };

    #[test]
    fn live_recovery_config_selects_range_topic_and_transactional_id() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let coordinator =
            LiveRecoveryConfig::new("localhost:9092", tenant.clone(), RangeId::COORDINATOR, None);
        let data_range = LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::new(9), None);

        assert!(coordinator.wal_topic() == "__gres_wal.tenant-a.r0");
        assert!(data_range.wal_topic() == "__gres_wal.tenant-a.r9");
        assert!(coordinator.transactional_id() == "__gres.tenant-a.r0");
        assert!(data_range.transactional_id() == "__gres.tenant-a.r9");
        assert!(coordinator.checkpoint_namespace() == "tenant-a/r0");
        assert!(data_range.checkpoint_namespace() == "tenant-a/r9");
        assert!(coordinator.wal_topic() != data_range.wal_topic());
        assert!(coordinator.transactional_id() != data_range.transactional_id());
    }

    #[tokio::test]
    async fn recovery_does_not_resurrect_unacked_records() {
        let log = InMemoryWalLog::shared();
        log.commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![frame(0, b"acked", b"yes")],
        })
        .await
        .expect("commit");
        log.append_unacked(WriterGeneration(0), &[frame(1, b"lost", b"no")])
            .await
            .expect("unacked");
        let kv = MemKv::default();

        let (_barrier, outcome) = recover_after_barrier(&kv, log.as_ref(), log.as_ref())
            .await
            .expect("recover");

        assert!(outcome.next_journal_seq == 1);
        assert!(kv.get(b"acked").expect("get") == Some(b"yes".to_vec()));
        assert!(kv.get(b"lost").expect("get").is_none());
    }

    #[tokio::test]
    async fn barrier_replay_stops_at_own_barrier() {
        let log = InMemoryWalLog::shared();
        let barrier = log.fence_with_barrier().await.expect("first barrier");
        log.commit_group(GroupCommitRequest {
            generation: barrier.generation,
            frames: vec![frame(0, b"after", b"visible")],
        })
        .await
        .expect("commit");
        let kv = MemKv::default();

        let outcome = replay_committed_frames(
            &kv,
            log.committed_from_start().await.expect("read"),
            barrier.offset,
        )
        .expect("replay");

        assert!(outcome.next_journal_seq == 0);
        assert!(kv.get(b"after").expect("get").is_none());
    }

    #[tokio::test]
    async fn bounded_committed_tail_is_exclusive_at_start_and_inclusive_at_barrier() {
        let log = InMemoryWalLog::shared();
        log.commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![frame(0, b"before", b"included")],
        })
        .await
        .expect("commit before barrier");
        let barrier = log.fence_with_barrier().await.expect("barrier");
        log.commit_group(GroupCommitRequest {
            generation: barrier.generation,
            frames: vec![frame(0, b"after", b"excluded")],
        })
        .await
        .expect("commit after barrier");

        let tail = bounded_committed_tail(log.as_ref(), -1, barrier.offset)
            .await
            .expect("bounded tail");

        assert!(tail.iter().map(|record| record.offset).collect::<Vec<_>>() == vec![0, 1]);
        assert!(
            tail.last()
                .is_some_and(|record| record.offset == barrier.offset)
        );
    }

    #[tokio::test]
    async fn stale_writer_rejected_after_recovery_fence() {
        let log = InMemoryWalLog::shared();
        let barrier = log.fence_with_barrier().await.expect("fence");

        let stale = log
            .commit_group(GroupCommitRequest {
                generation: WriterGeneration(0),
                frames: vec![frame(0, b"stale", b"no")],
            })
            .await
            .expect_err("fenced");

        assert!(barrier.generation == WriterGeneration(1));
        assert!(matches!(stale, SubstrateError::Fenced));
    }

    #[tokio::test]
    async fn in_memory_wal_exposes_current_generation_and_next_offset() {
        let log = InMemoryWalLog::shared();

        assert!(log.current_generation().await == WriterGeneration(0));
        assert!(log.next_offset().await == 0);

        let barrier = log.fence_with_barrier().await.expect("fence");

        assert!(barrier.generation == WriterGeneration(1));
        assert!(log.current_generation().await == WriterGeneration(1));
        assert!(log.next_offset().await == barrier.offset + 1);
    }

    #[tokio::test]
    async fn recovery_preserves_merge_rules() {
        let log = InMemoryWalLog::shared();
        let seq_key = key::seq_key(3);
        log.commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![WalFrame {
                journal_seq: 0,
                ops: vec![
                    WriteOp::Put {
                        key: seq_key.clone(),
                        value: 9_u64.to_be_bytes().to_vec(),
                    },
                    WriteOp::Put {
                        key: seq_key.clone(),
                        value: 7_u64.to_be_bytes().to_vec(),
                    },
                    clog::put_op(12, clog::XidStatus::Committed),
                    clog::put_op(12, clog::XidStatus::Aborted),
                ],
            }],
        })
        .await
        .expect("commit");
        let kv = MemKv::default();

        recover_after_barrier(&kv, log.as_ref(), log.as_ref())
            .await
            .expect("recover");

        assert!(kv.get(&seq_key).expect("get") == Some(9_u64.to_be_bytes().to_vec()));
        assert!(
            kv.get(&key::clog_key(12))
                .expect("get")
                .is_some_and(|value| clog::is_terminal(&value))
        );
    }

    #[tokio::test]
    async fn checkpoint_recovery_restores_snapshot_and_reads_only_tail() {
        let checkpoints = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        base.put(b"base".to_vec(), b"checkpoint".to_vec())
            .expect("base put");
        write_checkpoint(
            checkpoints.as_ref(),
            "tenant-a",
            &base,
            checkpoint_snapshot(1, 2),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");
        let reader = TrackingReader::new(
            vec![
                replay_item(1, &frame(99, b"base", b"stale-tail")),
                replay_item(2, &frame(2, b"tail", b"yes")),
                replay_item(3, &frame(3, b"next-tail", b"also")),
                replay_item(
                    4,
                    &WalFrame {
                        journal_seq: BARRIER_SEQ,
                        ops: Vec::new(),
                    },
                ),
            ],
            None,
        );
        let restored = MemKv::default();

        let outcome = recover_store_after_barrier(
            &restored,
            Some(&restored),
            Some(&LiveRecoveryCheckpoints { store: checkpoints }),
            "tenant-a",
            &reader,
            &RecoveryBarrier {
                generation: WriterGeneration(0),
                offset: 4,
            },
        )
        .await
        .expect("recover");

        assert!(outcome.next_journal_seq == 4);
        assert!(reader.requested_start() == 2);
        assert!(restored.get(b"base").expect("get") == Some(b"checkpoint".to_vec()));
        assert!(restored.get(b"tail").expect("get") == Some(b"yes".to_vec()));
        assert!(restored.get(b"next-tail").expect("get") == Some(b"also".to_vec()));
    }

    #[tokio::test]
    async fn checkpoint_recovery_from_range_namespace_covers_pruned_wal() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let config = LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::COORDINATOR, None);
        let checkpoint_namespace = config.checkpoint_namespace();
        let checkpoints = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        base.put(b"base".to_vec(), b"checkpoint".to_vec())
            .expect("base put");
        write_checkpoint(
            checkpoints.as_ref(),
            &checkpoint_namespace,
            &base,
            checkpoint_snapshot(1, 2),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");
        let reader = TrackingReader::new(
            vec![
                replay_item(2, &frame(2, b"tail", b"retained")),
                replay_item(
                    3,
                    &WalFrame {
                        journal_seq: BARRIER_SEQ,
                        ops: Vec::new(),
                    },
                ),
            ],
            Some(2),
        );
        let restored = MemKv::default();

        let outcome = recover_store_after_barrier(
            &restored,
            Some(&restored),
            Some(&LiveRecoveryCheckpoints { store: checkpoints }),
            &checkpoint_namespace,
            &reader,
            &RecoveryBarrier {
                generation: WriterGeneration(0),
                offset: 3,
            },
        )
        .await
        .expect("recover checkpoint and retained tail");

        assert!(outcome.next_journal_seq == 3);
        assert!(reader.requested_start() == 2);
        assert!(restored.get(b"base").expect("get base") == Some(b"checkpoint".to_vec()));
        assert!(restored.get(b"tail").expect("get tail") == Some(b"retained".to_vec()));
    }

    #[tokio::test]
    async fn recovery_without_checkpoint_config_replays_from_start() {
        let reader = TrackingReader::new(
            vec![
                replay_item(0, &frame(0, b"base", b"wal")),
                replay_item(
                    1,
                    &WalFrame {
                        journal_seq: BARRIER_SEQ,
                        ops: Vec::new(),
                    },
                ),
            ],
            None,
        );
        let kv = MemKv::default();

        let outcome = recover_store_after_barrier(
            &kv,
            None,
            None,
            "tenant-a",
            &reader,
            &RecoveryBarrier {
                generation: WriterGeneration(0),
                offset: 1,
            },
        )
        .await
        .expect("recover");

        assert!(outcome.next_journal_seq == 1);
        assert!(reader.requested_start() == 0);
        assert!(kv.get(b"base").expect("get") == Some(b"wal".to_vec()));
    }

    #[tokio::test]
    async fn checkpoint_recovery_fails_on_torn_log_start() {
        let checkpoints = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        base.put(b"base".to_vec(), b"checkpoint".to_vec())
            .expect("base put");
        write_checkpoint(
            checkpoints.as_ref(),
            "tenant-a",
            &base,
            checkpoint_snapshot(1, 2),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");
        let reader = TrackingReader::new(Vec::new(), Some(3));
        let restored = MemKv::default();

        let error = recover_store_after_barrier(
            &restored,
            Some(&restored),
            Some(&LiveRecoveryCheckpoints { store: checkpoints }),
            "tenant-a",
            &reader,
            &RecoveryBarrier {
                generation: WriterGeneration(0),
                offset: 3,
            },
        )
        .await
        .expect_err("torn truncation");

        assert!(matches!(
            error,
            SubstrateError::TornTruncation {
                log_start: 3,
                newest_manifest: 1,
            }
        ));
    }

    #[tokio::test]
    async fn checkpoint_recovery_skips_corrupt_latest_and_restores_older_valid() {
        let checkpoints = InMemoryCheckpointStore::shared();
        let old = MemKv::default();
        old.put(b"base".to_vec(), b"old".to_vec()).expect("old put");
        write_checkpoint(
            checkpoints.as_ref(),
            "tenant-a",
            &old,
            checkpoint_snapshot(1, 2),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("old checkpoint");
        let new = MemKv::default();
        new.put(b"base".to_vec(), b"new".to_vec()).expect("new put");
        let manifest = write_checkpoint(
            checkpoints.as_ref(),
            "tenant-a",
            &new,
            checkpoint_snapshot(2, 3),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("new checkpoint");
        checkpoints
            .put(&manifest.parts[0].name, b"corrupt".to_vec())
            .await
            .expect("corrupt latest");
        let reader = TrackingReader::new(
            vec![replay_item(
                3,
                &WalFrame {
                    journal_seq: BARRIER_SEQ,
                    ops: Vec::new(),
                },
            )],
            None,
        );
        let restored = MemKv::default();

        let outcome = recover_store_after_barrier(
            &restored,
            Some(&restored),
            Some(&LiveRecoveryCheckpoints { store: checkpoints }),
            "tenant-a",
            &reader,
            &RecoveryBarrier {
                generation: WriterGeneration(0),
                offset: 3,
            },
        )
        .await
        .expect("recover");

        assert!(outcome.next_journal_seq == 2);
        assert!(reader.requested_start() == 2);
        assert!(restored.get(b"base").expect("get") == Some(b"old".to_vec()));
    }

    #[test]
    fn log_start_decode_accepts_offset_out_of_range_partition() {
        let response = FetchResponse {
            responses: vec![FetchableTopicResponse {
                partitions: vec![PartitionData {
                    partition_index: PARTITION,
                    error_code: OFFSET_OUT_OF_RANGE,
                    log_start_offset: 7,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let fetched = decode_fetch_response(&response, FetchDecodeMode::LogStart)
            .expect("log start from offset out of range response");

        assert!(fetched.log_start_offset == 7);
        assert!(fetched.records.is_empty());
    }

    #[test]
    fn replay_decode_rejects_offset_out_of_range_partition() {
        let response = FetchResponse {
            responses: vec![FetchableTopicResponse {
                partitions: vec![PartitionData {
                    partition_index: PARTITION,
                    error_code: OFFSET_OUT_OF_RANGE,
                    log_start_offset: 7,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let error = decode_fetch_response(&response, FetchDecodeMode::ReplayRecords)
            .expect_err("replay must fail on offset out of range");

        assert!(matches!(error, SubstrateError::Unavailable(_)));
    }

    #[tokio::test]
    async fn configured_checkpoint_recovery_replays_when_no_checkpoint_exists_yet() {
        let checkpoints = InMemoryCheckpointStore::shared();
        let reader = TrackingReader::new(
            vec![
                replay_item(0, &frame(0, b"boot", b"wal")),
                replay_item(
                    1,
                    &WalFrame {
                        journal_seq: BARRIER_SEQ,
                        ops: Vec::new(),
                    },
                ),
            ],
            Some(0),
        );
        let restored = MemKv::default();

        let outcome = recover_store_after_barrier(
            &restored,
            Some(&restored),
            Some(&LiveRecoveryCheckpoints { store: checkpoints }),
            "tenant-a",
            &reader,
            &RecoveryBarrier {
                generation: WriterGeneration(0),
                offset: 1,
            },
        )
        .await
        .expect("bootstrap replay");

        assert!(outcome.next_journal_seq == 1);
        assert!(reader.requested_start() == 0);
        assert!(restored.get(b"boot").expect("get") == Some(b"wal".to_vec()));
    }

    #[tokio::test]
    async fn configured_checkpoint_recovery_rejects_malformed_visible_manifest() {
        let checkpoints = InMemoryCheckpointStore::shared();
        checkpoints
            .put(
                &crate::checkpoint::manifest_key(&crate::checkpoint::ckpt_dir("tenant-a", 0, 1, 0)),
                b"not a manifest".to_vec(),
            )
            .await
            .expect("put manifest");
        let reader = TrackingReader::new(Vec::new(), Some(0));
        let restored = MemKv::default();

        let error = recover_store_after_barrier(
            &restored,
            Some(&restored),
            Some(&LiveRecoveryCheckpoints { store: checkpoints }),
            "tenant-a",
            &reader,
            &RecoveryBarrier {
                generation: WriterGeneration(0),
                offset: 1,
            },
        )
        .await
        .expect_err("invalid configured checkpoint must not fall back");

        assert!(matches!(error, SubstrateError::Checkpoint(_)));
        assert!(reader.requested_start() == -1);
    }

    #[tokio::test]
    async fn configured_checkpoint_recovery_rejects_invalid_visible_manifest_modes() {
        for mode in [
            InvalidVisibleCheckpointMode::ChecksumMismatch,
            InvalidVisibleCheckpointMode::TenantMismatch,
            InvalidVisibleCheckpointMode::GenerationMismatch,
            InvalidVisibleCheckpointMode::RestoreTargetNotEmpty,
        ] {
            let checkpoints = InMemoryCheckpointStore::shared();
            let restored = MemKv::default();
            write_invalid_visible_checkpoint(checkpoints.as_ref(), &restored, mode).await;
            let reader =
                TrackingReader::new(vec![replay_item(2, &frame(0, b"boot", b"wal"))], Some(0));

            let error = recover_store_after_barrier(
                &restored,
                Some(&restored),
                Some(&LiveRecoveryCheckpoints { store: checkpoints }),
                "tenant-a",
                &reader,
                &RecoveryBarrier {
                    generation: WriterGeneration(0),
                    offset: 2,
                },
            )
            .await
            .expect_err(mode.name());

            assert!(mode.matches_error(&error));
            assert!(reader.requested_start() == -1);
            assert!(restored.get(b"boot").expect("get boot").is_none());
        }
    }

    fn frame(seq: u64, key: &[u8], value: &[u8]) -> WalFrame {
        WalFrame {
            journal_seq: seq,
            ops: vec![WriteOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        }
    }

    fn replay_item(offset: i64, frame: &WalFrame) -> ReplayItem {
        ReplayItem {
            offset,
            bytes: frame.encode(),
        }
    }

    fn checkpoint_snapshot(
        covered_offset: i64,
        journal_seq: u64,
    ) -> crate::checkpoint::CheckpointSnapshot {
        crate::checkpoint::CheckpointSnapshot {
            covered_offset,
            journal_seq,
            producer_epoch: 0,
            wal_generation: 0,
            garbage_horizon_xid: 0,
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum InvalidVisibleCheckpointMode {
        ChecksumMismatch,
        TenantMismatch,
        GenerationMismatch,
        RestoreTargetNotEmpty,
    }

    impl InvalidVisibleCheckpointMode {
        fn name(self) -> &'static str {
            match self {
                Self::ChecksumMismatch => "checksum mismatch must not replay",
                Self::TenantMismatch => "tenant mismatch must not replay",
                Self::GenerationMismatch => "generation mismatch must not replay",
                Self::RestoreTargetNotEmpty => "restore failure must not replay",
            }
        }

        fn matches_error(self, error: &SubstrateError) -> bool {
            match self {
                Self::ChecksumMismatch => matches!(error, SubstrateError::ChecksumMismatch { .. }),
                Self::TenantMismatch | Self::GenerationMismatch => {
                    matches!(error, SubstrateError::Checkpoint(_))
                }
                Self::RestoreTargetNotEmpty => {
                    matches!(error, SubstrateError::Kv(KvError::RestoreTargetNotEmpty))
                }
            }
        }
    }

    async fn write_invalid_visible_checkpoint(
        checkpoints: &InMemoryCheckpointStore,
        restored: &MemKv,
        mode: InvalidVisibleCheckpointMode,
    ) {
        match mode {
            InvalidVisibleCheckpointMode::ChecksumMismatch => {
                let manifest = write_valid_checkpoint(checkpoints, "tenant-a", 0).await;
                checkpoints
                    .put(&manifest.parts[0].name, b"corrupt".to_vec())
                    .await
                    .expect("corrupt part");
            }
            InvalidVisibleCheckpointMode::TenantMismatch => {
                let manifest = manifest_for_visible_checkpoint("other-tenant", 0);
                checkpoints
                    .put(
                        &crate::checkpoint::manifest_key(&crate::checkpoint::ckpt_dir(
                            "tenant-a", 0, 1, 0,
                        )),
                        manifest.encode().expect("encode manifest"),
                    )
                    .await
                    .expect("put mismatched tenant manifest");
            }
            InvalidVisibleCheckpointMode::GenerationMismatch => {
                let manifest = manifest_for_visible_checkpoint("tenant-a", 1);
                checkpoints
                    .put(
                        &crate::checkpoint::manifest_key(&crate::checkpoint::ckpt_dir(
                            "tenant-a", 1, 1, 0,
                        )),
                        manifest.encode().expect("encode manifest"),
                    )
                    .await
                    .expect("put newer generation manifest");
            }
            InvalidVisibleCheckpointMode::RestoreTargetNotEmpty => {
                write_valid_checkpoint(checkpoints, "tenant-a", 0).await;
                restored
                    .put(b"already".to_vec(), b"present".to_vec())
                    .expect("seed restore target");
            }
        }
    }

    async fn write_valid_checkpoint(
        checkpoints: &InMemoryCheckpointStore,
        tenant: &str,
        wal_generation: u64,
    ) -> Manifest {
        let base = MemKv::default();
        base.put(b"base".to_vec(), b"checkpoint".to_vec())
            .expect("base put");
        write_checkpoint(
            checkpoints,
            tenant,
            &base,
            crate::checkpoint::CheckpointSnapshot {
                covered_offset: 1,
                journal_seq: 2,
                producer_epoch: 0,
                wal_generation,
                garbage_horizon_xid: 0,
            },
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint")
    }

    fn manifest_for_visible_checkpoint(tenant: &str, wal_generation: u64) -> Manifest {
        Manifest::new(tenant.to_string(), 1, 2, 0, wal_generation, Vec::new())
    }

    struct TrackingReader {
        records: Vec<ReplayItem>,
        log_start: Option<i64>,
        requested_start: AtomicI64,
    }

    impl TrackingReader {
        fn new(records: Vec<ReplayItem>, log_start: Option<i64>) -> Self {
            Self {
                records,
                log_start,
                requested_start: AtomicI64::new(-1),
            }
        }

        fn requested_start(&self) -> i64 {
            self.requested_start.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl CommittedWalReader for TrackingReader {
        async fn committed_from(
            &self,
            start_offset: i64,
        ) -> Result<Vec<ReplayItem>, SubstrateError> {
            self.requested_start.store(start_offset, Ordering::SeqCst);
            Ok(self
                .records
                .iter()
                .filter(|record| record.offset >= start_offset)
                .cloned()
                .collect())
        }

        async fn log_start_offset(&self) -> Result<Option<i64>, SubstrateError> {
            Ok(self.log_start)
        }
    }
}

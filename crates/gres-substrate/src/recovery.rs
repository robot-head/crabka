//! Barrier-based recovery primitives.

use std::{sync::Arc, time::Duration};

use crabka_client_admin::AdminClient;
use crabka_client_core::{
    Connection, ConnectionOptions, IsolatedFetch, fetch_partition_with_isolation_progress,
    security::ClientSecurity,
};
use crabka_client_producer::{
    Acks, Producer, ProducerFlushTimeout, ProducerRetryPolicy, ProducerThroughputPolicy,
};
use crabka_gres_ranges::{RangeId, TenantName};
use crabka_pgkv::{Kv, RestoreKv};
use crabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use refined_type::rule::{GreaterI32, GreaterU64, GreaterUsize};
use tokio::sync::Mutex;

use crate::{
    checkpoint::{CheckpointStore, CheckpointWalPruner, restore_latest_at_or_before},
    error::SubstrateError,
    follower::{CommittedEndConnection, CommittedEndDialer, LiveCommittedEndSampler},
    frame::{BARRIER_SEQ, WalFrame},
    replay::{ReplayItem, ReplayOutcome, replay_committed_frames, replay_committed_frames_from},
    topic::{
        WalAdminPolicy, ensure_wal_topic_name_with_policy, transactional_id_for_range,
        wal_topic_for_generation,
    },
    writer::{
        FenceLease, GroupCommitAck, GroupCommitRequest, TransactionalWalWriter, WalAppendAck,
        WriterGeneration,
    },
};

const READ_COMMITTED: i8 = 1;
const PARTITION: i32 = 0;
/// Default broker long-poll wait for committed-WAL recovery fetches.
pub const DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS: i32 = 100;
/// Default per-partition byte limit for committed-WAL recovery fetches.
pub const DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES: i32 = 1_048_576;
/// Default whole-response byte limit for committed-WAL recovery fetches.
pub const DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES: i32 =
    crabka_client_core::DEFAULT_FETCH_RESPONSE_MAX_BYTES;
/// Default consecutive empty-fetch retries after the initial empty fetch.
pub const DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES: usize = 100;
/// Default timeout for resolving a raw committed-WAL broker address.
pub const DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS: u64 = 10_000;
/// Default timeout for establishing a raw committed-WAL connection.
pub const DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS: u64 = 10_000;
/// Default timeout for requests on a raw committed-WAL connection.
pub const DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS: u64 = 30_000;
/// Committed-end sample fetches must return immediately when the cursor is
/// already at the stable end; a positive wait would park every barrier call
/// on the broker's long-poll timer.
const END_SAMPLE_MAX_WAIT_MS: i32 = 0;
const OFFSET_OUT_OF_RANGE: i16 = 1;

/// Validated limits for committed-WAL recovery reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReadPolicy {
    fetch_max_wait_ms: i32,
    fetch_partition_max_bytes: i32,
    fetch_response_max_bytes: i32,
    empty_fetch_retries: usize,
    dns_timeout: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl RecoveryReadPolicy {
    /// Validate recovery read limits.
    ///
    /// # Errors
    ///
    /// Returns an error when any value is not positive.
    pub fn new(
        fetch_max_wait_ms: i32,
        fetch_partition_max_bytes: i32,
        fetch_response_max_bytes: i32,
        empty_fetch_retries: usize,
    ) -> Result<Self, String> {
        Ok(Self {
            fetch_max_wait_ms: GreaterI32::<0>::new(fetch_max_wait_ms)
                .map_err(|error| error.to_string())?
                .into_value(),
            fetch_partition_max_bytes: GreaterI32::<0>::new(fetch_partition_max_bytes)
                .map_err(|error| error.to_string())?
                .into_value(),
            fetch_response_max_bytes: GreaterI32::<0>::new(fetch_response_max_bytes)
                .map_err(|error| error.to_string())?
                .into_value(),
            empty_fetch_retries: GreaterUsize::<0>::new(empty_fetch_retries)
                .map_err(|error| error.to_string())?
                .into_value(),
            dns_timeout: validated_timeout(DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS)?,
            connect_timeout: validated_timeout(DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS)?,
            request_timeout: validated_timeout(DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS)?,
        })
    }

    /// Replace the raw WAL DNS lookup timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the timeout is zero.
    pub fn with_dns_timeout(mut self, dns_timeout_ms: u64) -> Result<Self, String> {
        self.dns_timeout = validated_timeout(dns_timeout_ms)?;
        Ok(self)
    }

    /// Replace the raw WAL connection timeouts.
    ///
    /// # Errors
    ///
    /// Returns an error when either timeout is zero.
    pub fn with_timeouts(
        mut self,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> Result<Self, String> {
        self.connect_timeout = validated_timeout(connect_timeout_ms)?;
        self.request_timeout = validated_timeout(request_timeout_ms)?;
        Ok(self)
    }

    /// Return the broker long-poll wait in milliseconds.
    #[must_use]
    pub const fn fetch_max_wait_ms(self) -> i32 {
        self.fetch_max_wait_ms
    }

    /// Return the per-partition fetch byte limit.
    #[must_use]
    pub const fn fetch_partition_max_bytes(self) -> i32 {
        self.fetch_partition_max_bytes
    }

    /// Return the whole-response fetch byte limit.
    #[must_use]
    pub const fn fetch_response_max_bytes(self) -> i32 {
        self.fetch_response_max_bytes
    }

    /// Return consecutive empty-fetch retries after the initial empty fetch.
    #[must_use]
    pub const fn empty_fetch_retries(self) -> usize {
        self.empty_fetch_retries
    }

    /// Return the raw WAL DNS lookup timeout.
    #[must_use]
    pub const fn dns_timeout(self) -> Duration {
        self.dns_timeout
    }

    /// Return the raw WAL connection establishment timeout.
    #[must_use]
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Return the timeout for requests on a raw WAL connection.
    #[must_use]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }
}

fn validated_timeout(milliseconds: u64) -> Result<Duration, String> {
    GreaterU64::<0>::new(milliseconds)
        .map(|value| Duration::from_millis(value.into_value()))
        .map_err(|error| error.to_string())
}

impl Default for RecoveryReadPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS,
            DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES,
            DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES,
            DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES,
        )
        .expect("default recovery read policy is valid")
    }
}

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
    /// WAL generation selected by lifecycle recreation. Generation zero is the initial topic.
    pub wal_generation: u64,
    /// Authenticated local range-control endpoint advertised by this compute.
    pub advertised_endpoint: Option<String>,
    read_policy: RecoveryReadPolicy,
    wal_admin_policy: WalAdminPolicy,
    producer_flush_timeout: ProducerFlushTimeout,
    producer_retry_policy: ProducerRetryPolicy,
    producer_throughput_policy: ProducerThroughputPolicy,
    /// Noncanonical identity used while a recovered range is staged and must not fence serving.
    staging_identity: Option<String>,
    replay_seed: Option<(i64, u64)>,
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
            wal_generation: 0,
            advertised_endpoint: None,
            read_policy: RecoveryReadPolicy::default(),
            wal_admin_policy: WalAdminPolicy::default(),
            producer_flush_timeout: ProducerFlushTimeout::default(),
            producer_retry_policy: ProducerRetryPolicy::default(),
            producer_throughput_policy: ProducerThroughputPolicy::default(),
            staging_identity: None,
            replay_seed: None,
        }
    }

    /// Override committed-WAL recovery read limits.
    #[must_use]
    pub fn with_read_policy(mut self, read_policy: RecoveryReadPolicy) -> Self {
        self.read_policy = read_policy;
        self
    }

    /// Return committed-WAL recovery read limits.
    #[must_use]
    pub const fn read_policy(&self) -> RecoveryReadPolicy {
        self.read_policy
    }

    /// Override WAL topic and admin connection settings.
    #[must_use]
    pub fn with_wal_admin_policy(mut self, wal_admin_policy: WalAdminPolicy) -> Self {
        self.wal_admin_policy = wal_admin_policy;
        self
    }

    /// Return WAL topic and admin connection settings.
    #[must_use]
    pub const fn wal_admin_policy(&self) -> WalAdminPolicy {
        self.wal_admin_policy
    }

    /// Override the WAL producer flush deadline.
    #[must_use]
    pub fn with_producer_flush_timeout(
        mut self,
        producer_flush_timeout: ProducerFlushTimeout,
    ) -> Self {
        self.producer_flush_timeout = producer_flush_timeout;
        self
    }

    /// Return the WAL producer flush deadline.
    #[must_use]
    pub const fn producer_flush_timeout(&self) -> ProducerFlushTimeout {
        self.producer_flush_timeout
    }

    /// Override WAL producer retry and transaction timing.
    #[must_use]
    pub fn with_producer_retry_policy(
        mut self,
        producer_retry_policy: ProducerRetryPolicy,
    ) -> Self {
        self.producer_retry_policy = producer_retry_policy;
        self
    }

    /// Return WAL producer retry and transaction timing.
    #[must_use]
    pub const fn producer_retry_policy(&self) -> ProducerRetryPolicy {
        self.producer_retry_policy
    }

    /// Override WAL producer batching and compression settings.
    #[must_use]
    pub fn with_producer_throughput_policy(
        mut self,
        producer_throughput_policy: ProducerThroughputPolicy,
    ) -> Self {
        self.producer_throughput_policy = producer_throughput_policy;
        self
    }

    /// Return WAL producer batching and compression settings.
    #[must_use]
    pub const fn producer_throughput_policy(&self) -> ProducerThroughputPolicy {
        self.producer_throughput_policy
    }

    /// Use checkpoint restore before committed WAL tail replay.
    #[must_use]
    pub fn with_checkpoints(mut self, store: Arc<dyn CheckpointStore>) -> Self {
        self.checkpoints = Some(LiveRecoveryCheckpoints { store });
        self
    }

    /// Select a recreated WAL generation whose topic offsets restart at zero.
    #[must_use]
    pub fn with_wal_generation(mut self, wal_generation: u64) -> Self {
        self.wal_generation = wal_generation;
        self
    }

    /// Bind local transfer descriptors to this compute's advertised endpoint.
    #[must_use]
    pub fn with_advertised_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.advertised_endpoint = Some(endpoint.into());
        self
    }

    /// Bind an endpoint when this runtime exposes authenticated range control.
    #[must_use]
    pub fn with_optional_advertised_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.advertised_endpoint = endpoint;
        self
    }

    /// Recover with an operation-scoped producer identity that cannot fence the serving writer.
    #[must_use]
    pub fn with_staging_identity(mut self, operation_id: impl Into<String>) -> Self {
        self.staging_identity = Some(operation_id.into());
        self
    }

    /// Continue a generation WAL from an already materialized checkpoint/tail fold.
    ///
    /// `offset` is the first offset in the selected generation topic and `journal_seq`
    /// is the sequence expected in its first non-barrier frame.
    #[must_use]
    pub const fn with_replay_seed(mut self, offset: i64, journal_seq: u64) -> Self {
        self.replay_seed = Some((offset, journal_seq));
        self
    }

    /// WAL topic selected for replay and barrier writes.
    #[must_use]
    pub fn wal_topic(&self) -> String {
        wal_topic_for_generation(&self.tenant, self.range, self.wal_generation)
    }

    /// Transactional producer id selected for fencing this range.
    #[must_use]
    pub fn transactional_id(&self) -> String {
        let canonical = transactional_id_for_range(&self.tenant, self.range);
        self.staging_identity
            .as_ref()
            .map_or(canonical.clone(), |identity| {
                format!("{canonical}-staged-{identity}")
            })
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
    let mut admin = connect_wal_admin(config, &bootstrap_addrs).await?;
    let topic_uuid = resolve_topic_uuid(&mut admin, &topic).await?;
    let reader = KafkaCommittedWalReader::new(
        bootstrap_addrs,
        topic,
        topic_uuid,
        barrier_offset,
        config.security.clone(),
        config.read_policy,
    );
    bounded_committed_tail(&reader, after_offset, barrier_offset).await
}

/// Read all committed frames still retained in the selected generation topic.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn read_live_retained_committed(
    config: &LiveRecoveryConfig,
    barrier_offset: i64,
) -> Result<Vec<ReplayItem>, SubstrateError> {
    let bootstrap_addrs = parse_bootstrap_addrs(&config.bootstrap)?;
    let topic = config.wal_topic();
    let mut admin = connect_wal_admin(config, &bootstrap_addrs).await?;
    let topic_uuid = resolve_topic_uuid(&mut admin, &topic).await?;
    let reader = KafkaCommittedWalReader::new(
        bootstrap_addrs,
        topic,
        topic_uuid,
        barrier_offset,
        config.security.clone(),
        config.read_policy,
    );
    let start = reader.log_start_offset().await?.unwrap_or(0);
    bounded_committed_tail(&reader, start.saturating_sub(1), barrier_offset).await
}

/// Return the last offset visible under broker `READ_COMMITTED` isolation.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn live_committed_end(config: &LiveRecoveryConfig) -> Result<i64, SubstrateError> {
    LiveCommittedEndSampler::new(config.clone())
        .committed_end()
        .await
}

/// Dials live broker attachments for [`LiveCommittedEndSampler`].
pub(crate) struct LiveEndDialer {
    config: LiveRecoveryConfig,
}

impl LiveEndDialer {
    pub(crate) const fn new(config: LiveRecoveryConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl CommittedEndDialer for LiveEndDialer {
    async fn dial(&self) -> Result<Box<dyn CommittedEndConnection>, SubstrateError> {
        let bootstrap_addrs = parse_bootstrap_addrs(&self.config.bootstrap)?;
        let topic = self.config.wal_topic();
        let mut admin = connect_wal_admin(&self.config, &bootstrap_addrs).await?;
        let topic_uuid = resolve_topic_uuid(&mut admin, &topic).await?;
        let connection = open_wal_connection(
            &bootstrap_addrs,
            self.config.security.clone(),
            "crabka-gres-substrate-end-sample",
            self.config.read_policy,
        )
        .await?;
        Ok(Box::new(LiveEndConnection {
            connection,
            topic,
            topic_uuid,
            read_policy: self.config.read_policy,
        }))
    }
}

/// One dialed broker connection with its resolved topic identity.
struct LiveEndConnection {
    connection: Connection,
    topic: String,
    topic_uuid: WireUuid,
    read_policy: RecoveryReadPolicy,
}

#[async_trait::async_trait]
impl CommittedEndConnection for LiveEndConnection {
    async fn fetch_page(&self, fetch_offset: i64) -> Result<FetchedWalPartition, SubstrateError> {
        let response: FetchResponse = self
            .connection
            .send(build_fetch_request(
                &self.topic,
                self.topic_uuid,
                fetch_offset,
                END_SAMPLE_MAX_WAIT_MS,
                self.read_policy,
            ))
            .await
            .map_err(|error| {
                SubstrateError::Unavailable(format!(
                    "committed-end fetch {} offset {fetch_offset}: {error}",
                    self.topic
                ))
            })?;
        decode_fetch_response(&response, true)
    }
}

/// Ensure the selected range-generation WAL topic exists without constructing
/// or initializing a transactional producer.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn ensure_live_wal_topic(config: &LiveRecoveryConfig) -> Result<String, SubstrateError> {
    let bootstrap_addrs = parse_bootstrap_addrs(&config.bootstrap)?;
    let mut admin = connect_wal_admin(config, &bootstrap_addrs).await?;
    ensure_wal_topic_name_with_policy(&mut admin, &config.wal_topic(), config.wal_admin_policy)
        .await
}

/// Restore and catch up a read-only range-zero follower without fencing a writer.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn bootstrap_live_range0_follower(
    config: &LiveRecoveryConfig,
    store: Arc<dyn RestoreKv>,
    checkpoints: Option<&dyn CheckpointStore>,
) -> Result<crate::follower::ReadOnlyRange0Follower, SubstrateError> {
    let end = live_committed_end(config).await?;
    let bootstrap_addrs = parse_bootstrap_addrs(&config.bootstrap)?;
    let topic = config.wal_topic();
    let mut admin = connect_wal_admin(config, &bootstrap_addrs).await?;
    let topic_uuid = resolve_topic_uuid(&mut admin, &topic).await?;
    let reader = KafkaCommittedWalReader::new(
        bootstrap_addrs,
        topic,
        topic_uuid,
        end,
        config.security.clone(),
        config.read_policy,
    );
    crate::follower::ReadOnlyRange0Follower::bootstrap(config, store, &reader, checkpoints).await
}

/// Report whether the live WAL has been trimmed past the frames a follower at
/// `applied_offset` still needs.
///
/// This is the discriminator between the one failure a follower cannot retry
/// its way out of and every transient fetch failure. It asks the broker for
/// the retained log start rather than inspecting a failed fetch's error text;
/// an error here means the broker could not be asked, which is itself
/// transient and leaves the caller retrying.
///
/// # Errors
///
/// Returns an error when the retained log start cannot be read.
pub async fn live_wal_trimmed_past_applied(
    config: &LiveRecoveryConfig,
    applied_offset: i64,
) -> Result<bool, SubstrateError> {
    let reader = live_committed_reader(config, applied_offset).await?;
    let log_start = reader.log_start_offset().await?;
    Ok(crate::follower::wal_trimmed_past_applied(
        applied_offset,
        log_start,
    ))
}

/// Rebuild a live range-0 follower tail from the newest checkpoint.
///
/// See [`crate::follower::rebuild_range0_tail_from_checkpoint`]: `fresh_store`
/// must be an empty store distinct from the one `tail` is serving.
///
/// # Errors
///
/// Returns an error when the WAL topic cannot be resolved, when no checkpoint
/// covers the retained WAL, or when the rebuild does not advance the tail.
pub async fn rebuild_live_range0_tail_from_checkpoint(
    config: &LiveRecoveryConfig,
    tail: &crabka_gres_ranges::Range0Tail,
    fresh_store: Arc<dyn RestoreKv>,
    checkpoints: Option<&dyn CheckpointStore>,
) -> Result<i64, SubstrateError> {
    let end = live_committed_end(config).await?;
    let reader = live_committed_reader(config, end).await?;
    crate::follower::rebuild_range0_tail_from_checkpoint(
        config,
        tail,
        fresh_store,
        &reader,
        checkpoints,
    )
    .await
}

async fn recover_live_for_range_inner(
    config: LiveRecoveryConfig,
    store: &dyn Kv,
    restore_store: Option<&dyn RestoreKv>,
) -> Result<LiveRecovered, SubstrateError> {
    let bootstrap_addrs = parse_bootstrap_addrs(&config.bootstrap)?;
    let mut admin = connect_wal_admin(&config, &bootstrap_addrs).await?;
    let topic =
        ensure_wal_topic_name_with_policy(&mut admin, &config.wal_topic(), config.wal_admin_policy)
            .await?;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap_addrs[0].clone())
            .client_id(config.client_id())
            .acks(Acks::All)
            .compression(config.producer_throughput_policy.compression())
            .linger(config.producer_throughput_policy.linger())
            .batch_size(config.producer_throughput_policy.batch_bytes())
            .request_timeout(config.producer_retry_policy.request_timeout())
            .flush_timeout(config.producer_flush_timeout().duration())
            .retries(config.producer_retry_policy.retries())
            .retry_backoff(config.producer_retry_policy.retry_backoff())
            .routing_retry_budget(config.producer_retry_policy.routing_retry_budget())
            .init_retry_timeout(config.producer_retry_policy.init_retry_timeout())
            .init_max_backoff(config.producer_retry_policy.init_max_backoff())
            .max_in_flight_per_connection(config.producer_throughput_policy.max_in_flight())
            .transactional_id(config.transactional_id())
            .transaction_timeout(config.producer_retry_policy.transaction_timeout())
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
            generation: WriterGeneration(config.wal_generation),
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
        config.read_policy,
    );
    let recovery_barrier = RecoveryBarrier {
        generation: WriterGeneration(config.wal_generation),
        offset: barrier.offset,
    };
    let outcome = recover_store_after_barrier_with_seed(
        store,
        restore_store,
        config.checkpoints.as_ref(),
        &checkpoint_namespace,
        &reader,
        &recovery_barrier,
        config.replay_seed,
    )
    .await?;

    Ok(LiveRecovered {
        producer,
        generation: WriterGeneration(config.wal_generation),
        next_journal_seq: outcome.next_journal_seq,
        barrier_offset: barrier.offset,
    })
}

async fn recover_store_after_barrier_with_seed(
    kv: &dyn Kv,
    restore_kv: Option<&dyn RestoreKv>,
    checkpoints: Option<&LiveRecoveryCheckpoints>,
    tenant: &str,
    reader: &dyn CommittedWalReader,
    barrier: &RecoveryBarrier,
    replay_seed: Option<(i64, u64)>,
) -> Result<ReplayOutcome, SubstrateError> {
    if let Some((replay_start, expected)) = replay_seed {
        return replay_committed_frames_from(
            kv,
            reader.committed_from(replay_start).await?,
            barrier.offset,
            replay_start,
            expected,
        );
    }
    let Some(checkpoints) = checkpoints else {
        return replay_committed_frames(kv, reader.committed_from_start().await?, barrier.offset);
    };
    let restore_kv = restore_kv.ok_or_else(|| {
        SubstrateError::Unavailable("checkpoint restore requires a restore-capable KV store".into())
    })?;
    let log_start = reader.log_start_offset().await?;
    let restored = restore_latest_at_or_before(
        checkpoints.store.as_ref(),
        tenant,
        restore_kv,
        barrier.generation.0,
        log_start,
        barrier.offset.saturating_sub(1),
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

#[cfg(test)]
async fn recover_store_after_barrier(
    kv: &dyn Kv,
    restore_kv: Option<&dyn RestoreKv>,
    checkpoints: Option<&LiveRecoveryCheckpoints>,
    tenant: &str,
    reader: &dyn CommittedWalReader,
    barrier: &RecoveryBarrier,
) -> Result<ReplayOutcome, SubstrateError> {
    recover_store_after_barrier_with_seed(
        kv,
        restore_kv,
        checkpoints,
        tenant,
        reader,
        barrier,
        None,
    )
    .await
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

fn wal_admin_connection_options(config: &LiveRecoveryConfig) -> ConnectionOptions {
    ConnectionOptions {
        client_id: config.client_id(),
        connect_timeout: config.wal_admin_policy.connect_timeout(),
        request_timeout: config.wal_admin_policy.request_timeout(),
        security: config.security.clone().map(Box::new),
    }
}

async fn connect_wal_admin(
    config: &LiveRecoveryConfig,
    bootstrap_addrs: &[String],
) -> Result<AdminClient, SubstrateError> {
    AdminClient::connect_with_options(bootstrap_addrs, wal_admin_connection_options(config))
        .await
        .map_err(|error| SubstrateError::Unavailable(format!("admin connect: {error}")))
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
    read_policy: RecoveryReadPolicy,
}

/// Construct the private Kafka reader for one already-sampled committed end.
pub(crate) async fn live_committed_reader(
    config: &LiveRecoveryConfig,
    sample_offset: i64,
) -> Result<impl CommittedWalReader + use<>, SubstrateError> {
    let bootstrap_addrs = parse_bootstrap_addrs(&config.bootstrap)?;
    let topic = config.wal_topic();
    let mut admin = connect_wal_admin(config, &bootstrap_addrs).await?;
    let topic_uuid = resolve_topic_uuid(&mut admin, &topic).await?;
    Ok(KafkaCommittedWalReader::new(
        bootstrap_addrs,
        topic,
        topic_uuid,
        sample_offset,
        config.security.clone(),
        config.read_policy,
    ))
}

impl KafkaCommittedWalReader {
    fn new(
        bootstrap_addrs: Vec<String>,
        topic: String,
        topic_uuid: WireUuid,
        barrier_offset: i64,
        security: Option<ClientSecurity>,
        read_policy: RecoveryReadPolicy,
    ) -> Self {
        Self {
            bootstrap_addrs,
            topic,
            topic_uuid,
            barrier_offset,
            security,
            read_policy,
        }
    }

    async fn open_connection(&self) -> Result<Connection, SubstrateError> {
        open_wal_connection(
            &self.bootstrap_addrs,
            self.security.clone(),
            "crabka-gres-substrate-replay",
            self.read_policy,
        )
        .await
    }
}

fn wal_connection_options(
    client_id: &str,
    security: Option<ClientSecurity>,
    read_policy: RecoveryReadPolicy,
) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_string(),
        connect_timeout: read_policy.connect_timeout(),
        request_timeout: read_policy.request_timeout(),
        security: security.map(Box::new),
    }
}

async fn resolve_wal_addr<I>(
    host_port: &str,
    timeout: Duration,
    lookup: impl std::future::Future<Output = std::io::Result<I>>,
) -> Result<std::net::SocketAddr, SubstrateError>
where
    I: Iterator<Item = std::net::SocketAddr>,
{
    let mut addrs = tokio::time::timeout(timeout, lookup)
        .await
        .map_err(|_| {
            SubstrateError::Unavailable(format!(
                "DNS lookup {host_port} timed out after {} ms",
                timeout.as_millis()
            ))
        })?
        .map_err(|error| SubstrateError::Unavailable(format!("DNS lookup {host_port}: {error}")))?;
    addrs
        .next()
        .ok_or_else(|| SubstrateError::Unavailable(format!("no addresses for {host_port}")))
}

/// Open one raw broker connection to the first bootstrap address.
async fn open_wal_connection(
    bootstrap_addrs: &[String],
    security: Option<ClientSecurity>,
    client_id: &str,
    read_policy: RecoveryReadPolicy,
) -> Result<Connection, SubstrateError> {
    let host_port = bootstrap_addrs.first().ok_or_else(|| {
        SubstrateError::Unavailable("substrate bootstrap address list is empty".into())
    })?;
    let addr = resolve_wal_addr(
        host_port,
        read_policy.dns_timeout(),
        tokio::net::lookup_host(host_port),
    )
    .await?;
    Connection::connect_with_options(
        addr,
        wal_connection_options(client_id, security, read_policy),
    )
    .await
    .map_err(|error| SubstrateError::Unavailable(format!("connect to {host_port}: {error}")))
}

#[async_trait::async_trait]
impl CommittedWalReader for KafkaCommittedWalReader {
    async fn committed_from(&self, start_offset: i64) -> Result<Vec<ReplayItem>, SubstrateError> {
        let conn = self.open_connection().await?;
        let mut items = Vec::new();
        let mut next_offset = start_offset;
        let mut empty_fetch_retries = None;
        loop {
            let fetched = self.fetch_partition(&conn, next_offset).await?;
            match empty_fetch_decision(
                empty_fetch_retries,
                next_offset,
                &fetched,
                self.read_policy.empty_fetch_retries(),
            ) {
                EmptyFetchDecision::Reset => empty_fetch_retries = None,
                EmptyFetchDecision::Continue { retries_used } => {
                    empty_fetch_retries = Some(retries_used);
                }
                EmptyFetchDecision::Exhausted => {
                    return Err(SubstrateError::Unavailable(format!(
                        "replay could not read recovery barrier {} for {} (topic id {:?}, next offset {}, log start {}, high watermark {}, last stable offset {}, decoded batches {}) before retry limit",
                        self.barrier_offset,
                        self.topic,
                        self.topic_uuid,
                        fetched.next_offset,
                        fetched.log_start_offset,
                        fetched.high_watermark,
                        fetched.last_stable_offset,
                        fetched.decoded_batches,
                    )));
                }
            }

            if fetched.records.is_empty() {
                if fetched.next_offset > next_offset {
                    next_offset = fetched.next_offset;
                    continue;
                }
                continue;
            }

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EmptyFetchDecision {
    Reset,
    Continue { retries_used: usize },
    Exhausted,
}

const fn empty_fetch_decision(
    retries_used: Option<usize>,
    fetch_offset: i64,
    fetched: &FetchedWalPartition,
    retry_limit: usize,
) -> EmptyFetchDecision {
    if !fetched.records.is_empty() || fetched.next_offset > fetch_offset {
        return EmptyFetchDecision::Reset;
    }
    match retries_used {
        None => EmptyFetchDecision::Continue { retries_used: 1 },
        Some(retries_used) if retries_used < retry_limit => EmptyFetchDecision::Continue {
            retries_used: retries_used + 1,
        },
        Some(_) => EmptyFetchDecision::Exhausted,
    }
}

/// One decoded committed-isolation fetch page of a WAL partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchedWalPartition {
    pub(crate) log_start_offset: i64,
    pub(crate) high_watermark: i64,
    pub(crate) last_stable_offset: i64,
    pub(crate) decoded_batches: usize,
    pub(crate) next_offset: i64,
    pub(crate) records: Vec<ReplayItem>,
}

impl KafkaCommittedWalReader {
    async fn fetch_partition(
        &self,
        conn: &Connection,
        fetch_offset: i64,
    ) -> Result<FetchedWalPartition, SubstrateError> {
        let result = fetch_partition_with_isolation_progress(
            conn,
            recovery_fetch(&self.topic, self.topic_uuid, fetch_offset, self.read_policy),
        )
        .await
        .map_err(|error| {
            SubstrateError::Unavailable(format!(
                "fetch {} partition {PARTITION} offset {fetch_offset}: {error}",
                self.topic
            ))
        })?;
        Ok(FetchedWalPartition {
            log_start_offset: 0,
            high_watermark: self.barrier_offset.saturating_add(1),
            last_stable_offset: self.barrier_offset.saturating_add(1),
            decoded_batches: usize::from(result.next_offset.is_some()),
            next_offset: result.next_offset.unwrap_or(fetch_offset),
            records: result
                .records
                .into_iter()
                .filter_map(|record| {
                    record.value.map(|bytes| ReplayItem {
                        offset: record.offset,
                        bytes: bytes.to_vec(),
                    })
                })
                .collect(),
        })
    }

    async fn fetch_partition_log_start(
        &self,
        conn: &Connection,
    ) -> Result<FetchedWalPartition, SubstrateError> {
        let response: FetchResponse = conn
            .send(build_fetch_request(
                &self.topic,
                self.topic_uuid,
                0,
                self.read_policy.fetch_max_wait_ms(),
                self.read_policy,
            ))
            .await
            .map_err(|error| {
                SubstrateError::Unavailable(format!(
                    "fetch {} partition {PARTITION} log start: {error}",
                    self.topic
                ))
            })?;
        decode_fetch_response(&response, true)
    }
}

fn build_fetch_request(
    topic: &str,
    topic_id: WireUuid,
    fetch_offset: i64,
    max_wait_ms: i32,
    read_policy: RecoveryReadPolicy,
) -> FetchRequest {
    FetchRequest {
        max_wait_ms,
        min_bytes: 1,
        max_bytes: read_policy.fetch_response_max_bytes(),
        isolation_level: READ_COMMITTED,
        topics: vec![FetchTopic {
            topic: topic.to_owned(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: PARTITION,
                fetch_offset,
                partition_max_bytes: read_policy.fetch_partition_max_bytes(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn recovery_fetch(
    topic: &str,
    topic_id: WireUuid,
    fetch_offset: i64,
    read_policy: RecoveryReadPolicy,
) -> IsolatedFetch<'_> {
    IsolatedFetch {
        topic,
        topic_id,
        partition: PARTITION,
        fetch_offset,
        max_wait_ms: read_policy.fetch_max_wait_ms(),
        max_bytes: read_policy.fetch_response_max_bytes(),
        partition_max_bytes: read_policy.fetch_partition_max_bytes(),
        isolation_level: READ_COMMITTED,
    }
}

fn decode_fetch_response(
    response: &FetchResponse,
    allow_offset_out_of_range: bool,
) -> Result<FetchedWalPartition, SubstrateError> {
    for topic in &response.responses {
        for partition in &topic.partitions {
            if partition.partition_index != PARTITION {
                continue;
            }
            if partition.error_code != 0 {
                if allow_offset_out_of_range && partition.error_code == OFFSET_OUT_OF_RANGE {
                    return Ok(FetchedWalPartition {
                        log_start_offset: partition.log_start_offset,
                        high_watermark: partition.high_watermark,
                        last_stable_offset: partition.last_stable_offset,
                        decoded_batches: partition
                            .records
                            .as_ref()
                            .and_then(|payload| payload.as_v2())
                            .map_or(0, <[_]>::len),
                        next_offset: partition.log_start_offset,
                        records: Vec::new(),
                    });
                }
                return Err(SubstrateError::Unavailable(format!(
                    "fetch partition {PARTITION} error code {}",
                    partition.error_code
                )));
            }
            if let Some(payload) = partition.records.as_ref()
                && payload.as_v2().is_none()
            {
                return Err(SubstrateError::Unavailable(format!(
                    "fetch partition {PARTITION} returned non-v2 records payload: {payload:?}"
                )));
            }
            let (records, next_offset) = decode_replay_items(partition);
            return Ok(FetchedWalPartition {
                log_start_offset: partition.log_start_offset,
                high_watermark: partition.high_watermark,
                last_stable_offset: partition.last_stable_offset,
                decoded_batches: partition
                    .records
                    .as_ref()
                    .and_then(|payload| payload.as_v2())
                    .map_or(0, <[_]>::len),
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
    fn recovery_read_policy_owns_defaults() {
        let policy = RecoveryReadPolicy::default();

        assert_eq!(
            policy.fetch_max_wait_ms(),
            DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS
        );
        assert_eq!(
            policy.fetch_partition_max_bytes(),
            DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES
        );
        assert_eq!(
            policy.fetch_response_max_bytes(),
            DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES
        );
        assert_eq!(
            policy.empty_fetch_retries(),
            DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES
        );
        assert_eq!(
            policy.connect_timeout(),
            std::time::Duration::from_millis(DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS)
        );
        assert_eq!(
            policy.request_timeout(),
            std::time::Duration::from_millis(DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS)
        );
        assert!(policy.dns_timeout() == Duration::from_millis(DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS));
    }

    #[test]
    fn recovery_read_policy_validates_and_replaces_dns_timeout() {
        assert!(RecoveryReadPolicy::default().with_dns_timeout(0).is_err());

        let policy = RecoveryReadPolicy::new(11, 22, 33, 44)
            .expect("valid policy")
            .with_timeouts(55, 66)
            .expect("valid connection timeouts")
            .with_dns_timeout(77)
            .expect("valid DNS timeout");

        assert!(policy.fetch_max_wait_ms() == 11);
        assert!(policy.connect_timeout() == Duration::from_millis(55));
        assert!(policy.request_timeout() == Duration::from_millis(66));
        assert!(policy.dns_timeout() == Duration::from_millis(77));
    }

    #[test]
    fn recovery_read_policy_rejects_zero_values() {
        assert!(RecoveryReadPolicy::new(0, 2, 3, 4).is_err());
        assert!(RecoveryReadPolicy::new(1, 0, 3, 4).is_err());
        assert!(RecoveryReadPolicy::new(1, 2, 0, 4).is_err());
        assert!(RecoveryReadPolicy::new(1, 2, 3, 0).is_err());
    }

    #[test]
    fn recovery_read_policy_preserves_valid_values() {
        let policy = RecoveryReadPolicy::new(11, 22, 33, 44).expect("valid policy");

        assert_eq!(policy.fetch_max_wait_ms(), 11);
        assert_eq!(policy.fetch_partition_max_bytes(), 22);
        assert_eq!(policy.fetch_response_max_bytes(), 33);
        assert_eq!(policy.empty_fetch_retries(), 44);
    }

    #[test]
    fn recovery_read_policy_rejects_zero_timeouts() {
        let policy = RecoveryReadPolicy::default();

        assert!(policy.with_timeouts(0, 2).is_err());
        assert!(policy.with_timeouts(1, 0).is_err());
    }

    #[test]
    fn recovery_read_policy_replaces_timeouts_without_changing_fetch_limits() {
        let policy = RecoveryReadPolicy::new(11, 22, 33, 44)
            .expect("valid policy")
            .with_timeouts(55, 66)
            .expect("valid timeouts");

        assert_eq!(policy.fetch_max_wait_ms(), 11);
        assert_eq!(policy.fetch_partition_max_bytes(), 22);
        assert_eq!(policy.fetch_response_max_bytes(), 33);
        assert_eq!(policy.empty_fetch_retries(), 44);
        assert_eq!(
            policy.connect_timeout(),
            std::time::Duration::from_millis(55)
        );
        assert_eq!(
            policy.request_timeout(),
            std::time::Duration::from_millis(66)
        );
    }

    #[tokio::test]
    async fn wal_dns_lookup_returns_first_address_and_reports_resolver_failures() {
        let first: std::net::SocketAddr = "127.0.0.1:9092".parse().expect("first address");
        let second: std::net::SocketAddr = "127.0.0.2:9092".parse().expect("second address");
        let resolved = resolve_wal_addr(
            "broker:9092",
            Duration::from_millis(10),
            std::future::ready(Ok(vec![first, second].into_iter())),
        )
        .await
        .expect("resolved address");
        assert!(resolved == first);

        let error = resolve_wal_addr(
            "broker:9092",
            Duration::from_millis(10),
            std::future::ready(Err::<std::vec::IntoIter<std::net::SocketAddr>, _>(
                std::io::Error::other("resolver failed"),
            )),
        )
        .await
        .expect_err("resolver error");
        assert!(
            error
                .to_string()
                .contains("DNS lookup broker:9092: resolver failed")
        );
    }

    #[tokio::test]
    async fn wal_dns_lookup_rejects_an_empty_result() {
        let error = resolve_wal_addr(
            "broker:9092",
            Duration::from_millis(10),
            std::future::ready(Ok(Vec::<std::net::SocketAddr>::new().into_iter())),
        )
        .await
        .expect_err("empty resolution");

        assert!(error.to_string().contains("no addresses for broker:9092"));
    }

    #[tokio::test(start_paused = true)]
    async fn wal_dns_lookup_stops_at_the_configured_timeout() {
        let error = resolve_wal_addr(
            "broker:9092",
            Duration::from_millis(37),
            std::future::pending::<std::io::Result<std::vec::IntoIter<std::net::SocketAddr>>>(),
        )
        .await
        .expect_err("DNS timeout");

        assert!(
            error
                .to_string()
                .contains("DNS lookup broker:9092 timed out after 37 ms")
        );
    }

    #[test]
    fn recovery_read_policy_builds_exact_wal_connection_options() {
        let security = ClientSecurity {
            protocol: crabka_security::ListenerProtocol::Plaintext,
            tls: None,
            sasl: None,
            sasl_host: Some("broker.internal".into()),
        };
        let policy = RecoveryReadPolicy::default()
            .with_timeouts(77, 88)
            .expect("valid timeouts");
        let options = wal_connection_options("replay-client", Some(security), policy);

        assert_eq!(options.client_id, "replay-client");
        assert_eq!(
            options.connect_timeout,
            std::time::Duration::from_millis(77)
        );
        assert_eq!(
            options.request_timeout,
            std::time::Duration::from_millis(88)
        );
        let security = options.security.expect("security");
        assert_eq!(
            security.protocol,
            crabka_security::ListenerProtocol::Plaintext
        );
        assert_eq!(security.sasl_host.as_deref(), Some("broker.internal"));
    }

    #[test]
    fn recovery_read_policy_defaults_and_replaces_in_live_config() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let config = LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::COORDINATOR, None);
        assert_eq!(config.read_policy(), RecoveryReadPolicy::default());

        let replacement = RecoveryReadPolicy::new(11, 22, 33, 44).expect("valid policy");
        assert_eq!(
            config.with_read_policy(replacement).read_policy(),
            replacement
        );
    }

    #[test]
    fn wal_admin_policy_defaults_and_replaces_in_live_config() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let config = LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::COORDINATOR, None);
        assert_eq!(config.wal_admin_policy(), WalAdminPolicy::default());

        let replacement = WalAdminPolicy::new(11, 22, 33, 44).expect("valid policy");
        assert_eq!(
            config.with_wal_admin_policy(replacement).wal_admin_policy(),
            replacement
        );
    }

    #[test]
    fn producer_retry_policy_defaults_and_replaces_in_live_config() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let config =
            LiveRecoveryConfig::new("localhost:9092", tenant.clone(), RangeId::new(7), None);
        assert_eq!(
            config.producer_retry_policy(),
            crabka_client_producer::ProducerRetryPolicy::default()
        );

        let replacement = crabka_client_producer::ProducerRetryPolicy::new(
            Duration::from_millis(31),
            32,
            Duration::from_millis(33),
            Duration::from_millis(34),
            Duration::from_millis(35),
            Duration::from_millis(36),
            Duration::from_millis(37),
        )
        .expect("valid policy");
        assert_eq!(
            LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::new(7), None)
                .with_producer_retry_policy(replacement)
                .producer_retry_policy(),
            replacement
        );
    }

    #[test]
    fn producer_flush_timeout_defaults_replaces_and_reaches_builder() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let config =
            LiveRecoveryConfig::new("localhost:9092", tenant.clone(), RangeId::new(7), None);
        assert_eq!(
            config.producer_flush_timeout(),
            crabka_client_producer::ProducerFlushTimeout::default()
        );
        assert_eq!(
            config.producer_flush_timeout().duration(),
            crabka_client_producer::DEFAULT_PRODUCER_FLUSH_TIMEOUT
        );
        assert_eq!(config.producer_flush_timeout().milliseconds(), 50_000);

        let replacement =
            crabka_client_producer::ProducerFlushTimeout::new(Duration::from_millis(31))
                .expect("valid timeout");
        assert_eq!(
            LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::new(7), None)
                .with_producer_flush_timeout(replacement)
                .producer_flush_timeout(),
            replacement
        );

        assert_eq!(
            include_str!("recovery.rs")
                .matches(concat!(
                    ".flush_timeout(config.",
                    "producer_flush_timeout().duration())"
                ))
                .count(),
            1
        );
    }

    #[test]
    fn producer_throughput_policy_defaults_and_replaces_in_live_config() {
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let config =
            LiveRecoveryConfig::new("localhost:9092", tenant.clone(), RangeId::new(7), None);
        assert_eq!(
            config.producer_throughput_policy(),
            crabka_client_producer::ProducerThroughputPolicy::default()
        );

        let replacement = crabka_client_producer::ProducerThroughputPolicy::new(
            crabka_client_producer::Compression::Zstd,
            Duration::from_millis(38),
            39,
            crabka_client_producer::DEFAULT_PRODUCER_MAX_IN_FLIGHT,
        )
        .expect("valid policy");
        assert_eq!(
            LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::new(7), None)
                .with_producer_throughput_policy(replacement)
                .producer_throughput_policy(),
            replacement
        );

        let source = include_str!("recovery.rs");
        for setting in [
            concat!(
                ".compression(config.",
                "producer_throughput_policy.compression())"
            ),
            concat!(".linger(config.", "producer_throughput_policy.linger())"),
            concat!(
                ".batch_size(config.",
                "producer_throughput_policy.batch_bytes())"
            ),
            concat!(
                ".max_in_flight_per_connection(config.",
                "producer_throughput_policy.max_in_flight())"
            ),
        ] {
            assert_eq!(source.matches(setting).count(), 1, "{setting}");
        }
    }

    #[test]
    fn wal_admin_policy_builds_exact_connection_options() {
        let security = ClientSecurity {
            protocol: crabka_security::ListenerProtocol::Plaintext,
            tls: None,
            sasl: None,
            sasl_host: Some("broker.internal".into()),
        };
        let tenant = TenantName::parse("tenant-a").expect("tenant");
        let policy = WalAdminPolicy::new(11, 22, 33, 44).expect("valid policy");
        let config =
            LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::new(7), Some(security))
                .with_wal_admin_policy(policy);

        let options = wal_admin_connection_options(&config);

        assert_eq!(options.client_id, "crabka-gres-tenant-a-r7");
        assert_eq!(
            options.connect_timeout,
            std::time::Duration::from_millis(33)
        );
        assert_eq!(
            options.request_timeout,
            std::time::Duration::from_millis(44)
        );
        assert_eq!(
            options.security.expect("security").sasl_host.as_deref(),
            Some("broker.internal")
        );
    }

    #[test]
    fn recovery_read_policy_wires_normal_fetch_settings() {
        let policy = RecoveryReadPolicy::new(11, 22, 33, 44).expect("valid policy");
        let fetch = recovery_fetch("topic", WireUuid([7; 16]), 42, policy);

        assert_eq!(fetch.max_wait_ms, 11);
        assert_eq!(fetch.partition_max_bytes, 22);
        assert_eq!(fetch.max_bytes, 33);
    }

    #[test]
    fn recovery_read_policy_keeps_end_sample_zero_wait() {
        let policy = RecoveryReadPolicy::new(11, 22, 33, 44).expect("valid policy");
        let request = build_fetch_request(
            "__gres_wal.t.r0",
            WireUuid([7; 16]),
            42,
            END_SAMPLE_MAX_WAIT_MS,
            policy,
        );

        assert_eq!(request.max_wait_ms, 0);
        assert_eq!(request.max_bytes, 33);
        assert_eq!(request.topics[0].partitions[0].partition_max_bytes, 22);
    }

    #[test]
    fn recovery_read_policy_retry_decision_handles_one_and_usize_max() {
        let stalled = FetchedWalPartition {
            log_start_offset: 0,
            high_watermark: 1,
            last_stable_offset: 1,
            decoded_batches: 0,
            next_offset: 0,
            records: Vec::new(),
        };

        assert_eq!(
            empty_fetch_decision(None, 0, &stalled, 1),
            EmptyFetchDecision::Continue { retries_used: 1 }
        );
        assert_eq!(
            empty_fetch_decision(Some(1), 0, &stalled, 1),
            EmptyFetchDecision::Exhausted
        );
        assert_eq!(
            empty_fetch_decision(Some(usize::MAX - 1), 0, &stalled, usize::MAX),
            EmptyFetchDecision::Continue {
                retries_used: usize::MAX
            }
        );
        assert_eq!(
            empty_fetch_decision(Some(usize::MAX), 0, &stalled, usize::MAX),
            EmptyFetchDecision::Exhausted
        );
    }

    #[test]
    fn recovery_read_policy_progress_resets_retry_count() {
        let progressed = FetchedWalPartition {
            log_start_offset: 0,
            high_watermark: 2,
            last_stable_offset: 2,
            decoded_batches: 1,
            next_offset: 1,
            records: Vec::new(),
        };
        let records = FetchedWalPartition {
            next_offset: 1,
            records: vec![ReplayItem {
                offset: 0,
                bytes: Vec::new(),
            }],
            ..progressed.clone()
        };

        assert_eq!(
            empty_fetch_decision(Some(44), 0, &progressed, usize::MAX),
            EmptyFetchDecision::Reset
        );
        assert_eq!(
            empty_fetch_decision(Some(44), 0, &records, usize::MAX),
            EmptyFetchDecision::Reset
        );
    }

    #[test]
    fn fetch_request_carries_the_requested_wait_and_committed_isolation() {
        let topic_id = WireUuid([7_u8; 16]);
        let policy = RecoveryReadPolicy::default();
        let request = build_fetch_request("__gres_wal.t.r0", topic_id, 42, 250, policy);

        assert!(request.max_wait_ms == 250);
        assert!(request.isolation_level == READ_COMMITTED);
        assert!(request.min_bytes == 1);
        let partition = &request.topics[0].partitions[0];
        assert!(partition.partition == PARTITION);
        assert!(partition.fetch_offset == 42);
        assert!(partition.partition_max_bytes == DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES);

        // The zero-wait sampler path must stay zero-wait: a positioned fetch at
        // the log end returns immediately instead of long-polling.
        let poll = build_fetch_request("__gres_wal.t.r0", topic_id, 42, 0, policy);
        assert!(poll.max_wait_ms == 0);
    }

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

    #[test]
    fn staged_recovery_uses_noncanonical_transactional_identity() {
        let tenant = TenantName::parse("deferred_activation").expect("tenant");
        let canonical = LiveRecoveryConfig::new("broker:9092", tenant, RangeId::new(4), None);
        let staged = canonical.clone().with_staging_identity("split-op-7");

        assert_ne!(staged.transactional_id(), canonical.transactional_id());
        assert!(staged.transactional_id().contains("split-op-7"));
        assert_eq!(staged.wal_topic(), canonical.wal_topic());
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
    async fn checkpoint_recovery_skips_snapshot_newer_than_recovery_barrier() {
        let checkpoints = InMemoryCheckpointStore::shared();
        let older = MemKv::default();
        older
            .put(b"base".to_vec(), b"older-checkpoint".to_vec())
            .expect("older base put");
        write_checkpoint(
            checkpoints.as_ref(),
            "tenant-a",
            &older,
            checkpoint_snapshot(1, 2),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("older checkpoint");
        let newer = MemKv::default();
        newer
            .put(b"base".to_vec(), b"unsafe-checkpoint".to_vec())
            .expect("newer base put");
        write_checkpoint(
            checkpoints.as_ref(),
            "tenant-a",
            &newer,
            checkpoint_snapshot(5, 7),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("newer checkpoint");
        let reader = TrackingReader::new(
            vec![
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
        .expect("recover from checkpoint before barrier");

        assert!(outcome.next_journal_seq == 4);
        assert!(reader.requested_start() == 2);
        assert!(restored.get(b"base").expect("get") == Some(b"older-checkpoint".to_vec()));
        assert!(restored.get(b"tail").expect("get") == Some(b"yes".to_vec()));
        assert!(restored.get(b"next-tail").expect("get") == Some(b"also".to_vec()));
    }

    #[tokio::test]
    async fn fresh_generation_recovery_fetches_actual_tail_from_zero() {
        let checkpoints = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        base.put(b"base".to_vec(), b"older-generation".to_vec())
            .expect("base put");
        write_checkpoint(
            checkpoints.as_ref(),
            "tenant-a",
            &base,
            checkpoint_snapshot(7, 9),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("older-generation checkpoint");
        let reader = TrackingReader::new(
            vec![
                replay_item(0, &frame(0, b"fresh", b"offset-zero")),
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
                generation: WriterGeneration(1),
                offset: 1,
            },
        )
        .await
        .expect("fresh generation recovery");

        assert!(reader.requested_start() == 0);
        assert!(outcome.next_journal_seq == 1);
        assert!(restored.get(b"base").expect("base") == Some(b"older-generation".to_vec()));
        assert!(restored.get(b"fresh").expect("fresh") == Some(b"offset-zero".to_vec()));
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

        let fetched = decode_fetch_response(&response, true)
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

        let error = decode_fetch_response(&response, false)
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

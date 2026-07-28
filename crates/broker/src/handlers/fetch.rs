//! `Fetch` (`api_key=1`) with long-poll support via per-partition
//! `Notify::notified()` futures.
//!
//! Records are returned as verbatim `RecordsPayload::Raw` bytes — the
//! on-disk `.log` bytes for whole v2 batches, read decode-free via
//! `Log::read_raw` and clamped at the visibility window: the high watermark
//! for `read_uncommitted` consumer fetches, `lso.min(hw)` for
//! `read_committed`, and the log-end offset (LEO) for follower fetches.
//! `read_committed` does NO server-side batch filtering — aborted/control
//! batches stay in the byte stream and the consumer drops them client-side
//! using the `aborted_transactions` list, matching Apache Kafka.

use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use crabka_log::{LeaderEpoch, Offset};
use crabka_metadata::AclOperation;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        fetch_request::FetchRequest,
        fetch_response::{
            AbortedTransaction, EpochEndOffset, FetchResponse, FetchableTopicResponse,
            LeaderIdAndEpoch, PartitionData,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::{RecordBatch, RecordsPayload},
};
use crabka_units::{
    Time,
    convert::{ByteSizeExt as _, TimeExt},
};
use num_traits::ToPrimitive as _;
use tokio::sync::Notify;

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    fetch_session::{CachedPartitionState, FetchSessionKey, INVALID_SESSION_ID, SessionDecision},
    partition::Partition,
};

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Resolved read for a single requested (topic, partition) tuple, kept
/// around so we can re-read after a long-poll wake.
pub(crate) struct PendingRead {
    pub(crate) topic_name: String,
    pub(crate) topic_id: WireUuid,
    pub(crate) partition_index: i32,
    pub(crate) fetch_offset: i64,
    pub(crate) max_bytes: i32,
    /// `true` when `isolation_level == 1` on a consumer fetch (not a
    /// follower fetch). Causes batch-level LSO filtering and populates
    /// `aborted_transactions` in the response.
    pub(crate) read_committed: bool,
    /// `true` when `replica_id >= 0` — i.e., the request is from a follower
    /// replicator rather than a consumer. Follower fetches see all records up
    /// to LEO and report LEO as HW/LSO; consumer fetches are clamped at HW.
    pub(crate) is_follower_fetch: bool,
    /// `None` for unknown topic/partition or out-of-range — final response is
    /// already filled out and won't be re-read on wake.
    pub(crate) partition: Option<Arc<Partition>>,
    /// Per-partition output, mutated in place by `do_read`.
    pub(crate) out: PartitionData,
    /// Accumulator for microseconds spent in this partition's `do_read`
    /// calls (first pass plus any long-poll re-reads). Measured as an
    /// `Instant` elapsed delta around each `do_read`. The heavy byte read
    /// runs in `spawn_blocking`, so this charges the read work without
    /// allocating a `tokio_metrics::TaskMonitor` per partition per fetch.
    /// Drained into the response-emit loop's `record_partition_cpu_micros`
    /// call.
    pub(crate) cpu_micros: u64,
}

impl PendingRead {
    fn planned(
        topic_name: &str,
        topic_id: WireUuid,
        partition: &EffectivePartition,
        mode: (bool, bool),
        resolved: Option<Arc<Partition>>,
        out: PartitionData,
    ) -> Self {
        Self {
            topic_name: topic_name.to_owned(),
            topic_id,
            partition_index: partition.partition,
            fetch_offset: partition.fetch_offset,
            max_bytes: partition.partition_max_bytes,
            read_committed: mode.0,
            is_follower_fetch: mode.1,
            partition: resolved,
            out,
            cpu_micros: 0,
        }
    }
}

/// Handle a `Fetch` request, returning the response **struct** (not yet
/// encoded) plus the negotiated `version`. The dispatch layer turns this into
/// either a zero-copy write-plan (v4+, the canonical codec) or a legacy
/// copy-encoded frame (v0–v3). Returning the struct — rather than `Bytes` —
/// lets the connection writer split out each partition's records region as a
/// separate write segment instead of materializing the whole body.
#[tracing::instrument(
    name = "handle_fetch",
    level = "info",
    skip_all,
    fields(api = "Fetch", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<(FetchResponse, i16), BrokerError> {
    // KIP-124 request_percentage meters server-side handler time; capture the
    // start so the request throttle can be combined with the consumer
    // byte-rate throttle below (KIP-219).
    let handler_start = std::time::Instant::now();
    let mut cur: &[u8] = req_bytes;
    let req: FetchRequest = if version < 4 {
        crabka_protocol::kafka_3_6_2::owned::fetch_request::FetchRequest::decode(&mut cur, version)?
            .into()
    } else {
        FetchRequest::decode(&mut cur, version)?
    };

    let preparation = match prepare_fetch(broker, &req, ctx) {
        Ok(preparation) => preparation,
        Err(code) => {
            let resp = FetchResponse {
                error_code: code,
                session_id: INVALID_SESSION_ID,
                responses: Vec::new(),
                ..Default::default()
            };
            return Ok((resp, version));
        }
    };
    let FetchPreparation {
        decision,
        effective_topics,
        image,
        denied_topics,
        effective_replica_id,
        is_follower_fetch,
        read_committed,
    } = preparation;

    let plan_context = PendingPlanContext {
        broker,
        image: &image,
        denied_topics: &denied_topics,
        rack_id: &req.rack_id,
        mode: (read_committed, is_follower_fetch),
        follower_id: effective_replica_id,
    };
    let pending = build_pending_reads(&plan_context, &effective_topics).await;

    let (mut responses, cpu_micros_by_idx) = execute_pending_reads(
        broker,
        pending,
        req.min_bytes,
        req.max_wait_ms,
        ctx.sendfile_capable,
    )
    .await?;

    downconvert_legacy_responses(broker, version, &mut responses);

    if is_follower_fetch {
        throttle_follower_responses(broker, &image, effective_replica_id, &mut responses);
    }

    let throttle_time_ms_val = if is_follower_fetch {
        0
    } else {
        apply_consumer_fetch_quota(broker, &image, ctx, handler_start, &responses).await
    };

    record_fetch_metrics(broker, &responses, &cpu_micros_by_idx, is_follower_fetch);

    let response_session_id = finalize_fetch_session(
        broker,
        &decision,
        &effective_topics,
        &mut responses,
        is_follower_fetch,
        &ctx.principal.name,
    );

    let resp = FetchResponse {
        throttle_time_ms: throttle_time_ms_val,
        error_code: 0,
        session_id: response_session_id,
        responses,
        ..Default::default()
    };
    Ok((resp, version))
}

/// Projection of `FetchRequest::topics` / cached session partitions —
/// the minimum the read loop needs. Built once at the top of the
/// handler from either source.
struct EffectiveTopic {
    topic: String,
    topic_id: WireUuid,
    partitions: Vec<EffectivePartition>,
}

struct EffectivePartition {
    partition: i32,
    current_leader_epoch: i32,
    /// KIP-320: the leader epoch of the last fetched record as reported by
    /// the fetcher. `-1` means "not set" (v0–v11 fetchers or session-cached
    /// partitions that never set the field).
    last_fetched_epoch: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

struct FetchPreparation {
    decision: SessionDecision,
    effective_topics: Vec<EffectiveTopic>,
    image: Arc<crabka_metadata::MetadataImage>,
    denied_topics: std::collections::HashSet<String>,
    effective_replica_id: i32,
    is_follower_fetch: bool,
    read_committed: bool,
}

fn prepare_fetch(
    broker: &Broker,
    request: &FetchRequest,
    context: &crate::handlers::RequestContext<'_>,
) -> Result<FetchPreparation, i16> {
    let effective_replica_id = if request.replica_id >= 0 {
        request.replica_id
    } else {
        request.replica_state.replica_id
    };
    let is_follower_fetch = effective_replica_id >= 0;
    let decision = broker.fetch_session_cache.classify(request);
    if let SessionDecision::Error { code } = decision {
        return Err(code);
    }
    let effective_topics = match &decision {
        SessionDecision::Incremental { partitions, .. } => {
            group_cached_into_effective_topics(partitions)
        }
        _ => request
            .topics
            .iter()
            .map(|topic| EffectiveTopic {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| EffectivePartition {
                        partition: partition.partition,
                        current_leader_epoch: partition.current_leader_epoch,
                        last_fetched_epoch: partition.last_fetched_epoch,
                        fetch_offset: partition.fetch_offset,
                        partition_max_bytes: partition.partition_max_bytes,
                    })
                    .collect(),
            })
            .collect(),
    };
    let image = broker.controller.current_image();
    let names: Vec<String> = effective_topics
        .iter()
        .map(|topic| {
            if !topic.topic.is_empty() {
                topic.topic.clone()
            } else if topic.topic_id != WireUuid::ZERO {
                image
                    .topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0))
                    .map(str::to_owned)
                    .unwrap_or_default()
            } else {
                String::new()
            }
        })
        .collect();
    let denied_topics = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        context.principal,
        context.peer,
        AclOperation::Read,
        names.iter().map(String::as_str),
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_owned())
    .collect();
    Ok(FetchPreparation {
        decision,
        effective_topics,
        image,
        denied_topics,
        effective_replica_id,
        is_follower_fetch,
        read_committed: !is_follower_fetch && request.isolation_level == 1,
    })
}

async fn update_follower_progress(partition: &Partition, follower_id: i32, fetch_offset: i64) {
    let leader_leo = partition.log_end_offset();
    let advanced = {
        let mut state = partition.replica_state.lock().await;
        let previous = state.hw;
        state.update_follower_leo(
            crabka_metadata::NodeId(u64::try_from(follower_id).unwrap_or(0)),
            Offset(fetch_offset),
            leader_leo,
            std::time::Instant::now(),
        ) > previous
    };
    if advanced {
        partition.hw_advance_notify.notify_waiters();
    }
}

fn preferred_read_replica(
    broker: &Broker,
    image: &crabka_metadata::MetadataImage,
    topic: &str,
    partition: i32,
    rack_id: &str,
) -> i32 {
    if rack_id.is_empty() {
        return -1;
    }
    let Some(record) = image.partition(topic, partition) else {
        return -1;
    };
    let isr: std::collections::HashSet<crabka_metadata::NodeId> =
        record.isr.iter().copied().collect();
    let replicas: Vec<crate::replica_selector::ReplicaView> = record
        .replicas
        .iter()
        .map(|&node_id| crate::replica_selector::ReplicaView {
            node_id: i32::try_from(node_id.0).unwrap_or(-1),
            rack: image.broker(node_id).and_then(|broker| broker.rack.clone()),
            in_isr: isr.contains(&node_id),
        })
        .collect();
    broker.config.replica_selector.select(
        Some(rack_id),
        i32::try_from(record.leader.0).unwrap_or(-1),
        &replicas,
    )
}

fn apply_epoch_checks(
    image: &crabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    request: &EffectivePartition,
    partition: &Partition,
    output: &mut PartitionData,
) -> bool {
    let current_epoch = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    if request.current_leader_epoch >= 0 && request.current_leader_epoch != current_epoch {
        output.error_code = if request.current_leader_epoch < current_epoch {
            codes::FENCED_LEADER_EPOCH
        } else {
            codes::UNKNOWN_LEADER_EPOCH
        };
        output.current_leader = LeaderIdAndEpoch {
            leader_id: image
                .partition(topic, partition_index)
                .map_or(-1, |record| i32::try_from(record.leader.0).unwrap_or(-1)),
            leader_epoch: current_epoch,
            ..Default::default()
        };
        return true;
    }
    if request.last_fetched_epoch < 0 {
        return false;
    }
    let (found_epoch, end_offset) = {
        let log = partition.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint().epoch_and_offset_for(
            LeaderEpoch(request.last_fetched_epoch),
            log.log_end_offset(),
        )
    };
    if found_epoch >= request.last_fetched_epoch && end_offset.0 >= request.fetch_offset {
        return false;
    }
    output.error_code = codes::NONE;
    output.diverging_epoch = EpochEndOffset {
        epoch: found_epoch.0,
        end_offset: end_offset.0,
        ..Default::default()
    };
    true
}

struct PendingPlanContext<'a> {
    broker: &'a Broker,
    image: &'a crabka_metadata::MetadataImage,
    denied_topics: &'a std::collections::HashSet<String>,
    rack_id: &'a str,
    mode: (bool, bool),
    follower_id: i32,
}

async fn plan_partition_read(
    context: &PendingPlanContext<'_>,
    topic_name: &str,
    topic_id: WireUuid,
    topic_error: Option<i16>,
    request: &EffectivePartition,
) -> PendingRead {
    let mut output = PartitionData {
        partition_index: request.partition,
        ..Default::default()
    };
    if context.denied_topics.contains(topic_name) {
        output.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if let Some(error_code) = topic_error {
        output.error_code = error_code;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    let partition = context
        .broker
        .partitions
        .get(topic_name, crabka_ids::PartitionIndex(request.partition));
    if let Some(partition) = partition.as_ref()
        && apply_epoch_checks(
            context.image,
            topic_name,
            request.partition,
            request,
            partition,
            &mut output,
        )
    {
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if let Some(partition) = partition.as_ref()
        && context
            .broker
            .log_dir_status
            .is_offline(&partition.log_dir.load())
    {
        output.error_code = codes::KAFKA_STORAGE_ERROR;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if context.mode.1
        && let Some(partition) = partition.as_ref()
    {
        update_follower_progress(partition, context.follower_id, request.fetch_offset).await;
    }
    if partition.is_none() || topic_name.is_empty() {
        output.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
        return PendingRead::planned(topic_name, topic_id, request, context.mode, None, output);
    }
    if !context.mode.1 {
        output.preferred_read_replica = preferred_read_replica(
            context.broker,
            context.image,
            topic_name,
            request.partition,
            context.rack_id,
        );
    }
    PendingRead::planned(
        topic_name,
        topic_id,
        request,
        context.mode,
        partition,
        output,
    )
}

async fn build_pending_reads(
    context: &PendingPlanContext<'_>,
    topics: &[EffectiveTopic],
) -> Vec<PendingRead> {
    let mut pending = Vec::new();
    for topic in topics {
        let (name, id, error) =
            match crate::topic_resolve::resolve(context.image, &topic.topic, topic.topic_id) {
                Ok(record) => (
                    record.name.clone(),
                    WireUuid(record.topic_id.into_bytes()),
                    None,
                ),
                Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => {
                    (topic.topic.clone(), topic.topic_id, None)
                }
                Err(error_code) => (topic.topic.clone(), topic.topic_id, Some(error_code)),
            };
        for partition in &topic.partitions {
            pending.push(plan_partition_read(context, &name, id, error, partition).await);
        }
    }
    pending
}

fn downconvert_legacy_responses(
    broker: &Broker,
    version: i16,
    responses: &mut [FetchableTopicResponse],
) {
    if version >= 4 {
        return;
    }
    for topic in responses {
        for partition in &mut topic.partitions {
            let Some(payload) = partition.records.take() else {
                continue;
            };
            match crate::handlers::fetch_downconvert::down_convert_payload_for_fetch(
                &payload, version,
            ) {
                Ok(Some(converted)) => {
                    if converted.payload_len() > 0 {
                        partition.records = Some(converted);
                    }
                    if !topic.topic.is_empty() {
                        broker.metrics.record_fetch_message_conversion(&topic.topic);
                    }
                }
                Ok(None) => {}
                Err(error_code) => partition.error_code = error_code,
            }
        }
    }
}

fn record_fetch_metrics(
    broker: &Broker,
    responses: &[FetchableTopicResponse],
    cpu_micros_by_index: &[Vec<u64>],
    is_follower_fetch: bool,
) {
    for (topic_index, topic) in responses.iter().enumerate() {
        if topic.topic.is_empty() {
            continue;
        }
        let mut topic_bytes = 0;
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            let bytes = partition
                .records
                .as_ref()
                .map_or(0, RecordsPayload::payload_len) as u64;
            broker
                .metrics
                .record_partition_fetch(&topic.topic, partition.partition_index, bytes);
            if partition.error_code != 0 {
                broker.metrics.record_failed_fetch(&topic.topic);
            }
            if is_follower_fetch {
                broker.metrics.record_replication_out(
                    &topic.topic,
                    partition.partition_index,
                    bytes,
                );
            }
            if let Some(micros) = cpu_micros_by_index
                .get(topic_index)
                .and_then(|partitions| partitions.get(partition_index))
            {
                broker.metrics.record_partition_cpu_micros(
                    &topic.topic,
                    partition.partition_index,
                    *micros,
                );
            }
            topic_bytes += bytes;
        }
        broker.metrics.record_fetch(&topic.topic, topic_bytes);
    }
}

fn throttle_follower_responses(
    broker: &Broker,
    image: &crabka_metadata::MetadataImage,
    follower_id: i32,
    responses: &mut [FetchableTopicResponse],
) {
    use crate::throttle::TopicThrottle;
    let follower_id = crabka_metadata::NodeId(u64::try_from(follower_id).unwrap_or(0));
    let mut byte_count = 0;
    let mut indexes = Vec::new();
    for (topic_index, topic) in responses.iter().enumerate() {
        let throttle = TopicThrottle::for_topic(image, &topic.topic);
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            if throttle
                .leader
                .contains(partition.partition_index, follower_id)
            {
                byte_count += partition
                    .records
                    .as_ref()
                    .map_or(0, RecordsPayload::payload_len) as u64;
                indexes.push((topic_index, partition_index));
            }
        }
    }
    if byte_count > 0 {
        let granted = broker.throttle_state.leader_out.try_consume(byte_count);
        if granted < byte_count {
            truncate_throttled_responses(responses, &indexes, granted);
        }
    }
}

async fn apply_consumer_fetch_quota(
    broker: &Broker,
    image: &crabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    handler_start: std::time::Instant,
    responses: &[FetchableTopicResponse],
) -> i32 {
    let data_delay = consume_consumer_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        sum_response_bytes(responses),
        broker.config.quota_throttle_max,
    );
    let elapsed_micros = u64::try_from(
        handler_start
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)),
    )
    .expect("elapsed microseconds clamped to u64");
    let request_delay = crate::quota::consume_request_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        elapsed_micros,
        broker.config.quota_throttle_max,
    );
    let delay = data_delay.max(request_delay);
    if delay <= <Time as TimeExt>::ZERO {
        return 0;
    }
    tokio::time::sleep(delay.to_std()).await;
    crate::quota::throttle_time_ms(delay)
}

fn finalize_fetch_session(
    broker: &Broker,
    decision: &SessionDecision,
    effective_topics: &[EffectiveTopic],
    responses: &mut Vec<FetchableTopicResponse>,
    is_follower_fetch: bool,
    principal_name: &str,
) -> i32 {
    let session_id = match decision {
        SessionDecision::Sessionless => INVALID_SESSION_ID,
        SessionDecision::Close { session_id } => {
            broker.fetch_session_cache.close(*session_id);
            INVALID_SESSION_ID
        }
        SessionDecision::NewSession => {
            let snapshot = snapshot_response_state(effective_topics, responses);
            broker.fetch_session_cache.try_allocate(
                is_follower_fetch,
                principal_name.to_owned(),
                snapshot,
            )
        }
        SessionDecision::Incremental {
            session_id,
            partitions,
            ..
        } => {
            let cached: std::collections::HashMap<FetchSessionKey, CachedPartitionState> =
                partitions.iter().cloned().collect();
            let sent = filter_incremental_response(responses, &cached);
            broker
                .fetch_session_cache
                .finalize_incremental(*session_id, &sent);
            *session_id
        }
        SessionDecision::Error { .. } => unreachable!("returned above"),
    };
    refresh_fetch_session_metrics(broker);
    session_id
}

fn refresh_fetch_session_metrics(broker: &Broker) {
    broker
        .metrics
        .incremental_fetch_sessions
        .set(i64::try_from(broker.fetch_session_cache.len()).unwrap_or(i64::MAX));
    broker.metrics.incremental_fetch_partitions_cached.set(
        i64::try_from(broker.fetch_session_cache.total_partitions_cached()).unwrap_or(i64::MAX),
    );
    let current = broker.fetch_session_cache.evictions_total();
    let previous = broker
        .metrics
        .incremental_fetch_session_evictions_total
        .get();
    if current > previous {
        broker
            .metrics
            .incremental_fetch_session_evictions_total
            .inc_by(current - previous);
    }
}

async fn execute_pending_reads(
    broker: &Broker,
    mut pending: Vec<PendingRead>,
    min_bytes: i32,
    max_wait_ms: i32,
    sendfile_capable: bool,
) -> Result<(Vec<FetchableTopicResponse>, Vec<Vec<u64>>), BrokerError> {
    let mut total_bytes = 0;
    for read in &mut pending {
        let Some(partition) = read.partition.clone() else {
            continue;
        };
        let started = std::time::Instant::now();
        total_bytes += do_read(
            &partition,
            ReadRequest {
                topic_id: Some(uuid::Uuid::from_bytes(read.topic_id.0)),
                hot_tail: Some(broker.hot_tail.clone()),
                fetch_offset: Offset(read.fetch_offset),
                max_bytes: read.max_bytes,
                read_committed: read.read_committed,
                is_follower_fetch: read.is_follower_fetch,
                sendfile_capable,
                sendfile_min_bytes: broker.config.sendfile_min.bytes_usize(),
            },
            &mut read.out,
        )
        .await?;
        read.cpu_micros = read
            .cpu_micros
            .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        if read.out.error_code == codes::OFFSET_OUT_OF_RANGE {
            if let Some(remote_bytes) = try_remote_read(broker, read, &partition).await {
                total_bytes += remote_bytes;
            } else if let Some(diskless_bytes) =
                crate::diskless::read::try_diskless_read(broker, read, &partition).await
            {
                total_bytes += diskless_bytes;
            }
        }
    }
    let wants_more = total_bytes < usize::try_from(min_bytes.max(0)).unwrap_or(0);
    if wants_more && max_wait_ms > 0 {
        long_poll_then_reread(broker, &mut pending, max_wait_ms, sendfile_capable).await?;
    }
    Ok(group_into_topic_responses(pending))
}

/// Re-group the flat `(key, state)` list returned by
/// `FetchSessionCache::classify` into per-topic chunks. Topic order is
/// the order in which keys first appear — `HashMap` iteration order is
/// not stable across runs but is stable within a single classify call.
fn group_cached_into_effective_topics(
    cached: &[(FetchSessionKey, CachedPartitionState)],
) -> Vec<EffectiveTopic> {
    use std::collections::HashMap;
    let mut order: Vec<String> = Vec::new();
    let mut by_topic: HashMap<String, EffectiveTopic> = HashMap::new();
    for (k, s) in cached {
        let entry = by_topic
            .entry(k.topic_name.clone())
            .or_insert_with(|| EffectiveTopic {
                topic: k.topic_name.clone(),
                topic_id: k.topic_id,
                partitions: Vec::new(),
            });
        entry.partitions.push(EffectivePartition {
            partition: k.partition,
            current_leader_epoch: s.current_leader_epoch,
            last_fetched_epoch: s.last_fetched_epoch,
            fetch_offset: s.fetch_offset,
            partition_max_bytes: s.max_bytes,
        });
        if !order.iter().any(|t| t == &k.topic_name) {
            order.push(k.topic_name.clone());
        }
    }
    order
        .into_iter()
        .map(|n| by_topic.remove(&n).expect("populated above"))
        .collect()
}

/// Walk `responses` and snapshot every `(topic, partition)` row into a
/// `CachedPartitionState` describing what was just emitted (the `last_*`
/// fields) merged with the client's desired state for that partition
/// from `effective` (`fetch_offset`, `max_bytes`, `leader_epoch`). Used to
/// seed a brand-new session.
fn snapshot_response_state(
    effective: &[EffectiveTopic],
    responses: &[FetchableTopicResponse],
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    use std::collections::HashMap;
    // Pre-index the desired state. Topic identity differs by wire
    // version: v ≤ 12 carries topic name and zero topic_id, v ≥ 13
    // carries topic_id and empty name. The server-side response always
    // has the resolved name *and* the id, but `effective` (built from
    // `req.topics`) may have only one or the other. Index by both so
    // lookup succeeds in either direction.
    let mut by_name: HashMap<(String, i32), &EffectivePartition> = HashMap::new();
    let mut by_id: HashMap<(WireUuid, i32), &EffectivePartition> = HashMap::new();
    for et in effective {
        for ep in &et.partitions {
            if !et.topic.is_empty() {
                by_name.insert((et.topic.clone(), ep.partition), ep);
            }
            if et.topic_id != WireUuid::ZERO {
                by_id.insert((et.topic_id, ep.partition), ep);
            }
        }
    }
    let mut out = Vec::new();
    for tr in responses {
        for p in &tr.partitions {
            let key = FetchSessionKey {
                topic_name: tr.topic.clone(),
                topic_id: tr.topic_id,
                partition: p.partition_index,
            };
            let mut state = CachedPartitionState {
                last_high_watermark: p.high_watermark,
                last_last_stable_offset: p.last_stable_offset,
                last_log_start_offset: p.log_start_offset,
                last_preferred_read_replica: p.preferred_read_replica,
                last_aborted_txns_hash: hash_aborted_transactions(p.aborted_transactions.as_ref()),
                last_error_code: p.error_code,
                ..Default::default()
            };
            let ep = by_id
                .get(&(tr.topic_id, p.partition_index))
                .or_else(|| by_name.get(&(tr.topic.clone(), p.partition_index)));
            if let Some(ep) = ep {
                state.fetch_offset = ep.fetch_offset;
                state.max_bytes = ep.partition_max_bytes;
                state.current_leader_epoch = ep.current_leader_epoch;
                state.last_fetched_epoch = ep.last_fetched_epoch;
            }
            out.push((key, state));
        }
    }
    out
}

/// KIP-227 incremental-response filter. Drops partitions whose
/// outgoing state matches the cached `last_*` snapshot (the broker
/// already told the client these values; re-sending wastes bytes).
/// Returns the `(key, sent_state)` list for the partitions that
/// survived — used by the caller to update the cache's `last_*` fields
/// to reflect what was just emitted.
fn filter_incremental_response(
    responses: &mut Vec<FetchableTopicResponse>,
    cached: &std::collections::HashMap<FetchSessionKey, CachedPartitionState>,
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    let mut sent: Vec<(FetchSessionKey, CachedPartitionState)> = Vec::new();
    for tr in responses.iter_mut() {
        tr.partitions.retain(|p| {
            let key = FetchSessionKey {
                topic_name: tr.topic.clone(),
                topic_id: tr.topic_id,
                partition: p.partition_index,
            };
            let aborted_hash = hash_aborted_transactions(p.aborted_transactions.as_ref());
            let records_present = p.records.as_ref().is_some_and(|b| b.payload_len() > 0);
            let changed = match cached.get(&key) {
                Some(prev) => {
                    records_present
                        || p.error_code != prev.last_error_code
                        || p.high_watermark != prev.last_high_watermark
                        || p.last_stable_offset != prev.last_last_stable_offset
                        || p.log_start_offset != prev.last_log_start_offset
                        || p.preferred_read_replica != prev.last_preferred_read_replica
                        || aborted_hash != prev.last_aborted_txns_hash
                        || p.diverging_epoch.end_offset >= 0
                }
                // Partition not in the cached set — newly added by this
                // request. Always send it once so the client sees its
                // initial state.
                None => true,
            };
            if changed {
                sent.push((
                    key,
                    CachedPartitionState {
                        last_high_watermark: p.high_watermark,
                        last_last_stable_offset: p.last_stable_offset,
                        last_log_start_offset: p.log_start_offset,
                        last_preferred_read_replica: p.preferred_read_replica,
                        last_aborted_txns_hash: aborted_hash,
                        last_error_code: p.error_code,
                        ..Default::default()
                    },
                ));
            }
            changed
        });
    }
    // Drop topics that ended up with no partitions.
    responses.retain(|tr| !tr.partitions.is_empty());
    sent
}

/// Stable hash of the aborted-transaction list for the "did anything
/// change?" comparison. Iteration order within a single response is
/// deterministic (the list is produced by `do_read` in offset order)
/// so a plain `DefaultHasher` over the sequence is enough.
fn hash_aborted_transactions(list: Option<&Vec<AbortedTransaction>>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match list {
        None => 0_u8.hash(&mut h),
        Some(v) => {
            1_u8.hash(&mut h);
            (v.len() as u64).hash(&mut h);
            for tx in v {
                tx.producer_id.hash(&mut h);
                tx.first_offset.hash(&mut h);
            }
        }
    }
    h.finish()
}

/// The pure read-path visibility decision: given a partition's watermarks and a
/// fetch's parameters, what offsets this fetch may expose and what HW/LSO it
/// reports. Extracted from [`do_read`] so it is the single source of truth for
/// the response fields (previously computed in two places — the
/// `OFFSET_OUT_OF_RANGE` path and the success path) and is exhaustively +
/// property-tested in isolation (see `fetch_visibility_model.rs`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct VisibilityWindow {
    /// `fetch_offset < log_start` — caller returns `OFFSET_OUT_OF_RANGE`.
    pub out_of_range: bool,
    /// `fetch_offset >= upper_bound` — nothing to read (no bytes).
    pub empty: bool,
    /// Exclusive upper offset the raw read may expose: `[fetch_offset, limit_offset)`.
    pub limit_offset: Offset,
    /// `read_committed` aborted-txn scan ceiling (`lso.min(hw)` for a
    /// `read_committed` consumer, else `lso`).
    pub effective_lso: Offset,
    /// Whether to populate `aborted_transactions` (a `read_committed` consumer).
    pub read_committed_aborts: bool,
    /// `out.high_watermark` to report.
    pub response_hw: Offset,
    /// `out.last_stable_offset` to report.
    pub response_lso: Offset,
}

/// Kafka invariants the caller upholds: `0 <= log_start <= hw <= log_end` and
/// `lso <= hw`; `read_committed` is only set for consumer fetches, so
/// `read_committed` implies `!is_follower`.
pub(crate) fn compute_visibility_window(
    is_follower: bool,
    read_committed: bool,
    log_start: Offset,
    hw: Offset,
    lso: Offset,
    log_end: Offset,
    fetch_offset: Offset,
) -> VisibilityWindow {
    let upper_bound = if is_follower { log_end } else { hw };
    let effective_lso = if read_committed && !is_follower {
        lso.min(hw)
    } else {
        lso
    };
    let response_hw = if is_follower { log_end } else { hw };
    let response_lso = if read_committed && !is_follower {
        lso.min(hw)
    } else if is_follower {
        log_end
    } else {
        hw
    };
    let limit_offset = if is_follower {
        log_end
    } else if read_committed {
        effective_lso
    } else {
        hw
    };
    let out_of_range = fetch_offset < log_start;
    let empty = !out_of_range && fetch_offset >= upper_bound;
    VisibilityWindow {
        out_of_range,
        empty,
        limit_offset,
        effective_lso,
        read_committed_aborts: read_committed && !is_follower,
        response_hw,
        response_lso,
    }
}

/// Hold the partition's log mutex briefly to read offsets + (optionally) the
/// verbatim on-disk batch bytes via `Log::read_raw`. Populates `out` in place
/// (with `RecordsPayload::Raw`) and returns the byte-size estimate of the
/// records placed in `out` (0 if none).
///
/// When `read_committed` is `true` (consumer fetch with `isolation_level=1`):
/// - raw bytes are clamped at `min(lso, hw)` (`base_offset < min(lso, hw)`)
/// - NO server-side batch filtering: aborted/control batches stay in the
///   byte stream; the consumer drops them client-side using the list below
/// - `out.last_stable_offset` is set to `min(lso, hw)`
/// - `out.aborted_transactions` is populated from the partition's `.txnindex`
///
/// When `is_follower_fetch` is `true`:
/// - raw bytes up to LEO are returned (no HW clamping)
/// - `out.high_watermark` and `out.last_stable_offset` are set to `log_end`
///
/// When `read_committed` is `false` and `is_follower_fetch` is `false`
/// (consumer fetch in `read_uncommitted`):
/// - raw bytes are clamped at HW (`base_offset < hw`)
/// - `out.high_watermark` and `out.last_stable_offset` are set to `hw`
/// - `out.aborted_transactions` is `None`
enum ReadPlan {
    OffsetOutOfRange,
    Empty,
    Read {
        limit_offset: Offset,
        effective_lso: Offset,
        read_committed_aborts: bool,
    },
}

struct ReadRequest {
    topic_id: Option<uuid::Uuid>,
    hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
    fetch_offset: Offset,
    max_bytes: i32,
    read_committed: bool,
    is_follower_fetch: bool,
    sendfile_capable: bool,
    sendfile_min_bytes: usize,
}

async fn do_read(
    part: &Partition,
    request: ReadRequest,
    out: &mut PartitionData,
) -> Result<usize, BrokerError> {
    let ReadRequest {
        topic_id,
        hot_tail,
        fetch_offset,
        max_bytes,
        read_committed,
        is_follower_fetch,
        sendfile_capable,
        sendfile_min_bytes,
    } = request;
    let hw = part.high_watermark().await;
    let (log_start, w, plan) = plan_read(
        part,
        fetch_offset,
        hw,
        read_committed,
        is_follower_fetch,
        out,
    );
    // Log mutex released here.

    if part.diskless
        && !read_committed
        && matches!(plan, ReadPlan::Read { .. })
        && let (Some(topic_id), Some(hot_tail)) = (topic_id, hot_tail.as_ref())
        && let Some(bytes) = hot_tail.get(
            topic_id,
            part.index,
            fetch_offset.0,
            usize::try_from(max_bytes.max(0)).unwrap_or(0),
        )
    {
        return Ok(finish_read(
            out,
            &w,
            log_start,
            read_committed,
            is_follower_fetch,
            Vec::new(),
            Some(RecordsPayload::Raw(bytes)),
        ));
    }

    let (records, aborted_txns): (Option<RecordsPayload>, Vec<AbortedTransaction>) = match plan {
        ReadPlan::OffsetOutOfRange => return Ok(0),
        ReadPlan::Empty => (None, Vec::new()),
        ReadPlan::Read {
            limit_offset,
            effective_lso,
            read_committed_aborts,
        } => {
            let read_max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
            // Run the blocking seek+read (and, for read_committed, the
            // aborted-txn index scan) off the reactor thread. The lock is
            // re-acquired inside the closure for the brief duration of the
            // syscalls.
            let log = part.log.clone();
            let join = tokio::task::spawn_blocking(move || {
                let log = log.lock().expect("log mutex poisoned");

                // Zero-copy (Increments D + E): on a plaintext connection
                // (SENDFILE alias: Linux + Apple + FreeBSD/DragonFly), describe
                // the records run with a cheap header-only walk (`read_raw_desc`)
                // instead of `pread`ing the payload. If the run is large enough
                // to amortize the sendfile syscall, return file-backed regions
                // for the `sendfile` drain; otherwise fall back to the byte-copy
                // `read_raw` path (small/fragmented fetches stay on the vectored
                // path). The descriptor is captured here under the log lock so
                // retention can't truncate the region out from under the later
                // async send (the `Arc<File>` pins the inode).
                #[cfg(any(
                    target_os = "linux",
                    target_os = "macos",
                    target_os = "ios",
                    target_os = "tvos",
                    target_os = "watchos",
                    target_os = "freebsd",
                    target_os = "dragonfly",
                ))]
                let records: RecordsPayload = {
                    let mut chosen: Option<RecordsPayload> = None;
                    // READ_COMMITTED responses also carry aborted-transaction
                    // metadata and are consumed by ordinary Kafka clients as one
                    // framed response. Keep those on the raw-byte encoder; the
                    // file-region writer can otherwise detach the records payload
                    // from the metadata frame and leave a fresh stable-topic reader
                    // with HW/LSO but no decoded batches.
                    if sendfile_capable && !read_committed_aborts {
                        let desc = log.read_raw_desc(fetch_offset, limit_offset, read_max)?;
                        if should_use_sendfile(
                            desc.total,
                            !desc.regions.is_empty(),
                            sendfile_min_bytes,
                        ) {
                            chosen = Some(RecordsPayload::FileRegions(desc.regions));
                        }
                    }
                    match chosen {
                        Some(p) => p,
                        None => RecordsPayload::Raw(
                            log.read_raw(fetch_offset, limit_offset, read_max)?.bytes,
                        ),
                    }
                };
                // Windows fallback: no safe `sendfile`/`TransmitFile`, so always
                // `read_raw` + copy (the Increment C vectored path drains it).
                #[cfg(not(any(
                    target_os = "linux",
                    target_os = "macos",
                    target_os = "ios",
                    target_os = "tvos",
                    target_os = "watchos",
                    target_os = "freebsd",
                    target_os = "dragonfly",
                )))]
                let records: RecordsPayload = {
                    let _ = sendfile_capable;
                    RecordsPayload::Raw(log.read_raw(fetch_offset, limit_offset, read_max)?.bytes)
                };

                // read_committed does NO server-side batch filtering: verbatim
                // bytes (including aborted/control batches) are returned and the
                // consumer drops them client-side via `aborted_transactions`,
                // matching Apache Kafka's behavior. Skip the Vec allocation
                // entirely when there are no aborted txns in range.
                let aborted = if read_committed_aborts {
                    let mut it = log
                        .aborted_in_range(fetch_offset, effective_lso)
                        .into_iter();
                    if let Some(first) = it.next() {
                        let mut v = vec![AbortedTransaction {
                            // Unwrap the log-layer `ProducerId` into the wire `i64` field.
                            producer_id: first.producer_id.get(),
                            // Unwrap the log-layer `Offset` into the wire `i64` field.
                            first_offset: first.start_offset.0,
                            ..Default::default()
                        }];
                        v.extend(it.map(|e| AbortedTransaction {
                            producer_id: e.producer_id.get(),
                            first_offset: e.start_offset.0,
                            ..Default::default()
                        }));
                        v
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                Ok::<_, BrokerError>((records, aborted))
            });
            await_blocking_read(join).await?
        }
    };

    Ok(finish_read(
        out,
        &w,
        log_start,
        read_committed,
        is_follower_fetch,
        aborted_txns,
        records,
    ))
}

async fn await_blocking_read(
    join: tokio::task::JoinHandle<Result<(RecordsPayload, Vec<AbortedTransaction>), BrokerError>>,
) -> Result<(Option<RecordsPayload>, Vec<AbortedTransaction>), BrokerError> {
    let (records, aborted) = join.await.map_err(|error| {
        BrokerError::Io(std::io::Error::other(format!(
            "fetch read task panicked: {error}"
        )))
    })??;
    let records = (records.payload_len() > 0).then_some(records);
    Ok((records, aborted))
}

fn finish_read(
    response: &mut PartitionData,
    window: &VisibilityWindow,
    log_start: Offset,
    read_committed: bool,
    follower_fetch: bool,
    aborted: Vec<AbortedTransaction>,
    records: Option<RecordsPayload>,
) -> usize {
    response.error_code = codes::NONE;
    response.high_watermark = window.response_hw.0;
    response.log_start_offset = log_start.0;
    response.last_stable_offset = window.response_lso.0;
    if read_committed && !follower_fetch {
        response.aborted_transactions = Some(aborted);
    }
    let bytes = records.as_ref().map_or(0, RecordsPayload::payload_len);
    response.records = records;
    bytes
}

fn plan_read(
    partition: &Partition,
    fetch_offset: Offset,
    high_watermark: Offset,
    read_committed: bool,
    follower_fetch: bool,
    response: &mut PartitionData,
) -> (Offset, VisibilityWindow, ReadPlan) {
    let log = partition.log.lock().expect("log mutex poisoned");
    let log_start = log.log_start_offset();
    let window = compute_visibility_window(
        follower_fetch,
        read_committed,
        log_start,
        high_watermark,
        log.lso(),
        log.log_end_offset(),
        fetch_offset,
    );
    let plan = if window.out_of_range {
        response.error_code = codes::OFFSET_OUT_OF_RANGE;
        response.log_start_offset = log_start.0;
        response.high_watermark = window.response_hw.0;
        response.last_stable_offset = window.response_lso.0;
        ReadPlan::OffsetOutOfRange
    } else if window.empty {
        ReadPlan::Empty
    } else {
        ReadPlan::Read {
            limit_offset: window.limit_offset,
            effective_lso: window.effective_lso,
            read_committed_aborts: window.read_committed_aborts,
        }
    };
    (log_start, window, plan)
}

/// KIP-405: try to serve `p`'s requested offset from the remote
/// tier when the local log returned `OFFSET_OUT_OF_RANGE` and the topic has
/// `remote.storage.enable=true`. On success, replaces the partition's error +
/// records and returns the encoded batch size; on miss / error / non-tiered,
/// leaves `p.out` untouched and returns `None`.
async fn try_remote_read(broker: &Broker, p: &mut PendingRead, part: &Partition) -> Option<usize> {
    let reader = broker.remote_reader.clone()?;
    let remote_storage_enable = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.config_snapshot().remote_storage_enable
    };
    if !remote_storage_enable {
        return None;
    }
    if p.topic_id == WireUuid::ZERO {
        // Without a topic_id we can't build `TopicIdPartition` keyed the
        // same way the RLMM stores entries (Kafka's equality is by id +
        // partition).
        return None;
    }
    let topic_id = uuid::Uuid::from_bytes(p.topic_id.0);
    let tp = crabka_remote_storage::TopicIdPartition::new(
        topic_id,
        p.topic_name.clone(),
        p.partition_index,
    );
    // Atomic stores the raw epoch; wrap into `LeaderEpoch` for the
    // remote-reader / RLMM seam that follows.
    let current_leader_epoch = LeaderEpoch(
        part.current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire),
    );
    // Resolve the leader epoch that *owned* the requested fetch offset from
    // the local leader-epoch checkpoint (Kafka's `epochForOffset`).  The
    // checkpoint is only appended-to / truncated-from-end (never pruned from
    // the start on local eviction), so tiered offsets that are no longer
    // stored locally still resolve to their copy-time epoch.  Fall back to
    // the current leader epoch when the checkpoint has no entries (empty /
    // fresh log) so behavior is at least as good as before.
    let leader_epoch = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint()
            .epoch_for_offset(Offset(p.fetch_offset))
            .unwrap_or(current_leader_epoch)
    };
    let max_bytes = usize::try_from(p.max_bytes.max(0)).unwrap_or(0);

    match reader
        .fetch_batch(&tp, leader_epoch, p.fetch_offset, max_bytes)
        .await
    {
        Ok(Some(batch)) => {
            let bytes_est = <RecordBatch as Encode>::encoded_len(&batch, 0);
            p.out.error_code = codes::NONE;
            // `log_start_offset` / HW / LSO stay at whatever `do_read`
            // wrote out (the local view); the remote tier doesn't change
            // those pointers.

            // KIP-405 read-committed: surface the aborted-transaction list
            // from the segment's `.txnindex` so the consumer drops aborted
            // records client-side, mirroring the local `aborted_in_range`
            // call in `do_read` — bounded here to the single batch this read
            // returns (inclusive last offset), since the local path bounds by
            // the returned window over the LSO. `Some(empty)` is the correct
            // read-committed signal (read-uncommitted leaves it `None`).
            if p.read_committed && !p.is_follower_fetch {
                let batch_last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
                let aborts = match reader
                    .aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)
                    .await
                {
                    Ok(aborts) => aborts,
                    Err(e) => {
                        // Degrade to "no aborts" but make it observable: an
                        // empty list in read-committed means the consumer may
                        // surface aborted records as committed.
                        tracing::warn!(
                            topic = %p.topic_name,
                            partition = p.partition_index,
                            offset = p.fetch_offset,
                            error = %e,
                            "remote-reader: aborted_transactions failed; returning empty abort list"
                        );
                        Vec::new()
                    }
                };
                p.out.aborted_transactions = Some(
                    aborts
                        .into_iter()
                        .map(|e| AbortedTransaction {
                            producer_id: e.producer_id,
                            first_offset: e.start_offset,
                            ..Default::default()
                        })
                        .collect(),
                );
            }

            p.out.records = Some(batch.into());
            Some(bytes_est)
        }
        Ok(None) => None,
        Err(crabka_remote_storage::RemoteStorageError::NotReady { partition }) => {
            // The metadata partition that would answer this read is assigned
            // to this broker but its consumer has not caught up yet. Leave
            // OFFSET_OUT_OF_RANGE (retryable) — NOT a definitive miss — so the
            // client retries. Expected churn during catch-up, so log at debug.
            tracing::debug!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                metadata_partition = partition,
                "remote-reader: metadata partition not yet caught up; \
                 leaving OFFSET_OUT_OF_RANGE for client retry"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                error = %e,
                "remote-reader: fetch_batch failed; leaving OFFSET_OUT_OF_RANGE"
            );
            None
        }
    }
}

/// Wait for any readable partition's `append_notify` to fire (with timeout),
/// then re-read every partition once. Resets each partition's accumulated
/// records before re-reading so the new read replaces the old one.
// cargo-mutants: long-poll serve-loop glue — parks on partition append/HW
// notifiers, then replays `do_read` per partition. The surviving `Ok(())`
// mutant only manifests under a live parked-consumer long poll (a notifier
// fires and the re-read must repopulate `p.out`), which the fetch integration
// suite drives; there is no in-file signal without a full HW-advanced
// partition + notifier fixture.
#[cfg_attr(test, mutants::skip)]
async fn long_poll_then_reread(
    broker: &Broker,
    pending: &mut [PendingRead],
    max_wait_ms: i32,
    sendfile_capable: bool,
) -> Result<(), BrokerError> {
    let mut notifies: Vec<Arc<Notify>> = Vec::new();
    for p in pending.iter() {
        if let Some(part) = p.partition.as_ref() {
            notifies.push(part.append_notify.clone());
            // KIP-392: a consumer reading from a follower becomes unblocked
            // when the follower's HW advances (via set_follower_hw), not only
            // on raw append. Follower (inter-broker) fetches don't need this.
            if !p.is_follower_fetch {
                notifies.push(part.hw_advance_notify.clone());
            }
        }
    }
    if notifies.is_empty() {
        return Ok(());
    }
    // `Notify::notified()` returns a non-Send `Notified<'_>` that borrows
    // from its `Arc<Notify>`. Move the Arc into an `async move` block so
    // the future owns its Arc and is `'static + Send` (see `WaitFut` type
    // alias above).
    let waits: Vec<WaitFut> = notifies
        .into_iter()
        .map(|n| Box::pin(async move { n.notified().await }) as WaitFut)
        .collect();
    let max_wait = Duration::from_millis(u64::from(u32::try_from(max_wait_ms).unwrap_or(0)));
    let _ = tokio::time::timeout(max_wait, futures_util::future::select_all(waits)).await;

    for p in pending.iter_mut() {
        let Some(part) = p.partition.clone() else {
            continue;
        };
        p.out = PartitionData {
            partition_index: p.partition_index,
            ..Default::default()
        };
        // Time the re-read so its duration accumulates into the same
        // per-partition CPU counter as the first pass (wall-clock delta;
        // see the first-pass comment for why this replaces TaskMonitor).
        let read_start = std::time::Instant::now();
        do_read(
            &part,
            ReadRequest {
                topic_id: Some(uuid::Uuid::from_bytes(p.topic_id.0)),
                hot_tail: Some(broker.hot_tail.clone()),
                // Wrap the decoded-request wire offset into `Offset` for the read.
                fetch_offset: Offset(p.fetch_offset),
                max_bytes: p.max_bytes,
                read_committed: p.read_committed,
                is_follower_fetch: p.is_follower_fetch,
                sendfile_capable,
                sendfile_min_bytes: broker.config.sendfile_min.bytes_usize(),
            },
            &mut p.out,
        )
        .await?;
        let micros = u64::try_from(read_start.elapsed().as_micros()).unwrap_or(u64::MAX);
        p.cpu_micros = p.cpu_micros.saturating_add(micros);

        // Re-attempt the remote-tier read on the re-read pass
        // so a long-poll that fires on a non-tiered partition doesn't
        // clobber the remote batch we'd already served on this one.
        if p.out.error_code == codes::OFFSET_OUT_OF_RANGE
            && try_remote_read(broker, p, &part).await.is_none()
        {
            let _ = crate::diskless::read::try_diskless_read(broker, p, &part).await;
        }
    }
    Ok(())
}

/// KIP-73 leader-side throttle: walk `throttled_idxs` in order and drop
/// whole-partition chunks until the remaining throttled bytes fit within
/// `budget`. Partitions are dropped completely (records set to `None`) — no
/// mid-batch truncation, since Kafka clients expect complete record batches.
fn truncate_throttled_responses(
    responses: &mut [FetchableTopicResponse],
    throttled_idxs: &[(usize, usize)],
    budget: u64,
) {
    let mut remaining = budget;
    for &(ti, pi) in throttled_idxs {
        let part = &mut responses[ti].partitions[pi];
        let chunk_size = part.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64;
        if chunk_size <= remaining {
            remaining -= chunk_size;
        } else {
            // Budget exhausted — drop this chunk and all subsequent throttled ones.
            part.records = None;
            remaining = 0;
        }
    }
}

/// Sum the encoded byte sizes of all record batches across all topic partitions
/// in the assembled Fetch response. Used by the KIP-13 `consumer_byte_rate` hook.
fn sum_response_bytes(responses: &[FetchableTopicResponse]) -> u64 {
    responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|p| p.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64)
        .sum()
}

/// KIP-13 `consumer_byte_rate` enforcement. Looks up the matching quota for
/// `(principal, client_id)`, consumes `bytes` from the bucket, and returns
/// the throttle delay capped at 1 second. Returns `Duration::ZERO` when no
/// quota is configured or the bucket has sufficient capacity.
fn consume_consumer_quota(
    image: &crabka_metadata::MetadataImage,
    buckets: &crate::quota::QuotaBuckets,
    principal: &str,
    client_id: &str,
    bytes: u64,
    maximum: Time,
) -> Time {
    let Some((entity_key, rate)) =
        crate::quota::lookup_quota_with_key(image, principal, client_id, "consumer_byte_rate")
    else {
        return <Time as TimeExt>::ZERO;
    };
    if rate <= 0.0 {
        return <Time as TimeExt>::ZERO;
    }
    let bucket = buckets.get_or_create(
        "consumer_byte_rate",
        &entity_key,
        rate.to_u64().unwrap_or(u64::MAX),
    );
    let granted = bucket.try_consume(bytes);
    if granted >= bytes {
        return <Time as TimeExt>::ZERO;
    }
    let overage = bytes - granted;
    let delay_secs = overage.to_f64().unwrap_or(f64::MAX) / rate;
    Time::from_secs_f64(delay_secs).min(maximum)
}

fn should_use_sendfile(total_bytes: usize, has_regions: bool, minimum_bytes: usize) -> bool {
    total_bytes >= minimum_bytes && has_regions
}

/// Group resolved `PendingRead`s back into per-topic response entries,
/// preserving the order topics first appeared in the request. Returns the
/// per-topic `cpu_micros` accumulators alongside, positionally aligned with
/// the returned `Vec` (`cpu_micros[ti][pi]` matches `responses[ti].partitions[pi]`)
/// so the caller can attribute CPU without re-keying by topic name.
type GroupedResponses = (Vec<FetchableTopicResponse>, Vec<Vec<u64>>);

fn group_into_topic_responses(pending: Vec<PendingRead>) -> GroupedResponses {
    let mut topic_order: Vec<String> = Vec::new();
    // Value: (topic_id, partitions, cpu_micros) — the trailing Vec mirrors
    // `partitions` positionally.
    let mut by_topic: std::collections::HashMap<String, (WireUuid, Vec<PartitionData>, Vec<u64>)> =
        std::collections::HashMap::new();
    for p in pending {
        let entry = by_topic
            .entry(p.topic_name.clone())
            .or_insert_with(|| (p.topic_id, Vec::new(), Vec::new()));
        entry.1.push(p.out);
        entry.2.push(p.cpu_micros);
        if !topic_order.iter().any(|t| t == &p.topic_name) {
            topic_order.push(p.topic_name);
        }
    }
    let mut responses = Vec::with_capacity(topic_order.len());
    let mut cpu_micros = Vec::with_capacity(topic_order.len());
    for name in topic_order {
        let (topic_id, parts, micros) = by_topic.remove(&name).expect("topic order populated");
        responses.push(FetchableTopicResponse {
            topic: name,
            topic_id,
            partitions: parts,
            ..Default::default()
        });
        cpu_micros.push(micros);
    }
    (responses, cpu_micros)
}

/// Encode a `FetchResponse` into a `BytesMut`, choosing the legacy
/// `kafka_3_6_2` codec for Fetch v0-3 and the current canonical codec
/// for v4+. The version boundary mirrors the request-decode boundary.
pub(crate) fn encode_fetch_response(
    resp: FetchResponse,
    version: i16,
) -> Result<BytesMut, crate::error::BrokerError> {
    if version < 4 {
        let legacy: crabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
            resp.into();
        let mut buf = BytesMut::with_capacity(legacy.encoded_len(version));
        legacy.encode(&mut buf, version)?;
        Ok(buf)
    } else {
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::{Time, convert::TimeExt, millis};
    #[test]
    fn consume_consumer_quota_tuple_match_overage_throttles() {
        use crabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app-x".into()),
                },
            ],
            config_key: "consumer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::new();
        let delay_match =
            super::consume_consumer_quota(&img, &buckets, "alice", "app-x", 4096, millis(25));
        assert!(
            delay_match == millis(25),
            "tuple quota match should honor the configured cap; got {delay_match:?}"
        );
        let buckets2 = crate::quota::QuotaBuckets::new();
        let delay_other =
            super::consume_consumer_quota(&img, &buckets2, "alice", "other", 4096, millis(25));
        assert!(
            delay_other == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }

    #[test]
    fn sendfile_eligibility_honors_nondefault_threshold() {
        assert!(super::should_use_sendfile(64, true, 64));
        assert!(!super::should_use_sendfile(63, true, 64));
        assert!(!super::should_use_sendfile(64, false, 64));
    }
}

#[cfg(test)]
#[path = "fetch_visibility_model.rs"]
mod fetch_visibility_model;

#[cfg(test)]
mod visibility_fuzz {
    use proptest::prelude::*;

    use super::{Offset, compute_visibility_window};

    proptest! {
        /// The per-fetch visibility contract over large-N random valid watermark
        /// tuples (`log_start <= lso <= hw <= log_end`) + fetch params.
        #[test]
        fn visibility_contract_holds(
            a in 0i64..1_000_000,
            b in 0i64..1_000_000,
            c in 0i64..1_000_000,
            d in 0i64..1_000_000,
            fo in 0i64..1_000_000,
            is_follower in any::<bool>(),
            rc_raw in any::<bool>(),
        ) {
            let mut v = [a, b, c, d];
            v.sort_unstable();
            let (log_start, lso, hw, log_end) = (v[0], v[1], v[2], v[3]);
            let read_committed = rc_raw && !is_follower; // read_committed ⟹ !follower
            let w = compute_visibility_window(
                is_follower,
                read_committed,
                Offset(log_start),
                Offset(hw),
                Offset(lso),
                Offset(log_end),
                Offset(fo),
            );
            // Unwrap the `Offset` window fields into this proptest's `i64` world.
            let (limit_offset, response_hw, response_lso, effective_lso) = (
                w.limit_offset.0,
                w.response_hw.0,
                w.response_lso.0,
                w.effective_lso.0,
            );
            prop_assert!(limit_offset >= 0 && response_hw >= 0 && response_lso >= 0);
            prop_assert_eq!(w.out_of_range, fo < log_start);
            let upper = if is_follower { log_end } else { hw };
            if !w.out_of_range {
                prop_assert_eq!(w.empty, fo >= upper);
            }
            if is_follower {
                prop_assert_eq!(limit_offset, log_end);
                prop_assert!(limit_offset >= hw);
                prop_assert_eq!(response_hw, log_end);
            } else {
                prop_assert!(limit_offset <= hw, "consumer fetch must not expose beyond HW");
                prop_assert_eq!(response_hw, hw);
                prop_assert!(response_lso <= response_hw);
                if read_committed {
                    prop_assert_eq!(effective_lso, lso.min(hw));
                    prop_assert!(limit_offset <= lso.min(hw));
                    prop_assert_eq!(response_lso, lso.min(hw));
                }
            }
        }

        /// KIP-227 monotonicity: advancing hw/lso/log_end never lowers the
        /// reported HW/LSO for any fixed fetch shape.
        #[test]
        fn response_monotonic(
            base in 0i64..100_000,
            d_end in 0i64..100_000,
            d_adv in 0i64..100_000,
            d_end2 in 0i64..100_000,
            is_follower in any::<bool>(),
            rc_raw in any::<bool>(),
        ) {
            let read_committed = rc_raw && !is_follower;
            let log_start = 0;
            // Valid baseline: lso == hw == base, log_end >= hw.
            let (hw, lso, log_end) = (base, base, base + d_end);
            // Advance all of hw/lso/log_end (still valid: lso == hw).
            let (hw2, lso2, log_end2) = (hw + d_adv, lso + d_adv, log_end + d_adv + d_end2);
            let w1 = compute_visibility_window(
                is_follower,
                read_committed,
                Offset(log_start),
                Offset(hw),
                Offset(lso),
                Offset(log_end),
                Offset(0),
            );
            let w2 = compute_visibility_window(
                is_follower,
                read_committed,
                Offset(log_start),
                Offset(hw2),
                Offset(lso2),
                Offset(log_end2),
                Offset(0),
            );
            prop_assert!(w2.response_hw >= w1.response_hw, "response_hw regressed");
            prop_assert!(w2.response_lso >= w1.response_lso, "response_lso regressed");
        }
    }
}

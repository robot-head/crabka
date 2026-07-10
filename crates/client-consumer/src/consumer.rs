//! `Consumer` — public lifecycle handle. Built via [`Consumer::builder`].
//! Subscribe-only — no `assign()`. Use `crabka-client-core` directly for
//! manual partition consumption.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::JoinGroupResponse,
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    assignor::Assignor,
    builder::{
        AutoOffsetReset, IsolationLevel, decode_assignment, decode_subscription, encode_assignment,
        encode_subscription,
    },
    coordinator::{
        COORDINATOR_RETRY_TIMEOUT, CoordinatorState, find_coordinator, with_coordinator_refind,
    },
    error::ConsumerError,
    group_metadata::ConsumerGroupMetadata,
};

/// Subscribe-style consumer handle. Construct via [`Consumer::builder`].
#[allow(dead_code)] // `session_timeout` / `heartbeat_interval`
// are captured for diagnostics; the live values are owned
// by the coordinator task post-start.
pub struct Consumer {
    pub(crate) client: Client,
    pub(crate) group_id: String,
    /// Node id of the group's coordinator broker, discovered via
    /// `FindCoordinator` at build time and kept current by the coordinator task
    /// (shared `Arc<AtomicI32>`). The commit path (`commit.rs`) reads it to route
    /// `OffsetCommit` to the coordinator over this data-path client.
    pub(crate) coordinator_id: Arc<AtomicI32>,
    pub(crate) member_id: String,
    pub(crate) group_instance_id: Option<String>,
    /// The group generation the commit path stamps onto `OffsetCommit`. Shared
    /// (`Arc<AtomicI32>`) with the coordinator task, which is the sole writer and
    /// publishes the new value on every (re)join — so a commit issued after a
    /// rebalance uses the *current* generation instead of a stale start-up
    /// snapshot (which the broker rejects with `ILLEGAL_GENERATION`).
    pub(crate) current_generation: Arc<AtomicI32>,
    pub(crate) subscribed_topics: Vec<String>,
    /// Current assigned partitions: `(topic, partition_index)`.
    pub(crate) assigned: Arc<Mutex<Vec<(String, i32)>>>,
    /// Next offset to fetch per partition.
    pub(crate) next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// KIP-320 per-partition leader-epoch metadata, keyed like `next_offsets`.
    pub(crate) positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
    /// Pending [`seek`](Consumer::seek) targets: `(topic, partition) -> next
    /// offset to fetch`. Applied at the top of `poll` once the partition is
    /// assigned, *after* the coordinator's post-assignment prime — so a seek
    /// requested before assignment is not overwritten by the prime (see
    /// `seek.rs`). Empty in steady state.
    pub(crate) pending_seeks: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// Topic UUIDs resolved at build time. Required by Fetch v ≥ 13
    /// (which carries `topic_id` instead of the topic name).
    pub(crate) topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub(crate) session_timeout: Duration,
    pub(crate) heartbeat_interval: Duration,
    #[allow(dead_code)]
    pub(crate) assignor: Assignor,
    pub(crate) coordinator_shutdown: CancellationToken,
    pub(crate) coordinator_handle: Option<JoinHandle<()>>,
    /// Controls which records are returned by `poll`.
    pub(crate) isolation_level: IsolationLevel,
    pub(crate) fetch_max_bytes: i32,
    pub(crate) fetch_partition_max_bytes: i32,
    /// What `poll` does on a missing offset / detected truncation. `None`
    /// surfaces `ConsumerError::LogTruncation`; otherwise the safe offset is
    /// applied (KIP-320).
    pub(crate) auto_offset_reset: AutoOffsetReset,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.coordinator_shutdown.cancel();
    }
}

/// A per-record header key/value pair, as defined by the Kafka v2 record
/// format. The key is a UTF-8 string; the value is optional raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

/// One record returned by `Consumer::poll`.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

fn initial_subscription_bytes(subscribe: &[String], client_rack: Option<&str>) -> bytes::Bytes {
    encode_subscription(subscribe, &[], -1, client_rack)
}

fn build_join_request(
    group_id: String,
    member_id: String,
    group_instance_id: Option<String>,
    protocol_name: String,
    subscription_bytes: bytes::Bytes,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id,
        protocol_type: "consumer".into(),
        member_id,
        group_instance_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        protocols: vec![JoinGroupRequestProtocol {
            name: protocol_name,
            metadata: subscription_bytes,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn first_join_member_id(resp: &JoinGroupResponse) -> Result<String, ConsumerError> {
    let member_id = if resp.error_code == 79 || resp.error_code == 0 {
        resp.member_id.clone()
    } else {
        return Err(ConsumerError::Server(resp.error_code));
    };
    if member_id.is_empty() {
        return Err(ConsumerError::RebalanceFailed(
            "broker did not assign a member_id".into(),
        ));
    }
    Ok(member_id)
}

fn is_subscribed_topic(subscribe: &[String], name: &str) -> bool {
    subscribe.iter().any(|s| s == name)
}

fn is_group_leader(leader: &str, member_id: &str) -> bool {
    leader == member_id
}

fn build_sync_assignment(
    member_id: String,
    partitions: &[(String, i32)],
) -> SyncGroupRequestAssignment {
    SyncGroupRequestAssignment {
        member_id,
        assignment: encode_assignment(partitions),
        ..Default::default()
    }
}

fn build_sync_request(
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
    protocol_name: String,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        protocol_type: Some("consumer".into()),
        protocol_name: Some(protocol_name),
        assignments,
        ..Default::default()
    }
}

async fn leave_startup_member(
    client: &Client,
    coordinator_id: &AtomicI32,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<String>,
) {
    if member_id.is_empty() {
        return;
    }
    let broker = client.broker(coordinator_id.load(Ordering::Relaxed));
    let send = broker.send(crate::coordinator::build_leave_group_request(
        group_id.to_string(),
        member_id.to_string(),
        group_instance_id,
    ));
    let _ = tokio::time::timeout(Duration::from_secs(5), send).await;
}

fn has_assigned_partitions(assigned_partitions: &[(String, i32)]) -> bool {
    !assigned_partitions.is_empty()
}

pub(crate) fn starting_offset(committed: i64, auto_offset_reset: AutoOffsetReset) -> i64 {
    if committed >= 0 {
        committed
    } else {
        reset_starting_offset(auto_offset_reset)
    }
}

pub(crate) fn reset_starting_offset(auto_offset_reset: AutoOffsetReset) -> i64 {
    match auto_offset_reset {
        AutoOffsetReset::Earliest => 0,
        // Resolved by poll() on first call.
        AutoOffsetReset::Latest | AutoOffsetReset::None => i64::MAX,
    }
}

fn primed_position(committed_epoch: i32) -> crate::position::PartitionPosition {
    crate::position::PartitionPosition {
        // Wrap the committed leader epoch (raw wire `int32` from OffsetFetch) at
        // the decode boundary.
        offset_epoch: crabka_ids::LeaderEpoch(committed_epoch),
        ..Default::default()
    }
}

/// Per-attempt timeout for `Consumer::start`.  Must exceed the default
/// `rebalance_timeout` (60 s) so a legitimately slow group-join isn't
/// cut short, while still bounding a true cold-boot hang.
const CONSUMER_START_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(90);

/// Wall-clock deadline across all retry attempts in `Consumer::start`.
const CONSUMER_START_DEADLINE: Duration = Duration::from_mins(5);

#[bon::bon]
impl Consumer {
    /// Build a [`Consumer`] subscribed to the given topics.
    ///
    /// Validates configuration eagerly (fail-fast before any network I/O),
    /// then calls [`Self::start_once`] with a per-attempt timeout.  If an
    /// attempt stalls (lost-wakeup during cold-boot group-join contention) or
    /// returns a transient error, the timed-out future is dropped — cancelling
    /// its in-flight connections — and a fresh attempt is started.  A genuine
    /// misconfiguration or persistent error surfaces immediately.
    #[builder(start_fn = builder, finish_fn = build)]
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(
        name = "consumer.start",
        level = "info",
        skip_all,
        fields(group_id = %group_id, client_id = %client_id),
        err
    )]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-consumer".to_string())] client_id: String,
        #[builder(into)] group_id: String,
        #[builder(default = std::time::Duration::from_secs(45))]
        session_timeout: std::time::Duration,
        #[builder(default = std::time::Duration::from_mins(1))]
        rebalance_timeout: std::time::Duration,
        #[builder(default = std::time::Duration::from_secs(3))]
        heartbeat_interval: std::time::Duration,
        #[builder(into)] subscribe: Vec<String>,
        #[builder(into)] group_instance_id: Option<String>,
        #[builder(default = AutoOffsetReset::Latest)] auto_offset_reset: AutoOffsetReset,
        #[builder(default = IsolationLevel::ReadUncommitted)] isolation_level: IsolationLevel,
        #[builder(default = Assignor::Range)] assignor: Assignor,
        #[builder(default = crate::poll::DEFAULT_FETCH_MAX_BYTES)] fetch_max_bytes: i32,
        #[builder(default = crate::poll::DEFAULT_FETCH_PARTITION_MAX_BYTES)]
        fetch_partition_max_bytes: i32,
        #[builder(default = std::time::Duration::from_secs(30))]
        request_timeout: std::time::Duration,
        #[builder(into)] client_rack: Option<String>,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ConsumerError> {
        // Fail fast on misconfig — before any retry loop.
        if subscribe.is_empty() {
            return Err(ConsumerError::NotSubscribed);
        }
        if group_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed("group_id required".into()));
        }
        if group_instance_id.as_deref().is_some_and(str::is_empty) {
            return Err(ConsumerError::RebalanceFailed(
                "group_instance_id must not be empty".into(),
            ));
        }
        if fetch_max_bytes <= 0 {
            return Err(ConsumerError::RebalanceFailed(
                "fetch_max_bytes must be positive".into(),
            ));
        }
        if fetch_partition_max_bytes <= 0 {
            return Err(ConsumerError::RebalanceFailed(
                "fetch_partition_max_bytes must be positive".into(),
            ));
        }

        let started = tokio::time::Instant::now();
        let mut backoff = Duration::from_millis(500);
        loop {
            match tokio::time::timeout(
                CONSUMER_START_ATTEMPT_TIMEOUT,
                Self::start_once(
                    bootstrap.clone(),
                    client_id.clone(),
                    group_id.clone(),
                    session_timeout,
                    rebalance_timeout,
                    heartbeat_interval,
                    subscribe.clone(),
                    group_instance_id.clone(),
                    auto_offset_reset,
                    isolation_level,
                    assignor,
                    fetch_max_bytes,
                    fetch_partition_max_bytes,
                    request_timeout,
                    client_rack.clone(),
                    security.clone(),
                ),
            )
            .await
            {
                Ok(Ok(consumer)) => return Ok(consumer),
                Ok(Err(error)) => {
                    if started.elapsed() < CONSUMER_START_DEADLINE
                        && is_retriable_consumer_start_error(&error)
                    {
                        tracing::warn!(
                            group = %group_id,
                            %error,
                            "consumer startup failed transiently; retrying with a fresh connection"
                        );
                    } else {
                        return Err(error);
                    }
                }
                Err(_elapsed) => {
                    if started.elapsed() >= CONSUMER_START_DEADLINE {
                        return Err(ConsumerError::Client(
                            crabka_client_core::ClientError::Timeout(
                                CONSUMER_START_ATTEMPT_TIMEOUT,
                            ),
                        ));
                    }
                    tracing::warn!(
                        group = %group_id,
                        timeout = ?CONSUMER_START_ATTEMPT_TIMEOUT,
                        "consumer startup exceeded attempt timeout \
                         (likely a cold-boot group-join stall); \
                         retrying with a fresh connection"
                    );
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
        }
    }

    /// Single attempt to build a [`Consumer`]: resolve bootstrap, `JoinGroup`
    /// (twice), compute assignment if elected leader, `SyncGroup`, prime
    /// offsets, then spawn the coordinator task.
    ///
    /// Called by [`Self::start`] under a per-attempt timeout.  Dropping the
    /// returned future at any point before the final `tokio::spawn` cancels
    /// all in-flight connections cleanly.  The coordinator task is only spawned
    /// at the very end of this function (no `.await` follows it), so a
    /// timed-out attempt can never orphan a coordinator task.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    #[tracing::instrument(
        name = "consumer.start_once",
        level = "info",
        skip_all,
        fields(
            group_id = %group_id,
            coordinator_id = tracing::field::Empty,
            member_id = tracing::field::Empty,
            generation = tracing::field::Empty,
            is_leader = tracing::field::Empty,
            assigned_partitions = tracing::field::Empty,
        ),
        err
    )]
    async fn start_once(
        bootstrap: String,
        client_id: String,
        group_id: String,
        session_timeout: std::time::Duration,
        rebalance_timeout: std::time::Duration,
        heartbeat_interval: std::time::Duration,
        subscribe: Vec<String>,
        group_instance_id: Option<String>,
        auto_offset_reset: AutoOffsetReset,
        isolation_level: IsolationLevel,
        assignor: Assignor,
        fetch_max_bytes: i32,
        fetch_partition_max_bytes: i32,
        request_timeout: std::time::Duration,
        client_rack: Option<String>,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ConsumerError> {
        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .connect_timeout(request_timeout)
            .request_timeout(request_timeout)
            .maybe_security(security.clone())
            .build()
            .await?;

        let session_timeout_ms = i32::try_from(session_timeout.as_millis()).unwrap_or(i32::MAX);
        let rebalance_timeout_ms = i32::try_from(rebalance_timeout.as_millis()).unwrap_or(i32::MAX);

        // 0. Discover the group's coordinator broker. Real Kafka (Strimzi)
        //    returns NOT_COORDINATOR (16) for any group RPC that doesn't reach
        //    the group's actual coordinator, so every Join/Sync/Heartbeat/Commit/
        //    Fetch/Leave must target it — not the arbitrary bootstrap broker.
        //    `find_coordinator` also `refresh_metadata`s the main client's pool
        //    so it learns the coordinator broker's address (needed by
        //    `client.broker(coordinator_id)` here and by `commit.rs`).
        let coordinator_id = Arc::new(AtomicI32::new(find_coordinator(&client, &group_id).await?));
        tracing::Span::current().record("coordinator_id", coordinator_id.load(Ordering::Relaxed));

        // First JoinGroup uses empty `owned_partitions` + `generation_id=-1`:
        // we've never been in the group before, so we have nothing to claim
        // and no prior generation to defend against zombie ownership.
        let subscription_bytes = initial_subscription_bytes(&subscribe, client_rack.as_deref());
        let protocol_name = assignor.protocol_name().to_string();

        // 1. First JoinGroup — empty member_id, expect MEMBER_ID_REQUIRED (79)
        //    or a regular response; either way the broker hands us a member_id.
        //    Routed to the coordinator broker, re-discovering it on a
        //    cold/relocating-coordinator code (14/15/16) before each retry.
        let r1 = with_coordinator_refind(
            &client,
            &group_id,
            &coordinator_id,
            COORDINATOR_RETRY_TIMEOUT,
            |r: &JoinGroupResponse| r.error_code,
            || {
                let group_id = group_id.clone();
                let protocol_name = protocol_name.clone();
                let subscription_bytes = subscription_bytes.clone();
                let group_instance_id = group_instance_id.clone();
                let client = &client;
                let target = coordinator_id.load(Ordering::Relaxed);
                async move {
                    client
                        .broker(target)
                        .send(build_join_request(
                            group_id,
                            String::new(),
                            group_instance_id.clone(),
                            protocol_name,
                            subscription_bytes,
                            session_timeout_ms,
                            rebalance_timeout_ms,
                        ))
                        .await
                        .map_err(ConsumerError::from)
                }
            },
        )
        .await?;
        let member_id = first_join_member_id(&r1)?;
        tracing::Span::current().record("member_id", member_id.as_str());

        let cleanup_client = client.clone();
        let cleanup_coordinator_id = Arc::clone(&coordinator_id);
        let cleanup_group_id = group_id.clone();
        let cleanup_member_id = member_id.clone();
        let cleanup_group_instance_id = group_instance_id.clone();
        let start_result = async {
            // 2. Second JoinGroup with the assigned member_id, on the coordinator.
            let r2 = with_coordinator_refind(
                &client,
                &group_id,
                &coordinator_id,
                COORDINATOR_RETRY_TIMEOUT,
                |r: &JoinGroupResponse| r.error_code,
                || {
                    let group_id = group_id.clone();
                    let protocol_name = protocol_name.clone();
                    let subscription_bytes = subscription_bytes.clone();
                    let member_id = member_id.clone();
                    let group_instance_id = group_instance_id.clone();
                    let client = &client;
                    let target = coordinator_id.load(Ordering::Relaxed);
                    async move {
                        client
                            .broker(target)
                            .send(build_join_request(
                                group_id,
                                member_id,
                                group_instance_id.clone(),
                                protocol_name,
                                subscription_bytes,
                                session_timeout_ms,
                                rebalance_timeout_ms,
                            ))
                            .await
                            .map_err(ConsumerError::from)
                    }
                },
            )
            .await?;
            if r2.error_code != 0 {
                return Err(ConsumerError::Server(r2.error_code));
            }

            // 3. Always issue a Metadata to resolve topic_ids (needed for
            //    Fetch v ≥ 13). If we are the leader, also use the partition
            //    counts to compute the assignment.
            //    `refresh_metadata` (not a bare `send`) so the main client's
            //    BrokerPool learns each broker's (id → addr) mapping up front,
            //    letting `poll`/`validate` route to partition leaders immediately
            //    rather than waiting for the first `refresh_leader_epochs` pass.
            let md = client.refresh_metadata().await?;
            let mut topic_ids: HashMap<String, WireUuid> = HashMap::new();
            let mut topic_partitions: HashMap<String, i32> = HashMap::new();
            for t in &md.topics {
                let Some(name) = &t.name else { continue };
                if is_subscribed_topic(&subscribe, name) {
                    let count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
                    topic_partitions.insert(name.clone(), count);
                    topic_ids.insert(name.clone(), t.topic_id);
                }
            }

            let is_leader = is_group_leader(&r2.leader, &member_id);
            tracing::Span::current().record("generation", r2.generation_id);
            tracing::Span::current().record("is_leader", is_leader);
            let assignments_for_sync: Vec<SyncGroupRequestAssignment> = if is_leader {
                let assignments = match assignor {
                    Assignor::Range => {
                        let inputs: Vec<(String, Vec<String>)> = r2
                            .members
                            .iter()
                            .map(|m| {
                                let ds = decode_subscription(&m.metadata);
                                (m.member_id.clone(), ds.topics)
                            })
                            .collect();
                        crate::assignor::range::assign(inputs, &topic_partitions)
                    }
                    Assignor::CooperativeSticky => {
                        let inputs: Vec<crate::assignor::cooperative_sticky::MemberInput> = r2
                            .members
                            .iter()
                            .map(|m| {
                                let ds = decode_subscription(&m.metadata);
                                (m.member_id.clone(), ds.topics, ds.owned, ds.generation_id)
                            })
                            .collect();
                        crate::assignor::cooperative_sticky::assign(&inputs, &topic_partitions)
                    }
                };
                assignments
                    .into_iter()
                    .map(|(m, partitions)| build_sync_assignment(m, &partitions))
                    .collect()
            } else {
                Vec::new()
            };

            // 4. SyncGroup — leader installs assignments; everyone receives their
            //    own assignment in the response. On the coordinator broker, with
            //    re-discovery on a cold/relocating-coordinator code.
            let r3 = with_coordinator_refind(
                &client,
                &group_id,
                &coordinator_id,
                COORDINATOR_RETRY_TIMEOUT,
                |r: &SyncGroupResponse| r.error_code,
                || {
                    let group_id = group_id.clone();
                    let protocol_name = protocol_name.clone();
                    let member_id = member_id.clone();
                    let assignments_for_sync = assignments_for_sync.clone();
                    let generation_id = r2.generation_id;
                    let group_instance_id = group_instance_id.clone();
                    let client = &client;
                    let target = coordinator_id.load(Ordering::Relaxed);
                    async move {
                        client
                            .broker(target)
                            .send(build_sync_request(
                                group_id,
                                generation_id,
                                member_id,
                                group_instance_id.clone(),
                                protocol_name,
                                assignments_for_sync,
                            ))
                            .await
                            .map_err(ConsumerError::from)
                    }
                },
            )
            .await?;
            if r3.error_code != 0 {
                return Err(ConsumerError::Server(r3.error_code));
            }
            let assigned_partitions = decode_assignment(&r3.assignment);
            tracing::Span::current().record("assigned_partitions", assigned_partitions.len());

            // 5. Fetch existing committed offsets so poll() resumes correctly.
            let mut next_offsets: HashMap<(String, i32), i64> = HashMap::new();
            let mut positions: HashMap<(String, i32), crate::position::PartitionPosition> =
                HashMap::new();
            if has_assigned_partitions(&assigned_partitions) {
                let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
                for (t, p) in &assigned_partitions {
                    by_topic.entry(t.clone()).or_default().push(*p);
                }
                // OffsetFetch is a coordinator RPC — route it to the coordinator
                // broker (its id is fresh from the join/sync above).
                let of = client
                    .broker(coordinator_id.load(Ordering::Relaxed))
                    .send(crate::offset_wire::build_offset_fetch(
                        &group_id, &by_topic, &topic_ids,
                    ))
                    .await?;
                let id_to_name = crate::offset_wire::id_to_name(&topic_ids);
                for (name, partition_index, committed, committed_epoch) in
                    crate::offset_wire::parse_offset_fetch(&of, &id_to_name)
                {
                    let starting = starting_offset(committed, auto_offset_reset);
                    next_offsets.insert((name.clone(), partition_index), starting);
                    positions.insert((name, partition_index), primed_position(committed_epoch));
                }
            }

            // 6. Spawn the coordinator task (heartbeat + rebalance loop) on its
            //    own connection.
            //
            //    The broker processes requests serially per TCP connection: a
            //    JoinGroup parked in the rebalance-join purgatory (up to
            //    INITIAL_REBALANCE_DELAY per round, and cooperative rounds
            //    cascade) blocks every later request on that same socket. If the
            //    coordinator shared `poll()`'s connection, a `Fetch` issued
            //    mid-rebalance would head-of-line-block behind the parked
            //    JoinGroup and stall until the client request timeout. A dedicated
            //    coordinator connection keeps the data path (`poll`/commit)
            //    independent of the group-protocol path. (The JVM client never
            //    hits this because real brokers serve a connection's requests
            //    concurrently.)
            let coordinator_client = Client::builder()
                .bootstrap(&bootstrap)
                .client_id(client_id.clone())
                .connect_timeout(request_timeout)
                .request_timeout(request_timeout)
                .maybe_security(security.clone())
                .build()
                .await?;

            let assigned = Arc::new(Mutex::new(assigned_partitions));
            let next_offsets = Arc::new(Mutex::new(next_offsets));
            let positions = Arc::new(Mutex::new(positions));
            let pending_seeks = Arc::new(Mutex::new(HashMap::new()));
            let topic_ids = Arc::new(Mutex::new(topic_ids));
            // Shared with the coordinator task so the commit path always stamps the
            // current generation; the coordinator publishes to it on every (re)join.
            let current_generation = Arc::new(AtomicI32::new(r2.generation_id));

            let shutdown = CancellationToken::new();
            let state = CoordinatorState {
                client: coordinator_client,
                group_id: group_id.clone(),
                coordinator_id: Arc::clone(&coordinator_id),
                member_id: member_id.clone(),
                group_instance_id: group_instance_id.clone(),
                generation_id: r2.generation_id,
                current_generation: Arc::clone(&current_generation),
                assignor,
                subscribed_topics: subscribe.clone(),
                assigned: Arc::clone(&assigned),
                next_offsets: Arc::clone(&next_offsets),
                positions: Arc::clone(&positions),
                topic_ids: Arc::clone(&topic_ids),
                session_timeout,
                rebalance_timeout,
                heartbeat_interval,
                auto_offset_reset,
                client_rack: client_rack.clone(),
                // The metadata snapshot this initial assignment was computed against,
                // threaded to the coordinator so its rejoin baseline starts from
                // exactly what we saw here — not a fresh fetch that could already
                // include a topic created during start-up (which would strand a
                // cold-start empty assignment permanently).
                initial_subscribed_counts: topic_partitions,
            };
            // IMPORTANT: `tokio::spawn` is the very last operation — no `.await`
            // follows it.  Dropping a timed-out `start_once` future before this
            // point cancels all in-flight connections without spawning anything.
            let coord_handle = tokio::spawn(crate::coordinator::run(state, shutdown.clone()));

            Ok(Consumer {
                client,
                group_id,
                coordinator_id,
                member_id,
                group_instance_id: group_instance_id.clone(),
                current_generation,
                subscribed_topics: subscribe,
                assigned,
                next_offsets,
                positions,
                pending_seeks,
                topic_ids,
                session_timeout,
                heartbeat_interval,
                assignor,
                coordinator_shutdown: shutdown,
                coordinator_handle: Some(coord_handle),
                isolation_level,
                fetch_max_bytes,
                fetch_partition_max_bytes,
                auto_offset_reset,
            })
        }
        .await;
        match start_result {
            Ok(consumer) => Ok(consumer),
            Err(error) => {
                leave_startup_member(
                    &cleanup_client,
                    &cleanup_coordinator_id,
                    &cleanup_group_id,
                    &cleanup_member_id,
                    cleanup_group_instance_id,
                )
                .await;
                Err(ConsumerError::StartupAfterJoin(Box::new(error)))
            }
        }
    }
}

/// Returns `true` for transient startup errors where dropping the half-built
/// consumer and retrying a fresh build is expected to succeed.
///
/// Returns `false` for permanent misconfig or decode errors so those surface
/// immediately without pointless retries.
fn is_retriable_consumer_start_error(error: &ConsumerError) -> bool {
    // Retry only conditions that a fresh attempt a moment later is likely to
    // clear: transient group-protocol codes (the group is mid-rebalance or the
    // coordinator is warming up / relocating), and a connection dropped
    // mid-join. The lost-wakeup *hang* the retry loop exists to survive is NOT
    // an error here — it never returns; it is caught by the per-attempt
    // timeout. We deliberately do NOT retry `Connect`/`Timeout`: an unreachable
    // or non-responding broker is a genuine fault that must surface promptly
    // (and is the broker's `depends_on: healthy` to prevent at cold boot), not
    // be masked by a long retry storm.
    matches!(
        error,
        // 14 COORDINATOR_LOAD_IN_PROGRESS, 15 COORDINATOR_NOT_AVAILABLE,
        // 16 NOT_COORDINATOR, 22 ILLEGAL_GENERATION, 25 UNKNOWN_MEMBER_ID,
        // 27 REBALANCE_IN_PROGRESS, 79 MEMBER_ID_REQUIRED.
        ConsumerError::Server(14 | 15 | 16 | 22 | 25 | 27 | 79)
            | ConsumerError::Client(crabka_client_core::ClientError::Disconnected)
    ) || matches!(
        error,
        ConsumerError::StartupAfterJoin(inner)
            if is_retriable_consumer_start_error(inner)
                || matches!(
                    inner.as_ref(),
                    ConsumerError::Client(crabka_client_core::ClientError::Timeout(_))
                )
    )
}

impl Consumer {
    /// The consumer's group id.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The member id assigned by the coordinator at join time.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The current group generation, kept live by the coordinator task across
    /// rejoins (shared `Arc<AtomicI32>`).
    #[must_use]
    pub fn generation_id(&self) -> i32 {
        self.current_generation.load(Ordering::Relaxed)
    }

    /// KIP-447 group metadata to hand to a transactional producer's
    /// `send_offsets_to_transaction`. The generation id is the coordinator's
    /// live generation (kept current across rejoins via the shared
    /// `Arc<AtomicI32>`).
    #[must_use]
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata {
            group_id: self.group_id.clone(),
            generation_id: self.current_generation.load(Ordering::Relaxed),
            member_id: self.member_id.clone(),
            group_instance_id: self.group_instance_id.clone(),
        }
    }

    /// Topics this consumer subscribed to at build time.
    #[must_use]
    pub fn subscribed_topics(&self) -> &[String] {
        &self.subscribed_topics
    }

    /// Snapshot of currently assigned `(topic, partition)` pairs.
    pub async fn assignment(&self) -> Vec<(String, i32)> {
        self.assigned.lock().await.clone()
    }

    /// Stop the coordinator task so the broker evicts this member promptly.
    ///
    /// The coordinator itself sends a best-effort `LeaveGroup` as the last
    /// thing it does on shutdown (see `crate::coordinator::run`), using its
    /// *live* `member_id`. That id can differ from the one captured at build
    /// time — a from-scratch rejoin (`UNKNOWN_MEMBER_ID`) replaces it — so the
    /// leave must come from the coordinator, which owns the current value;
    /// sending it here with `self.member_id` would silently leave a stale id
    /// and orphan the real member until its session expires. Cancel + join is
    /// prompt because the coordinator races its in-tick RPCs against the
    /// shutdown token.
    #[tracing::instrument(
        name = "consumer.close",
        level = "info",
        skip_all,
        fields(group_id = %self.group_id, member_id = %self.member_id),
        err
    )]
    pub async fn close(mut self) -> Result<(), ConsumerError> {
        self.coordinator_shutdown.cancel();
        if let Some(h) = self.coordinator_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod security_arg_tests {
    use assert2::check;
    use crabka_client_core::{
        ClientError, MockBroker,
        security::{ClientSecurity, SaslCredentials},
    };
    use crabka_protocol::{
        Encode, UnknownTaggedFields,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            leave_group_request,
            leave_group_response::{self, LeaveGroupResponse},
        },
    };
    use crabka_security::ListenerProtocol;

    use super::*;
    use crate::builder::DecodedSubscription;

    fn api_versions_for_startup_cleanup() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: leave_group_request::API_KEY,
                    min_version: 0,
                    max_version: leave_group_request::MAX_VERSION,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn leave_group_response_at(version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        if version >= leave_group_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00]);
        }
        LeaveGroupResponse::default()
            .encode(&mut buf, version)
            .unwrap();
        buf.to_vec()
    }

    #[tokio::test]
    async fn startup_member_cleanup_sends_leave_group() {
        let saw_leave = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_leave_in_mock = Arc::clone(&saw_leave);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_for_startup_cleanup())
            } else if api_key == leave_group_request::API_KEY {
                saw_leave_in_mock.store(true, Ordering::SeqCst);
                Some(leave_group_response_at(version))
            } else {
                None
            }
        })
        .await;

        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(Duration::from_millis(100))
            .build()
            .await
            .unwrap();
        let coordinator_id = AtomicI32::new(0);

        leave_startup_member(
            &client,
            &coordinator_id,
            "group-a",
            "member-a",
            Some("instance-a".into()),
        )
        .await;

        mock.stop();
        assert2::assert!(saw_leave.load(Ordering::SeqCst));
    }

    #[test]
    fn initial_subscription_uses_unknown_generation_and_empty_owned_partitions() {
        let bytes = initial_subscription_bytes(&["topic".into()], Some("rack-a"));
        let decoded = decode_subscription(&bytes);

        check!(
            decoded
                == DecodedSubscription {
                    topics: vec!["topic".into()],
                    owned: Vec::new(),
                    generation_id: -1,
                    rack_id: Some("rack-a".into()),
                }
        );
    }

    #[test]
    fn build_join_request_preserves_group_member_timeouts_and_protocol() {
        let metadata = bytes::Bytes::from_static(b"metadata");
        let req = build_join_request(
            "group-a".into(),
            "member-a".into(),
            Some("instance-a".into()),
            "range".into(),
            metadata.clone(),
            45_000,
            60_000,
        );

        assert2::assert!(
            req == JoinGroupRequest {
                group_id: "group-a".into(),
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: 60_000,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                protocol_type: "consumer".into(),
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                reason: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        );
    }

    #[test]
    fn first_join_member_id_accepts_required_or_success_and_rejects_errors() {
        enum Expected<'a> {
            Member(&'a str),
            Server(i16),
            RebalanceFailed,
        }
        for (_name, error_code, member_id, expected) in [
            (
                "member id required",
                79,
                "member-a",
                Expected::Member("member-a"),
            ),
            ("success", 0, "member-b", Expected::Member("member-b")),
            ("server error", 42, "member-c", Expected::Server(42)),
            ("empty member", 0, "", Expected::RebalanceFailed),
        ] {
            let response = JoinGroupResponse {
                error_code,
                member_id: member_id.into(),
                ..Default::default()
            };
            let actual = first_join_member_id(&response);
            let matches = match expected {
                Expected::Member(expected) => matches!(actual, Ok(actual) if actual == expected),
                Expected::Server(expected) => {
                    matches!(actual, Err(ConsumerError::Server(code)) if code == expected)
                }
                Expected::RebalanceFailed => {
                    matches!(actual, Err(ConsumerError::RebalanceFailed(_)))
                }
            };
            assert2::assert!(matches);
        }
    }

    #[test]
    fn is_subscribed_topic_matches_exact_topic_names() {
        let subscribe = vec!["orders".to_string(), "payments".to_string()];

        for (name, expected) in [
            ("orders", true),
            ("payments", true),
            ("shipments", false),
            ("order", false),
        ] {
            assert2::assert!(is_subscribed_topic(&subscribe, name) == expected);
        }
    }

    #[test]
    fn is_group_leader_matches_exact_member_id() {
        for (_name, leader, member_id, expected) in [
            ("exact leader", "member-a", "member-a", true),
            ("different member", "member-a", "member-b", false),
            ("empty member", "member-a", "", false),
        ] {
            assert2::assert!(is_group_leader(leader, member_id) == expected);
        }
    }

    #[test]
    fn build_sync_assignment_preserves_member_and_assignment_payload() {
        let assignment = build_sync_assignment("member-a".into(), &[("orders".into(), 3)]);
        let decoded = decode_assignment(&assignment.assignment);

        assert2::assert!(
            (assignment.member_id.as_str(), decoded) == ("member-a", vec![("orders".into(), 3)])
        );
    }

    #[test]
    fn build_sync_request_preserves_group_generation_member_protocol_and_assignments() {
        let assignment = build_sync_assignment("member-a".into(), &[("orders".into(), 3)]);
        let req = build_sync_request(
            "group-a".into(),
            7,
            "member-a".into(),
            Some("instance-a".into()),
            "range".into(),
            vec![assignment.clone()],
        );

        assert2::assert!(
            req == SyncGroupRequest {
                group_id: "group-a".into(),
                generation_id: 7,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                assignments: vec![assignment],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        );
    }

    #[test]
    fn offset_prime_helpers_preserve_assignment_presence_offsets_and_epochs() {
        for (_name, partitions, expected) in [
            ("empty assignment", vec![], false),
            ("assigned partition", vec![("orders".to_string(), 0)], true),
        ] {
            assert2::assert!(has_assigned_partitions(&partitions) == expected);
        }

        for (_name, committed, reset, expected) in [
            ("committed offset", 12, AutoOffsetReset::Earliest, 12),
            ("missing earliest", -1, AutoOffsetReset::Earliest, 0),
            ("missing latest", -1, AutoOffsetReset::Latest, i64::MAX),
            ("missing none", -1, AutoOffsetReset::None, i64::MAX),
        ] {
            assert2::assert!(starting_offset(committed, reset) == expected);
        }

        let position = primed_position(9);
        assert2::assert!(
            position
                == crate::position::PartitionPosition {
                    offset_epoch: crabka_ids::LeaderEpoch(9),
                    leader_id: -1,
                    leader_epoch: crabka_ids::LeaderEpoch(-1),
                    awaiting_validation: false,
                }
        );
    }

    #[tokio::test]
    async fn consumer_builder_uses_request_timeout_for_initial_connect_handshake() {
        let mock = MockBroker::start(|_api_key, _version, _corr_id, _body| None).await;

        let build = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .client_id("timeout-consumer")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .request_timeout(Duration::from_millis(100))
            .build();
        let res = tokio::time::timeout(Duration::from_millis(500), build).await;

        mock.stop();

        let Err(err) = res.expect("consumer build should not retain the 30s connect timeout")
        else {
            panic!("silent broker must time out during build")
        };
        assert2::assert!(matches!(
            err,
            ConsumerError::Client(ClientError::Timeout(d))
                if d == Duration::from_millis(100)
        ));
    }

    #[tokio::test]
    async fn consumer_builder_rejects_non_positive_fetch_budgets_before_network_io() {
        let max_bytes = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .fetch_max_bytes(0)
            .build()
            .await;
        assert2::assert!(matches!(max_bytes, Err(ConsumerError::RebalanceFailed(_))));

        let partition_max_bytes = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .fetch_partition_max_bytes(0)
            .build()
            .await;
        assert2::assert!(matches!(
            partition_max_bytes,
            Err(ConsumerError::RebalanceFailed(_))
        ));
    }

    async fn test_consumer() -> Consumer {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id("test-client")
            .build()
            .await
            .unwrap();
        Consumer {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(3)),
            member_id: "member-a".into(),
            group_instance_id: Some("instance-a".into()),
            current_generation: Arc::new(AtomicI32::new(7)),
            subscribed_topics: vec!["orders".into(), "payments".into()],
            assigned: Arc::new(Mutex::new(vec![("orders".into(), 0)])),
            next_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            pending_seeks: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: std::time::Duration::from_secs(45),
            heartbeat_interval: std::time::Duration::from_secs(3),
            assignor: Assignor::Range,
            coordinator_shutdown: CancellationToken::new(),
            coordinator_handle: None,
            isolation_level: IsolationLevel::ReadUncommitted,
            fetch_max_bytes: crate::poll::DEFAULT_FETCH_MAX_BYTES,
            fetch_partition_max_bytes: crate::poll::DEFAULT_FETCH_PARTITION_MAX_BYTES,
            auto_offset_reset: AutoOffsetReset::Latest,
        }
    }

    #[tokio::test]
    async fn accessors_return_consumer_identity_subscription_and_assignment() {
        let consumer = test_consumer().await;

        check!(
            (
                consumer.group_id(),
                consumer.member_id(),
                consumer.generation_id(),
                consumer.subscribed_topics(),
                consumer.assignment().await,
            ) == (
                "group-a",
                "member-a",
                7,
                &["orders".to_string(), "payments".to_string()][..],
                vec![("orders".into(), 0)],
            )
        );

        let metadata = consumer.group_metadata();
        assert2::assert!(
            metadata
                == ConsumerGroupMetadata {
                    group_id: "group-a".into(),
                    generation_id: 7,
                    member_id: "member-a".into(),
                    group_instance_id: Some("instance-a".into()),
                }
        );
    }

    /// Regression: the generation the commit path stamps must track the
    /// coordinator's (re)joins, not a start-up snapshot. The coordinator is the
    /// sole writer and publishes via the shared `current_generation` atomic; the
    /// accessor + group-metadata + commit path all read it live, so a commit
    /// issued after a rebalance carries the CURRENT generation instead of the
    /// stale one the broker rejects with `ILLEGAL_GENERATION (22)`.
    #[tokio::test]
    async fn generation_tracks_coordinator_rejoins_via_shared_atomic() {
        let consumer = test_consumer().await;
        assert2::assert!(
            (
                consumer.generation_id(),
                consumer.group_metadata().generation_id
            ) == (7, 7)
        );

        // Simulate the coordinator publishing a new generation on rejoin.
        consumer.current_generation.store(11, Ordering::Relaxed);

        assert2::assert!(
            (
                consumer.generation_id(),
                consumer.group_metadata().generation_id
            ) == (11, 11)
        );
    }

    #[tokio::test]
    async fn close_cancels_shutdown_token_even_without_spawned_handle() {
        let consumer = test_consumer().await;
        let shutdown = consumer.coordinator_shutdown.clone();

        consumer.close().await.unwrap();

        assert2::assert!(shutdown.is_cancelled());
    }

    // --- is_retriable_consumer_start_error ---

    #[test]
    fn retriable_error_classification() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        use crabka_client_core::ClientError;

        // Transient group-protocol server codes.
        let transient_codes: &[i16] = &[14, 15, 16, 22, 25, 27, 79];
        for &code in transient_codes {
            assert2::assert!(is_retriable_consumer_start_error(&ConsumerError::Server(
                code
            )));
        }

        // Non-transient server code (e.g. INVALID_REQUEST = 42).
        assert2::assert!(!is_retriable_consumer_start_error(&ConsumerError::Server(
            42
        )));

        for (_name, error, expected) in [
            // A connection dropped mid-join is transient — retry a fresh attempt.
            (
                "disconnected",
                ConsumerError::Client(ClientError::Disconnected),
                true,
            ),
            // Connect/Timeout are NOT retried: an unreachable or non-responding
            // broker is a genuine fault that must surface promptly (the lost-wakeup
            // hang the retry loop survives never returns Timeout — it is caught by
            // the per-attempt timeout, not classified here).
            (
                "timeout",
                ConsumerError::Client(ClientError::Timeout(Duration::from_secs(1))),
                false,
            ),
            (
                "startup after join",
                ConsumerError::StartupAfterJoin(Box::new(ConsumerError::Client(
                    ClientError::Timeout(Duration::from_secs(1)),
                ))),
                true,
            ),
            (
                "connect refused",
                ConsumerError::Client(ClientError::Connect {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092),
                    source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
                }),
                false,
            ),
            // Permanent misconfig errors — must NOT be retriable.
            ("not subscribed", ConsumerError::NotSubscribed, false),
            (
                "rebalance failed",
                ConsumerError::RebalanceFailed("group_id required".into()),
                false,
            ),
            (
                "incompatible version",
                ConsumerError::Client(ClientError::IncompatibleVersion {
                    api_key: 0,
                    broker_min: 0,
                    broker_max: 5,
                    client_min: 7,
                    client_max: 10,
                }),
                false,
            ),
        ] {
            assert2::assert!(is_retriable_consumer_start_error(&error) == expected);
        }
    }

    #[tokio::test]
    async fn consumer_builder_accepts_security() {
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        // 127.0.0.1:1 is unroutable; the consumer build connects eagerly
        // (JoinGroup), so it must fail — proving the security arg is
        // threaded (not a type error).
        let res = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("g")
            .subscribe(vec!["t".to_string()])
            .security(security)
            .build()
            .await;
        assert2::assert!(res.is_err());
    }
}

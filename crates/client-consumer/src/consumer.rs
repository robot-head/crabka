//! `Consumer` — public lifecycle handle. Built via [`Consumer::builder`].
//! Subscribe-only — no `assign()`. Use `crabka-client-core` directly for
//! manual partition consumption.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::builder::{
    AutoOffsetReset, IsolationLevel, decode_assignment, decode_subscription, encode_assignment,
    encode_subscription,
};
use crate::error::ConsumerError;
use crate::heartbeat::RebalanceNotice;

/// Subscribe-style consumer handle. Construct via [`Consumer::builder`].
#[allow(dead_code)] // `session_timeout` / `heartbeat_interval` are kept for
// future re-join / re-handshake support (slice 5 MVP
// only spawns one heartbeat task at build time).
pub struct Consumer {
    pub(crate) client: Client,
    pub(crate) group_id: String,
    pub(crate) member_id: String,
    pub(crate) generation_id: i32,
    pub(crate) subscribed_topics: Vec<String>,
    /// Current assigned partitions: `(topic, partition_index)`.
    pub(crate) assigned: Arc<Mutex<Vec<(String, i32)>>>,
    /// Next offset to fetch per partition.
    pub(crate) next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// Topic UUIDs resolved at build time. Required by Fetch v ≥ 13
    /// (which carries `topic_id` instead of the topic name).
    pub(crate) topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub(crate) session_timeout: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) rebalance_rx: Mutex<mpsc::Receiver<RebalanceNotice>>,
    pub(crate) heartbeat_shutdown: CancellationToken,
    pub(crate) heartbeat_handle: Option<JoinHandle<()>>,
    /// Controls which records are returned by `poll`.
    pub(crate) isolation_level: IsolationLevel,
}

/// One record returned by `Consumer::poll`.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

#[bon::bon]
impl Consumer {
    /// Build a [`Consumer`] subscribed to the given topics: resolve bootstrap,
    /// `JoinGroup` (twice), compute the range assignment if we're the elected
    /// leader, `SyncGroup`, prime offsets, then spawn the heartbeat task.
    #[builder(start_fn = builder, finish_fn = build)]
    #[allow(clippy::too_many_lines)]
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
        #[builder(default = AutoOffsetReset::Latest)] auto_offset_reset: AutoOffsetReset,
        #[builder(default = IsolationLevel::ReadUncommitted)] isolation_level: IsolationLevel,
    ) -> Result<Self, ConsumerError> {
        if subscribe.is_empty() {
            return Err(ConsumerError::NotSubscribed);
        }
        if group_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed("group_id required".into()));
        }

        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .build()
            .await?;

        let session_timeout_ms = i32::try_from(session_timeout.as_millis()).unwrap_or(i32::MAX);
        let rebalance_timeout_ms = i32::try_from(rebalance_timeout.as_millis()).unwrap_or(i32::MAX);

        // 1. First JoinGroup — empty member_id, expect MEMBER_ID_REQUIRED (79)
        //    or a regular response; either way the broker hands us a member_id.
        let r1 = client
            .send(JoinGroupRequest {
                group_id: group_id.clone(),
                protocol_type: "consumer".into(),
                member_id: String::new(),
                session_timeout_ms,
                rebalance_timeout_ms,
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: encode_subscription(&subscribe),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        let member_id = if r1.error_code == 79 || r1.error_code == 0 {
            r1.member_id.clone()
        } else {
            return Err(ConsumerError::Server(r1.error_code));
        };
        if member_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed(
                "broker did not assign a member_id".into(),
            ));
        }

        // 2. Second JoinGroup with the assigned member_id.
        let r2 = client
            .send(JoinGroupRequest {
                group_id: group_id.clone(),
                protocol_type: "consumer".into(),
                member_id: member_id.clone(),
                session_timeout_ms,
                rebalance_timeout_ms,
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: encode_subscription(&subscribe),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        if r2.error_code != 0 {
            return Err(ConsumerError::Server(r2.error_code));
        }

        // 3. Always issue a Metadata to resolve topic_ids (needed for
        //    Fetch v ≥ 13). If we are the leader, also use the partition
        //    counts to compute the range assignment.
        let md = client.send(MetadataRequest::default()).await?;
        let mut topic_ids: HashMap<String, WireUuid> = HashMap::new();
        let mut topic_partitions: HashMap<String, i32> = HashMap::new();
        for t in &md.topics {
            let Some(name) = &t.name else { continue };
            if subscribe.iter().any(|s| s == name) {
                let count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
                topic_partitions.insert(name.clone(), count);
                topic_ids.insert(name.clone(), t.topic_id);
            }
        }

        let is_leader = r2.leader == member_id;
        let assignments_for_sync: Vec<SyncGroupRequestAssignment> = if is_leader {
            let members: Vec<(String, Vec<String>)> = r2
                .members
                .iter()
                .map(|m| (m.member_id.clone(), decode_subscription(&m.metadata)))
                .collect();
            let assignments = crate::assignor::range::assign(members, &topic_partitions);
            assignments
                .into_iter()
                .map(|(m, partitions)| SyncGroupRequestAssignment {
                    member_id: m,
                    assignment: encode_assignment(&partitions),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };

        // 4. SyncGroup — leader installs assignments; everyone receives
        //    their own assignment in the response.
        let r3 = client
            .send(SyncGroupRequest {
                group_id: group_id.clone(),
                generation_id: r2.generation_id,
                member_id: member_id.clone(),
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                assignments: assignments_for_sync,
                ..Default::default()
            })
            .await?;
        if r3.error_code != 0 {
            return Err(ConsumerError::Server(r3.error_code));
        }
        let assigned_partitions = decode_assignment(&r3.assignment);

        // 5. Fetch existing committed offsets so poll() resumes correctly.
        let mut next_offsets: HashMap<(String, i32), i64> = HashMap::new();
        if !assigned_partitions.is_empty() {
            let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
            for (t, p) in &assigned_partitions {
                by_topic.entry(t.clone()).or_default().push(*p);
            }
            let topics: Vec<OffsetFetchRequestTopic> = by_topic
                .into_iter()
                .map(|(name, partition_indexes)| OffsetFetchRequestTopic {
                    name,
                    partition_indexes,
                    ..Default::default()
                })
                .collect();
            let of = client
                .send(OffsetFetchRequest {
                    group_id: group_id.clone(),
                    topics: Some(topics),
                    ..Default::default()
                })
                .await?;
            for t in &of.topics {
                for p in &t.partitions {
                    let committed = p.committed_offset;
                    let starting = if committed >= 0 {
                        committed
                    } else {
                        match auto_offset_reset {
                            AutoOffsetReset::Earliest => 0,
                            // Resolved by poll() on first call.
                            AutoOffsetReset::Latest => i64::MAX,
                        }
                    };
                    next_offsets.insert((t.name.clone(), p.partition_index), starting);
                }
            }
        }

        // 6. Spawn the heartbeat task.
        let (notice_tx, notice_rx) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let hb_handle = tokio::spawn(crate::heartbeat::run(
            client.clone(),
            group_id.clone(),
            member_id.clone(),
            r2.generation_id,
            heartbeat_interval,
            notice_tx,
            shutdown.clone(),
        ));

        Ok(Consumer {
            client,
            group_id,
            member_id,
            generation_id: r2.generation_id,
            subscribed_topics: subscribe,
            assigned: Arc::new(Mutex::new(assigned_partitions)),
            next_offsets: Arc::new(Mutex::new(next_offsets)),
            topic_ids: Arc::new(Mutex::new(topic_ids)),
            session_timeout,
            heartbeat_interval,
            rebalance_rx: Mutex::new(notice_rx),
            heartbeat_shutdown: shutdown,
            heartbeat_handle: Some(hb_handle),
            isolation_level,
        })
    }
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

    /// The generation id captured at the most recent successful join.
    #[must_use]
    pub fn generation_id(&self) -> i32 {
        self.generation_id
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

    /// Stop the heartbeat task. Returns immediately if already shut down.
    pub async fn close(mut self) -> Result<(), ConsumerError> {
        self.heartbeat_shutdown.cancel();
        if let Some(h) = self.heartbeat_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}

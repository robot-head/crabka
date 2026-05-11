//! `ConsumerBuilder` — runs the `JoinGroup` → `SyncGroup` handshake,
//! computes the initial range assignment on the leader, primes
//! per-partition next offsets from `OffsetFetch`, and spawns the
//! heartbeat task.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};

use crate::assignor::range;
use crate::consumer::Consumer;
use crate::error::ConsumerError;
use crate::heartbeat;

/// What to do when a partition has no committed offset.
#[derive(Debug, Clone, Copy)]
pub enum AutoOffsetReset {
    /// Start from offset 0.
    Earliest,
    /// Start from the log-end offset. Resolved lazily by `Consumer::poll`
    /// using `ListOffsets(timestamp=-1)`.
    Latest,
}

/// Builder for [`Consumer`].
pub struct ConsumerBuilder {
    bootstrap: String,
    client_id: String,
    group_id: String,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    heartbeat_interval: Duration,
    topics: Vec<String>,
    auto_offset_reset: AutoOffsetReset,
}

impl ConsumerBuilder {
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            client_id: "crabka-consumer".into(),
            group_id: String::new(),
            session_timeout: Duration::from_secs(45),
            rebalance_timeout: Duration::from_mins(1),
            heartbeat_interval: Duration::from_secs(3),
            topics: Vec::new(),
            auto_offset_reset: AutoOffsetReset::Latest,
        }
    }

    #[must_use]
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    #[must_use]
    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.group_id = id.into();
        self
    }

    #[must_use]
    pub fn session_timeout(mut self, t: Duration) -> Self {
        self.session_timeout = t;
        self
    }

    #[must_use]
    pub fn rebalance_timeout(mut self, t: Duration) -> Self {
        self.rebalance_timeout = t;
        self
    }

    #[must_use]
    pub fn heartbeat_interval(mut self, t: Duration) -> Self {
        self.heartbeat_interval = t;
        self
    }

    #[must_use]
    pub fn subscribe(mut self, topics: &[&str]) -> Self {
        self.topics = topics.iter().map(|s| (*s).to_string()).collect();
        self
    }

    #[must_use]
    pub fn auto_offset_reset(mut self, x: AutoOffsetReset) -> Self {
        self.auto_offset_reset = x;
        self
    }

    /// Build the [`Consumer`]: resolve bootstrap, `JoinGroup` (twice — the
    /// first hop is to obtain a `member_id`), compute the range assignment
    /// if we're the elected leader, `SyncGroup`, prime offsets, then spawn
    /// the heartbeat task.
    #[allow(clippy::too_many_lines)]
    pub async fn build(self) -> Result<Consumer, ConsumerError> {
        if self.topics.is_empty() {
            return Err(ConsumerError::NotSubscribed);
        }
        if self.group_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed("group_id required".into()));
        }

        let client = Client::builder(&self.bootstrap)
            .client_id(self.client_id.clone())
            .build()
            .await?;

        let session_timeout_ms =
            i32::try_from(self.session_timeout.as_millis()).unwrap_or(i32::MAX);
        let rebalance_timeout_ms =
            i32::try_from(self.rebalance_timeout.as_millis()).unwrap_or(i32::MAX);

        // 1. First JoinGroup — empty member_id, expect MEMBER_ID_REQUIRED (79)
        //    or a regular response; either way the broker hands us a member_id.
        let r1 = client
            .send(JoinGroupRequest {
                group_id: self.group_id.clone(),
                protocol_type: "consumer".into(),
                member_id: String::new(),
                session_timeout_ms,
                rebalance_timeout_ms,
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: encode_subscription(&self.topics),
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
                group_id: self.group_id.clone(),
                protocol_type: "consumer".into(),
                member_id: member_id.clone(),
                session_timeout_ms,
                rebalance_timeout_ms,
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: encode_subscription(&self.topics),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        if r2.error_code != 0 {
            return Err(ConsumerError::Server(r2.error_code));
        }

        // 3. If we are the leader, compute the assignment via the range
        //    assignor. Otherwise SyncGroup with an empty assignments list.
        let is_leader = r2.leader == member_id;
        let assignments_for_sync: Vec<SyncGroupRequestAssignment> = if is_leader {
            let md = client.send(MetadataRequest::default()).await?;
            let mut topic_partitions: HashMap<String, i32> = HashMap::new();
            for t in &md.topics {
                let Some(name) = &t.name else { continue };
                if self.topics.iter().any(|s| s == name) {
                    let count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
                    topic_partitions.insert(name.clone(), count);
                }
            }
            let members: Vec<(String, Vec<String>)> = r2
                .members
                .iter()
                .map(|m| (m.member_id.clone(), decode_subscription(&m.metadata)))
                .collect();
            let assignments = range::assign(members, &topic_partitions);
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
                group_id: self.group_id.clone(),
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
                    group_id: self.group_id.clone(),
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
                        match self.auto_offset_reset {
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
        let hb_handle = tokio::spawn(heartbeat::run(
            client.clone(),
            self.group_id.clone(),
            member_id.clone(),
            r2.generation_id,
            self.heartbeat_interval,
            notice_tx,
            shutdown.clone(),
        ));

        Ok(Consumer {
            client,
            group_id: self.group_id,
            member_id,
            generation_id: r2.generation_id,
            subscribed_topics: self.topics,
            assigned: Arc::new(Mutex::new(assigned_partitions)),
            next_offsets: Arc::new(Mutex::new(next_offsets)),
            session_timeout: self.session_timeout,
            heartbeat_interval: self.heartbeat_interval,
            rebalance_rx: Mutex::new(notice_rx),
            heartbeat_shutdown: shutdown,
            heartbeat_handle: Some(hb_handle),
        })
    }
}

// ── subscription / assignment codec (ConsumerProtocol v1) ─────────────────

/// Encode a `ConsumerProtocolSubscription` v1 record:
/// version (i16=1) + topics (array<STRING>) + `user_data` (BYTES=-1).
pub(crate) fn encode_subscription(topics: &[String]) -> Bytes {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    let n = i32::try_from(topics.len()).expect("topics fit in i32");
    buf.put_i32(n);
    for t in topics {
        let len = i16::try_from(t.len()).expect("topic name fits in i16");
        buf.put_i16(len);
        buf.put_slice(t.as_bytes());
    }
    buf.put_i32(-1); // user_data null
    buf.freeze()
}

pub(crate) fn decode_subscription(bytes: &[u8]) -> Vec<String> {
    use bytes::Buf;
    let mut cur = bytes;
    if cur.remaining() < 2 {
        return Vec::new();
    }
    let _version = cur.get_i16();
    if cur.remaining() < 4 {
        return Vec::new();
    }
    let n = cur.get_i32();
    let cap = usize::try_from(n.max(0)).unwrap_or(0);
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        if cur.remaining() < 2 {
            break;
        }
        let len = cur.get_i16();
        let len = usize::try_from(len.max(0)).unwrap_or(0);
        if cur.remaining() < len {
            break;
        }
        let mut s = vec![0u8; len];
        cur.copy_to_slice(&mut s);
        if let Ok(s) = String::from_utf8(s) {
            out.push(s);
        }
    }
    out
}

/// Encode a `ConsumerProtocolAssignment` v1:
/// version (i16=1) + `assigned_partitions` (array<{topic, partitions: array<i32>}>)
/// + `user_data` (BYTES=-1).
pub(crate) fn encode_assignment(partitions: &[(String, i32)]) -> Bytes {
    use bytes::BufMut;
    let mut by_topic: std::collections::BTreeMap<&str, Vec<i32>> =
        std::collections::BTreeMap::new();
    for (t, p) in partitions {
        by_topic.entry(t.as_str()).or_default().push(*p);
    }
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    let n = i32::try_from(by_topic.len()).expect("topics fit in i32");
    buf.put_i32(n);
    for (topic, parts) in by_topic {
        let len = i16::try_from(topic.len()).expect("topic name fits in i16");
        buf.put_i16(len);
        buf.put_slice(topic.as_bytes());
        let pn = i32::try_from(parts.len()).expect("partition count fits in i32");
        buf.put_i32(pn);
        for p in parts {
            buf.put_i32(p);
        }
    }
    buf.put_i32(-1);
    buf.freeze()
}

pub(crate) fn decode_assignment(bytes: &[u8]) -> Vec<(String, i32)> {
    use bytes::Buf;
    let mut cur = bytes;
    if cur.remaining() < 2 {
        return Vec::new();
    }
    let _version = cur.get_i16();
    if cur.remaining() < 4 {
        return Vec::new();
    }
    let topic_count = cur.get_i32();
    let mut out = Vec::new();
    for _ in 0..topic_count.max(0) {
        if cur.remaining() < 2 {
            break;
        }
        let len = cur.get_i16();
        let len = usize::try_from(len.max(0)).unwrap_or(0);
        if cur.remaining() < len {
            break;
        }
        let mut name = vec![0u8; len];
        cur.copy_to_slice(&mut name);
        let Ok(topic) = String::from_utf8(name) else {
            break;
        };
        if cur.remaining() < 4 {
            break;
        }
        let pcount = cur.get_i32();
        for _ in 0..pcount.max(0) {
            if cur.remaining() < 4 {
                break;
            }
            out.push((topic.clone(), cur.get_i32()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_round_trip() {
        let s = encode_subscription(&["t1".into(), "t2".into()]);
        let decoded = decode_subscription(&s);
        assert_eq!(decoded, vec!["t1", "t2"]);
    }

    #[test]
    fn subscription_empty_round_trip() {
        let s = encode_subscription(&[]);
        let decoded = decode_subscription(&s);
        assert!(decoded.is_empty());
    }

    #[test]
    fn assignment_round_trip() {
        let s = encode_assignment(&[
            ("t".into(), 0),
            ("t".into(), 1),
            ("u".into(), 0),
        ]);
        let decoded = decode_assignment(&s);
        assert!(decoded.contains(&("t".into(), 0)));
        assert!(decoded.contains(&("t".into(), 1)));
        assert!(decoded.contains(&("u".into(), 0)));
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn assignment_empty_round_trip() {
        let s = encode_assignment(&[]);
        let decoded = decode_assignment(&s);
        assert!(decoded.is_empty());
    }
}

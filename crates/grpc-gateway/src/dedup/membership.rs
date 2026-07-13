//! Gateway membership + owner-routing for active-active forwarding.
//!
//! Each replica publishes `{advertised_addr, owned, epoch}` (keyed by a
//! per-process `node_id`) to the compacted, single-partition membership topic
//! on every dedup-assignment change. Every replica tails the whole topic — a
//! unique consumer group per process ⇒ it is the sole member ⇒ assigned all
//! partitions ⇒ a broadcast read — into a `dedup_partition → owner_addr`
//! routing table. A crashed node's stale ownership record cannot shadow the
//! live owner: the table breaks ties by record offset, and the topic's single
//! partition makes those offsets a total order, so the most-recent claim wins.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use serde::{Deserialize, Serialize};

use crate::error::GatewayError;

/// One replica's published membership (value; key = `node_id`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub advertised_addr: String,
    pub owned: Vec<u32>,
    pub epoch: u64,
}

struct NodeEntry {
    info: NodeInfo,
    /// Membership-topic offset of this node's latest record (recency tiebreak).
    offset: i64,
}

/// Materialized membership + the derived `partition → owner_addr` routing table.
pub struct MembershipStore {
    nodes: RwLock<HashMap<String, NodeEntry>>,
    routing: RwLock<HashMap<u32, String>>,
}

impl MembershipStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            routing: RwLock::new(HashMap::new()),
        }
    }

    /// Owner advertised-addr for dedup-partition `p`, if any replica claims it.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn owner_of(&self, p: u32) -> Option<String> {
        self.routing.read().expect("routing lock").get(&p).cloned()
    }

    fn apply(&self, node_id: String, info: Option<NodeInfo>, offset: i64) {
        {
            let mut nodes = self.nodes.write().expect("nodes lock");
            match info {
                Some(info) => {
                    nodes.insert(node_id, NodeEntry { info, offset });
                }
                None => {
                    nodes.remove(&node_id);
                }
            }
        }
        self.rebuild();
    }

    /// Rebuild `partition → owner_addr`: for each partition, the claimant whose
    /// record has the highest offset (most recent publish) wins.
    fn rebuild(&self) {
        let nodes = self.nodes.read().expect("nodes lock");
        let mut best: HashMap<u32, (i64, String)> = HashMap::new();
        for entry in nodes.values() {
            for &p in &entry.info.owned {
                let slot = best.entry(p).or_insert((i64::MIN, String::new()));
                if entry.offset >= slot.0 {
                    *slot = (entry.offset, entry.info.advertised_addr.clone());
                }
            }
        }
        *self.routing.write().expect("routing lock") =
            best.into_iter().map(|(p, (_, addr))| (p, addr)).collect();
    }

    /// Tail the membership topic into the routing table until `shutdown`.
    /// `group` MUST be unique per process (node-scoped) so this replica is the
    /// sole member and is assigned every partition (a broadcast read). Closes
    /// the consumer on exit so the coordinator + group member don't leak.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn run_membership(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        membership_topic: String,
        group: String,
        shutdown: tokio_util::sync::CancellationToken,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<(), GatewayError> {
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .group_id(group)
            .subscribe(vec![membership_topic])
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .assignor(crabka_client_consumer::Assignor::CooperativeSticky)
            .maybe_security(security)
            .build()
            .await?;

        let mut poll_err: Option<GatewayError> = None;
        loop {
            let batch = tokio::select! {
                () = shutdown.cancelled() => break,
                b = consumer.poll(Duration::from_millis(500)) => match b {
                    Ok(batch) => batch,
                    Err(e) => { poll_err = Some(e.into()); break; }
                },
            };
            for r in batch {
                let Some(key_bytes) = r.key else { continue };
                let node_id = String::from_utf8_lossy(&key_bytes).into_owned();
                match r.value {
                    None => self.apply(node_id, None, r.offset),
                    // Skip malformed records; never kill the loop.
                    Some(v) => {
                        if let Ok(info) = serde_json::from_slice::<NodeInfo>(&v) {
                            self.apply(node_id, Some(info), r.offset);
                        }
                    }
                }
            }
        }

        let _ = consumer.close().await;
        match poll_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Default for MembershipStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Publishes this replica's membership on each dedup-assignment change.
pub struct MembershipPublisher {
    producer: Producer,
    node_id: String,
    advertised_addr: String,
    membership_topic: String,
    epoch: AtomicU64,
}

impl MembershipPublisher {
    /// Build the publisher's idempotent producer.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn new(
        bootstrap: &str,
        client_id: &str,
        node_id: String,
        advertised_addr: String,
        membership_topic: String,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .maybe_security(security)
            .build()
            .await?;
        Ok(Self {
            producer,
            node_id,
            advertised_addr,
            membership_topic,
            epoch: AtomicU64::new(0),
        })
    }

    /// Publish the current owned set (bumps `epoch`). Keyed by `node_id` so the
    /// compacted topic keeps exactly one live record per replica.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn publish(&self, owned: &HashSet<u32>) -> Result<(), GatewayError> {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst);
        let mut owned: Vec<u32> = owned.iter().copied().collect();
        owned.sort_unstable();
        let info = NodeInfo {
            advertised_addr: self.advertised_addr.clone(),
            owned,
            epoch,
        };
        let rec = ProducerRecord {
            topic: self.membership_topic.clone(),
            partition: None,
            key: Some(Bytes::from(self.node_id.clone().into_bytes())),
            value: Some(Bytes::from(serde_json::to_vec(&info)?)),
            headers: vec![],
            timestamp_ms: None,
        };
        self.producer
            .send(rec)
            .await
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;
        Ok(())
    }
}

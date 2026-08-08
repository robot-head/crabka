//! Gateway membership and owner-routing for active-active forwarding.
//!
//! On every dedup-assignment change, each replica publishes
//! `{advertised_addr, owned, epoch}` to the compacted, single-partition
//! membership topic, keyed by a per-process `node_id`. Every replica tails the
//! whole topic into a `dedup_partition → owner_addr` routing table. Each
//! process uses a unique consumer group ⇒ it is the sole member ⇒ it is
//! assigned all partitions ⇒ the read is a broadcast.
//!
//! A crashed node's stale ownership record cannot shadow the live owner. The
//! table breaks ties by record offset, and the topic's single partition makes
//! those offsets a total order, so the most-recent claim wins.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_units::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{config::GatewayRuntimeConfig, error::GatewayError};

/// One replica's published membership. It is the record value, and the key is
/// `node_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub advertised_addr: String,
    pub owned: Vec<u32>,
    pub epoch: u64,
}

struct NodeEntry {
    info: NodeInfo,
    /// Membership-topic offset of this node's latest record. The table uses it
    /// as the recency tiebreak.
    offset: i64,
}

/// Materialized membership and the derived `partition → owner_addr` routing
/// table.
pub struct MembershipStore {
    nodes: RwLock<HashMap<String, NodeEntry>>,
    routing: RwLock<HashMap<u32, String>>,
    poll_timeout: Time,
}

impl MembershipStore {
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_policy(&GatewayRuntimeConfig::default())
    }

    #[must_use]
    pub fn new_with_policy(runtime: &GatewayRuntimeConfig) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            routing: RwLock::new(HashMap::new()),
            poll_timeout: runtime.consumer_poll_timeout,
        }
    }

    /// The owner's advertised address for dedup-partition `p`, if a replica
    /// claims it.
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

    /// Rebuild `partition → owner_addr`. For each partition, the claimant whose
    /// record has the highest offset wins, because that is the most recent
    /// publish.
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
    ///
    /// `group` MUST be unique per process, that is, node-scoped. This replica
    /// is then the sole member and is assigned every partition, which makes the
    /// read a broadcast. The task closes the consumer on exit, so the
    /// coordinator and the group member do not leak.
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
        self.run_membership_with_policy(
            bootstrap,
            client_id,
            membership_topic,
            group,
            shutdown,
            (security, crate::config::GatewayRuntimeConfig::default()),
        )
        .await
    }

    /// Tail membership with the deployment's client resource policy.
    /// # Errors
    /// Returns an error when consuming fails.
    pub async fn run_membership_with_policy(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        membership_topic: String,
        group: String,
        shutdown: tokio_util::sync::CancellationToken,
        client_policy: (
            Option<crabka_client_core::security::ClientSecurity>,
            crate::config::GatewayRuntimeConfig,
        ),
    ) -> Result<(), GatewayError> {
        let (security, policy) = client_policy;
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .dispatch_queue_capacity(policy.client_dispatch_queue_capacity.get())
            .frame_max(policy.client_frame_max.size())
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
                b = consumer.poll(self.poll_timeout) => match b {
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

/// Publisher of this replica's membership on each dedup-assignment change.
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
        Self::new_with_policy(
            bootstrap,
            client_id,
            node_id,
            advertised_addr,
            membership_topic,
            security,
            &crate::config::GatewayRuntimeConfig::default(),
        )
        .await
    }

    /// Build the publisher with the deployment's client resource policy.
    /// # Errors
    /// Returns an error when client construction fails.
    pub async fn new_with_policy(
        bootstrap: &str,
        client_id: &str,
        node_id: String,
        advertised_addr: String,
        membership_topic: String,
        security: Option<crabka_client_core::security::ClientSecurity>,
        policy: &crate::config::GatewayRuntimeConfig,
    ) -> Result<Self, GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .dispatch_queue_capacity(policy.client_dispatch_queue_capacity.get())
            .frame_max(policy.client_frame_max.size())
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

    /// Publish the current owned set. This bumps `epoch`. The record is keyed
    /// by `node_id`, so the compacted topic keeps exactly one live record per
    /// replica.
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

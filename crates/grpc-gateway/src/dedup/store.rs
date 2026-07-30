//! Materialized view of the compacted dedup-claim topic. The ownership
//! consumer (`run_ownership`) joins the owners consumer group, tracks the
//! assigned dedup partitions, and keeps the claim map warm. P3 gates every
//! produce on ownership + warmth so only the owning replica may write.

use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::{
    config::GatewayRuntimeConfig,
    error::GatewayError,
    ids::{Offset, PartitionIndex},
};

/// The value stored under each `idempotency_key` claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimValue {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
}

pub struct DedupStore {
    map: DashMap<String, ClaimValue>,
    partitions: u32,
    /// Dedup-partition ids this replica currently owns (consumer-group assignment).
    owned: std::sync::RwLock<std::collections::HashSet<u32>>,
    /// Caught up reading owned partitions since the last assignment change.
    warm: AtomicBool,
    /// Has been warm at least once (drives /readyz).
    warmed_once: AtomicBool,
    /// Optional membership publisher; set by the binary before `run_ownership`
    /// starts. `None` in single-owner/unit contexts means no publishing.
    membership: OnceLock<Arc<crate::dedup::membership::MembershipPublisher>>,
    poll_timeout: Duration,
    warmup_empty_polls: u32,
}

impl DedupStore {
    #[must_use]
    pub fn new(partitions: u32) -> Self {
        Self::new_with_policy(partitions, &GatewayRuntimeConfig::default())
    }

    #[must_use]
    pub fn new_with_policy(partitions: u32, runtime: &GatewayRuntimeConfig) -> Self {
        assert2::assert!(partitions > 0);
        assert2::assert!(i32::try_from(partitions).is_ok());
        Self {
            map: DashMap::new(),
            partitions,
            owned: std::sync::RwLock::new(std::collections::HashSet::new()),
            warm: AtomicBool::new(false),
            warmed_once: AtomicBool::new(false),
            membership: OnceLock::new(),
            poll_timeout: Duration::from_millis(runtime.consumer_poll_timeout_ms),
            warmup_empty_polls: runtime.ownership_warmup_empty_polls,
        }
    }

    /// Install the membership publisher. Call before spawning `run_ownership`
    /// so the first assignment is published.
    pub fn set_membership(&self, publisher: Arc<crate::dedup::membership::MembershipPublisher>) {
        let _ = self.membership.set(publisher);
    }

    /// True if dedup-partition `p` is currently owned by this replica.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn owns(&self, p: u32) -> bool {
        self.owned.read().expect("owned lock").contains(&p)
    }

    /// True once caught up on owned partitions since the last assignment change.
    #[must_use]
    pub fn is_warm(&self) -> bool {
        self.warm.load(Ordering::SeqCst)
    }

    /// Has warmed at least once (readiness probe).
    #[must_use]
    pub fn has_warmed_once(&self) -> bool {
        self.warmed_once.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<ClaimValue> {
        self.map.get(key).map(|v| v.clone())
    }

    /// Apply a claim to the in-memory map (called locally after a commit).
    pub fn apply(&self, key: String, value: ClaimValue) {
        self.map.insert(key, value);
    }

    /// Run the ownership consumer until `shutdown` fires. Joins the owners group
    /// on the dedup topic; its assignment is the owned-partition set. Reads owned
    /// partitions from earliest (never commits) to (re)build the claim map,
    /// re-arming the warm gate on each assignment change. Closes the consumer on
    /// exit so the coordinator task + group member don't leak.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn run_ownership(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        dedup_topic: String,
        group: String,
        shutdown: tokio_util::sync::CancellationToken,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<(), GatewayError> {
        self.run_ownership_with_policy(
            bootstrap,
            client_id,
            dedup_topic,
            group,
            shutdown,
            (security, crate::config::GatewayRuntimeConfig::default()),
        )
        .await
    }

    /// Run ownership with the deployment's client resource policy.
    /// # Errors
    /// Returns an error when consuming fails.
    /// # Panics
    /// Panics if synchronized ownership state is poisoned.
    pub async fn run_ownership_with_policy(
        self: Arc<Self>,
        bootstrap: String,
        client_id: String,
        dedup_topic: String,
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
            .subscribe(vec![dedup_topic.clone()])
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .assignor(crabka_client_consumer::Assignor::CooperativeSticky)
            .maybe_security(security)
            .build()
            .await?;

        let mut current: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut empty_polls = 0u32;
        let mut poll_err: Option<GatewayError> = None;

        loop {
            let batch = tokio::select! {
                () = shutdown.cancelled() => break,
                b = consumer.poll(self.poll_timeout) => match b {
                    Ok(batch) => batch,
                    Err(e) => { poll_err = Some(e.into()); break; }
                },
            };

            let assigned: std::collections::HashSet<u32> = consumer
                .assignment()
                .await
                .into_iter()
                .filter(|(t, _)| *t == dedup_topic)
                .filter_map(|(_, p)| u32::try_from(p).ok())
                .collect();
            if assigned != current {
                let revoked: std::collections::HashSet<u32> =
                    current.difference(&assigned).copied().collect();
                if !revoked.is_empty() {
                    self.map.retain(|k, _| {
                        !revoked.contains(&crate::dedup::partition_for(k, self.partitions))
                    });
                }
                current.clone_from(&assigned);
                *self.owned.write().expect("owned lock") = assigned;
                self.warm.store(false, Ordering::SeqCst);
                empty_polls = 0;
                crate::metrics::metrics()
                    .set_owned_partitions(i64::try_from(current.len()).expect("count fits i64"));
                if let Some(publisher) = self.membership.get()
                    && let Err(e) = publisher.publish(&current).await
                {
                    tracing::warn!(error = %e, "membership publish failed");
                }
            }

            // Warm heuristic: the configured empty-poll count since the last
            // assignment change ⇒ owned partitions drained to the tail, safe to
            // serve. Assumes a low-traffic, bursty claim topic (it is: tiny
            // compacted claims that replay then idle); a continuously-saturated
            // owned partition would defer warmth until it next idles. A future
            // HWM-precise gate (spec §2) removes that theoretical caveat.
            if batch.is_empty() {
                empty_polls = empty_polls.saturating_add(1);
                if ownership_is_warm(empty_polls, self.warmup_empty_polls) {
                    self.warm.store(true, Ordering::SeqCst);
                    self.warmed_once.store(true, Ordering::SeqCst);
                }
                continue;
            }
            empty_polls = 0;
            for r in batch {
                let Some(key_bytes) = r.key else { continue };
                let key = String::from_utf8_lossy(&key_bytes).into_owned();
                match r.value {
                    None => {
                        self.map.remove(&key);
                    }
                    // A malformed claim must not kill the ownership loop; skip it.
                    Some(v) => {
                        if let Ok(claim) = serde_json::from_slice::<ClaimValue>(&v) {
                            self.map.insert(key, claim);
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

    /// Test/helper writer: produce a single claim record (compacted topic key
    /// = idempotency key, value = JSON `ClaimValue`) to its hashed partition.
    /// # Panics
    /// Panics if the validated partition count cannot be represented by Kafka.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn write_claim(
        &self,
        bootstrap: &str,
        client_id: &str,
        dedup_topic: &str,
        key: &str,
        value: &ClaimValue,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<(), GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .maybe_security(security)
            .build()
            .await?;
        let partition = i32::try_from(crate::dedup::partition_for(key, self.partitions))
            .expect("validated partition fits i32");
        let prec = ProducerRecord {
            topic: dedup_topic.to_string(),
            partition: Some(partition),
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value: Some(Bytes::from(serde_json::to_vec(value)?)),
            headers: vec![],
            timestamp_ms: None,
        };
        let meta = producer
            .send(prec)
            .await
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?;
        meta.map_err(GatewayError::Producer)?;
        self.apply(key.to_string(), value.clone());
        Ok(())
    }
}

#[must_use]
fn ownership_is_warm(empty_polls: u32, warmup_empty_polls: u32) -> bool {
    empty_polls >= warmup_empty_polls
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::ownership_is_warm;

    #[test]
    fn ownership_warmup_uses_configured_empty_poll_threshold() {
        assert!(!ownership_is_warm(2, 3));
        assert!(ownership_is_warm(3, 3));
    }
}

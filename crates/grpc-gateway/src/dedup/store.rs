//! Materialized view of the compacted dedup-claim topic. In single-owner
//! P2, the owner updates the map locally on each commit AND rebuilds it from
//! the topic at startup (`warm_up`) for crash recovery. P3 replaces the
//! local update with a continuous `read_committed` tail across owners.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Acks, Producer, ProducerRecord};

use crate::error::GatewayError;

/// The value stored under each `idempotency_key` claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimValue {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

pub struct DedupStore {
    map: DashMap<String, ClaimValue>,
    partitions: u32,
    ready: AtomicBool,
}

impl DedupStore {
    #[must_use]
    pub fn new(partitions: u32) -> Self {
        Self {
            map: DashMap::new(),
            partitions,
            ready: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<ClaimValue> {
        self.map.get(key).map(|v| v.clone())
    }

    /// Apply a claim to the in-memory map (called locally after a commit).
    pub fn apply(&self, key: String, value: ClaimValue) {
        self.map.insert(key, value);
    }

    /// Rebuild the map from the compacted topic, then mark ready. Reads with
    /// `read_committed` from earliest until caught up (two consecutive empty
    /// polls). Single-member unique group ⇒ all partitions assigned.
    pub async fn warm_up(
        self: &Arc<Self>,
        bootstrap: &str,
        client_id: &str,
        dedup_topic: &str,
    ) -> Result<(), GatewayError> {
        let group = format!("{client_id}-{}", Uuid::new_v4());
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .group_id(group)
            .subscribe(vec![dedup_topic.to_string()])
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await?;

        let mut empty_polls = 0;
        while empty_polls < 2 {
            let batch = consumer.poll(Duration::from_millis(500)).await?;
            if batch.is_empty() {
                empty_polls += 1;
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
                    Some(v) => {
                        let claim: ClaimValue = serde_json::from_slice(&v)?;
                        self.map.insert(key, claim);
                    }
                }
            }
        }
        self.ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Test/helper writer: produce a single claim record (compacted topic key
    /// = idempotency key, value = JSON `ClaimValue`) to its hashed partition.
    pub async fn write_claim(
        &self,
        bootstrap: &str,
        client_id: &str,
        dedup_topic: &str,
        key: &str,
        value: &ClaimValue,
    ) -> Result<(), GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .build()
            .await?;
        let partition =
            i32::try_from(crate::dedup::partition_for(key, self.partitions)).unwrap_or(0);
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

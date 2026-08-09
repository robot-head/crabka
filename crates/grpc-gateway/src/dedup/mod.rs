//! Single-owner exactly-once dedup engine.

pub mod membership;
pub mod store;
pub mod topic;

/// Deterministic FNV-1a-64 over the key, modulo the partition count.
///
/// The result is stable across processes and restarts, unlike
/// `DefaultHasher`'s per-run state, so a given key always maps to the same
/// dedup partition.
///
/// # Panics
///
/// Panics when `partitions` is zero. Process configuration validates this
/// invariant before constructing the engine.
#[must_use]
pub fn partition_for(key: &str, partitions: u32) -> u32 {
    assert2::assert!(partitions > 0);
    assert2::assert!(i32::try_from(partitions).is_ok());
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // `hash % partitions` is < `partitions` (a u32), so it always fits in u32.
    u32::try_from(hash % u64::from(partitions)).expect("partition modulo fits in u32")
}

use std::sync::Arc;

use bytes::Bytes;
use crabka_client_producer::{Acks, Producer, ProducerRecord, RecordMetadata};
use tokio::sync::Mutex;

use self::store::{ClaimValue, DedupStore};
use crate::{
    error::GatewayError,
    ids::{Offset, PartitionIndex},
    produce::to_producer_record,
    types::{GatewayRecord, RecordOutcome},
};

/// A lazily-initialized transactional producer pinned to one dedup partition.
///
/// Only one transaction can be in flight at a time, so the `Mutex` serializes
/// that partition's record and claim transactions.
type TxnSlot = Mutex<Option<Producer>>;

pub struct DedupEngine {
    bootstrap: String,
    client_id: String,
    txn_id_prefix: String,
    dedup_topic: String,
    partitions: u32,
    slots: Vec<TxnSlot>,
    store: Arc<DedupStore>,
    security: Option<crabka_client_core::security::ClientSecurity>,
    dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: crabka_client_core::ClientFrameMax,
}

impl DedupEngine {
    /// Construct a dedup engine for a validated non-zero partition count.
    ///
    /// # Panics
    ///
    /// Panics when `partitions` is zero.
    #[must_use]
    pub fn new(
        bootstrap: &str,
        client_id: &str,
        txn_id_prefix: &str,
        dedup_topic: String,
        partitions: u32,
        store: Arc<DedupStore>,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Self {
        Self::new_with_policy(
            bootstrap,
            client_id,
            txn_id_prefix,
            dedup_topic,
            partitions,
            store,
            (security, &crate::config::GatewayRuntimeConfig::default()),
        )
    }

    /// Construct with the deployment's client resource policy.
    #[must_use]
    pub fn new_with_policy(
        bootstrap: &str,
        client_id: &str,
        txn_id_prefix: &str,
        dedup_topic: String,
        partitions: u32,
        store: Arc<DedupStore>,
        client_policy: (
            Option<crabka_client_core::security::ClientSecurity>,
            &crate::config::GatewayRuntimeConfig,
        ),
    ) -> Self {
        let (security, policy) = client_policy;
        assert2::assert!(partitions > 0);
        assert2::assert!(i32::try_from(partitions).is_ok());
        let slots = (0..partitions).map(|_| Mutex::new(None)).collect();
        Self {
            bootstrap: bootstrap.to_string(),
            client_id: client_id.to_string(),
            txn_id_prefix: txn_id_prefix.to_string(),
            dedup_topic,
            partitions,
            slots,
            store,
            security,
            dispatch_queue_capacity: policy.client_dispatch_queue_capacity,
            frame_max: policy.client_frame_max,
        }
    }

    /// The dedup partition a key hashes to. Routing decisions use it.
    #[must_use]
    pub fn partition_for_key(&self, key: &str) -> u32 {
        partition_for(key, self.partitions)
    }

    /// True if this replica currently owns dedup-partition `p`.
    #[must_use]
    pub fn owns(&self, p: u32) -> bool {
        self.store.owns(p)
    }

    /// EOS produce.
    ///
    /// A fast-path map hit returns the cached offset. A miss takes the
    /// partition's transactional producer, writes the data record and the claim
    /// atomically, then updates the local map.
    #[tracing::instrument(skip_all)]
    /// # Panics
    /// Panics if the validated partition count cannot be represented locally.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn dedup_produce(
        &self,
        rec: &GatewayRecord,
        value: Bytes,
    ) -> Result<RecordOutcome, GatewayError> {
        let key = rec.idempotency_key.as_deref().ok_or_else(|| {
            GatewayError::Other("dedup_produce called without idempotency_key".into())
        })?;
        let p = partition_for(key, self.partitions);
        // Mutual exclusion: only the owner of `p` may produce its keys, and only
        // once warmed (claim map rebuilt). Otherwise refuse so the caller retries
        // against the owning replica.
        if !self.store.owns(p) || !self.store.is_warm() {
            return Err(GatewayError::Unavailable);
        }

        // Fast path: already claimed.
        if let Some(c) = self.store.get(key) {
            crate::metrics::metrics().record_dedup_hit();
            return Ok(RecordOutcome {
                partition: c.partition,
                offset: c.offset,
                deduplicated: true,
            });
        }

        let mut slot = self.slots[usize::try_from(p).expect("u32 partition fits usize")]
            .lock()
            .await;

        // Re-check under the lock (another task may have just claimed it).
        if let Some(c) = self.store.get(key) {
            crate::metrics::metrics().record_dedup_hit();
            return Ok(RecordOutcome {
                partition: c.partition,
                offset: c.offset,
                deduplicated: true,
            });
        }

        // Run the transactional write. On ANY error, `txn_write` has already
        // best-effort aborted any transaction it opened (only the guard it
        // holds internally can do that — a flat `abort_transaction` call from
        // out here can no longer reach it); just drop the producer so the
        // next call re-initializes from `Ready`. Otherwise a single transient
        // error would strand this partition's producer mid-transaction and
        // brick every key that hashes to it until the process restarts.
        match self.txn_write(&mut slot, rec, value, key, p).await {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                *slot = None;
                Err(e)
            }
        }
    }

    /// The fallible begin, record, claim, and commit sequence for one keyed
    /// record.
    ///
    /// This method is separate so `dedup_produce` can reset the producer slot
    /// on any error. The abort-on-error logic lives here, and not in
    /// `dedup_produce`, because only the holder of the `Transaction` guard that
    /// `begin_transaction` returns can abort it. The caller must hold `slot`'s
    /// lock and must already have confirmed that the key is not claimed.
    async fn txn_write(
        &self,
        slot: &mut Option<Producer>,
        rec: &GatewayRecord,
        value: Bytes,
        key: &str,
        p: u32,
    ) -> Result<RecordOutcome, GatewayError> {
        // Lazily init the partition's transactional producer.
        if slot.is_none() {
            let txn_id = format!("{}-{}", self.txn_id_prefix, p);
            let producer = Producer::builder()
                .bootstrap(self.bootstrap.clone())
                .client_id(format!("{}-dedup-{}", self.client_id, p))
                .dispatch_queue_capacity(self.dispatch_queue_capacity.get())
                .frame_max(self.frame_max.size())
                .enable_idempotence(true)
                .acks(Acks::All)
                .transactional_id(txn_id)
                .maybe_security(self.security.clone())
                .build()
                .await?;
            producer.init_transactions().await?;
            *slot = Some(producer);
        }
        let producer = slot.as_ref().expect("just initialized");

        let txn = producer.begin_transaction().await?;

        let sent: Result<(RecordMetadata, ClaimValue), GatewayError> = async {
            // 1. data record → user topic
            let data = to_producer_record(rec, value);
            let meta = producer
                .send(data)
                .await
                .await
                .map_err(|_| GatewayError::ProducerCanceled)?
                .map_err(GatewayError::Producer)?;

            // 2. claim → dedup topic (partition p), key = idempotency key
            let claim = ClaimValue {
                topic: rec.topic.clone(),
                partition: PartitionIndex(meta.partition),
                offset: Offset(meta.offset),
            };
            let claim_rec = ProducerRecord {
                topic: self.dedup_topic.clone(),
                partition: Some(i32::try_from(p).expect("validated partition fits i32")),
                key: Some(Bytes::from(key.as_bytes().to_vec())),
                value: Some(Bytes::from(serde_json::to_vec(&claim)?)),
                headers: vec![],
                timestamp_ms: None,
            };
            producer
                .send(claim_rec)
                .await
                .await
                .map_err(|_| GatewayError::ProducerCanceled)?
                .map_err(GatewayError::Producer)?;

            Ok((meta, claim))
        }
        .await;

        let (meta, claim) = match sent {
            Ok(pair) => pair,
            Err(e) => {
                let _ = txn.abort().await;
                crate::metrics::metrics().record_txn("abort");
                return Err(e);
            }
        };

        txn.commit()
            .await
            .map_err(|e| GatewayError::Producer(e.source))?;
        crate::metrics::metrics().record_txn("commit");

        // Single-owner: update the local map directly.
        self.store.apply(key.to_string(), claim);
        Ok(RecordOutcome {
            partition: PartitionIndex(meta.partition),
            offset: Offset(meta.offset),
            deduplicated: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::{DedupEngine, partition_for, store::DedupStore};

    #[test]
    fn partition_for_rejects_zero_partitions() {
        assert!(std::panic::catch_unwind(|| partition_for("key", 0)).is_err());
    }

    #[test]
    fn partition_for_rejects_counts_kafka_cannot_represent() {
        assert!(
            std::panic::catch_unwind(|| {
                let _ = partition_for("key", i32::MAX.cast_unsigned() + 1);
            })
            .is_err()
        );
    }

    #[test]
    fn dedup_engine_rejects_zero_partitions() {
        assert!(
            std::panic::catch_unwind(|| {
                DedupEngine::new(
                    "localhost:9092",
                    "test",
                    "test-txn",
                    "dedup".into(),
                    0,
                    Arc::new(DedupStore::new(1)),
                    None,
                )
            })
            .is_err()
        );
    }
}

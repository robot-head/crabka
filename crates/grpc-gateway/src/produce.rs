//! Core produce engine.
//!
//! Keyed records, which carry an `idempotency_key`, go through the dedup engine
//! for EOS. Unkeyed records take the plain idempotent path (`acks=all`). The
//! engine is transport-agnostic. Front-ends convert to `GatewayRecord` and
//! receive `RecordOutcome`.

use std::sync::Arc;

use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_security::Principal;

use crate::{
    codec::RecordCodec,
    dedup::membership::MembershipStore,
    error::GatewayError,
    forward::Forwarder,
    ids::{Offset, PartitionIndex},
    types::{GatewayRecord, RecordOutcome},
};

pub struct ProduceCore {
    producer_all: Arc<Producer>,
    producer_one: Arc<Producer>,
    producer_zero: Arc<Producer>,
    codec: Arc<dyn RecordCodec>,
    /// When absent, keyed records take the plain producer path too.
    dedup: Option<Arc<crate::dedup::DedupEngine>>,
    forwarding: Option<Forwarding>,
}

struct Forwarding {
    membership: Arc<MembershipStore>,
    forwarder: Arc<Forwarder>,
    self_addr: String,
}

impl ProduceCore {
    /// Build a plain idempotent producer (`acks=all`, no transactional id).
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn new(
        bootstrap: &str,
        client_id: &str,
        codec: Arc<dyn RecordCodec>,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, GatewayError> {
        Box::pin(Self::new_with_policy(
            bootstrap,
            client_id,
            codec,
            security,
            &crate::config::GatewayRuntimeConfig::default(),
        ))
        .await
    }

    /// Build a plain producer with the deployment's client resource policy.
    /// # Errors
    /// Returns an error when client construction fails.
    pub async fn new_with_policy(
        bootstrap: &str,
        client_id: &str,
        codec: Arc<dyn RecordCodec>,
        security: Option<crabka_client_core::security::ClientSecurity>,
        policy: &crate::config::GatewayRuntimeConfig,
    ) -> Result<Self, GatewayError> {
        let build = |acks, enable_idempotence| {
            Producer::builder()
                .bootstrap(bootstrap.to_string())
                .client_id(client_id.to_string())
                .dispatch_queue_capacity(policy.client_dispatch_queue_capacity.get())
                .frame_max(policy.client_frame_max.size())
                .enable_idempotence(enable_idempotence)
                .acks(acks)
                .maybe_security(security.clone())
                .build()
        };
        let (producer_all, producer_one, producer_zero) = tokio::try_join!(
            build(Acks::All, true),
            build(Acks::One, false),
            build(Acks::Zero, false),
        )?;
        Ok(Self {
            producer_all: Arc::new(producer_all),
            producer_one: Arc::new(producer_one),
            producer_zero: Arc::new(producer_zero),
            codec,
            dedup: None,
            forwarding: None,
        })
    }

    /// Build a non-idempotent producer for unit tests that do not need a real
    /// broker. The producer fails at the first send because no bootstrap is
    /// available. Route-layer tests that stop before they produce can use it.
    ///
    /// # Errors
    /// Returns an error if the test producer cannot be configured.
    #[cfg(test)]
    pub async fn new_for_test(
        bootstrap: &str,
        client_id: &str,
        codec: Arc<dyn RecordCodec>,
    ) -> Result<Self, GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(false)
            .acks(Acks::One)
            .build()
            .await?;
        let producer = Arc::new(producer);
        Ok(Self {
            producer_all: Arc::clone(&producer),
            producer_one: Arc::clone(&producer),
            producer_zero: producer,
            codec,
            dedup: None,
            forwarding: None,
        })
    }

    /// Inject the dedup engine used by keyed records.
    #[must_use]
    pub fn with_dedup(mut self, dedup: Arc<crate::dedup::DedupEngine>) -> Self {
        self.dedup = Some(dedup);
        self
    }

    /// Enable active-active forwarding. A keyed record this replica does not
    /// own routes to the owner named by the membership routing table.
    #[must_use]
    pub fn with_forwarding(
        mut self,
        membership: Arc<MembershipStore>,
        forwarder: Arc<Forwarder>,
        self_addr: String,
    ) -> Self {
        self.forwarding = Some(Forwarding {
            membership,
            forwarder,
            self_addr,
        });
        self
    }

    #[must_use]
    pub fn codec(&self) -> &Arc<dyn RecordCodec> {
        &self.codec
    }

    /// Public produce entry point.
    ///
    /// A keyed record whose dedup-partition this replica does not own goes to
    /// the owner named by the membership routing table. Every other record is
    /// produced locally. `principal` is the resolved caller identity. A forward
    /// relays it so the owning replica can re-authorize the original caller.
    /// The local path does not use it.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn produce(
        &self,
        rec: GatewayRecord,
        principal: &Principal,
    ) -> Result<RecordOutcome, GatewayError> {
        self.produce_with_acks(rec, principal, Acks::All).await
    }

    /// Produce using the caller's requested acknowledgement level. Keyed dedup
    /// records require `acks=all` because their exactly-once transaction must be
    /// durably acknowledged.
    ///
    /// # Errors
    ///
    /// Returns an error for incompatible acknowledgement and dedup settings, or
    /// when routing, encoding, or the producer fails.
    pub async fn produce_with_acks(
        &self,
        rec: GatewayRecord,
        principal: &Principal,
        acks: Acks,
    ) -> Result<RecordOutcome, GatewayError> {
        if rec.idempotency_key.is_some() && acks != Acks::All {
            return Err(GatewayError::Other(
                "idempotency_key requires ACKS_ALL".to_string(),
            ));
        }
        // Resolve the route without holding a borrow of `rec` across its move.
        let forward_addr: Option<String> =
            match (&self.dedup, &self.forwarding, &rec.idempotency_key) {
                (Some(dedup), Some(fwd), Some(key)) => {
                    let p = dedup.partition_for_key(key);
                    if dedup.owns(p) {
                        None
                    } else {
                        match fwd.membership.owner_of(p) {
                            Some(addr) if addr == fwd.self_addr => None,
                            Some(addr) => Some(addr),
                            None => return Err(GatewayError::Unavailable),
                        }
                    }
                }
                _ => None,
            };

        match forward_addr {
            Some(addr) => {
                let fwd = self.forwarding.as_ref().ok_or_else(|| {
                    GatewayError::Other("forwarding route has no forwarder".to_string())
                })?;
                fwd.forwarder.forward(&addr, &rec, principal).await
            }
            None => self.produce_local_with_acks(rec, acks).await,
        }
    }

    /// Local produce, with NO forwarding.
    ///
    /// A keyed record goes to the dedup engine, behind the owner and warm gate.
    /// An unkeyed record goes to the plain idempotent producer. The public path
    /// calls this when this replica owns the key, and so does the internal
    /// forward endpoint.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn produce_local(&self, rec: GatewayRecord) -> Result<RecordOutcome, GatewayError> {
        self.produce_local_with_acks(rec, Acks::All).await
    }

    async fn produce_local_with_acks(
        &self,
        rec: GatewayRecord,
        acks: Acks,
    ) -> Result<RecordOutcome, GatewayError> {
        let value = self
            .codec
            .encode(&rec.topic, rec.encode_body())
            .await
            .map_err(GatewayError::from)?;
        match (&self.dedup, &rec.idempotency_key) {
            (Some(dedup), Some(_key)) => dedup.dedup_produce(&rec, value).await,
            _ => self.produce_plain(&rec, value, acks).await,
        }
    }

    async fn produce_plain(
        &self,
        rec: &GatewayRecord,
        value: bytes::Bytes,
        acks: Acks,
    ) -> Result<RecordOutcome, GatewayError> {
        let prec = to_producer_record(rec, value);
        let producer = match acks {
            Acks::All => &self.producer_all,
            Acks::One => &self.producer_one,
            Acks::Zero => &self.producer_zero,
        };
        let rx = producer.send(prec).await;
        let meta = rx
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;
        Ok(RecordOutcome {
            partition: PartitionIndex(meta.partition),
            offset: Offset(meta.offset),
            deduplicated: false,
        })
    }
}

/// Map a `GatewayRecord` to the native `ProducerRecord`.
pub(crate) fn to_producer_record(rec: &GatewayRecord, value: bytes::Bytes) -> ProducerRecord {
    ProducerRecord {
        topic: rec.topic.clone(),
        partition: rec.partition,
        key: rec.key.clone(),
        value: Some(value),
        headers: rec
            .headers
            .iter()
            .map(|(k, v)| Header {
                key: k.clone(),
                value: v.clone(),
            })
            .collect(),
        timestamp_ms: rec.timestamp_ms,
    }
}

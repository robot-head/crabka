//! Sink side: residency gate, naming, and provenance loop-guard, then produce
//! to the target and emit source-to-target offset syncs.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use crabka_client_producer::{
    Acks, Header, OwnedTransaction, Producer, ProducerError, ProducerRecord, RecordMetadata,
};
use crabka_connect::{ConnectError, ConnectRecord, OffsetValue, Sink, SourceOffset};
use crabka_units::prelude::TimeExt as _;
use tokio::sync::oneshot::Receiver;
use tracing::warn;

use crate::{
    checkpoint_store::STATE_TOPIC,
    config::{ClientResourcePolicy, Delivery, NamingPolicy, PolicyConfig, ReplicatorRuntimePolicy},
    ids::{DownstreamOffset, PartitionIndex, UpstreamOffset},
    mm2::OffsetSync,
    naming::{PROVENANCE_HEADER, Renamer},
    record::ReplicatedRecord,
    residency::ResidencyGate,
};

/// One in-flight produce awaiting its broker ack: the ack receiver plus the
/// source-side coordinates needed to build the [`OffsetSync`] once the ack
/// supplies the downstream offset.
struct PendingProduce {
    /// Receiver for the broker ack carrying the downstream [`RecordMetadata`].
    rx: Receiver<Result<RecordMetadata, ProducerError>>,
    /// Source topic name.
    topic: String,
    /// Source partition index.
    partition: PartitionIndex,
    /// Source (upstream) offset of the record being produced.
    upstream: UpstreamOffset,
}

/// Parameters required to start a [`TargetSink`].
pub struct SinkParams {
    /// Bootstrap address of the target cluster.
    pub target_bootstrap: String,
    /// Stable flow name used as the transaction and checkpoint identity.
    pub flow_name: String,
    /// Alias of the source cluster (stamped as provenance header).
    pub source_alias: String,
    /// Delivery guarantee for this flow.
    pub delivery: Delivery,
    /// How to rename source topics on the target.
    pub naming: NamingPolicy,
    /// Compliance zones of the target cluster (used for residency checks).
    pub target_zones: Vec<String>,
    /// Residency policies to enforce.
    pub policies: Vec<PolicyConfig>,
    /// Optional TLS/SASL security for the target cluster.
    pub security: Option<crabka_client_core::security::ClientSecurity>,
    /// Source partition count by source topic.
    pub source_partition_counts: BTreeMap<String, i32>,
}

/// A [`Sink`] that applies residency filtering, topic renaming, and loop-guard
/// logic before it produces records to the target cluster.
///
/// At-least-once flows drain pending acknowledgements and write MM2-compatible
/// offset-syncs on [`flush`](Sink::flush). Exactly-once flows put target data,
/// offset-syncs, and the source checkpoint in one producer transaction and make
/// all three visible together on [`commit`](Sink::commit).
pub struct TargetSink {
    producer: Arc<Producer>,
    delivery: Delivery,
    flow_name: String,
    renamer: Renamer,
    gate: ResidencyGate,
    target_zones: Vec<String>,
    offset_syncs_topic: String,
    source_alias: String,
    /// Bootstrap address of the target cluster, used for lazy topic creation.
    target_bootstrap: String,
    /// Security config retained for lazy topic creation calls.
    security: Option<crabka_client_core::security::ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
    runtime_policy: ReplicatorRuntimePolicy,
    source_partition_counts: BTreeMap<String, i32>,
    /// Target topics that the sink has already ensured, so it makes no
    /// redundant admin calls.
    created_topics: HashSet<String>,
    /// In-flight produces awaiting broker acks (see [`PendingProduce`]).
    pending: Vec<PendingProduce>,
    /// Transaction guard held across the connect runtime's begin/put/commit calls.
    transaction: Option<OwnedTransaction>,
    /// Last source position committed in a target transaction.
    committed_checkpoint: SourceOffset,
    /// Source position staged by the current transaction.
    pending_checkpoint: Option<SourceOffset>,
    /// Completed offset-syncs, accessible through [`drain_offset_syncs`].
    offset_syncs: Vec<OffsetSync>,
}

impl TargetSink {
    /// Build a [`TargetSink`], ensure the offset-syncs topic exists, and connect
    /// the producer to the target cluster.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Backend`] if the producer cannot connect or the
    /// offset-syncs topic cannot be created.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(target = %params.target_bootstrap, source_alias = %params.source_alias),
        err,
    )]
    pub async fn start(params: SinkParams) -> Result<Self, ConnectError> {
        Self::start_with_policy(params, ClientResourcePolicy::default()).await
    }

    /// Start with the deployment's client resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Backend`] if the producer cannot connect or the
    /// offset-syncs topic cannot be created.
    pub async fn start_with_policy(
        params: SinkParams,
        client_resource_policy: ClientResourcePolicy,
    ) -> Result<Self, ConnectError> {
        Self::start_with_runtime_policy(
            params,
            client_resource_policy,
            ReplicatorRuntimePolicy::default(),
        )
        .await
    }

    pub(crate) async fn start_with_runtime_policy(
        params: SinkParams,
        client_resource_policy: ClientResourcePolicy,
        runtime_policy: ReplicatorRuntimePolicy,
    ) -> Result<Self, ConnectError> {
        let offset_syncs_topic = OffsetSync::topic_name(&params.source_alias);

        // Ensure the offset-syncs topic exists before we start producing.
        crate::admin_util::ensure_topic_with_runtime_policy(
            &params.target_bootstrap,
            &offset_syncs_topic,
            1,
            params.security.clone(),
            client_resource_policy,
            &runtime_policy,
            runtime_policy.internal_topic_replication_factor,
        )
        .await
        .map_err(ConnectError::Backend)?;

        if params.delivery == Delivery::ExactlyOnce {
            crate::admin_util::ensure_compacted_topic_with_runtime_policy(
                &params.target_bootstrap,
                STATE_TOPIC,
                params.security.clone(),
                client_resource_policy,
                &runtime_policy,
            )
            .await
            .map_err(ConnectError::Offset)?;
        }

        // Exactly-once flows use one stable transactional producer per flow.
        let producer = build_producer(
            &params.target_bootstrap,
            &params.flow_name,
            params.delivery,
            params.security.clone(),
            client_resource_policy,
            &runtime_policy,
        )
        .await
        .map_err(|e| ConnectError::Backend(e.to_string()))?;

        let committed_checkpoint = if params.delivery == Delivery::ExactlyOnce {
            let bytes = crate::admin_util::read_last_value_for_key_with_runtime_policy(
                &params.target_bootstrap,
                STATE_TOPIC,
                params.flow_name.as_bytes(),
                params.security.clone(),
                client_resource_policy,
                &runtime_policy,
            )
            .await
            .map_err(ConnectError::Offset)?;
            bytes.map_or_else(
                || Ok(SourceOffset::default()),
                |bytes| {
                    serde_json::from_slice(&bytes)
                        .map_err(|error| ConnectError::Offset(error.to_string()))
                },
            )?
        } else {
            SourceOffset::default()
        };

        let renamer = Renamer::new(params.naming, &params.source_alias);

        let gate = ResidencyGate::compile(&params.policies)
            .map_err(|e| ConnectError::Backend(e.to_string()))?;

        Ok(Self {
            producer,
            delivery: params.delivery,
            flow_name: params.flow_name,
            renamer,
            gate,
            target_zones: params.target_zones,
            offset_syncs_topic,
            source_alias: params.source_alias,
            target_bootstrap: params.target_bootstrap,
            security: params.security,
            client_resource_policy,
            runtime_policy,
            source_partition_counts: params.source_partition_counts,
            created_topics: HashSet::new(),
            pending: Vec::new(),
            transaction: None,
            committed_checkpoint,
            pending_checkpoint: None,
            offset_syncs: Vec::new(),
        })
    }

    /// Return and clear all completed [`OffsetSync`] records accumulated since
    /// the last call, or since construction.
    pub fn drain_offset_syncs(&mut self) -> Vec<OffsetSync> {
        std::mem::take(&mut self.offset_syncs)
    }

    fn stage_source_offset(&mut self, record: &ReplicatedRecord) -> Result<(), ConnectError> {
        let Some(checkpoint) = self.pending_checkpoint.as_mut() else {
            return Ok(());
        };
        let next = record.offset.0.checked_add(1).ok_or_else(|| {
            ConnectError::Offset(format!(
                "source offset overflow for {}-{}",
                record.topic, record.partition
            ))
        })?;
        checkpoint.position.0.insert(
            format!("{}-{}", record.topic, record.partition),
            OffsetValue::Long(next),
        );
        Ok(())
    }

    async fn flush_pending(&mut self) -> Result<Vec<OffsetSync>, ConnectError> {
        let pending = std::mem::take(&mut self.pending);
        let mut completed = Vec::with_capacity(pending.len());

        for PendingProduce {
            rx,
            topic,
            partition,
            upstream,
        } in pending
        {
            let meta = rx
                .await
                .map_err(|_| ConnectError::Backend("producer dropped sender".into()))?
                .map_err(|error| ConnectError::Backend(error.to_string()))?;
            let offset_sync = OffsetSync {
                topic,
                partition,
                upstream,
                downstream: DownstreamOffset(meta.offset),
            };
            let sync_rx = self
                .producer
                .send(ProducerRecord {
                    topic: self.offset_syncs_topic.clone(),
                    partition: None,
                    key: Some(Bytes::from(offset_sync.key_bytes())),
                    value: Some(Bytes::from(offset_sync.value_bytes())),
                    headers: Vec::new(),
                    timestamp_ms: None,
                })
                .await;
            sync_rx
                .await
                .map_err(|_| ConnectError::Backend("producer dropped offset-sync sender".into()))?
                .map_err(|error| ConnectError::Backend(error.to_string()))?;
            completed.push(offset_sync);
        }

        self.producer
            .flush()
            .await
            .map_err(|error| ConnectError::Backend(error.to_string()))?;
        Ok(completed)
    }

    async fn write_transaction_checkpoint(&self) -> Result<(), ConnectError> {
        let checkpoint = self.pending_checkpoint.as_ref().ok_or_else(|| {
            ConnectError::Transaction("exactly-once transaction was not begun".into())
        })?;
        let bytes = serde_json::to_vec(checkpoint)
            .map_err(|error| ConnectError::Offset(error.to_string()))?;
        self.producer
            .send(ProducerRecord {
                topic: STATE_TOPIC.to_owned(),
                partition: None,
                key: Some(Bytes::copy_from_slice(self.flow_name.as_bytes())),
                value: Some(Bytes::from(bytes)),
                headers: Vec::new(),
                timestamp_ms: None,
            })
            .await
            .await
            .map_err(|_| ConnectError::Offset("producer dropped checkpoint sender".into()))?
            .map_err(|error| ConnectError::Offset(error.to_string()))?;
        Ok(())
    }
}

/// Build the target producer for the selected delivery guarantee.
async fn build_producer(
    bootstrap: &str,
    flow_name: &str,
    delivery: Delivery,
    security: Option<crabka_client_core::security::ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
    runtime_policy: &ReplicatorRuntimePolicy,
) -> Result<Arc<Producer>, crabka_client_producer::ProducerError> {
    let builder = Producer::builder()
        .bootstrap(bootstrap)
        .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
        .frame_max(client_resource_policy.frame_max.size())
        .dns_timeout(runtime_policy.client_dns_timeout)
        .request_timeout(runtime_policy.client_request_timeout.to_std())
        .acks(Acks::All);
    let producer = match (delivery, security) {
        (Delivery::AtLeastOnce, Some(security)) => {
            builder
                .enable_idempotence(false)
                .security(security)
                .build()
                .await?
        }
        (Delivery::AtLeastOnce, None) => builder.enable_idempotence(false).build().await?,
        (Delivery::ExactlyOnce, Some(security)) => {
            builder
                .enable_idempotence(true)
                .transactional_id(format!("crabka-replicator-{flow_name}"))
                .security(security)
                .build()
                .await?
        }
        (Delivery::ExactlyOnce, None) => {
            builder
                .enable_idempotence(true)
                .transactional_id(format!("crabka-replicator-{flow_name}"))
                .build()
                .await?
        }
    };
    let producer = Arc::new(producer);
    if delivery == Delivery::ExactlyOnce {
        producer.init_transactions().await?;
    }
    Ok(producer)
}

#[async_trait]
impl Sink<(), ReplicatedRecord> for TargetSink {
    /// Accept a batch of replicated records, apply filtering and loop-guard
    /// logic, then enqueue produce calls for the accepted records.
    ///
    /// The sink drops a record, and does not buffer it, when:
    /// - `value` is `None`, that is, a tombstone or no payload.
    /// - The identity-naming loop-guard fires: the `__crabka_origin` header of
    ///   the record matches our own `source_alias`.
    /// - The residency gate blocks the topic for the zones of the target.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(records = records.len(), accepted = tracing::field::Empty),
        err,
    )]
    async fn put(
        &mut self,
        records: Vec<ConnectRecord<(), ReplicatedRecord>>,
    ) -> Result<(), ConnectError> {
        if self.delivery == Delivery::ExactlyOnce && self.transaction.is_none() {
            return Err(ConnectError::Transaction(
                "exactly-once put must follow begin".into(),
            ));
        }
        let mut accepted = 0usize;
        for cr in records {
            let Some(r) = cr.value else { continue };
            self.stage_source_offset(&r)?;

            // Identity-naming loop-guard: skip if already produced by us.
            if self.renamer.policy() == NamingPolicy::Identity {
                let own_alias = self.source_alias.as_bytes();
                let is_loop = r
                    .headers
                    .iter()
                    .any(|(k, v)| k == PROVENANCE_HEADER && v.as_deref() == Some(own_alias));
                if is_loop {
                    continue;
                }
            }

            // Residency gate.
            if !self.gate.permits(&r.topic, &self.target_zones) {
                warn!(
                    topic = %r.topic,
                    target_zones = ?self.target_zones,
                    "residency gate blocked record; dropping"
                );
                continue;
            }

            // Build target topic name.
            let target_topic = self.renamer.target_name(&r.topic);

            // Lazily ensure the target topic exists (no-op after first visit).
            if !self.created_topics.contains(&target_topic) {
                let partitions = self
                    .source_partition_counts
                    .get(&r.topic)
                    .copied()
                    .ok_or_else(|| {
                        ConnectError::Backend(format!(
                            "missing source partition count for {}",
                            r.topic
                        ))
                    })?;
                crate::admin_util::ensure_topic_with_runtime_policy(
                    &self.target_bootstrap,
                    &target_topic,
                    partitions,
                    self.security.clone(),
                    self.client_resource_policy,
                    &self.runtime_policy,
                    self.runtime_policy.data_topic_replication_factor,
                )
                .await
                .map_err(ConnectError::Backend)?;
                self.created_topics.insert(target_topic.clone());
            }

            // Build headers: original headers + provenance.
            let mut headers: Vec<Header> = r
                .headers
                .iter()
                .map(|(k, v)| Header {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect();
            headers.push(Header {
                key: PROVENANCE_HEADER.to_owned(),
                value: Some(Bytes::from(self.source_alias.clone())),
            });

            // Enqueue the produce and pair with an in-flight OffsetSync.
            let rx = self
                .producer
                .send(ProducerRecord {
                    topic: target_topic,
                    partition: Some(r.partition.0),
                    key: r.key.clone(),
                    value: r.value.clone(),
                    headers,
                    timestamp_ms: Some(r.timestamp.into()),
                })
                .await;

            self.pending.push(PendingProduce {
                rx,
                topic: r.topic,
                partition: r.partition,
                // The record's source offset is the upstream side of the sync.
                upstream: r.offset.into(),
            });
            accepted += 1;
        }

        tracing::Span::current().record("accepted", accepted);
        Ok(())
    }

    /// Await all pending produce acks, write offset-syncs to the target, and
    /// flush the producer.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(pending = self.pending.len()),
        err,
    )]
    async fn flush(&mut self) -> Result<(), ConnectError> {
        if self.delivery == Delivery::ExactlyOnce {
            return Err(ConnectError::Transaction(
                "exactly-once sink must use commit".into(),
            ));
        }
        let completed = self.flush_pending().await?;
        self.offset_syncs.extend(completed);
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.delivery == Delivery::ExactlyOnce
    }

    async fn begin(&mut self) -> Result<(), ConnectError> {
        if self.delivery != Delivery::ExactlyOnce {
            return Ok(());
        }
        if self.transaction.is_some() {
            return Err(ConnectError::Transaction(
                "exactly-once transaction is already open".into(),
            ));
        }
        self.transaction = Some(
            Arc::clone(&self.producer)
                .begin_transaction_owned()
                .await
                .map_err(|error| ConnectError::Transaction(error.to_string()))?,
        );
        self.pending_checkpoint = Some(self.committed_checkpoint.clone());
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), ConnectError> {
        if self.delivery != Delivery::ExactlyOnce {
            return self.flush().await;
        }
        if self.transaction.is_none() {
            return Err(ConnectError::Transaction(
                "exactly-once commit must follow begin".into(),
            ));
        }

        let completed = self.flush_pending().await?;
        self.write_transaction_checkpoint().await?;
        let transaction = self
            .transaction
            .take()
            .expect("transaction checked immediately above");
        match transaction.commit().await {
            Ok(()) => {
                self.offset_syncs.extend(completed);
                self.committed_checkpoint = self
                    .pending_checkpoint
                    .take()
                    .expect("begin staged a checkpoint");
                Ok(())
            }
            Err(error) => {
                self.transaction = Some(error.transaction);
                Err(ConnectError::Transaction(error.source.to_string()))
            }
        }
    }

    async fn abort(&mut self) -> Result<(), ConnectError> {
        if self.delivery != Delivery::ExactlyOnce {
            return Ok(());
        }
        self.pending.clear();
        self.pending_checkpoint = None;
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        match transaction.abort().await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.transaction = Some(error.transaction);
                Err(ConnectError::Transaction(error.source.to_string()))
            }
        }
    }

    async fn close(&mut self) -> Result<(), ConnectError> {
        self.abort().await
    }
}

#[cfg(test)]
mod tests {

    use crabka_connect::{CheckpointStore, Sink};

    use super::*;
    use crate::ids::{Offset, Timestamp};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn produces_renamed_and_records_offset_sync_but_blocks_denied() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = broker.listen_addr().to_string();

        let mut sink = TargetSink::start(SinkParams {
            target_bootstrap: target.clone(),
            flow_name: "us-east__eu-west".into(),
            source_alias: "us-east".into(),
            delivery: Delivery::AtLeastOnce,
            naming: crate::config::NamingPolicy::Default,
            target_zones: vec!["us".into()],
            policies: vec![crate::config::PolicyConfig {
                name: "p".into(),
                topics: vec!["secret".into()],
                residency: Some(crate::config::Residency {
                    allow_zones: vec!["gdpr".into()],
                    deny_zones: vec![],
                }),
            }],
            security: None,
            source_partition_counts: [("orders".to_string(), 3)].into(),
        })
        .await
        .unwrap();

        let allowed = ConnectRecord::new(
            None,
            Some(ReplicatedRecord {
                topic: "orders".into(),
                partition: PartitionIndex(2),
                offset: Offset(5),
                timestamp: Timestamp(1),
                key: Some("k".into()),
                value: Some("v".into()),
                headers: vec![],
            }),
        );
        let denied = ConnectRecord::new(
            None,
            Some(ReplicatedRecord {
                topic: "secret".into(),
                partition: PartitionIndex(0),
                offset: Offset(9),
                timestamp: Timestamp(1),
                key: None,
                value: Some("x".into()),
                headers: vec![],
            }),
        );

        sink.put(vec![allowed, denied]).await.unwrap();
        sink.flush().await.unwrap();

        assert2::assert!(
            crate::test_util::topic_record_count(&target, "us-east.orders").await == 1
        );
        assert2::assert!(
            crate::test_util::topic_record_count(&target, "us-east.secret").await == 0
        );

        let syncs = sink.drain_offset_syncs();
        assert2::assert!(
            syncs
                .iter()
                .any(|s| s.topic == "orders" && s.partition == 2 && s.upstream == 5)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn identity_naming_loop_guard_skips_only_own_provenance() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = broker.listen_addr().to_string();

        // Identity naming (no rename) + permit-all residency. The loop-guard
        // must skip ONLY a record whose `__crabka_origin` header equals our own
        // source alias.
        let mut sink = TargetSink::start(SinkParams {
            target_bootstrap: target.clone(),
            flow_name: "us-east__eu-west".into(),
            source_alias: "us-east".into(),
            delivery: Delivery::AtLeastOnce,
            naming: crate::config::NamingPolicy::Identity,
            target_zones: vec!["us".into()],
            policies: vec![],
            security: None,
            source_partition_counts: [("orders".to_string(), 1)].into(),
        })
        .await
        .unwrap();

        // A: our own provenance -> loop -> MUST be skipped.
        let a = ConnectRecord::new(
            None,
            Some(ReplicatedRecord {
                topic: "orders".into(),
                partition: PartitionIndex(0),
                offset: Offset(1),
                timestamp: Timestamp(1),
                key: Some("a".into()),
                value: Some("v".into()),
                headers: vec![(PROVENANCE_HEADER.into(), Some("us-east".into()))],
            }),
        );
        // B: different origin -> MUST be produced.
        let b = ConnectRecord::new(
            None,
            Some(ReplicatedRecord {
                topic: "orders".into(),
                partition: PartitionIndex(0),
                offset: Offset(2),
                timestamp: Timestamp(1),
                key: Some("b".into()),
                value: Some("v".into()),
                headers: vec![(PROVENANCE_HEADER.into(), Some("eu-west".into()))],
            }),
        );
        // C: a non-provenance header carrying our alias -> MUST be produced.
        let c = ConnectRecord::new(
            None,
            Some(ReplicatedRecord {
                topic: "orders".into(),
                partition: PartitionIndex(0),
                offset: Offset(3),
                timestamp: Timestamp(1),
                key: Some("c".into()),
                value: Some("v".into()),
                headers: vec![("other".into(), Some("us-east".into()))],
            }),
        );

        sink.put(vec![a, b, c]).await.unwrap();
        sink.flush().await.unwrap();

        // Identity naming means the target topic is "orders" (no prefix). Read the
        // produced records back and assert the EXACT set of keys: B and C land,
        // A (our own provenance) is filtered. Checking the key set — not just the
        // count — is what distinguishes the four loop-guard mutants, since some of
        // them merely swap *which* record is skipped while keeping the count at 2.
        let produced = crate::admin_util::read_all(&target, "orders", None)
            .await
            .unwrap();
        let mut keys: Vec<Vec<u8>> = produced
            .into_iter()
            .filter_map(|(k, _)| k.map(|b| b.to_vec()))
            .collect();
        keys.sort();
        assert2::assert!(keys == vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exactly_once_abort_hides_data_sync_and_checkpoint_then_commit_reveals_all() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = broker.listen_addr().to_string();
        let flow_name = "us-east__eu-west";
        let mut sink = TargetSink::start(SinkParams {
            target_bootstrap: target.clone(),
            flow_name: flow_name.into(),
            source_alias: "us-east".into(),
            delivery: Delivery::ExactlyOnce,
            naming: NamingPolicy::Default,
            target_zones: vec!["eu".into()],
            policies: Vec::new(),
            security: None,
            source_partition_counts: [("orders".to_owned(), 1)].into(),
        })
        .await
        .unwrap();
        let record = || {
            ConnectRecord::new(
                None,
                Some(ReplicatedRecord {
                    topic: "orders".into(),
                    partition: PartitionIndex(0),
                    offset: Offset(5),
                    timestamp: Timestamp(1),
                    key: Some("k".into()),
                    value: Some("v".into()),
                    headers: Vec::new(),
                }),
            )
        };

        sink.begin().await.unwrap();
        sink.put(vec![record()]).await.unwrap();
        sink.abort().await.unwrap();

        let store =
            crate::checkpoint_store::InternalTopicCheckpointStore::start(&target, flow_name, None)
                .await
                .unwrap();
        assert2::assert!(
            crate::admin_util::read_all(&target, "us-east.orders", None)
                .await
                .unwrap()
                .is_empty()
        );
        assert2::assert!(
            crate::admin_util::read_all(&target, &OffsetSync::topic_name("us-east"), None,)
                .await
                .unwrap()
                .is_empty()
        );
        assert2::assert!(store.load().await.unwrap().is_none());
        assert2::assert!(sink.drain_offset_syncs().is_empty());

        sink.begin().await.unwrap();
        sink.put(vec![record()]).await.unwrap();
        sink.commit().await.unwrap();

        assert2::assert!(
            crate::admin_util::read_all(&target, "us-east.orders", None)
                .await
                .unwrap()
                .len()
                == 1
        );
        assert2::assert!(
            crate::admin_util::read_all(&target, &OffsetSync::topic_name("us-east"), None,)
                .await
                .unwrap()
                .len()
                == 1
        );
        let checkpoint = store.load().await.unwrap().unwrap();
        assert2::assert!(checkpoint.position.get("orders-0") == Some(&OffsetValue::Long(6)));
        assert2::assert!(sink.drain_offset_syncs().len() == 1);
    }
}

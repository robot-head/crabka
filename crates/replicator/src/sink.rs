//! Sink side: residency gate + naming + provenance loop-guard, produce to target,
//! and emit source->target offset syncs.

use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;
use crabka_client_producer::{
    Acks, Header, Producer, ProducerError, ProducerRecord, RecordMetadata,
};
use crabka_connect::{ConnectError, ConnectRecord, Sink};
use tokio::sync::oneshot::Receiver;
use tracing::warn;

use crate::{
    config::{NamingPolicy, PolicyConfig},
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
    /// Alias of the source cluster (stamped as provenance header).
    pub source_alias: String,
    /// How to rename source topics on the target.
    pub naming: NamingPolicy,
    /// Compliance zones of the target cluster (used for residency checks).
    pub target_zones: Vec<String>,
    /// Residency policies to enforce.
    pub policies: Vec<PolicyConfig>,
    /// Optional TLS/SASL security for the target cluster.
    pub security: Option<crabka_client_core::security::ClientSecurity>,
}

/// A [`Sink`] that applies residency filtering, topic renaming, and
/// loop-guard logic before producing records to the target cluster.
///
/// On each [`flush`](Sink::flush) it drains pending produce acknowledgements,
/// sets the downstream offset on each [`OffsetSync`], and writes those syncs
/// back to the target's offset-syncs topic (MM2-compatible).
pub struct TargetSink {
    producer: Producer,
    renamer: Renamer,
    gate: ResidencyGate,
    target_zones: Vec<String>,
    offset_syncs_topic: String,
    source_alias: String,
    /// Bootstrap address of the target cluster (used to lazily create topics).
    target_bootstrap: String,
    /// Security config retained for lazy topic creation calls.
    security: Option<crabka_client_core::security::ClientSecurity>,
    /// Target topics that have already been ensured (to avoid redundant admin calls).
    created_topics: HashSet<String>,
    /// In-flight produces awaiting broker acks (see [`PendingProduce`]).
    pending: Vec<PendingProduce>,
    /// Completed offset-syncs, accessible via [`drain_offset_syncs`].
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
        let offset_syncs_topic = OffsetSync::topic_name(&params.source_alias);

        // Ensure the offset-syncs topic exists before we start producing.
        crate::admin_util::ensure_topic(
            &params.target_bootstrap,
            &offset_syncs_topic,
            1,
            params.security.clone(),
        )
        .await
        .map_err(ConnectError::Backend)?;

        // Build the producer (non-idempotent for at-least-once Slice 1).
        let producer = build_producer(&params.target_bootstrap, params.security.clone())
            .await
            .map_err(|e| ConnectError::Backend(e.to_string()))?;

        let renamer = Renamer::new(params.naming, &params.source_alias);

        let gate = ResidencyGate::compile(&params.policies)
            .map_err(|e| ConnectError::Backend(e.to_string()))?;

        Ok(Self {
            producer,
            renamer,
            gate,
            target_zones: params.target_zones,
            offset_syncs_topic,
            source_alias: params.source_alias,
            target_bootstrap: params.target_bootstrap,
            security: params.security,
            created_topics: HashSet::new(),
            pending: Vec::new(),
            offset_syncs: Vec::new(),
        })
    }

    /// Return and clear all completed [`OffsetSync`] records accumulated since
    /// the last call (or since construction).
    pub fn drain_offset_syncs(&mut self) -> Vec<OffsetSync> {
        std::mem::take(&mut self.offset_syncs)
    }
}

/// Build a non-idempotent producer with `acks=All`.
async fn build_producer(
    bootstrap: &str,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<Producer, crabka_client_producer::ProducerError> {
    let builder = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(false)
        .acks(Acks::All);
    match security {
        Some(s) => builder.security(s).build().await,
        None => builder.build().await,
    }
}

#[async_trait]
impl Sink<(), ReplicatedRecord> for TargetSink {
    /// Accept a batch of replicated records, applying filtering and loop-guard
    /// logic, then enqueue produce calls for the accepted records.
    ///
    /// Records are dropped (not buffered) when:
    /// - `value` is `None` (tombstone or no payload).
    /// - Identity-naming loop-guard fires: the record's `__crabka_origin` header
    ///   matches our own `source_alias`.
    /// - Residency gate blocks the topic for the target's zones.
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
        let mut accepted = 0usize;
        for cr in records {
            let Some(r) = cr.value else { continue };

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
                crate::admin_util::ensure_topic(
                    &self.target_bootstrap,
                    &target_topic,
                    1,
                    self.security.clone(),
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
                    partition: None,
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
        let pending = std::mem::take(&mut self.pending);

        for PendingProduce {
            rx,
            topic,
            partition,
            upstream,
        } in pending
        {
            // Await the broker ack for this produce.
            let meta = rx
                .await
                .map_err(|_| ConnectError::Backend("producer dropped sender".into()))?
                .map_err(|e| ConnectError::Backend(e.to_string()))?;

            // The downstream offset is only known once the broker acks, so the
            // full OffsetSync is assembled here rather than carrying a
            // placeholder through `pending`.
            let offset_sync = OffsetSync {
                topic,
                partition,
                upstream,
                downstream: DownstreamOffset(meta.offset),
            };

            // Write the offset-sync record to the target cluster.
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
                .map_err(|e| ConnectError::Backend(e.to_string()))?;

            self.offset_syncs.push(offset_sync);
        }

        self.producer
            .flush()
            .await
            .map_err(|e| ConnectError::Backend(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_connect::Sink;

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
            source_alias: "us-east".into(),
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
        })
        .await
        .unwrap();

        let allowed = ConnectRecord::new(
            None,
            Some(ReplicatedRecord {
                topic: "orders".into(),
                partition: PartitionIndex(0),
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

        assert_eq!(
            crate::test_util::topic_record_count(&target, "us-east.orders").await,
            1
        );
        assert_eq!(
            crate::test_util::topic_record_count(&target, "us-east.secret").await,
            0
        );

        let syncs = sink.drain_offset_syncs();
        assert!(
            syncs
                .iter()
                .any(|s| s.topic == "orders" && s.partition == 0 && s.upstream == 5)
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
            source_alias: "us-east".into(),
            naming: crate::config::NamingPolicy::Identity,
            target_zones: vec!["us".into()],
            policies: vec![],
            security: None,
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
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    }
}

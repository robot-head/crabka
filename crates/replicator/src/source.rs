//! Source side: a consumer on the source cluster that emits [`ReplicatedRecord`]s
//! and snapshots all partition positions as a [`SourceOffset`].

use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_connect::{ConnectError, ConnectRecord, OffsetMap, OffsetValue, Source, SourceOffset};
use crabka_units::prelude::Time;

use crate::{
    config::{ClientResourcePolicy, ReplicatorRuntimePolicy},
    ids::{Offset, PartitionIndex, Timestamp},
    record::ReplicatedRecord,
};

/// A [`Source`] implementation backed by a Kafka consumer on the source cluster.
///
/// This type wraps a [`Consumer`] and translates each
/// [`crabka_client_consumer::ConsumerRecord`] into a [`ReplicatedRecord`]. That
/// record carries the full envelope of topic, partition, offset, timestamp, and
/// headers next to the raw payload. The connect runtime receives the whole
/// `ReplicatedRecord` value, including the source coordinates that offset-sync
/// generation needs.
pub struct SourceConsumer {
    consumer: Option<Consumer>,
    buf: VecDeque<ReplicatedRecord>,
    /// Next offset to read per `"<topic>-<partition>"` key, that is,
    /// `last_offset + 1`.
    positions: BTreeMap<String, i64>,
    poll_timeout: Time,
}

/// Split a `"<topic>-<partition>"` checkpoint key back into its parts.
///
/// [`SourceConsumer::poll`] and [`checkpoint`] build the key as
/// `format!("{topic}-{partition}")`. Kafka topic names can themselves contain
/// `-`, so this function splits on the **last** `-` and parses the suffix as
/// the partition index. It returns `None` if there is no `-`, if the suffix is
/// not a valid `i32`, or if the topic part is empty.
///
/// [`checkpoint`]: SourceConsumer::checkpoint
fn split_topic_partition(key: &str) -> Option<(String, i32)> {
    let (topic, part) = key.rsplit_once('-')?;
    if topic.is_empty() {
        return None;
    }
    let partition: i32 = part.parse().ok()?;
    Some((topic.to_string(), partition))
}

impl SourceConsumer {
    /// Build and start a [`SourceConsumer`] that subscribes to `topics` on the
    /// cluster at `bootstrap` and joins `group_id`.
    ///
    /// Offsets reset to earliest, because the group has no previously committed
    /// offset. Pass `security` when the source cluster needs authentication or
    /// TLS.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Backend`] if the consumer cannot join the group.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(bootstrap = %bootstrap, group_id = %group_id, topics = topics.len()),
        err,
    )]
    pub async fn start(
        bootstrap: &str,
        group_id: &str,
        topics: &[String],
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ConnectError> {
        Self::start_with_policy(
            bootstrap,
            group_id,
            topics,
            security,
            ClientResourcePolicy::default(),
        )
        .await
    }

    /// Build with the deployment's client resource policy.
    ///
    /// # Errors
    ///
    /// Returns a [`ConnectError`] if the consumer cannot connect.
    pub async fn start_with_policy(
        bootstrap: &str,
        group_id: &str,
        topics: &[String],
        security: Option<crabka_client_core::security::ClientSecurity>,
        client_resource_policy: ClientResourcePolicy,
    ) -> Result<Self, ConnectError> {
        Self::start_with_runtime_policy(
            bootstrap,
            group_id,
            topics,
            security,
            client_resource_policy,
            &ReplicatorRuntimePolicy::default(),
        )
        .await
    }

    pub(crate) async fn start_with_runtime_policy(
        bootstrap: &str,
        group_id: &str,
        topics: &[String],
        security: Option<crabka_client_core::security::ClientSecurity>,
        client_resource_policy: ClientResourcePolicy,
        runtime_policy: &ReplicatorRuntimePolicy,
    ) -> Result<Self, ConnectError> {
        let builder = Consumer::builder()
            .bootstrap(bootstrap)
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .request_timeout(runtime_policy.client_request_timeout)
            .group_id(group_id)
            .subscribe(topics.to_vec())
            .auto_offset_reset(AutoOffsetReset::Earliest);

        let consumer = match security {
            Some(s) => builder.security(s).build().await,
            None => builder.build().await,
        }
        .map_err(|e| ConnectError::Backend(e.to_string()))?;

        Ok(Self {
            consumer: Some(consumer),
            buf: VecDeque::new(),
            positions: BTreeMap::new(),
            poll_timeout: runtime_policy.source_poll_timeout,
        })
    }
}

#[async_trait]
impl Source<(), ReplicatedRecord> for SourceConsumer {
    /// Poll the source cluster for the next record.
    ///
    /// Returns `Ok(None)` when the consumer is momentarily caught up. The
    /// runtime should back off and retry. Returns `Ok(Some(_))` with the
    /// next [`ReplicatedRecord`] in every other case.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Backend`] if the underlying consumer poll fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(fetched = tracing::field::Empty),
        err,
    )]
    async fn poll(&mut self) -> Result<Option<ConnectRecord<(), ReplicatedRecord>>, ConnectError> {
        if self.buf.is_empty() {
            let recs = self
                .consumer
                .as_mut()
                .ok_or_else(|| ConnectError::Backend("source consumer is closed".into()))?
                .poll(self.poll_timeout)
                .await
                .map_err(|e| ConnectError::Backend(e.to_string()))?;

            tracing::Span::current().record("fetched", recs.len());
            for r in recs {
                // Track the next offset to read (committed position = offset + 1).
                self.positions
                    .insert(format!("{}-{}", r.topic, r.partition), r.offset + 1);

                self.buf.push_back(ReplicatedRecord {
                    topic: r.topic,
                    partition: PartitionIndex(r.partition),
                    offset: Offset(r.offset),
                    timestamp: Timestamp(r.timestamp),
                    key: r.key,
                    value: r.value,
                    headers: r.headers.into_iter().map(|h| (h.key, h.value)).collect(),
                });
            }
        }

        Ok(self
            .buf
            .pop_front()
            .map(|payload| ConnectRecord::new(None, Some(payload))))
    }

    /// Snapshot the current read positions for all partitions seen so far.
    ///
    /// Returns `None` before the first successful poll, when there is nothing to
    /// commit yet.
    fn checkpoint(&self) -> Option<SourceOffset> {
        if self.positions.is_empty() {
            return None;
        }
        let position: OffsetMap = self
            .positions
            .iter()
            .map(|(k, v)| (k.clone(), OffsetValue::Long(*v)))
            .collect();
        Some(SourceOffset::new(BTreeMap::new().into(), position.into()))
    }

    /// Restore the read position from a previously-checkpointed [`SourceOffset`].
    ///
    /// The runtime calls this method once before the first [`poll`](Self::poll)
    /// and passes the position that it loaded from the durable checkpoint store
    /// on the target. Each `position` entry maps the key
    /// `"<topic>-<partition>"` to [`OffsetValue::Long`]`(next_offset)`, the
    /// value that [`checkpoint`](Self::checkpoint) wrote as `last_consumed + 1`.
    /// This method decodes each key back into `(topic, partition)` and hands the
    /// offset to the [`seek`](crabka_client_consumer::Consumer::seek) of the
    /// consumer.
    ///
    /// The consumer holds each seek as *pending*. It materialises the seek at
    /// the top of the first `poll` that sees the partition assigned, after the
    /// post-assignment offset prime of the group but before any `Fetch`. The
    /// sought offset is therefore the offset that the consumer fetches. A
    /// restart then resumes **from the last fully-committed record** and does
    /// not re-read the topic from offset 0. No record below the sought offset is
    /// re-delivered and no record above it is skipped, so there is no data gap.
    /// Delivery stays **at-least-once**: a crash between a sink flush and the
    /// checkpoint save can re-deliver the in-flight batch, but it never loses a
    /// record.
    ///
    /// This method skips a malformed key with a warning and does not fail the
    /// restore. A malformed key has no `-`, or a non-integer partition or
    /// offset. One corrupt entry must not strand recovery for the partitions
    /// that decoded cleanly.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Backend`] if the consumer is already closed.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(positions = offset.position.len()),
        err,
    )]
    async fn seek(&mut self, offset: SourceOffset) -> Result<(), ConnectError> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| ConnectError::Backend("source consumer is closed".into()))?;

        for (key, value) in offset.position.iter() {
            let OffsetValue::Long(next) = value else {
                tracing::warn!(key, "checkpoint position value is not a Long; skipping");
                continue;
            };
            let Some((topic, partition)) = split_topic_partition(key) else {
                tracing::warn!(
                    key,
                    "checkpoint position key is not '<topic>-<partition>'; skipping"
                );
                continue;
            };
            // Seed our local position view too, so a `checkpoint()` taken before
            // the first poll reflects the restored position rather than dropping
            // back to empty.
            self.positions.insert(key.clone(), *next);
            consumer
                .seek(topic, partition, *next)
                .await
                .map_err(|e| ConnectError::Backend(e.to_string()))?;
        }
        Ok(())
    }

    /// Close the underlying consumer. The close sends `LeaveGroup`, so a
    /// restarted replicator can rejoin the group immediately and does not wait
    /// out the session timeout of the departed member.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Backend`] if the consumer fails to close cleanly.
    #[tracing::instrument(level = "info", skip_all, err)]
    async fn close(&mut self) -> Result<(), ConnectError> {
        if let Some(consumer) = self.consumer.take() {
            consumer
                .close()
                .await
                .map_err(|e| ConnectError::Backend(e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crabka_connect::Source;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_polls_records_with_topic_and_offset() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let bootstrap = broker.listen_addr().to_string();

        crate::test_util::create_topic(&bootstrap, "orders", 1).await;
        crate::test_util::produce(&bootstrap, "orders", b"k", b"v").await;

        let mut src = SourceConsumer::start(
            &bootstrap,
            "crabka-replicator-flow1",
            &["orders".to_string()],
            None,
        )
        .await
        .unwrap();

        // Poll until the produced record surfaces. Bounded (not an open `loop`)
        // so the `poll -> Ok(None)` mutant — a source that never yields — fails
        // this test fast instead of spinning to the cargo-mutants timeout.
        let mut rec = None;
        for _ in 0..200 {
            if let Some(r) = src.poll().await.unwrap() {
                rec = Some(r);
                break;
            }
        }
        let rec = rec.expect("source did not yield the produced record");

        let payload = rec.value.unwrap();
        assert2::assert!(payload.topic.as_str() == "orders");
        assert2::assert!(payload.partition == PartitionIndex(0));
        assert2::assert!(payload.offset == Offset(0));
        assert2::assert!(payload.value.as_deref() == Some(b"v".as_slice()));

        // The checkpoint position is the NEXT offset to read: `last_offset + 1`.
        // Having consumed offset 0, the position for `orders-0` must be exactly
        // 1 (not 0 from `*1` or -1 from `-1`).
        let off = src.checkpoint().unwrap();
        assert2::assert!(off.position.get("orders-0") == Some(&OffsetValue::Long(1)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_takes_consumer_so_poll_fails_afterwards() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let bootstrap = broker.listen_addr().to_string();

        crate::test_util::create_topic(&bootstrap, "orders", 1).await;

        let mut src = SourceConsumer::start(
            &bootstrap,
            "crabka-replicator-flow-close",
            &["orders".to_string()],
            None,
        )
        .await
        .unwrap();

        // A real close takes (and closes) the inner consumer; after that the
        // consumer is `None`, so a subsequent poll must surface a backend error.
        // The `close -> Ok(())` mutant skips the take, leaving the consumer live
        // and the poll succeeding.
        src.close().await.unwrap();
        assert2::assert!(src.poll().await.is_err());
    }
}

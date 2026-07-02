//! Source side: a consumer on the source cluster that emits [`ReplicatedRecord`]s
//! and snapshots all partition positions as a [`SourceOffset`].

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use async_trait::async_trait;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_connect::{ConnectError, ConnectRecord, OffsetValue, Source, SourceOffset};

use crate::record::ReplicatedRecord;

/// A [`Source`] implementation backed by a Kafka consumer on the source cluster.
///
/// Wraps a [`Consumer`] and translates each [`crabka_client_consumer::ConsumerRecord`]
/// into a [`ReplicatedRecord`] that carries the full envelope (topic, partition,
/// offset, timestamp, headers) alongside the raw payload. The connect runtime
/// never sees topic/partition directly — only the `ReplicatedRecord` value.
pub struct SourceConsumer {
    consumer: Option<Consumer>,
    buf: VecDeque<ReplicatedRecord>,
    /// Next-offset-to-read per `"<topic>-<partition>"` key (i.e. `last_offset + 1`).
    positions: BTreeMap<String, i64>,
}

/// Split a `"<topic>-<partition>"` checkpoint key back into its parts.
///
/// The key is built by [`SourceConsumer::poll`] / [`checkpoint`] as
/// `format!("{topic}-{partition}")`. Kafka topic names may themselves contain
/// `-`, so we split on the **last** `-` and parse the suffix as the partition
/// index. Returns `None` if there is no `-`, the suffix is not a valid `i32`,
/// or the topic part is empty.
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
    /// Build and start a [`SourceConsumer`] subscribed to `topics` on the
    /// cluster at `bootstrap`, joining `group_id`.
    ///
    /// Offsets reset to earliest (no previously committed offset for the group).
    /// Pass `security` when the source cluster requires authentication/TLS.
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
        let builder = Consumer::builder()
            .bootstrap(bootstrap)
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
        })
    }
}

#[async_trait]
impl Source<(), ReplicatedRecord> for SourceConsumer {
    /// Poll the source cluster for the next record.
    ///
    /// Returns `Ok(None)` when the consumer is momentarily caught up (the
    /// runtime should back off and retry).  Returns `Ok(Some(_))` with the
    /// next [`ReplicatedRecord`] otherwise.
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
                .poll(Duration::from_millis(500))
                .await
                .map_err(|e| ConnectError::Backend(e.to_string()))?;

            tracing::Span::current().record("fetched", recs.len());
            for r in recs {
                // Track the next offset to read (committed position = offset + 1).
                self.positions
                    .insert(format!("{}-{}", r.topic, r.partition), r.offset + 1);

                self.buf.push_back(ReplicatedRecord {
                    topic: r.topic,
                    partition: r.partition,
                    offset: r.offset,
                    timestamp: r.timestamp,
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
    /// Returns `None` before the first successful poll (nothing to commit yet).
    fn checkpoint(&self) -> Option<SourceOffset> {
        if self.positions.is_empty() {
            return None;
        }
        let position = self
            .positions
            .iter()
            .map(|(k, v)| (k.clone(), OffsetValue::Long(*v)))
            .collect();
        Some(SourceOffset::new(BTreeMap::new(), position))
    }

    /// Restore the read position from a previously-checkpointed [`SourceOffset`].
    ///
    /// The runtime calls this once before the first [`poll`](Self::poll), passing
    /// the position loaded from the durable checkpoint store on the target. Each
    /// `position` entry is keyed `"<topic>-<partition>"` →
    /// [`OffsetValue::Long`]`(next_offset)` (the value [`checkpoint`](Self::checkpoint)
    /// wrote: `last_consumed + 1`). We decode each key back into `(topic,
    /// partition)` and hand the offset to the consumer's
    /// [`seek`](crabka_client_consumer::Consumer::seek).
    ///
    /// The consumer holds each seek as *pending* and materialises it at the top
    /// of the first `poll` that sees the partition assigned — after the group's
    /// post-assignment offset prime, but before any `Fetch` — so the sought
    /// offset is the one fetched. That makes restart resume **from the last
    /// fully-committed record** rather than re-reading the topic from offset 0:
    /// no record below the sought offset is re-delivered, and none above it is
    /// skipped (no data gap). Delivery remains **at-least-once** — a crash
    /// between a sink flush and the checkpoint save can re-deliver the in-flight
    /// batch, but never lose a record.
    ///
    /// A malformed key (no `-`, or a non-integer partition/offset) is skipped
    /// with a warning rather than failing the restore: one corrupt entry must
    /// not strand recovery for the partitions that decoded cleanly.
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

        for (key, value) in &offset.position {
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

    /// Close the underlying consumer, sending `LeaveGroup` so a restarted
    /// replicator can rejoin the group immediately instead of waiting out the
    /// departed member's session timeout.
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
    use assert2::assert;
    use assert2::check;

    use super::*;
    use crabka_connect::Source;

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
        check!(payload.topic == "orders");
        check!(payload.partition == 0);
        check!(payload.offset == 0);
        check!(payload.value.as_deref() == Some(b"v".as_slice()));

        // The checkpoint position is the NEXT offset to read: `last_offset + 1`.
        // Having consumed offset 0, the position for `orders-0` must be exactly
        // 1 (not 0 from `*1` or -1 from `-1`).
        let off = src.checkpoint().unwrap();
        assert!(off.position.get("orders-0") == Some(&OffsetValue::Long(1)));
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
        assert!(src.poll().await.is_err());
    }
}

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
    consumer: Consumer,
    buf: VecDeque<ReplicatedRecord>,
    /// Next-offset-to-read per `"<topic>-<partition>"` key (i.e. `last_offset + 1`).
    positions: BTreeMap<String, i64>,
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
            consumer,
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
    async fn poll(&mut self) -> Result<Option<ConnectRecord<(), ReplicatedRecord>>, ConnectError> {
        if self.buf.is_empty() {
            let recs = self
                .consumer
                .poll(Duration::from_millis(500))
                .await
                .map_err(|e| ConnectError::Backend(e.to_string()))?;

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

    /// No-op seek: the consumer uses committed group offsets on restart.
    ///
    /// A full `Consumer::seek` implementation is deferred; for now the group's
    /// committed offsets serve as the resume point.
    ///
    /// # Errors
    ///
    /// Always returns `Ok(())`.
    async fn seek(&mut self, _offset: SourceOffset) -> Result<(), ConnectError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

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

        let rec = loop {
            if let Some(r) = src.poll().await.unwrap() {
                break r;
            }
        };

        let payload = rec.value.unwrap();
        assert!(payload.topic == "orders");
        assert!(payload.partition == 0);
        assert!(payload.offset == 0);
        assert!(payload.value.as_deref() == Some(b"v".as_slice()));
    }
}

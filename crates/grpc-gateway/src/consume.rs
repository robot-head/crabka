//! Consume core: a group-subscribed session that yields records and commits
//! offsets. The streaming/poll wire (later plan) drives this. Records are
//! decoded through the codec on the way out.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use crabka_client_consumer::{AutoOffsetReset, Consumer, Header, IsolationLevel};

use crate::{
    codec::{RecordCodec, SchemaMeta},
    error::GatewayError,
    ids::{Offset, PartitionIndex, Timestamp},
};

/// Cap on buffered out-of-order acks per partition.
pub const MAX_PENDING_PER_PARTITION: usize = 100_000;

/// Recording an ack would exceed the pending cap.
#[derive(Debug)]
pub struct AckOverflow;

/// Per-partition observed-ack frontier. Commit value = the next offset after
/// the highest acked observed record before the first observed unacked record.
#[derive(Debug)]
pub struct PartitionAckState {
    /// Observed offsets at or beyond `next_committable_offset`.
    observed: BTreeSet<i64>,
    /// Acked/filtered offsets waiting for all earlier observed offsets to complete.
    completed: BTreeSet<i64>,
    /// Next offset that can be committed without passing an observed unacked record.
    next_committable_offset: i64,
    /// Highest next offset already committed; unchanged positions are not emitted.
    last_committed_offset: i64,
}

impl PartitionAckState {
    /// Create ack state from the next offset that may be committed once acked.
    #[must_use]
    pub fn new(next_committable_offset: i64) -> Self {
        assert!(
            next_committable_offset >= 0,
            "next committable offset must be non-negative"
        );
        Self {
            observed: BTreeSet::new(),
            completed: BTreeSet::new(),
            next_committable_offset,
            last_committed_offset: next_committable_offset,
        }
    }

    /// Record that Kafka delivered or filtered an offset. Unobserved numeric gaps
    /// are not blockers because Kafka offsets may be sparse.
    pub fn record_observed(&mut self, offset: i64) {
        if offset < self.next_committable_offset {
            return;
        }

        self.observed.insert(offset);
        self.advance_over_completed_observed_offsets();
    }

    /// Record an acknowledged offset, coalescing completed observed offsets.
    pub fn record(&mut self, offset: i64) -> Result<(), AckOverflow> {
        if offset < self.next_committable_offset {
            return Ok(());
        }

        if offset == self.next_committable_offset {
            self.observed.insert(offset);
        }

        if !self.completed.contains(&offset) && self.completed.len() >= MAX_PENDING_PER_PARTITION {
            return Err(AckOverflow);
        }
        self.completed.insert(offset);
        self.advance_over_completed_observed_offsets();
        Ok(())
    }

    fn advance_over_completed_observed_offsets(&mut self) {
        loop {
            let Some(offset) = self
                .observed
                .range(self.next_committable_offset..)
                .next()
                .copied()
            else {
                self.drop_offsets_before_frontier();
                return;
            };

            if !self.completed.remove(&offset) {
                self.drop_offsets_before_frontier();
                return;
            }

            self.observed.remove(&offset);
            self.next_committable_offset = offset + 1;
        }
    }

    fn drop_offsets_before_frontier(&mut self) {
        while self
            .completed
            .first()
            .is_some_and(|offset| *offset < self.next_committable_offset)
        {
            self.completed.pop_first();
        }
        while self
            .observed
            .first()
            .is_some_and(|offset| *offset < self.next_committable_offset)
        {
            self.observed.pop_first();
        }
    }

    /// The next-to-consume commit value, if the frontier advanced since the
    /// previous commit.
    #[must_use]
    pub fn commit_value(&self) -> Option<i64> {
        if self.last_committed_offset == self.next_committable_offset {
            return None;
        }
        Some(self.next_committable_offset)
    }

    /// Mark the current frontier as successfully committed.
    pub fn mark_committed(&mut self) {
        self.last_committed_offset = self.next_committable_offset;
    }
}

#[derive(Debug, Clone)]
pub struct DecodedConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp: Timestamp,
    pub key: Option<bytes::Bytes>,
    /// Original Kafka value bytes, exactly as fetched from the broker.
    pub raw_value: bytes::Bytes,
    /// Decoded payload bytes used for schema-aware evaluation.
    pub value: bytes::Bytes,
    pub headers: Vec<Header>,
    pub schema: Option<SchemaMeta>,
    pub json: Option<bytes::Bytes>,
}

pub struct ConsumeSession {
    /// Held in an `Option` so [`Drop`] can `take()` the consumer and tear down
    /// its background coordinator (see the `Drop` impl). Always `Some` while the
    /// session is alive; only `None` transiently inside `drop`.
    consumer: Option<Consumer>,
    codec: Arc<dyn RecordCodec>,
}

impl ConsumeSession {
    pub async fn new(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
        security: Option<crabka_client_core::security::ClientSecurity>,
        codec: Arc<dyn RecordCodec>,
    ) -> Result<Self, GatewayError> {
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .group_id(group_id.to_string())
            .subscribe(topics)
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .maybe_security(security)
            .build()
            .await?;
        Ok(Self {
            consumer: Some(consumer),
            codec,
        })
    }

    /// Poll a batch; record values are decoded through the codec.
    pub async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<DecodedConsumerRecord>, GatewayError> {
        let batch = self
            .consumer
            .as_mut()
            .expect("ConsumeSession polled after close")
            .poll(timeout)
            .await?;
        let mut decoded_batch = Vec::with_capacity(batch.len());
        for r in batch {
            let (raw_value, value, schema, json) = match r.value {
                Some(v) => {
                    let raw_value = v.clone();
                    let decoded = self.codec.decode(&r.topic, v).await?;
                    (raw_value, decoded.value, decoded.schema, decoded.json)
                }
                None => (bytes::Bytes::new(), bytes::Bytes::new(), None, None),
            };
            decoded_batch.push(DecodedConsumerRecord {
                topic: r.topic,
                partition: PartitionIndex(r.partition),
                offset: Offset(r.offset),
                timestamp: Timestamp(r.timestamp),
                key: r.key,
                raw_value,
                value,
                headers: r.headers,
                schema,
                json,
            });
        }
        Ok(decoded_batch)
    }

    /// Commit current positions (at-least-once: call after delivery is acked).
    pub async fn commit(&self) -> Result<(), GatewayError> {
        self.consumer
            .as_ref()
            .expect("ConsumeSession committed after close")
            .commit_sync()
            .await?;
        Ok(())
    }

    /// Commit explicit next offsets for the supplied topic partitions.
    pub async fn commit_offsets(
        &self,
        offsets: HashMap<(String, i32), i64>,
    ) -> Result<(), GatewayError> {
        self.consumer
            .as_ref()
            .expect("ConsumeSession committed after close")
            .commit_offsets_sync(offsets)
            .await?;
        Ok(())
    }
}

impl Drop for ConsumeSession {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            // The underlying `Consumer` runs a background coordinator task
            // (heartbeat + rebalance loop) that is torn down ONLY by
            // `Consumer::close()`. Merely dropping the consumer detaches that
            // task's `JoinHandle`, so it keeps heartbeating forever — leaking a
            // task + socket and orphaning a live group member (which stalls
            // rebalances for the rest of the group). Streaming drops sessions on
            // EVERY exit path (control-stream close/error, any break, or an
            // abrupt client disconnect dropping the response generator), so the
            // teardown belongs here.
            //
            // `close()` is async and consumes `self`, so spawn it detached. The
            // gateway always drops sessions inside the server's tokio runtime,
            // so a runtime is guaranteed to be available here.
            tokio::spawn(async move {
                let _ = consumer.close().await;
            });
        }
    }
}

#[cfg(test)]
mod ack_tests {
    use assert2::assert;

    use super::*;

    fn st(next_committable_offset: i64) -> PartitionAckState {
        PartitionAckState::new(next_committable_offset)
    }

    #[test]
    fn initial_frontier_is_not_committed_before_ack() {
        let state = st(5);

        assert!(state.commit_value().is_none());
    }

    #[test]
    fn first_ack_at_next_committable_offset_advances_frontier() {
        let mut state = st(5);

        state.record(5).unwrap();

        assert!(state.commit_value() == Some(6));
    }

    #[test]
    fn gap_first_ack_remains_pending_without_advancing_frontier() {
        let mut state = st(5);

        state.record(100).unwrap();

        assert!(state.commit_value().is_none());
        assert!(state.completed.contains(&100));
    }

    #[test]
    fn in_order_acks_advance() {
        let mut state = st(10);

        for offset in 10..=13 {
            state.record(offset).unwrap();
        }

        assert!(state.commit_value() == Some(14));
        assert!(state.completed.is_empty());
    }

    #[test]
    fn out_of_order_ack_above_gap_does_not_advance() {
        let mut state = st(10);
        state.record_observed(11);
        state.record_observed(12);

        state.record(10).unwrap();
        state.record(12).unwrap();

        assert!(state.commit_value() == Some(11));
        assert!(state.completed.contains(&12));
    }

    #[test]
    fn filling_the_gap_coalesces_in_one_drain() {
        let mut state = st(10);
        state.record_observed(12);
        state.record_observed(13);

        state.record(10).unwrap();
        state.record(12).unwrap();
        state.record(13).unwrap();
        state.record_observed(11);
        state.record(11).unwrap();

        assert!(state.commit_value() == Some(14));
        assert!(state.completed.is_empty());
    }

    #[test]
    fn below_frontier_ack_is_idempotent() {
        let mut state = st(10);

        state.record(10).unwrap();
        state.record(10).unwrap();
        state.record(3).unwrap();

        assert!(state.commit_value() == Some(11));
    }

    #[test]
    fn unchanged_frontier_not_recommitted() {
        let mut state = st(10);

        state.record(10).unwrap();
        state.last_committed_offset = 11;

        assert!(state.commit_value().is_none());
    }

    #[test]
    fn completed_sparse_observed_offsets_commit_past_unobserved_numeric_gaps() {
        let mut state = st(0);
        state.record_observed(0);
        state.record_observed(2);

        state.record(0).unwrap();
        state.record(2).unwrap();

        assert!(state.commit_value() == Some(3));
    }

    #[test]
    fn sparse_completed_offset_waits_for_earlier_observed_unacked_offset() {
        let mut state = st(0);
        state.record_observed(0);
        state.record_observed(2);

        state.record(2).unwrap();

        assert!(state.commit_value().is_none());
    }

    #[test]
    fn duplicate_pending_ack_is_idempotent_after_cap_is_full() {
        let mut state = st(0);
        state.record(0).unwrap();
        for offset in 2..(2 + i64::try_from(MAX_PENDING_PER_PARTITION).unwrap()) {
            state.record(offset).unwrap();
        }

        state.record(2).unwrap();

        assert!(state.completed.len() == MAX_PENDING_PER_PARTITION);
    }

    #[test]
    fn pending_cap_overflows() {
        let mut state = st(0);
        state.record(0).unwrap();
        for offset in 2..(2 + i64::try_from(MAX_PENDING_PER_PARTITION).unwrap()) {
            state.record(offset).unwrap();
        }

        assert!(let Err(AckOverflow) =
            state.record(2 + i64::try_from(MAX_PENDING_PER_PARTITION).unwrap()));
    }
}

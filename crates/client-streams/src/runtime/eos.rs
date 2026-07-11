//! Exactly-once (EOS v2 / KIP-447) primitives: the processing-guarantee config,
//! the transactional-producer I/O seam, and the streams group metadata used by
//! `send_offsets_to_transaction`. Wired into the runtime in T2/T3.

use async_trait::async_trait;

use crate::error::StreamsClientError;

/// Delivery guarantee for the runtime (`processing.guarantee`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProcessingGuarantee {
    /// Produce-then-commit; a crash mid-cycle may replay (the default).
    #[default]
    AtLeastOnce,
    /// Transactional: produce + offset-commit atomically (KIP-447).
    ExactlyOnceV2,
}

/// Streams group metadata for `send_offsets_to_transaction` (maps to the native
/// `crabka_client_consumer::ConsumerGroupMetadata`).
#[derive(Debug, Clone)]
pub struct StreamsGroupMeta {
    pub group: String,
    /// The member epoch (next-gen "generation").
    pub generation: i32,
    pub member: String,
    pub group_instance: Option<String>,
}

/// EOS-v2 transactional producer seam (DI'd; `BrokerTransactionalProducer` in
/// production, `MockTransactionalProducer` for tests). Also impls
/// `RecordProducer` for `send`.
#[async_trait]
pub trait TransactionalProducer: crate::runtime::io::RecordProducer {
    async fn init_transactions(&self) -> Result<(), StreamsClientError>;
    async fn begin_transaction(&self) -> Result<(), StreamsClientError>;
    async fn send_offsets_to_transaction(
        &self,
        offsets: &[(String, i32, i64)],
        group_meta: &StreamsGroupMeta,
    ) -> Result<(), StreamsClientError>;
    async fn commit_transaction(&self) -> Result<(), StreamsClientError>;
    async fn abort_transaction(&self) -> Result<(), StreamsClientError>;
}

/// KIP-447 transactional id: stable per (application, thread) so a restart fences
/// a zombie via the producer-epoch bump in `init_transactions`.
#[must_use]
pub fn transactional_id(application_id: &str, thread_idx: usize) -> String {
    format!("{application_id}-{thread_idx}")
}

#[cfg(test)]
pub(crate) mod mock {
    use std::sync::Mutex;

    use bytes::Bytes;

    use super::*;

    type SentRecord = (String, Option<i32>, Option<Bytes>, Option<Bytes>);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Step {
        Init,
        Begin,
        Send,
        SendOffsets,
        Commit,
        Abort,
    }

    /// Records the call sequence; `fail_at` makes the FIRST time that step is
    /// reached return an error (drives the abort/rollback tests).
    #[derive(Default)]
    pub struct MockTransactionalProducer {
        pub calls: Mutex<Vec<Step>>,
        /// `(topic, partition, key, value)` of each `send` — a test-only record log.
        pub sent: Mutex<Vec<SentRecord>>,
        pub fail_at: Mutex<Option<Step>>,
    }
    impl MockTransactionalProducer {
        fn record(&self, s: Step) -> Result<(), StreamsClientError> {
            self.calls.lock().unwrap().push(s);
            let mut f = self.fail_at.lock().unwrap();
            if *f == Some(s) {
                *f = None;
                return Err(StreamsClientError::Runtime(format!("mock fail at {s:?}")));
            }
            Ok(())
        }
    }
    #[async_trait]
    impl crate::runtime::io::RecordProducer for MockTransactionalProducer {
        async fn send(
            &self,
            topic: &str,
            partition: Option<i32>,
            key: Option<Bytes>,
            value: Option<Bytes>,
        ) -> Result<(), StreamsClientError> {
            self.sent
                .lock()
                .unwrap()
                .push((topic.to_string(), partition, key, value));
            self.record(Step::Send)
        }
        async fn flush(&self) -> Result<(), StreamsClientError> {
            Ok(())
        }
    }
    #[async_trait]
    impl TransactionalProducer for MockTransactionalProducer {
        async fn init_transactions(&self) -> Result<(), StreamsClientError> {
            self.record(Step::Init)
        }
        async fn begin_transaction(&self) -> Result<(), StreamsClientError> {
            self.record(Step::Begin)
        }
        async fn send_offsets_to_transaction(
            &self,
            _o: &[(String, i32, i64)],
            _m: &StreamsGroupMeta,
        ) -> Result<(), StreamsClientError> {
            self.record(Step::SendOffsets)
        }
        async fn commit_transaction(&self) -> Result<(), StreamsClientError> {
            self.record(Step::Commit)
        }
        async fn abort_transaction(&self) -> Result<(), StreamsClientError> {
            self.record(Step::Abort)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn transactional_id_is_stable_per_thread() {
        check!(transactional_id("word-count", 0) == "word-count-0");
        check!(transactional_id("word-count", 1) == "word-count-1");
    }

    #[tokio::test]
    async fn mock_records_calls_and_can_fail() {
        use mock::{MockTransactionalProducer, Step};

        use crate::runtime::io::RecordProducer;
        let p = MockTransactionalProducer {
            fail_at: std::sync::Mutex::new(Some(Step::Commit)),
            ..Default::default()
        };
        p.begin_transaction().await.unwrap();
        p.send("out", None, None, None).await.unwrap();
        check!(p.commit_transaction().await.is_err());
        check!(*p.calls.lock().unwrap() == vec![Step::Begin, Step::Send, Step::Commit]);
    }
}

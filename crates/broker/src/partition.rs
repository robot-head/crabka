//! A single partition's runtime handle. Owned by the partition registry
//! inside `Broker`. The handle gives any task:
//!
//! - read access to the partition's [`Log`] via `Arc<Mutex<Log>>`
//! - write access via a `mpsc::Sender<ProduceJob>` (a single writer task
//!   drains the channel; see `partition_writer.rs`)
//! - a [`Notify`] that fires after every successful append, used by
//!   long-poll Fetch to wake when new data arrives.

// Wired into the partition registry / Produce + Fetch handlers in later
// batches. Suppress the temporary dead-code warning until then.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use crabka_log::Log;
use crabka_protocol::records::RecordBatch;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::error::BrokerError;

/// Message sent from a Produce handler to the partition's writer task.
#[derive(Debug)]
pub struct ProduceJob {
    /// The batch to append. The writer mutates `base_offset` before append.
    pub batch: RecordBatch,
    /// Oneshot for the writer to report success (base offset assigned)
    /// or failure back to the handler.
    pub ack: oneshot::Sender<Result<i64, BrokerError>>,
}

/// Runtime handle for a single partition.
///
/// Cheap to clone — `log`, `writer_tx`, `append_notify` are all `Arc`-ish
/// and the writer handle isn't cloned (`Arc<JoinHandle<()>>` wraps it).
#[derive(Clone)]
// `partition_id` mirrors Kafka's wire naming and is the conventional term
// used throughout the broker; renaming to `id` would shadow `Partition`'s
// own identity at every call site.
#[allow(clippy::struct_field_names)]
pub struct Partition {
    pub topic: String,
    pub partition_id: i32,
    pub log: Arc<Mutex<Log>>,
    pub writer_tx: mpsc::Sender<ProduceJob>,
    pub append_notify: Arc<Notify>,
    /// Held so the writer task is reaped when every Partition handle is
    /// dropped. Not used directly.
    pub _writer_handle: Arc<JoinHandle<()>>,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately does NOT include `log` — formatting a `Mutex<Log>`
        // would block on the mutex and dump internal segment state into
        // tracing output.
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("partition_id", &self.partition_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_log::LogConfig;
    use tempfile::tempdir;

    #[test]
    fn partition_is_clone_and_send() {
        // Compile-time check.
        fn assert_send<T: Send>() {}
        fn assert_clone<T: Clone>() {}
        assert_send::<Partition>();
        assert_clone::<Partition>();
    }

    #[tokio::test]
    async fn debug_does_not_dump_log() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<ProduceJob>(1);
        let writer = tokio::spawn(async {});
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            _writer_handle: Arc::new(writer),
        };
        let s = format!("{p:?}");
        assert!(s.contains("topic"));
        assert!(s.contains("partition_id"));
        // The mutex/log internals must NOT appear in Debug output.
        assert!(!s.contains("Mutex"));
        assert!(!s.contains("segments"));
    }
}

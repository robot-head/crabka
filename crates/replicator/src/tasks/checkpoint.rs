//! Periodic checkpoint task: translates and emits consumer-group offset checkpoints.
//!
//! Reads committed consumer-group offsets from the SOURCE cluster, translates
//! them to TARGET offsets via an [`OffsetSyncStore`], and writes MM2-compatible
//! [`Checkpoint`] records to `<source>.checkpoints.internal` on the TARGET cluster.

use bytes::Bytes;
use crabka_client_admin::AdminClient;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use tracing::warn;

use crate::{
    config::NamingPolicy,
    error::ReplicatorError,
    ids::{CommittedOffset, PartitionIndex},
    mm2::{Checkpoint, OffsetSync},
    naming::Renamer,
    offset_sync_store::OffsetSyncStore,
    selector::Selector,
};

/// Parameters required to run the checkpoint task.
pub struct CheckpointParams {
    /// Bootstrap address of the source cluster (where consumer-group offsets live).
    pub source_bootstrap: String,
    /// Bootstrap address of the target cluster (where checkpoints are written).
    pub target_bootstrap: String,
    /// Alias of the source cluster (used to derive MM2 topic names).
    pub source_alias: String,
    /// Naming policy for the flow, used to rename the checkpoint's topic to the
    /// remote name a failed-over consumer reads (MM2 `RemoteClusterUtils` parity).
    pub naming: NamingPolicy,
    /// Selector for which consumer groups to checkpoint.
    pub group_selector: Selector,
    /// Optional TLS/SASL security applied to the target producer + admin.
    pub security: Option<crabka_client_core::security::ClientSecurity>,
}

/// One translation pass: reads source group offsets, translates via `store`,
/// and writes [`Checkpoint`] records to the target checkpoints topic.
///
/// # Errors
/// Returns [`ReplicatorError`] on topic-ensure, admin-connect, or produce failures.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(source_alias = %params.source_alias, groups = tracing::field::Empty),
    err,
)]
pub async fn run_once(
    params: &CheckpointParams,
    store: &OffsetSyncStore,
) -> Result<(), ReplicatorError> {
    let checkpoint_topic = Checkpoint::topic_name(&params.source_alias);

    // 1. Ensure the checkpoints topic exists on the target.
    crate::admin_util::ensure_topic(
        &params.target_bootstrap,
        &checkpoint_topic,
        1,
        params.security.clone(),
    )
    .await
    .map_err(|e| ReplicatorError::Client(format!("ensure checkpoints topic: {e}")))?;

    // 2. Connect to the source cluster to list group offsets.
    let mut admin = AdminClient::connect_secured(
        std::slice::from_ref(&params.source_bootstrap),
        None, // source cluster: no security (security param applies to target only)
    )
    .await
    .map_err(|e| ReplicatorError::Client(format!("source admin connect: {e}")))?;

    // 3. List and filter consumer groups; skip our own internal groups.
    let all_groups = admin
        .list_groups()
        .await
        .map_err(|e| ReplicatorError::Client(format!("list_groups: {e}")))?;

    let groups: Vec<String> = all_groups
        .into_iter()
        .filter(|g| !g.starts_with("crabka-replicator-"))
        .filter(|g| params.group_selector.matches(g))
        .collect();
    tracing::Span::current().record("groups", groups.len());

    // 4. Build a producer to write checkpoint records to the target.
    let producer = build_producer(&params.target_bootstrap, params.security.clone())
        .await
        .map_err(|e| ReplicatorError::Client(format!("build target producer: {e}")))?;

    // 5. For each matched group, translate committed offsets and produce checkpoints.
    // The checkpoint's topic is the REMOTE (renamed) topic a failed-over consumer
    // reads on the target, matching MM2's `renameTopicPartition`. Offset-sync
    // translation still keys on the SOURCE topic name.
    let renamer = Renamer::new(params.naming, &params.source_alias);
    for group in &groups {
        let offsets = match admin.list_consumer_group_offsets(group).await {
            Ok(o) => o,
            Err(e) => {
                warn!(group = %group, error = %e, "list_consumer_group_offsets failed; skipping group");
                continue;
            }
        };

        for ((topic, partition), committed) in offsets {
            // `offsets` comes from the cross-crate admin client as raw
            // `(i32, i64)`; wrap at this boundary so the translation call and
            // the checkpoint fields are type-checked.
            let partition = PartitionIndex(partition);
            let committed = CommittedOffset(committed);
            let Some(downstream) = store.translate(&topic, partition, committed) else {
                continue;
            };

            let checkpoint = Checkpoint {
                group: group.clone(),
                topic: renamer.target_name(&topic),
                partition,
                // A group's committed source offset is the upstream side of the
                // checkpoint; the translated target offset is the downstream.
                upstream: committed.into(),
                downstream,
                metadata: String::new(),
            };

            let _rx = producer
                .send(ProducerRecord {
                    topic: checkpoint_topic.clone(),
                    partition: None,
                    key: Some(Bytes::from(checkpoint.key_bytes())),
                    value: Some(Bytes::from(checkpoint.value_bytes())),
                    headers: Vec::new(),
                    timestamp_ms: None,
                })
                .await;
        }
    }

    // 6. Flush to ensure all checkpoint records are durably written.
    producer
        .flush()
        .await
        .map_err(|e| ReplicatorError::Client(format!("producer flush: {e}")))?;

    Ok(())
}

/// Background task that periodically rebuilds the [`OffsetSyncStore`] from the
/// target cluster's offset-syncs topic and then calls [`run_once`].
pub struct CheckpointTask {
    handle: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl CheckpointTask {
    /// Start the checkpoint loop with the given interval.
    ///
    /// Each cycle:
    /// 1. Reads all records from `mm2-offset-syncs.<source>.internal` on the TARGET.
    /// 2. Builds a fresh [`OffsetSyncStore`].
    /// 3. Calls [`run_once`] to write translated checkpoints.
    ///
    /// Errors in any cycle are logged as warnings; the loop continues until shutdown.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(source_alias = %params.source_alias, interval_ms = interval.as_millis()),
    )]
    pub fn start(params: CheckpointParams, interval: std::time::Duration) -> Self {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            loop {
                // Build OffsetSyncStore from target offset-syncs topic.
                let mut store = OffsetSyncStore::default();
                let offset_syncs_topic = OffsetSync::topic_name(&params.source_alias);

                match crate::admin_util::read_all(
                    &params.target_bootstrap,
                    &offset_syncs_topic,
                    params.security.clone(),
                )
                .await
                {
                    Ok(records) => {
                        for (k, v) in records {
                            if let (Some(k), Some(v)) = (k, v) {
                                match OffsetSync::from_bytes(&k, &v) {
                                    Ok(os) => store.ingest(os),
                                    Err(e) => {
                                        warn!(error = %e, "failed to decode offset-sync record; skipping");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to read offset-syncs topic; skipping cycle");
                    }
                }

                if let Err(e) = run_once(&params, &store).await {
                    warn!(error = %e, "checkpoint run_once failed");
                }

                // Wait for the next interval or a shutdown signal.
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        Self {
            handle,
            shutdown: shutdown_tx,
        }
    }

    /// Signal the task to stop and await its completion.
    // cargo-mutants: shutdown signal/join not asserted by unit tests
    #[cfg_attr(test, mutants::skip)]
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.handle.await;
    }
}

/// Build a non-idempotent producer with `acks=All` targeting the given bootstrap.
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        ids::{DownstreamOffset, UpstreamOffset},
        mm2::{Checkpoint, OffsetSync},
        offset_sync_store::OffsetSyncStore,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_translated_checkpoints() {
        let s_dir = tempfile::TempDir::new().unwrap();
        let t_dir = tempfile::TempDir::new().unwrap();
        let source = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            s_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            t_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let sb = source.listen_addr().to_string();
        let tb = target.listen_addr().to_string();

        crate::test_util::create_topic(&sb, "orders", 1).await;
        for _ in 0..200 {
            crate::test_util::produce(&sb, "orders", b"k", b"v").await;
        }
        crate::test_util::commit_group(&sb, "g", "orders").await;

        let mut syncs = OffsetSyncStore::default();
        syncs.ingest(OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(0),
            downstream: DownstreamOffset(0),
        });
        syncs.ingest(OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(200),
            downstream: DownstreamOffset(165),
        });

        run_once(
            &CheckpointParams {
                source_bootstrap: sb,
                target_bootstrap: tb.clone(),
                source_alias: "us-east".into(),
                naming: NamingPolicy::Default,
                group_selector: Selector::compile(&["g".into()], &[]).unwrap(),
                security: None,
            },
            &syncs,
        )
        .await
        .unwrap();

        let raw = crate::admin_util::read_last_value_for_key(
            &tb,
            &Checkpoint::topic_name("us-east"),
            b"",
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert2::assert!(!raw.is_empty());
    }
}

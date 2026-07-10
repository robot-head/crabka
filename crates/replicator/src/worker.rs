//! Per-flow replication worker: drives one directional connect pipeline
//! (`SourceConsumer` -> `TargetSink`) plus the heartbeat and checkpoint
//! background tasks, with build-retry resilience and clean shutdown.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crabka_connect::{ConnectorRuntime, RuntimeState};
use tracing::warn;

use crate::{
    checkpoint_store::InternalTopicCheckpointStore,
    config::{NamingPolicy, PolicyConfig},
    record::ReplicatedRecord,
    selector::Selector,
    sink::{SinkParams, TargetSink},
    source::SourceConsumer,
    tasks::{
        checkpoint::{CheckpointParams, CheckpointTask},
        heartbeat::{HeartbeatParams, HeartbeatTask},
    },
};

/// Maximum wall-clock time to keep retrying the pipeline build before giving up.
const MAX_BUILD_ELAPSED: Duration = Duration::from_secs(30);
/// Initial backoff between build attempts; doubles each retry up to [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
/// Cap on the per-retry backoff interval.
const MAX_BACKOFF: Duration = Duration::from_secs(8);

/// Parameters to start a [`FlowWorker`]. The supervisor resolves selectors into
/// a concrete topic list and passes the per-flow cluster addresses, aliases,
/// naming policy, residency policies, and security here.
pub struct FlowWorkerParams {
    /// Unique flow name (e.g. `"us-east__eu-west"`); seeds the consumer group id
    /// and the checkpoint-store key.
    pub flow_name: String,
    /// Bootstrap address of the source cluster.
    pub source_bootstrap: String,
    /// Bootstrap address of the target cluster.
    pub target_bootstrap: String,
    /// Alias of the source cluster (stamped as provenance / used for MM2 names).
    pub source_alias: String,
    /// Alias of the target cluster (written into heartbeat records).
    pub target_alias: String,
    /// How source topics are renamed on the target.
    pub naming: NamingPolicy,
    /// Already-resolved source topic list (supervisor resolves selectors).
    pub topics: Vec<String>,
    /// Compliance zones of the target cluster (used for residency checks).
    pub target_zones: Vec<String>,
    /// Residency policies to enforce on the sink.
    pub policies: Vec<PolicyConfig>,
    /// Selector for which consumer groups the checkpoint task translates.
    pub group_selector: Selector,
    /// Optional TLS/SASL security for the source cluster.
    pub security_source: Option<crabka_client_core::security::ClientSecurity>,
    /// Optional TLS/SASL security for the target cluster.
    pub security_target: Option<crabka_client_core::security::ClientSecurity>,
}

/// One directional replication flow: the running connect runtime plus the
/// heartbeat and checkpoint background tasks.
pub struct FlowWorker {
    runtime: crabka_connect::ConnectorHandle,
    heartbeat: HeartbeatTask,
    checkpoint: CheckpointTask,
}

/// Current wall-clock time in milliseconds since the Unix epoch.
fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// Compute the next build-retry backoff: double the current interval, capped at
/// [`MAX_BACKOFF`].
fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(MAX_BACKOFF)
}

impl FlowWorker {
    /// Build and start the pipeline, retrying transient build failures with
    /// bounded exponential backoff (a cluster may be briefly unreachable).
    ///
    /// Backoff starts at ~250ms and doubles (capped at ~8s) until the cumulative
    /// elapsed time exceeds ~30s, after which the last build error is returned.
    ///
    /// # Errors
    ///
    /// Returns the last build error if the pipeline cannot be constructed within
    /// the retry budget.
    // cargo-mutants: retry/backoff loop has no value-asserting unit coverage
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(flow = %p.flow_name, source = %p.source_alias, target = %p.target_alias, topics = p.topics.len()),
        err,
    )]
    pub async fn start(p: FlowWorkerParams) -> crate::Result<Self> {
        let mut backoff = INITIAL_BACKOFF;
        let mut elapsed = Duration::ZERO;

        loop {
            match Self::build(&p).await {
                Ok(worker) => return Ok(worker),
                Err(e) => {
                    if elapsed >= MAX_BUILD_ELAPSED {
                        return Err(e);
                    }
                    warn!(
                        flow = %p.flow_name,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "flow worker build failed; retrying after backoff"
                    );
                    tokio::time::sleep(backoff).await;
                    elapsed += backoff;
                    backoff = next_backoff(backoff);
                }
            }
        }
    }

    /// A single build attempt: stand up the source, sink, checkpoint store,
    /// connect runtime, and both background tasks.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(flow = %p.flow_name, group_id = tracing::field::Empty),
        err,
    )]
    async fn build(p: &FlowWorkerParams) -> crate::Result<Self> {
        let group_id = format!("crabka-replicator-{}", p.flow_name);
        tracing::Span::current().record("group_id", group_id.as_str());

        let source = SourceConsumer::start(
            &p.source_bootstrap,
            &group_id,
            &p.topics,
            p.security_source.clone(),
        )
        .await?;

        let sink = TargetSink::start(SinkParams {
            target_bootstrap: p.target_bootstrap.clone(),
            source_alias: p.source_alias.clone(),
            naming: p.naming,
            target_zones: p.target_zones.clone(),
            policies: p.policies.clone(),
            security: p.security_target.clone(),
        })
        .await?;

        let store = InternalTopicCheckpointStore::start(
            &p.target_bootstrap,
            &p.flow_name,
            p.security_target.clone(),
        )
        .await?;

        let runtime = ConnectorRuntime::<(), ReplicatedRecord>::new()
            .add_source(source)
            .add_sink(sink)
            .checkpoint_store(Arc::new(store))
            .commit_interval(Duration::from_millis(500))
            .max_batch(500)
            .run();

        let heartbeat = HeartbeatTask::start(HeartbeatParams {
            target_bootstrap: p.target_bootstrap.clone(),
            source_alias: p.source_alias.clone(),
            target_alias: p.target_alias.clone(),
            interval: Duration::from_secs(1),
            now_ms,
            sleeper: Arc::new(qubit_clock::sleep::SystemSleeper::new()),
            security: p.security_target.clone(),
        })
        .await?;

        let checkpoint = CheckpointTask::start(
            CheckpointParams {
                source_bootstrap: p.source_bootstrap.clone(),
                target_bootstrap: p.target_bootstrap.clone(),
                source_alias: p.source_alias.clone(),
                naming: p.naming,
                group_selector: p.group_selector.clone(),
                security: p.security_target.clone(),
            },
            Duration::from_secs(5),
        )
        .await?;

        Ok(Self {
            runtime,
            heartbeat,
            checkpoint,
        })
    }

    /// Current connect-runtime state, for the supervisor to detect `Failed`
    /// workers and restart them.
    #[must_use]
    pub fn state(&self) -> RuntimeState {
        self.runtime.state()
    }

    /// Graceful stop: drain and shut down the connect runtime, then stop both
    /// background tasks.
    // cargo-mutants: shutdown ordering not asserted by unit tests
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn shutdown(self) {
        let _ = self.runtime.shutdown().await;
        self.heartbeat.shutdown().await;
        self.checkpoint.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn now_ms_is_a_recent_epoch_millis() {
        // Epoch millis for 2023-11-14T22:13:20Z; any sane current time exceeds
        // it. The `-> -1 / 0 / 1` mutants all fall below this floor.
        assert!(super::now_ms() > 1_700_000_000_000);
    }

    #[test]
    fn next_backoff_doubles_and_caps() {
        // Doubling: 250ms -> 500ms (the `*2`→`/2` mutant gives 125ms).
        assert_eq!(
            super::next_backoff(Duration::from_millis(250)),
            Duration::from_millis(500)
        );
        // Cap: never exceeds MAX_BACKOFF even when already at the cap.
        assert_eq!(super::next_backoff(MAX_BACKOFF), MAX_BACKOFF);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_replicates_one_flow() {
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
        crate::test_util::produce(&sb, "orders", b"k", b"v").await;

        let worker = FlowWorker::start(FlowWorkerParams {
            flow_name: "us-east__eu-west".into(),
            source_bootstrap: sb,
            target_bootstrap: tb.clone(),
            source_alias: "us-east".into(),
            target_alias: "eu-west".into(),
            naming: crate::config::NamingPolicy::Default,
            topics: vec!["orders".to_string()],
            target_zones: vec!["us".into()],
            policies: vec![],
            group_selector: crate::selector::Selector::compile(&[], &[]).unwrap(),
            security_source: None,
            security_target: None,
        })
        .await
        .unwrap();

        crate::test_util::await_topic_count(
            &tb,
            "us-east.orders",
            1,
            std::time::Duration::from_secs(15),
        )
        .await;

        worker.shutdown().await;
        assert!(crate::test_util::topic_record_count(&tb, "us-east.orders").await >= 1);
    }
}

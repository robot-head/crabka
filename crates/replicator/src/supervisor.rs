//! Supervisor: top-level task tree that owns and restarts replication workers.
//!
//! The [`FlowSupervisor`] turns a validated [`ReplicatorConfig`] into one
//! running [`FlowWorker`] per flow, then runs a background supervision loop that
//! restarts any worker whose connect runtime has entered
//! [`RuntimeState::Failed`].

use crabka_client_admin::AdminClient;
use crabka_connect::RuntimeState;
use crabka_units::prelude::{Time, TimeExt as _, secs};
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    config::{NamingPolicy, PolicyConfig, ReplicatorConfig},
    error::ReplicatorError,
    naming::Renamer,
    selector::Selector,
    worker::{FlowWorker, FlowWorkerParams},
};

/// How often the supervision loop polls each worker's runtime state.
const SUPERVISE_INTERVAL: Time = secs(3);

/// Owned, `Clone`-able inputs needed to rebuild a [`FlowWorkerParams`] (and thus
/// a fresh [`FlowWorker`]) when the supervision loop restarts a failed worker.
///
/// `FlowWorkerParams` itself lives in another module and is not `Clone`, so we
/// keep the resolved inputs here and reconstruct the params via [`make_params`].
#[derive(Clone)]
struct RebuildSpec {
    flow_name: String,
    source_bootstrap: String,
    target_bootstrap: String,
    source_alias: String,
    target_alias: String,
    naming: NamingPolicy,
    topics: Vec<String>,
    target_zones: Vec<String>,
    policies: Vec<PolicyConfig>,
    group_selector: Selector,
}

/// Build a fresh [`FlowWorkerParams`] from a [`RebuildSpec`]. All security is
/// `None` in Slice 1 (plaintext).
fn make_params(spec: &RebuildSpec) -> FlowWorkerParams {
    FlowWorkerParams {
        flow_name: spec.flow_name.clone(),
        source_bootstrap: spec.source_bootstrap.clone(),
        target_bootstrap: spec.target_bootstrap.clone(),
        source_alias: spec.source_alias.clone(),
        target_alias: spec.target_alias.clone(),
        naming: spec.naming,
        topics: spec.topics.clone(),
        target_zones: spec.target_zones.clone(),
        policies: spec.policies.clone(),
        group_selector: spec.group_selector.clone(),
        security_source: None,
        security_target: None,
    }
}

/// Returns `true` if `name` is a Kafka or replicator internal topic that must
/// never be selected as a replication source.
///
/// Excludes Kafka internals (anything starting with `__`) and the replicator's
/// own state topics: heartbeats, the consolidated offsets topic, per-flow
/// checkpoint topics (`*.checkpoints.internal`), and MM2 offset-sync topics
/// (`mm2-offset-syncs.*`).
fn is_internal(name: &str) -> bool {
    name.starts_with("__")
        || name == "heartbeats"
        || name == "crabka-replicator-offsets"
        || name.ends_with(".checkpoints.internal")
        || name.starts_with("mm2-offset-syncs.")
}

/// Control plane that owns and supervises the per-flow replication workers.
pub struct FlowSupervisor {
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl FlowSupervisor {
    /// Validate `config`, resolve and start one [`FlowWorker`] per flow, then
    /// spawn a supervision loop that owns the workers and restarts any that
    /// enter [`RuntimeState::Failed`].
    ///
    /// For each flow the source cluster's topics are listed and filtered by the
    /// flow's topic selector, excluding already-remote topics (default-naming
    /// loop guard) and Kafka/replicator internal topics.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicatorError`] if the config is invalid, a source cluster
    /// cannot be reached for topic discovery, a selector fails to compile, or an
    /// initial worker fails to build within its retry budget.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(flows = config.flows.len(), clusters = config.clusters.len()),
        err,
    )]
    pub async fn run(config: ReplicatorConfig) -> crate::Result<Self> {
        config.validate()?;

        let mut entries: Vec<(RebuildSpec, FlowWorker)> = Vec::with_capacity(config.flows.len());

        for flow in &config.flows {
            // validate() guarantees both aliases resolve.
            let from = &config.clusters[&flow.from];
            let to = &config.clusters[&flow.to];

            let flow_name = format!("{}__{}", flow.from, flow.to);

            // Resolve the concrete topic list from the source cluster's metadata.
            let mut admin = AdminClient::connect(std::slice::from_ref(&from.bootstrap))
                .await
                .map_err(|e| ReplicatorError::Client(e.to_string()))?;
            let md = admin
                .metadata(&[])
                .await
                .map_err(|e| ReplicatorError::Client(e.to_string()))?;

            let topic_sel = Selector::compile(&flow.topics.include, &flow.topics.exclude)?;
            let renamer = Renamer::new(flow.naming, &flow.from);

            let mut topics: Vec<String> = md
                .topics
                .into_iter()
                .map(|t| t.name)
                .filter(|name| {
                    topic_sel.matches(name) && !renamer.is_remote(name) && !is_internal(name)
                })
                .collect();
            topics.sort();
            topics.dedup();

            let group_selector = Selector::compile(&flow.groups.include, &flow.groups.exclude)?;

            let spec = RebuildSpec {
                flow_name: flow_name.clone(),
                source_bootstrap: from.bootstrap.clone(),
                target_bootstrap: to.bootstrap.clone(),
                source_alias: flow.from.clone(),
                target_alias: flow.to.clone(),
                naming: flow.naming,
                topics,
                target_zones: to.zones.clone(),
                policies: config.policies.clone(),
                group_selector,
            };

            let worker = Box::pin(FlowWorker::start(make_params(&spec))).await?;
            tracing::info!(
                flow = %flow_name,
                source = %flow.from,
                target = %flow.to,
                topics = spec.topics.len(),
                "started flow worker",
            );
            entries.push((spec, worker));
        }

        let (shutdown, mut rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(SUPERVISE_INTERVAL.to_std());
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    res = rx.changed() => {
                        // Sender dropped or shutdown signalled — stop supervising.
                        if res.is_err() || *rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        for (spec, worker) in &mut entries {
                            if worker.state() == RuntimeState::Failed {
                                tracing::warn!(
                                    flow = %spec.flow_name,
                                    "flow worker failed; rebuilding",
                                );
                                // Take the failed worker out, stop it, and
                                // replace it with a freshly built one.
                                let old = std::mem::replace(
                                    worker,
                            match Box::pin(FlowWorker::start(make_params(spec))).await {
                                        Ok(w) => w,
                                        Err(e) => {
                                            tracing::error!(
                                                flow = %spec.flow_name,
                                                error = %e,
                                                "failed to rebuild flow worker; \
                                                 will retry next tick",
                                            );
                                            continue;
                                        }
                                    },
                                );
                                old.shutdown().await;
                            }
                        }
                    }
                }
            }

            for (_spec, worker) in entries {
                worker.shutdown().await;
            }
        });

        Ok(Self { shutdown, handle })
    }

    /// Signal the supervision loop to stop and gracefully shut down all workers.
    // cargo-mutants: shutdown signal/join not asserted by unit tests
    #[cfg_attr(test, mutants::skip)]
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.handle.await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::{
        ClusterConfig, Delivery, FlowConfig, NamingPolicy, ReplicatorConfig, Selectors,
    };

    #[test]
    fn is_internal_excludes_kafka_and_replicator_topics() {
        for (topic, want) in [
            ("__consumer_offsets", true),
            ("heartbeats", true),
            ("crabka-replicator-offsets", true),
            ("us-east.eu-west.checkpoints.internal", true),
            ("mm2-offset-syncs.us-east.internal", true),
            ("orders", false),
            ("telemetry.cpu", false),
        ] {
            assert2::assert!(is_internal(topic) == want);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawns_a_worker_per_flow_and_replicates() {
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

        // A second LOCAL topic that is non-remote and non-internal but NOT in the
        // flow's `topics.include`. It pins the topic filter's `selector.matches`
        // conjunct: with `&&`→`||` the filter would replicate `noise` (since it
        // is !remote && !internal), so `us-east.noise` must stay absent.
        crate::test_util::create_topic(&sb, "noise", 1).await;
        crate::test_util::produce(&sb, "noise", b"k", b"v").await;

        let mut clusters = BTreeMap::new();
        clusters.insert(
            "us-east".to_string(),
            ClusterConfig {
                bootstrap: sb.clone(),
                region: "us".into(),
                zones: vec!["us".into()],
            },
        );
        clusters.insert(
            "eu-west".to_string(),
            ClusterConfig {
                bootstrap: tb.clone(),
                region: "eu".into(),
                zones: vec!["eu".into(), "gdpr".into()],
            },
        );
        let config = ReplicatorConfig {
            clusters,
            flows: vec![FlowConfig {
                from: "us-east".into(),
                to: "eu-west".into(),
                topics: Selectors {
                    include: vec!["orders".into()],
                    exclude: vec![],
                },
                groups: Selectors::default(),
                naming: NamingPolicy::Default,
                delivery: Delivery::AtLeastOnce,
            }],
            policies: vec![],
        };

        let sup = FlowSupervisor::run(config).await.unwrap();
        crate::test_util::await_topic_count(&tb, "us-east.orders", 1, secs(20)).await;
        sup.shutdown().await;
        assert2::assert!(crate::test_util::topic_record_count(&tb, "us-east.orders").await >= 1);
        // `noise` was excluded by the selector, so it must never have been
        // replicated to the target.
        assert2::assert!(crate::test_util::topic_record_count(&tb, "us-east.noise").await == 0);
    }
}

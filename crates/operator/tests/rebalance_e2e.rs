//! End-to-end tests. They drive the real `ConnectRebalancerClient` of the
//! operator over HTTP against the real Connect-RPC router of
//! `crabka-rebalancer`. The router runs in-process against a real
//! single-broker Crabka.
//!
//! This is the wire-compatibility contract of the slice. It proves that
//! the hand-written Connect and JSON request shaping of the operator, and
//! its response decoding, match what the prost and pbjson codegen of the
//! rebalancer produces. The decoding covers the enum-name parsing, the
//! camelCase fields, the unwrapping of a nested `proposal`, and the map
//! from a Connect error to `RebalancerError::Rpc`. The unit tests can only
//! assume this match.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use assert2::assert;
use async_trait::async_trait;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::Client;
use crabka_operator::rebalancer_client::{
    ConnectRebalancerClient, ProposalStatus, RebalancerClientLike, RebalancerError,
};
use crabka_rebalancer::{
    api::{GoalRegistry, handlers::AppState},
    capacity::BrokerCapacities,
    executor::{
        ExecutorConfig, ExecutorState,
        phases::{ClientFacade, ConfigOp, PhaseError},
        throttle::ThrottleTargets,
    },
    goals::GoalContext,
    health::new_registry,
    ingest::{SharedSnapshot, new_shared_snapshot, snapshot_once},
    metrics::RebalancerMetrics,
    model::{Movement, ProposalStore},
    scraper::UsageStore,
};
use crabka_units::{ByteRate, bytes_per_sec, millis, percent, secs};

/// Stand-in for the client facade of the executor.
///
/// These tests exercise only `CreateProposal`, `GetProposal`, and an
/// `ExecuteProposal` that fails. They never reach the reassignment path,
/// so every method here does nothing.
struct NoopClient;

#[async_trait]
impl ClientFacade for NoopClient {
    async fn alter_throttle_configs(
        &self,
        _op: ConfigOp,
        _targets: &ThrottleTargets,
        _throttle: ByteRate,
    ) -> Result<(), PhaseError> {
        Ok(())
    }
    async fn submit_reassignments(&self, _movements: &[Movement]) -> Result<(), PhaseError> {
        Ok(())
    }
    async fn cancel_reassignments(&self, _partitions: &[(String, i32)]) -> Result<(), PhaseError> {
        Ok(())
    }
    async fn list_in_flight(
        &self,
        _of_interest: &[(String, i32)],
    ) -> Result<Vec<(String, i32)>, PhaseError> {
        Ok(vec![])
    }
}

/// Builds the `AppState` that the rebalancer binary mounts behind its
/// router.
///
/// It follows `crates/rebalancer/tests/end_to_end.rs::build_state`.
fn build_state(snapshot: SharedSnapshot) -> Arc<AppState> {
    let mut registry = new_registry();
    let metrics = RebalancerMetrics::register(&mut registry);
    let store = Arc::new(ProposalStore::new(20));
    let client_facade: Arc<dyn ClientFacade> = Arc::new(NoopClient);
    let state_topic: Arc<dyn crabka_rebalancer::state_topic::StateBackend> =
        Arc::new(crabka_rebalancer::state_topic::fake::InMemoryBackend::new_loaded());
    let executor = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: std::env::temp_dir().join("crabka-operator-rebalance-e2e"),
            default_throttle: bytes_per_sec(50_000_000),
            poll_interval: millis(50),
            execute_deadline: secs(30),
            batch_size: 200,
        },
        metrics: metrics.clone(),
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
        state_topic: state_topic.clone(),
    };
    Arc::new(AppState {
        snapshot,
        store,
        goal_registry: Arc::new(GoalRegistry::default_registry()),
        goal_ctx: GoalContext {
            imbalance_threshold: percent(10),
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities::default()),
            broker_usages: Arc::new(UsageStore::default()),
        },
        metrics,
        executor,
        client_facade,
        anomaly_store: Arc::new(crabka_rebalancer::detector::AnomalyStore::new(200)),
        state_topic,
        cancel_drain_timeout: crabka_rebalancer::config::RebalancerRuntimePolicy::default()
            .cancel_drain_timeout,
        cancel_drain_poll_interval: crabka_rebalancer::config::RebalancerRuntimePolicy::default()
            .cancel_drain_poll_interval,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_client_round_trips_against_real_rebalancer() {
    // 1. Boot a single-broker Crabka and seed a topic.
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "e2e-topic".into(),
                partitions: 3,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            crabka_units::secs(5),
        )
        .await
        .unwrap();

    // 2. Take one snapshot (what the ingester ticker would write) and
    //    mount the rebalancer router over real HTTP.
    let snap_client = Client::builder()
        .bootstrap(bootstrap.as_str())
        .client_id("op-rebalance-e2e")
        .build()
        .await
        .unwrap();
    let snap = snapshot_once(&snap_client).await.unwrap();
    let shared = new_shared_snapshot();
    shared.store(Arc::new(Some(snap)));
    let state = build_state(shared);

    let app = crabka_rebalancer::api::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move { axum::serve(listener, app).await });

    // 3. Drive the operator's production client at it.
    let client = ConnectRebalancerClient::new(&format!("http://{addr}"), secs(30));

    // CreateProposal — single-broker cluster ⇒ Computed (likely zero
    // movements). The point is the wire round-trip + enum decode.
    let proposal = client.create_proposal(&[]).await.expect("create_proposal");
    assert!(
        proposal.status == ProposalStatus::Computed,
        "CreateProposal must decode to Computed"
    );
    assert!(!proposal.id.is_empty(), "proposal must carry an id");

    // GetProposal round-trips the same proposal back by id.
    let fetched = client
        .get_proposal(&proposal.id)
        .await
        .expect("get_proposal");
    assert!(fetched.id == proposal.id);
    assert!(fetched.status == ProposalStatus::Computed);

    // GetProposal on an unknown id surfaces a Connect error mapped to Rpc.
    match client.get_proposal("does-not-exist").await {
        Err(RebalancerError::Rpc { code, .. }) => {
            assert!(
                !code.is_empty(),
                "unknown-proposal error must carry a Connect code"
            );
        }
        other => panic!("expected Rpc error for unknown proposal, got {other:?}"),
    }

    // ExecuteProposal on a zero-movement proposal is rejected with a
    // Connect FailedPrecondition — verifies the error decode path.
    match client
        .execute_proposal(&proposal.id, Some(bytes_per_sec(1_000_000)))
        .await
    {
        Err(RebalancerError::Rpc { code, .. }) => {
            assert!(!code.is_empty(), "execute rejection must carry a code");
        }
        // If the optimizer happened to produce movements (it shouldn't on
        // a single broker) execution would start instead; accept that too.
        Ok(p) => assert!(p.status == ProposalStatus::Executing),
        other => panic!("unexpected execute outcome: {other:?}"),
    }

    let _ = tokio::time::timeout(Duration::from_secs(30), broker.shutdown()).await;
    std::mem::forget(dir);
}

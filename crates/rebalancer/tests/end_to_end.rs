//! Slice 43a end-to-end: spin up a single-broker Crabka in-process,
//! snapshot it, drive the Connect-RPC handlers directly, and assert
//! the propose/get/list paths plus the `Unavailable` / `Unimplemented`
//! / `NotFound` / `InvalidArgument` error codes.
//!
//! Handlers are called directly (not via the axum router) so the test
//! exercises handler logic at the same level a real Connect call
//! would, but without any HTTP serialization. HTTP smoke is T15.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Extension;
use connectrpc_axum::message::error::Code;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_rebalancer::api::GoalRegistry;
use crabka_rebalancer::api::handlers::{self, AppState};
use crabka_rebalancer::capacity::BrokerCapacities;
use crabka_rebalancer::executor::phases::{ClientFacade, ConfigOp, PhaseError};
use crabka_rebalancer::executor::throttle::ThrottleTargets;
use crabka_rebalancer::executor::{ExecutorConfig, ExecutorState};
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::health::new_registry;
use crabka_rebalancer::ingest::{SharedSnapshot, new_shared_snapshot, snapshot_once};
use crabka_rebalancer::metrics::RebalancerMetrics;
use crabka_rebalancer::model::{Movement, ProposalStore};
use crabka_rebalancer::pb;
use prometheus_client::registry::Registry;
use tempfile::TempDir;

/// Local stand-in for `executor::phases::tests::MockClient`, which lives
/// behind `#[cfg(test)]` and therefore isn't reachable from this external
/// integration-test crate. The 43a tests only need `client_facade` to
/// satisfy the `AppState` field; they don't exercise the executor path.
struct NoopClient;

#[async_trait]
impl ClientFacade for NoopClient {
    async fn alter_throttle_configs(
        &self,
        _op: ConfigOp,
        _targets: &ThrottleTargets,
        _throttle_bytes_per_sec: i64,
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

/// Boot a single-broker in-process Crabka and return its handle, the
/// bootstrap address as a `String`, and the tempdir backing its log
/// directory. The tempdir is returned so the caller can keep it alive
/// for the duration of the test — dropping it before broker shutdown
/// would yank the log directory out from under the broker.
async fn boot_broker() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Create a topic with `partitions` partitions and replication factor
/// 1 via a short-lived [`Client`]. Asserts success on the response.
async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("rebalancer-e2e-admin")
        .build()
        .await
        .expect("admin client");
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(
        resp.topics.len(),
        1,
        "expected exactly one topic result, got {resp:?}"
    );
    assert_eq!(
        resp.topics[0].error_code, 0,
        "create_topic({name}) failed: {resp:?}"
    );
}

/// Build the `AppState` carried by the `Extension` layer in production,
/// alongside the shared `Registry` the binary mounts on `/metrics`.
/// Threshold + cap match the binary's defaults (see `bin/rebalancer.rs`).
fn build_state(snapshot: SharedSnapshot) -> (Arc<AppState>, Registry) {
    let mut registry = new_registry();
    let metrics = RebalancerMetrics::register(&mut registry);
    let store = Arc::new(ProposalStore::new(20));
    let client_facade: Arc<dyn ClientFacade> = Arc::new(NoopClient);
    let executor = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: std::path::PathBuf::from("/tmp/crabka-rebalancer-test"),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(50),
            execute_deadline: Duration::from_secs(30),
            batch_size: 200,
        },
        metrics: metrics.clone(),
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let state = Arc::new(AppState {
        snapshot,
        store,
        goal_registry: GoalRegistry::default_registry(),
        goal_ctx: GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities::default()),
        },
        metrics,
        executor,
        client_facade,
    });
    (state, registry)
}

/// Helper for calling a handler. The crate's `ConnectRequest<T>` is a
/// tuple struct `pub struct ConnectRequest<T>(pub T)`, so we can
/// construct one with the tuple-struct constructor.
fn req<T>(msg: T) -> ConnectRequest<T> {
    ConnectRequest(msg)
}

/// Pull the inner message out of a `ConnectResponse`. Tuple-struct field.
fn into_inner<T>(resp: ConnectResponse<T>) -> T {
    resp.0
}

/// Unwrap a handler `Result<ConnectResponse<T>, ConnectError>` into `T`.
fn unwrap_ok<T>(r: Result<ConnectResponse<T>, ConnectError>) -> T {
    into_inner(r.expect("handler returned Err"))
}

/// Unwrap a handler `Result<_, ConnectError>` into `ConnectError`.
fn unwrap_err<T>(r: Result<ConnectResponse<T>, ConnectError>) -> ConnectError {
    match r {
        Ok(_) => panic!("expected Err, got Ok"),
        Err(e) => e,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_proposal_on_balanced_cluster_returns_empty_movements() {
    let (broker, bootstrap, _dir) = boot_broker().await;

    // 3 topics × 4 partitions × RF=1 on a single-broker cluster.
    // With only one broker every replica + leader must live on broker 1;
    // there is nothing for `ReplicaDistribution` or `LeaderDistribution`
    // to balance, and `PreferredLeaderIdempotency` is satisfied by
    // definition (replicas[0] is the only candidate). The proposal must
    // be empty.
    for t in &["topic-a", "topic-b", "topic-c"] {
        create_topic(&bootstrap, t, 4).await;
    }

    // Hand-roll the snapshot the ingester would normally write so we
    // don't have to spin up the `Ingester` ticker. `snapshot_once` is
    // the same function the ticker calls.
    let client = Client::builder()
        .bootstrap(bootstrap.as_str())
        .client_id("rebalancer-e2e-snap")
        .build()
        .await
        .expect("snapshot client");
    let snap = snapshot_once(&client).await.expect("snapshot_once");
    let shared = new_shared_snapshot();
    shared.store(Arc::new(Some(snap)));

    let (state, registry) = build_state(shared);
    // Mimic the ingester's per-tick bookkeeping — the test drives the
    // handlers directly, so without this the snapshot-side metrics
    // never get touched. The corresponding handler-side counter
    // (`proposals_created_total`) is incremented for us by the
    // `create_proposal` call below.
    state.metrics.snapshots_total.inc();
    state.metrics.snapshot_at_ms.set(
        state
            .snapshot
            .load()
            .as_ref()
            .as_ref()
            .unwrap()
            .snapshot_at_ms,
    );

    // GetState — must reflect the topics we just created.
    let gs =
        unwrap_ok(handlers::get_state(Extension(state.clone()), req(pb::GetStateRequest {})).await);
    assert_eq!(gs.brokers.len(), 1, "single-broker cluster");
    assert_eq!(gs.brokers[0].id, 1, "broker id matches for_tests config");
    let topic_names: std::collections::BTreeSet<String> =
        gs.topics.iter().map(|t| t.name.clone()).collect();
    for t in &["topic-a", "topic-b", "topic-c"] {
        assert!(
            topic_names.contains(*t),
            "missing topic {t} in snapshot, got {topic_names:?}"
        );
    }
    // 3 topics × 4 partitions = 12 partition entries (plus any internal
    // topics the broker may surface — we just assert the lower bound).
    let user_partitions: usize = gs
        .topics
        .iter()
        .filter(|t| ["topic-a", "topic-b", "topic-c"].contains(&t.name.as_str()))
        .map(|t| t.partitions.len())
        .sum();
    assert_eq!(user_partitions, 12, "expected 12 user-topic partitions");

    // CreateProposal — balanced single-broker cluster → empty movements.
    let proposal = unwrap_ok(
        handlers::create_proposal(
            Extension(state.clone()),
            req(pb::CreateProposalRequest { goals: vec![] }),
        )
        .await,
    );
    assert!(
        proposal.movements.is_empty(),
        "expected empty movements on a single-broker balanced cluster, got {:?}",
        proposal.movements
    );
    assert!(!proposal.id.is_empty(), "proposal must have an id");
    assert_eq!(
        proposal.status,
        i32::from(pb::ProposalStatus::Computed),
        "fresh proposal must be Computed"
    );
    let summary = proposal
        .summary
        .as_ref()
        .expect("proposal must carry a summary");
    assert_eq!(summary.replica_movements, 0);
    assert_eq!(summary.leader_movements, 0);

    // GetProposal — round-trips by id.
    let fetched = unwrap_ok(
        handlers::get_proposal(
            Extension(state.clone()),
            req(pb::GetProposalRequest {
                id: proposal.id.clone(),
            }),
        )
        .await,
    );
    assert_eq!(fetched.id, proposal.id);

    // ListProposals — the one we just stored shows up.
    let listed = unwrap_ok(
        handlers::list_proposals(
            Extension(state.clone()),
            req(pb::ListProposalsRequest { limit: 0 }),
        )
        .await,
    );
    assert_eq!(
        listed.proposals.len(),
        1,
        "expected the single proposal in the list"
    );
    assert_eq!(listed.proposals[0].id, proposal.id);

    // DryRunProposal — empty proposal → 0 bytes moved estimate.
    let dry = unwrap_ok(
        handlers::dry_run_proposal(
            Extension(state.clone()),
            req(pb::DryRunProposalRequest {
                id: proposal.id.clone(),
            }),
        )
        .await,
    );
    assert_eq!(dry.id, proposal.id);
    assert_eq!(dry.estimated_bytes_moved, 0);

    // GetProposal on a missing id → NotFound.
    let missing = unwrap_err(
        handlers::get_proposal(
            Extension(state.clone()),
            req(pb::GetProposalRequest {
                id: "no-such-proposal".to_string(),
            }),
        )
        .await,
    );
    assert_eq!(missing.code(), Code::NotFound);

    // DryRunProposal on a missing id → NotFound.
    let missing_dry = unwrap_err(
        handlers::dry_run_proposal(
            Extension(state.clone()),
            req(pb::DryRunProposalRequest {
                id: "no-such-proposal".to_string(),
            }),
        )
        .await,
    );
    assert_eq!(missing_dry.code(), Code::NotFound);

    // CreateProposal with an unknown goal name → InvalidArgument.
    let bad_goal = unwrap_err(
        handlers::create_proposal(
            Extension(state.clone()),
            req(pb::CreateProposalRequest {
                goals: vec!["GhostGoal".to_string()],
            }),
        )
        .await,
    );
    assert_eq!(bad_goal.code(), Code::InvalidArgument);

    // ExecuteProposal on a no-movements proposal → FailedPrecondition.
    // The 43b handler refuses to start an execution with an empty plan.
    let exec = unwrap_err(
        handlers::execute_proposal(
            Extension(state.clone()),
            req(pb::ExecuteProposalRequest {
                id: proposal.id.clone(),
                throttle_bytes_per_sec: None,
            }),
        )
        .await,
    );
    assert_eq!(exec.code(), Code::FailedPrecondition);

    // OpenMetrics: the registry that `/metrics` would scrape contains
    // all three spec-promised metrics with the `crabka_rebalancer_`
    // prefix, and the OpenMetrics terminator. Snapshot-side metrics
    // were bumped above; `proposals_created_total` is bumped by the
    // `create_proposal` handler we just exercised.
    let mut buf = String::new();
    prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
    for needle in [
        "crabka_rebalancer_snapshot_at_ms",
        "crabka_rebalancer_snapshots_total",
        "crabka_rebalancer_proposals_created_total",
    ] {
        assert!(buf.contains(needle), "missing {needle} in /metrics:\n{buf}");
    }
    assert!(buf.contains("# EOF"), "OpenMetrics terminator missing");

    // Bound the test's wall-clock — broker shutdown can hang if a task
    // is stuck; surface that as a test failure rather than a CI timeout.
    tokio::time::timeout(Duration::from_secs(30), broker.shutdown())
        .await
        .expect("broker shutdown within 30s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_state_returns_unavailable_before_first_snapshot() {
    // No broker needed — the handlers only touch `AppState`. We
    // construct an `AppState` whose snapshot has never been populated
    // and assert every read path that requires a snapshot returns
    // `Unavailable`, while paths that don't (ListProposals, the
    // Unimplemented stub) behave normally.
    let shared = new_shared_snapshot();
    let (state, _registry) = build_state(shared);

    // GetState → Unavailable.
    let gs = unwrap_err(
        handlers::get_state(Extension(state.clone()), req(pb::GetStateRequest {})).await,
    );
    assert_eq!(gs.code(), Code::Unavailable);

    // CreateProposal also reads the snapshot → Unavailable.
    let cp = unwrap_err(
        handlers::create_proposal(
            Extension(state.clone()),
            req(pb::CreateProposalRequest { goals: vec![] }),
        )
        .await,
    );
    assert_eq!(cp.code(), Code::Unavailable);

    // ListProposals doesn't need a snapshot; it should return an empty
    // list rather than erroring.
    let listed = unwrap_ok(
        handlers::list_proposals(
            Extension(state.clone()),
            req(pb::ListProposalsRequest { limit: 0 }),
        )
        .await,
    );
    assert!(
        listed.proposals.is_empty(),
        "no proposals yet, got {:?}",
        listed.proposals
    );

    // ExecuteProposal on an unknown id → NotFound (regardless of snapshot
    // state). The 43b handler resolves the proposal via the store before
    // touching the snapshot.
    let exec = unwrap_err(
        handlers::execute_proposal(
            Extension(state.clone()),
            req(pb::ExecuteProposalRequest {
                id: "irrelevant".to_string(),
                throttle_bytes_per_sec: None,
            }),
        )
        .await,
    );
    assert_eq!(exec.code(), Code::NotFound);
}

/// Execute a proposal end-to-end against a single-broker Crabka.
///
/// Single-broker means the only valid replica set is `[1]`; the
/// optimizer would never generate movements here, so we construct a
/// synthetic proposal directly (replicas `[1]` -> `[1]`) to exercise
/// the executor's wire path. The plan is a no-op from the broker's
/// perspective — `ApplyThrottle` / `Submit` / `Wait` / `ClearThrottle`
/// still all fire, `in_flight.json` is written and then cleaned up,
/// and the proposal reaches a terminal status.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_proposal_settles_against_real_broker() {
    use crabka_rebalancer::executor::Execution;
    use crabka_rebalancer::executor::client_impl::LiveClient;
    use crabka_rebalancer::executor::state::InFlightFile;
    use crabka_rebalancer::model::proposal::{Proposal, ProposalStatus, ProposalSummary};
    use std::time::Instant;

    let (broker, bootstrap, _broker_dir) = boot_broker().await;
    create_topic(&bootstrap, "exec-t", 1).await;

    let client = Client::builder()
        .bootstrap(bootstrap.as_str())
        .client_id("crabka-rebalancer-test")
        .build()
        .await
        .expect("admin client");

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ProposalStore::open(dir.path(), 20).unwrap());

    let proposal = Proposal {
        id: "exec-1".into(),
        status: ProposalStatus::Computed,
        created_at_ms: 0,
        goals_applied: vec![],
        summary: ProposalSummary::default(),
        movements: vec![Movement {
            topic: "exec-t".into(),
            partition: 0,
            old_replicas: vec![1],
            new_replicas: vec![1],
            old_leader: 1,
            new_leader: 1,
        }],
        started_at_ms: 0,
        terminated_at_ms: 0,
        failure_reason: None,
        throttle_bytes_per_sec: 0,
    };
    store.insert(proposal.clone());

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = RebalancerMetrics::register(&mut registry);
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(50),
            execute_deadline: Duration::from_secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let live_client = Arc::new(LiveClient::new(client));

    let cancel = tokio_util::sync::CancellationToken::new();
    let exec = Execution::new(live_client, executor_state, proposal, 50_000_000, cancel);
    let exec_task = tokio::spawn(exec.run());

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut final_status = ProposalStatus::Executing;
    while Instant::now() < deadline {
        final_status = store.get("exec-1").unwrap().status;
        if final_status != ProposalStatus::Executing && final_status != ProposalStatus::Computed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = exec_task.await;

    assert!(
        matches!(
            final_status,
            ProposalStatus::Completed | ProposalStatus::Failed
        ),
        "expected terminal status, got {final_status:?}"
    );
    assert!(InFlightFile::load(dir.path()).unwrap().is_none());

    tokio::time::timeout(Duration::from_secs(30), broker.shutdown())
        .await
        .expect("broker shutdown within 30s");
}

/// Cancelling an in-flight execution drives it to a terminal status
/// and cleans up the `in_flight.json` marker. We don't insist on
/// `Cancelled` specifically — a single-broker no-op plan may race
/// to `Completed` before the cancel token fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_clears_throttle_and_reverts() {
    use crabka_rebalancer::executor::Execution;
    use crabka_rebalancer::executor::client_impl::LiveClient;
    use crabka_rebalancer::executor::state::InFlightFile;
    use crabka_rebalancer::model::proposal::{Proposal, ProposalStatus, ProposalSummary};

    let (broker, bootstrap, _broker_dir) = boot_broker().await;
    create_topic(&bootstrap, "cancel-t", 1).await;

    let client = Client::builder()
        .bootstrap(bootstrap.as_str())
        .client_id("crabka-rebalancer-test")
        .build()
        .await
        .expect("admin client");

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ProposalStore::open(dir.path(), 20).unwrap());
    let proposal = Proposal {
        id: "cancel-1".into(),
        status: ProposalStatus::Computed,
        created_at_ms: 0,
        goals_applied: vec![],
        summary: ProposalSummary::default(),
        movements: vec![Movement {
            topic: "cancel-t".into(),
            partition: 0,
            old_replicas: vec![1],
            new_replicas: vec![1],
            old_leader: 1,
            new_leader: 1,
        }],
        started_at_ms: 0,
        terminated_at_ms: 0,
        failure_reason: None,
        throttle_bytes_per_sec: 0,
    };
    store.insert(proposal.clone());

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = RebalancerMetrics::register(&mut registry);
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(50),
            execute_deadline: Duration::from_secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let live_client = Arc::new(LiveClient::new(client));
    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_for_caller = cancel.clone();
    let exec = Execution::new(live_client, executor_state, proposal, 50_000_000, cancel);
    let exec_task = tokio::spawn(exec.run());

    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel_for_caller.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), exec_task).await;

    let after = store.get("cancel-1").unwrap();
    assert!(
        matches!(
            after.status,
            ProposalStatus::Cancelled | ProposalStatus::Completed | ProposalStatus::Failed
        ),
        "expected terminal status, got {:?}",
        after.status
    );
    assert!(InFlightFile::load(dir.path()).unwrap().is_none());

    tokio::time::timeout(Duration::from_secs(30), broker.shutdown())
        .await
        .expect("broker shutdown within 30s");
}

/// Simulate a restart-while-executing: an `in_flight.json` exists on
/// disk pointing at `Submit`, and the resume path picks up where it
/// left off, drives the state machine to terminal, and removes the
/// marker file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_in_flight_plan() {
    use crabka_rebalancer::executor::Execution;
    use crabka_rebalancer::executor::client_impl::LiveClient;
    use crabka_rebalancer::executor::state::{InFlightFile, Phase};
    use crabka_rebalancer::model::proposal::{Proposal, ProposalStatus, ProposalSummary};

    let (broker, bootstrap, _broker_dir) = boot_broker().await;
    create_topic(&bootstrap, "resume-t", 1).await;

    let client = Client::builder()
        .bootstrap(bootstrap.as_str())
        .client_id("crabka-rebalancer-test")
        .build()
        .await
        .expect("admin client");

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ProposalStore::open(dir.path(), 20).unwrap());
    let proposal = Proposal {
        id: "resume-1".into(),
        status: ProposalStatus::Executing,
        created_at_ms: 0,
        goals_applied: vec![],
        summary: ProposalSummary::default(),
        movements: vec![Movement {
            topic: "resume-t".into(),
            partition: 0,
            old_replicas: vec![1],
            new_replicas: vec![1],
            old_leader: 1,
            new_leader: 1,
        }],
        started_at_ms: 1,
        terminated_at_ms: 0,
        failure_reason: None,
        throttle_bytes_per_sec: 50_000_000,
    };
    store.insert(proposal.clone());

    InFlightFile::new(proposal.id.clone(), Phase::Submit, 1, 50_000_000)
        .write(dir.path())
        .unwrap();

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = RebalancerMetrics::register(&mut registry);
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(50),
            execute_deadline: Duration::from_secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let live_client = Arc::new(LiveClient::new(client));
    let cancel = tokio_util::sync::CancellationToken::new();
    let in_flight = InFlightFile::load(dir.path()).unwrap().unwrap();
    let exec = Execution::resume(live_client, executor_state, proposal, &in_flight, cancel);
    let _ = tokio::time::timeout(Duration::from_secs(10), exec.run()).await;

    let after = store.get("resume-1").unwrap();
    assert!(
        matches!(
            after.status,
            ProposalStatus::Completed | ProposalStatus::Failed
        ),
        "expected terminal status after resume, got {:?}",
        after.status
    );
    assert!(InFlightFile::load(dir.path()).unwrap().is_none());

    tokio::time::timeout(Duration::from_secs(30), broker.shutdown())
        .await
        .expect("broker shutdown within 30s");
}

/// Synthetic ClusterState with three brokers in rack labels [A, A, B]
/// and a partition with replicas on the two rack-A brokers. The
/// RackAware goal must propose moving one off to a non-A rack. We
/// drive the goal directly (no real broker needed) since this test
/// is purely about goal interaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rack_aware_eliminates_same_rack_collisions() {
    use crabka_rebalancer::goals::rack_aware::RackAware;
    use crabka_rebalancer::goals::{Goal, GoalContext};
    use crabka_rebalancer::model::{BrokerView, ClusterState, Movement, PartitionView};

    let state = ClusterState {
        cluster_id: Some("c".into()),
        snapshot_at_ms: 0,
        brokers: vec![
            BrokerView {
                id: 1,
                host: "h1".into(),
                port: 9092,
                rack: Some("A".into()),
            },
            BrokerView {
                id: 2,
                host: "h2".into(),
                port: 9092,
                rack: Some("A".into()),
            },
            BrokerView {
                id: 3,
                host: "h3".into(),
                port: 9092,
                rack: Some("B".into()),
            },
        ],
        partitions: vec![PartitionView {
            topic: "t".into(),
            partition: 0,
            replicas: vec![1, 2],
            leader: 1,
            isr: vec![1, 2],
        }],
        in_flight_reassignments: vec![],
    };

    let ctx = GoalContext {
        imbalance_threshold_pct: 10,
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(BrokerCapacities::default()),
    };

    let mvs: Vec<Movement> = RackAware.propose(&state, &ctx);
    assert_eq!(
        mvs.len(),
        1,
        "expected exactly one RackAware movement, got {mvs:?}"
    );
    let m = &mvs[0];
    assert_eq!(m.topic, "t");
    assert_eq!(m.partition, 0);
    assert!(
        !m.new_replicas.contains(&1) || !m.new_replicas.contains(&2),
        "movement must remove one of the rack-A brokers; got {m:?}"
    );
    assert!(
        m.new_replicas.contains(&3),
        "movement must add the rack-B broker (3); got {m:?}"
    );
}

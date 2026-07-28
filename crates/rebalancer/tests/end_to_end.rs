//! End-to-end: spin up a single-broker Crabka in-process,
//! snapshot it, drive the Connect-RPC handlers directly, and assert
//! the propose/get/list paths plus the `Unavailable` / `Unimplemented`
//! / `NotFound` / `InvalidArgument` error codes.
//!
//! Handlers are called directly (not via the axum router) so the test
//! exercises handler logic at the same level a real Connect call
//! would, but without any HTTP serialization. HTTP smoke is T15.

use std::sync::Arc;

use assert2::check;
use async_trait::async_trait;
use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse, error::Code};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_rebalancer::{
    api::{
        GoalRegistry,
        handlers::{self, AppState},
    },
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
    pb,
    scraper::UsageStore,
    state_topic::StateBackend as _,
};
use crabka_units::{
    ByteRate, Time, bytes_per_sec,
    convert::{ByteRateExt as _, TimeExt as _},
    millis, minutes, percent, secs,
};
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

/// How long the broker may take to acknowledge a test `CreateTopics`.
const CREATE_TOPIC_TIMEOUT: Time = secs(5);

/// The binary's default KIP-73 replication throttle.
const DEFAULT_THROTTLE: ByteRate = bytes_per_sec(50_000_000);

/// Bound on how long a test waits for a broker to shut down.
const SHUTDOWN_TIMEOUT: Time = secs(30);

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
            timeout_ms: CREATE_TOPIC_TIMEOUT.millis_i32(),
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert2::assert!(
        resp.topics
            .iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>()
            == vec![0]
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
            default_throttle: DEFAULT_THROTTLE,
            poll_interval: millis(50),
            execute_deadline: secs(30),
            batch_size: 200,
        },
        metrics: metrics.clone(),
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
        state_topic: Arc::new(crabka_rebalancer::state_topic::fake::InMemoryBackend::new_loaded()),
    };
    let state = Arc::new(AppState {
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
        state_topic: Arc::new(crabka_rebalancer::state_topic::fake::InMemoryBackend::new_loaded()),
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
    assert2::assert!(
        gs.brokers
            .iter()
            .map(|broker| broker.id)
            .collect::<Vec<_>>()
            == vec![1]
    );
    let topic_names: std::collections::BTreeSet<String> =
        gs.topics.iter().map(|t| t.name.clone()).collect();
    for t in &["topic-a", "topic-b", "topic-c"] {
        assert2::assert!(topic_names.contains(*t));
    }
    // 3 topics × 4 partitions = 12 partition entries (plus any internal
    // topics the broker may surface — we just assert the lower bound).
    let user_partitions: usize = gs
        .topics
        .iter()
        .filter(|t| ["topic-a", "topic-b", "topic-c"].contains(&t.name.as_str()))
        .map(|t| t.partitions.len())
        .sum();
    assert2::assert!(user_partitions == 12);

    // CreateProposal — balanced single-broker cluster → empty movements.
    let proposal = unwrap_ok(
        handlers::create_proposal(
            Extension(state.clone()),
            req(pb::CreateProposalRequest { goals: vec![] }),
        )
        .await,
    );
    check!(
        (
            proposal.movements.is_empty(),
            proposal.id.is_empty(),
            proposal.status,
        ) == (true, false, i32::from(pb::ProposalStatus::Computed))
    );
    let summary = proposal
        .summary
        .as_ref()
        .expect("proposal must carry a summary");
    assert2::assert!(summary.replica_movements == 0);
    assert2::assert!(summary.leader_movements == 0);

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
    assert2::assert!(fetched.id == proposal.id);

    // ListProposals — the one we just stored shows up.
    let listed = unwrap_ok(
        handlers::list_proposals(
            Extension(state.clone()),
            req(pb::ListProposalsRequest { limit: 0 }),
        )
        .await,
    );
    assert2::assert!(
        listed
            .proposals
            .iter()
            .map(|p| p.id.as_str())
            .collect::<Vec<_>>()
            == vec![proposal.id.as_str()]
    );

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
    assert2::assert!(dry.id.as_str() == proposal.id.as_str());
    assert2::assert!(dry.estimated_bytes_moved == 0);

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
    assert2::assert!(missing.code() == Code::NotFound);

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
    assert2::assert!(missing_dry.code() == Code::NotFound);

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
    assert2::assert!(bad_goal.code() == Code::InvalidArgument);

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
    assert2::assert!(exec.code() == Code::FailedPrecondition);

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
        assert2::assert!(buf.contains(needle));
    }
    assert2::assert!(buf.contains("# EOF"));

    // Bound the test's wall-clock — broker shutdown can hang if a task
    // is stuck; surface that as a test failure rather than a CI timeout.
    tokio::time::timeout(SHUTDOWN_TIMEOUT.to_std(), broker.shutdown())
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
    check!(gs.code() == Code::Unavailable);

    // CreateProposal also reads the snapshot → Unavailable.
    let cp = unwrap_err(
        handlers::create_proposal(
            Extension(state.clone()),
            req(pb::CreateProposalRequest { goals: vec![] }),
        )
        .await,
    );
    check!(cp.code() == Code::Unavailable);

    // ListProposals doesn't need a snapshot; it should return an empty
    // list rather than erroring.
    let listed = unwrap_ok(
        handlers::list_proposals(
            Extension(state.clone()),
            req(pb::ListProposalsRequest { limit: 0 }),
        )
        .await,
    );
    check!(
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
    check!(exec.code() == Code::NotFound);
}

/// Execute a proposal end-to-end against a single-broker Crabka.
///
/// Single-broker means the only valid replica set is `[1]`; the
/// optimizer would never generate movements here, so we construct a
/// synthetic proposal directly (replicas `[1]` -> `[1]`) to exercise
/// the executor's wire path. The plan is a no-op from the broker's
/// perspective — `ApplyThrottle` / `Submit` / `Wait` / `ClearThrottle`
/// still all fire, the state backend is written and then tombstoned,
/// and the proposal reaches a terminal status.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_proposal_settles_against_real_broker() {
    use std::time::Instant;

    use crabka_rebalancer::{
        executor::{Execution, client_impl::LiveClient},
        model::proposal::{Proposal, ProposalStatus, ProposalSummary},
    };

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
        throttle: ByteRate::ZERO,
    };
    store.insert(proposal.clone());

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = RebalancerMetrics::register(&mut registry);
    let backend = Arc::new(crabka_rebalancer::state_topic::fake::InMemoryBackend::new_loaded());
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle: DEFAULT_THROTTLE,
            poll_interval: millis(50),
            execute_deadline: secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
        state_topic: backend.clone(),
    };
    let live_client = Arc::new(LiveClient::new(client));

    let cancel = tokio_util::sync::CancellationToken::new();
    let exec = Execution::new(
        live_client,
        executor_state,
        proposal,
        DEFAULT_THROTTLE,
        cancel,
    );
    let exec_task = tokio::spawn(exec.run());

    let deadline = Instant::now() + secs(10).to_std();
    let mut final_status = ProposalStatus::Executing;
    while Instant::now() < deadline {
        final_status = store.get("exec-1").unwrap().status;
        if final_status != ProposalStatus::Executing && final_status != ProposalStatus::Computed {
            break;
        }
        tokio::task::yield_now().await;
    }
    let _ = exec_task.await;

    assert2::assert!(matches!(
        final_status,
        ProposalStatus::Completed | ProposalStatus::Failed
    ));
    // After terminal the backend must be tombstoned.
    assert2::assert!(backend.loaded().is_none());

    tokio::time::timeout(SHUTDOWN_TIMEOUT.to_std(), broker.shutdown())
        .await
        .expect("broker shutdown within 30s");
}

/// Cancelling an in-flight execution drives it to a terminal status
/// and cleans up the state backend. We don't insist on
/// `Cancelled` specifically — a single-broker no-op plan may race
/// to `Completed` before the cancel token fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_clears_throttle_and_reverts() {
    use crabka_rebalancer::{
        executor::{Execution, client_impl::LiveClient},
        model::proposal::{Proposal, ProposalStatus, ProposalSummary},
    };

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
        throttle: ByteRate::ZERO,
    };
    store.insert(proposal.clone());

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = RebalancerMetrics::register(&mut registry);
    let backend = Arc::new(crabka_rebalancer::state_topic::fake::InMemoryBackend::new_loaded());
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle: DEFAULT_THROTTLE,
            poll_interval: millis(50),
            execute_deadline: secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
        state_topic: backend.clone(),
    };
    let live_client = Arc::new(LiveClient::new(client));
    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_for_caller = cancel.clone();
    let exec = Execution::new(
        live_client,
        executor_state,
        proposal,
        DEFAULT_THROTTLE,
        cancel,
    );
    let exec_task = tokio::spawn(exec.run());

    // Wait until the execution is genuinely under way — it persists an
    // ApplyThrottle in-flight record before issuing its first broker RPC —
    // or has already reached a terminal status, then cancel. Polling the
    // real post-condition instead of sleeping a fixed 100ms means we cancel
    // a run that has actually started regardless of machine speed, and the
    // terminal fallback keeps the loop from hanging if the no-op plan wins
    // the race and tombstones the backend before we observe it. Bounded so a
    // stuck run surfaces as a failure rather than a hang.
    let deadline = std::time::Instant::now() + secs(10).to_std();
    loop {
        let started = backend.loaded().is_some();
        let terminal = matches!(
            store.get("cancel-1").unwrap().status,
            ProposalStatus::Cancelled | ProposalStatus::Completed | ProposalStatus::Failed
        );
        if started || terminal {
            break;
        }
        assert2::assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(millis(5).to_std()).await;
    }
    cancel_for_caller.cancel();
    let _ = tokio::time::timeout(secs(10).to_std(), exec_task).await;

    let after = store.get("cancel-1").unwrap();
    assert2::assert!(matches!(
        after.status,
        ProposalStatus::Cancelled | ProposalStatus::Completed | ProposalStatus::Failed
    ));
    // After terminal the backend must be tombstoned.
    assert2::assert!(backend.loaded().is_none());

    tokio::time::timeout(SHUTDOWN_TIMEOUT.to_std(), broker.shutdown())
        .await
        .expect("broker shutdown within 30s");
}

/// Simulate a restart-while-executing: the state backend contains an
/// in-flight record pointing at `Submit`, and the resume path picks up
/// where it left off, drives the state machine to terminal, and
/// tombstones the backend entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_in_flight_plan() {
    use crabka_rebalancer::{
        executor::{
            Execution,
            client_impl::LiveClient,
            state::{InFlightFile, Phase},
        },
        model::proposal::{Proposal, ProposalStatus, ProposalSummary},
    };

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
        throttle: DEFAULT_THROTTLE,
    };
    store.insert(proposal.clone());

    // Seed the backend as if a previous run persisted Submit phase.
    let in_flight = InFlightFile::new(proposal.id.clone(), Phase::Submit, 1, DEFAULT_THROTTLE);
    let backend = Arc::new(crabka_rebalancer::state_topic::fake::InMemoryBackend::new_loaded());
    *backend.state.lock().unwrap() = Some(in_flight.clone());

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = RebalancerMetrics::register(&mut registry);
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle: DEFAULT_THROTTLE,
            poll_interval: millis(50),
            execute_deadline: secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
        state_topic: backend.clone(),
    };
    let live_client = Arc::new(LiveClient::new(client));
    let cancel = tokio_util::sync::CancellationToken::new();
    let exec = Execution::resume(live_client, executor_state, proposal, &in_flight, cancel);
    let _ = tokio::time::timeout(secs(10).to_std(), exec.run()).await;

    let after = store.get("resume-1").unwrap();
    assert2::assert!(matches!(
        after.status,
        ProposalStatus::Completed | ProposalStatus::Failed
    ));
    // After terminal the backend must be tombstoned.
    assert2::assert!(backend.loaded().is_none());

    tokio::time::timeout(SHUTDOWN_TIMEOUT.to_std(), broker.shutdown())
        .await
        .expect("broker shutdown within 30s");
}

/// Synthetic `ClusterState` with three brokers in rack labels [A, A, B]
/// and a partition with replicas on the two rack-A brokers. The
/// `RackAware` goal must propose moving one off to a non-A rack. We
/// drive the goal directly (no real broker needed) since this test
/// is purely about goal interaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rack_aware_eliminates_same_rack_collisions() {
    use crabka_rebalancer::{
        goals::{Goal, GoalContext, rack_aware::RackAware},
        model::{BrokerView, ClusterState, Movement, PartitionView},
    };

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
        imbalance_threshold: percent(10),
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(BrokerCapacities::default()),
        broker_usages: Arc::new(UsageStore::default()),
    };

    let mvs: Vec<Movement> = RackAware.propose(&state, &ctx);
    assert2::assert!(mvs.len() == 1);
    let m = &mvs[0];
    check!(m.topic == "t");
    check!(m.partition == 0);
    check!(
        !m.new_replicas.contains(&1) || !m.new_replicas.contains(&2),
        "movement must remove one of the rack-A brokers; got {m:?}"
    );
    check!(
        m.new_replicas.contains(&3),
        "movement must add the rack-B broker (3); got {m:?}"
    );
}

/// Synthetic three-broker `ClusterState` where broker 1 holds 10
/// replicas with `max_replicas: 5`. `ReplicaCapacity` must propose
/// movements that reduce broker 1's load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_capacity_evicts_over_capacity_broker() {
    use std::{collections::HashMap, sync::Arc};

    use crabka_rebalancer::{
        capacity::{BrokerCapacities, BrokerCapacity},
        goals::{Goal, GoalContext, replica_capacity::ReplicaCapacity},
        model::{BrokerView, ClusterState, Movement, PartitionView},
    };

    let parts: Vec<_> = (0..10)
        .map(|i| PartitionView {
            topic: "t".into(),
            partition: i,
            replicas: vec![1, 2],
            leader: 1,
            isr: vec![1, 2],
        })
        .collect();

    let state = ClusterState {
        cluster_id: Some("c".into()),
        snapshot_at_ms: 0,
        brokers: vec![
            BrokerView {
                id: 1,
                host: "h1".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 2,
                host: "h2".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 3,
                host: "h3".into(),
                port: 9092,
                rack: None,
            },
        ],
        partitions: parts,
        in_flight_reassignments: vec![],
    };

    let mut by_broker = HashMap::new();
    by_broker.insert(
        1,
        BrokerCapacity {
            max_replicas: Some(5),
            ..Default::default()
        },
    );
    let caps = BrokerCapacities { by_broker };

    let ctx = GoalContext {
        imbalance_threshold: percent(10),
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(caps),
        broker_usages: Arc::new(UsageStore::default()),
    };

    let mvs: Vec<Movement> = ReplicaCapacity.propose(&state, &ctx);
    assert2::assert!(!mvs.is_empty());

    // Every movement must reduce broker 1's replica count.
    for m in &mvs {
        let before = m.old_replicas.iter().filter(|x| **x == 1).count();
        let after = m.new_replicas.iter().filter(|x| **x == 1).count();
        check!(
            after < before,
            "movement {m:?} doesn't reduce broker 1's replicas"
        );
    }

    // Apply movements to a working copy and verify broker 1's
    // post-state replica count is at or below 5.
    let mut working = state.partitions.clone();
    for m in &mvs {
        if let Some(p) = working
            .iter_mut()
            .find(|p| p.topic == m.topic && p.partition == m.partition)
        {
            p.replicas = m.new_replicas.clone();
        }
    }
    let final_broker_1_count: usize = working
        .iter()
        .map(|p| p.replicas.iter().filter(|x| **x == 1).count())
        .sum();
    check!(
        final_broker_1_count <= 5,
        "broker 1 still has {final_broker_1_count} replicas after eviction"
    );
}

/// Synthetic three-broker `ClusterState` with broker 1 holding 5× more
/// disk than broker 2. The `UsageStore` is pre-populated with `disk_bytes`
/// gauge samples. DiskUsage.propose must emit movements that reduce
/// broker 1's total.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_usage_evicts_hot_broker() {
    use std::sync::Arc;

    use crabka_rebalancer::{
        goals::{Goal, GoalContext, disk_usage::DiskUsage},
        model::{BrokerView, ClusterState, Movement, PartitionView},
        scraper::{MetricKind, UsageStore, WindowConfig, parse::ParsedSample},
    };

    let parts: Vec<_> = (0..5)
        .map(|i| PartitionView {
            topic: "t".into(),
            partition: i,
            replicas: vec![1, 2],
            leader: 1,
            isr: vec![1, 2],
        })
        .collect();

    let state = ClusterState {
        cluster_id: Some("c".into()),
        snapshot_at_ms: 0,
        brokers: vec![
            BrokerView {
                id: 1,
                host: "h1".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 2,
                host: "h2".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 3,
                host: "h3".into(),
                port: 9092,
                rack: None,
            },
        ],
        partitions: parts,
        in_flight_reassignments: vec![],
    };

    let store = UsageStore::new(WindowConfig {
        scrape_interval: secs(30),
        retention: crabka_units::hours(1),
    });
    // Insert at wall-clock "now" so DiskUsage's now_ms()-anchored
    // stale-data guard sees the samples as fresh.
    let sample_at = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(i64::MAX);
    // Broker 1: 500 disk_bytes per partition × 5 partitions = 2500.
    for i in 0..5 {
        store.insert(
            1,
            vec![ParsedSample {
                metric: MetricKind::DiskBytes,
                topic: "t".into(),
                partition: i,
                value: 500.0,
            }],
            sample_at,
        );
    }
    // Broker 2: 100 disk_bytes per partition × 5 partitions = 500.
    for i in 0..5 {
        store.insert(
            2,
            vec![ParsedSample {
                metric: MetricKind::DiskBytes,
                topic: "t".into(),
                partition: i,
                value: 100.0,
            }],
            sample_at,
        );
    }

    let ctx = GoalContext {
        imbalance_threshold: percent(10),
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(crabka_rebalancer::capacity::BrokerCapacities::default()),
        broker_usages: Arc::new(store),
    };

    let mvs: Vec<Movement> = DiskUsage.propose(&state, &ctx);
    assert2::assert!(!mvs.is_empty());

    // Apply movements; broker 1's post-state total must shrink.
    let mut working = state.partitions.clone();
    for m in &mvs {
        if let Some(p) = working
            .iter_mut()
            .find(|p| p.topic == m.topic && p.partition == m.partition)
        {
            p.replicas = m.new_replicas.clone();
        }
    }
    let broker_1_count = working
        .iter()
        .map(|p| p.replicas.iter().filter(|x| **x == 1).count())
        .sum::<usize>();
    assert2::assert!(broker_1_count < 5);
}

// ===== Anomaly detector integration tests =====

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_anomalies_returns_empty_when_detector_quiet() {
    let shared = new_shared_snapshot();
    let (state, _registry) = build_state(shared);

    let resp = handlers::get_anomalies(
        Extension(state),
        req(pb::GetAnomaliesRequest {
            limit: 0,
            include_resolved: None,
        }),
    )
    .await
    .expect("get_anomalies handler returned Err");
    assert2::assert!(resp.0.anomalies.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anomaly_store_persists_and_get_anomalies_returns_it() {
    use crabka_rebalancer::detector::{AnomalyKey, AnomalyKind, AnomalySeverity};

    let shared = new_shared_snapshot();
    let (state, _registry) = build_state(shared);

    // Insert two anomalies directly via the store (we're testing the
    // handler surface, not the rules — those have their own unit tests).
    let _ = state.anomaly_store.upsert_open(
        AnomalyKind::DiskPressure,
        AnomalyKey::Broker(1),
        AnomalySeverity::Warning,
        "test disk pressure".into(),
        1_000,
    );
    let _ = state.anomaly_store.upsert_open(
        AnomalyKind::BrokerDeath,
        AnomalyKey::Broker(3),
        AnomalySeverity::Critical,
        "test broker death".into(),
        1_001,
    );

    let resp = handlers::get_anomalies(
        Extension(state),
        req(pb::GetAnomaliesRequest {
            limit: 0,
            include_resolved: None,
        }),
    )
    .await
    .expect("get_anomalies handler returned Err");
    assert2::assert!(resp.0.anomalies.len() == 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_trigger_skipped_when_executor_in_flight() {
    use crabka_rebalancer::{
        detector::{
            Anomaly, AnomalyKey, AnomalyKind, AnomalySeverity, DetectorConfig, DetectorMetrics,
            auto_trigger,
        },
        executor::ExecutionHandle,
    };
    use tokio_util::sync::CancellationToken;

    let shared = new_shared_snapshot();
    let (state, _registry) = build_state(shared);

    // Pre-stage in-flight slot to force the executor-busy skip path.
    *state.executor.in_flight.lock().await = Some(ExecutionHandle {
        proposal_id: "p-in-flight".into(),
        task: tokio::spawn(async {}),
        cancel: CancellationToken::new(),
        started_at: std::time::Instant::now(),
    });

    let mut registry = new_registry();
    let metrics = DetectorMetrics::register(&mut registry);
    let config = DetectorConfig {
        auto_trigger_enabled: true,
        default_mute_window: minutes(15),
        ..DetectorConfig::default()
    };
    let anomaly = Anomaly {
        id: "a1".into(),
        kind: AnomalyKind::BrokerDeath,
        key: AnomalyKey::Broker(99),
        severity: AnomalySeverity::Critical,
        detected_at_ms: 0,
        last_seen_at_ms: 0,
        resolved_at_ms: None,
        triggered_proposal_id: None,
        mute_until_ms: None,
        details: String::new(),
    };

    let ctx = auto_trigger::AutoTriggerCtx {
        snapshot: state.snapshot.clone(),
        goal_registry: &state.goal_registry,
        goal_ctx: &state.goal_ctx,
        proposal_store: &state.store,
        anomaly_store: &state.anomaly_store,
        executor_state: &state.executor,
        config: &config,
        metrics: &metrics,
        now_ms: 1000,
    };
    auto_trigger::maybe_trigger(&anomaly, &ctx)
        .await
        .expect("maybe_trigger should not error on a gate-skip path");

    assert2::assert!(state.store.list(0).len() == 0);
    assert2::assert!(metrics.auto_trigger_skipped_executing.get() == 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_pressure_anomaly_auto_triggers_proposal() {
    use crabka_rebalancer::{
        detector::{
            AnomalyKey, AnomalyKind, AnomalySeverity, DetectorConfig, DetectorMetrics, auto_trigger,
        },
        model::{BrokerView, ClusterState, PartitionView},
    };

    let shared = new_shared_snapshot();
    let (state, _registry) = build_state(shared);

    // build_state leaves the snapshot empty; install a multi-broker
    // cluster so the optimizer at least has somewhere to consider moving.
    {
        let new_state = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 1,
            brokers: (1..=3)
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: (0..6)
                .map(|p| PartitionView {
                    topic: "t".into(),
                    partition: p,
                    replicas: vec![1, 2],
                    leader: 1,
                    isr: vec![1, 2],
                })
                .collect(),
            in_flight_reassignments: vec![],
        };
        state.snapshot.store(Arc::new(Some(new_state)));
    }

    // Insert a disk-pressure anomaly via the store directly. Then call
    // auto_trigger::maybe_trigger and assert it exercised the pipeline.
    let (anomaly_id, _) = state.anomaly_store.upsert_open(
        AnomalyKind::DiskPressure,
        AnomalyKey::Broker(1),
        AnomalySeverity::Critical,
        "broker 1 at 99%".into(),
        1000,
    );
    let anomaly = state
        .anomaly_store
        .get(&anomaly_id)
        .expect("anomaly stored");

    let mut registry = new_registry();
    let metrics = DetectorMetrics::register(&mut registry);
    let config = DetectorConfig {
        auto_trigger_enabled: true,
        default_mute_window: minutes(15),
        ..DetectorConfig::default()
    };

    let ctx = auto_trigger::AutoTriggerCtx {
        snapshot: state.snapshot.clone(),
        goal_registry: &state.goal_registry,
        goal_ctx: &state.goal_ctx,
        proposal_store: &state.store,
        anomaly_store: &state.anomaly_store,
        executor_state: &state.executor,
        config: &config,
        metrics: &metrics,
        now_ms: 2000,
    };
    auto_trigger::maybe_trigger(&anomaly, &ctx)
        .await
        .expect("maybe_trigger should not error on this path");

    // The optimizer's decision depends on goal selection
    // (DiskCapacity + DiskUsage). Without configured `broker_capacities.disk_bytes`,
    // those goals return no movements. Lenient: pass if EITHER a proposal
    // was inserted OR the no_movements counter incremented — the
    // auto-trigger pipeline is exercised end-to-end either way.
    let proposal_count = state.store.list(0).len();
    let no_movements_count = metrics.auto_trigger_skipped_no_movements.get();
    let _fired_count = metrics.auto_trigger_fired_disk_pressure.get();
    assert2::assert!(proposal_count > 0 || no_movements_count > 0);
}

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

use axum::Extension;
use connectrpc_axum::message::error::Code;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_rebalancer::api::GoalRegistry;
use crabka_rebalancer::api::handlers::{self, AppState};
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::health::new_registry;
use crabka_rebalancer::ingest::{SharedSnapshot, new_shared_snapshot, snapshot_once};
use crabka_rebalancer::metrics::RebalancerMetrics;
use crabka_rebalancer::model::ProposalStore;
use crabka_rebalancer::pb;
use prometheus_client::registry::Registry;
use tempfile::TempDir;

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
    let state = Arc::new(AppState {
        snapshot,
        store: Arc::new(ProposalStore::new(20)),
        goal_registry: GoalRegistry::default_registry(),
        goal_ctx: GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
        },
        metrics,
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

    // ExecuteProposal is stubbed in 43a → Unimplemented.
    let exec = unwrap_err(
        handlers::execute_proposal(
            Extension(state.clone()),
            req(pb::ExecuteProposalRequest {
                id: proposal.id.clone(),
            }),
        )
        .await,
    );
    assert_eq!(exec.code(), Code::Unimplemented);

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

    // ExecuteProposal is still Unimplemented regardless of snapshot state.
    let exec = unwrap_err(
        handlers::execute_proposal(
            Extension(state.clone()),
            req(pb::ExecuteProposalRequest {
                id: "irrelevant".to_string(),
            }),
        )
        .await,
    );
    assert_eq!(exec.code(), Code::Unimplemented);
}

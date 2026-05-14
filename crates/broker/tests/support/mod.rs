//! Shared helpers for broker integration tests.
//!
//! # Single-broker helper
//!
//! [`start`] / [`InProcess`] boot one broker + one client for simple
//! unit-style integration tests.
//!
//! # Multi-broker helpers
//!
//! [`start_n_node_with_retry`] boots an `n`-broker cluster with
//! ephemeral ports + short raft timings. Each `tests/*.rs` integration-test
//! crate that needs a 3-broker cluster declares `mod support;` and reaches
//! in for `start_n_node_with_retry`.
//!
//! Cargo treats `tests/support/mod.rs` (rather than `tests/support.rs`) as
//! a non-binary submodule, so it doesn't get compiled as its own test
//! crate.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use tempfile::TempDir;

use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle};
use crabka_client_core::Client;

pub struct InProcess {
    pub broker: BrokerHandle,
    pub client: Client,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-broker-test")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

// ── Multi-broker helpers ──────────────────────────────────────────────────────
//
// The functions below are only meaningful on non-Windows targets because
// openraft's debug_assert! races on the hosted Windows task scheduler.
// Individual test files gate their use with `#![cfg(not(target_os = "windows"))]`.

/// Lazily-initialized tracing subscriber so `RUST_LOG=...` works in
/// integration tests. Safe to call multiple times; `try_init` is a no-op
/// after the first success.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Reserve `n` pairs of ephemeral loopback ports (client + controller per
/// broker) via the bind-and-drop trick: bind a `TcpListener` on
/// `127.0.0.1:0`, read its assigned port, then drop the listener. The OS
/// won't immediately reuse the port for another bind, so we can pass it
/// to `Broker::start` and the broker re-binds it on the same address.
///
/// Avoids the Linux `TIME_WAIT` trap that fixed ports hit when multiple
/// tests in the same binary boot 3-broker clusters back-to-back.
pub async fn bind_and_drop_ports(n: usize) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let mut client_addrs = Vec::with_capacity(n);
    let mut controller_addrs = Vec::with_capacity(n);
    for _ in 0..n {
        let cl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_addrs.push(cl.local_addr().unwrap());
        let ct = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        controller_addrs.push(ct.local_addr().unwrap());
        drop((cl, ct));
    }
    (client_addrs, controller_addrs)
}

/// Build a `BrokerConfig` for broker `i` (0-indexed) in an `n`-broker
/// cluster using the supplied ephemeral port lists + voter map. All
/// callers want the same `BrokerConfig::for_tests`-style short raft
/// timings; this helper centralizes the boilerplate so individual tests
/// don't drift on field values when `BrokerConfig` grows.
pub fn broker_config(
    i: usize,
    client_addrs: &[SocketAddr],
    controller_addrs: &[SocketAddr],
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
    mode: BootstrapMode,
) -> BrokerConfig {
    let listen = client_addrs[i];
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.node_id = u64::try_from(i + 1).unwrap();
    cfg.controller_listen_addr = controller_addrs[i];
    cfg.controller_quorum_voters = voters.to_vec();
    cfg.bootstrap_mode = mode;
    cfg
}

/// Boot an `n`-broker cluster with ephemeral ports + short raft timings
/// using a deterministic bootstrap-then-join pattern:
///
/// * Phase 1: broker 0 boots alone in `Bootstrap` mode — singleton voter,
///   trivially elects itself with no split-vote risk.
/// * Phase 2: brokers 1..n start in `Join` mode — their `Broker::start`
///   blocks waiting for a raft leader to appear.
/// * Phase 3: the bootstrap broker calls `add_learner` for each joiner,
///   then promotes them all to voters via a single `change_membership`.
///   The joiners' `watch_leader` fires and their `Broker::start` returns.
///
/// Returns `(handle, config, tempdir)` triples preserving spawn order;
/// `cluster[0]` is `broker_id` 1.
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    init_tracing();

    let n_usize = usize::try_from(n).unwrap();
    let (client_addrs, controller_addrs) = bind_and_drop_ports(n_usize).await;
    let voters: Vec<(u64, SocketAddr)> = (0..n)
        .map(|i| (i + 1, controller_addrs[usize::try_from(i).unwrap()]))
        .collect();

    // Phase 1: bootstrap broker 0 alone. Initializes as singleton voter,
    // becomes leader on first election timeout (no contention).
    let dir0 = TempDir::new().unwrap();
    let cfg0 = broker_config(
        0,
        &client_addrs,
        &controller_addrs,
        &voters,
        dir0.path(),
        BootstrapMode::Bootstrap,
    );
    let broker0 = Broker::start(cfg0.clone()).await?;

    // Phase 2: spawn brokers 1..n in Join mode. Their Broker::start
    // blocks on watch_leader; we'll add_learner + change_membership below
    // to make them part of the cluster.
    let mut join_handles = Vec::with_capacity(n_usize.saturating_sub(1));
    let mut join_metas: Vec<(TempDir, BrokerConfig)> =
        Vec::with_capacity(n_usize.saturating_sub(1));
    for i in 1..n_usize {
        let dir = TempDir::new().unwrap();
        let cfg = broker_config(
            i,
            &client_addrs,
            &controller_addrs,
            &voters,
            dir.path(),
            BootstrapMode::Join,
        );
        let cfg_clone = cfg.clone();
        join_handles.push(tokio::spawn(async move { Broker::start(cfg_clone).await }));
        join_metas.push((dir, cfg));
    }

    // Phase 3: add each Join broker as a learner, then promote them all
    // to voters in a single change_membership. The bootstrap broker
    // replicates the existing log to each follower as part of add_learner.
    for (idx, addr) in controller_addrs
        .iter()
        .enumerate()
        .skip(1)
        .take(n_usize - 1)
    {
        broker0
            .add_learner(u64::try_from(idx + 1).unwrap(), *addr)
            .await?;
    }
    let target_voters: std::collections::BTreeSet<u64> =
        (1..=u64::try_from(n_usize).unwrap()).collect();
    broker0.change_membership(target_voters).await?;

    // Now join brokers' watch_leader fires and Broker::start returns.
    let mut out: Vec<(BrokerHandle, BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    out.push((broker0, cfg0, dir0));
    for (h, (dir, cfg)) in join_handles.into_iter().zip(join_metas) {
        let broker = h.await.expect("broker spawn join")?;
        out.push((broker, cfg, dir));
    }
    Ok(out)
}

/// Retry `start_n_node` up to 3 times. Short raft timings occasionally
/// split-vote on slow runners; a fresh tempdir + port set on retry
/// clears the openraft state and usually succeeds within 2 attempts.
pub async fn start_n_node_with_retry(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match start_n_node(n).await {
            Ok(cluster) => return cluster,
            Err(e) => {
                tracing::warn!(attempt, error = %e, "cluster start failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts; last error: {last_err:?}");
}

/// Poll every broker's controller image until each one sees `n`
/// brokers registered. Required before any test that needs the
/// partition's replica set to include all `n` nodes (`CreateTopics`
/// reads `image.brokers()` to pick replicas; a race here silently
/// degrades to a smaller replica set).
pub async fn wait_for_all_brokers_registered(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    n: usize,
) {
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        let mut all = true;
        for (h, _, _) in cluster {
            if h.broker_count().await < n {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "brokers didn't converge on {n}-broker view within 2 min"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

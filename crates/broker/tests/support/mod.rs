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

use assert2::assert;
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
// Individual test files gate their use with ``.

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
/// cluster using the supplied ephemeral port lists + static voter map.
/// This is the *static-voter* bootstrap-then-join helper, kept for tests
/// (like `elect_leaders`) that drive `add_learner` / `change_membership`
/// manually and need to layer extra config overrides per broker — a flow
/// that `start_n_node`'s auto-join path can't accommodate.
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

/// Build a `BrokerConfig` for broker `i` (0-indexed) in a static `n`-voter
/// cluster. Every broker boots in `Bootstrap` mode with the *same* configured
/// `controller_quorum_voters` set, so each node seeds the full voter set and
/// elects among the configured peers over the real KIP-595 wire — no
/// auto-join (KIP-853 dynamic reconfig is Slice 5).
fn static_voter_broker_config(
    i: usize,
    own_client_addr: SocketAddr,
    own_controller_addr: SocketAddr,
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.node_id = u64::try_from(i + 1).unwrap();
    // Bind a concrete (pre-bound) client port. The broker self-registers its
    // `advertised_listener` host:port into the controller image *before* it
    // binds its listeners and rewrites a `:0` advertised port to the real one
    // — so a `:0` here would register port 0 and break the inter-broker
    // heartbeat / replication dial. Give it a real port up front.
    cfg.listen_addr = own_client_addr;
    cfg.advertised_listener = own_client_addr.to_string();
    // The controller listener must bind the *same* concrete port that this
    // node advertises in the shared voter set, or its peers can't dial it.
    cfg.controller_listen_addr = own_controller_addr;
    cfg.directory_id = uuid::Uuid::from_u128(u128::from(cfg.node_id));
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = voters.to_vec();
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg
}

/// Boot an `n`-broker cluster with ephemeral ports + short raft timings via
/// **static multi-voter bootstrap** (KIP-595 Slice 3c):
///
/// * All `n` brokers boot in `Bootstrap` mode (`auto_join = false`), each
///   configured with the *same* `controller_quorum_voters` = the full
///   `[(1, ctrl_addr_1), …, (n, ctrl_addr_n)]` set.
/// * Each node seeds the full static voter set and they elect a leader among
///   themselves over the real KIP-595 wire — no `AddRaftVoter` / auto-join.
///
/// Blocks until a leader emerges and reports the full `n`-voter committed set.
/// Returns `(handle, config, tempdir)` triples preserving spawn order;
/// `cluster[0]` is `broker_id` 1.
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    init_tracing();

    let n_usize = usize::try_from(n).unwrap();

    // Reserve concrete client + controller ports for every broker by binding
    // ephemeral loopback listeners and *holding them live* until each broker
    // adopts its pair via `Broker::start_with_listeners`. The ports must be
    // concrete up front: each controller addr goes into the shared static
    // voter set so peers can dial it, and each broker self-registers its
    // advertised client `host:port` into the controller image *before* it
    // binds its data-plane listener — a `:0` there would register port 0 and
    // break the inter-broker heartbeat / replication dial.
    //
    // Unlike the bind-and-drop trick (`bind_and_drop_ports`), these sockets are
    // never dropped before the broker re-binds them, so there is no TOCTOU
    // window for a concurrently-running test to steal a just-released port —
    // the `AddrInUse` flake under parallel `cargo test` / `cargo llvm-cov`.
    let mut client_listeners = Vec::with_capacity(n_usize);
    let mut controller_listeners = Vec::with_capacity(n_usize);
    for _ in 0..n_usize {
        client_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
        controller_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
    }
    let client_addrs: Vec<SocketAddr> = client_listeners
        .iter()
        .map(tokio::net::TcpListener::local_addr)
        .collect::<std::io::Result<_>>()?;
    let controller_addrs: Vec<SocketAddr> = controller_listeners
        .iter()
        .map(tokio::net::TcpListener::local_addr)
        .collect::<std::io::Result<_>>()?;

    // The shared static voter set every node is configured with.
    let voters: Vec<(u64, SocketAddr)> = (0..n_usize)
        .map(|i| (u64::try_from(i + 1).unwrap(), controller_addrs[i]))
        .collect();

    // Start all n brokers in Bootstrap mode with the same voter set,
    // *concurrently*. `Broker::start*` blocks until the cold-boot controller
    // sees a committed leader (step 2: it waits on `watch_leader` before
    // submitting its self-registration), and a leader can only be elected once
    // a majority of the static voter set is up and dialable. So a sequential
    // `start().await` on the first broker would deadlock — it can never elect
    // alone. Spawn every broker's `start` and join them.
    let mut starts = Vec::with_capacity(n_usize);
    let mut metas: Vec<(BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    for (i, (data_listener, controller_listener)) in client_listeners
        .into_iter()
        .zip(controller_listeners)
        .enumerate()
    {
        let dir = TempDir::new().unwrap();
        let cfg = static_voter_broker_config(
            i,
            client_addrs[i],
            controller_addrs[i],
            &voters,
            dir.path(),
        );
        let cfg_for_spawn = cfg.clone();
        starts.push(tokio::spawn(async move {
            Broker::start_with_listeners(
                cfg_for_spawn,
                Some(controller_listener),
                Some(data_listener),
            )
            .await
        }));
        metas.push((cfg, dir));
    }

    let mut out: Vec<(BrokerHandle, BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    for (handle, (cfg, dir)) in starts.into_iter().zip(metas) {
        let broker = handle
            .await
            .map_err(|e| BrokerError::Startup(format!("broker start task panicked: {e}")))??;
        out.push((broker, cfg, dir));
    }

    // Wait (bounded) for the static set to elect a leader and for *some* node
    // to report the full `n`-voter committed set. With a static set every node
    // is seeded with all `n` voters from the start, so once a leader emerges
    // the count is `n` immediately; we poll all handles so whichever node won
    // the election satisfies the check.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut any_leader_with_full_set = false;
        for (h, _, _) in &out {
            if h.controller_leader_id().await.is_some() && h.voter_count_for_test() >= n_usize {
                any_leader_with_full_set = true;
                break;
            }
        }
        if any_leader_with_full_set {
            break;
        }
        if std::time::Instant::now() > deadline {
            let counts: Vec<usize> = out
                .iter()
                .map(|(h, _, _)| h.voter_count_for_test())
                .collect();
            let mut leaders = Vec::with_capacity(out.len());
            for (h, _, _) in &out {
                leaders.push(h.controller_leader_id().await);
            }
            return Err(BrokerError::Startup(format!(
                "static cluster did not elect a leader with {n_usize} voters within 30s \
                 (voter counts={counts:?}, leader_ids={leaders:?})"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
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

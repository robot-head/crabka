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

/// Build a `BrokerConfig` for broker 0 (the bootstrap node). It binds a
/// concrete (pre-bound) controller port so its self-bootstrap seed advertises
/// a reachable endpoint, and an ephemeral client listener.
fn bootstrap_broker_config(
    bootstrap_client_addr: SocketAddr,
    bootstrap_controller_addr: SocketAddr,
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = 1;
    cfg.node_id = 1;
    // Bind a concrete (pre-bound) client port. The broker self-registers its
    // `advertised_listener` host:port into the controller image *before* it
    // binds its listeners and rewrites a `:0` advertised port to the real one
    // — so a `:0` here would register port 0 and break the inter-broker
    // heartbeat / replication dial. Give it a real port up front.
    cfg.listen_addr = bootstrap_client_addr;
    cfg.advertised_listener = bootstrap_client_addr.to_string();
    cfg.directory_id = uuid::Uuid::from_u128(1);
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_listen_addr = bootstrap_controller_addr;
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg
}

/// Build a `BrokerConfig` for a joiner (broker `i`, 0-indexed, `i >= 1`).
/// Joiners boot in `Join` mode with `auto_join` enabled and
/// `bootstrap_servers` pointing at the bootstrap broker's **client** listener
/// — that's where the `AddRaftVoter` (`api_key` 80) handler lives (the
/// controller listener only serves raft RPCs). They bind ephemeral ports for
/// both listeners; auto-join advertises their *real* bound controller addr to
/// the leader so its `add_learner` can dial them back.
fn joiner_broker_config(
    i: usize,
    own_client_addr: SocketAddr,
    bootstrap_client_addr: SocketAddr,
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.node_id = u64::try_from(i + 1).unwrap();
    // Concrete (pre-bound) client port for self-registration — see the note in
    // `bootstrap_broker_config`. The controller listener can stay `:0` because
    // auto-join advertises its *real bound* controller addr to the leader.
    cfg.listen_addr = own_client_addr;
    cfg.advertised_listener = own_client_addr.to_string();
    cfg.directory_id = uuid::Uuid::from_u128(u128::from(cfg.node_id));
    cfg.bootstrap_mode = BootstrapMode::Join;
    cfg.controller_listen_addr = "127.0.0.1:0".parse().unwrap();
    cfg.auto_join = true;
    cfg.bootstrap_servers = vec![bootstrap_client_addr];
    cfg
}

/// Boot an `n`-broker cluster with ephemeral ports + short raft timings via
/// KIP-853 auto-join:
///
/// * Broker 0 boots in `Bootstrap` mode on a concrete controller port. Its
///   standalone self-bootstrap forms a single-voter cluster of itself and it
///   self-elects on the first election timeout (no contention).
/// * Brokers 1..n boot in `Join` mode with `auto_join = true` and
///   `bootstrap_servers = [broker0_client_addr]`. Each runs the auto-join
///   loop, sending `AddRaftVoter(self)` to broker 0's client listener (the
///   leader), which replicates the log, promotes them, and commits the new
///   `V1Voters` set.
///
/// Blocks until the leader's committed voter set reaches size `n`. Returns
/// `(handle, config, tempdir)` triples preserving spawn order; `cluster[0]`
/// is `broker_id` 1.
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    init_tracing();

    let n_usize = usize::try_from(n).unwrap();

    // Pre-bind concrete client ports for every broker (and one concrete
    // controller port for broker 0's self-bootstrap seed). The broker
    // self-registers its advertised client `host:port` into the controller
    // image *before* binding its listeners, so a `:0` advertised port would
    // register port 0 and break the inter-broker heartbeat / replication dial.
    // The bind-and-drop trick avoids the TIME_WAIT trap of fixed ports across
    // back-to-back cluster boots.
    let (client_addrs, controller_addrs) = bind_and_drop_ports(n_usize).await;
    let bootstrap_controller_addr = controller_addrs[0];

    // Broker 0: bootstrap. Must be up (and leader) before joiners can join.
    let dir0 = TempDir::new().unwrap();
    let cfg0 = bootstrap_broker_config(client_addrs[0], bootstrap_controller_addr, dir0.path());
    let broker0 = Broker::start(cfg0.clone()).await?;
    // The joiners send `AddRaftVoter` to the leader's *client* data-plane
    // listener (that's where api_key 80 is served), not its controller
    // listener — so point them at broker 0's bound client address.
    let bootstrap_client_addr = broker0.listen_addr();

    // Brokers 1..n: join via auto-join. Their `Broker::start` returns once
    // openraft hands them their first leader (the auto-join task keeps running
    // in the background, driving the join, until they're voters).
    let mut out: Vec<(BrokerHandle, BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    out.push((broker0, cfg0, dir0));
    for (i, &own_client_addr) in client_addrs.iter().enumerate().take(n_usize).skip(1) {
        let dir = TempDir::new().unwrap();
        let mut cfg = joiner_broker_config(i, own_client_addr, bootstrap_client_addr, dir.path());
        let broker = Broker::start(cfg.clone()).await?;
        // The controller listener used `:0`; record its real bound port so
        // tests that read `cfg.controller_listen_addr` get the resolved value.
        cfg.controller_listen_addr = broker.controller_addr();
        out.push((broker, cfg, dir));
    }

    // Wait for the bootstrap broker's committed voter set to reach `n`. This is
    // what proves auto-join converged. Bounded so a stuck join fails the test
    // rather than hanging forever.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if out[0].0.voter_count_for_test() >= n_usize {
            break;
        }
        if std::time::Instant::now() > deadline {
            return Err(BrokerError::Startup(format!(
                "auto-join did not reach {n_usize} voters within 30s (have {})",
                out[0].0.voter_count_for_test()
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

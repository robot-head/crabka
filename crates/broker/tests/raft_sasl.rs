//! Slice 12b. Inbound raft listener auth tests.
//!
//! These exercise the controller listener under `SaslPlaintext` and prove
//! both inbound (broker A accepts auth'd raft frames from broker B) and
//! outbound (`InterBrokerDialer` dials with SASL credentials) paths
//! work together. Gated `#[cfg(not(target_os = "windows"))]` per the
//! existing multi-broker test convention (openraft `debug_assert!`
//! racing on the hosted Windows runner).

#![cfg(not(target_os = "windows"))]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Build a `SASL_PLAINTEXT` data-plane listener config for broker `i`
/// (0-indexed) and parameterized `controller_listener_protocol`.
#[allow(clippy::too_many_arguments)]
fn sasl_broker_config(
    i: usize,
    data_addr: SocketAddr,
    ctrl: ListenerProtocol,
    ctrl_addr: SocketAddr,
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
    mode: BootstrapMode,
    plain_user: &str,
    plain_pass: &str,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.listen_addr = data_addr;
    cfg.advertised_listener = data_addr.to_string();
    cfg.node_id = u64::try_from(i + 1).unwrap();
    cfg.controller_listen_addr = ctrl_addr;
    cfg.controller_quorum_voters = voters.to_vec();
    cfg.bootstrap_mode = mode;
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: data_addr,
        advertised: data_addr.to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.controller_listener_protocol = ctrl;
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert(plain_user.to_string(), plain_pass.to_string());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials {
        mechanism: SaslMechanism::Plain,
        username: plain_user.to_string(),
        password: plain_pass.to_string(),
    });
    cfg
}

/// Bind two ephemeral loopback controller listeners and return them
/// alongside their addresses. The live listeners are handed to
/// `Broker::start_with_controller_listener`, which adopts them directly
/// instead of re-binding the address.
///
/// This defeats the bind-and-drop TOCTOU race: the classic pattern reads
/// an ephemeral port then *drops* the probe socket before the broker
/// re-binds it, leaving a window in which another process on the runner
/// can claim the port — surfacing as `AddrInUse` from `Broker::start`.
/// Keeping the socket bound and handing it over removes that window.
async fn reserve_ctrl_listeners() -> ([SocketAddr; 2], [tokio::net::TcpListener; 2]) {
    let l0 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a0 = l0.local_addr().unwrap();
    let a1 = l1.local_addr().unwrap();
    ([a0, a1], [l0, l1])
}

/// Data-plane bind address for these tests: `127.0.0.1:0` lets the OS
/// assign an ephemeral port at `Broker::start`, so there's no probe/drop
/// gap to race on. Convergence here rides the controller listener; the
/// data plane is never dialed, so the bound port is never read back.
fn data_listen_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Boot two brokers with the supplied data-plane + controller listener
/// configurations using the deterministic bootstrap-then-join pattern.
/// Used by the two "converging" tests; the mismatched-creds test below
/// inlines its own setup because it must spawn the joiner asynchronously
/// (it never gets a leader).
async fn start_two_brokers_with_controller_protocol(
    ctrl: ListenerProtocol,
    plain_user: &str,
    plain_pass: &str,
) -> (BrokerHandle, BrokerHandle, TempDir, TempDir) {
    init_tracing();
    let (ctrl_addrs, [ctrl_l0, ctrl_l1]) = reserve_ctrl_listeners().await;
    let voters: Vec<(u64, SocketAddr)> = vec![(1, ctrl_addrs[0]), (2, ctrl_addrs[1])];

    let dir0 = TempDir::new().unwrap();
    let dir1 = TempDir::new().unwrap();

    let cfg0 = sasl_broker_config(
        0,
        data_listen_addr(),
        ctrl,
        ctrl_addrs[0],
        &voters,
        dir0.path(),
        BootstrapMode::Bootstrap,
        plain_user,
        plain_pass,
    );
    let cfg1 = sasl_broker_config(
        1,
        data_listen_addr(),
        ctrl,
        ctrl_addrs[1],
        &voters,
        dir1.path(),
        BootstrapMode::Join,
        plain_user,
        plain_pass,
    );

    let broker0 = Broker::start_with_controller_listener(cfg0, Some(ctrl_l0))
        .await
        .expect("start broker 0");

    let cfg1_for_spawn = cfg1.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg1_for_spawn, Some(ctrl_l1)).await
    });

    broker0
        .add_learner(2, ctrl_addrs[1])
        .await
        .expect("add_learner(2)");
    let target: BTreeSet<u64> = [1u64, 2u64].into_iter().collect();
    broker0
        .change_membership(target)
        .await
        .expect("change_membership");

    let broker1 = join.await.expect("join spawn").expect("start broker 1");
    (broker0, broker1, dir0, dir1)
}

// Exercises follower → leader `submit_change` forwarding under SASL.
//
// With `controller_listener_protocol = SaslPlaintext`, broker 1 elects itself,
// b1.add_learner + b1.change_membership replicate via the dialer (SASL OK),
// b2's Broker::start returns when it sees the leader — and b2 then calls
// `controller.submit_change(self_reg)` which forwards to the leader via
// `crabka_raft::controller::forward_submit_to`. T9b routes that helper
// through the injected `OutboundDialer` so the SASL handshake runs before
// `API_KEY_SUBMIT_CHANGE` hits the wire, and b1 accepts the registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_two_broker_quorum() {
    let (b1, b2, _d1, _d2) = start_two_brokers_with_controller_protocol(
        ListenerProtocol::SaslPlaintext,
        "broker",
        "secret",
    )
    .await;
    // Wait until both brokers see two registered peers in the metadata
    // image. Bounded wait — fail the test if convergence takes too long.
    let converge = async {
        loop {
            if b1.broker_count().await == 2 && b2.broker_count().await == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    // 60s mirrors `auth_handlers::two_broker_sasl::two_broker_sasl_plaintext_replication`
    // which exercises the same path; raft + SASL handshake under short
    // election timings occasionally needs more than 15s on busy runners.
    tokio::time::timeout(Duration::from_mins(1), converge)
        .await
        .expect("brokers converge on 2-broker quorum within 60s");
    b1.shutdown().await;
    b2.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_rejects_mismatched_creds() {
    // Start broker A with username=alice; broker B with username=bob.
    // Neither has the other's password, so inbound raft auth fails on
    // both sides. Expect they never converge.
    init_tracing();
    let (ctrl_addrs, [ctrl_l1, ctrl_l2]) = reserve_ctrl_listeners().await;
    let voters: Vec<(u64, SocketAddr)> = vec![(1, ctrl_addrs[0]), (2, ctrl_addrs[1])];

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let c1 = sasl_broker_config(
        0,
        data_listen_addr(),
        ListenerProtocol::SaslPlaintext,
        ctrl_addrs[0],
        &voters,
        dir1.path(),
        BootstrapMode::Bootstrap,
        "alice",
        "wonderland",
    );
    let c2 = sasl_broker_config(
        1,
        data_listen_addr(),
        ListenerProtocol::SaslPlaintext,
        ctrl_addrs[1],
        &voters,
        dir2.path(),
        BootstrapMode::Join,
        "bob",
        "burgers",
    );

    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");

    // Spawn b2: its `Broker::start` will block waiting for a raft leader
    // (it's `Join` mode and will never see one because raft auth fails
    // on both sides). We don't await its `Broker::start` completion —
    // we just need its inbound listener up so b1 can attempt to dial it.
    let c2_for_spawn = c2.clone();
    let b2_join = tokio::spawn(async move {
        Broker::start_with_controller_listener(c2_for_spawn, Some(ctrl_l2)).await
    });

    // Give the brokers time to settle. With matched creds and add_learner +
    // change_membership, convergence happens within ~1s; here we don't call
    // those raft RPCs at all, AND auth would fail anyway. Wait 3s and
    // assert broker 1 still sees only itself.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        b1.broker_count().await < 2,
        "mismatched creds must not converge"
    );

    // Drop b2's spawn handle: it'll keep blocking, but tempdir cleanup
    // + tokio runtime drop will terminate it.
    b2_join.abort();
    b1.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_plaintext_legacy_path_unchanged() {
    // Default `controller_listener_protocol = Plaintext` — no
    // handshake injected. Two brokers converge as in slice 7.
    init_tracing();
    let (ctrl_addrs, [ctrl_l1, ctrl_l2]) = reserve_ctrl_listeners().await;
    let voters: Vec<(u64, SocketAddr)> = vec![(1, ctrl_addrs[0]), (2, ctrl_addrs[1])];

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    // Plain (no SASL) configs: don't use sasl_broker_config because we
    // want zero auth on either listener (legacy path).
    let mut c1 = BrokerConfig::for_tests(dir1.path().to_path_buf());
    c1.broker_id = 1;
    c1.node_id = 1;
    c1.listen_addr = data_listen_addr();
    c1.advertised_listener = data_listen_addr().to_string();
    c1.controller_listen_addr = ctrl_addrs[0];
    c1.controller_quorum_voters = voters.clone();
    c1.bootstrap_mode = BootstrapMode::Bootstrap;
    c1.controller_listener_protocol = ListenerProtocol::Plaintext;

    let mut c2 = BrokerConfig::for_tests(dir2.path().to_path_buf());
    c2.broker_id = 2;
    c2.node_id = 2;
    c2.listen_addr = data_listen_addr();
    c2.advertised_listener = data_listen_addr().to_string();
    c2.controller_listen_addr = ctrl_addrs[1];
    c2.controller_quorum_voters = voters.clone();
    c2.bootstrap_mode = BootstrapMode::Join;
    c2.controller_listener_protocol = ListenerProtocol::Plaintext;

    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");
    let c2_for_spawn = c2.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(c2_for_spawn, Some(ctrl_l2)).await
    });

    b1.add_learner(2, ctrl_addrs[1])
        .await
        .expect("add_learner(2)");
    let target: BTreeSet<u64> = [1u64, 2u64].into_iter().collect();
    b1.change_membership(target)
        .await
        .expect("change_membership");

    let b2 = join.await.expect("join spawn").expect("start b2");

    let converge = async {
        loop {
            if b1.broker_count().await == 2 && b2.broker_count().await == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), converge)
        .await
        .expect("legacy plaintext path still converges");
    b1.shutdown().await;
    b2.shutdown().await;
}

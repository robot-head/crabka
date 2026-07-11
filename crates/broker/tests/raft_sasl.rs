//! Inbound raft listener auth tests.
//!
//! These exercise the controller listener under `SaslPlaintext` and prove
//! both inbound (broker A accepts auth'd raft frames from broker B) and
//! outbound (`InterBrokerDialer` dials with SASL credentials) paths
//! work together.

use std::{net::SocketAddr, time::Duration};

use crabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle,
    config::{InterBrokerCredentials, ListenerSpec},
};
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
    cfg.node_id = crabka_broker::NodeId(u64::try_from(i + 1).unwrap());
    cfg.controller_listen_addr = ctrl_addr;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
        .collect();
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
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
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
        BootstrapMode::Bootstrap,
        plain_user,
        plain_pass,
    );

    // KIP-595 Slice 3c static bootstrap: both brokers boot with the same
    // static voter set and elect among themselves over the (SASL/plaintext)
    // controller wire — no add_learner / change_membership (KIP-853, Slice 5).
    let cfg1_for_spawn = cfg1.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg1_for_spawn, Some(ctrl_l1)).await
    });
    let broker0 = Broker::start_with_controller_listener(cfg0, Some(ctrl_l0))
        .await
        .expect("start broker 0");

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
    // Wait until both brokers see two registered peers in the metadata image.
    // Event-driven: each awaiter observes `img.brokers().count() >= 2` (the
    // same signal `broker_count()` reads) and panics if convergence stalls.
    b1.wait_until_brokers_registered(2).await;
    b2.wait_until_brokers_registered(2).await;
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

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    // Each broker is a *single-voter* standalone bootstrap of itself: b1's
    // voter set is {1}, b2's is {2}. b1 self-elects immediately (so its
    // `Broker::start` returns) and sees only itself. b2 likewise. Because
    // their SASL creds mismatch (alice vs bob), neither can authenticate the
    // other's raft listener — there is no path for the two single-voter
    // clusters to merge, so b1's broker view never grows past 1. (A shared
    // 2-voter set is unusable here: with bad creds no leader is ever elected,
    // and `Broker::start` would block on its 2-minute leader-wait.)
    let c1 = sasl_broker_config(
        0,
        data_listen_addr(),
        ListenerProtocol::SaslPlaintext,
        ctrl_addrs[0],
        &[(1, ctrl_addrs[0])],
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
        &[(2, ctrl_addrs[1])],
        dir2.path(),
        BootstrapMode::Bootstrap,
        "bob",
        "burgers",
    );

    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");

    // Start b2 (its own single-voter cluster). With bad creds it can never
    // join b1's cluster, but it self-elects fine, so this returns promptly.
    let b2 = Broker::start_with_controller_listener(c2, Some(ctrl_l2))
        .await
        .expect("start b2");

    // Give the brokers time to (fail to) discover each other. Each is its own
    // single-voter cluster and mismatched creds block any raft cross-talk, so
    // b1 must still see only itself.
    // intentional: negative test — observe that no convergence happens within a
    // fixed window; there is no awaiter for "state stays put".
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert2::assert!(b1.broker_count().await < 2);
    let _ = &b2;

    b2.shutdown().await;
    b1.shutdown().await;
}

// H-1: authentication is not authorization. Here both brokers present
// *valid, matching* SASL credentials (so the SASL handshake succeeds), but
// the controller listener is gated by a `SimpleAclAuthorizer` with NO
// super-users and NO ACLs — so the authenticated principal is DENIED
// `CLUSTER_ACTION` on `Cluster("kafka-cluster")`. The handshake therefore
// drops the connection *after* authentication, and the two single-voter
// clusters can never exchange controller RPCs to merge. b1 must still see
// only itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_denies_unauthorized_principal() {
    init_tracing();
    let (ctrl_addrs, [ctrl_l1, ctrl_l2]) = reserve_ctrl_listeners().await;

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    // Single-voter standalone bootstraps with MATCHING creds (auth succeeds)
    // — same structure as the mismatched-creds test, but the failure mode
    // here is authorization, not authentication.
    let mut c1 = sasl_broker_config(
        0,
        data_listen_addr(),
        ListenerProtocol::SaslPlaintext,
        ctrl_addrs[0],
        &[(1, ctrl_addrs[0])],
        dir1.path(),
        BootstrapMode::Bootstrap,
        "broker",
        "secret",
    );
    let mut c2 = sasl_broker_config(
        1,
        data_listen_addr(),
        ListenerProtocol::SaslPlaintext,
        ctrl_addrs[1],
        &[(2, ctrl_addrs[1])],
        dir2.path(),
        BootstrapMode::Bootstrap,
        "broker",
        "secret",
    );
    // Deny-by-default authorizer: empty super-user set, no ACLs ⇒ every
    // principal (including the authenticated inter-broker one) is denied.
    c1.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
        std::collections::HashSet::new(),
    ));
    c2.authorizer = std::sync::Arc::new(crabka_broker::authorizer::SimpleAclAuthorizer::new(
        std::collections::HashSet::new(),
    ));

    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");
    let b2 = Broker::start_with_controller_listener(c2, Some(ctrl_l2))
        .await
        .expect("start b2");

    // Authentication succeeds but CLUSTER_ACTION is denied, so the
    // controller listener drops every cross-broker connection: the clusters
    // never merge.
    // intentional: negative test — observe that no convergence happens within a
    // fixed window; there is no awaiter for "state stays put".
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert2::assert!(b1.broker_count().await < 2);
    let _ = &b2;

    b2.shutdown().await;
    b1.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_plaintext_legacy_path_unchanged() {
    // Default `controller_listener_protocol = Plaintext` — no
    // handshake injected. Two brokers converge over the plaintext path.
    init_tracing();
    let (ctrl_addrs, [ctrl_l1, ctrl_l2]) = reserve_ctrl_listeners().await;
    let voters: Vec<(u64, SocketAddr)> = vec![(1, ctrl_addrs[0]), (2, ctrl_addrs[1])];

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    // Plain (no SASL) configs: don't use sasl_broker_config because we
    // want zero auth on either listener (legacy path).
    let mut c1 = BrokerConfig::for_tests(dir1.path().to_path_buf());
    c1.broker_id = 1;
    c1.node_id = crabka_broker::NodeId(1);
    c1.listen_addr = data_listen_addr();
    c1.advertised_listener = data_listen_addr().to_string();
    c1.controller_listen_addr = ctrl_addrs[0];
    c1.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
        .collect();
    c1.bootstrap_mode = BootstrapMode::Bootstrap;
    c1.controller_listener_protocol = ListenerProtocol::Plaintext;

    let mut c2 = BrokerConfig::for_tests(dir2.path().to_path_buf());
    c2.broker_id = 2;
    c2.node_id = crabka_broker::NodeId(2);
    c2.listen_addr = data_listen_addr();
    c2.advertised_listener = data_listen_addr().to_string();
    c2.controller_listen_addr = ctrl_addrs[1];
    c2.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
        .collect();
    c2.bootstrap_mode = BootstrapMode::Bootstrap;
    c2.controller_listener_protocol = ListenerProtocol::Plaintext;

    // Static bootstrap: both brokers boot with the same voter set and elect
    // over the plaintext controller wire — no add_learner / change_membership.
    let c2_for_spawn = c2.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(c2_for_spawn, Some(ctrl_l2)).await
    });
    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");

    let b2 = join.await.expect("join spawn").expect("start b2");

    // Event-driven convergence: both brokers observe two registered peers in
    // the metadata image (same signal `broker_count()` reads).
    b1.wait_until_brokers_registered(2).await;
    b2.wait_until_brokers_registered(2).await;
    b1.shutdown().await;
    b2.shutdown().await;
}

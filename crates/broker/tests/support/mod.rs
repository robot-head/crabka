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

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId};
use crabka_client_core::Client;
use tempfile::TempDir;

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

/// Start a broker rooted at `dir` (caller owns the directory).
///
/// Used by restart tests: pass the same path across two boots to verify
/// that persistent state (audit chain, spool) is recovered correctly.
/// Automatically detects if a raft log already exists and uses `Rejoin`.
pub async fn start_with_dir(dir: &std::path::Path) -> (BrokerHandle, crabka_client_core::Client) {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    // Mirror the production heuristic from `detect_bootstrap_mode` in
    // broker.rs: key Rejoin on `metadata_log_nonempty` (committed
    // quorum-state), NOT bare directory presence.  The segment dir is created
    // before the first raft commit, so dir-existence would re-bootstrap a node
    // killed mid-election instead of letting it rejoin correctly.
    let metadata_dir = dir.join("__cluster_metadata");
    if crabka_raft::metadata_log_nonempty(&metadata_dir) {
        config.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
    }
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = crabka_client_core::Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-broker-test")
        .build()
        .await
        .expect("client build");
    (broker, client)
}

/// Fetch the audit topic and return the `seq` header value (parsed as `u64`)
/// from each non-checkpoint record, in order.
pub async fn audit_record_seqs(client: &crabka_client_core::Client) -> Vec<u64> {
    use crabka_broker::coordinator::AUDIT_TOPIC;
    use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};

    let topic_id = topic_id_for(client, AUDIT_TOPIC).await;
    let fr = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("FetchRequest for audit topic");

    let mut seqs = Vec::new();
    if let Some(part) = fr.responses.first().and_then(|r| r.partitions.first())
        && let Some(batches) = part.records.as_ref().and_then(|r| r.as_v2())
    {
        for batch in batches {
            for rec in &batch.records {
                // Skip checkpoint records — they have no `seq` header.
                let is_checkpoint = rec
                    .headers
                    .iter()
                    .any(|h| h.key == "event_class" && h.value.as_deref() == Some(b"checkpoint"));
                if is_checkpoint {
                    continue;
                }
                if let Some(seq_val) = rec
                    .headers
                    .iter()
                    .find(|h| h.key == "seq")
                    .and_then(|h| h.value.as_ref())
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    seqs.push(seq_val);
                }
            }
        }
    }
    seqs
}

/// Start a broker configured with an audit signing key and a given checkpoint cadence.
///
/// Uses `every_secs = 3600` so only the count-based trigger fires in tests.
pub async fn start_with_audit_key(
    key_path: &std::path::Path,
    key_id: &str,
    every_n: u64,
) -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.audit_signing_key_path = Some(key_path.to_path_buf());
    config.audit_signing_key_id = Some(key_id.to_string());
    config.audit_checkpoint_every_n = every_n;
    config.audit_checkpoint_every_secs = 3600; // only count trigger fires
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-broker-test-audit-key")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

/// Start a broker whose authorizer is `SimpleAclAuthorizer` with no ACLs and no
/// super-users (deny-all for the anonymous test client). Audit is enabled via
/// `for_tests` defaults. The anonymous client will be denied every admin
/// operation, triggering `AuthorizationDenied` audit events.
pub async fn start_with_deny_all_authz() -> InProcess {
    use std::collections::HashSet;

    use crabka_broker::authorizer::SimpleAclAuthorizer;

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    // Replace the default AllowAllAuthorizer with a deny-all SimpleAclAuthorizer
    // (empty ACL store, no super-users). The anonymous test client connects
    // with no credentials so it has no super-user bypass — every operation is
    // denied and the auditing decorator emits AuthorizationDenied events.
    config.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(HashSet::new()));
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-broker-test-deny")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

/// Fetch all records from `AUDIT_TOPIC` partition 0 and JSON-decode each
/// record value, returning the decoded objects. Mirrors the
/// `broker_started_event_is_written_to_audit_topic` fetch pattern.
pub async fn wait_for_audit_record<F>(
    client: &crabka_client_core::Client,
    what: &str,
    mut predicate: F,
) -> Vec<serde_json::Value>
where
    F: FnMut(&serde_json::Value) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = consume_audit_records(client).await;
        if records.iter().any(&mut predicate) {
            return records;
        }
        assert!(
            Instant::now() <= deadline,
            "audit record '{what}' did not appear within 30s; last={records:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_for_audit_seq_count(
    client: &crabka_client_core::Client,
    min_count: usize,
) -> Vec<u64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seqs = audit_record_seqs(client).await;
        if seqs.len() >= min_count {
            return seqs;
        }
        assert!(
            Instant::now() <= deadline,
            "audit seq count did not reach {min_count} within 30s; last={seqs:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn consume_audit_records(client: &crabka_client_core::Client) -> Vec<serde_json::Value> {
    use crabka_broker::coordinator::AUDIT_TOPIC;
    use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};

    let topic_id = topic_id_for(client, AUDIT_TOPIC).await;
    let fr = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("FetchRequest for audit topic");

    let mut records = Vec::new();
    if let Some(part) = fr.responses.first().and_then(|r| r.partitions.first())
        && let Some(batches) = part.records.as_ref().and_then(|r| r.as_v2())
    {
        for batch in batches {
            for rec in &batch.records {
                if let Some(value) = &rec.value
                    && let Ok(j) = serde_json::from_slice::<serde_json::Value>(value)
                {
                    records.push(j);
                }
            }
        }
    }
    records
}

/// Round-trip a Metadata request to learn the topic's assigned UUID.
/// Produce / Fetch at v ≥ 13 carry only `topic_id` on the wire, so the
/// caller must plumb the real UUID through.
pub async fn topic_id_for(
    client: &crabka_client_core::Client,
    name: &str,
) -> crabka_protocol::primitives::uuid::Uuid {
    use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};

    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
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

/// Race-free replacement for [`bind_and_drop_ports`]: bind `n` pairs of
/// ephemeral loopback listeners (client + controller per broker) and return
/// their concrete addrs **alongside the still-open listeners**, index-aligned.
///
/// Hand `client_listeners[i]` / `controller_listeners[i]` to
/// [`crabka_broker::Broker::start_with_listeners`] (or
/// `start_with_controller_listener`) so the OS port is never released before
/// the broker adopts it — closing the [`bind_and_drop_ports`] TOCTOU window
/// where a concurrently-running test binary steals the freed port
/// (`AddrInUse`) under parallel `cargo nextest`.
///
/// The returned `SocketAddr`s are the listeners' real `local_addr()`s, so the
/// caller builds its static voter set / advertised addresses from them exactly
/// as with [`bind_and_drop_ports`]; the only call-site change is passing the
/// matching listener into `start_with_listeners` instead of letting
/// `Broker::start` re-bind the address.
#[allow(dead_code)] // not every test binary that includes `support` uses this
pub async fn bind_and_hold_ports(
    n: usize,
) -> (
    Vec<SocketAddr>,
    Vec<SocketAddr>,
    Vec<tokio::net::TcpListener>,
    Vec<tokio::net::TcpListener>,
) {
    let mut client_listeners = Vec::with_capacity(n);
    let mut controller_listeners = Vec::with_capacity(n);
    for _ in 0..n {
        client_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
        controller_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let client_addrs = client_listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    let controller_addrs = controller_listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();
    (
        client_addrs,
        controller_addrs,
        client_listeners,
        controller_listeners,
    )
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
    cfg.node_id = NodeId(u64::try_from(i + 1).unwrap());
    cfg.controller_listen_addr = controller_addrs[i];
    // `controller_quorum_voters` carries `<host>:<port>` strings (the dialer
    // re-resolves per connect); test voter sets are built from `SocketAddr`s,
    // so stringify here.
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (NodeId(*id), a.to_string()))
        .collect();
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
    cfg.node_id = NodeId(u64::try_from(i + 1).unwrap());
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
    cfg.directory_id = uuid::Uuid::from_u128(u128::from(cfg.node_id.0));
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (NodeId(*id), a.to_string()))
        .collect();
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
    start_n_node_with(n, |_, _| {}).await
}

/// Like [`start_n_node`] but invokes `customize(i, &mut cfg)` on each broker's
/// `BrokerConfig` before start, letting a test layer per-broker overrides
/// (e.g. `rack`, `replica_selector`) while keeping the race-free held-listener
/// bootstrap — no `bind_and_drop_ports` TOCTOU window for a concurrently
/// running test to steal a just-released port (`AddrInUse`).
pub async fn start_n_node_with(
    n: u64,
    mut customize: impl FnMut(usize, &mut BrokerConfig),
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
        let mut cfg = static_voter_broker_config(
            i,
            client_addrs[i],
            controller_addrs[i],
            &voters,
            dir.path(),
        );
        customize(i, &mut cfg);
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

    // Wait (event-driven, bounded) for the static set to elect a leader. We await
    // the first broker's controller leader watch channel rather than the panicking
    // `wait_until_controller_leader()` helper, because a timeout here must return
    // `Err` so `start_n_node_with_retry` can retry (a panic would not be retried).
    let mut leader_rx = out[0].0.watch_leader_for_test();
    let elected = tokio::time::timeout(
        Duration::from_secs(30),
        leader_rx.wait_for(|l| matches!(l, Some(id) if *id != NodeId(0))),
    )
    .await;
    let timed_out = match &elected {
        Err(_elapsed) => true,      // tokio::time::timeout fired
        Ok(Err(_recv_err)) => true, // watch channel closed unexpectedly
        Ok(Ok(_)) => false,
    };
    if timed_out {
        let counts: Vec<usize> = out
            .iter()
            .map(|(h, _, _)| h.voter_count_for_test())
            .collect();
        return Err(BrokerError::Startup(format!(
            "static cluster did not elect a leader with {n_usize} voters within 30s \
             (voter counts={counts:?})"
        )));
    }
    assert!(
        out.iter()
            .any(|(h, _, _)| h.voter_count_for_test() >= n_usize),
        "leader elected but voter set not committed to {n_usize}"
    );

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

/// Await every broker's controller image until each one sees `n` brokers
/// registered. Required before any test that needs the partition's replica set
/// to include all `n` nodes (`CreateTopics` reads `image.brokers()` to pick
/// replicas; a race here silently degrades to a smaller replica set).
///
/// Uses the panicking `wait_until_brokers_registered` awaiter — that is
/// intentional here: this helper is called directly from tests (not from the
/// `start_n_node_with_retry` path), so a timeout should fail the test loudly.
pub async fn wait_for_all_brokers_registered(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    n: usize,
) {
    for (h, _, _) in cluster {
        h.wait_until_brokers_registered(n).await;
    }
}

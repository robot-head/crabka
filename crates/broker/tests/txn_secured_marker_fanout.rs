#![cfg(not(target_os = "windows"))]
//! Cross-broker transaction-marker fan-out over a *secured* inter-broker
//! listener.
//!
//! `EndTxn` fans `WriteTxnMarkers` out to every partition leader involved in
//! the transaction. When a partition is led by a *remote* broker the marker
//! travels over the inter-broker listener, so the fan-out must run the same
//! TLS / SASL handshakes that listener demands. It dials through the shared
//! `InterBrokerClient` (which carries the inter-broker TLS connector + SASL
//! credentials), not a bare one-shot `crabka_client_core::Client` (which
//! carries neither, and so could only ever reach a PLAINTEXT inter-broker
//! listener).
//!
//! The test boots a two-broker cluster whose inter-broker listener is
//! `SASL_PLAINTEXT`, creates a topic whose two partitions round-robin onto
//! different brokers (P0 → node 1, P1 → node 2), then drives the transaction
//! control plane *directly* with SASL-authenticated low-level clients:
//! `FindCoordinator → InitProducerId → AddPartitionsToTxn → EndTxn`. The
//! partition added to the transaction is deliberately the one led by the
//! broker that is *not* the coordinator, so `EndTxn` must fan a marker to a
//! remote leader over the SASL listener. With the pre-fix one-shot dial that
//! handshake fails and `EndTxn` returns a retriable error; with the pooled
//! `InterBrokerClient` it returns `NONE`.
//!
//! Driving the control plane by hand (rather than via the high-level
//! `Producer`) sidesteps a separate, pre-existing gap where the producer's
//! transaction-coordinator connection ignores client security — keeping this
//! test focused on the broker→broker fan-out that this change fixes.
//!
//! Windows-gated like the other multi-node transactional tests (openraft +
//! tokio scheduling races on the hosted Windows runner).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle};
use crabka_client_core::Client;
use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_protocol::owned::add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction,
};
use crabka_protocol::owned::common::add_partitions_to_txn_topic::AddPartitionsToTxnTopic;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::end_txn_request::EndTxnRequest;
use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_security::{ListenerProtocol, SaslMechanism};

mod support;

const USER: &str = "broker";
const PASS: &str = "secret";
const IB_LISTENER: &str = "SASL_PLAINTEXT";
const TID: &str = "remote-fanout-tid";
const TOPIC: &str = "t";

/// A single `SASL_PLAINTEXT` data listener (also the inter-broker listener),
/// bound + advertised at the concrete `addr`. The advertised port must be
/// concrete: self-registration records `ListenerSpec::advertised` *before* the
/// listener is bound, so a `:0` would register port 0 and break the
/// inter-broker dial.
fn sasl_listener(addr: SocketAddr) -> Vec<ListenerSpec> {
    vec![ListenerSpec {
        name: IB_LISTENER.to_string(),
        bind_addr: addr,
        advertised: addr.to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }]
}

/// Layer the `SASL_PLAINTEXT` inter-broker listener + matching `SASL/PLAIN`
/// credentials onto a base config whose listener binds `addr`.
fn apply_sasl(cfg: &mut BrokerConfig, addr: SocketAddr) {
    cfg.listen_addr = addr;
    cfg.advertised_listener = addr.to_string();
    cfg.listeners = sasl_listener(addr);
    cfg.inter_broker_listener_name = IB_LISTENER.to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert(USER.to_string(), PASS.to_string());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: USER.to_string(),
        password: PASS.to_string(),
    });
}

/// Client-side `SASL/PLAIN` so the test's low-level clients authenticate
/// against the brokers' `SASL_PLAINTEXT` listener.
fn client_security() -> ClientSecurity {
    ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: USER.to_string(),
            password: PASS.to_string(),
        }),
        sasl_host: None,
    }
}

/// Open a SASL-authenticated client to `addr`.
async fn sasl_client(addr: &str) -> Client {
    Client::builder()
        .bootstrap(addr.to_string())
        .client_id("crabka-txn-fanout-test")
        .security(client_security())
        .build()
        .await
        .expect("sasl client connect")
}

/// Boot a two-broker cluster (KIP-853 auto-join) whose data / inter-broker
/// listener is `SASL_PLAINTEXT`. Mirrors `support::start_n_node`'s concrete-port
/// handling — the marker fan-out resolves the leader's advertised inter-broker
/// endpoint, which must be a real reachable port.
async fn start_two_sasl() -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    support::init_tracing();

    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(2).await;

    // Broker 0: bootstrap, concrete controller + data ports.
    let dir0 = TempDir::new().unwrap();
    let mut cfg0 = BrokerConfig::for_tests(dir0.path().to_path_buf());
    cfg0.broker_id = 1;
    cfg0.node_id = 1;
    cfg0.directory_id = uuid::Uuid::from_u128(1);
    cfg0.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg0.controller_listen_addr = controller_addrs[0];
    cfg0.auto_join = false;
    cfg0.bootstrap_servers = vec![];
    apply_sasl(&mut cfg0, client_addrs[0]);
    let broker0 = Broker::start(cfg0.clone()).await?;
    let bootstrap_ib_addr = broker0.listen_addr();

    // Broker 1: join via auto-join, dialing broker 0's SASL data listener
    // (where api_key 80 is served) — the inter-broker dialer runs SASL.
    let dir1 = TempDir::new().unwrap();
    let mut cfg1 = BrokerConfig::for_tests(dir1.path().to_path_buf());
    cfg1.broker_id = 2;
    cfg1.node_id = 2;
    cfg1.directory_id = uuid::Uuid::from_u128(2);
    cfg1.bootstrap_mode = BootstrapMode::Join;
    cfg1.controller_listen_addr = "127.0.0.1:0".parse().unwrap();
    cfg1.auto_join = true;
    cfg1.bootstrap_servers = vec![bootstrap_ib_addr];
    apply_sasl(&mut cfg1, client_addrs[1]);
    let broker1 = Broker::start(cfg1.clone()).await?;
    cfg1.controller_listen_addr = broker1.controller_addr();

    // Block until auto-join converges on a 2-voter quorum.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if broker0.voter_count_for_test() >= 2 {
            break;
        }
        if Instant::now() > deadline {
            return Err(BrokerError::Startup(format!(
                "auto-join did not reach 2 voters within 30s (have {})",
                broker0.voter_count_for_test()
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(vec![(broker0, cfg0, dir0), (broker1, cfg1, dir1)])
}

/// Retry cluster boot a few times — short raft timings occasionally split-vote
/// on busy runners. Mirrors `support::start_n_node_with_retry`.
async fn start_two_sasl_with_retry() -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last = None;
    for attempt in 1..=3 {
        match start_two_sasl().await {
            Ok(c) => return c,
            Err(e) => {
                tracing::warn!(attempt, error = %e, "SASL cluster boot failed; retrying");
                last = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("SASL cluster boot failed after 3 attempts: {last:?}");
}

/// Wait until every broker's metadata image lists both peers, so `CreateTopics`
/// round-robins partition leadership across the two brokers.
async fn wait_both_registered(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let mut all = true;
        for (h, _, _) in cluster {
            if h.broker_count().await < 2 {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "brokers didn't converge on a 2-broker view within 60s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Resolve `TOPIC`'s partition → leader-node map via Metadata, retrying until
/// both partitions report a real leader (`leader_id >= 0`).
async fn partition_leaders(client: &Client) -> Vec<(i32, i32)> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let resp = client
            .send(MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some(TOPIC.to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await
            .expect("metadata");
        if let Some(topic) = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(TOPIC))
        {
            let leaders: Vec<(i32, i32)> = topic
                .partitions
                .iter()
                .map(|p| (p.partition_index, p.leader_id))
                .collect();
            if leaders.len() == 2 && leaders.iter().all(|&(_, l)| l >= 0) {
                return leaders;
            }
        }
        assert!(
            Instant::now() < deadline,
            "topic leaders not assigned within 30s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn end_txn_marker_fanout_to_remote_leader_over_sasl() {
    let cluster = start_two_sasl_with_retry().await;
    wait_both_registered(&cluster).await;

    let bootstrap = cluster[0].1.listen_addr.to_string();
    let admin = sasl_client(&bootstrap).await;

    // Topic with 2 partitions, RF=1. Round-robin places P0 on node 1 and P1 on
    // node 2.
    let cr = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 2,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic: error_code={}",
        cr.topics[0].error_code
    );

    let leaders = partition_leaders(&admin).await;
    let distinct: std::collections::BTreeSet<i32> = leaders.iter().map(|&(_, l)| l).collect();
    assert_eq!(
        distinct.len(),
        2,
        "expected partition leadership split across both brokers, got {leaders:?}"
    );

    // Locate the transaction coordinator for TID.
    let fc = admin
        .send(FindCoordinatorRequest {
            key: TID.into(),
            key_type: 1, // TRANSACTION
            coordinator_keys: vec![TID.into()],
            ..Default::default()
        })
        .await
        .expect("find coordinator");
    let (coord_node, coord_host, coord_port) = fc.coordinators.first().map_or_else(
        || (fc.node_id, fc.host.clone(), fc.port),
        |c| (c.node_id, c.host.clone(), c.port),
    );
    assert!(coord_node >= 0, "no txn coordinator: {fc:?}");

    // Pick the partition led by the broker that is NOT the coordinator, so
    // EndTxn must fan a marker to a *remote* leader over the SASL listener.
    let remote_partition = leaders
        .iter()
        .find(|&&(_, leader)| leader != coord_node)
        .map(|&(p, _)| p)
        .expect("a partition led by a non-coordinator broker");

    // Connect to the coordinator and run the transaction control plane.
    let coord = sasl_client(&format!("{coord_host}:{coord_port}")).await;

    let init = coord
        .send(InitProducerIdRequest {
            transactional_id: Some(TID.into()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("init producer id");
    assert_eq!(init.error_code, 0, "InitProducerId failed: {init:?}");
    let (pid, epoch) = (init.producer_id, init.producer_epoch);

    // AddPartitionsToTxn for the remote-led partition. Fill both the v4+
    // `transactions` array and the v3-and-below flat fields so the request is
    // correct whatever version negotiates.
    let topic = AddPartitionsToTxnTopic {
        name: TOPIC.into(),
        partitions: vec![remote_partition],
        ..Default::default()
    };
    let add = coord
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: TID.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: false,
                topics: vec![topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: TID.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![topic],
            ..Default::default()
        })
        .await
        .expect("add partitions to txn");
    assert_eq!(
        add.error_code, 0,
        "AddPartitionsToTxn top-level error: {add:?}"
    );

    // EndTxn(commit): the coordinator fans a WriteTxnMarkers to the remote
    // partition's leader over the SASL_PLAINTEXT inter-broker listener. This is
    // the path the fix repairs — the pre-fix one-shot client could not
    // authenticate and EndTxn would surface a retriable UNKNOWN_SERVER_ERROR.
    let end = coord
        .send(EndTxnRequest {
            transactional_id: TID.into(),
            producer_id: pid,
            producer_epoch: epoch,
            committed: true,
            ..Default::default()
        })
        .await
        .expect("end txn");
    assert_eq!(
        end.error_code, 0,
        "EndTxn must succeed: remote marker fan-out over SASL inter-broker (error_code={})",
        end.error_code
    );

    admin.close();
    coord.close();
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

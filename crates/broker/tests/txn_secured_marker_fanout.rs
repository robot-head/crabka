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

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use crabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle,
    config::{InterBrokerCredentials, ListenerSpec},
};
use crabka_client_core::{
    Client,
    security::{ClientSecurity, SaslCredentials},
};
use crabka_protocol::owned::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
    common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    end_txn_request::EndTxnRequest,
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

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

    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(2).await;

    // KIP-595 Slice 3c static bootstrap: both brokers boot in `Bootstrap` mode
    // with the same static 2-voter set (concrete controller ports) and elect
    // among themselves over the SASL controller wire — no auto-join (KIP-853,
    // Slice 5).
    let voters: Vec<(u64, SocketAddr)> = vec![(1, controller_addrs[0]), (2, controller_addrs[1])];

    let dir0 = TempDir::new().unwrap();
    let mut cfg0 = BrokerConfig::for_tests(dir0.path().to_path_buf());
    cfg0.broker_id = 1;
    cfg0.node_id = crabka_broker::NodeId(1);
    cfg0.directory_id = uuid::Uuid::from_u128(1);
    cfg0.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg0.controller_listen_addr = controller_addrs[0];
    cfg0.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
        .collect();
    cfg0.auto_join = false;
    cfg0.bootstrap_servers = vec![];
    apply_sasl(&mut cfg0, client_addrs[0]);

    let dir1 = TempDir::new().unwrap();
    let mut cfg1 = BrokerConfig::for_tests(dir1.path().to_path_buf());
    cfg1.broker_id = 2;
    cfg1.node_id = crabka_broker::NodeId(2);
    cfg1.directory_id = uuid::Uuid::from_u128(2);
    cfg1.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg1.controller_listen_addr = controller_addrs[1];
    cfg1.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
        .collect();
    cfg1.auto_join = false;
    cfg1.bootstrap_servers = vec![];
    apply_sasl(&mut cfg1, client_addrs[1]);

    // Pull held listeners before the spawns so each spawn owns its pair.
    let mut data_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let (data0, controller0) = (data_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (data1, controller1) = (data_ls.next().unwrap(), ctrl_ls.next().unwrap());

    // Start both concurrently: `Broker::start` blocks until a leader is
    // committed, which needs a voter majority up, so a sequential
    // `start().await` on broker0 alone would deadlock.
    let cfg0_for_spawn = cfg0.clone();
    let cfg1_for_spawn = cfg1.clone();
    let join0 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg0_for_spawn, Some(controller0), Some(data0)).await
    });
    let join1 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg1_for_spawn, Some(controller1), Some(data1)).await
    });
    let broker0 = join0
        .await
        .map_err(|e| BrokerError::Startup(format!("broker0 task panicked: {e}")))??;
    let broker1 = join1
        .await
        .map_err(|e| BrokerError::Startup(format!("broker1 task panicked: {e}")))??;

    // Block until the static set converges on a 2-voter quorum.
    // `voter_count_for_test` reads the committed metadata image's voter set, so
    // `wait_for_image` observes the same convergence event-driven (image watch
    // channel) rather than polling on a fixed cadence. Both nodes are static
    // voters, so broker0's image reflects the full set once the quorum forms.
    broker0.wait_for_image(|img| img.voters().len() >= 2).await;

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
                // intentional: backoff before re-booting the whole cluster after
                // a failed boot attempt (lets stray raft timings settle) — not a
                // wait on any observable crabka broker/image/metric state.
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("SASL cluster boot failed after 3 attempts: {last:?}");
}

/// Wait until every broker's metadata image lists both peers, so `CreateTopics`
/// round-robins partition leadership across the two brokers.
async fn wait_both_registered(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    // Each broker's `broker_count` reads its committed metadata image;
    // `wait_until_brokers_registered` observes that same `img.brokers().count()`
    // via the image watch channel, so wait per-broker instead of polling.
    for (h, _, _) in cluster {
        h.wait_until_brokers_registered(2).await;
    }
}

/// Resolve `TOPIC`'s partition → leader-node map via Metadata, once both
/// partitions have an elected leader in `handle`'s metadata image — the same
/// image the admin client (connected to that broker) is served Metadata from.
async fn partition_leaders(client: &Client, handle: &BrokerHandle) -> Vec<(i32, i32)> {
    // A non-zero `leader` in the image is exactly the wire condition the old
    // loop polled for (`leader_id >= 0`); await both partitions' elections
    // event-driven via the image watch channel, then take one Metadata snapshot.
    handle
        .wait_for_image(|img| {
            (0..2).all(|p| img.partition(TOPIC, p).is_some_and(|pr| pr.leader != 0))
        })
        .await;
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
    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(TOPIC))
        .expect("topic present in metadata after leader election");
    topic
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.leader_id))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    let leaders = partition_leaders(&admin, &cluster[0].0).await;
    let distinct: std::collections::BTreeSet<i32> = leaders.iter().map(|&(_, l)| l).collect();
    assert!(
        distinct.len() == 2,
        "expected partition leadership split across both brokers, got {leaders:?}"
    );

    // Locate the transaction coordinator for TID.
    // The transaction coordinator partition (`__transaction_state[hash(TID)]`)
    // is auto-created and its leader elected lazily on first access, so the
    // initial FindCoordinator can race ahead of that election and briefly
    // return COORDINATOR_NOT_AVAILABLE ("partition not found"). Poll until it
    // resolves — matching the retry-with-deadline idiom used elsewhere in this
    // test — so a genuine never-available coordinator still surfaces (after the
    // deadline) rather than flaking on the timing window.
    let fc_deadline = Instant::now() + Duration::from_secs(30);
    let (coord_node, coord_host, coord_port) = loop {
        let fc = admin
            .send(FindCoordinatorRequest {
                key: TID.into(),
                key_type: 1, // TRANSACTION
                coordinator_keys: vec![TID.into()],
                ..Default::default()
            })
            .await
            .expect("find coordinator");
        let (node, host, port) = fc.coordinators.first().map_or_else(
            || (fc.node_id, fc.host.clone(), fc.port),
            |c| (c.node_id, c.host.clone(), c.port),
        );
        if node >= 0 {
            break (node, host, port);
        }
        assert!(
            Instant::now() <= fc_deadline,
            "txn coordinator never became available: {fc:?}"
        );
        tokio::task::yield_now().await;
    };

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
    assert!(init.error_code == 0, "InitProducerId failed: {init:?}");
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
    assert!(
        add.error_code == 0,
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
    assert!(
        end.error_code == 0,
        "EndTxn must succeed: remote marker fan-out over SASL inter-broker (error_code={})",
        end.error_code
    );

    admin.close();
    coord.close();
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

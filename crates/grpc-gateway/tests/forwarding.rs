//! Active-active forwarding: two gateway replicas split the dedup partitions and
//! each tails membership. A keyed record whose partition is owned by B, when
//! submitted to A, is forwarded to B over HTTP and produced exactly once;
//! re-submitting the same key dedups. A record with no known owner is Unavailable.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::{
    codec::RawCodec,
    config::GatewayConfig,
    dedup::{
        DedupEngine,
        membership::{MembershipPublisher, MembershipStore},
        partition_for,
        store::DedupStore,
        topic::{ensure_dedup_topic, ensure_membership_topic},
    },
    error::GatewayError,
    forward::{self, Forwarder},
    produce::ProduceCore,
    state::AppState,
    types::GatewayRecord,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const N: u32 = 4;
const DEDUP: &str = "__crabka_grpc_dedup";
const MEMBERSHIP: &str = "__crabka_grpc_gateway_membership";
const OWNERS_GROUP: &str = "__crabka_grpc_gateway_dedup_owners";
const USER_TOPIC: &str = "fwd-user";

struct Gw {
    addr: String,
    state: Arc<AppState>,
    store: Arc<DedupStore>,
    membership: Arc<MembershipStore>,
    token: CancellationToken,
}

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Bind a listener first (to learn the advertised addr), install the membership
/// publisher, start ownership + membership, then serve Connect + forward routes.
#[allow(clippy::too_many_lines)]
async fn spawn_gateway(bootstrap: &str, client: &str) -> Gw {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let token = CancellationToken::new();

    let store = Arc::new(DedupStore::new(N));
    let node_id = format!("{client}-{addr}");
    let publisher = Arc::new(
        MembershipPublisher::new(
            bootstrap,
            &format!("{client}-pub"),
            node_id.clone(),
            addr.clone(),
            MEMBERSHIP.into(),
            None,
        )
        .await
        .unwrap(),
    );
    store.set_membership(publisher);

    // Ownership consumer (shared owners group).
    {
        let store = store.clone();
        let bootstrap = bootstrap.to_string();
        let token = token.clone();
        tokio::spawn(store.run_ownership(
            bootstrap,
            format!("{client}-owner"),
            DEDUP.into(),
            OWNERS_GROUP.into(),
            token,
            None,
        ));
    }

    // Membership reader (unique group per replica).
    let membership = Arc::new(MembershipStore::new());
    {
        let membership = membership.clone();
        let bootstrap = bootstrap.to_string();
        let token = token.clone();
        tokio::spawn(membership.clone().run_membership(
            bootstrap,
            format!("{client}-memb"),
            MEMBERSHIP.into(),
            format!("__crabka_grpc_gateway_membership_reader-{node_id}"),
            token,
            None,
        ));
    }

    let engine = Arc::new(DedupEngine::new(
        bootstrap,
        client,
        &format!("crabka-grpc-dedup-{client}"),
        DEDUP.into(),
        N,
        store.clone(),
        None,
    ));
    let forwarder = Arc::new(Forwarder::new());
    let produce = ProduceCore::new(bootstrap, client, Arc::new(RawCodec), None)
        .await
        .unwrap()
        .with_dedup(engine)
        .with_forwarding(membership.clone(), forwarder, addr.clone());
    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: client.into(),
            dedup_topic: DEDUP.into(),
            dedup_partitions: N,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: format!("crabka-grpc-dedup-{client}"),
            advertised_addr: addr.clone(),
            membership_topic: MEMBERSHIP.into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
        }),
        authz: Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
    });

    // Serve Connect + forward routes (health omitted — not needed here).
    {
        let app = crabka_grpc_gateway::router(state.clone())
            .merge(forward::forward_router(state.clone()));
        let token = token.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await;
        });
    }

    Gw {
        addr,
        state,
        store,
        membership,
        token,
    }
}

async fn count_in_user_topic(bootstrap: &str, key_filter: &str) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("fwd-verify")
        .group_id("fwd-verify-grp")
        .subscribe(vec![USER_TOPIC.to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut n = 0;
    for _ in 0..10 {
        let batch = consumer.poll(Duration::from_millis(500)).await.unwrap();
        for r in batch {
            if r.value.as_deref() == Some(key_filter.as_bytes()) {
                n += 1;
            }
        }
    }
    let _ = consumer.close().await;
    n
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn keyed_record_forwards_to_owner_and_dedups() {
    let (broker, bootstrap, _dir) = boot().await;
    ensure_dedup_topic(&bootstrap, DEDUP, N, 3_600_000, 1, None)
        .await
        .unwrap();
    ensure_membership_topic(&bootstrap, MEMBERSHIP, 1, None)
        .await
        .unwrap();
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: USER_TOPIC.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let gw_a = spawn_gateway(&bootstrap, "gwa").await;
    let gw_b = spawn_gateway(&bootstrap, "gwb").await;

    // Wait for a disjoint, covering split where both replicas are warm AND both
    // membership tables route every partition (forwarding can resolve any key).
    let mut ready = false;
    for _ in 0..160 {
        let split_ok = (0..N).all(|p| gw_a.store.owns(p) ^ gw_b.store.owns(p))
            && gw_a.store.has_warmed_once()
            && gw_b.store.has_warmed_once();
        let routes_ok = (0..N).all(|p| gw_a.membership.owner_of(p).is_some())
            && (0..N).all(|p| gw_b.membership.owner_of(p).is_some());
        if split_ok && routes_ok {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert2::assert!(ready);

    // Pick a key owned by B (so submitting through A must forward to B).
    let key = (0..1000)
        .map(|i| format!("k{i}"))
        .find(|k| gw_b.store.owns(partition_for(k, N)))
        .expect("a key owned by B");
    let p = partition_for(&key, N);
    assert2::assert!(gw_b.store.owns(p) && !gw_a.store.owns(p));
    assert2::assert!(gw_a.membership.owner_of(p).as_deref() == Some(gw_b.addr.as_str()));

    let mk = || GatewayRecord {
        topic: USER_TOPIC.into(),
        key: None,
        value: Bytes::from(key.clone().into_bytes()),
        body_structured: None,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some(key.clone()),
    };

    // The resolved caller relayed on the forward; with AllowAll the owner's
    // re-authz always allows it, so forwarding behavior is unchanged.
    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };

    // Submit through A → forwarded to B → produced (not deduplicated).
    let first = gw_a.state.produce.produce(mk(), &anon).await.unwrap();

    // Same key through A again → forwarded to B → B's map hit → deduplicated.
    let second = gw_a.state.produce.produce(mk(), &anon).await.unwrap();
    assert2::assert!(!first.deduplicated);
    assert2::assert!(second.deduplicated);
    assert2::assert!(first.offset == first.offset);
    assert2::assert!(second.offset == first.offset);

    // Exactly one record with that value landed in the user topic.
    assert2::assert!(count_in_user_topic(&bootstrap, &key).await == 1);

    gw_a.token.cancel();
    gw_b.token.cancel();
    broker.shutdown().await;
}

#[tokio::test]
async fn no_known_owner_is_unavailable() {
    // A produce core with dedup but an EMPTY membership table: a keyed record
    // for an unowned partition has no route ⇒ Unavailable (origin retries).
    // ProduceCore::new connects to the broker during build, so we need a real
    // broker even though this test never actually produces a record.
    let (_broker, bootstrap, _dir) = boot().await;
    let store = Arc::new(DedupStore::new(N));
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "gw",
        "crabka-grpc-dedup",
        DEDUP.into(),
        N,
        store,
        None,
    ));
    let membership = Arc::new(MembershipStore::new());
    let forwarder = Arc::new(Forwarder::new());
    let produce = ProduceCore::new(&bootstrap, "gw", Arc::new(RawCodec), None)
        .await
        .unwrap()
        .with_dedup(engine)
        .with_forwarding(membership, forwarder, "127.0.0.1:9999".into());
    let rec = GatewayRecord {
        topic: USER_TOPIC.into(),
        key: None,
        value: Bytes::from_static(b"v"),
        body_structured: None,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("k".into()),
    };
    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    let err = produce.produce(rec, &anon).await.unwrap_err();
    assert2::assert!(matches!(err, GatewayError::Unavailable));
}

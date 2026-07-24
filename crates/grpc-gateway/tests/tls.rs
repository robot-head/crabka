//! TLS / mTLS: the gateway serves the listener over rustls; mTLS `Required`
//! rejects un-certed clients; the mTLS peer principal is extracted; and two
//! TLS gateways forward over mutually-authenticated https. Certs are generated
//! at runtime via `crabka_security::ca` (no fixtures). Pure 127.0.0.1 — no Docker.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::{
    codec::RawCodec,
    config::{ClientAuthMode, GatewayConfig, TlsSettings},
    dedup::{
        DedupEngine,
        membership::{MembershipPublisher, MembershipStore},
        partition_for,
        store::DedupStore,
        topic::{ensure_dedup_topic, ensure_membership_topic},
    },
    forward::{self, Forwarder},
    health::{self, Readiness},
    produce::ProduceCore,
    serve,
    state::AppState,
    types::GatewayRecord,
};
use crabka_security::ca::{
    SubjectAltName, generate_clients_ca, issue_broker_cert, issue_user_cert,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const N: u32 = 4;
const DEDUP: &str = "__crabka_grpc_dedup";
const MEMBERSHIP: &str = "__crabka_grpc_gateway_membership";
const OWNERS_GROUP: &str = "__crabka_grpc_gateway_dedup_owners";
const USER_TOPIC: &str = "tls-fwd-user";

fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn write(dir: &std::path::Path, name: &str, pem: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, pem).unwrap();
    p
}

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Generate CA + a gateway server/client cert (SAN 127.0.0.1) + a standalone
/// client cert, all chaining to one CA. Returns paths in `dir`.
struct Certs {
    ca: PathBuf,
    gw_cert: PathBuf,
    gw_key: PathBuf,
    #[allow(dead_code)]
    client_cert: PathBuf,
    #[allow(dead_code)]
    client_key: PathBuf,
}

fn gen_certs(dir: &std::path::Path) -> Certs {
    let ca = generate_clients_ca("p4-ca", 365).unwrap();
    let sans = vec![
        SubjectAltName::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        SubjectAltName::Dns("localhost".into()),
    ];
    let gw = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "gateway", &sans, &[], 365).unwrap();
    let client = issue_user_cert(&ca.cert_pem, &ca.key_pem, "peer-gateway", 365).unwrap();
    Certs {
        ca: write(dir, "ca.pem", &ca.cert_pem),
        gw_cert: write(dir, "gw_cert.pem", &gw.cert_pem),
        gw_key: write(dir, "gw_key.pem", &gw.key_pem),
        client_cert: write(dir, "client_cert.pem", &client.cert_pem),
        client_key: write(dir, "client_key.pem", &client.key_pem),
    }
}

/// Build the `TlsSettings` for a gateway: the shared gateway cert/key + the CA
/// as both trust-roots (peer server cert verification) and client-CA (incoming
/// client cert verification). `client_auth` selects Disabled/Optional/Required.
fn tls_settings(certs: &Certs, client_auth: ClientAuthMode) -> TlsSettings {
    TlsSettings {
        cert_chain_path: certs.gw_cert.clone(),
        private_key_path: certs.gw_key.clone(),
        trust_roots_path: Some(certs.ca.clone()),
        client_ca_path: Some(certs.ca.clone()),
        client_auth,
        reload_interval_secs: 3600,
    }
}

struct Gw {
    addr: String,
    state: Arc<AppState>,
    store: Arc<DedupStore>,
    membership: Arc<MembershipStore>,
    token: CancellationToken,
}

/// Bind a listener first (to learn the advertised addr), serve it over TLS via
/// `serve::build_and_watch_tls` + `serve::serve`, build the forwarder over mTLS
/// (`Forwarder::with_tls`), and start ownership + membership. Models
/// `tests/forwarding.rs::spawn_gateway` but with the TLS transport.
async fn spawn_gateway_tls(bootstrap: &str, client: &str, settings: TlsSettings) -> Gw {
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

    // mTLS forwarder: presents the gateway's own client identity + trusts the CA.
    let forwarder = Arc::new(
        Forwarder::with_tls(
            settings
                .to_security()
                .build_client_config_with_identity()
                .unwrap(),
        )
        .unwrap(),
    );

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
            dedup_ownership_group: OWNERS_GROUP.into(),
            dedup_txn_id_prefix: format!("crabka-grpc-dedup-{client}"),
            advertised_addr: addr.clone(),
            membership_topic: MEMBERSHIP.into(),
            // TLS configured ⇒ /internal/v1/forward enforces the mTLS principal gate.
            tls: Some(settings.clone()),
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
            runtime: crabka_grpc_gateway::config::GatewayRuntimeConfig::default(),
        }),
        authz: Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
    });

    // Serve Connect + health + forward routes over TLS.
    {
        let app = crabka_grpc_gateway::router(state.clone())
            .merge(health::router(Readiness::new()))
            .merge(forward::forward_router(state.clone()));
        let dynamic = serve::build_and_watch_tls(
            settings.to_security(),
            settings.reload_interval_secs,
            token.clone(),
        )
        .unwrap();
        let token = token.clone();
        tokio::spawn(async move {
            let _ = serve::serve(listener, app, Some(dynamic), token).await;
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

/// reqwest https client that trusts the test CA. When `identity` is `Some`,
/// presents that client cert (mTLS); otherwise anonymous. The gateway's server
/// cert chains to this CA, so server-cert verification passes regardless of
/// whatever built-in roots the resolved reqwest TLS backend also bundles.
fn https_client(ca_pem: &str, identity: Option<(&str, &str)>) -> reqwest::Client {
    let ca = reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap();
    let mut builder = reqwest::Client::builder().add_root_certificate(ca);
    if let Some((cert_pem, key_pem)) = identity {
        let mut pem = String::new();
        pem.push_str(cert_pem);
        if !cert_pem.ends_with('\n') {
            pem.push('\n');
        }
        pem.push_str(key_pem);
        let id = reqwest::Identity::from_pem(pem.as_bytes()).unwrap();
        builder = builder.identity(id);
    }
    builder.build().unwrap()
}

async fn count_in_user_topic(bootstrap: &str, key_filter: &str) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("tls-fwd-verify")
        .group_id("tls-fwd-verify-grp")
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

/// (1) Server-side TLS: a reqwest client trusting only the test CA completes the
/// rustls handshake (gateway cert SAN = 127.0.0.1) and gets `/healthz` ⇒ 200.
/// `client_auth = Optional` ⇒ a client with no identity is still accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_tls_handshake_and_health() {
    install_provider();
    let (broker, bootstrap, _dir) = boot().await;
    ensure_dedup_topic(
        &bootstrap,
        DEDUP,
        N,
        3_600_000,
        &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
            replication_factor: 1,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    ensure_membership_topic(
        &bootstrap,
        MEMBERSHIP,
        &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
            replication_factor: 1,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let certs_dir = TempDir::new().unwrap();
    let certs = gen_certs(certs_dir.path());
    let ca_pem = std::fs::read_to_string(&certs.ca).unwrap();

    let gw = spawn_gateway_tls(
        &bootstrap,
        "gw-health",
        tls_settings(&certs, ClientAuthMode::Optional),
    )
    .await;

    let client = https_client(&ca_pem, None);
    let url = format!("https://{}/healthz", gw.addr);

    // The TLS accept loop spawns just after bind; retry briefly for readiness.
    let mut status = None;
    for _ in 0..50 {
        match client.get(&url).send().await {
            Ok(resp) => {
                status = Some(resp.status());
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    assert2::assert!(status == Some(reqwest::StatusCode::OK));

    gw.token.cancel();
    broker.shutdown().await;
}

/// (2) mTLS `Required` rejects a client with no certificate: the rustls
/// handshake fails, so reqwest's `.send()` returns an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mtls_required_rejects_no_client_cert() {
    install_provider();
    let (broker, bootstrap, _dir) = boot().await;
    ensure_dedup_topic(
        &bootstrap,
        DEDUP,
        N,
        3_600_000,
        &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
            replication_factor: 1,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    ensure_membership_topic(
        &bootstrap,
        MEMBERSHIP,
        &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
            replication_factor: 1,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let certs_dir = TempDir::new().unwrap();
    let certs = gen_certs(certs_dir.path());
    let ca_pem = std::fs::read_to_string(&certs.ca).unwrap();

    let gw = spawn_gateway_tls(
        &bootstrap,
        "gw-mtls",
        tls_settings(&certs, ClientAuthMode::Required),
    )
    .await;

    // Trusts the CA's server cert but presents NO client identity ⇒ the mTLS
    // `Required` server rejects the handshake.
    let client = https_client(&ca_pem, None);
    let url = format!("https://{}/healthz", gw.addr);

    // Give the accept loop a moment; an early connection-refused would also be
    // `is_err()`, but we want to prove the *handshake* (not startup) rejects, so
    // wait until the listener is up, then assert the certless request fails.
    // real-time wait (not a progress poll): waits on a real mTLS handshake to reject a certless client; settle-then-assert-failure over a network round-trip.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let result = client.get(&url).send().await;
    assert2::assert!(result.is_err());

    gw.token.cancel();
    broker.shutdown().await;
}

/// (3) End-to-end mTLS forwarding. Two gateways A and B, each over TLS with
/// `client_auth = Required` and a `Forwarder::with_tls` identity. A key owned by
/// B, submitted through A, is forwarded to B over mutually-authenticated https,
/// produced once, then deduplicated on resend — proving A's forward identity,
/// B's server mTLS verification, and B's `/internal/forward` principal gate all
/// line up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_forward_between_two_gateways() {
    install_provider();
    let (broker, bootstrap, _dir) = boot().await;
    ensure_dedup_topic(
        &bootstrap,
        DEDUP,
        N,
        3_600_000,
        &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
            replication_factor: 1,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    ensure_membership_topic(
        &bootstrap,
        MEMBERSHIP,
        &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
            replication_factor: 1,
            ..Default::default()
        },
        None,
    )
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

    let certs_dir = TempDir::new().unwrap();
    let certs = gen_certs(certs_dir.path());

    let gw_a = spawn_gateway_tls(
        &bootstrap,
        "tls-gwa",
        tls_settings(&certs, ClientAuthMode::Required),
    )
    .await;
    let gw_b = spawn_gateway_tls(
        &bootstrap,
        "tls-gwb",
        tls_settings(&certs, ClientAuthMode::Required),
    )
    .await;

    // Wait for a disjoint, covering split where both replicas are warm AND both
    // membership tables route every partition (forwarding can resolve any key).
    let mut ready = false;
    for _ in 0..240 {
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

    // The resolved caller relayed on the mTLS forward; with AllowAll the
    // owner's re-authz always allows it, so forwarding behavior is unchanged.
    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };

    // Submit through A → forwarded to B over mTLS https → produced (not dedup'd).
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

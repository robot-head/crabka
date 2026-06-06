#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]
//! In-process integration of the registry security stack against a real broker.
//!
//! Boots a Crabka `Broker`, seeds Kafka ACLs (`User:alice` Allow Write/Read on
//! `Topic:s`), then starts secure registry node(s) wired with the full middleware
//! stack — `auth_layer` (require Basic) → `authz_layer` (enabled, refreshed from
//! the broker's `DescribeAcls`) → `forward_layer`. It asserts the end-to-end
//! HTTP contract:
//!
//! - `401` with no / bad credentials (and `WWW-Authenticate: Basic`),
//! - `403` for an authenticated principal lacking an ACL,
//! - `200` for an authorized `register`,
//! - reads succeed where the principal holds `Read`,
//! - a write to a SECONDARY is authorized at ingress and forwarded to the
//!   primary (forward-authz across two nodes),
//! - an HTTPS round-trip (`serve_https`) with auth enforced over TLS.
//!
//! The ACL cache populates a few hundred ms after start (the `run_acl_refresh`
//! timer), so the authorized assertions poll to a deadline rather than asserting
//! once.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{
    AclEntry, AclOperation, AdminClient, PatternType, PermissionType, ResourceType,
};
use crabka_schema_registry::auth::AuthState;
use crabka_schema_registry::auth::basic::BasicAuthStore;
use crabka_schema_registry::authz::SchemaRegistryAuthz;
use crabka_schema_registry::config::{
    AuthzConfig, BasicAuthConfig, RegistryConfig, SecurityConfig,
};
use crabka_schema_registry::election::{Election, PrimaryState};
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::{self, AppState, SecurityLayers, forward::ForwardState};
use crabka_security::TlsConfig;
use crabka_security::ca::{SubjectAltName, generate_clients_ca, issue_broker_cert};
use tokio_util::sync::CancellationToken;

const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";
/// A minimal valid Avro record schema (empty fields), as a registration body.
const SCHEMA_BODY: &str = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[]}"}"#;

/// The lone user known to every node's Basic store.
fn alice_users() -> HashMap<String, String> {
    [("alice".to_string(), "pw".to_string())]
        .into_iter()
        .collect()
}

/// A secure-node config: require Basic auth + enabled authz with a fast ACL
/// refresh, plain HTTP (TLS is layered separately via [`tls_config`]).
fn secure_cfg(bootstrap: &str, port: i32, tls: Option<TlsConfig>) -> RegistryConfig {
    RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: format!("sr-sec-{port}"),
        advertised_url: format!("http://127.0.0.1:{port}"),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        security: SecurityConfig {
            require_auth: true,
            realm: "test".into(),
            basic: Some(BasicAuthConfig {
                users: alice_users(),
                file: None,
            }),
            bearer: None,
            tls,
            authz: Some(AuthzConfig {
                enabled: true,
                super_users: HashSet::new(),
                acl_refresh: Duration::from_millis(300),
            }),
            client: None,
        },
    }
}

/// An ACL entry for `User:alice` `Allow` `op` on `Topic:s` (literal, any host).
fn alice_acl(op: AclOperation) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "s".into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: op,
        permission_type: PermissionType::Allow,
    }
}

/// Seed `User:alice` Allow Write AND Read on `Topic:s`. `register` maps to
/// `Write`, `GET /subjects/s/versions` to `Read` (see `authz::authz_target`);
/// `SimpleAclAuthorizer` does not imply Read←Write, so both are required.
async fn seed_acls(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    let outcomes = admin
        .create_acls(&[
            alice_acl(AclOperation::Write),
            alice_acl(AclOperation::Read),
        ])
        .await
        .expect("create_acls");
    for o in outcomes {
        assert!(o.error.is_none(), "ACL create error: {:?}", o.error);
    }
}

struct Node {
    port: i32,
    // Kept alive so the node's background reader/store outlives `start_secure_node`.
    _store: Arc<KafkaStore>,
    primary: tokio::sync::watch::Receiver<PrimaryState>,
    cancel: CancellationToken,
}

/// Boot a secure registry node: `KafkaStore` + `Election` + the full security
/// middleware stack (`auth_layer` → `authz_layer` → `forward_layer`) served over
/// `serve_http`. Binds the listener FIRST so `advertised_url` carries the real
/// port. Models `ha.rs::start_node`, adding the auth + authz layers.
async fn start_secure_node(bootstrap: &str) -> Node {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let cfg = secure_cfg(bootstrap, port, None);
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    let primary = Election::start(&cfg, cancel.clone()).await.unwrap();

    let auth = AuthState {
        basic: Some(Arc::new(BasicAuthStore::from_users(alice_users()))),
        bearer: None,
        require_auth: true,
        realm: "test".into(),
    };
    let authz = Arc::new(SchemaRegistryAuthz::new(HashSet::new(), true));
    {
        let admin = AdminClient::connect(&[bootstrap.to_string()])
            .await
            .expect("authz admin connect");
        let authz = authz.clone();
        let refresh_cancel = cancel.clone();
        tokio::spawn(async move {
            authz
                .run_acl_refresh(admin, Duration::from_millis(300), refresh_cancel)
                .await;
        });
    }

    let fwd = ForwardState {
        primary: primary.clone(),
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
    };
    let app: Router = rest::router_with_security(
        AppState {
            store: store.clone(),
        },
        SecurityLayers {
            auth,
            authz: Some(authz),
            forward: fwd,
        },
    );
    let serve_cancel = cancel.clone();
    tokio::spawn(async move {
        rest::serve::serve_http(listener, app, serve_cancel)
            .await
            .ok();
    });
    Node {
        port,
        _store: store,
        primary,
        cancel,
    }
}

/// Wait until `pred(state)` holds or `secs` elapses; returns the matching state.
async fn await_state(
    rx: &mut tokio::sync::watch::Receiver<PrimaryState>,
    secs: u64,
    pred: impl Fn(&PrimaryState) -> bool,
) -> PrimaryState {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if pred(&rx.borrow()) {
            return rx.borrow().clone();
        }
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => panic!("state never matched: {:?}", *rx.borrow()),
            r = rx.changed() => { r.expect("election task alive"); }
        }
    }
}

/// POST a `register` for `subject` to `port` as `alice:pw`; returns the status.
async fn register_as_alice(
    http: &reqwest::Client,
    port: i32,
    subject: &str,
) -> reqwest::StatusCode {
    http.post(format!(
        "http://127.0.0.1:{port}/subjects/{subject}/versions"
    ))
    .header("content-type", SR_CONTENT_TYPE)
    .basic_auth("alice", Some("pw"))
    .body(SCHEMA_BODY)
    .send()
    .await
    .unwrap()
    .status()
}

/// Poll `register` on `subject` as `alice:pw` until it returns `200` (the ACL
/// cache populates ~300ms after start) or `secs` elapses. Registering the same
/// schema repeatedly is idempotent (returns the same id), so re-POSTing is safe.
async fn await_register_200(http: &reqwest::Client, port: i32, subject: &str, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let st = register_as_alice(http, port, subject).await;
        if st == 200 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "register on {subject} never returned 200 (last status {st})"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// Poll `GET url` (as `alice:pw`) until the body equals `expected` or `secs`
/// elapses. A non-writing node reflects a forwarded write only once it consumes
/// the `_schemas` record (eventually consistent).
async fn await_get_body_as_alice(http: &reqwest::Client, url: &str, expected: &str, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Ok(r) = http.get(url).basic_auth("alice", Some("pw")).send().await
            && r.status() == 200
            && let Ok(b) = r.text().await
            && b == expected
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "GET {url} never returned {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_enforces_authn_and_authz() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    seed_acls(&bootstrap).await;

    let mut node = start_secure_node(&bootstrap).await;
    await_state(&mut node.primary, 25, |s| s.is_primary).await;
    let port = node.port;
    let http = reqwest::Client::new();
    let register_url = format!("http://127.0.0.1:{port}/subjects/s/versions");

    // ── 401: no credentials → Unauthorized + a Basic challenge. ──────────────
    let r = http
        .post(&register_url)
        .header("content-type", SR_CONTENT_TYPE)
        .body(SCHEMA_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "no credentials → 401");
    let www = r
        .headers()
        .get(reqwest::header::WWW_AUTHENTICATE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        www.starts_with("Basic"),
        "WWW-Authenticate should advertise Basic, got {www:?}"
    );

    // ── 401: wrong password, and an unknown user. ────────────────────────────
    let r = http
        .post(&register_url)
        .header("content-type", SR_CONTENT_TYPE)
        .basic_auth("alice", Some("wrong"))
        .body(SCHEMA_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "alice:wrong → 401");
    let r = http
        .post(&register_url)
        .header("content-type", SR_CONTENT_TYPE)
        .basic_auth("bob", Some("pw"))
        .body(SCHEMA_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "bob:pw (unknown user) → 401");

    // ── 200: alice has Write on `s` → authorized register (poll: ACL cache). ─
    await_register_200(&http, port, "s", 15).await;

    // ── 403: alice authenticates but has no ACL for `other`. ─────────────────
    let st = register_as_alice(&http, port, "other").await;
    assert_eq!(st, 403, "alice has no ACL on `other` → 403");

    // ── 200 read: alice has Read on `s`. ─────────────────────────────────────
    await_get_body_as_alice(&http, &register_url, "[1]", 15).await;

    node.cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_round_trip_enforces_auth_over_tls() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    seed_acls(&bootstrap).await;

    // A self-signed CA → a server leaf cert with a 127.0.0.1 IP SAN so reqwest
    // verifies both the chain (CA added as a root) and the hostname.
    let certdir = tempfile::tempdir().unwrap();
    let ca = generate_clients_ca("sr-sec-ca", 365).unwrap();
    let sans = vec![SubjectAltName::Ip(std::net::IpAddr::V4(
        std::net::Ipv4Addr::LOCALHOST,
    ))];
    let leaf = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "sr", &sans, &[], 365).unwrap();
    let cert_path = certdir.path().join("cert.pem");
    let key_path = certdir.path().join("key.pem");
    std::fs::write(&cert_path, &leaf.cert_pem).unwrap();
    std::fs::write(&key_path, &leaf.key_pem).unwrap();
    let tls = TlsConfig {
        cert_chain_path: cert_path,
        private_key_path: key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: crabka_security::ClientAuthMode::Disabled,
    };

    // Boot a node serving HTTPS. Listener bound first for the real port.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let cfg = secure_cfg(&bootstrap, port, Some(tls.clone()));
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    let mut primary = Election::start(&cfg, cancel.clone()).await.unwrap();
    let auth = AuthState {
        basic: Some(Arc::new(BasicAuthStore::from_users(alice_users()))),
        bearer: None,
        require_auth: true,
        realm: "test".into(),
    };
    // authz disabled (None) here: this test exercises auth-over-TLS, not ACLs.
    let fwd = ForwardState {
        primary: primary.clone(),
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
    };
    let app: Router = rest::router_with_security(
        AppState {
            store: store.clone(),
        },
        SecurityLayers {
            auth,
            authz: None,
            forward: fwd,
        },
    );
    let serve_cancel = cancel.clone();
    let tls_for_serve = tls.clone();
    tokio::spawn(async move {
        rest::serve::serve_https(listener, app, &tls_for_serve, serve_cancel)
            .await
            .ok();
    });
    await_state(&mut primary, 25, |s| s.is_primary).await;

    let ca_cert = reqwest::Certificate::from_pem(ca.cert_pem.as_bytes()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        .build()
        .unwrap();
    let base = format!("https://127.0.0.1:{port}/");

    // No credentials over TLS → 401, confirming auth runs on the HTTPS path.
    let r = client.get(&base).send().await.unwrap();
    assert_eq!(r.status(), 401, "anonymous GET over TLS → 401");

    // alice:pw over TLS → 200 (the registry root, no authz requirement).
    let r = client
        .get(&base)
        .basic_auth("alice", Some("pw"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "alice:pw GET / over TLS → 200");

    cancel.cancel();
    broker.shutdown().await;
}

// TODO(slice-6): mTLS multi-node forward integration test. This Basic two-node
// case plus the `auth::tests::forwarded_request_bypasses_require_auth` unit test
// already cover model A end-to-end (ingress authn+authz → proxy with no creds,
// only FORWARD_HEADER → primary auth_layer + authz_layer trust the forward). An
// mTLS variant (two `serve_https` nodes with `ClientAuthMode::Required`, an mTLS
// alice client writing to the secondary, the write landing on the primary) would
// additionally prove model A works where model B could not — but it needs a
// bespoke HTTPS harness (the secondary's forward `reqwest::Client` must carry its
// own client identity + CA trust to complete the mTLS handshake to the primary)
// plus a full-subject-DN ACL (`User:CN=alice,…`), so it is deferred as too heavy.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn two_nodes_authorize_then_forward_to_primary() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    seed_acls(&bootstrap).await;

    let mut a = start_secure_node(&bootstrap).await;
    let mut b = start_secure_node(&bootstrap).await;
    await_state(&mut a.primary, 25, |s| s.primary_url.is_some()).await;
    await_state(&mut b.primary, 25, |s| s.primary_url.is_some()).await;
    let a_is_primary = a.primary.borrow().is_primary;
    assert_ne!(
        a_is_primary,
        b.primary.borrow().is_primary,
        "exactly one primary"
    );
    let secondary_port = if a_is_primary { b.port } else { a.port };

    let http = reqwest::Client::new();

    // A write to the SECONDARY for `s`: authorized at ingress (alice has Write),
    // forwarded to the primary, lands. Poll because both the ingress ACL cache
    // and the forward target must be ready.
    await_register_200(&http, secondary_port, "s", 20).await;

    // The write is readable on BOTH nodes (poll the non-writer for consume lag).
    for port in [a.port, b.port] {
        await_get_body_as_alice(
            &http,
            &format!("http://127.0.0.1:{port}/subjects/s/versions"),
            "[1]",
            20,
        )
        .await;
    }

    // A write to the SECONDARY for `other` (no ACL): denied at ingress with 403,
    // never forwarded.
    let st = register_as_alice(&http, secondary_port, "other").await;
    assert_eq!(
        st, 403,
        "secondary denies an unauthorized write before forwarding"
    );

    a.cancel.cancel();
    b.cancel.cancel();
    broker.shutdown().await;
}

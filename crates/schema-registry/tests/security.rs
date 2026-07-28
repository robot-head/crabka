//! In-process integration of the registry security stack against a real broker.
//!
//! Boots a Crabka `Broker`, seeds Kafka ACLs (`User:alice` Allow Write/Read on
//! `Topic:s`), then starts secure registry node(s) wired with the full middleware
//! stack — `auth_layer` (require Basic) → `authz_layer` (enabled, refreshed from
//! the broker's `DescribeAcls`) → `forward_layer`. It asserts the end-to-end
//! HTTP contract:
//!
//! - `401` with no / bad credentials (and `WWW-Authenticate: basic`),
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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use axum::Router;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{
    AclEntry, AclOperation, AdminClient, PatternType, PermissionType, ResourceType,
};
use crabka_schema_registry::{
    auth::{AuthState, basic::BasicAuthStore},
    authz::SchemaRegistryAuthz,
    cli::{SecurityCliInput, build_security},
    config::{AuthzConfig, BasicAuthConfig, RegistryConfig, SecurityConfig},
    election::{Election, PrimaryState},
    kafkastore::KafkaStore,
    rest::{self, AppState, SecurityLayers, forward::ForwardState},
};
use crabka_security::{
    ClientAuthMode, Jwks, TlsConfig,
    ca::{SubjectAltName, generate_clients_ca, issue_broker_cert, issue_user_cert},
};
use crabka_units::prelude::*;
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
fn secure_cfg_with_scheme(
    bootstrap: &str,
    port: i32,
    scheme: &str,
    tls: Option<TlsConfig>,
) -> RegistryConfig {
    RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: format!("sr-sec-{port}"),
        advertised_url: format!("{scheme}://127.0.0.1:{port}"),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        runtime: crabka_schema_registry::config::RegistryRuntimeConfig::default(),
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
                acl_refresh: millis(300),
            }),
            client: None,
        },
    }
}

fn secure_cfg(bootstrap: &str, port: i32, tls: Option<TlsConfig>) -> RegistryConfig {
    secure_cfg_with_scheme(bootstrap, port, "http", tls)
}

/// An ACL entry granting `principal` `Allow` `op` on `Topic:s` (literal, any host).
fn principal_acl(principal: &str, op: AclOperation) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "s".into(),
        pattern_type: PatternType::Literal,
        principal: principal.into(),
        host: "*".into(),
        operation: op,
        permission_type: PermissionType::Allow,
    }
}

/// Seed `User:alice` Allow Write AND Read on `Topic:s`. `register` maps to
/// `Write`, `GET /subjects/s/versions` to `Read` (see `authz::authz_target`);
/// `SimpleAclAuthorizer` does not imply Read←Write, so both are required.
async fn seed_acls(bootstrap: &str) {
    seed_acls_for(bootstrap, "User:alice").await;
}

async fn seed_acls_for(bootstrap: &str, principal: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    let outcomes = admin
        .create_acls(&[
            principal_acl(principal, AclOperation::Write),
            principal_acl(principal, AclOperation::Read),
        ])
        .await
        .expect("create_acls");
    for o in outcomes {
        assert2::assert!(o.error.is_none());
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
                .run_acl_refresh(admin, millis(300), refresh_cancel)
                .await;
        });
    }

    let fwd = ForwardState {
        primary: primary.clone(),
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
        forward_max_body: cfg.runtime.forward_max_body,
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
        assert2::assert!(tokio::time::Instant::now() < deadline);
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
        assert2::assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn https_client(ca_pem: &str, identity: Option<(&str, &str)>) -> reqwest::Client {
    let ca = reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap();
    let mut builder = reqwest::Client::builder().add_root_certificate(ca);
    if let Some((cert_pem, key_pem)) = identity {
        let mut pem = String::with_capacity(cert_pem.len() + key_pem.len() + 1);
        pem.push_str(cert_pem);
        if !cert_pem.ends_with('\n') {
            pem.push('\n');
        }
        pem.push_str(key_pem);
        builder = builder.identity(reqwest::Identity::from_pem(pem.as_bytes()).unwrap());
    }
    builder.build().unwrap()
}

async fn register_over_mtls(
    http: &reqwest::Client,
    port: i32,
    subject: &str,
) -> reqwest::StatusCode {
    http.post(format!(
        "https://127.0.0.1:{port}/subjects/{subject}/versions"
    ))
    .header("content-type", SR_CONTENT_TYPE)
    .body(SCHEMA_BODY)
    .send()
    .await
    .unwrap()
    .status()
}

async fn await_register_mtls_200(http: &reqwest::Client, port: i32, subject: &str, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let status = register_over_mtls(http, port, subject).await;
        if status == 200 {
            return;
        }
        assert2::assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn await_get_body_over_mtls(
    http: &reqwest::Client,
    port: i32,
    subject: &str,
    expected: &str,
    secs: u64,
) {
    let url = format!("https://127.0.0.1:{port}/subjects/{subject}/versions");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Ok(response) = http.get(&url).send().await
            && response.status() == 200
            && let Ok(body) = response.text().await
            && body == expected
        {
            return;
        }
        assert2::assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn start_mtls_node(bootstrap: &str, tls: TlsConfig, forward_http: reqwest::Client) -> Node {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let cfg = secure_cfg_with_scheme(bootstrap, port, "https", Some(tls.clone()));
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    let primary = Election::start(&cfg, cancel.clone()).await.unwrap();

    let auth = AuthState {
        basic: None,
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
                .run_acl_refresh(admin, millis(300), refresh_cancel)
                .await;
        });
    }

    let fwd = ForwardState {
        primary: primary.clone(),
        http: forward_http,
        node_id: cfg.advertised_url.clone(),
        forward_max_body: cfg.runtime.forward_max_body,
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
        rest::serve::serve_https(listener, app, &tls, serve_cancel)
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
    let status = r.status();
    let www = r
        .headers()
        .get(reqwest::header::WWW_AUTHENTICATE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    // cp-calibrated form: lowercase `basic` scheme + the configured realm
    // (this node uses `realm: "test"`). See tests/fixtures/auth/basic.json.
    assert2::assert!(status.as_u16() == 401);
    assert2::assert!(www == r#"basic realm="test""#);

    // ── 401: wrong password, and an unknown user. ────────────────────────────
    for (_name, user, password) in [
        ("wrong_password", "alice", "wrong"),
        ("unknown_user", "bob", "pw"),
    ] {
        let response = http
            .post(&register_url)
            .header("content-type", SR_CONTENT_TYPE)
            .basic_auth(user, Some(password))
            .body(SCHEMA_BODY)
            .send()
            .await
            .unwrap();
        assert2::assert!(response.status() == 401);
    }

    // ── 200: alice has Write on `s` → authorized register (poll: ACL cache). ─
    await_register_200(&http, port, "s", 15).await;

    // ── 403: alice authenticates but has no ACL for `other`. ─────────────────
    let st = register_as_alice(&http, port, "other").await;
    assert2::assert!(st == 403);

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
        forward_max_body: cfg.runtime.forward_max_body,
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
    let anonymous_status = client.get(&base).send().await.unwrap().status();

    // alice:pw over TLS → 200 (the registry root, no authz requirement).
    let authenticated_status = client
        .get(&base)
        .basic_auth("alice", Some("pw"))
        .send()
        .await
        .unwrap()
        .status();
    for (_name, actual, expected) in [
        ("anonymous", anonymous_status, 401),
        ("authenticated", authenticated_status, 200),
    ] {
        assert2::assert!(actual == expected);
    }

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn mtls_two_nodes_authorize_then_forward_to_primary() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    seed_acls_for(&bootstrap, "User:CN=alice").await;

    let certdir = tempfile::tempdir().unwrap();
    let ca = generate_clients_ca("sr-mtls-ca", 365).unwrap();
    let server_sans = vec![SubjectAltName::Ip(std::net::IpAddr::V4(
        std::net::Ipv4Addr::LOCALHOST,
    ))];
    let server =
        issue_broker_cert(&ca.cert_pem, &ca.key_pem, "sr-node", &server_sans, &[], 365).unwrap();
    let alice = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).unwrap();

    let ca_path = certdir.path().join("ca.pem");
    let server_cert_path = certdir.path().join("server-cert.pem");
    let server_key_path = certdir.path().join("server-key.pem");
    std::fs::write(&ca_path, &ca.cert_pem).unwrap();
    std::fs::write(&server_cert_path, &server.cert_pem).unwrap();
    std::fs::write(&server_key_path, &server.key_pem).unwrap();

    let tls = TlsConfig {
        cert_chain_path: server_cert_path,
        private_key_path: server_key_path,
        trust_roots_path: Some(ca_path.clone()),
        client_ca_path: Some(ca_path),
        client_auth: ClientAuthMode::Required,
    };
    let forward_http = https_client(&ca.cert_pem, Some((&server.cert_pem, &server.key_pem)));
    let mtls_alice = https_client(&ca.cert_pem, Some((&alice.cert_pem, &alice.key_pem)));

    let mut a = start_mtls_node(&bootstrap, tls.clone(), forward_http.clone()).await;
    let mut b = start_mtls_node(&bootstrap, tls, forward_http).await;
    await_state(&mut a.primary, 25, |s| s.primary_url.is_some()).await;
    await_state(&mut b.primary, 25, |s| s.primary_url.is_some()).await;
    let a_is_primary = a.primary.borrow().is_primary;
    assert2::assert!(a_is_primary != b.primary.borrow().is_primary);
    let (primary_port, secondary_port) = if a_is_primary {
        (a.port, b.port)
    } else {
        (b.port, a.port)
    };

    await_register_mtls_200(&mtls_alice, secondary_port, "s", 20).await;
    await_get_body_over_mtls(&mtls_alice, primary_port, "s", "[1]", 20).await;
    await_get_body_over_mtls(&mtls_alice, secondary_port, "s", "[1]", 20).await;

    let status = register_over_mtls(&mtls_alice, secondary_port, "other").await;
    assert2::assert!(status == 403);

    a.cancel.cancel();
    b.cancel.cancel();
    broker.shutdown().await;
}

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
    assert2::assert!(a_is_primary != b.primary.borrow().is_primary);
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
    assert2::assert!(st == 403);

    a.cancel.cancel();
    b.cancel.cancel();
    broker.shutdown().await;
}

// ── JWKS test helpers ────────────────────────────────────────────────────────

/// Static RSA-2048 PKCS#8 private key used for JWT signing in tests.
/// This is the same constant as in `crates/security/src/jwks.rs` tests —
/// reproduced here because those helpers are `pub(crate)` and not accessible
/// from outside the security crate.
const RSA_PKCS8_B64: &str = "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC1Ekoc++7sSsH55QXBCq/aj71helk6ZCTkzYxfLRZXbox0FcV7vOkLNodetJLY7nAUekZLltQ7Q6FJ42geqGV+vgttF63Ue9OP24mPmn/OiFqVYhBaJDRI5BMBLCqZbUfpNBDh7ZOCczwlX8Z5FQS0QJBA4F26H9AKzFRvofwHFk1wxqiGdgwDyClgi+eDnhEGGhBEHuTl1edvTRif88rLDfPHKG1TRqKC6LMXCZQdNy7lrDEGPKHqfW4mb2mq7Vj6h2Jjv+1SpsSxdqX8Tsua4/LrAKvFIXfoZAnjzhACbhXqf1DdSdInZ0i1adY8JpgJQ+WtJ0i9aIOnnmDYwgMvAgMBAAECggEAHqBqUr62Kdd3Odpn/7/cAL7hTHSSVRMNPnoZ7RtGNSGothXcolJQpKnjebxXPkQORxhrfWuUmDWXOVUyjkTzbd2dNyWTLGaJYULD4LtENN3RXIUKuQR4p3+US1V6Gxtl12cMF/rEQYNWQAgUHPTWJ9rny2Fn2Qx6dukauwsOAvCU47fL873sm06SYgPJsLm7MKVeifl8dDudgpURxeC9z37cm9kjjE6n6aiBTNAuBEkMaAbcfgJ0RZfzaMo7IpsOeyOwp932JDlKROpQWKA+lz08YzhkU81qHJYOS/js2F0jxzFz31D9IN+OLu7vRCANFLJl/qnin1JEgVPh7gxSKQKBgQDfrQEsutvH1746ytfE+4jUXyv7Fuaz9MML8uaJbC4hMFdCJuMuLBY07bDE23+4byuWY7JHrgsLRaZ+qpNGWs3LH2x6xsHiK8Ivpuy8TVUJ6hgkPK1cr8yUJxaDcyV8tJAZ+mFmyyWx7wUdlgJFCa2MQF1HnrlBKZvSLWV4CjctZQKBgQDPPR2wLwyk6JlyapsVnCpNBGcXqbJxPh1TM7uPqlODxTzegUK+TMJDZ840u2aBNXf2D5WIJMl+/ohYefOOqK9z2OJUGObnJMgGusH04rdbBoDCdBwfwjiluU7vxbuQKBu8JNXzeb7HJhmgxtXWdJuFYcYbmGu8leFvmUxZTPRfAwKBgQCm6Gpf/m/SiGMjbAnmq+xGzV38V/J/hr2lRPRSx68EhRYX/vy3j55ikJu/yitcbViROIPoiS8kkizTiGWtskSuthw04ev74btd46n0OaCjbVPmdoDHEUgPpbtfC6WFkReWyweztRPD2yBuG2pGKhqe9cilkQOcZHgqNkXpdXYHIQKBgQCO0BQkdNfm0O/l3DdRdhPkjVMqCGSTC3YT/0OS5pK07PhccYF4ONdqsh91UWt7QUiRBf5LGubMoEV/i1LfjbmTQPP/dkWxJjS+Bndg9dfbX6jd2DwFWsfE1OXj8ESoPCuYxV23cr+Y59WjaUK1jhgam9106N3d0P/Q8zidFZ4V1wKBgQDFvIqMLnpaInWhb7kP+X6o0tPQSg+6odMWPnjhwnpSIiUjPUTZV4ijc/d1tPsUemFQxDe+ZreQXDMVGcAVldFnoEMyL8iAtMAHtsSmq2E80RNZfc6nUgy5esQ9rJeX2pH9aZCVvKv6iVTeUtAxS+ltjmEG9BSEI2WQI1WDzPbKiA==";

/// Decode RSA-2048 PKCS#8 DER bytes from the base64 constant above.
fn rsa_pkcs8_der() -> Vec<u8> {
    use base64::engine::general_purpose::STANDARD;
    STANDARD.decode(RSA_PKCS8_B64).unwrap()
}

/// Mint an RS256 JWT signed by the static RSA-2048 key and return
/// `(token_string, jwks_json_string)` where the JWKS contains the matching
/// public key with the given `kid`.
///
/// `claims_json` is the full JWT payload as a JSON string. Set `exp` to a
/// large Unix timestamp so tokens don't expire during tests.
fn mint_rs256_for_test(kid: &str, claims_json: &str) -> (String, String) {
    use ring::{
        rand::SystemRandom,
        signature::{RSA_PKCS1_SHA256, RsaKeyPair},
    };

    let der = rsa_pkcs8_der();
    let kp = RsaKeyPair::from_pkcs8(&der).unwrap();
    let header = format!("{{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"{kid}\"}}");
    let signing_input = format!(
        "{}.{}",
        B64.encode(header.as_bytes()),
        B64.encode(claims_json.as_bytes()),
    );
    let mut sig = vec![0u8; kp.public().modulus_len()];
    kp.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        signing_input.as_bytes(),
        &mut sig,
    )
    .unwrap();
    let token = format!("{signing_input}.{}", B64.encode(&sig));

    let (n, e) = split_pkcs1_for_jwks(kp.public().as_ref());
    let jwks = format!(
        "{{\"keys\":[{{\"kty\":\"RSA\",\"kid\":\"{kid}\",\"alg\":\"RS256\",\"use\":\"sig\",\"n\":\"{}\",\"e\":\"{}\"}}]}}",
        B64.encode(&n),
        B64.encode(&e),
    );
    (token, jwks)
}

/// Extract the (n, e) big-endian unsigned integers from a PKCS#1 `RSAPublicKey`
/// DER blob. ring exposes the public key as raw PKCS#1 bytes.
fn split_pkcs1_for_jwks(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
    fn read_len(b: &[u8]) -> (usize, usize) {
        if b[0] & 0x80 == 0 {
            (b[0] as usize, 1)
        } else {
            let nb = (b[0] & 0x7f) as usize;
            let mut l = 0usize;
            for i in 0..nb {
                l = (l << 8) | b[1 + i] as usize;
            }
            (l, 1 + nb)
        }
    }
    fn read_int(der: &[u8], p: &mut usize) -> Vec<u8> {
        assert2::assert!(der[*p] == 0x02);
        *p += 1;
        let (len, adv) = read_len(&der[*p..]);
        *p += adv;
        let mut bytes = der[*p..*p + len].to_vec();
        *p += len;
        if bytes.first() == Some(&0) {
            bytes.remove(0); // strip DER sign byte
        }
        bytes
    }
    let mut p = 0usize;
    assert2::assert!(der[p] == 0x30);
    p += 1;
    let (_, adv) = read_len(&der[p..]);
    p += adv;
    let n = read_int(der, &mut p);
    let e = read_int(der, &mut p);
    (n, e)
}

/// Start an SR node with JWKS Bearer auth. Routes through `build_security()` so
/// the production CLI code path (and its coverage) is exercised. The caller
/// provides a pre-built `Jwks` that is stored in the handle immediately,
/// bypassing the HTTP refresher (no real JWKS endpoint needed in tests).
async fn start_jwks_node(
    bootstrap: &str,
    initial_jwks: Jwks,
    valid_issuer: Option<String>,
) -> Node {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());

    // Exercise the production CLI assembly path so cli.rs JWKS code is covered.
    let input = SecurityCliInput {
        require_auth: true,
        realm: "test".into(),
        bearer: "jwks".into(),
        jwks_endpoint_uri: Some("https://test.invalid/.well-known/jwks.json".into()),
        jwks_valid_issuer: valid_issuer,
        // Default::default() gives "" for String fields; provide the same
        // defaults the binary's clap layer would supply at runtime.
        kafka_security_protocol: "PLAINTEXT".into(),
        bearer_principal_claim: "sub".into(),
        ..Default::default()
    };
    let out = build_security(&input).expect("build_security should succeed with JWKS input");
    let jwks_for_refresh = out.jwks_handle.unwrap();
    // Pre-load test keys directly — no HTTP fetch or refresher task in tests.
    jwks_for_refresh.handle.store(initial_jwks);

    let cfg = RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: format!("sr-jwks-{port}"),
        advertised_url: format!("http://127.0.0.1:{port}"),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        runtime: crabka_schema_registry::config::RegistryRuntimeConfig::default(),
        security: out.config,
    };
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    let primary = Election::start(&cfg, cancel.clone()).await.unwrap();

    let bearer_validator = cfg.security.bearer.as_ref().map(|b| b.validator.clone());
    let auth = AuthState {
        basic: None,
        bearer: bearer_validator,
        require_auth: true,
        realm: "test".into(),
    };
    let fwd = ForwardState {
        primary: primary.clone(),
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
        forward_max_body: cfg.runtime.forward_max_body,
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

// ── JWKS integration tests ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jwks_bearer_valid_signed_token_returns_200() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let (token, jwks_json) = mint_rs256_for_test("k1", r#"{"sub":"alice","exp":9999999999}"#);
    let jwks = Jwks::from_json(&jwks_json, true).unwrap();
    let mut node = start_jwks_node(&bootstrap, jwks, None).await;
    // Wait until the node becomes primary so _schemas topic is ready.
    await_state(&mut node.primary, 25, |s| s.is_primary).await;
    let base = format!("http://127.0.0.1:{}", node.port);
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/subjects/s/versions"))
        .header("Content-Type", SR_CONTENT_TYPE)
        .header("Authorization", format!("Bearer {token}"))
        .body(SCHEMA_BODY)
        .send()
        .await
        .unwrap();
    assert2::assert!(resp.status() == 200);

    node.cancel.cancel();
}

#[tokio::test]
async fn jwks_bearer_unsigned_token_returns_401() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let (_token, jwks_json) = mint_rs256_for_test("k1", r#"{"sub":"alice","exp":9999999999}"#);
    let jwks = Jwks::from_json(&jwks_json, true).unwrap();
    let node = start_jwks_node(&bootstrap, jwks, None).await;
    let base = format!("http://127.0.0.1:{}", node.port);
    let client = reqwest::Client::new();

    // Unsigned (alg:none) JWT — the Signed validator must reject it.
    let unsigned_token = format!(
        "{}.{}.{}",
        B64.encode(br#"{"alg":"none","typ":"JWT"}"#),
        B64.encode(br#"{"sub":"alice","exp":9999999999}"#),
        "",
    );
    let resp = client
        .post(format!("{base}/subjects/s/versions"))
        .header("Content-Type", SR_CONTENT_TYPE)
        .header("Authorization", format!("Bearer {unsigned_token}"))
        .body(SCHEMA_BODY)
        .send()
        .await
        .unwrap();
    assert2::assert!(resp.status() == 401);

    node.cancel.cancel();
}

#[tokio::test]
async fn jwks_bearer_wrong_issuer_returns_401() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let (token, jwks_json) = mint_rs256_for_test(
        "k1",
        r#"{"sub":"alice","iss":"https://wrong-idp.example.com","exp":9999999999}"#,
    );
    let jwks = Jwks::from_json(&jwks_json, true).unwrap();
    // Configure node to require iss = "https://idp.example.com"
    let node = start_jwks_node(&bootstrap, jwks, Some("https://idp.example.com".into())).await;
    let base = format!("http://127.0.0.1:{}", node.port);
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/subjects/s/versions"))
        .header("Content-Type", SR_CONTENT_TYPE)
        .header("Authorization", format!("Bearer {token}"))
        .body(SCHEMA_BODY)
        .send()
        .await
        .unwrap();
    assert2::assert!(resp.status() == 401);

    node.cancel.cancel();
}

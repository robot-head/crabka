//! Trusted-proxy authorization, end-to-end through the gateway's public
//! surface. These exercise the identity → ACL gate the proxy chain relies on:
//!
//! - the default `AllowAllAuthorizer` leaves produce unrestricted (regression),
//! - a `SimpleAclAuthorizer` over an ACL cache fetched from the broker denies an
//!   ungranted produce (`PERMISSION_DENIED`, no record) and allows a granted one,
//! - a bearer token resolves to the principal the ACL is written against,
//! - a forwarded record is re-authorized against the OWNER's cache (so a granted
//!   caller produces exactly once and an ungranted caller is rejected), and
//! - the per-decision audit event fires.
//!
//! The ACL cache is populated the same way the running gateway does it: create
//! ACLs via `AdminClient::create_acls`, spawn `GatewayAuthz::run_acl_refresh`
//! with a short interval, then poll until an authorized probe passes (the cache
//! is eventually consistent — never assert before it converges).

use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use axum::Extension;
use bytes::Bytes;
use connectrpc_axum::message::{ConnectRequest, ConnectResponse};
use crabka_authz::{
    AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, SimpleAclAuthorizer,
};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::{
    authz::GatewayAuthz,
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
    handlers, pb,
    produce::ProduceCore,
    state::AppState,
    types::GatewayRecord,
};

struct AuditVisitor(Option<String>, Option<bool>);

impl tracing::field::Visit for AuditVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "principal" {
            self.0 = Some(format!("{value:?}"));
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() == "allowed" {
            self.1 = Some(value);
        }
    }
}
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::{AuthMethod, Principal};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

/// gRPC `PERMISSION_DENIED`.
const PERMISSION_DENIED: i32 = 7;
const N: u32 = 4;
const DEDUP: &str = "__crabka_grpc_dedup";
const MEMBERSHIP: &str = "__crabka_grpc_gateway_membership";
const OWNERS_GROUP: &str = "__crabka_grpc_gateway_dedup_owners";

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// A resolved caller identity (mTLS, no groups) — the shape the trusted proxy
/// injects. ACLs are written against `User:{name}`.
fn principal(name: &str) -> Principal {
    Principal {
        name: name.to_string(),
        auth_method: AuthMethod::MTls,
        groups: vec![],
    }
}

fn anonymous() -> Principal {
    Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: AuthMethod::Anonymous,
        groups: vec![],
    }
}

/// An admin-crate `AclEntry` granting `User:{user}` Allow `op` on
/// `Topic:{topic}` (Literal pattern, any host). The admin crate keeps its own
/// ACL enum copies, so `create_acls` must be built from these — not
/// `crabka_metadata`'s.
fn topic_acl(
    user: &str,
    op: crabka_client_admin::AclOperation,
    topic: &str,
) -> crabka_client_admin::AclEntry {
    crabka_client_admin::AclEntry {
        resource_type: crabka_client_admin::ResourceType::Topic,
        resource_name: topic.to_string(),
        pattern_type: crabka_client_admin::PatternType::Literal,
        principal: format!("User:{user}"),
        host: "*".to_string(),
        operation: op,
        permission_type: crabka_client_admin::PermissionType::Allow,
    }
}

/// Build an `AppState` whose `authz` is the supplied authorizer. `dedup_topic`
/// etc. are wired but unused on the plain produce path these tests drive.
async fn app_state(bootstrap: &str, client: &str, authz: Arc<GatewayAuthz>) -> Arc<AppState> {
    let produce = ProduceCore::new(bootstrap, client, Arc::new(RawCodec), None)
        .await
        .unwrap();
    Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: client.into(),
            dedup_topic: DEDUP.into(),
            dedup_partitions: N,
            dedup_window_ms: 3_600_000,
            dedup_ownership_group: OWNERS_GROUP.into(),
            dedup_txn_id_prefix: format!("{client}-dedup"),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: MEMBERSHIP.into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
            runtime: crabka_grpc_gateway::config::GatewayRuntimeConfig::default(),
        }),
        authz,
        codec: Arc::new(RawCodec),
    })
}

/// Create a topic with one partition, replication 1.
async fn create_topic(bootstrap: &str, name: &str) {
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap.to_string()))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: name.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
}

/// Build a single `pb::SendRequest` carrying one unkeyed record (plain produce
/// path — no dedup, no forwarding).
fn send_one(topic: &str, value: &[u8]) -> pb::SendRequest {
    pb::SendRequest {
        records: vec![pb::Record {
            topic: topic.into(),
            key: None,
            body: Some(pb::record::Body::Raw(value.to_vec())),
            headers: BTreeMap::new().into_iter().collect(),
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
            schema: None,
        }],
        acks: pb::Acks::All as i32,
    }
}

/// Drive `handlers::send` for a single record as `principal`, returning the lone
/// `RecordResult`.
async fn send_as(
    state: &Arc<AppState>,
    principal: &Principal,
    topic: &str,
    value: &[u8],
) -> pb::RecordResult {
    let resp: ConnectResponse<pb::SendResponse> = handlers::send(
        Extension(state.clone()),
        Some(Extension(principal.clone())),
        None,
        ConnectRequest(send_one(topic, value)),
    )
    .await
    .expect("handler returned Err");
    let [result]: [pb::RecordResult; 1] = resp
        .0
        .results
        .try_into()
        .unwrap_or_else(|results: Vec<_>| panic!("expected one result, got {results:?}"));
    result
}

/// Spawn `run_acl_refresh` (short interval) and poll until an authorization
/// probe against the cache yields `expect`. The cache is eventually consistent
/// after `create_acls`, so this is how every ACL-dependent test arms itself —
/// it never asserts before convergence. Returns once the probe matches (≤ 20s).
async fn wait_until_probe(
    authz: &Arc<GatewayAuthz>,
    probe_principal: &Principal,
    rt: ResourceType,
    name: &str,
    op: AclOperation,
    expect: AuthorizationResult,
) {
    let host: SocketAddr = "0.0.0.0:0".parse().unwrap();
    for _ in 0..80 {
        let cache = authz.cache();
        let req = AuthorizationRequest {
            principal: probe_principal,
            host: &host,
            resource_type: rt,
            resource_name: name,
            operation: op,
        };
        if authz.authorizer().authorize(&**cache, &req) == expect {
            return;
        }
        drop(cache);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("ACL cache never converged to the expected decision for {name}");
}

/// 1. The default `AllowAllAuthorizer` leaves produce unrestricted: an
///    anonymous caller produces to a created topic with no authz error.
///    (Regression guard — installing the authz seam must not change the
///    out-of-the-box behavior.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allow_all_default_is_unrestricted() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "aa-topic").await;

    let authz = Arc::new(GatewayAuthz::new(Arc::new(AllowAllAuthorizer)));
    let state = app_state(&bootstrap, "aa", authz).await;

    let result = send_as(&state, &anonymous(), "aa-topic", b"hello").await;
    assert2::assert!(result.error.as_ref() == None);
    assert2::assert!(result.partition == 0);
    assert2::assert!(result.offset >= 0);

    broker.shutdown().await;
}

/// 2. `SimpleAclAuthorizer` with NO ACLs granted (default-deny): a produce as
///    `alice` to topic `t` is denied with `PERMISSION_DENIED` and the record
///    never reaches the topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simpleacl_denies_unauthorized_produce() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "t").await;

    // SimpleAcl, no super-users, EMPTY cache ⇒ default-deny for everyone.
    let authz = Arc::new(GatewayAuthz::new(Arc::new(SimpleAclAuthorizer::new(
        HashSet::new(),
    ))));
    let state = app_state(&bootstrap, "deny", authz).await;

    let alice = principal("alice");
    let result = send_as(&state, &alice, "t", b"nope").await;
    let err = result
        .error
        .expect("expected a per-record PERMISSION_DENIED");
    assert2::assert!(err.code == PERMISSION_DENIED);
    assert2::assert!(result.partition == -1);
    assert2::assert!(result.offset == -1);

    // The record must NOT have landed in the topic.
    assert2::assert!(count_value(&bootstrap, "t", b"nope").await == 0);

    broker.shutdown().await;
}

/// 3. With an ACL granting `User:alice Allow Write Topic:t`, refreshing the
///    cache, a produce as `alice` to `t` succeeds and the record is present; a
///    produce to `other` (no ACL) is still denied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simpleacl_allows_authorized_produce() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "t").await;
    create_topic(&bootstrap, "other").await;

    // Grant alice Write on Topic:t (only).
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    let outcomes = admin
        .create_acls(&[topic_acl(
            "alice",
            crabka_client_admin::AclOperation::Write,
            "t",
        )])
        .await
        .unwrap();
    assert2::assert!(outcomes.iter().all(|o| o.error.is_none()));

    let authz = Arc::new(GatewayAuthz::new(Arc::new(SimpleAclAuthorizer::new(
        HashSet::new(),
    ))));
    let shutdown = CancellationToken::new();
    tokio::spawn(authz.clone().run_acl_refresh(
        bootstrap.clone(),
        Duration::from_millis(200),
        shutdown.clone(),
        None,
    ));

    let alice = principal("alice");
    // Arm: wait until the refreshed cache authorizes alice's Write on `t`.
    wait_until_probe(
        &authz,
        &alice,
        ResourceType::Topic,
        "t",
        AclOperation::Write,
        AuthorizationResult::Allow,
    )
    .await;

    let state = app_state(&bootstrap, "allow", authz).await;

    // Granted topic → produced, no error, present in the topic.
    let ok = send_as(&state, &alice, "t", b"yes").await;
    assert2::assert!(ok.error.as_ref() == None);
    assert2::assert!(ok.partition == 0);
    assert2::assert!(ok.offset >= 0);
    assert2::assert!(count_value(&bootstrap, "t", b"yes").await == 1);

    // Ungranted topic → PERMISSION_DENIED, not produced.
    let denied = send_as(&state, &alice, "other", b"yes").await;
    assert2::assert!(denied.error.as_ref().map(|e| e.code) == Some(PERMISSION_DENIED));
    assert2::assert!(count_value(&bootstrap, "other", b"yes").await == 0);

    shutdown.cancel();
    broker.shutdown().await;
}

/// 4. The bearer path: `BearerSettings.build()` yields a validator that, given
///    an unsecured (`alg:none`) JWS with `sub=alice`, resolves a principal named
///    `alice`. Exercises the token → principal step the auth middleware drives
///    (the middleware wiring itself is covered by `auth_layer` unit tests).
#[tokio::test]
async fn bearer_token_resolves_principal() {
    use crabka_grpc_gateway::config::BearerSettings;

    let validator = BearerSettings {
        principal_claim_name: "sub".to_string(),
        allowable_clock_skew_ms: 30_000,
    }
    .build()
    .expect("bearer validator builds");

    // Unsecured JWS: base64url({"alg":"none"}).base64url({"sub":"alice","exp":..}).
    // (empty signature). `exp` is required + must be in the future.
    let token = unsecured_jws_alice();
    let now_ms: i64 = 1_000_000_000_000; // well before exp=9999999999s.
    let outcome = validator
        .validate(&token, now_ms)
        .await
        .expect("token validates");
    assert2::assert!(outcome.principal.name.as_str() == "alice");
    assert2::assert!(outcome.principal.auth_method == AuthMethod::SaslOAuthBearer);
    assert2::assert!(outcome.principal.groups.is_empty());
}

/// 5. Forwarding re-authorizes the ORIGINAL caller against the OWNER's cache.
///    Two gateways A and B share the same ACLs; a key owned by B is submitted
///    through A, which forwards to B. B re-authorizes the caller:
///    - `alice` (granted Write Topic:t) ⇒ produced exactly once;
///    - `mallory` (not granted) ⇒ B denies; the forward surfaces an error and
///      no extra record lands.
///
///    Deny surface: `forward_handler` returns HTTP 403 on a denied forward, and
///    the forwarding client parses that body and maps it to a non-retriable
///    `GatewayError::Unauthorized` (so the caller doesn't retry a permanent
///    denial). The load-bearing assertions are "allowed ⇒ produced once" and
///    "denied ⇒ Unauthorized + not produced".
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn forwarding_owner_reauthorizes_caller() {
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
    create_topic(&bootstrap, "t").await;

    // Grant alice Write Topic:t (mallory gets nothing).
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    let outcomes = admin
        .create_acls(&[topic_acl(
            "alice",
            crabka_client_admin::AclOperation::Write,
            "t",
        )])
        .await
        .unwrap();
    assert2::assert!(outcomes.iter().all(|o| o.error.is_none()));

    // Two SimpleAcl gateways with the SAME ACL cache (both refresh from the
    // broker) — so both authorize identically.
    let gw_a = spawn_acl_gateway(&bootstrap, "gwa").await;
    let gw_b = spawn_acl_gateway(&bootstrap, "gwb").await;

    // Wait for a disjoint covering split + converged routing (as in forwarding.rs).
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

    // Arm BOTH caches: alice's Write on `t` must authorize on each replica
    // before we drive a forward (the owner is the one that re-authorizes).
    let alice = principal("alice");
    for gw in [&gw_a, &gw_b] {
        wait_until_probe(
            &gw.state.authz,
            &alice,
            ResourceType::Topic,
            "t",
            AclOperation::Write,
            AuthorizationResult::Allow,
        )
        .await;
    }

    // Pick a key owned by B so a submit through A forwards to B.
    let key = (0..1000)
        .map(|i| format!("k{i}"))
        .find(|k| gw_b.store.owns(partition_for(k, N)))
        .expect("a key owned by B");
    let p = partition_for(&key, N);
    assert2::assert!(gw_b.store.owns(p) && !gw_a.store.owns(p));
    assert2::assert!(gw_a.membership.owner_of(p).as_deref() == Some(gw_b.addr.as_str()));

    let mk = |val: &str| GatewayRecord {
        topic: "t".into(),
        key: None,
        value: Bytes::from(val.to_string().into_bytes()),
        body_structured: None,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some(key.clone()),
    };

    // Allowed: alice forwarded A→B, B re-authorizes Write ⇒ produced once.
    let first = gw_a
        .state
        .produce
        .produce(mk("alice-val"), &alice)
        .await
        .expect("granted forward should produce");
    assert2::assert!(!first.deduplicated);
    assert2::assert!(count_value(&bootstrap, "t", b"alice-val").await == 1);

    // Denied: mallory forwarded A→B, B's cache denies ⇒ origin sees a
    // non-retriable Unauthorized (403 body parsed) and nothing extra lands.
    let mallory = principal("mallory");
    let denied = gw_a
        .state
        .produce
        .produce(mk("mallory-val"), &mallory)
        .await;
    assert2::assert!(matches!(denied, Err(GatewayError::Unauthorized(_))));
    assert2::assert!(count_value(&bootstrap, "t", b"mallory-val").await == 0);

    gw_a.token.cancel();
    gw_b.token.cancel();
    broker.shutdown().await;
}

/// 6. An audit event fires on a produce authz decision. We install a capturing
///    `tracing` layer scoped to the `gateway::audit` target and assert an event
///    with the principal + `allowed` field is emitted.
///
///    Capture is via a PROCESS-GLOBAL capturing subscriber installed once
///    (`set_global_default`). A thread-local `set_default` is *not* reliable
///    here: callsite interest is process-global, and on a multi-thread runtime
///    the synchronous audit `info!` can be emitted on a worker thread that does
///    not own the thread-local subscriber — so the event is silently missed
///    (the historical flake). A global subscriber fixes both: interest is
///    rebuilt under the capturing layer, and capture works from any thread.
///    To keep this immune to concurrent sibling tests writing into the shared
///    global capture, we produce under a UNIQUE principal and assert only its
///    own event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_log_emitted() {
    use std::sync::{Mutex, OnceLock};

    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    // Process-global capture of `gateway::audit` events as (principal, allowed),
    // filled by a global subscriber so it is thread-agnostic and immune to
    // callsite-interest caching. The test filters by its unique principal, so
    // concurrent sibling tests' audit events never affect the assertion.
    type AuditEvents = Mutex<Vec<(String, Option<bool>)>>;
    static AUDIT_EVENTS: OnceLock<AuditEvents> = OnceLock::new();
    fn events() -> &'static AuditEvents {
        AUDIT_EVENTS.get_or_init(|| Mutex::new(Vec::new()))
    }

    struct Cap;
    impl<S: tracing::Subscriber> Layer<S> for Cap {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() != "gateway::audit" {
                return;
            }
            // `principal = %name` records through the Display-as-Debug wrapper,
            // so it lands in `record_debug` (not `record_str`); `{:?}` is the
            // bare name.
            let mut v = AuditVisitor(None, None);
            event.record(&mut v);
            if let Some(principal) = v.0 {
                events().lock().unwrap().push((principal, v.1));
            }
        }
    }

    // Install the global capturing subscriber exactly once for this test binary.
    // `set_global_default` rebuilds the callsite-interest cache under the
    // capturing layer, so the `gateway::audit` event reaches it regardless of
    // which worker thread fires it. Nothing else in this binary installs a
    // global subscriber (boot() does not init tracing), so this never conflicts.
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Cap))
            .expect("install global audit-capturing subscriber");
    });

    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "audit-topic").await;
    let authz = Arc::new(GatewayAuthz::new(Arc::new(AllowAllAuthorizer)));
    let state = app_state(&bootstrap, "audit", authz).await;

    // Unique principal so concurrent sibling tests' audits can't satisfy (or
    // race) this assertion in the shared global capture.
    let probe = principal("audit-probe");
    let resp: ConnectResponse<pb::SendResponse> = handlers::send(
        Extension(state.clone()),
        Some(Extension(probe.clone())),
        None,
        ConnectRequest(send_one("audit-topic", b"audit-me")),
    )
    .await
    .expect("handler returned Err");
    let result = resp.0.results.into_iter().next().unwrap();
    // AllowAll ⇒ produced, no error.
    assert2::assert!(result.error.is_none());

    let found = {
        let g = events().lock().unwrap();
        g.iter().find(|(p, _)| p == "audit-probe").cloned()
    };
    let (_, allowed) = found.expect("no gateway::audit event for principal audit-probe");
    assert2::assert!(allowed == Some(true));

    broker.shutdown().await;
}

// ---- helpers ---------------------------------------------------------------

/// Count records in `topic` whose value equals `value` (drains a fresh
/// earliest-from-start read-committed consumer).
async fn count_value(bootstrap: &str, topic: &str, value: &[u8]) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("authz-verify")
        .group_id(format!("authz-verify-{topic}-{}", uuid_suffix()))
        .subscribe(vec![topic.to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut n = 0;
    for _ in 0..10 {
        let batch = consumer.poll(Duration::from_millis(500)).await.unwrap();
        for r in batch {
            if r.value.as_deref() == Some(value) {
                n += 1;
            }
        }
    }
    let _ = consumer.close().await;
    n
}

/// A short, unique suffix so each verify consumer uses a distinct group (avoids
/// cross-test offset bleed when tests run in the same process).
fn uuid_suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// An unsecured (`alg:none`) JWS with `sub=alice`, `exp=9999999999`
/// (year ~2286). Pre-encoded base64url (no padding), empty signature segment —
/// same construction the `auth_layer` unit tests use.
fn unsecured_jws_alice() -> String {
    // header  = {"alg":"none"}
    // payload = {"sub": "alice", "exp": 9999999999}
    "eyJhbGciOiJub25lIn0.eyJzdWIiOiAiYWxpY2UiLCAiZXhwIjogOTk5OTk5OTk5OX0.".to_string()
}

/// A gateway replica with a `SimpleAcl` authorizer (empty super-users) whose ACL
/// cache refreshes from the broker, full dedup + forwarding wiring, serving the
/// Connect + forward routers. Mirrors `forwarding.rs::spawn_gateway` but swaps
/// in the `SimpleAcl` authorizer + ACL refresh loop.
struct AclGw {
    addr: String,
    state: Arc<AppState>,
    store: Arc<DedupStore>,
    membership: Arc<MembershipStore>,
    token: CancellationToken,
}

async fn spawn_acl_gateway(bootstrap: &str, client: &str) -> AclGw {
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

    // SimpleAcl authorizer + ACL refresh loop (the distinguishing bit).
    let authz = Arc::new(GatewayAuthz::new(Arc::new(SimpleAclAuthorizer::new(
        HashSet::new(),
    ))));
    {
        let authz = authz.clone();
        let bootstrap = bootstrap.to_string();
        let token = token.clone();
        tokio::spawn(authz.run_acl_refresh(bootstrap, Duration::from_millis(200), token, None));
    }

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
            tls: None,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
            runtime: crabka_grpc_gateway::config::GatewayRuntimeConfig::default(),
        }),
        authz,
        codec: Arc::new(RawCodec),
    });

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

    AclGw {
        addr,
        state,
        store,
        membership,
        token,
    }
}

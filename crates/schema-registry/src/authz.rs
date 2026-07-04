//! Topic-ACL authorization for the registry REST surface.
//!
//! Reuses Kafka's ACL model via `crabka-authz`: each schema *subject* maps to a
//! `ResourceType::Topic` ACL by subject name; cluster-global operations map to
//! `ResourceType::Cluster` name `"kafka-cluster"`. ACLs are sourced from the
//! broker's `DescribeAcls` into an [`AclCache`] (the gateway pattern), refreshed
//! on a timer by [`SchemaRegistryAuthz::run_acl_refresh`].
//!
//! [`authz_target`] is the pure `(method, path) -> (resource, operation)` map;
//! [`authz_layer`] is the `from_fn_with_state` middleware that gates each request
//! (`403` on deny) and lets trusted intra-cluster forwards through untouched.

use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use crabka_authz::{AclCache, AuthorizationRequest, AuthorizationResult, Authorizer};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;
use tokio_util::sync::CancellationToken;

/// The `ResourceType::Cluster` resource name for cluster-global operations,
/// matching Kafka's convention.
const KAFKA_CLUSTER: &str = "kafka-cluster";

/// Map an incoming `(method, path)` to the `(resource_type, resource_name,
/// operation)` it must be authorized against, or `None` when the path carries no
/// authorization requirement (the registry root, health, unknown paths).
///
/// Subject-scoped endpoints map to `ResourceType::Topic` named by the subject;
/// cluster-global endpoints map to `ResourceType::Cluster` named
/// `KAFKA_CLUSTER`.
// One arm per REST endpoint keeps this a readable routing table; several
// distinct paths/methods legitimately map to the same decision (e.g. multiple
// reads -> (Cluster, Read); PUT and DELETE on /mode/{subject} both -> Alter), so
// `match_same_arms` (which would coalesce them and drop the per-path comments)
// is intentionally allowed here.
#[allow(clippy::match_same_arms)]
#[must_use]
pub fn authz_target(method: &Method, path: &str) -> Option<(ResourceType, String, AclOperation)> {
    // Split into non-empty segments.
    let seg: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Percent-decode a subject segment before using it as the resource name. The
    // handlers receive the subject via axum `Path<String>`, which percent-decodes
    // it (e.g. `foo%2Fbar` -> `foo/bar`); authz MUST evaluate the same resource,
    // or an `Allow Topic:foo/bar` over-denies and a `Deny Topic:foo/bar` is
    // evadable via the encoded form.
    let topic = |seg: &str, op| {
        let name = percent_encoding::percent_decode_str(seg)
            .decode_utf8_lossy()
            .into_owned();
        Some((ResourceType::Topic, name, op))
    };
    let cluster = |op| Some((ResourceType::Cluster, KAFKA_CLUSTER.to_string(), op));

    match seg.as_slice() {
        // ---- /subjects ... ------------------------------------------------
        // GET /subjects — list every subject (cluster-wide read of names).
        ["subjects"] if method == Method::GET => cluster(AclOperation::Describe),
        // POST /subjects/{subject} — look up a schema under a subject.
        // DELETE /subjects/{subject} — delete the whole subject.
        ["subjects", subject] => match *method {
            Method::POST => topic(subject, AclOperation::Read),
            Method::DELETE => topic(subject, AclOperation::Delete),
            _ => None,
        },
        // GET /subjects/{subject}/versions — list the version numbers (read).
        // POST /subjects/{subject}/versions — register a new schema version.
        ["subjects", subject, "versions"] => match *method {
            Method::GET => topic(subject, AclOperation::Read),
            Method::POST => topic(subject, AclOperation::Write),
            _ => None,
        },
        // GET /subjects/{subject}/versions/{version} — read a version.
        // DELETE /subjects/{subject}/versions/{version} — delete a version.
        ["subjects", subject, "versions", _version] => match *method {
            Method::GET => topic(subject, AclOperation::Read),
            Method::DELETE => topic(subject, AclOperation::Delete),
            _ => None,
        },
        // GET /subjects/{subject}/versions/{version}/{schema|referencedby}.
        [
            "subjects",
            subject,
            "versions",
            _version,
            "schema" | "referencedby",
        ] if method == Method::GET => topic(subject, AclOperation::Read),

        // ---- /config ... --------------------------------------------------
        // Global compatibility level.
        ["config"] => match *method {
            Method::PUT => cluster(AclOperation::Alter),
            Method::GET => cluster(AclOperation::Describe),
            _ => None,
        },
        // Per-subject compatibility level.
        ["config", subject] => match *method {
            Method::PUT => topic(subject, AclOperation::Alter),
            Method::GET => topic(subject, AclOperation::Describe),
            _ => None,
        },

        // ---- /mode ... ----------------------------------------------------
        // Global mode (mirrors /config: PUT = Alter, GET = Describe).
        ["mode"] => match *method {
            Method::PUT => cluster(AclOperation::Alter),
            Method::GET => cluster(AclOperation::Describe),
            _ => None,
        },
        // Per-subject mode override (mirrors /config/{subject}); PUT sets and
        // DELETE clears it (both mutations → Alter), GET reads → Describe.
        ["mode", subject] => match *method {
            Method::PUT => topic(subject, AclOperation::Alter),
            Method::DELETE => topic(subject, AclOperation::Alter),
            Method::GET => topic(subject, AclOperation::Describe),
            _ => None,
        },

        // ---- /compatibility ... -------------------------------------------
        // Test a candidate schema against an existing version (read-only).
        ["compatibility", "subjects", subject, "versions", _version] if method == Method::POST => {
            topic(subject, AclOperation::Read)
        }

        // ---- /schemas ... -------------------------------------------------
        // GET /schemas/types — list the supported schema types (cluster info).
        ["schemas", "types"] if method == Method::GET => cluster(AclOperation::Describe),
        // POST /schemas/import — bulk-register a FileDescriptorSet spanning
        // multiple subjects, so authorize it as a cluster-level schema write.
        ["schemas", "import"] if method == Method::POST => cluster(AclOperation::Write),
        // GET /schemas/ids/{id} and GET /schemas — read schemas by id / list all.
        ["schemas", "ids", _] | ["schemas"] if method == Method::GET => cluster(AclOperation::Read),
        // GET /schemas/ids/{id}/versions — subjects/versions using a schema id.
        ["schemas", "ids", _, "versions"] if method == Method::GET => cluster(AclOperation::Read),

        // Root, health, and anything unrecognized carry no authz requirement.
        _ => None,
    }
}

/// Topic-ACL authorization decision point for the registry. Holds the
/// [`Authorizer`] (a `crabka-authz` evaluator) and an `ArcSwap`'d [`AclCache`]
/// refreshed from the broker's `DescribeAcls` by [`Self::run_acl_refresh`].
/// Mirrors `grpc-gateway`'s `GatewayAuthz`.
pub struct SchemaRegistryAuthz {
    authorizer: Arc<dyn Authorizer>,
    cache: ArcSwap<AclCache>,
    super_users: HashSet<String>,
    enabled: bool,
}

impl SchemaRegistryAuthz {
    /// Build with the configured super-user set. When `enabled` is false every
    /// request is allowed (the authz-disabled default). The super-user bypass is
    /// enforced both by the name short-circuit in [`Self::authorize`] and by the
    /// underlying [`crabka_authz::SimpleAclAuthorizer`], which is constructed
    /// with the same set.
    #[must_use]
    pub fn new(super_users: HashSet<String>, enabled: bool) -> Self {
        let authorizer: Arc<dyn Authorizer> =
            Arc::new(crabka_authz::SimpleAclAuthorizer::new(super_users.clone()));
        Self {
            authorizer,
            cache: ArcSwap::from_pointee(AclCache::default()),
            super_users,
            enabled,
        }
    }

    /// Poll `DescribeAcls` into the cache until `shutdown` (the gateway pattern).
    /// On error the prior snapshot is kept and a warning is logged.
    pub async fn run_acl_refresh(
        &self,
        mut admin: crabka_client_admin::AdminClient,
        refresh: Duration,
        shutdown: CancellationToken,
    ) {
        loop {
            match admin
                .describe_acls(&crabka_client_admin::AclEntryFilter::default())
                .await
            {
                Ok(entries) => {
                    let entries = entries.into_iter().map(acl_entry_from_admin).collect();
                    self.cache.store(Arc::new(AclCache::new(entries)));
                }
                Err(e) => tracing::warn!(error = %e, "ACL refresh failed; keeping prior snapshot"),
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(refresh) => {}
            }
        }
    }

    /// Decide whether `principal` may perform `op` on `(rt, name)` from `host`.
    /// Short-circuits to `true` when authz is disabled or the principal is a
    /// super-user; otherwise consults the current [`AclCache`].
    #[must_use]
    pub fn authorize(
        &self,
        principal: &Principal,
        host: &SocketAddr,
        rt: ResourceType,
        name: &str,
        op: AclOperation,
    ) -> bool {
        if !self.enabled || self.super_users.contains(&principal.name) {
            return true;
        }
        let cache = self.cache.load();
        matches!(
            self.authorizer.authorize(
                &**cache,
                &AuthorizationRequest {
                    principal,
                    host,
                    resource_type: rt,
                    resource_name: name,
                    operation: op,
                }
            ),
            AuthorizationResult::Allow
        )
    }
}

/// Convert a `crabka_client_admin::AclEntry` into a `crabka_metadata::AclEntry`.
/// The admin crate keeps a structurally identical local copy (same field names
/// and enum variants) to avoid a broker dependency; this maps between them.
fn acl_entry_from_admin(e: crabka_client_admin::AclEntry) -> crabka_metadata::AclEntry {
    use crabka_client_admin::{
        AclOperation as AO, PatternType as PT, PermissionType as Perm, ResourceType as RT,
    };
    use crabka_metadata::{
        AclEntry as ME, AclOperation as MAO, PatternType as MPT, PermissionType as MPerm,
        ResourceType as MRT,
    };

    let resource_type = match e.resource_type {
        RT::Topic => MRT::Topic,
        RT::Group => MRT::Group,
        RT::Cluster => MRT::Cluster,
        RT::TransactionalId => MRT::TransactionalId,
    };
    let pattern_type = match e.pattern_type {
        PT::Literal => MPT::Literal,
        PT::Prefixed => MPT::Prefixed,
    };
    let operation = match e.operation {
        AO::All => MAO::All,
        AO::Read => MAO::Read,
        AO::Write => MAO::Write,
        AO::Create => MAO::Create,
        AO::Delete => MAO::Delete,
        AO::Alter => MAO::Alter,
        AO::Describe => MAO::Describe,
        AO::ClusterAction => MAO::ClusterAction,
        AO::DescribeConfigs => MAO::DescribeConfigs,
        AO::AlterConfigs => MAO::AlterConfigs,
        AO::IdempotentWrite => MAO::IdempotentWrite,
        AO::TwoPhaseCommit => MAO::TwoPhaseCommit,
    };
    let permission_type = match e.permission_type {
        Perm::Allow => MPerm::Allow,
        Perm::Deny => MPerm::Deny,
    };

    ME {
        resource_type,
        resource_name: e.resource_name,
        pattern_type,
        principal: e.principal,
        host: e.host,
        operation,
        permission_type,
    }
}

/// `from_fn_with_state` middleware that gates each request. Trusted intra-cluster
/// forwards (carrying [`crate::rest::forward::FORWARD_HEADER`]) skip authz — they
/// were already authorized at the receiving node. Paths with no authz
/// requirement ([`authz_target`] returns `None`) pass through. On deny, `403`.
pub async fn authz_layer(
    State(az): State<Arc<SchemaRegistryAuthz>>,
    req: Request,
    next: Next,
) -> Response {
    // SECURITY: a request carrying the inter-node forward header skips authz — the
    // ingress node already authorized it. This trusts the inter-node link: a CLIENT
    // that sets `X-Forwarded-For-Registry` directly bypasses authz on this node.
    // Operators MUST isolate the inter-node forwarding link (network policy /
    // inter-node mTLS) so external clients cannot reach it.
    if req
        .headers()
        .contains_key(crate::rest::forward::FORWARD_HEADER)
    {
        return next.run(req).await;
    }
    let Some((rt, name, op)) = authz_target(req.method(), req.uri().path()) else {
        return next.run(req).await;
    };
    let principal = req
        .extensions()
        .get::<Principal>()
        .cloned()
        .unwrap_or_else(crate::auth::anonymous);
    // The TLS accept loop inserts the peer `SocketAddr` into extensions (the
    // gateway pattern); fall back to a wildcard host when it is absent (e.g.
    // plain HTTP without peer wiring). Host-scoped ACLs are rare.
    let host: SocketAddr = req
        .extensions()
        .get::<SocketAddr>()
        .copied()
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
    if az.authorize(&principal, &host, rt, &name, op) {
        next.run(req).await
    } else {
        (StatusCode::FORBIDDEN, "authorization denied").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(m: &str, p: &str) -> Option<(ResourceType, String, AclOperation)> {
        authz_target(&m.parse().unwrap(), p)
    }

    #[test]
    fn unmapped_methods_on_known_routes_have_no_authz_target() {
        // A method that isn't a valid Schema-Registry operation on an otherwise
        // known route maps to None (no authz requirement) — the handler rejects
        // it with 405 on its own. This pins every per-route `_ => None` arm so a
        // future route edit can't silently turn a real mutation into a no-authz
        // pass-through.
        for (m, p) in [
            ("PUT", "/subjects/s"),
            ("DELETE", "/subjects/s/versions"),
            ("PUT", "/subjects/s/versions/1"),
            ("DELETE", "/config"),
            ("DELETE", "/config/s"),
            ("DELETE", "/mode"),
            ("POST", "/mode/s"),
        ] {
            assert_eq!(t(m, p), None, "{m} {p} should have no authz target");
        }
    }

    #[test]
    fn acl_entry_from_admin_maps_two_phase_commit() {
        // KIP-939: the admin→metadata ACL conversion must carry TwoPhaseCommit
        // through (it is the operation a 2PC grant on a TransactionalId uses).
        let admin = crabka_client_admin::AclEntry {
            resource_type: crabka_client_admin::ResourceType::TransactionalId,
            resource_name: "my-txn".into(),
            pattern_type: crabka_client_admin::PatternType::Literal,
            principal: "User:flink".into(),
            host: "*".into(),
            operation: crabka_client_admin::AclOperation::TwoPhaseCommit,
            permission_type: crabka_client_admin::PermissionType::Allow,
        };
        let meta = acl_entry_from_admin(admin);
        assert_eq!(
            meta.operation,
            crabka_metadata::AclOperation::TwoPhaseCommit
        );
        assert_eq!(
            meta.resource_type,
            crabka_metadata::ResourceType::TransactionalId
        );
    }

    #[test]
    fn register_is_write_on_topic_subject() {
        assert_eq!(
            t("POST", "/subjects/orders-value/versions"),
            Some((
                ResourceType::Topic,
                "orders-value".to_string(),
                AclOperation::Write
            ))
        );
    }

    #[test]
    fn read_version_is_read_on_topic() {
        assert_eq!(
            t("GET", "/subjects/orders-value/versions/1"),
            Some((
                ResourceType::Topic,
                "orders-value".to_string(),
                AclOperation::Read
            ))
        );
    }

    #[test]
    fn version_schema_and_referencedby_are_read() {
        assert_eq!(
            t("GET", "/subjects/s/versions/1/schema"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Read))
        );
        assert_eq!(
            t("GET", "/subjects/s/versions/1/referencedby"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Read))
        );
    }

    #[test]
    fn delete_subject_is_delete() {
        assert_eq!(
            t("DELETE", "/subjects/orders-value"),
            Some((
                ResourceType::Topic,
                "orders-value".to_string(),
                AclOperation::Delete
            ))
        );
    }

    #[test]
    fn delete_version_is_delete() {
        assert_eq!(
            t("DELETE", "/subjects/s/versions/2"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Delete))
        );
    }

    #[test]
    fn lookup_post_subject_is_read() {
        assert_eq!(
            t("POST", "/subjects/s"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Read))
        );
    }

    #[test]
    fn subject_config_put_is_alter_get_is_describe() {
        assert_eq!(
            t("PUT", "/config/s"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Alter))
        );
        assert_eq!(
            t("GET", "/config/s"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Describe))
        );
    }

    #[test]
    fn subject_mode_delete_is_alter() {
        assert_eq!(
            t("DELETE", "/mode/s"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Alter))
        );
    }

    #[test]
    fn compatibility_is_read() {
        assert_eq!(
            t("POST", "/compatibility/subjects/s/versions/1"),
            Some((ResourceType::Topic, "s".to_string(), AclOperation::Read))
        );
    }

    #[test]
    fn global_config_put_is_alter_cluster() {
        assert_eq!(
            t("PUT", "/config"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Alter
            ))
        );
    }

    #[test]
    fn global_config_get_and_mode_get_are_describe_cluster() {
        assert_eq!(
            t("GET", "/config"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Describe
            ))
        );
        assert_eq!(
            t("GET", "/mode"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Describe
            ))
        );
    }

    #[test]
    fn list_subjects_is_describe_cluster() {
        assert_eq!(
            t("GET", "/subjects"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Describe
            ))
        );
    }

    #[test]
    fn schemas_endpoints_are_read_cluster() {
        assert_eq!(
            t("GET", "/schemas/ids/1"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Read
            ))
        );
        assert_eq!(
            t("GET", "/schemas"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Read
            ))
        );
    }

    #[test]
    fn schemas_import_post_is_write_cluster_only() {
        assert_eq!(
            t("POST", "/schemas/import"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Write
            ))
        );
        assert_eq!(t("GET", "/schemas/import"), None);
        assert_eq!(t("PUT", "/schemas/import"), None);
    }

    #[test]
    fn schemas_types_is_describe_cluster() {
        assert_eq!(
            t("GET", "/schemas/types"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".to_string(),
                AclOperation::Describe
            ))
        );
    }

    #[test]
    fn put_global_mode_is_alter_cluster() {
        assert_eq!(
            t("PUT", "/mode"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".into(),
                AclOperation::Alter
            ))
        );
    }

    #[test]
    fn subject_mode_put_is_alter_get_is_describe() {
        assert_eq!(
            t("PUT", "/mode/s"),
            Some((ResourceType::Topic, "s".into(), AclOperation::Alter))
        );
        assert_eq!(
            t("GET", "/mode/s"),
            Some((ResourceType::Topic, "s".into(), AclOperation::Describe))
        );
    }

    #[test]
    fn list_versions_is_read_topic() {
        assert_eq!(
            t("GET", "/subjects/s/versions"),
            Some((ResourceType::Topic, "s".into(), AclOperation::Read))
        );
    }

    #[test]
    fn schemas_id_versions_is_read_cluster() {
        assert_eq!(
            t("GET", "/schemas/ids/1/versions"),
            Some((
                ResourceType::Cluster,
                "kafka-cluster".into(),
                AclOperation::Read
            ))
        );
    }

    #[test]
    fn subject_is_percent_decoded() {
        assert_eq!(
            t("POST", "/subjects/foo%2Fbar/versions"),
            Some((ResourceType::Topic, "foo/bar".into(), AclOperation::Write))
        );
    }

    #[test]
    fn root_is_none() {
        assert_eq!(t("GET", "/"), None);
    }

    // ---- SchemaRegistryAuthz::authorize ----------------------------------

    use crabka_metadata::{AclEntry, PatternType, PermissionType};

    fn host() -> SocketAddr {
        "0.0.0.0:0".parse().unwrap()
    }

    /// An ACL allowing `User:alice` to `Write` `Topic:"s"` (literal, any host).
    fn alice_write_s() -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "s".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Write,
            permission_type: PermissionType::Allow,
        }
    }

    fn alice() -> Principal {
        Principal {
            name: "alice".into(),
            auth_method: crabka_security::AuthMethod::SaslPlain,
            groups: vec![],
        }
    }

    fn with_acls(
        super_users: HashSet<String>,
        enabled: bool,
        acls: Vec<AclEntry>,
    ) -> SchemaRegistryAuthz {
        let az = SchemaRegistryAuthz::new(super_users, enabled);
        az.cache.store(Arc::new(AclCache::new(acls)));
        az
    }

    #[test]
    fn enabled_allows_matching_acl() {
        let az = with_acls(HashSet::new(), true, vec![alice_write_s()]);
        assert!(az.authorize(
            &alice(),
            &host(),
            ResourceType::Topic,
            "s",
            AclOperation::Write
        ));
    }

    #[test]
    fn enabled_denies_other_subject() {
        let az = with_acls(HashSet::new(), true, vec![alice_write_s()]);
        // Same principal/op, but a different topic name → no matching ACL → deny.
        assert!(!az.authorize(
            &alice(),
            &host(),
            ResourceType::Topic,
            "other",
            AclOperation::Write
        ));
    }

    #[test]
    fn super_user_is_allowed_without_acls() {
        let supers: HashSet<String> = ["alice".to_string()].into_iter().collect();
        // No ACLs at all, yet the super-user is allowed.
        let az = with_acls(supers, true, vec![]);
        assert!(az.authorize(
            &alice(),
            &host(),
            ResourceType::Topic,
            "s",
            AclOperation::Write
        ));
    }

    #[test]
    fn disabled_allows_everything() {
        // enabled=false → allow-all, even with no matching ACL and no super-user.
        let az = with_acls(HashSet::new(), false, vec![]);
        assert!(az.authorize(
            &alice(),
            &host(),
            ResourceType::Topic,
            "s",
            AclOperation::Write
        ));
        // Cluster-scoped op is also allowed when disabled.
        assert!(az.authorize(
            &crate::auth::anonymous(),
            &host(),
            ResourceType::Cluster,
            "kafka-cluster",
            AclOperation::Describe
        ));
    }

    // ---- authz_layer middleware ------------------------------------------
    //
    // Driven over a tiny `Router` + `oneshot` (the auth-layer test pattern).
    // NOTE: `run_acl_refresh` needs a live admin/broker and is exercised by the
    // `tests/security.rs` integration test, so it is intentionally NOT unit-
    // tested here; these cover the pure request-gating branches.

    use axum::{
        Router,
        body::Body,
        http::Request,
        routing::{get, post},
    };
    use tower::ServiceExt as _; // for `oneshot`

    /// A router with `authz_layer` over `az`, exposing `/` (no authz target),
    /// `GET /subjects` (cluster Describe) and `POST /subjects/{s}/versions`
    /// (topic Write). The handler echoes `ok` so a pass-through is `200`.
    fn authz_app(az: SchemaRegistryAuthz) -> Router {
        Router::new()
            .route("/", get(|| async { "ok" }))
            .route("/subjects", get(|| async { "ok" }))
            .route("/subjects/{subject}/versions", post(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(az),
                authz_layer,
            ))
    }

    #[tokio::test]
    async fn authz_layer_denies_principal_without_acl() {
        // enabled, no ACLs, non-super-user, non-forwarded → 403 on a gated path.
        let app = authz_app(with_acls(HashSet::new(), true, vec![]));
        let req = Request::builder()
            .method("POST")
            .uri("/subjects/orders-value/versions")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn authz_layer_allows_matching_acl() {
        // An ACL granting Write on Topic:"orders-value" → the principal passes.
        // The middleware falls back to the anonymous principal (no auth layer in
        // this router), so the ACL is written for "User:ANONYMOUS".
        let acl = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders-value".into(),
            pattern_type: PatternType::Literal,
            principal: "User:ANONYMOUS".into(),
            host: "*".into(),
            operation: AclOperation::Write,
            permission_type: PermissionType::Allow,
        };
        let app = authz_app(with_acls(HashSet::new(), true, vec![acl]));
        let req = Request::builder()
            .method("POST")
            .uri("/subjects/orders-value/versions")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn authz_layer_passes_path_with_no_target() {
        // `GET /` maps to `None` in authz_target → no authz requirement → passes
        // even when enabled with no ACLs.
        let app = authz_app(with_acls(HashSet::new(), true, vec![]));
        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn authz_layer_skips_forwarded_request() {
        // A trusted intra-cluster forward (FORWARD_HEADER present) skips authz
        // entirely — even on a gated path that would otherwise 403.
        let app = authz_app(with_acls(HashSet::new(), true, vec![]));
        let req = Request::builder()
            .method("POST")
            .uri("/subjects/orders-value/versions")
            .header(crate::rest::forward::FORWARD_HEADER, "ingress-node")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "forwarded request must skip authz"
        );
    }
}

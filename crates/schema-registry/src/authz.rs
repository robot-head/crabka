//! Topic-ACL authorization for the registry REST surface.
//!
//! This module reuses Kafka's ACL model through `crabka-authz`. Each schema
//! *subject* maps to a `ResourceType::Topic` ACL by subject name.
//! Cluster-global operations map to `ResourceType::Cluster` name
//! `"kafka-cluster"`. ACLs come from the broker's `DescribeAcls` into an
//! [`AclCache`], which is the gateway pattern, and
//! [`SchemaRegistryAuthz::run_acl_refresh`] refreshes them on a timer.
//!
//! [`authz_target`] is the pure `(method, path) -> (resource, operation)` map.
//! [`authz_layer`] is the `from_fn_with_state` middleware that gates each
//! request, returns `403` on deny, and lets trusted intra-cluster forwards
//! through untouched.

use std::{collections::HashSet, net::SocketAddr, sync::Arc};

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
use crabka_units::prelude::*;
use tokio_util::sync::CancellationToken;

/// The `ResourceType::Cluster` resource name for cluster-global operations,
/// matching Kafka's convention.
const KAFKA_CLUSTER: &str = "kafka-cluster";

/// Map an incoming `(method, path)` to the `(resource_type, resource_name,
/// operation)` it must be authorized against. Returns `None` when the path
/// carries no authorization requirement, such as the registry root, health, and
/// unknown paths.
///
/// Subject-scoped endpoints map to `ResourceType::Topic` named by the subject;
/// cluster-global endpoints map to `ResourceType::Cluster` named
/// `KAFKA_CLUSTER`.
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
        // GET /subjects and /schemas/types expose cluster-wide metadata.
        ["subjects"] | ["schemas", "types"] if method == Method::GET => {
            cluster(AclOperation::Describe)
        }
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
        // Global compatibility level and global mode share the same policy.
        ["config" | "mode"] => match *method {
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
        // Per-subject mode override (mirrors /config/{subject}); PUT sets and
        // DELETE clears it (both mutations → Alter), GET reads → Describe.
        ["mode", subject] => match *method {
            Method::PUT | Method::DELETE => topic(subject, AclOperation::Alter),
            Method::GET => topic(subject, AclOperation::Describe),
            _ => None,
        },

        // ---- /compatibility ... -------------------------------------------
        // Test a candidate schema against an existing version (read-only).
        ["compatibility", "subjects", subject, "versions", _version] if method == Method::POST => {
            topic(subject, AclOperation::Read)
        }

        // ---- /schemas ... -------------------------------------------------
        // POST /schemas/import — bulk-register a FileDescriptorSet spanning
        // multiple subjects, so authorize it as a cluster-level schema write.
        ["schemas", "import"] if method == Method::POST => cluster(AclOperation::Write),
        // GET /schemas/ids/{id} and GET /schemas — read schemas by id / list all.
        ["schemas", "ids", _] | ["schemas"] | ["schemas", "ids", _, "versions"]
            if method == Method::GET =>
        {
            cluster(AclOperation::Read)
        }

        // Root, health, and anything unrecognized carry no authz requirement.
        _ => None,
    }
}

/// Topic-ACL authorization decision point for the registry.
///
/// It holds the [`Authorizer`], which is a `crabka-authz` evaluator, and an
/// `ArcSwap`'d [`AclCache`] that [`Self::run_acl_refresh`] refreshes from the
/// broker's `DescribeAcls`. It mirrors `grpc-gateway`'s `GatewayAuthz`.
pub struct SchemaRegistryAuthz {
    authorizer: Arc<dyn Authorizer>,
    cache: ArcSwap<AclCache>,
    super_users: HashSet<String>,
    enabled: bool,
}

impl SchemaRegistryAuthz {
    /// Build with the configured super-user set. When `enabled` is false, every
    /// request is allowed, which is the authz-disabled default. The
    /// super-user bypass is enforced both by the name short-circuit in
    /// [`Self::authorize`] and by the underlying
    /// [`crabka_authz::SimpleAclAuthorizer`], which is constructed with the
    /// same set.
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

    /// Poll `DescribeAcls` into the cache until `shutdown`. This is the gateway
    /// pattern. On error this method keeps the prior snapshot and logs a
    /// warning.
    pub async fn run_acl_refresh(
        &self,
        mut admin: crabka_client_admin::AdminClient,
        refresh: Time,
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
                () = tokio::time::sleep(refresh.to_std()) => {}
            }
        }
    }

    /// Decide whether `principal` may perform `op` on `(rt, name)` from `host`.
    /// Short-circuits to `true` when authz is disabled or the principal is a
    /// super-user. In every other case it consults the current [`AclCache`].
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
/// The admin crate keeps a structurally identical local copy, with the same
/// field names and enum variants, to avoid a broker dependency. This function
/// maps between them.
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

/// `from_fn_with_state` middleware that gates each request.
///
/// Trusted intra-cluster forwards, which carry
/// [`crate::rest::forward::FORWARD_HEADER`], skip authz. The receiving node
/// already authorized them. Paths with no authz requirement, where
/// [`authz_target`] returns `None`, pass through. On deny the middleware
/// returns `403`.
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
            assert2::assert!(t(m, p) == None);
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
        assert2::assert!(
            meta == crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::TransactionalId,
                resource_name: "my-txn".to_string(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:flink".to_string(),
                host: "*".to_string(),
                operation: crabka_metadata::AclOperation::TwoPhaseCommit,
                permission_type: crabka_metadata::PermissionType::Allow,
            }
        );
    }

    use AclOperation::{Alter, Delete, Describe, Read, Write};
    use ResourceType::{Cluster, Topic};

    type RouteMappingCase = (
        &'static str,
        &'static str,
        &'static str,
        Option<(ResourceType, &'static str, AclOperation)>,
    );

    const ROUTE_MAPPING_CASES: &[RouteMappingCase] = &[
        (
            "register",
            "POST",
            "/subjects/orders-value/versions",
            Some((Topic, "orders-value", Write)),
        ),
        (
            "read-version",
            "GET",
            "/subjects/orders-value/versions/1",
            Some((Topic, "orders-value", Read)),
        ),
        (
            "version-schema",
            "GET",
            "/subjects/s/versions/1/schema",
            Some((Topic, "s", Read)),
        ),
        (
            "referenced-by",
            "GET",
            "/subjects/s/versions/1/referencedby",
            Some((Topic, "s", Read)),
        ),
        (
            "delete-subject",
            "DELETE",
            "/subjects/orders-value",
            Some((Topic, "orders-value", Delete)),
        ),
        (
            "delete-version",
            "DELETE",
            "/subjects/s/versions/2",
            Some((Topic, "s", Delete)),
        ),
        (
            "lookup-subject",
            "POST",
            "/subjects/s",
            Some((Topic, "s", Read)),
        ),
        (
            "alter-subject-config",
            "PUT",
            "/config/s",
            Some((Topic, "s", Alter)),
        ),
        (
            "describe-subject-config",
            "GET",
            "/config/s",
            Some((Topic, "s", Describe)),
        ),
        (
            "delete-subject-mode",
            "DELETE",
            "/mode/s",
            Some((Topic, "s", Alter)),
        ),
        (
            "compatibility",
            "POST",
            "/compatibility/subjects/s/versions/1",
            Some((Topic, "s", Read)),
        ),
        (
            "alter-global-config",
            "PUT",
            "/config",
            Some((Cluster, "kafka-cluster", Alter)),
        ),
        (
            "describe-global-config",
            "GET",
            "/config",
            Some((Cluster, "kafka-cluster", Describe)),
        ),
        (
            "describe-global-mode",
            "GET",
            "/mode",
            Some((Cluster, "kafka-cluster", Describe)),
        ),
        (
            "list-subjects",
            "GET",
            "/subjects",
            Some((Cluster, "kafka-cluster", Describe)),
        ),
        (
            "schema-by-id",
            "GET",
            "/schemas/ids/1",
            Some((Cluster, "kafka-cluster", Read)),
        ),
        (
            "list-schemas",
            "GET",
            "/schemas",
            Some((Cluster, "kafka-cluster", Read)),
        ),
        (
            "import-schemas",
            "POST",
            "/schemas/import",
            Some((Cluster, "kafka-cluster", Write)),
        ),
        ("get-import-unmapped", "GET", "/schemas/import", None),
        ("put-import-unmapped", "PUT", "/schemas/import", None),
        (
            "schema-types",
            "GET",
            "/schemas/types",
            Some((Cluster, "kafka-cluster", Describe)),
        ),
        (
            "alter-global-mode",
            "PUT",
            "/mode",
            Some((Cluster, "kafka-cluster", Alter)),
        ),
        (
            "alter-subject-mode",
            "PUT",
            "/mode/s",
            Some((Topic, "s", Alter)),
        ),
        (
            "describe-subject-mode",
            "GET",
            "/mode/s",
            Some((Topic, "s", Describe)),
        ),
        (
            "list-versions",
            "GET",
            "/subjects/s/versions",
            Some((Topic, "s", Read)),
        ),
        (
            "schema-id-versions",
            "GET",
            "/schemas/ids/1/versions",
            Some((Cluster, "kafka-cluster", Read)),
        ),
        (
            "percent-decoded-subject",
            "POST",
            "/subjects/foo%2Fbar/versions",
            Some((Topic, "foo/bar", Write)),
        ),
        ("root-unmapped", "GET", "/", None),
    ];

    #[test]
    fn route_mappings_are_named_and_table_driven() {
        for (_name, method, path, expected) in ROUTE_MAPPING_CASES {
            let expected = expected.map(|(resource_type, resource_name, operation)| {
                (resource_type, resource_name.to_string(), operation)
            });
            assert2::assert!(t(method, path) == expected);
        }
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
    fn authorize_decisions_are_named_and_table_driven() {
        for (
            _name,
            super_users,
            enabled,
            acls,
            principal,
            resource_type,
            resource_name,
            operation,
            expected,
        ) in [
            (
                "matching-acl",
                HashSet::new(),
                true,
                vec![alice_write_s()],
                alice(),
                ResourceType::Topic,
                "s",
                AclOperation::Write,
                true,
            ),
            (
                "different-subject",
                HashSet::new(),
                true,
                vec![alice_write_s()],
                alice(),
                ResourceType::Topic,
                "other",
                AclOperation::Write,
                false,
            ),
            (
                "super-user-without-acl",
                ["alice".to_string()].into_iter().collect(),
                true,
                Vec::new(),
                alice(),
                ResourceType::Topic,
                "s",
                AclOperation::Write,
                true,
            ),
            (
                "disabled-topic",
                HashSet::new(),
                false,
                Vec::new(),
                alice(),
                ResourceType::Topic,
                "s",
                AclOperation::Write,
                true,
            ),
            (
                "disabled-cluster",
                HashSet::new(),
                false,
                Vec::new(),
                crate::auth::anonymous(),
                ResourceType::Cluster,
                "kafka-cluster",
                AclOperation::Describe,
                true,
            ),
        ] {
            let authz = with_acls(super_users, enabled, acls);
            assert2::assert!(
                authz.authorize(&principal, &host(), resource_type, resource_name, operation,)
                    == expected
            );
        }
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

    /// A router with `authz_layer` over `az`. It exposes `/` with no authz
    /// target, `GET /subjects` for cluster Describe, and
    /// `POST /subjects/{s}/versions` for topic Write. The handler echoes `ok`,
    /// so a pass-through is `200`.
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
    async fn middleware_statuses_are_named_and_table_driven() {
        for (_name, matching_acl, method, path, forwarded, expected) in [
            (
                "deny-without-acl",
                false,
                "POST",
                "/subjects/orders-value/versions",
                false,
                StatusCode::FORBIDDEN,
            ),
            (
                "allow-matching-acl",
                true,
                "POST",
                "/subjects/orders-value/versions",
                false,
                StatusCode::OK,
            ),
            ("ungated-root", false, "GET", "/", false, StatusCode::OK),
            (
                "trusted-forward",
                false,
                "POST",
                "/subjects/orders-value/versions",
                true,
                StatusCode::OK,
            ),
        ] {
            let acls = matching_acl.then(|| AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: "orders-value".into(),
                pattern_type: PatternType::Literal,
                principal: "User:ANONYMOUS".into(),
                host: "*".into(),
                operation: AclOperation::Write,
                permission_type: PermissionType::Allow,
            });
            let app = authz_app(with_acls(HashSet::new(), true, acls.into_iter().collect()));
            let mut request = Request::builder().method(method).uri(path);
            if forwarded {
                request = request.header(crate::rest::forward::FORWARD_HEADER, "ingress-node");
            }
            let response = app
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert2::assert!(response.status() == expected);
        }
    }
}

//! Shared Kafka-ACL authorization evaluator (broker + gateway).
//!
//! Holds the [`Authorizer`] trait + ACL evaluator ([`SimpleAclAuthorizer`] /
//! [`AllowAllAuthorizer`]) plus an [`AclSource`] abstraction so one evaluator
//! serves both the broker (a `MetadataImage` snapshot) and the gateway (an
//! [`AclCache`] over a `Vec<AclEntry>` fetched via `DescribeAcls`). The decision
//! logic (super-user bypass, deny-wins, operation implication) lives here once
//! so the two callers can never drift.
//!
//! ## Authorizing a request
//!
//! ```rust
//! use crabka_authz::{AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer};
//! use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
//! use crabka_security::{AuthMethod, Principal};
//! use std::net::SocketAddr;
//! use uuid::Uuid;
//!
//! let image = MetadataImage::new(Uuid::nil());
//! let principal = Principal {
//!     name: "alice".into(),
//!     auth_method: AuthMethod::SaslPlain,
//!     groups: vec![],
//! };
//! let host: SocketAddr = "127.0.0.1:9092".parse().unwrap();
//! let req = AuthorizationRequest {
//!     principal: &principal,
//!     host: &host,
//!     resource_type: ResourceType::Topic,
//!     resource_name: "orders",
//!     operation: AclOperation::Read,
//! };
//!
//! assert_eq!(
//!     AllowAllAuthorizer.authorize(&image, &req),
//!     AuthorizationResult::Allow,
//! );
//! ```
#![forbid(unsafe_code)]

mod allow_all;
pub mod cache;
#[cfg(test)]
mod precedence;
mod simple;
mod source;

pub use allow_all::AllowAllAuthorizer;
pub use cache::AclCache;
pub use simple::SimpleAclAuthorizer;
pub use source::AclSource;

use std::net::SocketAddr;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;

/// What `authorize` is being asked: which principal wants to do which
/// operation on which resource, from which host. References are borrowed
/// so handler-side construction is allocation-free.
#[derive(Debug, Clone)]
pub struct AuthorizationRequest<'a> {
    pub principal: &'a Principal,
    pub host: &'a SocketAddr,
    pub resource_type: ResourceType,
    pub resource_name: &'a str,
    pub operation: AclOperation,
}

/// Binary outcome — Kafka's ACL surface is allow/deny; intermediate
/// states (e.g. "not yet decided") aren't exposed at the trait boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationResult {
    Allow,
    Deny,
}

/// Pluggable per-broker / per-gateway authorization decision point.
/// Implementations own whatever state they need to render a decision
/// (super-user set, HTTP client, decision cache) and the caller holds a
/// single `Arc<dyn Authorizer>`.
///
/// Implementations MUST be `Send + Sync + Debug`: handler code paths
/// are async and the broker logs configs at startup.
///
/// The decision consults a [`AclSource`] — the broker passes its
/// `MetadataImage`, the gateway passes an [`AclCache`]; ACL-free impls
/// (`AllowAll`, OPA) ignore it.
pub trait Authorizer: Send + Sync + std::fmt::Debug {
    /// Decide whether `req.principal` may perform `req.operation` on
    /// `(req.resource_type, req.resource_name)` from `req.host`. The
    /// authorizer is free to consult `source` (for ACL-backed impls)
    /// or ignore it entirely (`AllowAll`, `Opa`).
    fn authorize(
        &self,
        source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult;
}

/// Batch-authorize a set of topic names against the same principal /
/// host / operation. Used by `Produce`, `Fetch`, and `Metadata`
/// per-topic enforcement. The returned map's keys are borrowed from
/// the input iterator so callers can avoid copying topic strings.
// Batch entry point for per-topic enforcement. skip_all keeps the borrowed
// principal/host out of span fields; only the shared operation + principal
// name are recorded. Each inner `authorize` opens its own child span, so this
// is a batch-level span, not a per-entry loop span.
#[must_use]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        principal = %principal.name,
        operation = ?operation,
        host = %host.ip(),
    )
)]
pub fn authorize_topics<'a>(
    authorizer: &dyn Authorizer,
    source: &dyn AclSource,
    principal: &Principal,
    host: &SocketAddr,
    operation: AclOperation,
    topic_names: impl IntoIterator<Item = &'a str>,
) -> std::collections::HashMap<&'a str, AuthorizationResult> {
    topic_names
        .into_iter()
        .map(|name| {
            let req = AuthorizationRequest {
                principal,
                host,
                resource_type: ResourceType::Topic,
                resource_name: name,
                operation,
            };
            (name, authorizer.authorize(source, &req))
        })
        .collect()
}

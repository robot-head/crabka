//! Shared Kafka-ACL authorization evaluator (broker + gateway).
//!
//! This crate holds the [`Authorizer`] trait, the ACL evaluator
//! ([`SimpleAclAuthorizer`] and [`AllowAllAuthorizer`]), and an [`AclSource`]
//! abstraction. One evaluator therefore serves both the broker and the gateway.
//! The broker passes a `MetadataImage` snapshot. The gateway passes an
//! [`AclCache`] over a `Vec<AclEntry>` that it fetched with `DescribeAcls`.
//!
//! The decision logic lives here once, so the two callers can never drift. That
//! logic covers the super-user bypass, deny-wins, and operation implication.
//!
//! ## Authorizing a request
//!
//! ```rust
//! use std::net::SocketAddr;
//!
//! use crabka_authz::{AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer};
//! use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
//! use crabka_security::{AuthMethod, Principal};
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

use std::net::SocketAddr;

pub use allow_all::AllowAllAuthorizer;
pub use cache::AclCache;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;
pub use simple::SimpleAclAuthorizer;
pub use source::AclSource;

/// What the caller asks `authorize`: which principal wants to do which
/// operation on which resource, and from which host.
///
/// The struct borrows its references, so handler-side construction is
/// allocation-free.
#[derive(Debug, Clone)]
pub struct AuthorizationRequest<'a> {
    pub principal: &'a Principal,
    pub host: &'a SocketAddr,
    pub resource_type: ResourceType,
    pub resource_name: &'a str,
    pub operation: AclOperation,
}

/// Binary outcome: Kafka's ACL surface is allow or deny.
///
/// The trait boundary does not expose intermediate states, for example "not yet
/// decided".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationResult {
    Allow,
    Deny,
}

/// Pluggable per-broker or per-gateway authorization decision point.
///
/// Implementations own whatever state they need to make a decision, for example
/// a super-user set, an HTTP client, or a decision cache. The caller holds a
/// single `Arc<dyn Authorizer>`.
///
/// Implementations MUST be `Send + Sync + Debug`: handler code paths
/// are async and the broker logs configs at startup.
///
/// The decision consults a [`AclSource`]. The broker passes its
/// `MetadataImage` and the gateway passes an [`AclCache`]. ACL-free
/// implementations (`AllowAll`, OPA) ignore it.
pub trait Authorizer: Send + Sync + std::fmt::Debug {
    /// Decide whether `req.principal` may do `req.operation` on
    /// `(req.resource_type, req.resource_name)` from `req.host`.
    ///
    /// The authorizer may consult `source`, as the ACL-backed implementations
    /// do, or ignore it entirely, as `AllowAll` and `Opa` do.
    fn authorize(
        &self,
        source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult;
}

/// Batch-authorize a set of topic names against the same principal, host, and
/// operation.
///
/// The `Produce`, `Fetch`, and `Metadata` per-topic enforcement paths call this
/// function. The returned map borrows its keys from the input iterator, so
/// callers can avoid a copy of the topic strings.
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

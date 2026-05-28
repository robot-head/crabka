//! Pluggable cluster authorizer. Single [`Authorizer`] trait
//! with three impls ([`AllowAllAuthorizer`], [`SimpleAclAuthorizer`],
//! [`opa::OpaAuthorizer`]); the broker holds one boxed instance
//! configured via `[authorization]` in `broker.toml`.
//!
//! The authorizer is an explicit type: the default is
//! [`AllowAllAuthorizer`], so the "no config = allow everything"
//! behavior is spelled out rather than hiding inside the ACL impl.

mod allow_all;
pub mod opa;
mod simple_acl;

pub use allow_all::AllowAllAuthorizer;
pub use simple_acl::SimpleAclAuthorizer;

use std::net::SocketAddr;

use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
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

/// Pluggable per-broker authorization decision point. Implementations
/// own whatever state they need to render a decision (super-user set,
/// HTTP client, decision cache) and the broker holds a single
/// `Arc<dyn Authorizer>` in [`crate::BrokerConfig`].
///
/// Implementations MUST be `Send + Sync + Debug`: handler code paths
/// are async and the broker logs configs at startup.
pub trait Authorizer: Send + Sync + std::fmt::Debug {
    /// Decide whether `req.principal` may perform `req.operation` on
    /// `(req.resource_type, req.resource_name)` from `req.host`. The
    /// authorizer is free to consult `image` (for ACL-backed impls)
    /// or ignore it entirely (`AllowAll`, `Opa`).
    fn authorize(
        &self,
        image: &MetadataImage,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult;
}

/// Batch-authorize a set of topic names against the same principal /
/// host / operation. Used by `Produce`, `Fetch`, and `Metadata`
/// per-topic enforcement. The returned map's keys are borrowed from
/// the input iterator so callers can avoid copying topic strings.
#[must_use]
pub fn authorize_topics<'a>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
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
            (name, authorizer.authorize(image, &req))
        })
        .collect()
}

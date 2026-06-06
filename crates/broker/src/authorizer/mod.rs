//! Cluster authorizer. The trait + ACL evaluator now live in `crabka-authz`
//! (shared with the gateway); this module re-exports them and keeps the
//! broker-only OPA plugin.
pub mod opa;

pub use crabka_authz::{
    AclSource, AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer,
    SimpleAclAuthorizer, authorize_topics,
};

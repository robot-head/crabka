//! Shared in-crate scaffolding for the per-handler `#[cfg(test)] mod tests`
//! modules.
//!
//! The mutant-hardening pass (#713) copied the same helper set into ~40
//! handler test modules: a deny-everything authorizer, principal, peer, and
//! request-context builders, wire codec helpers, and a temp-dir broker
//! launcher. This module holds one copy of each. Handlers keep only thin,
//! behaviour-specific facades over them. Their own principal name, client id,
//! `BrokerConfig` tweaks, and negotiated wire version live at the call site, so
//! this module centralises no behaviour that a handler needs to vary.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use crabka_protocol::{Decode, Encode};
use crabka_security::{AuthMethod, Principal};

use crate::{
    broker::{Broker, BrokerHandle},
    config::BrokerConfig,
    handlers::RequestContext,
};

/// Authorizer that denies every request. It drives the authorization-failure
/// path in every handler that consults the cluster authorizer.
#[derive(Debug)]
pub(crate) struct DenyAll;

impl crate::authorizer::Authorizer for DenyAll {
    fn authorize(
        &self,
        _source: &dyn crabka_authz::AclSource,
        _req: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        crate::authorizer::AuthorizationResult::Deny
    }
}

/// Build an anonymous-auth [`Principal`] with the given name and no groups.
///
/// The name matters. Authorization decisions and audit records key on this
/// subject, so each handler passes the identity its scenario expects, such as
/// `"alice"`, `"admin"`, or `"ANONYMOUS"`.
pub(crate) fn principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    }
}

/// The loopback peer address (`127.0.0.1:9092`) that handler tests attribute
/// requests to.
pub(crate) fn peer() -> SocketAddr {
    "127.0.0.1:9092".parse().unwrap()
}

/// Build a [`RequestContext`] over the given principal, peer, and client id.
///
/// The remaining fields are the plaintext, non-sendfile defaults that every
/// handler test shares. `client_id` is a parameter because it feeds
/// client-quota lookups and therefore varies per handler.
pub(crate) fn request_context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
    client_id: &'a str,
) -> RequestContext<'a> {
    RequestContext {
        principal,
        peer,
        client_id,
        connection_id: "test-connection",
        sendfile_capable: false,
        connection_listener_name: "PLAINTEXT",
    }
}

/// Encode a request to wire bytes at `version`.
pub(crate) fn encode_request<T: Encode>(req: &T, version: i16) -> Bytes {
    let mut buf = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut buf, version).expect("encode request");
    buf.freeze()
}

/// Decode a response from `bytes` at `version`, and assert that the decoder
/// consumed every byte.
pub(crate) fn decode_response<T: Decode<'static>>(bytes: &Bytes, version: i16) -> T {
    let mut cur: &[u8] = bytes.as_ref();
    let resp = T::decode(&mut cur, version).expect("decode response");
    assert!(cur.is_empty(), "response decoder consumed all bytes");
    resp
}

/// Start an in-process broker over a fresh temp dir. It applies `configure` to
/// the [`BrokerConfig::for_tests`] baseline before start.
///
/// Each handler passes a closure with exactly the config tweaks it needs, such
/// as an authorizer to install, an `audit_enabled` toggle, or share and streams
/// groups to enable. The returned [`tempfile::TempDir`] must outlive the
/// broker.
pub(crate) fn start_broker_with(
    configure: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)> {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    configure(&mut cfg);
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    })
}

/// Start an in-process broker with only its authorizer swapped in.
///
/// This is the most common `start_broker` shape across handler test modules;
/// see [`wire_helpers`]. A handler test drives an authorization-failure
/// path with [`DenyAll`] or a custom authorizer, and it otherwise takes
/// the `for_tests` defaults.
pub(crate) async fn start_broker_with_authorizer(
    authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| cfg.authorizer = authorizer).await
}

/// Like [`start_broker_with_authorizer`], but it also disables audit logging.
///
/// This is the second most common `start_broker` shape. Admin-handler tests
/// that do not exercise the audit path swap the authorizer and turn
/// `audit_enabled` off, so that audit-log assertions elsewhere in the suite
/// stay stable.
pub(crate) async fn start_broker_with_authorizer_no_audit(
    authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
    })
    .await
}

/// Generate the `encode_request` / `decode_response` / `test_context`
/// wrapper trio that every handler's `#[cfg(test)] mod handler_tests` binds
/// over [`encode_request`], [`decode_response`], and [`request_context`].
///
/// Two forms:
///
/// - `wire_helpers!(ReqTy, RespTy, version = V, client_id = "id")`: for
///   handlers that always drive one fixed wire version.
/// - `wire_helpers!(ReqTy, RespTy, client_id = "id")`: for handlers whose
///   tests vary `version` per call, for version-negotiation behaviour.
macro_rules! wire_helpers {
    ($req:ty, $resp:ty, version = $version:expr, client_id = $client_id:expr) => {
        fn encode_request(req: &$req) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, $version)
        }

        fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }

        fn test_context<'a>(
            principal: &'a crabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
    ($req:ty, $resp:ty, client_id = $client_id:expr) => {
        fn encode_request(req: &$req, version: i16) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, version)
        }

        fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }

        fn test_context<'a>(
            principal: &'a crabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
}
pub(crate) use wire_helpers;

/// Like [`wire_helpers`], but for handlers whose `handle()` takes an
/// already-typed request, so there is nothing to encode, and returns wire
/// `Bytes`. Only `decode_response` and `test_context` are needed.
macro_rules! response_helpers {
    ($resp:ty, version = $version:expr, client_id = $client_id:expr) => {
        fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }

        fn test_context<'a>(
            principal: &'a crabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
    ($resp:ty, client_id = $client_id:expr) => {
        fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }

        fn test_context<'a>(
            principal: &'a crabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
}
pub(crate) use response_helpers;

/// Like [`wire_helpers`], but for handlers that take no [`RequestContext`],
/// so there is no `test_context` to generate. It generates only
/// `encode_request` and `decode_response`.
macro_rules! codec_helpers {
    ($req:ty, $resp:ty, version = $version:expr) => {
        fn encode_request(req: &$req) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, $version)
        }

        fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }
    };
    ($req:ty, $resp:ty) => {
        fn encode_request(req: &$req, version: i16) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, version)
        }

        fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }
    };
}
pub(crate) use codec_helpers;

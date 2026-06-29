//! `ListTransactions` (`api_key=66`, KIP-664). Admin RPC that returns
//! a summary of every transaction the broker's coordinator is currently
//! tracking — `(transactional_id, producer_id, state)` triples — with
//! optional state / producer-id filters.
//!
//! ## ACL
//!
//! Per-tid `Describe` on `TransactionalId(name)`. Entries the principal
//! can't describe are silently filtered out (matches the JVM behavior).
//! Cluster-wide auth isn't required — the JVM allows un-credentialed
//! listing of "transactions you can describe."
//!
//! ## State strings
//!
//! The wire field is a string. Crabka's [`crate::txn::state::TxnState`]
//! enum already matches the JVM names verbatim (`Empty`, `Ongoing`, ...)
//! so the mapping is trivial.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::list_transactions_request::ListTransactionsRequest;
use crabka_protocol::owned::list_transactions_response::{
    ListTransactionsResponse, TransactionState,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::txn::state::TxnState;

/// JVM-canonical string form of a Crabka [`TxnState`]. Matches the names
/// the JVM coordinator emits on `TransactionState.toString()`.
fn txn_state_str(s: TxnState) -> &'static str {
    match s {
        TxnState::Empty => "Empty",
        TxnState::Ongoing => "Ongoing",
        TxnState::PrepareCommit => "PrepareCommit",
        TxnState::PrepareAbort => "PrepareAbort",
        TxnState::CompleteCommit => "CompleteCommit",
        TxnState::CompleteAbort => "CompleteAbort",
        TxnState::Dead => "Dead",
    }
}

#[tracing::instrument(
    name = "handle_list_transactions",
    level = "info",
    skip_all,
    fields(api = "ListTransactions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = ListTransactionsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Snapshot every coordinator-local txn entry.
    let entries = broker.txn_coordinator.snapshot().await;

    let state_filter: std::collections::HashSet<String> =
        req.state_filters.iter().cloned().collect();
    let pid_filter: std::collections::HashSet<i64> =
        req.producer_id_filters.iter().copied().collect();

    // KIP-664: if filtered states include a string the broker doesn't
    // recognize, surface it in `unknown_state_filters` so the client
    // knows its filter is overly conservative.
    let known_states: std::collections::HashSet<&'static str> = [
        "Empty",
        "Ongoing",
        "PrepareCommit",
        "PrepareAbort",
        "CompleteCommit",
        "CompleteAbort",
        "Dead",
    ]
    .into_iter()
    .collect();
    let unknown_state_filters: Vec<String> = req
        .state_filters
        .iter()
        .filter(|s| !known_states.contains(s.as_str()))
        .cloned()
        .collect();

    let mut out: Vec<TransactionState> = Vec::with_capacity(entries.len());
    for entry in entries {
        let state = txn_state_str(entry.state);

        // State filter: empty = no filter; otherwise the entry's state
        // must be one of the requested ones.
        if !state_filter.is_empty() && !state_filter.contains(state) {
            continue;
        }
        // Producer-id filter: same semantics — empty means no filter.
        if !pid_filter.is_empty() && !pid_filter.contains(&entry.producer_id) {
            continue;
        }
        // ACL: per-tid `Describe` on `TransactionalId`. Silent filter on
        // Deny.
        let allow = broker.config.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::TransactionalId,
                resource_name: entry.transactional_id.as_str(),
                operation: AclOperation::Describe,
            },
        );
        if allow == AuthorizationResult::Deny {
            continue;
        }

        out.push(TransactionState {
            transactional_id: entry.transactional_id.clone(),
            producer_id: entry.producer_id,
            transaction_state: state.to_string(),
            ..Default::default()
        });
    }

    let resp = ListTransactionsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        unknown_state_filters,
        transaction_states: out,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::Authorizer;
    use crate::broker::{Broker, BrokerHandle};
    use crate::config::BrokerConfig;

    #[test]
    fn txn_state_str_matches_jvm_names() {
        assert!(txn_state_str(TxnState::Empty) == "Empty");
        assert!(txn_state_str(TxnState::Ongoing) == "Ongoing");
        assert!(txn_state_str(TxnState::PrepareCommit) == "PrepareCommit");
        assert!(txn_state_str(TxnState::PrepareAbort) == "PrepareAbort");
        assert!(txn_state_str(TxnState::CompleteCommit) == "CompleteCommit");
        assert!(txn_state_str(TxnState::CompleteAbort) == "CompleteAbort");
        assert!(txn_state_str(TxnState::Dead) == "Dead");
    }

    fn encode_request(req: &ListTransactionsRequest, version: i16) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes, version: i16) -> ListTransactionsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = ListTransactionsResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:9092".parse().unwrap()
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "admin-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    #[tokio::test]
    async fn handler_reports_unknown_state_filters_and_top_level_fields() {
        let version = crabka_protocol::owned::list_transactions_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = ListTransactionsRequest {
            state_filters: vec!["Ongoing".into(), "MysteryState".into()],
            producer_id_filters: vec![42],
            duration_filter: -1,
            transactional_id_pattern: Some("txn-*".into()),
            ..Default::default()
        };
        let req = encode_request(&req, version);

        let bytes = handle(&broker, version, 123, &req, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes, version);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::NONE);
        assert!(resp.unknown_state_filters == vec!["MysteryState"]);
        assert!(resp.transaction_states.is_empty());
        broker_handle.shutdown().await;
    }
}

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

/// Every transaction state the coordinator can report. Filter strings outside
/// this set (via [`txn_state_str`]) are echoed back in the KIP-664
/// `unknown_state_filters` response field.
const ALL_TXN_STATES: [TxnState; 7] = [
    TxnState::Empty,
    TxnState::Ongoing,
    TxnState::PrepareCommit,
    TxnState::PrepareAbort,
    TxnState::CompleteCommit,
    TxnState::CompleteAbort,
    TxnState::Dead,
];

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
    let known_states: std::collections::HashSet<&'static str> =
        ALL_TXN_STATES.into_iter().map(txn_state_str).collect();
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
    use std::sync::Arc;

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;
    use crate::test_support::{peer, principal};

    #[test]
    fn txn_state_str_matches_jvm_names() {
        let cases = [
            (TxnState::Empty, "Empty"),
            (TxnState::Ongoing, "Ongoing"),
            (TxnState::PrepareCommit, "PrepareCommit"),
            (TxnState::PrepareAbort, "PrepareAbort"),
            (TxnState::CompleteCommit, "CompleteCommit"),
            (TxnState::CompleteAbort, "CompleteAbort"),
            (TxnState::Dead, "Dead"),
        ];
        for (state, want) in cases {
            assert!(txn_state_str(state) == want, "{state:?}");
        }
    }

    crate::test_support::wire_helpers!(
        ListTransactionsRequest,
        ListTransactionsResponse,
        client_id = "admin-client"
    );

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

        let expected = ListTransactionsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            unknown_state_filters: vec!["MysteryState".to_string()],
            transaction_states: vec![],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}

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

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        list_transactions_request::ListTransactionsRequest,
        list_transactions_response::{ListTransactionsResponse, TransactionState},
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    txn::state::TxnState,
};

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
        // Producer-id filter: same semantics — empty means no filter. The
        // wire filter set is raw `i64`; unwrap the entry's `ProducerId` to match.
        if !pid_filter.is_empty() && !pid_filter.contains(&entry.producer_id.get()) {
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
            // Unwrap into the raw-`i64` wire field.
            producer_id: entry.producer_id.get(),
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
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::test_support::{
        peer, principal, start_broker_with_authorizer_no_audit as start_broker,
    };

    #[test]
    fn txn_state_str_matches_jvm_names() {
        let cases = [
            ("empty", TxnState::Empty, "Empty"),
            ("ongoing", TxnState::Ongoing, "Ongoing"),
            ("prepare commit", TxnState::PrepareCommit, "PrepareCommit"),
            ("prepare abort", TxnState::PrepareAbort, "PrepareAbort"),
            (
                "complete commit",
                TxnState::CompleteCommit,
                "CompleteCommit",
            ),
            ("complete abort", TxnState::CompleteAbort, "CompleteAbort"),
            ("dead", TxnState::Dead, "Dead"),
        ];
        for (case, state, want) in cases {
            assert!(txn_state_str(state) == want, "case: {case}; {state:?}");
        }
    }

    crate::test_support::wire_helpers!(
        ListTransactionsRequest,
        ListTransactionsResponse,
        client_id = "admin-client"
    );

    /// The producer-id filter keeps entries whose pid IS in the filter set.
    /// With a single seeded txn whose pid matches the filter, the entry is
    /// returned. Deleting the `!` in `!pid_filter.contains(..)` would invert
    /// the guard and drop the matching entry instead.
    #[tokio::test]
    async fn producer_id_filter_keeps_matching_pid() {
        use crabka_log::ProducerId;

        use crate::txn::state::TxnEntry;

        let version = crabka_protocol::owned::list_transactions_response::MAX_VERSION;
        let (broker_handle, dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();

        // Materialize the __transaction_state partition this tid hashes to so
        // the coordinator can persist the seeded entry.
        let tid = "txn-list-pid-filter";
        let coord = &broker.txn_coordinator;
        let p = coord.partition_for(tid);
        let part_dir =
            crate::log_dir::partition_dir(dir.path(), crate::txn::bootstrap::TOPIC, p.get());
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = crabka_log::Log::open(&part_dir, crabka_log::LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            crate::txn::bootstrap::TOPIC.to_string(),
            p,
            dir.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
        );
        broker
            .partitions
            .insert(crate::txn::bootstrap::TOPIC.to_string(), p, part);

        let entry = TxnEntry::new_empty(tid.to_string(), ProducerId(100), 0, 60_000, 0);
        coord
            .put(entry, crate::txn::version::TxnVersion::Classic)
            .await
            .expect("seed txn entry");

        let p_alice = principal("admin");
        let peer = peer();
        let ctx = test_context(&p_alice, &peer);
        // Filter on the seeded pid → the matching entry must be kept.
        let req = ListTransactionsRequest {
            producer_id_filters: vec![100],
            duration_filter: -1,
            ..Default::default()
        };
        let req = encode_request(&req, version);
        let bytes = handle(&broker, version, 123, &req, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes, version);

        let pids: Vec<i64> = resp
            .transaction_states
            .iter()
            .map(|s| s.producer_id)
            .collect();
        assert!(
            pids == vec![100],
            "pid filter must keep the matching entry, got {pids:?}"
        );
        broker_handle.shutdown().await;
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

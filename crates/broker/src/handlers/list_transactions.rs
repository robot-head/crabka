//! `ListTransactions` (`api_key=66`, KIP-664). Admin RPC that returns
//! a summary of every transaction the broker's coordinator is currently
//! tracking. Each summary is a `(transactional_id, producer_id, state)`
//! triple. The request can carry optional state, producer-id, duration, and
//! transactional-id-pattern filters.
//!
//! ## ACL
//!
//! Per-tid `Describe` on `TransactionalId(name)`. The handler silently
//! filters out entries the principal cannot describe, which matches the
//! JVM behavior. Cluster-wide auth is not necessary. The JVM allows
//! un-credentialed listing of "transactions you can describe."
//!
//! ## State strings
//!
//! The wire field is a string. Crabka's [`crate::txn::state::TxnState`]
//! enum already matches the JVM names verbatim (`Empty`, `Ongoing`, ...),
//! so the mapping is direct.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        list_transactions_request::ListTransactionsRequest,
        list_transactions_response::{ListTransactionsResponse, TransactionState},
    },
};
use java_regex::{PatternSyntaxError, Regex};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    txn::state::TxnState,
};

/// Every transaction state the coordinator can report. The handler compares
/// filter strings against this set with [`txn_state_str`]. It echoes any
/// filter string outside the set back in the KIP-664
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

/// JVM-canonical string form of a Crabka [`TxnState`]. These names match the
/// names the JVM coordinator emits on `TransactionState.toString()`.
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

/// Kafka's duration filter is a strict lower bound: a transaction whose age
/// equals the filter is excluded. Any negative value disables the filter.
fn matches_duration_filter(start_ms: i64, now_ms: i64, duration_filter: i64) -> bool {
    duration_filter < 0 || now_ms.saturating_sub(start_ms) > duration_filter
}

/// Java's `Matcher.matches()` requires the pattern to match the complete
/// transactional id.
fn compile_transactional_id_pattern(
    pattern: Option<&str>,
) -> Result<Option<Regex>, PatternSyntaxError> {
    pattern
        .filter(|pattern| !pattern.is_empty())
        .map(Regex::new)
        .transpose()
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

    // v0/v1 decode this field as `None`; v2 null and empty values both disable
    // the filter. Kafka returns a top-level INVALID_REGULAR_EXPRESSION rather
    // than treating malformed syntax as a pattern that matches nothing.
    let transactional_id_pattern =
        match compile_transactional_id_pattern(req.transactional_id_pattern.as_deref()) {
            Ok(pattern) => pattern,
            Err(error) => {
                tracing::debug!(
                    pattern = req.transactional_id_pattern.as_deref().unwrap_or_default(),
                    %error,
                    "invalid ListTransactions transactional-id pattern"
                );
                return crate::handlers::encode_response(
                    &ListTransactionsResponse {
                        throttle_time_ms: 0,
                        error_code: codes::INVALID_REGULAR_EXPRESSION,
                        ..Default::default()
                    },
                    version,
                );
            }
        };

    let image = broker.controller.current_image();

    // Snapshot every coordinator-local txn entry.
    let entries = broker.txn_coordinator.snapshot().await;

    let state_filter: std::collections::HashSet<String> =
        req.state_filters.iter().cloned().collect();
    let pid_filter: std::collections::HashSet<i64> =
        req.producer_id_filters.iter().copied().collect();
    let now_ms = crate::txn::util::now_millis();

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
        // DurationFilter is present from v1. Decoding older versions supplies
        // the protocol default (-1), which disables this strict lower bound.
        if !matches_duration_filter(entry.start_ms, now_ms, req.duration_filter) {
            continue;
        }
        // TransactionalIdPattern is present from v2. Null/empty values compile
        // to `None`, while a non-empty pattern is a full-string match.
        if transactional_id_pattern
            .as_ref()
            .is_some_and(|pattern| !pattern.matches(&entry.transactional_id))
        {
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

    #[test]
    fn duration_filter_is_a_strict_lower_bound() {
        let now_ms = 10_000;
        let cases = [
            ("negative disables", 20_000, -2, true),
            ("older", 8_999, 1_000, true),
            ("equal", 9_000, 1_000, false),
            ("newer", 9_001, 1_000, false),
            ("future", 11_000, 0, false),
        ];

        for (name, start_ms, filter, expected) in cases {
            assert!(
                matches_duration_filter(start_ms, now_ms, filter) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn transactional_id_pattern_is_optional_and_matches_the_whole_id() {
        let cases = [
            (None, "anything", true),
            (Some(""), "anything", true),
            (Some("txn-.*"), "txn-alpha", true),
            (Some("txn-.*"), "prefix-txn-alpha", false),
            (Some("txn|txn-long"), "txn-long", true),
        ];

        for (pattern, transactional_id, expected) in cases {
            let compiled = compile_transactional_id_pattern(pattern).expect("valid pattern");
            let matches = compiled
                .as_ref()
                .is_none_or(|pattern| pattern.matches(transactional_id));
            assert!(
                matches == expected,
                "{pattern:?} against {transactional_id}"
            );
        }
        assert!(
            compile_transactional_id_pattern(Some("(?=txn-).*"))
                .unwrap()
                .unwrap()
                .matches("txn-alpha")
        );
        assert!(
            compile_transactional_id_pattern(Some(r"(txn)-\1"))
                .unwrap()
                .unwrap()
                .matches("txn-txn")
        );
        assert!(compile_transactional_id_pattern(Some("(unclosed")).is_err());
    }

    crate::test_support::wire_helpers!(
        ListTransactionsRequest,
        ListTransactionsResponse,
        client_id = "admin-client"
    );

    /// Seed one transaction and exercise the filters through their actual wire
    /// versions. This also protects the producer-id filter from being inverted.
    #[tokio::test]
    async fn filters_follow_wire_versions_and_keep_matching_entries() {
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
            false,
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

        let cases: [(i16, i64, Option<&str>, &[i64]); 4] = [
            // v0 omits both newer fields, even when the in-memory request sets
            // DurationFilter.
            (0, i64::MAX, None, &[100]),
            // v1 carries DurationFilter and this transaction is not old enough
            // to exceed the maximum threshold.
            (1, i64::MAX, None, &[]),
            // v2 carries TransactionalIdPattern and uses full-string matching.
            (2, -1, Some("txn-list-.*"), &[100]),
            (2, -1, Some("list"), &[]),
        ];
        for (version, duration_filter, pattern, expected_pids) in cases {
            let req = ListTransactionsRequest {
                producer_id_filters: vec![100],
                duration_filter,
                transactional_id_pattern: pattern.map(str::to_owned),
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
                .map(|state| state.producer_id)
                .collect();
            assert!(
                pids == expected_pids,
                "version {version}, pattern {pattern:?}"
            );
        }

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

    #[tokio::test]
    async fn invalid_pattern_is_rejected_only_when_present_on_the_wire() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        for (version, expected_error) in [(1, codes::NONE), (2, codes::INVALID_REGULAR_EXPRESSION)]
        {
            let req = encode_request(
                &ListTransactionsRequest {
                    transactional_id_pattern: Some("(unclosed".into()),
                    ..Default::default()
                },
                version,
            );
            let bytes = handle(&broker, version, 123, &req, &ctx)
                .await
                .expect("handle");
            let response = decode_response(&bytes, version);
            assert!(response.error_code == expected_error, "version {version}");
            assert!(response.transaction_states.is_empty(), "version {version}");
        }

        broker_handle.shutdown().await;
    }
}

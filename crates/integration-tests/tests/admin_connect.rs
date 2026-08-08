//! Integration test for the bootstrap walk in `AdminClient::connect`.
//!
//! When the first address in the list refuses connections, `connect` must go on
//! to the second address and succeed.
//!
//! This test adds to the predicate-level unit tests in `src/topics.rs`. Those
//! tests are `controller_endpoint_picks_broker_with_matching_node_id` and
//! `any_not_controller_predicate_matches_code_41`, and they lock the pure parts
//! of the `NOT_CONTROLLER` retry path. `round_trip.rs` covers the full retry
//! pipeline against a real broker: response, then reconnect, then resend.

#[path = "../../broker/tests/support/mod.rs"]
mod support;

use crabka_client_admin::AdminClient;

/// Spec test: `connect_walks_bootstrap_list`.
///
/// The test binds an ephemeral port and drops it. The address of that port
/// refuses TCP connects with `ECONNREFUSED`. The test then starts the
/// in-process broker and uses its address as the second bootstrap entry.
/// `AdminClient::connect` must skip the refused address and succeed against
/// the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_walks_bootstrap_list() {
    support::init_tracing();

    // First bootstrap: an address whose connects get ECONNREFUSED.
    let (refused_addrs, _) = support::bind_and_drop_ports(1).await;
    let refused = refused_addrs[0].to_string();

    // Second bootstrap: the real in-process broker.
    let proc = support::start().await;
    let real = proc.broker.listen_addr().to_string();

    let mut admin = AdminClient::connect(&[refused, real])
        .await
        .expect("second bootstrap should succeed even though first refuses");

    // Confirm it's a usable client by issuing a metadata request.
    let md = admin
        .metadata(&["nonexistent"])
        .await
        .expect("metadata request against second bootstrap should succeed");
    // `controller_id` is populated by the broker after Raft has a leader;
    // we don't assert on a specific value (singleton bootstrap → id 1)
    // because the meaningful signal is that the request round-tripped.
    let _ = md.controller_id;
}

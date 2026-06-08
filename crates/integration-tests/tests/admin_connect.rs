//! Integration test for `AdminClient::connect`'s bootstrap-walking
//! behavior: when the first address in the list refuses connections,
//! it must fall through to the second and succeed.
//!
//! This complements the predicate-level unit tests in
//! `src/topics.rs` (`controller_endpoint_picks_broker_with_matching_node_id`,
//! `any_not_controller_predicate_matches_code_41`) which lock the
//! pure pieces of the `NOT_CONTROLLER` retry path. The full retry
//! pipeline (response → reconnect → resend) is covered by
//! `round_trip.rs` against a real broker.

#[path = "../../broker/tests/support/mod.rs"]
mod support;

use crabka_client_admin::AdminClient;

/// Spec test: `connect_walks_bootstrap_list`.
///
/// Bind-and-drop an ephemeral port to obtain an address whose TCP
/// connects will be refused with `ECONNREFUSED`, then start the
/// in-process broker and use its address as the second bootstrap
/// entry. `AdminClient::connect` must skip the refused address and
/// succeed against the broker.
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

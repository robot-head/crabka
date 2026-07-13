// rustc 1.95 clippy::pedantic ICEs on these test files (an upstream bug
// in clippy's body-analysis pass that also bites `acl_handlers.rs` and
// `admin_handlers.rs`). Disable pedantic locally; the rest of the
// workspace still enforces the full pedantic gate.

//! KIP-429 batch-4 T4A: broker-side `JoinGroup` protocol-set negotiation tests.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::{
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    join_group_response::JoinGroupResponse,
};

// Kafka error codes consumed by these tests.
const ERR_NONE: i16 = 0;
const ERR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;

/// Boot a single-broker no-auth test cluster. Returns the handle, a
/// shared bootstrap address string, and the tempdir guard. Tests construct
/// per-member clients via `connect_client` because the broker processes
/// requests on a single TCP connection serially — racing concurrent
/// `JoinGroup` waits over one `Client` would deadlock the second member
/// behind the first member's `INITIAL_REBALANCE_DELAY` wait.
async fn start_broker() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let handle = Broker::start(config).await.expect("broker must start");
    let bootstrap = handle.listen_addr().to_string();
    (handle, bootstrap, tempdir)
}

/// Build a fresh `Client` (and therefore a fresh TCP connection) against
/// `bootstrap`. Each member in a racing test gets its own client.
async fn connect_client(bootstrap: &str, client_id: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .build()
        .await
        .expect("client build")
}

/// Construct a `JoinGroup` request proposing `protocols` (in caller order)
/// with `protocol_type` and `member_id`. `session_timeout` / `rebalance_timeout`
/// are short enough that a stuck rebalance fails the test quickly rather
/// than holding the runtime.
fn join_group_request(
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    protocols: &[(&str, &[u8])],
) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: protocol_type.to_string(),
        protocols: protocols
            .iter()
            .map(|(name, meta)| JoinGroupRequestProtocol {
                name: (*name).to_string(),
                metadata: Bytes::copy_from_slice(meta),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// First-round `JoinGroup` with empty `member_id`: the broker replies
/// `MEMBER_ID_REQUIRED (79)` and the broker-generated member id (KIP-394).
/// Asserts both and returns the member id for use in the second round.
async fn bootstrap_member_id(
    client: &Client,
    group_id: &str,
    protocol_type: &str,
    protocols: &[(&str, &[u8])],
) -> String {
    let req = join_group_request(group_id, "", protocol_type, protocols);
    let resp = client
        .send(req)
        .await
        .expect("first JoinGroup must round-trip");
    assert!(
        resp.error_code == ERR_MEMBER_ID_REQUIRED,
        "first JoinGroup (empty member_id) must return MEMBER_ID_REQUIRED (79), got {resp:?}"
    );
    assert!(
        !resp.member_id.is_empty(),
        "broker must return a non-empty generated member_id on MEMBER_ID_REQUIRED"
    );
    resp.member_id
}

/// Second-round `JoinGroup` with the broker-supplied member id. Returns the
/// raw response so the caller can assert on `error_code` / `protocol_name`.
async fn second_join(
    client: &Client,
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    protocols: &[(&str, &[u8])],
) -> JoinGroupResponse {
    let req = join_group_request(group_id, member_id, protocol_type, protocols);
    client
        .send(req)
        .await
        .expect("second JoinGroup must round-trip")
}

/// Two-step `JoinGroup` against an already-stable group (or single member):
/// bootstrap a member id then immediately join again. The second call
/// blocks for up to `INITIAL_REBALANCE_DELAY` (~3 s) before the broker
/// completes the rebalance and returns NONE. Returns the full response.
async fn full_join(
    client: &Client,
    group_id: &str,
    protocol_type: &str,
    protocols: &[(&str, &[u8])],
) -> JoinGroupResponse {
    let member_id = bootstrap_member_id(client, group_id, protocol_type, protocols).await;
    second_join(client, group_id, &member_id, protocol_type, protocols).await
}

/// Members A and B propose disjoint protocol lists (`range` only vs.
/// `cooperative-sticky` only). The intersection is empty, so the broker
/// must surface `INCONSISTENT_GROUP_PROTOCOL (23)` to at least one (and
/// in this implementation, both) racing members.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_intersection_returns_inconsistent_group_protocol() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-empty-intersection";

    // One TCP connection per racing member — see `start_broker` doc.
    let client_a = connect_client(&bootstrap, "member-a").await;
    let client_b = connect_client(&bootstrap, "member-b").await;

    // Bootstrap both member ids serially — these calls short-circuit on
    // MEMBER_ID_REQUIRED before any rebalance wait, so they're effectively
    // instantaneous.
    let member_a = bootstrap_member_id(&client_a, group_id, "consumer", &[("range", b"")]).await;
    let member_b = bootstrap_member_id(
        &client_b,
        group_id,
        "consumer",
        &[("cooperative-sticky", b"")],
    )
    .await;

    // Race the two second-round joins. Both enter PreparingRebalance,
    // both wake after the 3-s initial-rebalance-delay, both compute
    // `select_protocol` over the union of members and find None, both
    // return INCONSISTENT_GROUP_PROTOCOL.
    let group_a = group_id.to_string();
    let group_b = group_id.to_string();
    let join_a = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            second_join(
                &client_a,
                &group_a,
                &member_a,
                "consumer",
                &[("range", b"")],
            ),
        )
        .await
        .expect("member A second JoinGroup timed out")
    });
    let join_b = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            second_join(
                &client_b,
                &group_b,
                &member_b,
                "consumer",
                &[("cooperative-sticky", b"")],
            ),
        )
        .await
        .expect("member B second JoinGroup timed out")
    });

    let resp_a = join_a.await.expect("member A task panic");
    let resp_b = join_b.await.expect("member B task panic");
    handle.shutdown().await;

    // At least one (here: both) must surface INCONSISTENT_GROUP_PROTOCOL.
    // We accept either both-error or one-error-one-empty (the post-error
    // notify could in theory let the other proceed in a future refactor),
    // but assert the contract loud enough that any regression is obvious.
    assert!(
        resp_a.error_code == ERR_INCONSISTENT_GROUP_PROTOCOL
            || resp_b.error_code == ERR_INCONSISTENT_GROUP_PROTOCOL,
        "at least one of (A, B) must return INCONSISTENT_GROUP_PROTOCOL (23); got A={resp_a:?} B={resp_b:?}"
    );
}

/// Three members. A and B vote first for `cooperative-sticky`, C votes
/// first for `range`. Both names are in every member's list (intersection
/// is `{cooperative-sticky, range}`). `cooperative-sticky` wins 2-1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_picks_cooperative_when_majority() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-vote-cooperative";

    // One TCP connection per racing member.
    let client_a = connect_client(&bootstrap, "member-a").await;
    let client_b = connect_client(&bootstrap, "member-b").await;
    let client_c = connect_client(&bootstrap, "member-c").await;

    // Bootstrap all three member ids serially (fast, no waits).
    let member_a = bootstrap_member_id(
        &client_a,
        group_id,
        "consumer",
        &[("cooperative-sticky", b""), ("range", b"")],
    )
    .await;
    let member_b = bootstrap_member_id(
        &client_b,
        group_id,
        "consumer",
        &[("cooperative-sticky", b""), ("range", b"")],
    )
    .await;
    let member_c = bootstrap_member_id(
        &client_c,
        group_id,
        "consumer",
        &[("range", b""), ("cooperative-sticky", b"")],
    )
    .await;

    // Race the three second-round joins. They all need to land inside the
    // 3-s initial-rebalance-delay window of the first one so the broker
    // votes over the full membership. `tokio::spawn` + no inter-spawn
    // delay easily clears that bar.
    let g_a = group_id.to_string();
    let g_b = group_id.to_string();
    let g_c = group_id.to_string();
    let join_a = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            second_join(
                &client_a,
                &g_a,
                &member_a,
                "consumer",
                &[("cooperative-sticky", b""), ("range", b"")],
            ),
        )
        .await
        .expect("member A second JoinGroup timed out")
    });
    let join_b = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            second_join(
                &client_b,
                &g_b,
                &member_b,
                "consumer",
                &[("cooperative-sticky", b""), ("range", b"")],
            ),
        )
        .await
        .expect("member B second JoinGroup timed out")
    });
    let join_c = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            second_join(
                &client_c,
                &g_c,
                &member_c,
                "consumer",
                &[("range", b""), ("cooperative-sticky", b"")],
            ),
        )
        .await
        .expect("member C second JoinGroup timed out")
    });

    let resp_a = join_a.await.expect("member A task panic");
    let resp_b = join_b.await.expect("member B task panic");
    let resp_c = join_c.await.expect("member C task panic");
    handle.shutdown().await;

    for (label, resp) in [("A", &resp_a), ("B", &resp_b), ("C", &resp_c)] {
        assert!(
            resp.error_code == ERR_NONE,
            "member {label} must succeed, got {resp:?}"
        );
        assert!(
            resp.protocol_name.as_deref() == Some("cooperative-sticky"),
            "member {label} must see protocol_name=cooperative-sticky (2 votes vs 1 for range), got {resp:?}"
        );
    }
}

/// Two members, one vote each, both names in the intersection. The tie
/// must break lexicographically — `'c' < 'r'`, so `cooperative-sticky`
/// wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_ties_broken_lexicographically() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-tie";

    // One TCP connection per racing member.
    let client_a = connect_client(&bootstrap, "member-a").await;
    let client_b = connect_client(&bootstrap, "member-b").await;

    let member_a = bootstrap_member_id(
        &client_a,
        group_id,
        "consumer",
        &[("range", b""), ("cooperative-sticky", b"")],
    )
    .await;
    let member_b = bootstrap_member_id(
        &client_b,
        group_id,
        "consumer",
        &[("cooperative-sticky", b""), ("range", b"")],
    )
    .await;

    let g_a = group_id.to_string();
    let g_b = group_id.to_string();
    let join_a = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            second_join(
                &client_a,
                &g_a,
                &member_a,
                "consumer",
                &[("range", b""), ("cooperative-sticky", b"")],
            ),
        )
        .await
        .expect("member A second JoinGroup timed out")
    });
    let join_b = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            second_join(
                &client_b,
                &g_b,
                &member_b,
                "consumer",
                &[("cooperative-sticky", b""), ("range", b"")],
            ),
        )
        .await
        .expect("member B second JoinGroup timed out")
    });

    let resp_a = join_a.await.expect("member A task panic");
    let resp_b = join_b.await.expect("member B task panic");
    handle.shutdown().await;

    for (label, resp) in [("A", &resp_a), ("B", &resp_b)] {
        assert!(
            resp.error_code == ERR_NONE,
            "member {label} must succeed, got {resp:?}"
        );
        assert!(
            resp.protocol_name.as_deref() == Some("cooperative-sticky"),
            "tie must break lexicographically to cooperative-sticky ('c' < 'r'), got {resp:?}"
        );
    }
}

/// A single member proposing `[range]` lands on `range` — the trivial
/// sanity check that the negotiation primitive doesn't somehow break
/// the simplest case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_picks_its_first_protocol() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let client = connect_client(&bootstrap, "single-member").await;
    let resp = full_join(&client, "cg-single", "consumer", &[("range", b"")]).await;
    handle.shutdown().await;

    assert!(
        resp.error_code == ERR_NONE,
        "single-member JoinGroup must succeed, got {resp:?}"
    );
    assert!(
        resp.protocol_name.as_deref() == Some("range"),
        "single-member must land on its only proposed protocol, got {resp:?}"
    );
}

/// Member A establishes the group with `protocol_type = "consumer"`.
/// Member B then joins with `protocol_type = "stream"`. The broker must
/// reject B with `INCONSISTENT_GROUP_PROTOCOL` before it ever enters the
/// rebalance — the type mismatch check fires on the second-round join.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_type_mismatch_rejected() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let client_a = connect_client(&bootstrap, "member-a").await;
    let client_b = connect_client(&bootstrap, "member-b").await;
    let group_id = "cg-type-mismatch";

    // Member A: full two-step, completes the rebalance. After this the
    // group has `protocol_type = Some("consumer")` and is Stable.
    let resp_a = full_join(&client_a, group_id, "consumer", &[("range", b"")]).await;
    assert!(
        resp_a.error_code == ERR_NONE,
        "member A must complete first-round rebalance, got {resp_a:?}"
    );
    assert!(resp_a.protocol_name.as_deref() == Some("range"));

    // Member B: bootstrap a member id, then join with `protocol_type =
    // "stream"`. The handler checks the existing group's protocol_type
    // before any rebalance work, so this returns immediately.
    let member_b = bootstrap_member_id(&client_b, group_id, "stream", &[("range", b"")]).await;
    let resp_b = second_join(&client_b, group_id, &member_b, "stream", &[("range", b"")]).await;
    handle.shutdown().await;

    assert!(
        resp_b.error_code == ERR_INCONSISTENT_GROUP_PROTOCOL,
        "member B with protocol_type=stream must hit INCONSISTENT_GROUP_PROTOCOL on a consumer group, got {resp_b:?}"
    );
}

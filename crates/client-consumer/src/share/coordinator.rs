//! Background `ShareGroupHeartbeat` loop for a [`ShareConsumer`].
//!
//! Mirrors the classic [`coordinator::run`](crate::coordinator::run) shape:
//! an interval ticker plus a `tokio::select!` that races each heartbeat RPC
//! against a shutdown token so `close()` returns promptly. Each tick sends a
//! `ShareGroupHeartbeat` (API key 76) with the live member epoch; on success
//! it adopts the broker-returned epoch and (when present) the new assignment.
//!
//! Share-group membership has no Join/Sync handshake — the heartbeat *is* the
//! join. A from-scratch rejoin therefore just resets the member epoch to 0 and
//! re-sends `subscribed_topic_names`; the broker hands back a fresh epoch and
//! assignment on the next ok.

use std::{collections::HashMap, sync::Arc};

use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::Assignment,
    },
    primitives::uuid::Uuid as WireUuid,
};
use crabka_units::{Time, convert::TimeExt as _};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// `FENCED_MEMBER_EPOCH` — our epoch is behind the broker's; rejoin.
const FENCED_MEMBER_EPOCH: i16 = 110;
/// `UNKNOWN_MEMBER_ID` — the broker has forgotten us (session expired); rejoin.
const UNKNOWN_MEMBER_ID: i16 = 25;
/// `STALE_MEMBER_EPOCH` — same family as fenced; rejoin from scratch.
const STALE_MEMBER_EPOCH: i16 = 113;

/// State owned by the share-group heartbeat task.
///
/// The `Arc<Mutex<...>>` fields are shared with the parent [`ShareConsumer`]
/// so `poll()` sees the live member epoch / assignment as the broker rebalances
/// the group.
pub(crate) struct ShareCoordinatorState {
    pub client: Client,
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: Arc<Mutex<i32>>,
    /// Live assignment as `(topic_id, topic_name, partition)`.
    pub assignment: Arc<Mutex<Vec<(WireUuid, String, i32)>>>,
    pub topic_names: Arc<Mutex<HashMap<WireUuid, String>>>,
    pub subscribe: Vec<String>,
    pub heartbeat_interval: Time,
    pub leave_heartbeat_timeout: Time,
}

/// Outcome of a single `ShareGroupHeartbeat` RPC.
enum HeartbeatOutcome {
    /// `error_code == 0` — steady state.
    Ok,
    /// Fenced / unknown-member / stale-epoch — reset to epoch 0 and re-send the
    /// subscription on the next tick so the broker re-admits us.
    RejoinFromScratch,
    /// Transport error or unexpected non-fatal code; retry next tick.
    Transient,
}

fn build_leave_heartbeat_request(
    group_id: String,
    member_id: String,
) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id,
        member_id,
        member_epoch: -1,
        ..Default::default()
    }
}

fn build_heartbeat_request(
    group_id: String,
    member_id: String,
    member_epoch: i32,
    subscribe: Option<Vec<String>>,
) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id,
        member_id,
        member_epoch,
        subscribed_topic_names: subscribe,
        ..Default::default()
    }
}

fn heartbeat_result(error_code: i16) -> HeartbeatOutcome {
    if error_code == 0 {
        HeartbeatOutcome::Ok
    } else if is_rejoin_error(error_code) {
        HeartbeatOutcome::RejoinFromScratch
    } else {
        HeartbeatOutcome::Transient
    }
}

fn is_rejoin_error(error_code: i16) -> bool {
    matches!(
        error_code,
        FENCED_MEMBER_EPOCH | UNKNOWN_MEMBER_ID | STALE_MEMBER_EPOCH
    )
}

/// Drive the heartbeat loop until `shutdown` fires.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: long-running I/O event loop, exercised by integration tests
pub(crate) async fn run(state: ShareCoordinatorState, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(state.heartbeat_interval.to_std());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // After a fence/unknown-member we must re-send `subscribed_topic_names` on
    // the next heartbeat (the broker treats epoch 0 + subscription as a join).
    let mut rejoining = false;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }

        // Race the RPC against shutdown so `close()` is prompt even when the
        // broker is slow. The send is cancellation-safe: `Client` multiplexes
        // on correlation ids, so dropping an in-flight send only abandons its
        // pending response.
        tokio::select! {
            () = shutdown.cancelled() => break,
            outcome = heartbeat_once(&state, rejoining) => match outcome {
                HeartbeatOutcome::Ok => rejoining = false,
                HeartbeatOutcome::Transient => {}
                HeartbeatOutcome::RejoinFromScratch => {
                    *state.member_epoch.lock().await = 0;
                    rejoining = true;
                }
            },
        }
    }

    // Graceful departure: a leave heartbeat (`member_epoch = -1`) tells the
    // broker to evict us now rather than waiting out the session timeout.
    // Best-effort and bounded so a hung broker can't block `close()`.
    leave_group(&state).await;
}

async fn leave_group(state: &ShareCoordinatorState) {
    let leave = state.client.send(build_leave_heartbeat_request(
        state.group_id.clone(),
        state.member_id.clone(),
    ));
    let _ = tokio::time::timeout(state.leave_heartbeat_timeout.to_std(), leave).await;
}

/// Send one `ShareGroupHeartbeat` and translate the response into a directive.
///
/// When `rejoining` we re-send `subscribed_topic_names` (the broker requires
/// the subscription on a fresh join); otherwise we send `None` (the broker
/// remembers it across steady-state heartbeats).
#[tracing::instrument(
    name = "share_consumer.heartbeat",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id, rejoining)
)]
async fn heartbeat_once(state: &ShareCoordinatorState, rejoining: bool) -> HeartbeatOutcome {
    let epoch = *state.member_epoch.lock().await;
    let subscribed = if rejoining {
        Some(state.subscribe.clone())
    } else {
        None
    };
    let result = state
        .client
        .send(build_heartbeat_request(
            state.group_id.clone(),
            state.member_id.clone(),
            epoch,
            subscribed,
        ))
        .await;
    match result {
        Ok(r) => match heartbeat_result(r.error_code) {
            HeartbeatOutcome::Ok => {
                *state.member_epoch.lock().await = r.member_epoch;
                if let Some(assignment) = r.assignment {
                    update_assignment(state, assignment).await;
                }
                HeartbeatOutcome::Ok
            }
            HeartbeatOutcome::RejoinFromScratch => {
                tracing::warn!(
                    error_code = r.error_code,
                    "share heartbeat fenced; rejoining from epoch 0"
                );
                HeartbeatOutcome::RejoinFromScratch
            }
            HeartbeatOutcome::Transient => {
                tracing::warn!(
                    error_code = r.error_code,
                    "unexpected share heartbeat error"
                );
                HeartbeatOutcome::Transient
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "share heartbeat send failed");
            HeartbeatOutcome::Transient
        }
    }
}

/// Replace the shared assignment with the broker-returned topic/partition set,
/// resolving topic names from the cached `topic_names` map (topic ids the map
/// doesn't know yet fall back to the id's hex form so `poll()` still has a
/// stable display name; a later Metadata refresh fixes it).
async fn update_assignment(state: &ShareCoordinatorState, assignment: Assignment) {
    let names = state.topic_names.lock().await;
    let mut next: Vec<(WireUuid, String, i32)> = Vec::new();
    for tp in &assignment.topic_partitions {
        let name = names
            .get(&tp.topic_id)
            .cloned()
            .unwrap_or_else(|| hex_topic_id(tp.topic_id));
        for &partition in &tp.partitions {
            next.push((tp.topic_id, name.clone(), partition));
        }
    }
    drop(names);
    *state.assignment.lock().await = next;
}

/// Hex display for a topic id whose name we haven't resolved yet. `Uuid` has no
/// `Display`; this gives `poll()` a stable, non-empty placeholder name until a
/// Metadata refresh fills the real one in.
fn hex_topic_id(id: WireUuid) -> String {
    use std::fmt::Write as _;
    id.0.iter().fold(String::with_capacity(32), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::BytesMut;
    use crabka_client_core::MockBroker;
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
            share_group_heartbeat_request,
        },
        tagged_fields::UnknownTaggedFields,
    };

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
    }

    fn api_versions_for_share_leave() -> Vec<u8> {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: share_group_heartbeat_request::API_KEY,
                    min_version: share_group_heartbeat_request::MIN_VERSION,
                    max_version: share_group_heartbeat_request::MAX_VERSION,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buffer = BytesMut::new();
        response
            .encode(&mut buffer, 0)
            .expect("encode API versions");
        buffer.to_vec()
    }

    #[tokio::test]
    async fn leave_group_uses_configured_timeout_for_one_best_effort_request() {
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&requests);
        let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_for_share_leave())
            } else if api_key == share_group_heartbeat_request::API_KEY {
                observed.fetch_add(1, Ordering::SeqCst);
                None
            } else {
                panic!("unexpected API key {api_key}");
            }
        })
        .await;
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .client_id("share-leave-timeout-test")
            .request_timeout(Duration::from_secs(5))
            .build()
            .await
            .expect("client");
        let state = ShareCoordinatorState {
            client,
            group_id: "group-a".into(),
            member_id: "member-a".into(),
            member_epoch: Arc::new(Mutex::new(3)),
            assignment: Arc::new(Mutex::new(Vec::new())),
            topic_names: Arc::new(Mutex::new(HashMap::new())),
            subscribe: vec!["topic-a".into()],
            heartbeat_interval: crabka_units::secs(1),
            leave_heartbeat_timeout: crabka_units::millis(37),
        };

        tokio::time::timeout(Duration::from_secs(1), leave_group(&state))
            .await
            .expect("configured leave deadline bounds shutdown");

        mock.stop();
        assert2::assert!(requests.load(Ordering::SeqCst) == 1);
    }

    async fn state() -> ShareCoordinatorState {
        let mut names = HashMap::new();
        names.insert(id(7), "topic-a".to_string());
        ShareCoordinatorState {
            client: Client::builder()
                .bootstrap("127.0.0.1:1")
                .client_id("share-coordinator-test")
                .build()
                .await
                .unwrap(),
            group_id: "group-a".into(),
            member_id: "member-a".into(),
            member_epoch: Arc::new(Mutex::new(3)),
            assignment: Arc::new(Mutex::new(Vec::new())),
            topic_names: Arc::new(Mutex::new(names)),
            subscribe: vec!["topic-a".into()],
            heartbeat_interval: crabka_units::secs(1),
            leave_heartbeat_timeout: crabka_units::secs(5),
        }
    }

    #[test]
    fn heartbeat_requests_preserve_group_member_epoch_and_subscription() {
        let leave = build_leave_heartbeat_request("group-a".into(), "member-a".into());
        assert2::assert!(
            leave
                == ShareGroupHeartbeatRequest {
                    group_id: "group-a".into(),
                    member_id: "member-a".into(),
                    member_epoch: -1,
                    rack_id: None,
                    subscribed_topic_names: None,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );

        let heartbeat = build_heartbeat_request(
            "group-a".into(),
            "member-a".into(),
            4,
            Some(vec!["topic-a".into()]),
        );
        assert2::assert!(
            heartbeat
                == ShareGroupHeartbeatRequest {
                    group_id: "group-a".into(),
                    member_id: "member-a".into(),
                    member_epoch: 4,
                    rack_id: None,
                    subscribed_topic_names: Some(vec!["topic-a".into()]),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );
    }

    #[test]
    fn heartbeat_result_classifies_success_rejoin_and_transient_errors() {
        for (_name, code, expected) in [
            ("success", 0, HeartbeatOutcome::Ok),
            (
                "fenced epoch",
                FENCED_MEMBER_EPOCH,
                HeartbeatOutcome::RejoinFromScratch,
            ),
            (
                "unknown member",
                UNKNOWN_MEMBER_ID,
                HeartbeatOutcome::RejoinFromScratch,
            ),
            (
                "stale epoch",
                STALE_MEMBER_EPOCH,
                HeartbeatOutcome::RejoinFromScratch,
            ),
            ("transient", 17, HeartbeatOutcome::Transient),
        ] {
            assert2::assert!(
                std::mem::discriminant(&heartbeat_result(code))
                    == std::mem::discriminant(&expected)
            );
        }
    }

    #[tokio::test]
    async fn update_assignment_resolves_names_and_hex_fallbacks() {
        let state = state().await;
        update_assignment(
            &state,
            Assignment {
                topic_partitions: vec![
                    TopicPartitions {
                        topic_id: id(7),
                        partitions: vec![1],
                        ..Default::default()
                    },
                    TopicPartitions {
                        topic_id: id(9),
                        partitions: vec![2],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .await;

        let assignment = state.assignment.lock().await.clone();
        assert2::assert!(
            assignment
                == vec![
                    (id(7), "topic-a".into(), 1),
                    (id(9), hex_topic_id(id(9)), 2)
                ]
        );
    }

    #[test]
    fn hex_topic_id_formats_all_uuid_bytes() {
        assert2::assert!(hex_topic_id(id(9)) == "00000000000000000000000000000009");
    }
}

// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.
#![allow(clippy::pedantic)]

//! KIP-919 `UnregisterBroker` admin RPC (api_key 64). Drops a broker
//! registration record so subsequent `Metadata` responses no longer
//! advertise the broker's endpoints.

use assert2::assert;
mod support;

use crabka_protocol::owned::{
    metadata_request::MetadataRequest, unregister_broker_request::UnregisterBrokerRequest,
};

#[tokio::test]
async fn unregister_known_broker_drops_it_from_metadata() {
    let p = support::start().await;

    // Single-broker support cluster: this broker registers itself as
    // node_id = 1 via raft bootstrap. Confirm it's present before the
    // call.
    let resp = p
        .client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata (before unregister)");
    assert!(resp.brokers.len() == 1);
    assert!(resp.brokers[0].node_id == 1);

    let r = p
        .client
        .send(UnregisterBrokerRequest {
            broker_id: 1,
            ..Default::default()
        })
        .await
        .expect("UnregisterBroker");
    assert!(r.error_code == 0, "{r:?}");
    assert!(r.error_message.is_none() || r.error_message.as_deref() == Some(""));

    // The Raft commit may race the Metadata response; await the controller
    // image dropping broker 1 instead of polling the wire.
    p.broker.wait_for_image(|img| img.broker(1).is_none()).await;

    p.broker.shutdown().await;
}

#[tokio::test]
async fn unregister_unknown_broker_returns_invalid_request() {
    let p = support::start().await;

    let r = p
        .client
        .send(UnregisterBrokerRequest {
            broker_id: 999,
            ..Default::default()
        })
        .await
        .expect("UnregisterBroker");
    assert!(r.error_code == 42, "expected INVALID_REQUEST (42): {r:?}");
    assert!(
        r.error_message
            .as_deref()
            .is_some_and(|m| m.contains("999") && m.contains("not registered")),
        "error message must name the broker and say it isn't registered: {r:?}",
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn unregister_negative_broker_id_rejected() {
    let p = support::start().await;

    let r = p
        .client
        .send(UnregisterBrokerRequest {
            broker_id: -1,
            ..Default::default()
        })
        .await
        .expect("UnregisterBroker");
    assert!(r.error_code == 42, "expected INVALID_REQUEST (42): {r:?}");
    assert!(
        r.error_message
            .as_deref()
            .is_some_and(|m| m.contains("non-negative")),
        "error must explain the broker_id sign requirement: {r:?}",
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn unregister_is_idempotent_on_repeat_call() {
    let p = support::start().await;

    // First call: success.
    let r1 = p
        .client
        .send(UnregisterBrokerRequest {
            broker_id: 1,
            ..Default::default()
        })
        .await
        .expect("UnregisterBroker 1");
    assert!(r1.error_code == 0, "{r1:?}");

    // Wait for the unregister to commit: await the controller image
    // dropping broker 1 rather than polling the wire.
    p.broker.wait_for_image(|img| img.broker(1).is_none()).await;

    // Second call against the now-removed broker: surfaces
    // INVALID_REQUEST (existence check fails). The image apply itself
    // is idempotent so a stale concurrent re-submit wouldn't break
    // anything, but the handler's existence check makes the wire
    // contract explicit.
    let r2 = p
        .client
        .send(UnregisterBrokerRequest {
            broker_id: 1,
            ..Default::default()
        })
        .await
        .expect("UnregisterBroker 2");
    assert!(r2.error_code == 42, "{r2:?}");

    p.broker.shutdown().await;
}

//! KIP-848 / KIP-584: the next-gen consumer-group protocol
//! (`ConsumerGroupHeartbeat`, `api_key` 68) is gated on a finalized
//! `group.version >= 1`. A freshly-bootstrapped broker finalizes
//! `group.version=1` (so next-gen is enabled); downgrading it to 0 (a KIP-584
//! tombstone) disables next-gen, and the broker then rejects heartbeats with
//! `UNSUPPORTED_VERSION` so the client falls back to the classic protocol.

use assert2::assert;
mod support;

use crabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

const UNSUPPORTED_VERSION: i16 = 35;

fn heartbeat(group: &str) -> ConsumerGroupHeartbeatRequest {
    // A well-formed first (join) heartbeat: empty member_id, member_epoch 0,
    // a rebalance timeout, and a subscription. Mirrors the field set used in
    // consumer_group_next_gen.rs. The feature gate is the first check after
    // decode, so this reaches it regardless of whether the topic exists.
    ConsumerGroupHeartbeatRequest {
        group_id: group.into(),
        member_epoch: 0,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: Some(vec!["t".into()]),
        ..Default::default()
    }
}

#[tokio::test]
async fn next_gen_accepted_when_group_version_finalized() {
    // Fresh broker self-bootstraps group.version=1 → next-gen enabled.
    let p = support::start().await;
    let resp = p
        .client
        .send(heartbeat("gv-accept"))
        .await
        .expect("heartbeat");
    // The feature gate did NOT fire (would be UNSUPPORTED_VERSION=35). The
    // group may still be mid-rebalance / need a topic, but it must not be a
    // group.version gate rejection.
    assert!(
        resp.error_code != UNSUPPORTED_VERSION,
        "gate must pass at gv=1: {resp:?}"
    );
    p.broker.shutdown().await;
}

#[tokio::test]
async fn next_gen_rejected_when_group_version_disabled() {
    let p = support::start().await;

    // Downgrade group.version to 0 (KIP-584 tombstone) → next-gen disabled.
    let dg = p
        .client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "group.version".into(),
                max_version_level: 0,
                upgrade_type: 2, // SAFE_DOWNGRADE
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures downgrade");
    assert!(dg.error_code == 0, "downgrade to 0 should succeed: {dg:?}");

    // Now a heartbeat must be rejected with UNSUPPORTED_VERSION (classic fallback).
    let resp = p
        .client
        .send(heartbeat("gv-reject"))
        .await
        .expect("heartbeat");
    assert!(
        resp.error_code == UNSUPPORTED_VERSION,
        "next-gen must be rejected once group.version is disabled: {resp:?}"
    );
    p.broker.shutdown().await;
}

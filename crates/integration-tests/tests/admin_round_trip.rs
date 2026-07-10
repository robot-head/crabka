//! Integration test: spin up an in-process broker via the existing
//! `crates/broker/tests/support` harness, drive every admin RPC
//! through `AdminClient`, assert the visible cluster state.
//!
//! # Coverage map for `NOT_CONTROLLER` retry
//!
//! The full retry pipeline — first response carries `NOT_CONTROLLER`
//! (41) → admin client issues a fresh `Metadata` → reconnects to the
//! reported controller → re-sends the original RPC — can't be unit-
//! tested against `AdminClient` directly because it holds a concrete
//! `crabka_client_core::Connection` (no trait seam at the byte layer).
//! Building a Kafka-protocol fake server for ~3 RPCs is order-of-
//! magnitude more code than the retry itself.
//!
//! Instead, coverage is split:
//!
//! * **Predicate** — `src/topics.rs::tests::any_not_controller_*` lock
//!   the retry-eligibility check on code 41 only.
//! * **Endpoint resolver** — `src/topics.rs::tests::controller_endpoint_*`
//!   lock the `MetadataResponse` → `host:port` mapping the retry uses
//!   to pick a reconnect target.
//! * **Pipeline** — *this file*'s `admin_round_trip_create_alter_delete`
//!   exercises the end-to-end happy path against a real broker. In a
//!   singleton bootstrap the broker is always the controller so the
//!   retry path doesn't fire here, but the same code path that would
//!   retry on `NOT_CONTROLLER` is the one that succeeds without retry
//!   when the response is clean — i.e. the integration path through
//!   `parse_create_topics` / `parse_delete_topics` / `parse_create_partitions`
//!   is fully exercised.

use std::{collections::BTreeMap, time::Duration};

use assert2::check;
use crabka_client_admin::{AdminClient, CreatePartitionsOp, CreateTopicSpec, IncrementalAlterOp};

#[path = "../../broker/tests/support/mod.rs"]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_round_trip_create_alter_delete() {
    support::init_tracing();
    let proc = support::start().await;
    let bootstrap = proc.broker.listen_addr().to_string();

    let mut admin = AdminClient::connect(&[bootstrap]).await.unwrap();

    // 1. Topic doesn't exist initially.
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    assert2::assert!(foo.error.is_some());

    // 2. Create the topic with one config override.
    let configs = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: "foo".into(),
                partitions: 3,
                replicas: 1,
                configs,
            }],
            5_000,
        )
        .await
        .unwrap();
    assert2::assert!(outcomes.len() == 1);
    assert2::assert!(outcomes[0].error.is_none());

    // 3. Metadata reflects the create.
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    check!(foo.error.is_none());
    assert2::assert!(foo.partition_count == 3);
    assert2::assert!(foo.replication_factor == 1);

    // 4. Increase partitions to 5.
    let outcomes = admin
        .create_partitions(
            &[CreatePartitionsOp {
                name: "foo".into(),
                new_total_count: 5,
            }],
            5_000,
        )
        .await
        .unwrap();
    assert2::assert!(outcomes[0].error.is_none());
    // Bounded poll until metadata reflects the partition increase to 5.
    let md = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let md = admin.metadata(&["foo"]).await.expect("metadata");
            if md
                .topics
                .iter()
                .any(|t| t.name == "foo" && t.partition_count == 5)
            {
                break md;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("partition count reached 5 within 10s");
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    assert2::assert!(foo.partition_count == 5);

    // 5. describe_configs reports retention.ms as a dynamic override.
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert2::assert!(overrides.len() == 1);
    assert2::assert!(
        overrides[0]
            .overrides
            .get("retention.ms")
            .map(String::as_str)
            == Some("60000")
    );

    // 6. incremental_alter SET a new key.
    let outcomes = admin
        .incremental_alter_configs(&[IncrementalAlterOp::Set {
            topic: "foo".into(),
            key: "cleanup.policy".into(),
            value: "compact".into(),
        }])
        .await
        .unwrap();
    assert2::assert!(outcomes[0].error.is_none());
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert2::assert!(
        overrides[0]
            .overrides
            .get("cleanup.policy")
            .map(String::as_str)
            == Some("compact")
    );

    // 7. incremental_alter DELETE the retention override.
    let outcomes = admin
        .incremental_alter_configs(&[IncrementalAlterOp::Delete {
            topic: "foo".into(),
            key: "retention.ms".into(),
        }])
        .await
        .unwrap();
    assert2::assert!(outcomes[0].error.is_none());
    // The broker may return either an empty Vec (no dynamic overrides remain
    // for the resource) or a Vec with one entry whose overrides map lacks
    // the deleted key. Both shapes indicate the DELETE landed. Iterate
    // instead of indexing to handle both.
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    let still_has_retention = overrides
        .iter()
        .any(|t| t.overrides.contains_key("retention.ms"));
    assert2::assert!(!still_has_retention);

    // 8. Delete the topic.
    let outcomes = admin.delete_topics(&["foo"], 5_000).await.unwrap();
    assert2::assert!(outcomes[0].error.is_none());
    // Bounded poll until metadata no longer reports `foo` as a live topic
    // (either absent entirely, or present but error-marked as unknown).
    let md = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let md = admin.metadata(&["foo"]).await.expect("metadata");
            let live = md
                .topics
                .iter()
                .any(|t| t.name == "foo" && t.error.is_none());
            if !live {
                break md;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("topic foo removed from metadata within 10s");
    let foo = md.topics.iter().find(|t| t.name == "foo");
    if let Some(t) = foo {
        assert2::assert!(t.error.is_some());
    }
}

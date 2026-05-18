//! Integration test: spin up an in-process broker via the existing
//! `crates/broker/tests/support` harness, drive every admin RPC slice
//! 35 needs through `AdminClient`, assert the visible cluster state.

#![cfg(not(target_os = "windows"))]

use std::collections::BTreeMap;
use std::time::Duration;

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
    assert!(foo.error.is_some(), "expected error for unknown topic");

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
    assert_eq!(outcomes.len(), 1);
    assert!(
        outcomes[0].error.is_none(),
        "create failed: {:?}",
        outcomes[0].error
    );

    // 3. Metadata reflects the create.
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    assert!(foo.error.is_none());
    assert_eq!(foo.partition_count, 3);
    assert_eq!(foo.replication_factor, 1);

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
    assert!(outcomes[0].error.is_none());
    // Brief wait for metadata refresh.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo").unwrap();
    assert_eq!(foo.partition_count, 5);

    // 5. describe_configs reports retention.ms as a dynamic override.
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert_eq!(overrides.len(), 1);
    assert_eq!(
        overrides[0].overrides.get("retention.ms").map(String::as_str),
        Some("60000"),
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
    assert!(outcomes[0].error.is_none());
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert_eq!(
        overrides[0]
            .overrides
            .get("cleanup.policy")
            .map(String::as_str),
        Some("compact"),
    );

    // 7. incremental_alter DELETE the retention override.
    let outcomes = admin
        .incremental_alter_configs(&[IncrementalAlterOp::Delete {
            topic: "foo".into(),
            key: "retention.ms".into(),
        }])
        .await
        .unwrap();
    assert!(outcomes[0].error.is_none());
    let overrides = admin.describe_configs(&["foo"]).await.unwrap();
    assert!(!overrides[0].overrides.contains_key("retention.ms"));

    // 8. Delete the topic.
    let outcomes = admin.delete_topics(&["foo"], 5_000).await.unwrap();
    assert!(outcomes[0].error.is_none());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let md = admin.metadata(&["foo"]).await.unwrap();
    let foo = md.topics.iter().find(|t| t.name == "foo");
    if let Some(t) = foo {
        assert!(t.error.is_some(), "deleted topic should report unknown");
    }
}

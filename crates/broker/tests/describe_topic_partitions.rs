//! KIP-966 `DescribeTopicPartitions` (`api_key` 75): the paginated topic
//! listing the JVM admin client uses for `kafka-topics --describe`
//! against Kafka 3.7+ brokers.
//!
//! Covered:
//!   * named-request, fetch-all, and unknown-topic paths
//!   * `is_internal` flag set on Crabka's three internal topics
//!   * `topic_authorized_operations` populated (KIP-430 helper) on every
//!     Allow row. This API's v0 schema has no opt-in flag.
//!   * Pagination through `response_partition_limit` and the `cursor` /
//!     `next_cursor` round-trip
//!   * Stable sort order on fetch-all (alphabetical)

use assert2::{assert, check};
mod support;

use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    describe_topic_partitions_request::{
        Cursor as RequestCursor, DescribeTopicPartitionsRequest, TopicRequest,
    },
};

// Bit positions (subset; cross-check'd with the KIP-430 unit tests).
const BIT_READ: i32 = 1 << 3;
const BIT_WRITE: i32 = 1 << 4;
const BIT_CREATE: i32 = 1 << 5;
const BIT_DELETE: i32 = 1 << 6;
const BIT_ALTER: i32 = 1 << 7;
const BIT_DESCRIBE: i32 = 1 << 8;
const BIT_DESCRIBE_CONFIGS: i32 = 1 << 10;
const BIT_ALTER_CONFIGS: i32 = 1 << 11;
const TOPIC_FULL_MASK: i32 = BIT_READ
    | BIT_WRITE
    | BIT_CREATE
    | BIT_DELETE
    | BIT_ALTER
    | BIT_DESCRIBE
    | BIT_DESCRIBE_CONFIGS
    | BIT_ALTER_CONFIGS;

async fn create_topic(p: &support::InProcess, name: &str, partitions: i32) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "{name} create: {resp:?}");
}

#[tokio::test]
async fn named_request_returns_listed_topics_with_partitions() {
    let p = support::start().await;
    create_topic(&p, "alpha", 2).await;
    create_topic(&p, "beta", 1).await;

    let resp = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![
                TopicRequest {
                    name: "alpha".into(),
                    ..Default::default()
                },
                TopicRequest {
                    name: "beta".into(),
                    ..Default::default()
                },
            ],
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");

    assert!(resp.topics.len() == 2);
    // Named-request order preserves the request order.
    check!(resp.topics[0].name.as_deref() == Some("alpha"));
    check!(resp.topics[0].error_code == 0);
    check!(resp.topics[0].partitions.len() == 2);
    for (i, part) in resp.topics[0].partitions.iter().enumerate() {
        check!(part.error_code == 0);
        check!(part.partition_index == i32::try_from(i).unwrap());
        check!(part.leader_id == 1);
    }
    check!(resp.topics[1].name.as_deref() == Some("beta"));
    check!(resp.topics[1].partitions.len() == 1);

    // No truncation expected — every partition fits under the default
    // 2000-partition budget.
    check!(
        resp.next_cursor.is_none(),
        "no cursor expected: {:?}",
        resp.next_cursor,
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn fetch_all_returns_topics_in_alphabetical_order() {
    let p = support::start().await;
    // Create in non-alphabetical order to prove the broker sorts.
    create_topic(&p, "gamma", 1).await;
    create_topic(&p, "alpha", 1).await;
    create_topic(&p, "beta", 1).await;

    let resp = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: Vec::new(), // empty → fetch-all
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");

    let names: Vec<&str> = resp
        .topics
        .iter()
        .filter_map(|t| t.name.as_deref())
        .collect();
    // Internal topics may appear too — filter to the user-created ones.
    let user_topics: Vec<&str> = names
        .iter()
        .copied()
        .filter(|n| !n.starts_with("__"))
        .collect();
    assert!(user_topics == vec!["alpha", "beta", "gamma"]);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn unknown_topic_in_named_request_returns_error_row() {
    let p = support::start().await;
    create_topic(&p, "real-topic", 1).await;

    let resp = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![
                TopicRequest {
                    name: "ghost".into(),
                    ..Default::default()
                },
                TopicRequest {
                    name: "real-topic".into(),
                    ..Default::default()
                },
            ],
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");
    assert!(resp.topics.len() == 2);
    // Unknown topic row carries UNKNOWN_TOPIC_OR_PARTITION (3).
    check!(resp.topics[0].name.as_deref() == Some("ghost"));
    check!(resp.topics[0].error_code == 3);
    check!(resp.topics[0].partitions.is_empty());
    // Known sibling still served on the same response.
    check!(resp.topics[1].name.as_deref() == Some("real-topic"));
    check!(resp.topics[1].error_code == 0);
    check!(resp.topics[1].partitions.len() == 1);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn internal_topics_carry_is_internal_flag() {
    let p = support::start().await;
    // Drive a `CreateTopics` on `__consumer_offsets` to make sure the
    // internal-topic row exists in the metadata image. (The broker's
    // bootstrap creates it lazily on group-coordination traffic, which
    // hasn't fired here.) We can't create internal topics through the
    // wire, but the bootstrap also runs eagerly on `support::start` for
    // `__consumer_offsets` — verify by inspection of the fetch-all
    // response.
    create_topic(&p, "regular", 1).await;

    let resp = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: Vec::new(),
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");

    for row in &resp.topics {
        let n = row.name.as_deref().unwrap_or("");
        let expect_internal = matches!(
            n,
            "__consumer_offsets" | "__transaction_state" | "__remote_log_metadata"
        );
        assert!(
            row.is_internal == expect_internal,
            "is_internal mismatch for {n}: {row:?}"
        );
    }

    p.broker.shutdown().await;
}

/// The JVM 3.8 admin client NPEs when `eligibleLeaderReplicas` or
/// `lastKnownElr` decode as `null`. The NPE happens in
/// `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo`. The
/// schema marks both nullable, but real Kafka brokers always emit empty
/// lists. This test pins the empty-list shape so the broker does not
/// regress.
#[tokio::test]
async fn elr_lists_are_empty_not_null_for_jvm_3_8_admin_compatibility() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;

    let resp = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![TopicRequest {
                name: "t".into(),
                ..Default::default()
            }],
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    // MUST be Some(_), not None. Both fields stay as empty vecs so the
    // JVM 3.8 admin client's unconditional `.stream()` call doesn't NPE.
    assert!(
        part.eligible_leader_replicas.as_deref() == Some(&[][..]),
        "eligible_leader_replicas must be empty list, not null"
    );
    assert!(
        part.last_known_elr.as_deref() == Some(&[][..]),
        "last_known_elr must be empty list, not null"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn topic_authorized_operations_populated_for_super_user() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;

    let resp = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![TopicRequest {
                name: "t".into(),
                ..Default::default()
            }],
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");

    assert!(resp.topics.len() == 1);
    let row = &resp.topics[0];
    assert!(row.error_code == 0);
    // `support::start` uses `AllowAllAuthorizer` by default, so every
    // supported topic operation is authorized → full mask.
    assert!(
        row.topic_authorized_operations == TOPIC_FULL_MASK,
        "expected full topic mask, got 0b{:b}",
        row.topic_authorized_operations
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn pagination_caps_response_at_partition_limit_and_returns_next_cursor() {
    let p = support::start().await;
    create_topic(&p, "big", 5).await;

    // Cap response to 3 partitions; expect 3 returned + a cursor pointing
    // at "big" / partition 3.
    let resp = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![TopicRequest {
                name: "big".into(),
                ..Default::default()
            }],
            response_partition_limit: 3,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");

    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].name.as_deref() == Some("big"));
    check!(resp.topics[0].partitions.len() == 3);
    let cursor = resp.next_cursor.expect("next_cursor must be set");
    assert!(cursor.topic_name == "big");
    assert!(cursor.partition_index == 3);

    // Resume from the cursor — should return partitions 3 and 4 only.
    let resp2 = p
        .client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![TopicRequest {
                name: "big".into(),
                ..Default::default()
            }],
            response_partition_limit: 2000,
            cursor: Some(RequestCursor {
                topic_name: cursor.topic_name.clone(),
                partition_index: cursor.partition_index,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions (resume)");
    assert!(resp2.topics.len() == 1);
    let parts: Vec<i32> = resp2.topics[0]
        .partitions
        .iter()
        .map(|p| p.partition_index)
        .collect();
    assert!(parts == vec![3, 4]);
    assert!(
        resp2.next_cursor.is_none(),
        "no more data should remain after the resume: {:?}",
        resp2.next_cursor,
    );

    p.broker.shutdown().await;
}

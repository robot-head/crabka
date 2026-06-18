mod support;

use crabka_broker::coordinator::AUDIT_TOPIC;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};

#[tokio::test]
async fn audit_topic_exists_after_startup() {
    let p = support::start().await;

    // Send a Metadata request for `__crabka_audit` and assert the broker
    // returns it with `error_code == 0` and at least one partition.
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(AUDIT_TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("MetadataRequest failed");

    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(AUDIT_TOPIC))
        .expect("__crabka_audit not in Metadata response");

    assert2::check!(
        topic.error_code == 0,
        "unexpected error code: {}",
        topic.error_code
    );
    assert2::check!(
        !topic.partitions.is_empty(),
        "__crabka_audit has no partitions"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn broker_started_event_is_written_to_audit_topic() {
    let p = support::start().await;

    // Let bootstrap + the BrokerStarted emit settle.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let topic_id = support::topic_id_for(&p.client, AUDIT_TOPIC).await;
    let fr = p
        .client
        .send(FetchRequest {
            max_wait_ms: 200,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

    let part = &fr.responses[0].partitions[0];
    assert2::check!(part.error_code == 0);
    let batches = part
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("v2 records");
    let mut saw_started = false;
    for b in batches {
        for r in &b.records {
            if let Some(v) = &r.value {
                let j: serde_json::Value = serde_json::from_slice(v).unwrap();
                if j["class_uid"] == 6002 && j["activity_name"] == "BrokerStarted" {
                    saw_started = true;
                }
            }
        }
    }
    assert2::check!(saw_started);

    p.broker.shutdown().await;
}

/// Verifies that a successful `CreateTopics` call emits an `AdminOperation`
/// audit record with `class_uid == 6003`, `api.operation == "CreateTopics"`,
/// `status_id == 1`, and the topic name in `resources[0].name`.
#[tokio::test]
async fn successful_create_topics_is_audited() {
    let p = support::start().await;

    let cr = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "audited-orders".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::check!(cr.topics[0].error_code == 0);

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let recs = support::consume_audit_records(&p.client).await;
    let saw = recs.iter().any(|j| {
        j["class_uid"] == 6003
            && j["api"]["operation"] == "CreateTopics"
            && j["status_id"] == 1
            && j["resources"][0]["name"] == "audited-orders"
    });
    assert2::check!(saw);

    p.broker.shutdown().await;
}

/// Verifies the authorizer-decorator path denies an unauthorized operation.
///
/// This test asserts that:
///   1. A `CreateTopics` request is denied with `CLUSTER_AUTHORIZATION_FAILED`.
///   2. The broker remains healthy and does not crash.
///
/// What this test does NOT assert:
///   - That an `AuthorizationDenied` audit record was emitted to the audit topic.
///
/// Why not: The full end-to-end path (send-denied-request → observe
/// `AuthorizationDenied` record in the audit topic via the same client) is
/// impractical because:
///   - The test client connects anonymously (principal `"ANONYMOUS"`).
///   - `SimpleAclAuthorizer` with no ACLs and no super-users denies
///     every request, including the `Fetch` needed to read back the
///     audit topic.
///   - There is no plaintext SASL path that would give the anonymous
///     reader an elevated principal without setting up SCRAM credentials.
///
/// The audit emit on deny is already proven by the unit test
/// `deny_decision_emits_audit_record` in `crates/broker/src/audit_authorizer.rs`.
#[tokio::test]
async fn denied_operation_returns_cluster_authorization_failed() {
    // Start a broker with a deny-all authorizer.
    let p = support::start_with_deny_all_authz().await;

    // Attempt a create that will be denied.
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "denied-topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();

    // Verify the broker actually denied the request (error_code
    // CLUSTER_AUTHORIZATION_FAILED = 31).
    let denied = resp
        .topics
        .iter()
        .any(|t| t.error_code == crabka_broker::codes::CLUSTER_AUTHORIZATION_FAILED);
    assert2::check!(denied, "expected CreateTopics to be denied; resp: {resp:?}");

    // Verify the broker is still alive by checking the audit topic is reachable.
    let topic_id = support::topic_id_for(&p.client, AUDIT_TOPIC).await;
    let fr = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 0,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    // The broker responded to the Fetch request without crashing.
    let _ = fr;

    p.broker.shutdown().await;
}

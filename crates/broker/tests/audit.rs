mod support;

use crabka_broker::coordinator::AUDIT_TOPIC;
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};

#[tokio::test]
async fn audit_topic_exists_after_startup() {
    let p = support::start().await;
    p.broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;

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

    // Wait for the BrokerStarted event to be durably written to the audit topic
    // (the sink increments `audit_events_total` on each successful produce).
    p.broker
        .wait_for_metrics("audit event written", |m| m.audit_events_total.get() >= 1)
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    support::wait_for_audit_record(&p.client, "BrokerStarted", |j| {
        j["class_uid"] == 6002 && j["activity_name"] == "BrokerStarted"
    })
    .await;

    p.broker.shutdown().await;
}

/// Verifies that a successful `CreateTopics` call emits an `AdminOperation`
/// audit record. That record must carry `class_uid == 6003`,
/// `api.operation == "CreateTopics"`, `status_id == 1`, and the topic name in
/// `resources[0].name`.
#[tokio::test]
async fn successful_create_topics_is_audited() {
    let p = support::start().await;

    let audit_before = p.broker.metrics().audit_events_total.get();
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

    // Wait for the CreateTopics AdminOperation audit record to be durable.
    p.broker
        .wait_for_metrics("audit event written", |m| {
            m.audit_events_total.get() > audit_before
        })
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    support::wait_for_audit_record(&p.client, "CreateTopics admin audit", |j| {
        j["class_uid"] == 6003
            && j["api"]["operation"] == "CreateTopics"
            && j["status_id"] == 1
            && j["resources"][0]["name"] == "audited-orders"
    })
    .await;

    p.broker.shutdown().await;
}

/// Verifies the checkpoint path. The broker is configured with an audit
/// signing key and a checkpoint cadence of `every_n = 1`. A `CreateTopics`
/// request must then put a `checkpoint` record on the audit topic with the
/// expected `key_id`.
#[tokio::test]
async fn signed_checkpoints_appear_on_audit_topic() {
    use ring::signature::Ed25519KeyPair;

    // Generate a key, write it to a temp file, start a broker configured to use it.
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let keydir = tempfile::tempdir().unwrap();
    let keypath = keydir.path().join("audit.pk8");
    std::fs::write(&keypath, pkcs8.as_ref()).unwrap();

    // Start a broker with audit signing + a tiny checkpoint cadence (every 1 record).
    let p = support::start_with_audit_key(&keypath, "k-test", 1).await;

    // Cause some audit events (a create succeeds; super-user path).
    let audit_before = p.broker.metrics().audit_events_total.get();
    let _ = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "cp-topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for the create's chained record AND its signed checkpoint to be
    // durable: with `every_n = 1`, each audit event triggers a checkpoint, so
    // the counter advances by 2 (chained record + checkpoint) per create.
    p.broker
        .wait_for_metrics("audit checkpoint written", |m| {
            m.audit_events_total.get() >= audit_before + 2
        })
        .await;

    let recs = support::wait_for_audit_record(&p.client, "signed checkpoint", |j| {
        j["type"] == "checkpoint" && j["key_id"] == "k-test"
    })
    .await;
    let saw_checkpoint = recs
        .iter()
        .any(|j| j["type"] == "checkpoint" && j["key_id"] == "k-test");
    assert2::check!(saw_checkpoint);

    p.broker.shutdown().await;
}

/// Verifies that the authorizer-decorator path denies an unauthorized
/// operation.
///
/// This test asserts that:
///   1. The broker denies a `CreateTopics` request with
///      `CLUSTER_AUTHORIZATION_FAILED`.
///   2. The broker stays healthy and does not crash.
///
/// This test does NOT assert that the broker emitted an `AuthorizationDenied`
/// audit record to the audit topic.
///
/// The full end-to-end path, which sends a denied request and then observes
/// the `AuthorizationDenied` record in the audit topic through the same
/// client, is impractical for these reasons:
///   - The test client connects anonymously, with the principal
///     `"ANONYMOUS"`.
///   - `SimpleAclAuthorizer` with no ACLs and no super-users denies every
///     request, including the `Fetch` that reads the audit topic back.
///   - There is no plaintext SASL path that would give the anonymous reader a
///     higher principal without SCRAM credentials.
///
/// The unit test `deny_decision_emits_audit_record` in
/// `crates/broker/src/audit_authorizer.rs` already proves the audit emit on a
/// deny.
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

/// Verifies that the audit hash-chain sequence numbers are contiguous, and
/// that none repeats, across a broker restart. That shows that chain recovery
/// worked, and that the second boot did NOT reset the chain to seq 0.
#[tokio::test]
async fn audit_chain_continues_across_restart() {
    let dir = tempfile::tempdir().unwrap();

    // First boot: generate some audit events, then shut down cleanly.
    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        let audit_before = broker.metrics().audit_events_total.get();
        let _ = client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "r1".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .unwrap();
        // Ensure the r1 CreateTopics audit record is durable before shutdown.
        broker
            .wait_for_metrics("audit event written", |m| {
                m.audit_events_total.get() > audit_before
            })
            .await;
        broker.shutdown().await;
    }

    // Second boot on the SAME data dir: more events.
    let (broker, client) = support::start_with_dir(dir.path()).await;
    let audit_before = broker.metrics().audit_events_total.get();
    let _ = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "r2".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    // Ensure the r2 CreateTopics audit record is durable before consuming.
    broker
        .wait_for_metrics("audit event written", |m| {
            m.audit_events_total.get() > audit_before
        })
        .await;

    // Consume the audit topic and assert seqs are a contiguous, duplicate-free
    // chain (recovery worked — no reset to 0 on the second boot).
    let seqs = support::wait_for_audit_seq_count(&client, 4).await;
    assert2::check!(seqs.len() >= 4); // 2 BrokerStarted + 2 CreateTopics (at least)
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert2::check!(sorted.len() == seqs.len()); // no duplicate seqs
    assert2::check!(sorted == (0..seqs.len() as u64).collect::<Vec<_>>()); // contiguous from 0

    broker.shutdown().await;
}

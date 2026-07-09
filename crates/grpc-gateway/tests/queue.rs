use std::{collections::BTreeMap, path::PathBuf, process::Command, sync::Arc, time::Duration};

use axum::Extension;
use bytes::Bytes;
use connectrpc_axum::message::{Code, ConnectRequest};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_producer::{Header as ProducerHeader, Producer, ProducerRecord};
use crabka_grpc_gateway::{
    authz::GatewayAuthz,
    codec::RawCodec,
    config::GatewayConfig,
    pb,
    produce::ProduceCore,
    queue::{self, QueueSessionConfig, QueueSessionTable},
    state::AppState,
};
use crabka_security::{AuthMethod, Principal};
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn boot_with_config(
    configure: impl FnOnce(&mut BrokerConfig),
) -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    configure(&mut config);
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str) {
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap.to_string()))
        .await
        .expect("admin");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: name.to_string(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .expect("create topic");
}

async fn state_for(bootstrap: &str) -> Arc<AppState> {
    state_for_with_queue_idle(bootstrap, 60).await
}

async fn state_for_with_queue_idle(bootstrap: &str, queue_session_idle_secs: u64) -> Arc<AppState> {
    let produce = ProduceCore::new(bootstrap, "queue-test", Arc::new(RawCodec), None)
        .await
        .expect("produce core");
    let config = GatewayConfig {
        bootstrap: bootstrap.to_string(),
        listen_addr: "127.0.0.1:0".parse().expect("listen addr"),
        client_id: "queue-test".into(),
        dedup_topic: "__queue_dedup".into(),
        dedup_partitions: 1,
        dedup_window_ms: 3_600_000,
        dedup_txn_id_prefix: "queue-dedup".into(),
        advertised_addr: "127.0.0.1:0".into(),
        membership_topic: "__queue_membership".into(),
        tls: None,
        broker_security: None,
        authz: None,
        webhooks: BTreeMap::new().into_iter().collect(),
        outbound: Vec::new(),
        schema_registry_url: None,
        queue_max_messages: 16,
        queue_wait_ms_cap: 1_000,
        queue_session_idle_secs,
        queue_max_sessions: 64,
    };
    Arc::new(AppState {
        produce: Arc::new(produce),
        queue_sessions: AppState::queue_sessions_from_config(&config),
        config: Arc::new(config),
        authz: Arc::new(GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
    })
}

async fn produce_record(bootstrap: &str, topic: &str, value: &'static [u8]) {
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("queue-producer".to_string())
        .build()
        .await
        .expect("producer");
    let ack = producer
        .send(ProducerRecord {
            topic: topic.to_string(),
            partition: Some(0),
            key: None,
            value: Some(Bytes::from_static(value)),
            headers: vec![
                ProducerHeader {
                    key: "x".into(),
                    value: Some(Bytes::from_static(b"first")),
                },
                ProducerHeader {
                    key: "x".into(),
                    value: None,
                },
                ProducerHeader {
                    key: "x".into(),
                    value: Some(Bytes::from_static(b"second")),
                },
            ],
            timestamp_ms: None,
        })
        .await;
    ack.await
        .expect("producer ack channel")
        .expect("produce succeeds");
}

fn principal() -> Principal {
    Principal {
        name: "ANONYMOUS".into(),
        auth_method: AuthMethod::Anonymous,
        groups: vec![],
    }
}

async fn acquire_until_message(
    state: Arc<AppState>,
    group_id: &str,
    topic: &str,
    session_id: String,
) -> pb::QueueAcquireResponse {
    let mut session_id = session_id;
    for _ in 0..20 {
        let response = queue::queue_acquire(
            Extension(state.clone()),
            Some(Extension(principal())),
            None,
            ConnectRequest(pb::QueueAcquireRequest {
                group_id: group_id.into(),
                topics: vec![topic.into()],
                max_messages: 1,
                wait_ms: 250,
                session_id,
                lock_duration_ms: 30_000,
            }),
        )
        .await
        .expect("queue acquire")
        .0;
        if !response.messages.is_empty() {
            return response;
        }
        session_id = response.session_id;
    }
    panic!("queue acquire did not return a message");
}

async fn acquire_until_messages(
    state: Arc<AppState>,
    group_id: &str,
    topic: &str,
    session_id: String,
    max_messages: u32,
) -> pb::QueueAcquireResponse {
    let mut session_id = session_id;
    for _ in 0..20 {
        let response = queue::queue_acquire(
            Extension(state.clone()),
            Some(Extension(principal())),
            None,
            ConnectRequest(pb::QueueAcquireRequest {
                group_id: group_id.into(),
                topics: vec![topic.into()],
                max_messages,
                wait_ms: 250,
                session_id,
                lock_duration_ms: 30_000,
            }),
        )
        .await
        .expect("queue acquire")
        .0;
        if response.messages.len() == max_messages as usize {
            return response;
        }
        session_id = response.session_id;
    }
    panic!("queue acquire did not return enough messages");
}

async fn acknowledge(
    state: Arc<AppState>,
    session_id: &str,
    message: &pb::QueuedMessage,
    ack_type: pb::QueueAckType,
) -> pb::QueueAcknowledgeResponse {
    queue::queue_acknowledge(
        Extension(state),
        Some(Extension(principal())),
        ConnectRequest(pb::QueueAcknowledgeRequest {
            session_id: session_id.into(),
            entries: vec![pb::QueueAckEntry {
                topic: message.topic.clone(),
                partition: message.partition,
                offset: message.offset,
                r#type: ack_type as i32,
            }],
        }),
    )
    .await
    .expect("queue acknowledge")
    .0
}

async fn acknowledge_batch(
    state: Arc<AppState>,
    session_id: &str,
    messages: &[pb::QueuedMessage],
    ack_type: pb::QueueAckType,
) -> pb::QueueAcknowledgeResponse {
    queue::queue_acknowledge(
        Extension(state),
        Some(Extension(principal())),
        ConnectRequest(pb::QueueAcknowledgeRequest {
            session_id: session_id.into(),
            entries: messages
                .iter()
                .map(|message| pb::QueueAckEntry {
                    topic: message.topic.clone(),
                    partition: message.partition,
                    offset: message.offset,
                    r#type: ack_type as i32,
                })
                .collect(),
        }),
    )
    .await
    .expect("queue acknowledge")
    .0
}

async fn renew_batch(
    state: Arc<AppState>,
    session_id: &str,
    messages: &[pb::QueuedMessage],
) -> pb::QueueRenewResponse {
    queue::queue_renew(
        Extension(state),
        Some(Extension(principal())),
        ConnectRequest(pb::QueueRenewRequest {
            session_id: session_id.into(),
            entries: messages
                .iter()
                .map(|message| pb::QueueAckEntry {
                    topic: message.topic.clone(),
                    partition: message.partition,
                    offset: message.offset,
                    r#type: pb::QueueAckType::Unspecified as i32,
                })
                .collect(),
        }),
    )
    .await
    .expect("queue renew")
    .0
}

fn conformance_message_id(message: &pb::QueuedMessage) -> String {
    format!("{}:{}:{}", message.topic, message.partition, message.offset)
}

fn jvm_queue_cross_consumer_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/scripts/jvm_queue_cross_consumer.sh")
}

fn jvm_queue_cross_consumer_args() -> Vec<String> {
    vec![
        "--gateway-endpoint".into(),
        "http://127.0.0.1:18080".into(),
        "--bootstrap-server".into(),
        "host.docker.internal:9092".into(),
        "--topic".into(),
        "queue-jvm-cross-consumer".into(),
        "--group".into(),
        "queue-jvm-cross-consumer-group".into(),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_returns_records_and_headers_then_accept_prevents_redelivery() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-accept").await;
    produce_record(&bootstrap, "queue-accept", b"payload").await;
    let state = state_for(&bootstrap).await;

    let acquired = acquire_until_message(
        state.clone(),
        "queue-accept-group",
        "queue-accept",
        String::new(),
    )
    .await;
    let message = acquired.messages.first().expect("message acquired");
    assert_eq!(message.value, b"payload");
    assert_eq!(message.headers.len(), 3);
    assert_eq!(message.headers[0].value, Some(b"first".to_vec()));
    assert_eq!(message.headers[1].key, "x");
    assert_eq!(message.headers[1].value, None);
    assert_eq!(message.headers[2].value, Some(b"second".to_vec()));

    let ack = acknowledge(
        state.clone(),
        &acquired.session_id,
        message,
        pb::QueueAckType::Accept,
    )
    .await;
    assert!(ack.results.iter().all(|result| result.error.is_none()));

    let second = queue::queue_acquire(
        Extension(state),
        Some(Extension(principal())),
        None,
        ConnectRequest(pb::QueueAcquireRequest {
            group_id: "queue-accept-group".into(),
            topics: vec!["queue-accept".into()],
            max_messages: 1,
            wait_ms: 100,
            session_id: acquired.session_id,
            lock_duration_ms: 30_000,
        }),
    )
    .await
    .expect("second acquire")
    .0;
    assert!(second.messages.is_empty());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_redelivers_record() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-release").await;
    produce_record(&bootstrap, "queue-release", b"again").await;
    let state = state_for(&bootstrap).await;

    let acquired = acquire_until_message(
        state.clone(),
        "queue-release-group",
        "queue-release",
        String::new(),
    )
    .await;
    let message = acquired.messages.first().expect("message acquired");
    let ack = acknowledge(
        state.clone(),
        &acquired.session_id,
        message,
        pb::QueueAckType::Release,
    )
    .await;
    assert!(ack.results.iter().all(|result| result.error.is_none()));

    let redelivered = acquire_until_message(
        state,
        "queue-release-group",
        "queue-release",
        acquired.session_id,
    )
    .await;
    assert_eq!(redelivered.messages[0].value, b"again");
    assert_eq!(redelivered.messages[0].delivery_count, 2);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_archives_record_and_prevents_redelivery() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-reject").await;
    produce_record(&bootstrap, "queue-reject", b"drop-me").await;
    let state = state_for(&bootstrap).await;

    let acquired = acquire_until_message(
        state.clone(),
        "queue-reject-group",
        "queue-reject",
        String::new(),
    )
    .await;
    let message = acquired.messages.first().expect("message acquired");
    let ack = acknowledge(
        state.clone(),
        &acquired.session_id,
        message,
        pb::QueueAckType::Reject,
    )
    .await;
    assert!(ack.results.iter().all(|result| result.error.is_none()));

    let second = queue::queue_acquire(
        Extension(state),
        Some(Extension(principal())),
        None,
        ConnectRequest(pb::QueueAcquireRequest {
            group_id: "queue-reject-group".into(),
            topics: vec!["queue-reject".into()],
            max_messages: 1,
            wait_ms: 100,
            session_id: acquired.session_id,
            lock_duration_ms: 30_000,
        }),
    )
    .await
    .expect("second acquire")
    .0;
    assert!(second.messages.is_empty());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_expiry_redelivers_with_incremented_delivery_count() {
    let (broker, bootstrap, _dir) = boot_with_config(|config| {
        config.share_group.record_lock_duration = Duration::from_millis(250);
    })
    .await;
    create_topic(&bootstrap, "queue-lock-expiry").await;
    produce_record(&bootstrap, "queue-lock-expiry", b"expired").await;
    let state = state_for(&bootstrap).await;

    let acquired = acquire_until_message(
        state.clone(),
        "queue-lock-expiry-group",
        "queue-lock-expiry",
        String::new(),
    )
    .await;
    assert_eq!(acquired.messages[0].delivery_count, 1);

    tokio::time::sleep(Duration::from_millis(800)).await;

    let redelivered = acquire_until_message(
        state,
        "queue-lock-expiry-group",
        "queue-lock-expiry",
        acquired.session_id,
    )
    .await;
    assert_eq!(redelivered.messages[0].value, b"expired");
    assert_eq!(redelivered.messages[0].delivery_count, 2);

    broker.shutdown().await;
}

#[tokio::test]
async fn non_default_lock_duration_is_invalid_argument() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-lock-duration").await;
    let state = state_for(&bootstrap).await;

    let error = queue::queue_acquire(
        Extension(state),
        Some(Extension(principal())),
        None,
        ConnectRequest(pb::QueueAcquireRequest {
            group_id: "queue-lock-duration-group".into(),
            topics: vec!["queue-lock-duration".into()],
            max_messages: 1,
            wait_ms: 100,
            session_id: String::new(),
            lock_duration_ms: 1_000,
        }),
    )
    .await
    .expect_err("non-default lock duration fails");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().is_some_and(|message| {
        message.contains("lock_duration_ms") && message.contains("not supported")
    }));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_expiry_reacquire_redelivers_unacked_record() {
    let (broker, bootstrap, _dir) = boot_with_config(|config| {
        config.share_group.record_lock_duration = Duration::from_millis(250);
    })
    .await;
    create_topic(&bootstrap, "queue-session-expiry").await;
    produce_record(&bootstrap, "queue-session-expiry", b"unacked").await;
    let state = state_for_with_queue_idle(&bootstrap, 1).await;

    let acquired = acquire_until_message(
        state.clone(),
        "queue-session-expiry-group",
        "queue-session-expiry",
        String::new(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_300)).await;

    let expired = queue::queue_acquire(
        Extension(state.clone()),
        Some(Extension(principal())),
        None,
        ConnectRequest(pb::QueueAcquireRequest {
            group_id: "queue-session-expiry-group".into(),
            topics: vec!["queue-session-expiry".into()],
            max_messages: 1,
            wait_ms: 100,
            session_id: acquired.session_id,
            lock_duration_ms: 30_000,
        }),
    )
    .await
    .expect_err("idle session expires");
    assert_eq!(expired.code(), Code::FailedPrecondition);

    let redelivered = acquire_until_message(
        state,
        "queue-session-expiry-group",
        "queue-session-expiry",
        String::new(),
    )
    .await;
    assert_eq!(redelivered.messages[0].value, b"unacked");
    assert!(redelivered.messages[0].delivery_count >= 2);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_ack_entry_is_per_entry_error_while_sibling_succeeds() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-expired-ack").await;
    produce_record(&bootstrap, "queue-expired-ack", b"fresh").await;
    let state = state_for(&bootstrap).await;

    let acquired = acquire_until_message(
        state.clone(),
        "queue-expired-ack-group",
        "queue-expired-ack",
        String::new(),
    )
    .await;
    let mut invalid = acquired.messages[0].clone();
    invalid.offset += 1;

    let ack = acknowledge_batch(
        state,
        &acquired.session_id,
        &[invalid, acquired.messages[0].clone()],
        pb::QueueAckType::Accept,
    )
    .await;
    assert_eq!(ack.results.len(), 2);
    assert!(ack.results[0].error.is_some());
    assert!(ack.results[1].error.is_none());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_acknowledge_batch_finalizes_after_broker_success() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-batch-accept").await;
    produce_record(&bootstrap, "queue-batch-accept", b"first").await;
    produce_record(&bootstrap, "queue-batch-accept", b"second").await;
    let state = state_for(&bootstrap).await;

    let acquired = acquire_until_messages(
        state.clone(),
        "queue-batch-accept-group",
        "queue-batch-accept",
        String::new(),
        2,
    )
    .await;
    let ack = acknowledge_batch(
        state.clone(),
        &acquired.session_id,
        &acquired.messages,
        pb::QueueAckType::Accept,
    )
    .await;
    assert!(ack.results.iter().all(|result| result.error.is_none()));

    let second = queue::queue_acquire(
        Extension(state),
        Some(Extension(principal())),
        None,
        ConnectRequest(pb::QueueAcquireRequest {
            group_id: "queue-batch-accept-group".into(),
            topics: vec!["queue-batch-accept".into()],
            max_messages: 2,
            wait_ms: 100,
            session_id: acquired.session_id,
            lock_duration_ms: 30_000,
        }),
    )
    .await
    .expect("second acquire")
    .0;
    assert!(second.messages.is_empty());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_overflow_is_returned_by_later_acquire() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-overflow").await;
    produce_record(&bootstrap, "queue-overflow", b"first").await;
    produce_record(&bootstrap, "queue-overflow", b"second").await;
    let state = state_for(&bootstrap).await;

    let first = acquire_until_message(
        state.clone(),
        "queue-overflow-group",
        "queue-overflow",
        String::new(),
    )
    .await;
    let second = acquire_until_message(
        state.clone(),
        "queue-overflow-group",
        "queue-overflow",
        first.session_id.clone(),
    )
    .await;
    let mut messages = first.messages.clone();
    messages.extend(second.messages.clone());

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].value, b"first");
    assert_eq!(messages[1].value, b"second");

    let ack = acknowledge_batch(
        state.clone(),
        &second.session_id,
        &messages,
        pb::QueueAckType::Accept,
    )
    .await;
    assert!(ack.results.iter().all(|result| result.error.is_none()));

    let third = queue::queue_acquire(
        Extension(state),
        Some(Extension(principal())),
        None,
        ConnectRequest(pb::QueueAcquireRequest {
            group_id: "queue-overflow-group".into(),
            topics: vec!["queue-overflow".into()],
            max_messages: 1,
            wait_ms: 100,
            session_id: second.session_id,
            lock_duration_ms: 30_000,
        }),
    )
    .await
    .expect("third acquire")
    .0;
    assert!(third.messages.is_empty());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_v1_1_gateway_shapes_match_conformance_vectors() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "queue-v1-1-shape").await;
    produce_record(&bootstrap, "queue-v1-1-shape", b"work").await;
    let state = state_for(&bootstrap).await;

    let acquired = acquire_until_message(
        state.clone(),
        "queue-v1-1-workers",
        "queue-v1-1-shape",
        String::new(),
    )
    .await;
    let message = acquired.messages.first().expect("message acquired");

    assert!(!acquired.session_id.is_empty());
    assert_eq!(conformance_message_id(message), "queue-v1-1-shape:0:0");
    assert_eq!(message.topic, "queue-v1-1-shape");
    assert_eq!(message.partition, 0);
    assert_eq!(message.offset, 0);
    assert_eq!(message.value, b"work");
    assert_eq!(message.delivery_count, 1);
    assert_eq!(message.headers.len(), 3);

    let renew = renew_batch(state.clone(), &acquired.session_id, &acquired.messages).await;
    assert_eq!(renew.results.len(), 1);
    assert_eq!(
        renew.results[0].entry.as_ref().expect("renew entry").topic,
        message.topic
    );
    assert!(renew.results[0].error.is_none());

    let ack = acknowledge_batch(
        state.clone(),
        &acquired.session_id,
        &acquired.messages,
        pb::QueueAckType::Accept,
    )
    .await;
    assert_eq!(ack.results.len(), 1);
    assert_eq!(
        ack.results[0].entry.as_ref().expect("ack entry").offset,
        message.offset
    );
    assert!(ack.results[0].error.is_none());

    broker.shutdown().await;
}

#[test]
fn jvm_queue_cross_consumer_script_documents_real_flow() {
    let output = Command::new("/bin/sh")
        .arg(jvm_queue_cross_consumer_script())
        .args(jvm_queue_cross_consumer_args())
        .arg("--dry-run")
        .output()
        .expect("run JVM queue cross-consumer script dry run");

    assert!(
        output.status.success(),
        "dry run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("dry run output is utf8");
    assert!(stdout.contains("kafka-console-producer.sh"));
    assert!(stdout.contains("/crabka.gateway.v1.Gateway/QueueAcquire"));
    assert!(stdout.contains("/crabka.gateway.v1.Gateway/QueueAcknowledge"));
    assert!(stdout.contains("kafka-console-share-consumer.sh"));
    assert!(stdout.contains("queue-jvm-cross-consumer"));
    assert!(stdout.contains("queue-jvm-cross-consumer-group"));
}

#[test]
fn jvm_queue_cross_consumer_script_requires_tooling_for_real_run() {
    let output = Command::new("/bin/sh")
        .arg(jvm_queue_cross_consumer_script())
        .args(jvm_queue_cross_consumer_args())
        .env("PATH", "/nonexistent")
        .output()
        .expect("run JVM queue cross-consumer script requirement check");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("missing required command 'docker' for JVM queue cross-consumer run")
    );
}

#[test]
#[ignore = "requires Docker, JVM Kafka tools, a host-advertised broker, and a running gateway"]
fn jvm_queue_cross_consumer_external_flow() {
    let gateway_endpoint = std::env::var("CRABKA_JVM_QUEUE_GATEWAY_ENDPOINT").expect(
        "set CRABKA_JVM_QUEUE_GATEWAY_ENDPOINT to the running gateway URL before running this ignored test",
    );
    let bootstrap_server = std::env::var("CRABKA_JVM_QUEUE_BOOTSTRAP").expect(
        "set CRABKA_JVM_QUEUE_BOOTSTRAP to the JVM-reachable broker address before running this ignored test",
    );
    let topic = std::env::var("CRABKA_JVM_QUEUE_TOPIC")
        .unwrap_or_else(|_| "queue-jvm-cross-consumer".into());
    let group = std::env::var("CRABKA_JVM_QUEUE_GROUP")
        .unwrap_or_else(|_| "queue-jvm-cross-consumer-group".into());

    let status = Command::new("/bin/sh")
        .arg(jvm_queue_cross_consumer_script())
        .args([
            "--gateway-endpoint",
            gateway_endpoint.as_str(),
            "--bootstrap-server",
            bootstrap_server.as_str(),
            "--topic",
            topic.as_str(),
            "--group",
            group.as_str(),
        ])
        .status()
        .expect("start JVM queue cross-consumer script");

    assert!(status.success(), "JVM queue cross-consumer script failed");
}

#[tokio::test]
async fn unknown_and_expired_sessions_are_failed_precondition() {
    let (_broker, bootstrap, _dir) = boot().await;
    let state = state_for(&bootstrap).await;
    let error = queue::queue_acknowledge(
        Extension(state),
        Some(Extension(principal())),
        ConnectRequest(pb::QueueAcknowledgeRequest {
            session_id: "missing".into(),
            entries: vec![],
        }),
    )
    .await
    .expect_err("missing session fails");
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error
            .message()
            .is_some_and(|message| message.contains("expired"))
    );

    let table = QueueSessionTable::new(QueueSessionConfig {
        idle_timeout: Duration::from_millis(1),
        max_sessions: 1,
    });
    let inserted_at = std::time::Instant::now();
    let session_id = table
        .insert_at(principal(), "session", inserted_at)
        .expect("insert session");
    assert!(
        table
            .get_at(
                &principal(),
                &session_id,
                inserted_at + Duration::from_millis(1),
            )
            .is_err()
    );
}

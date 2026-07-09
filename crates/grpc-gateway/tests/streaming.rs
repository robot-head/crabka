//! Streaming Connect handlers: `SendStream` (produce) and `Subscribe` (consume).

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use assert2::check;
use bytes::Bytes;
use connectrpc_axum::message::{Code, Streaming};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Acks, Header as ProducerHeader, Producer, ProducerRecord};
use crabka_grpc_gateway::{
    codec::{CodecError, Decoded, EncodeBody, RawCodec, RecordCodec},
    config::GatewayConfig,
    pb,
    produce::ProduceCore,
    state::AppState,
    streaming,
};
use futures_util::StreamExt;
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn state_for(bootstrap: &str) -> Arc<AppState> {
    state_for_codec(bootstrap, Arc::new(RawCodec)).await
}

async fn state_for_codec(bootstrap: &str, codec: Arc<dyn RecordCodec>) -> Arc<AppState> {
    let produce = ProduceCore::new(bootstrap, "stream", Arc::new(RawCodec), None)
        .await
        .unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: addr,
            client_id: "stream".into(),
            dedup_topic: "__crabka_grpc_dedup".into(),
            dedup_partitions: 4,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: "stream-dedup".into(),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_grpc_gateway_membership".into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
            queue_max_messages: GatewayConfig::DEFAULT_QUEUE_MAX_MESSAGES,
            queue_wait_ms_cap: GatewayConfig::DEFAULT_QUEUE_WAIT_MS_CAP,
            queue_session_idle_secs: GatewayConfig::DEFAULT_QUEUE_SESSION_IDLE_SECS,
            queue_max_sessions: GatewayConfig::DEFAULT_QUEUE_MAX_SESSIONS,
        }),
        authz: Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec,
        queue_sessions: AppState::queue_sessions_from_config(&GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: addr,
            client_id: "stream".into(),
            dedup_topic: "__crabka_grpc_dedup".into(),
            dedup_partitions: 4,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: "stream-dedup".into(),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_grpc_gateway_membership".into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
            queue_max_messages: GatewayConfig::DEFAULT_QUEUE_MAX_MESSAGES,
            queue_wait_ms_cap: GatewayConfig::DEFAULT_QUEUE_WAIT_MS_CAP,
            queue_session_idle_secs: GatewayConfig::DEFAULT_QUEUE_SESSION_IDLE_SECS,
            queue_max_sessions: GatewayConfig::DEFAULT_QUEUE_MAX_SESSIONS,
        }),
    })
}

#[derive(Debug)]
struct JsonEchoCodec;

#[async_trait::async_trait]
impl RecordCodec for JsonEchoCodec {
    async fn encode(&self, _topic: &str, body: EncodeBody) -> Result<Bytes, CodecError> {
        Ok(match body {
            EncodeBody::Raw(bytes) => bytes,
            EncodeBody::Structured { json, .. } => json,
        })
    }

    async fn decode(&self, _topic: &str, value: Bytes) -> Result<Decoded, CodecError> {
        Ok(Decoded {
            value: value.clone(),
            schema: None,
            json: Some(value),
        })
    }
}

/// On-behalf-of identity for the `*_inner` helpers: ANONYMOUS over the unknown
/// host. State carries an `AllowAllAuthorizer`, so the value is immaterial to
/// the decision (every record is allowed) — it just satisfies the signature.
fn anon() -> (crabka_security::Principal, SocketAddr) {
    (
        crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        },
        "0.0.0.0:0".parse().unwrap(),
    )
}

fn rec(topic: &str, value: &'static [u8]) -> pb::Record {
    pb::Record {
        topic: topic.into(),
        key: None,
        body: Some(pb::record::Body::Raw(value.to_vec())),
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: None,
        schema: None,
    }
}

async fn produce_raw_records(bootstrap: &str, topic: &str, values: &[&'static [u8]]) {
    let records: Vec<_> = values.iter().map(|value| (0, *value)).collect();
    produce_raw_records_to_partitions(bootstrap, topic, &records).await;
}

async fn produce_raw_records_to_partitions(
    bootstrap: &str,
    topic: &str,
    records: &[(i32, &'static [u8])],
) {
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_string())
        .acks(Acks::All)
        .build()
        .await
        .unwrap();
    for (partition, value) in records {
        producer
            .send(ProducerRecord {
                topic: topic.to_string(),
                partition: Some(*partition),
                value: Some(Bytes::from_static(value)),
                ..ProducerRecord::default()
            })
            .await
            .await
            .unwrap()
            .unwrap();
    }
    producer.close().await.unwrap();
}

#[cfg(feature = "arrow")]
async fn produce_bytes_records(bootstrap: &str, topic: &str, values: &[Bytes]) {
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_string())
        .acks(Acks::All)
        .build()
        .await
        .unwrap();
    for value in values {
        producer
            .send(ProducerRecord {
                topic: topic.to_string(),
                partition: Some(0),
                value: Some(value.clone()),
                ..ProducerRecord::default()
            })
            .await
            .await
            .unwrap()
            .unwrap();
    }
    producer.close().await.unwrap();
}

#[cfg(feature = "arrow")]
fn arrow_ipc_order_record(statuses: &[&str], prices: &[f64]) -> Bytes {
    use arrow::{
        array::{Float64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
        ipc::writer::StreamWriter,
    };

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(statuses.to_vec())),
            Arc::new(Float64Array::from(prices.to_vec())),
        ],
    )
    .expect("order batch builds");
    let mut encoded = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut encoded, &batch.schema()).expect("Arrow IPC writer builds");
        writer.write(&batch).expect("Arrow IPC batch writes");
        writer.finish().expect("Arrow IPC stream finishes");
    }
    Bytes::from(encoded)
}

async fn await_committed_offset(
    admin: &mut AdminClient,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> Option<i64> {
    for _ in 0..100 {
        let offsets = admin.list_consumer_group_offsets(group_id).await.unwrap();
        if let Some(offset) = offsets.get(&(topic.to_string(), partition)).copied() {
            return Some(offset);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_stream_produces_all_records() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "ss-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    let input = futures_util::stream::iter(vec![
        Ok(pb::SendRequest {
            records: vec![rec("ss-topic", b"a")],
            acks: 0,
        }),
        Ok(pb::SendRequest {
            records: vec![rec("ss-topic", b"b")],
            acks: 0,
        }),
    ]);
    let inbound = Streaming::new(Box::pin(input));

    let (p, h) = anon();
    let acks: Vec<_> = streaming::send_stream_inner(inbound, state, p, h)
        .collect()
        .await;
    check!(acks.len() == 2);
    for a in &acks {
        let ack = a.as_ref().expect("ack ok");
        check!(ack.results.len() == 1);
        check!(ack.results[0].error.is_none());
    }

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("ss-reader")
        .subscribe(vec!["ss-topic".to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut seen = 0;
    for _ in 0..10 {
        seen += consumer
            .poll(std::time::Duration::from_millis(500))
            .await
            .unwrap()
            .len();
        if seen >= 2 {
            break;
        }
    }
    check!(seen == 2);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_streams_records_then_commits() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "sub-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    // Produce one record up front.
    let (prod_principal, _) = anon();
    crabka_grpc_gateway::produce::ProduceCore::new(
        &bootstrap,
        "sub-prod",
        Arc::new(RawCodec),
        None,
    )
    .await
    .unwrap()
    .produce(
        crabka_grpc_gateway::types::GatewayRecord {
            topic: "sub-topic".into(),
            key: None,
            value: Bytes::from_static(b"hello"),
            body_structured: None,
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        },
        &prod_principal,
    )
    .await
    .unwrap();

    // Control stream: a Start frame (auto_commit), then stays open until dropped.
    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "sub-group".into(),
            topics: vec!["sub-topic".into()],
            auto_commit: true,
            filter: String::new(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));

    let (p, h) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, p, h));
    let mut got = None;
    for _ in 0..20 {
        match tokio::time::timeout(std::time::Duration::from_millis(600), out.next()).await {
            Ok(Some(Ok(msg))) => {
                got = Some(msg);
                break;
            }
            Ok(Some(Err(e))) => panic!("subscribe error: {e:?}"),
            Ok(None) => break,
            Err(_) => {} // timed out this round; retry the poll
        }
    }
    // The loop above already captured (and broke on) the first record, so this
    // asserts on the already-received Inbound. Dropping the control stream just
    // releases the session's resources — the test does not wait to observe the
    // subscription closing.
    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), out.next()).await;
    let msg = got.expect("received an Inbound record");
    check!(msg.topic == "sub-topic");
    check!(msg.value == b"hello");
    check!(await_committed_offset(&mut admin, "sub-group", "sub-topic", 0).await == Some(1));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_explicit_ack_commits_ack_plus_one() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "ack-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    let (prod_principal, _) = anon();
    crabka_grpc_gateway::produce::ProduceCore::new(
        &bootstrap,
        "ack-prod",
        Arc::new(RawCodec),
        None,
    )
    .await
    .unwrap()
    .produce(
        crabka_grpc_gateway::types::GatewayRecord {
            topic: "ack-topic".into(),
            key: None,
            value: Bytes::from_static(b"hello"),
            body_structured: None,
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        },
        &prod_principal,
    )
    .await
    .unwrap();

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "ack-group".into(),
            topics: vec!["ack-topic".into()],
            auto_commit: false,
            filter: String::new(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));

    let (p, h) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, p, h));
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(msg.topic == "ack-topic");
    check!(msg.offset == 0);

    tx.send(Ok(pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Ack(pb::SubscribeAck {
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
        })),
    }))
    .unwrap();
    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), out.next()).await;

    check!(await_committed_offset(&mut admin, "ack-group", "ack-topic", 0).await == Some(1));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_explicit_future_ack_is_invalid_and_does_not_commit() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "future-ack-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    produce_raw_records(&bootstrap, "future-ack-topic", &[b"zero"]).await;
    let state = state_for(&bootstrap).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "future-ack-group".into(),
            topics: vec!["future-ack-topic".into()],
            auto_commit: false,
            filter: String::new(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, principal, host));

    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(msg.offset == 0);

    tx.send(Ok(pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Ack(pb::SubscribeAck {
            topic: msg.topic,
            partition: msg.partition,
            offset: msg.offset + 2,
        })),
    }))
    .unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("invalid ack error arrives")
        .expect("stream item")
        .expect_err("future ack is rejected");
    assert_eq!(error.code(), Code::InvalidArgument);
    drop(tx);

    check!(
        await_committed_offset(&mut admin, "future-ack-group", "future-ack-topic", 0)
            .await
            .is_none()
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_explicit_filter_commits_all_partitions_advanced_in_batch() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "filter-batch-ack-topic".into(),
                partitions: 3,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    produce_raw_records_to_partitions(
        &bootstrap,
        "filter-batch-ack-topic",
        &[
            (0, br#"{"keep":false,"partition":0}"#),
            (1, br#"{"keep":false,"partition":1}"#),
            (2, br#"{"keep":true,"partition":2}"#),
        ],
    )
    .await;
    let state = state_for_codec(&bootstrap, Arc::new(JsonEchoCodec)).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "filter-batch-ack-group".into(),
            topics: vec!["filter-batch-ack-topic".into()],
            auto_commit: false,
            filter: "keep = true".into(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, principal, host));

    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("matching record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(msg.partition == 2);

    let pump = tokio::spawn(async move { while out.next().await.is_some() {} });
    tokio::task::yield_now().await;

    check!(
        await_committed_offset(
            &mut admin,
            "filter-batch-ack-group",
            "filter-batch-ack-topic",
            0,
        )
        .await
            == Some(1)
    );
    check!(
        await_committed_offset(
            &mut admin,
            "filter-batch-ack-group",
            "filter-batch-ack-topic",
            1,
        )
        .await
            == Some(1)
    );

    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), pump).await;

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_explicit_ack_gap_redelivers_from_frontier() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "ack-gap-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    produce_raw_records(&bootstrap, "ack-gap-topic", &[b"zero", b"one"]).await;
    let state = state_for(&bootstrap).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "ack-gap-group".into(),
            topics: vec!["ack-gap-topic".into()],
            auto_commit: false,
            filter: String::new(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(
        inbound,
        state.clone(),
        principal,
        host,
    ));

    let first = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("first record arrives")
        .expect("stream item")
        .expect("inbound ok");
    let second = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("second record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(first.offset == 0);
    check!(second.offset == 1);

    tx.send(Ok(pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Ack(pb::SubscribeAck {
            topic: second.topic,
            partition: second.partition,
            offset: second.offset,
        })),
    }))
    .unwrap();
    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), out.next()).await;
    check!(
        await_committed_offset(&mut admin, "ack-gap-group", "ack-gap-topic", 0)
            .await
            .is_none()
    );

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "ack-gap-group".into(),
            topics: vec!["ack-gap-topic".into()],
            auto_commit: false,
            filter: String::new(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, principal, host));
    let redelivered = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("redelivery arrives")
        .expect("stream item")
        .expect("inbound ok");
    drop(tx);
    check!(redelivered.offset == 0);
    check!(redelivered.value == b"zero");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_explicit_filter_auto_ack_prevents_filtered_gap_stall() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "filter-ack-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    produce_raw_records(
        &bootstrap,
        "filter-ack-topic",
        &[br#"{"keep":false}"#, br#"{"keep":true}"#],
    )
    .await;
    let state = state_for_codec(&bootstrap, Arc::new(JsonEchoCodec)).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "filter-ack-group".into(),
            topics: vec!["filter-ack-topic".into()],
            auto_commit: false,
            filter: "keep = true".into(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, principal, host));

    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("matching record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(msg.offset == 1);

    tx.send(Ok(pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Ack(pb::SubscribeAck {
            topic: msg.topic,
            partition: msg.partition,
            offset: msg.offset,
        })),
    }))
    .unwrap();
    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), out.next()).await;

    check!(
        await_committed_offset(&mut admin, "filter-ack-group", "filter-ack-topic", 0).await
            == Some(2)
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_explicit_filter_does_not_commit_filtered_later_offset_before_delivered_ack() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "filter-order-ack-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    produce_raw_records(
        &bootstrap,
        "filter-order-ack-topic",
        &[br#"{"keep":true}"#, br#"{"keep":false}"#],
    )
    .await;
    let state = state_for_codec(&bootstrap, Arc::new(JsonEchoCodec)).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "filter-order-ack-group".into(),
            topics: vec!["filter-order-ack-topic".into()],
            auto_commit: false,
            filter: "keep = true".into(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, principal, host));

    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("matching record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(msg.offset == 0);

    let no_second_match =
        tokio::time::timeout(std::time::Duration::from_millis(750), out.next()).await;
    check!(no_second_match.is_err());
    check!(
        !admin
            .list_consumer_group_offsets("filter-order-ack-group")
            .await
            .unwrap()
            .contains_key(&("filter-order-ack-topic".to_string(), 0))
    );

    tx.send(Ok(pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Ack(pb::SubscribeAck {
            topic: msg.topic,
            partition: msg.partition,
            offset: msg.offset,
        })),
    }))
    .unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), out.next()).await;
    drop(tx);

    check!(
        await_committed_offset(
            &mut admin,
            "filter-order-ack-group",
            "filter-order-ack-topic",
            0,
        )
        .await
            == Some(2)
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_filter_matches_enum_dictionary_and_numeric_fields() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "enum-filter-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    produce_raw_records(
        &bootstrap,
        "enum-filter-topic",
        &[
            br#"{"status":{"$type":"enum","name":"NETWORK_NODE","number":1},"priority":9}"#,
            br#"{"status":{"$type":"enum","number":7},"priority":4}"#,
            br#"{"status":{"$type":"dictionary","key":7,"value":"UNKNOWN_7"},"priority":2}"#,
            br"unstructured",
        ],
    )
    .await;
    let state = state_for_codec(&bootstrap, Arc::new(JsonEchoCodec)).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "enum-filter-group".into(),
            topics: vec!["enum-filter-topic".into()],
            auto_commit: true,
            filter: "status = 'UNKNOWN_7' AND priority >= 3".into(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, principal, host));

    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("matching enum dictionary record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(msg.offset == 1);
    check!(msg.value == br#"{"status":{"$type":"enum","number":7},"priority":4}"#);

    let no_second_match =
        tokio::time::timeout(std::time::Duration::from_millis(750), out.next()).await;
    drop(tx);
    check!(no_second_match.is_err());
    check!(
        await_committed_offset(&mut admin, "enum-filter-group", "enum-filter-topic", 0).await
            == Some(4)
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_auto_commit_fully_filtered_batch_commits_current_position() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "auto-filter-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    produce_raw_records(
        &bootstrap,
        "auto-filter-topic",
        &[br#"{"keep":false,"n":0}"#, br#"{"keep":false,"n":1}"#],
    )
    .await;
    let state = state_for_codec(&bootstrap, Arc::new(JsonEchoCodec)).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "auto-filter-group".into(),
            topics: vec!["auto-filter-topic".into()],
            auto_commit: true,
            filter: "keep = true".into(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(
        inbound,
        state.clone(),
        principal,
        host,
    ));

    let mut committed = None;
    for _ in 0..100 {
        tokio::select! {
            item = out.next() => match item {
                Some(Ok(msg)) => panic!("fully filtered subscription emitted record: {msg:?}"),
                Some(Err(e)) => panic!("fully filtered subscription failed: {e:?}"),
                None => break,
            },
            () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
        committed = admin
            .list_consumer_group_offsets("auto-filter-group")
            .await
            .unwrap()
            .get(&("auto-filter-topic".to_string(), 0))
            .copied();
        if committed.is_some() {
            break;
        }
    }

    check!(committed == Some(2));

    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), out.next()).await;

    let replay_start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "auto-filter-group".into(),
            topics: vec!["auto-filter-topic".into()],
            auto_commit: false,
            filter: String::new(),
        })),
    };
    let (replay_tx, replay_rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    replay_tx.send(Ok(replay_start)).unwrap();
    let replay_inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(replay_rx),
    ));
    let (principal, host) = anon();
    let mut replay = Box::pin(streaming::subscribe_inner(
        replay_inbound,
        state,
        principal,
        host,
    ));

    let replayed = tokio::time::timeout(std::time::Duration::from_millis(750), replay.next()).await;
    drop(replay_tx);
    check!(replayed.is_err());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "arrow")]
async fn subscribe_filter_uses_batched_arrow_masks_and_preserves_raw_ipc_bytes() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "arrow-filter-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let rejected = arrow_ipc_order_record(&["PENDING", "PAID"], &[200.0, 50.0]);
    let accepted = arrow_ipc_order_record(&["PENDING", "PAID"], &[10.0, 125.0]);
    produce_bytes_records(
        &bootstrap,
        "arrow-filter-topic",
        &[rejected.clone(), accepted.clone()],
    )
    .await;
    let state = state_for(&bootstrap).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "arrow-filter-group".into(),
            topics: vec!["arrow-filter-topic".into()],
            auto_commit: true,
            filter: "status = 'PAID' AND price > 100".into(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, principal, host));

    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("matching Arrow IPC record arrives")
        .expect("stream item")
        .expect("inbound ok");
    check!(msg.offset == 1);
    check!(msg.value == accepted.to_vec());

    let no_second_match =
        tokio::time::timeout(std::time::Duration::from_millis(750), out.next()).await;
    drop(tx);
    check!(no_second_match.is_err());
    check!(
        await_committed_offset(&mut admin, "arrow-filter-group", "arrow-filter-topic", 0).await
            == Some(2)
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "arrow")]
async fn subscribe_arrow_filter_commits_fully_filtered_non_empty_poll() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "arrow-filtered-commit-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let first = arrow_ipc_order_record(&["PENDING"], &[200.0]);
    let second = arrow_ipc_order_record(&["PAID"], &[50.0]);
    produce_bytes_records(&bootstrap, "arrow-filtered-commit-topic", &[first, second]).await;
    let state = state_for(&bootstrap).await;

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "arrow-filtered-commit-group".into(),
            topics: vec!["arrow-filtered-commit-topic".into()],
            auto_commit: true,
            filter: "status = 'PAID' AND price > 100".into(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let (principal, host) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(
        inbound,
        state.clone(),
        principal,
        host,
    ));

    let mut committed = None;
    for _ in 0..100 {
        tokio::select! {
            item = out.next() => match item {
                Some(Ok(msg)) => panic!("fully filtered Arrow subscription emitted record: {msg:?}"),
                Some(Err(e)) => panic!("fully filtered Arrow subscription failed: {e:?}"),
                None => break,
            },
            () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
        committed = admin
            .list_consumer_group_offsets("arrow-filtered-commit-group")
            .await
            .unwrap()
            .get(&("arrow-filtered-commit-topic".to_string(), 0))
            .copied();
        if committed.is_some() {
            break;
        }
    }

    check!(committed == Some(2));

    drop(tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), out.next()).await;

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_preserves_header_order_duplicates_and_null_values() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "header-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .acks(Acks::All)
        .build()
        .await
        .unwrap();
    producer
        .send(ProducerRecord {
            topic: "header-topic".into(),
            value: Some(Bytes::from_static(b"hello")),
            headers: vec![
                ProducerHeader {
                    key: "duplicate".into(),
                    value: Some(Bytes::from_static(b"first")),
                },
                ProducerHeader {
                    key: "null".into(),
                    value: None,
                },
                ProducerHeader {
                    key: "duplicate".into(),
                    value: Some(Bytes::from_static(b"last")),
                },
            ],
            ..ProducerRecord::default()
        })
        .await
        .await
        .unwrap()
        .unwrap();
    producer.close().await.unwrap();

    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "header-group".into(),
            topics: vec!["header-topic".into()],
            auto_commit: true,
            filter: String::new(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));

    let (p, h) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, p, h));
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), out.next())
        .await
        .expect("record arrives")
        .expect("stream item")
        .expect("inbound ok");
    drop(tx);

    check!(msg.topic == "header-topic");
    check!(msg.value == b"hello");
    assert_eq!(
        msg.headers,
        vec![
            pb::Header {
                key: "duplicate".into(),
                value: Some(b"first".to_vec()),
            },
            pb::Header {
                key: "null".into(),
                value: None,
            },
            pb::Header {
                key: "duplicate".into(),
                value: Some(b"last".to_vec()),
            },
        ]
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_wrappers_and_router_build() {
    use connectrpc_axum::message::{ConnectError as CErr, ConnectRequest};

    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "wrap-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    // Router builds with both streaming methods registered (covers lib::router).
    let _router = crabka_grpc_gateway::router(state.clone());

    // send_stream wrapper → Ok with a StreamBody (covers the wrapper).
    let send_input = futures_util::stream::iter(vec![Ok::<_, CErr>(pb::SendRequest {
        records: vec![rec("wrap-topic", b"x")],
        acks: 0,
    })]);
    let send_req = ConnectRequest(Streaming::new(Box::pin(send_input)));
    let send_resp =
        streaming::send_stream(axum::Extension(state.clone()), None, None, send_req).await;
    check!(send_resp.is_ok());

    // subscribe wrapper → Ok (inner stream is lazy; not driven here).
    let sub_input = futures_util::stream::iter(Vec::<Result<pb::SubscribeFrame, CErr>>::new());
    let sub_req = ConnectRequest(Streaming::new(Box::pin(sub_input)));
    let sub_resp = streaming::subscribe(axum::Extension(state.clone()), None, None, sub_req).await;
    check!(sub_resp.is_ok());

    broker.shutdown().await;
}

/// Connect proto content-type regression: a connect-go client posts a unary
/// `application/proto` request and requires the 200 response to echo it. An
/// all-default `SendRequest` (no records) encodes to an empty body and the
/// `Send` handler returns 200 without producing. Before the `.build_connect()`
/// fix the router replied `application/json`, which proto clients reject with
/// `invalid content-type: "application/json"; expecting "application/proto"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_echoes_proto_content_type() {
    use axum::{
        body::Body,
        http::{Method, Request, header::CONTENT_TYPE},
    };
    use tower::ServiceExt as _;

    let (broker, bootstrap, _dir) = boot().await;
    let state = state_for(&bootstrap).await;
    let app = crabka_grpc_gateway::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/crabka.gateway.v1.Gateway/Send")
                .header(CONTENT_TYPE, "application/proto")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds");

    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    check!(status.is_success());
    check!(content_type.starts_with("application/proto"));

    broker.shutdown().await;
}

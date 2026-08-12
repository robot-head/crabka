//! Differential test: produce a record through the gateway's `ProduceCore`
//! against a host-advertised in-process broker, then read it back with the JVM
//! `kafka-console-consumer` from a cp-kafka container. This test needs Docker.

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::{codec::RawCodec, produce::ProduceCore, types::GatewayRecord};
use crabka_security::ListenerProtocol;

const HOST_BOOTSTRAP: &str = "127.0.0.1:19092";
const DOCKER_BOOTSTRAP: &str = "host.docker.internal:19094";
const IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_consumer_reads_gateway_output() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Give host-side Rust clients and the Dockerized JVM consumer distinct
    // listener identities. Metadata then advertises an address reachable from
    // the same network that made the request.
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = HOST_BOOTSTRAP.parse().unwrap();
    config.advertised_listener = HOST_BOOTSTRAP.into();
    config.listeners = vec![
        ListenerSpec {
            name: "HOST".into(),
            bind_addr: HOST_BOOTSTRAP.parse().unwrap(),
            advertised: HOST_BOOTSTRAP.into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        },
        ListenerSpec {
            name: "DOCKER".into(),
            bind_addr: "0.0.0.0:19094".parse().unwrap(),
            advertised: DOCKER_BOOTSTRAP.into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        },
    ];
    config.inter_broker_listener_name = "HOST".into();
    let broker: BrokerHandle = Broker::start(config).await.expect("broker");

    let mut admin = AdminClient::connect(&[HOST_BOOTSTRAP.to_string()])
        .await
        .expect("admin");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "gw-jvm".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            crabka_units::secs(10),
        )
        .await
        .expect("create");

    let core = ProduceCore::new(HOST_BOOTSTRAP, "gw-jvm", Arc::new(RawCodec), None)
        .await
        .expect("core");
    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    core.produce(
        GatewayRecord {
            topic: "gw-jvm".into(),
            key: None,
            value: Bytes::from_static(b"jvm-sees-this"),
            body_structured: None,
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        },
        &anon,
    )
    .await
    .expect("produce");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            IMAGE,
            "kafka-console-consumer",
            "--bootstrap-server",
            DOCKER_BOOTSTRAP,
            "--topic",
            "gw-jvm",
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "10000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("docker run");
    let s = String::from_utf8_lossy(&out.stdout);
    assert2::assert!(s.contains("jvm-sees-this"));

    broker.shutdown().await;
}

//! Differential: produce a record via the gateway's `ProduceCore` against a
//! host-advertised in-process broker, then read it back with the JVM
//! `kafka-console-consumer` from a cp-kafka container. Requires Docker.

use std::collections::BTreeMap;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::types::GatewayRecord;

const BOOTSTRAP: &str = "host.docker.internal:9092";
const IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_consumer_reads_gateway_output() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Build a host-advertised BrokerConfig modeled on broker/tests/jvm_acceptance.rs.
    // `for_tests` gives us port-0 ephemeral defaults; we override listen_addr and
    // advertised_listener so Docker containers can reach the broker via
    // `host.docker.internal:9092`.
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = "0.0.0.0:9092".parse().unwrap();
    config.advertised_listener = BOOTSTRAP.into();
    let broker: BrokerHandle = Broker::start(config).await.expect("broker");

    let mut admin = AdminClient::connect(&[BOOTSTRAP.to_string()])
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
            10_000,
        )
        .await
        .expect("create");

    let core = ProduceCore::new(BOOTSTRAP, "gw-jvm", Arc::new(RawCodec), None)
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
            BOOTSTRAP,
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
    assert!(
        s.contains("jvm-sees-this"),
        "JVM consumer output: {s:?} / err {}",
        String::from_utf8_lossy(&out.stderr)
    );

    broker.shutdown().await;
}

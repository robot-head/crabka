//! Differential: produce a record via the gateway's `ProduceCore` against a
//! host-advertised in-process broker, then read it back with the JVM
//! `kafka-console-consumer` from a cp-kafka container. Requires Docker.

use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr, ToSocketAddrs as _},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::{codec::RawCodec, produce::ProduceCore, types::GatewayRecord};

const HOST_PORT: u16 = 9092;
const DOCKER_HOST: &str = "host.docker.internal";
const IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";

fn shared_bootstrap() -> String {
    if (DOCKER_HOST, HOST_PORT)
        .to_socket_addrs()
        .is_ok_and(|mut addrs| addrs.next().is_some())
    {
        return format!("{DOCKER_HOST}:{HOST_PORT}");
    }

    let out = Command::new("docker")
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
        ])
        .output()
        .expect("docker network inspect");
    assert2::assert!(
        out.status.success(),
        "docker network inspect failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let gateway: IpAddr = String::from_utf8(out.stdout)
        .expect("Docker bridge gateway is UTF-8")
        .trim()
        .parse()
        .expect("Docker bridge gateway is an IP address");
    SocketAddr::new(gateway, HOST_PORT).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_consumer_reads_gateway_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bootstrap = shared_bootstrap();

    // Build a host-advertised BrokerConfig modeled on broker/tests/jvm_acceptance.rs.
    // `for_tests` gives us port-0 ephemeral defaults; we override listen_addr and
    // advertised_listener with one address both this host process and the Kafka
    // container can reach.
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = SocketAddr::from(([0, 0, 0, 0], HOST_PORT));
    config.advertised_listener = bootstrap.clone();
    let broker: BrokerHandle = Broker::start(config).await.expect("broker");

    let mut admin = AdminClient::connect(&[bootstrap.clone()])
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

    let core = ProduceCore::new(&bootstrap, "gw-jvm", Arc::new(RawCodec), None)
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
            &bootstrap,
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

//! Integration tests against a real Kafka with testcontainers.
//!
//! All tests are gated with `#[ignore]` so `cargo test --workspace` does not
//! pull Docker by default. Run with:
//!
//! ```text
//! cargo test -p crabka-client-core --test integration -- --ignored --nocapture
//! ```
//!
//! Each test starts a fresh `mirror.gcr.io/confluentinc/cp-kafka:6.1.1`
//! container and removes it when the test exits.
//!
//! ## Why the Confluent image (and not `apache/kafka-native`)?
//!
//! `testcontainers-modules` v0.10 ships two Kafka modules: `apache` (using the
//! `apache/kafka-native:3.8.0` `KRaft` image) and `confluent` (using
//! `mirror.gcr.io/confluentinc/cp-kafka:6.1.1`). The `apache` module sets up
//! advertised listeners with a two-step trick: the container's `cmd` polls for
//! a `testcontainers_start.sh` file that `exec_after_start` writes once the
//! mapped host port is known. In CI this races. The broker binds its TCP
//! listener on `0.0.0.0:9092`, and Docker's userland proxy accepts connections
//! on the mapped port, before `KRaft` initialization finishes. The first
//! client request therefore lands on a half-initialized broker that resets the
//! connection mid-RPC. That surfaces in Crabka as `ClientError::Disconnected`
//! on the bootstrap `ApiVersions` roundtrip.
//!
//! The Confluent module uses the standard `kafka-configs --alter` pattern in
//! `exec_after_start` and waits for "Creating new log file" before it returns.
//! `start().await` therefore returns a fully-warm broker with correct
//! advertised listeners. The upstream `testcontainers-modules` Kafka examples
//! use this same image.
//!
//! ## Why a retry helper on the first request?
//!
//! Even with the Confluent module's `WaitFor` strategy, the "Creating new log
//! file" log line fires while the broker still finishes controller election
//! and `ApiVersions` table construction. On slow CI runners the first
//! `ApiVersions` RPC sometimes lands mid-bringup and the broker resets the
//! TCP stream. Crabka surfaces this as `ClientError::Disconnected`. To keep
//! the suite robust without a dependency on broker internals, every test
//! bootstraps with [`bootstrap_client`], which retries the initial
//! `ApiVersions` send for up to ~15s before it gives up.

// Skip compilation on Windows runners where testcontainers + Docker reliability
// is poor. On Linux CI the tests run via the `client-core-integration` job.

use std::time::Duration;

use assert2::{assert, check};
use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use testcontainers::runners::AsyncRunner;
// `testcontainers_modules::kafka` re-exports `confluent::*`, so the bare
// `Kafka` here is the Confluent module's container type.
use testcontainers_modules::kafka::{KAFKA_PORT, Kafka};

/// Maximum attempts for the initial `ApiVersions` round-trip while the broker
/// still warms up. With a 1s pause between attempts this gives ~15s of
/// tolerance, which empirically covers slow CI runners.
const BOOTSTRAP_MAX_ATTEMPTS: u32 = 15;
const BOOTSTRAP_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Initialise a per-test tracing subscriber so `--nocapture` runs surface
/// client-side connection and dispatch logs. It is safe to call this many
/// times.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("crabka_client_core=debug,info")
        .with_test_writer()
        .try_init();
}

/// Start a Kafka container and return the container handle + bootstrap address.
async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let kafka = Kafka::default()
        .start()
        .await
        .expect("kafka container failed to start");
    let port = kafka
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to get mapped port");
    (kafka, format!("127.0.0.1:{port}"))
}

/// Build a `Client` and drive the bootstrap `ApiVersions` round-trip with
/// retry on `ClientError::Disconnected`.
///
/// This function returns a primed `Client` whose internal version map has
/// already been negotiated against the broker.
///
/// `Client::builder().build()` is lazy: it does not open a TCP connection.
/// The first `send` is what actually negotiates `ApiVersions`, so a retry of
/// `send` is enough and the client needs no rebuild. But on a hard
/// `Disconnected` the underlying reader task has exited, so this function
/// rebuilds the `Client` on each retry to get a fresh writer/reader pair.
async fn bootstrap_client(bootstrap: &str) -> Client {
    let mut last_err: Option<ClientError> = None;
    for attempt in 1..=BOOTSTRAP_MAX_ATTEMPTS {
        let client = Client::builder()
            .bootstrap(bootstrap)
            .client_id("crabka-integration")
            .build()
            .await
            .expect("client build failed");

        match client.send(ApiVersionsRequest::default()).await {
            Ok(_) => {
                tracing::info!(attempt, "broker ready, bootstrap ApiVersions succeeded");
                return client;
            }
            Err(ClientError::Disconnected) => {
                tracing::warn!(
                    attempt,
                    max = BOOTSTRAP_MAX_ATTEMPTS,
                    "bootstrap ApiVersions returned Disconnected; broker likely still warming up"
                );
                client.close();
                last_err = Some(ClientError::Disconnected);
                // real-time wait (not a progress poll): retry/backoff between full
                // ApiVersions RPC round-trips while a real Docker broker warms up;
                // iteration-count-bounded by BOOTSTRAP_MAX_ATTEMPTS.
                tokio::time::sleep(BOOTSTRAP_RETRY_DELAY).await;
            }
            Err(e) => panic!("bootstrap ApiVersions failed with non-retryable error: {e}"),
        }
    }
    panic!(
        "broker never accepted ApiVersions after {BOOTSTRAP_MAX_ATTEMPTS} attempts; \
         last error: {last_err:?}"
    );
}

// ── tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Docker"]
async fn api_versions_against_real_broker() {
    init_tracing();
    let (kafka, bootstrap) = start_kafka().await;
    let client = bootstrap_client(&bootstrap).await;

    // KIP-511: at v3+, brokers reject empty `client_software_name`/`version`
    // with `INVALID_REQUEST` (error 42). The regex `^[a-zA-Z0-9.\-]+$` must
    // match each field.
    let resp = client
        .send(ApiVersionsRequest {
            client_software_name: "crabka".into(),
            client_software_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions failed");

    check!(
        resp.error_code == 0,
        "ApiVersions returned error: {}",
        resp.error_code
    );
    check!(!resp.api_keys.is_empty(), "broker advertised no APIs");
    // ApiVersions (key 18) is always present in any modern Kafka broker.
    check!(
        resp.api_keys.iter().any(|k| k.api_key == 18),
        "ApiVersions key 18 not found in response"
    );

    client.close();
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn metadata_against_real_broker() {
    init_tracing();
    let (kafka, bootstrap) = start_kafka().await;
    let client = bootstrap_client(&bootstrap).await;

    let resp = client
        .refresh_metadata()
        .await
        .expect("refresh_metadata failed");
    assert!(
        !resp.brokers.is_empty(),
        "expected at least one broker in metadata"
    );

    client.close();
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_then_delete_topic() {
    use crabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    };

    init_tracing();
    let (kafka, bootstrap) = start_kafka().await;
    let client = bootstrap_client(&bootstrap).await;

    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "crabka-test-topic".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let create_resp = client.send(create).await.expect("CreateTopics failed");
    let topic_result = &create_resp.topics[0];
    assert!(
        topic_result.error_code == 0,
        "CreateTopics error: {topic_result:?}"
    );

    // The schema's split between v0-5 (`topic_names: []string`) and v6+
    // (`topics: []DeleteTopicState`) is settled by the encoder per the
    // negotiated version. Populate both so the test works against any broker.
    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("crabka-test-topic".into()),
            ..Default::default()
        }],
        topic_names: vec!["crabka-test-topic".into()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let delete_resp = client.send(delete).await.expect("DeleteTopics failed");
    let del_result = &delete_resp.responses[0];
    assert!(
        del_result.error_code == 0,
        "DeleteTopics error: {del_result:?}"
    );

    client.close();
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn list_topics() {
    use crabka_protocol::owned::metadata_request::MetadataRequest;

    init_tracing();
    let (kafka, bootstrap) = start_kafka().await;
    let client = bootstrap_client(&bootstrap).await;

    // `MetadataRequest::default()` has `topics = None`, which lists all topics.
    let resp = client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata failed");
    // Smoke-test: we can decode the response without errors.
    let _ = resp.topics;

    client.close();
    drop(kafka);
}

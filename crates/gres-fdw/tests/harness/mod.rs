#![cfg(feature = "roundtrip")]

//! In-process bring-up for the Kafka FDW round-trip test.
//!
//! [`KafkaStack`] starts a single-node crabka broker (`KRaft`, ephemeral port)
//! and a Confluent-compatible schema registry served over a real ephemeral
//! HTTP socket, then exposes:
//!
//! * the broker bootstrap address (`bootstrap`),
//! * the registry base URL (`registry_url`),
//! * topic creation (`create_topic`),
//! * Avro registration (`register_avro`) → schema id,
//! * record production (`produce`) → assigned offset.
//!
//! Readiness is **condition-driven** (broker metadata fetch succeeds; registry
//! `GET /subjects` returns 200), each bounded by a timeout — never a fixed
//! sleep.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    metadata_request::MetadataRequest,
};
use crabka_schema_registry::{
    config::{RegistryConfig, RegistryRuntimeConfig, SecurityConfig},
    format::SchemaType,
    kafkastore::{KafkaStore, RegisterSchema, record::SchemaReference},
    rest::{self, AppState},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

/// How long any readiness condition is allowed to take before the test fails.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll cadence while waiting on a readiness condition.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A live in-process broker + schema registry.
///
/// Holds every resource (broker handle, registry store, cancellation token,
/// temp log dir) so the stack stays up for the lifetime of the value and is
/// torn down on drop / [`KafkaStack::shutdown`].
pub struct KafkaStack {
    broker: Option<BrokerHandle>,
    bootstrap: String,
    registry_url: String,
    cancel: CancellationToken,
    store: Arc<KafkaStore>,
    producer: Producer,
    _log_dir: TempDir,
}

impl KafkaStack {
    /// Bring up a single-node broker + registry on ephemeral ports and wait
    /// until both are serving requests.
    ///
    /// # Panics
    /// Panics (via `expect`) if any component fails to start or does not become
    /// ready within [`READY_TIMEOUT`] — a test harness wants a loud failure.
    pub async fn start() -> Self {
        crabka_fdw_install_provider();

        // ── broker ───────────────────────────────────────────────────────
        let log_dir = TempDir::new().expect("broker temp log dir");
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();

        // Readiness: a metadata fetch over a fresh short-lived client succeeds.
        wait_for_broker(&bootstrap).await;

        // ── schema registry (KafkaStore + axum router over a real socket) ──
        let cancel = CancellationToken::new();
        let cfg = RegistryConfig {
            bootstrap: bootstrap.clone(),
            schemas_topic: "_schemas".into(),
            schemas_topic_rf: 1,
            client_id: "kafka-fdw-it-sr".into(),
            advertised_url: "http://127.0.0.1:0".into(),
            group_id: "schema-registry".into(),
            leader_eligibility: true,
            security: SecurityConfig::default(),
            runtime: RegistryRuntimeConfig::default(),
        };
        let store = KafkaStore::start(&cfg, cancel.clone())
            .await
            .expect("schema registry store start");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("registry http bind");
        let registry_addr = listener.local_addr().expect("registry addr");
        let registry_url = format!("http://{registry_addr}");

        let app = rest::router(AppState {
            store: Arc::clone(&store),
        });
        let serve_cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = rest::serve::serve_http(listener, app, serve_cancel).await;
        });

        // Readiness: GET /subjects returns 200.
        wait_for_registry(&registry_url).await;

        // ── producer (acks=all so sends are durable + return real offsets) ─
        let producer = Producer::builder()
            .bootstrap(bootstrap.clone())
            .client_id("kafka-fdw-it-producer")
            .acks(Acks::All)
            .linger(Duration::from_millis(5))
            .build()
            .await
            .expect("producer build");

        Self {
            broker: Some(broker),
            bootstrap,
            registry_url,
            cancel,
            store,
            producer,
            _log_dir: log_dir,
        }
    }

    /// The broker bootstrap address (`host:port`).
    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    /// The schema-registry base URL (`http://host:port`).
    pub fn registry_url(&self) -> &str {
        &self.registry_url
    }

    /// Create `name` with `partitions` partitions (replication factor 1).
    ///
    /// # Panics
    /// Panics if the `CreateTopics` RPC fails or the broker rejects the topic.
    pub async fn create_topic(&self, name: &str, partitions: i32) {
        let client = Client::builder()
            .bootstrap(self.bootstrap.clone())
            .client_id("kafka-fdw-it-admin")
            .build()
            .await
            .expect("admin client");
        let resp = client
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
        assert_eq!(
            resp.topics[0].error_code, 0,
            "create_topic({name}) failed: {resp:?}"
        );
        client.close();
    }

    /// Register an Avro schema for `subject` (typically `"<topic>-value"`) and
    /// return the assigned Confluent schema id.
    ///
    /// # Panics
    /// Panics if registration fails.
    pub async fn register_avro(&self, subject: &str, schema_json: &str) -> u32 {
        let no_refs: &[SchemaReference] = &[];
        let reg = self
            .store
            .register(RegisterSchema {
                subject,
                ty: SchemaType::Avro,
                schema: schema_json,
                references: no_refs,
                message_type: None,
                import_id: None,
                import_version: None,
            })
            .await
            .expect("register avro schema");
        u32::try_from(reg.id.0).expect("schema id fits in u32")
    }

    /// Register a JSON Schema for `subject` and return its Confluent schema id.
    ///
    /// # Panics
    /// Panics if registration fails.
    pub async fn register_json(&self, subject: &str, schema_json: &str) -> u32 {
        let no_refs: &[SchemaReference] = &[];
        let reg = self
            .store
            .register(RegisterSchema {
                subject,
                ty: SchemaType::Json,
                schema: schema_json,
                references: no_refs,
                message_type: None,
                import_id: None,
                import_version: None,
            })
            .await
            .expect("register JSON schema");
        u32::try_from(reg.id.0).expect("schema id fits in u32")
    }

    /// Register a Protobuf schema for `subject` and return the assigned
    /// Confluent schema id.
    ///
    /// # Panics
    /// Panics if registration fails.
    pub async fn register_protobuf(
        &self,
        subject: &str,
        schema_proto: &str,
        message_type: Option<&str>,
    ) -> u32 {
        self.register_protobuf_with_references(subject, schema_proto, message_type, &[])
            .await
    }

    /// Register a Protobuf schema with Schema Registry references.
    ///
    /// # Panics
    /// Panics if registration fails.
    pub async fn register_protobuf_with_references(
        &self,
        subject: &str,
        schema_proto: &str,
        message_type: Option<&str>,
        references: &[SchemaReference],
    ) -> u32 {
        let reg = self
            .store
            .register(RegisterSchema {
                subject,
                ty: SchemaType::Protobuf,
                schema: schema_proto,
                references,
                message_type,
                import_id: None,
                import_version: None,
            })
            .await
            .expect("register protobuf schema");
        u32::try_from(reg.id.0).expect("schema id fits in u32")
    }

    /// Produce one record to `topic` partition `partition` with the given
    /// (already framed) `value` bytes, returning the assigned offset.
    ///
    /// `value` is the raw on-wire payload: for Avro it must be the Confluent
    /// frame (`magic | id | avro-body`); for the raw-fallback path it is the
    /// verbatim bytes.
    ///
    /// # Panics
    /// Panics if the broker does not ack the record.
    pub async fn produce(&self, topic: &str, partition: i32, value: Bytes) -> i64 {
        self.produce_with_headers(topic, partition, value, Vec::new())
            .await
    }

    /// Produce one record with headers to `topic` partition `partition`.
    ///
    /// # Panics
    /// Panics if the broker does not ack the record.
    pub async fn produce_with_headers(
        &self,
        topic: &str,
        partition: i32,
        value: Bytes,
        headers: Vec<Header>,
    ) -> i64 {
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: topic.into(),
                partition: Some(partition),
                value: Some(value),
                headers,
                ..Default::default()
            })
            .await;
        self.producer.flush().await.expect("producer flush");
        let meta = ack
            .await
            .expect("produce ack oneshot")
            .expect("produce ack result");
        meta.offset
    }

    /// Tear down the registry and broker. Idempotent-ish: safe to call once.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(broker) = self.broker.take() {
            broker.shutdown().await;
        }
    }
}

impl Drop for KafkaStack {
    fn drop(&mut self) {
        // Best-effort: signal the registry serve task to stop. The broker
        // handle's async `shutdown` can only run from `shutdown(self)`; on a
        // plain drop the temp dir + process exit reclaim the rest.
        self.cancel.cancel();
    }
}

/// Install the rustcrypto rustls provider used by the FDW + crabka clients.
fn crabka_fdw_install_provider() {
    crabka_gres_fdw::provider::install_default_provider();
}

/// Wait until a `Metadata` RPC against `bootstrap` succeeds.
async fn wait_for_broker(bootstrap: &str) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last_err = String::from("(no attempt)");
    while Instant::now() < deadline {
        match Client::builder()
            .bootstrap(bootstrap.to_string())
            .client_id("kafka-fdw-it-ready")
            .build()
            .await
        {
            Ok(client) => {
                let res = client.send(MetadataRequest::default()).await;
                client.close();
                if res.is_ok() {
                    return;
                }
                last_err = format!("metadata: {:?}", res.err());
            }
            Err(e) => last_err = format!("connect: {e}"),
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    panic!("broker {bootstrap} not ready within {READY_TIMEOUT:?}: {last_err}");
}

/// Wait until `GET {registry_url}/subjects` returns HTTP 200.
async fn wait_for_registry(registry_url: &str) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let url = format!("{registry_url}/subjects");
    let mut last = String::from("(no attempt)");
    while Instant::now() < deadline {
        match http_get(&url).await {
            Ok((200, _)) => return,
            Ok((code, _)) => last = format!("status {code}"),
            Err(e) => last = format!("error {e}"),
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    panic!("registry {url} not ready within {READY_TIMEOUT:?}: {last}");
}

/// Minimal HTTP/1.1 GET returning `(status, body)`, using only `tokio` (no
/// extra HTTP-client dev-dependency). Sufficient for a readiness probe /
/// diagnostic against the local registry.
async fn http_get(url: &str) -> Result<(u16, String), String> {
    let rest = url.strip_prefix("http://").ok_or("url must be http://")?;
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .map_err(|e| e.to_string())?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| e.to_string())?;
    let raw = String::from_utf8_lossy(&buf);
    // Status line: `HTTP/1.1 200 OK`.
    let status_line = raw.lines().next().ok_or("empty response")?;
    let code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("no status code")?
        .parse::<u16>()
        .map_err(|e| e.to_string())?;
    // Body follows the blank line separating headers from the payload.
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok((code, body))
}

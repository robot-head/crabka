//! Subprocess adapter harness for SDK conformance vectors.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::{
    codec::{CodecError, Decoded, EncodeBody, RecordCodec},
    config::GatewayConfig,
    produce::ProduceCore,
    serve,
    state::AppState,
};
use crabka_units::prelude::*;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, BufWriter},
    net::TcpListener,
    process::{Child, ChildStdin, ChildStdout, Command as ProcessCommand},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    protocol::{CONTRACT_MAJOR, Command, Response},
    vectors::{ContractVersion, Vector, VectorError, load_vectors},
};

const LIVE_COMPATIBLE_VECTOR_IDS: &[&str] = &[
    "messaging_roundtrip",
    "ce_binary_mapping",
    "filter_delivers_matches_only",
    "header_shape",
    "queue_v1_1_ack_error_shape",
    "queue_v1_1_ack_shape",
    "queue_v1_1_acquire_error_shape",
    "queue_v1_1_acquire_shape",
    "queue_v1_1_live_error_mapping",
    "queue_v1_1_lock_duration_error_shape",
    "queue_v1_1_renew_shape",
    "queue_v1_1_session_ownership",
];
const ADAPTER_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Harness configuration.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    /// Adapter executable path.
    pub adapter: PathBuf,
    /// Adapter executable arguments.
    pub adapter_args: Vec<String>,
    /// Vector directory.
    pub vectors_dir: PathBuf,
    /// Optional vector id filter.
    pub filter: Option<String>,
    /// Gateway endpoint sent through `Configure`.
    pub endpoint: String,
    /// Substrate used by the harness.
    pub substrate: HarnessSubstrate,
    /// Run only explicit live-only vectors supported by the current live Rust app SDK.
    pub live_compatible_only: bool,
}

/// Substrate used for adapter calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessSubstrate {
    /// Use the configured endpoint as-is.
    External,
    /// Boot an in-process broker and plaintext h2c gateway on `127.0.0.1:0`.
    Live,
}

/// SDK conformance harness.
#[derive(Debug, Clone)]
pub struct Harness {
    config: HarnessConfig,
}

impl Harness {
    /// Create a harness.
    #[must_use]
    pub fn new(config: HarnessConfig) -> Self {
        Self { config }
    }

    /// Load vectors and run them against the adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when vectors cannot be loaded, the adapter or live
    /// substrate cannot start, adapter I/O fails, or shutdown times out.
    pub async fn run(&self) -> Result<RunSummary, HarnessError> {
        let adapter_version = self.discover_adapter_version().await?;
        let mut vectors = load_vectors(&self.config.vectors_dir)?;
        let mut skipped = newer_contract_skips(adapter_version, &vectors);
        vectors.retain(|vector| adapter_version.satisfies(vector));
        if let Some(filter) = &self.config.filter {
            vectors.retain(|vector| vector.id == *filter);
            skipped.retain(|skipped| skipped.vector_id == *filter);
        }
        if self.config.substrate != HarnessSubstrate::Live {
            let live_only = vectors
                .iter()
                .filter(|vector| vector.live_only)
                .map(|vector| SkippedVector {
                    vector_id: vector.id.clone(),
                    reason: "requires live substrate".into(),
                })
                .collect::<Vec<_>>();
            vectors.retain(|vector| !vector.live_only);
            skipped.extend(live_only);
        }
        let live_plan = if self.config.live_compatible_only {
            LiveCompatiblePlan::from_full_vectors(vectors)
        } else {
            LiveCompatiblePlan::full_vectors(vectors)
        };
        skipped.extend(live_plan.skipped);
        match self.config.substrate {
            HarnessSubstrate::External => {
                self.run_vectors_with_endpoint(live_plan.vectors, skipped, &self.config.endpoint)
                    .await
            }
            HarnessSubstrate::Live => {
                let topic_names = topic_names_for_vectors(&live_plan.vectors);
                let live = LiveSubstrate::boot(&topic_names).await?;
                let endpoint = live.endpoint.clone();
                let result = self
                    .run_vectors_with_endpoint(live_plan.vectors, skipped, &endpoint)
                    .await;
                live.shutdown().await?;
                result
            }
        }
    }

    /// Run already-loaded vectors.
    ///
    /// # Errors
    ///
    /// Returns an error when the adapter cannot start, adapter I/O fails, or an
    /// adapter call times out.
    pub async fn run_vectors(&self, vectors: Vec<Vector>) -> Result<RunSummary, HarnessError> {
        self.run_vectors_with_endpoint(vectors, vec![], &self.config.endpoint)
            .await
    }

    async fn run_vectors_with_endpoint(
        &self,
        vectors: Vec<Vector>,
        skipped: Vec<SkippedVector>,
        endpoint: &str,
    ) -> Result<RunSummary, HarnessError> {
        let mut passed = 0;
        let mut failed = vec![];
        for vector in vectors {
            match self.run_vector(&vector, endpoint).await? {
                Some(failure) => failed.push(failure),
                None => passed += 1,
            }
        }
        Ok(RunSummary {
            passed,
            failed,
            skipped,
        })
    }

    async fn run_vector(
        &self,
        vector: &Vector,
        endpoint: &str,
    ) -> Result<Option<VectorFailure>, HarnessError> {
        let mut adapter = AdapterProcess::spawn(&self.config.adapter, &self.config.adapter_args)?;
        let hello = adapter.call(Command::Hello).await?;
        match hello {
            Response::Hello {
                contract_major,
                contract_minor,
                ..
            } if ContractVersion::new(contract_major, contract_minor).satisfies(vector) => {}
            actual => {
                let expected = Response::Hello {
                    contract_major: CONTRACT_MAJOR,
                    contract_minor: vector.since.minor,
                    language: "<adapter>".into(),
                };
                return Ok(Some(VectorFailure {
                    vector_id: vector.id.clone(),
                    step: "hello".into(),
                    expected,
                    actual,
                }));
            }
        }
        let configured = adapter
            .call(Command::Configure {
                endpoint: endpoint.to_string(),
                bearer: None,
            })
            .await?;
        let expected_configuration =
            Response::Ok(serde_json::json!({ "bearer_configured": false }));
        if configured != expected_configuration {
            return Ok(Some(VectorFailure {
                vector_id: vector.id.clone(),
                step: "configure".into(),
                expected: expected_configuration,
                actual: configured,
            }));
        }
        for step in &vector.steps {
            let actual = adapter.call(step.command.clone()).await?;
            if actual != step.expect {
                return Ok(Some(VectorFailure {
                    vector_id: vector.id.clone(),
                    step: step.name.clone(),
                    expected: step.expect.clone(),
                    actual,
                }));
            }
        }
        Ok(None)
    }

    async fn discover_adapter_version(&self) -> Result<ContractVersion, HarnessError> {
        let mut adapter = AdapterProcess::spawn(&self.config.adapter, &self.config.adapter_args)?;
        let hello = adapter.call(Command::Hello).await?;
        let Response::Hello {
            contract_major,
            contract_minor,
            language: _,
        } = hello
        else {
            return Err(HarnessError::AdapterProtocol(
                "adapter hello did not return a hello response",
            ));
        };
        if contract_major != CONTRACT_MAJOR {
            return Err(HarnessError::AdapterProtocol(
                "adapter contract major is not supported",
            ));
        }
        Ok(ContractVersion::new(contract_major, contract_minor))
    }
}

fn newer_contract_skips(
    adapter_version: ContractVersion,
    vectors: &[Vector],
) -> Vec<SkippedVector> {
    vectors
        .iter()
        .filter(|vector| adapter_version < vector.since)
        .map(|vector| SkippedVector {
            vector_id: vector.id.clone(),
            reason: format!(
                "requires contract {}.{}; adapter declares {}.{}",
                vector.since.major,
                vector.since.minor,
                adapter_version.major,
                adapter_version.minor
            ),
        })
        .collect()
}

struct LiveCompatiblePlan {
    vectors: Vec<Vector>,
    skipped: Vec<SkippedVector>,
}

impl LiveCompatiblePlan {
    fn full_vectors(vectors: Vec<Vector>) -> Self {
        Self {
            vectors,
            skipped: vec![],
        }
    }

    fn from_full_vectors(vectors: Vec<Vector>) -> Self {
        let mut live_vectors = Vec::new();
        let mut skipped = Vec::new();

        for vector in vectors {
            if !LIVE_COMPATIBLE_VECTOR_IDS.contains(&vector.id.as_str()) {
                skipped.push(SkippedVector {
                    vector_id: vector.id,
                    reason: "not supported by live substrate".into(),
                });
                continue;
            }
            live_vectors.push(vector);
        }

        Self {
            vectors: live_vectors,
            skipped,
        }
    }
}

fn topic_names_for_vectors(vectors: &[Vector]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for step in vectors.iter().flat_map(|vector| &vector.steps) {
        let topic = match &step.command {
            Command::Publish { topic, .. } | Command::PublishEvent { topic, .. } => topic,
            Command::Subscribe { topics, .. } => {
                for topic in topics {
                    if is_creatable_topic(topic) {
                        names.insert(topic.clone());
                    }
                }
                continue;
            }
            _ => continue,
        };
        if is_creatable_topic(topic) {
            names.insert(topic.clone());
        }
    }
    names.into_iter().collect()
}

fn is_creatable_topic(topic: &str) -> bool {
    !topic.is_empty() && !topic.starts_with("__missing_")
}

struct LiveSubstrate {
    endpoint: String,
    broker: BrokerHandle,
    shutdown: CancellationToken,
    gateway_task: JoinHandle<std::io::Result<()>>,
    _data_dir: tempfile::TempDir,
}

impl LiveSubstrate {
    async fn boot(topic_names: &[String]) -> Result<Self, HarnessError> {
        let data_dir = tempfile::TempDir::new()?;
        let mut broker_config = BrokerConfig::for_tests(data_dir.path().to_path_buf());
        broker_config.classic_group_initial_rebalance_delay = millis(1);
        let broker = Broker::start(broker_config)
            .await
            .map_err(HarnessError::BrokerStart)?;
        let bootstrap = broker.listen_addr().to_string();
        create_topics(&bootstrap, topic_names).await?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let state = gateway_state(&bootstrap, listen_addr).await?;
        let app = crabka_grpc_gateway::router(state);
        let shutdown = CancellationToken::new();
        let gateway_shutdown = shutdown.clone();
        let gateway_task =
            tokio::spawn(async move { serve::serve(listener, app, None, gateway_shutdown).await });

        Ok(Self {
            endpoint: format!("http://{listen_addr}"),
            broker,
            shutdown,
            gateway_task,
            _data_dir: data_dir,
        })
    }

    async fn shutdown(self) -> Result<(), HarnessError> {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.gateway_task)
            .await
            .map_err(|_| HarnessError::SubstrateTimeout)??
            .map_err(HarnessError::GatewayServe)?;
        self.broker.shutdown().await;
        Ok(())
    }
}

async fn create_topics(bootstrap: &str, topic_names: &[String]) -> Result<(), HarnessError> {
    if topic_names.is_empty() {
        return Ok(());
    }
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .map_err(HarnessError::Admin)?;
    let specs = topic_names
        .iter()
        .map(|name| CreateTopicSpec {
            name: name.clone(),
            partitions: 1,
            replicas: 1,
            configs: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    admin
        .create_topics(&specs, millis(10_000))
        .await
        .map(|_| ())
        .map_err(HarnessError::Admin)
}

async fn gateway_state(
    bootstrap: &str,
    listen_addr: SocketAddr,
) -> Result<Arc<AppState>, HarnessError> {
    let codec = Arc::new(ConformanceCodec);
    let produce = ProduceCore::new(bootstrap, "sdk-conformance", codec.clone(), None)
        .await
        .map_err(HarnessError::GatewayInit)?;
    let config = Arc::new(gateway_config(bootstrap, listen_addr));
    Ok(Arc::new(AppState {
        produce: Arc::new(produce),
        config,
        authz: Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec,
        queue: Arc::new(crabka_grpc_gateway::queue::QueueSessionTable::default()),
    }))
}

#[derive(Debug)]
struct ConformanceCodec;

#[async_trait::async_trait]
impl RecordCodec for ConformanceCodec {
    async fn encode(&self, _topic: &str, body: EncodeBody) -> Result<bytes::Bytes, CodecError> {
        Ok(match body {
            EncodeBody::Raw(value) => value,
            EncodeBody::Structured { json, .. } => json,
        })
    }

    async fn decode(&self, _topic: &str, value: bytes::Bytes) -> Result<Decoded, CodecError> {
        let json = serde_json::from_slice::<serde_json::Value>(&value)
            .ok()
            .map(|_| value.clone());
        Ok(Decoded {
            value,
            schema: None,
            json,
        })
    }
}

fn gateway_config(bootstrap: &str, listen_addr: SocketAddr) -> GatewayConfig {
    GatewayConfig {
        bootstrap: bootstrap.to_string(),
        listen_addr,
        client_id: "sdk-conformance".into(),
        dedup_topic: "__crabka_grpc_dedup".into(),
        dedup_partitions: 4,
        dedup_window: hours(1),
        dedup_ownership_group: "sdk-conformance-owners".into(),
        dedup_txn_id_prefix: "sdk-conformance-dedup".into(),
        advertised_addr: listen_addr.to_string(),
        membership_topic: "__crabka_grpc_gateway_membership".into(),
        tls: None,
        broker_security: None,
        authz: None,
        webhooks: BTreeMap::new().into_iter().collect(),
        outbound: Vec::new(),
        schema_registry_url: None,
        runtime: crabka_grpc_gateway::config::GatewayRuntimeConfig::default(),
    }
}

/// Summary returned by a conformance run.
#[derive(Debug, Clone, PartialEq)]
pub struct RunSummary {
    /// Number of vectors that passed.
    pub passed: usize,
    /// Failed vector diagnostics.
    pub failed: Vec<VectorFailure>,
    /// Vectors intentionally excluded from this run.
    pub skipped: Vec<SkippedVector>,
}

impl RunSummary {
    /// Whether every vector passed.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failed.is_empty()
    }
}

/// One vector excluded from the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedVector {
    /// Vector id.
    pub vector_id: String,
    /// Human-readable reason.
    pub reason: String,
}

/// One vector mismatch.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorFailure {
    /// Vector id.
    pub vector_id: String,
    /// Step name.
    pub step: String,
    /// Expected response.
    pub expected: Response,
    /// Actual response.
    pub actual: Response,
}

struct AdapterProcess {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl AdapterProcess {
    fn spawn(adapter: &PathBuf, args: &[String]) -> Result<Self, HarnessError> {
        let mut child = ProcessCommand::new(adapter)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(HarnessError::Spawn)?;
        let stdin = child
            .stdin
            .take()
            .ok_or(HarnessError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(HarnessError::MissingPipe("stdout"))?;
        Ok(Self {
            child,
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
        })
    }

    async fn call(&mut self, command: Command) -> Result<Response, HarnessError> {
        let line = serde_json::to_string(&command)?;
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        let mut response = String::new();
        tokio::time::timeout(ADAPTER_CALL_TIMEOUT, self.stdout.read_line(&mut response))
            .await
            .map_err(|_| HarnessError::AdapterTimeout)??;
        if response.is_empty() {
            return Err(HarnessError::AdapterEof);
        }
        Ok(serde_json::from_str(response.trim_end())?)
    }
}

impl Drop for AdapterProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Harness errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HarnessError {
    /// Vector load failed.
    #[error("vectors: {0}")]
    Vectors(#[from] VectorError),
    /// Adapter process failed to spawn.
    #[error("spawn adapter: {0}")]
    Spawn(std::io::Error),
    /// Adapter pipe was not available.
    #[error("adapter missing {0} pipe")]
    MissingPipe(&'static str),
    /// Adapter I/O failed.
    #[error("adapter io: {0}")]
    Io(#[from] std::io::Error),
    /// Protocol JSON failed.
    #[error("adapter json: {0}")]
    Json(#[from] serde_json::Error),
    /// Adapter did not answer in time.
    #[error("adapter timed out")]
    AdapterTimeout,
    /// Adapter exited without a response.
    #[error("adapter exited before responding")]
    AdapterEof,
    /// Adapter violated the conformance protocol.
    #[error("adapter protocol: {0}")]
    AdapterProtocol(&'static str),
    /// Broker failed to start.
    #[error("live substrate broker start: {0}")]
    BrokerStart(crabka_broker::BrokerError),
    /// Admin client setup failed.
    #[error("live substrate admin: {0}")]
    Admin(crabka_client_admin::AdminError),
    /// Gateway state failed to initialize.
    #[error("live substrate gateway init: {0}")]
    GatewayInit(crabka_grpc_gateway::error::GatewayError),
    /// Gateway server returned an error.
    #[error("live substrate gateway serve: {0}")]
    GatewayServe(std::io::Error),
    /// Gateway task join failed.
    #[error("live substrate gateway task: {0}")]
    GatewayTask(#[from] tokio::task::JoinError),
    /// Live substrate did not shut down in time.
    #[error("live substrate timed out")]
    SubstrateTimeout,
}

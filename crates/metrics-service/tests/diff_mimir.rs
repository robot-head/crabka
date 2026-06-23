//! Docker-backed differential probe against real Grafana Mimir (the headline
//! equality test for the metrics slice: Mimir is the system Crabka replaces, so
//! corpus equality over identical `remote_write` input is the strongest single
//! correctness signal).
//!
//! Ignored by default because it pulls and runs `grafana/mimir` under Docker.
//! Run with:
//!
//! `cargo test -p crabka-metrics-service --test diff_mimir -- --ignored --nocapture`
//!
//! Structure mirrors `diff_prometheus.rs`: one `remote_write` body is written to
//! BOTH the in-process Crabka write+query path and a Mimir monolithic container,
//! then `query_corpus()` is run against both and compared with
//! `assert_query_equal`. The shared corpus/differ lives in the `crabka-metrics`
//! crate and is path-included below, exactly as `diff_prometheus.rs` does it, so
//! both differential suites share one corpus definition.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use crabka_metrics::distributor::{DistributorState, ProduceError, WalSink};
use crabka_metrics::{WalRecord, wire::pb};
use crabka_promql::WalHead;
use diff_corpus::{QueryKind, assert_query_equal, query_corpus, seed_dataset};
use prost::Message;
use reqwest::StatusCode;
use serde_json::Value;
use testcontainers::core::{Host, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::oneshot;

#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Fixed tenant header used on both sides. Mimir requires `X-Scope-OrgID` on
/// every push/query (multitenancy is on; we simply pin one tenant), and Crabka's
/// querier keys storage by the same header.
const TENANT: &str = "compliance";

/// Mimir's default HTTP server port (serves `/ready`, `/api/v1/push`, and the
/// `/prometheus/api/v1/query*` read endpoints in monolithic mode).
const MIMIR_PORT: u16 = 9009;

/// Pinned Mimir image tag (no `:latest`). Override via `CRABKA_MIMIR_IMAGE_TAG`.
const MIMIR_IMAGE_TAG: &str = "2.16.1";

/// Queries where Mimir legitimately diverges from Crabka and must be skipped.
///
/// Each entry MUST carry a justification comment. The list is intentionally
/// empty: `normalize()` already strips Mimir's volatile `warnings`/`infos`/
/// `stats` envelope fields, and the corpus is curated to stay inside the
/// ingester head window so no block-compaction / internal-label divergence is
/// expected. If a future corpus addition surfaces a genuine Mimir-specific
/// difference (e.g. a `__mimir__`-injected label on a particular query), add the
/// `QueryCase::name` here with a one-line reason rather than loosening the
/// differ.
const MIMIR_KNOWN_DIVERGENCE: &[&str] = &[];

/// Minimal Mimir monolithic config: single-binary (`-target=all` is passed on
/// the CLI), filesystem object storage under `/tmp`, and native histograms
/// enabled so the corpus's histogram cases are accepted. Multitenancy stays on
/// (the test pins one `X-Scope-OrgID`).
const MIMIR_CONFIG: &str = r"
multitenancy_enabled: true

server:
  http_listen_port: 9009
  grpc_listen_port: 9095

common:
  storage:
    backend: filesystem
    filesystem:
      dir: /tmp/mimir/data

blocks_storage:
  backend: filesystem
  filesystem:
    dir: /tmp/mimir/blocks
  bucket_store:
    sync_dir: /tmp/mimir/tsdb-sync
  tsdb:
    dir: /tmp/mimir/tsdb

compactor:
  data_dir: /tmp/mimir/compactor

ruler_storage:
  backend: filesystem
  filesystem:
    dir: /tmp/mimir/ruler

ingester:
  ring:
    # Monolithic single-binary has exactly one ingester; the default
    # replication factor of 3 makes the distributor reject every push with
    # 'at least 2 live replicas required, could only find 1'.
    replication_factor: 1

limits:
  native_histograms_ingestion_enabled: true
";

#[tokio::test]
#[ignore = "requires Docker"]
async fn mimir_compliance_corpus_matches_crabka() -> TestResult {
    let client = reqwest::Client::new();

    // Real Mimir in monolithic mode.
    let mimir = start_mimir().await?;
    let mimir_base = mapped_base_url(&mimir, MIMIR_PORT).await?;
    wait_for_http_ok(&client, &mimir_base, "/ready").await?;

    // In-process Crabka write+query path (identical to diff_prometheus.rs).
    let crabka = start_crabka_query_server().await?;

    // One body, two destinations.
    let remote_write = remote_write_body();
    post_remote_write(
        &client,
        &crabka.base_url,
        "/api/v1/write",
        TENANT,
        &remote_write,
    )
    .await?;
    // Mimir's remote_write receiver lives at /api/v1/push.
    post_remote_write(&client, &mimir_base, "/api/v1/push", TENANT, &remote_write).await?;

    // Both read paths expose the Prometheus query API; Mimir prefixes it with
    // /prometheus, which the Crabka router also serves.
    wait_for_query_ready(&client, &crabka.base_url, "/api/v1/query", TENANT, "up").await?;
    wait_for_query_ready(
        &client,
        &mimir_base,
        "/prometheus/api/v1/query",
        TENANT,
        "up",
    )
    .await?;

    for case in query_corpus() {
        if MIMIR_KNOWN_DIVERGENCE.contains(&case.name) {
            continue;
        }
        let crabka_json = query_case(
            &client,
            &crabka.base_url,
            "",
            TENANT,
            case.kind,
            case.promql,
        )
        .await?;
        let mimir_json = query_case(
            &client,
            &mimir_base,
            "/prometheus",
            TENANT,
            case.kind,
            case.promql,
        )
        .await?;
        assert_query_equal(case.name, &crabka_json, &mimir_json);
    }

    crabka.shutdown();
    Ok(())
}

async fn start_mimir() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag =
        std::env::var("CRABKA_MIMIR_IMAGE_TAG").unwrap_or_else(|_| MIMIR_IMAGE_TAG.to_string());
    Ok(GenericImage::new("grafana/mimir".to_string(), tag)
        .with_exposed_port(MIMIR_PORT.tcp())
        // Mimir 2.16.x logs go-kit lines to stderr; the HTTP server announces
        // itself with "server listening on addresses" once the port is up. (It
        // never logs the literal "Starting Mimir" — its banner is "Starting
        // application".) Readiness of the ingester ring is then polled via the
        // /ready endpoint below, which can take ~30s in monolithic mode.
        .with_wait_for(WaitFor::message_on_stderr("server listening on addresses"))
        // Copy the config into the image rather than bind-mounting a host path,
        // so the test has no host-filesystem prerequisites.
        .with_copy_to("/etc/mimir/mimir.yaml", MIMIR_CONFIG.as_bytes().to_vec())
        // host-gateway entry kept for symmetry with the other Docker suites; the
        // differential path here is container->host-agnostic (we dial Mimir's
        // mapped port), but it makes the container reachable both ways on Linux.
        .with_host("host.docker.internal", Host::HostGateway)
        .with_cmd([
            "-target=all",
            "-config.file=/etc/mimir/mimir.yaml",
            // The corpus uses epoch-relative timestamps (t=0..45s, i.e. 1970),
            // far older than Mimir's default 13h ingester-query window — without
            // this the querier never looks in the (head-resident) ingester and
            // every query returns empty. 0 = always query ingesters regardless
            // of sample age. (CLI flag form: the YAML field lives under a
            // different config path than `querier:`.)
            "-querier.query-ingesters-within=0",
        ])
        .start()
        .await?)
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port.tcp()).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

struct CrabkaServer {
    base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl CrabkaServer {
    fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_crabka_query_server() -> TestResult<CrabkaServer> {
    let head = WalHead::new();
    let query_router = crabka_metrics_service::prometheus_router_for_store(head.clone());
    let sink: Arc<dyn WalSink> = Arc::new(WalHeadSink { head });
    let distributor = Arc::new(DistributorState::new(sink));
    let router = query_router.merge(crabka_metrics::distributor::router(distributor));
    let (tx, rx) = oneshot::channel();
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let bound = crabka_metrics_service::serve_prometheus_router(addr, router, async move {
        let _ = rx.await;
    })
    .await?;

    Ok(CrabkaServer {
        base_url: format!("http://{bound}"),
        shutdown: Some(tx),
    })
}

struct WalHeadSink {
    head: WalHead,
}

#[async_trait::async_trait]
impl WalSink for WalHeadSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.head.apply_wal_record(&record);
        Ok(())
    }
}

fn remote_write_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: seed_dataset()
            .into_iter()
            .map(|point| pb::v1::TimeSeries {
                labels: remote_write_labels(point.metric, point.labels),
                samples: point
                    .samples
                    .iter()
                    .map(|(timestamp, value)| pb::v1::Sample {
                        value: *value,
                        timestamp: *timestamp,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy remote_write")
}

fn remote_write_labels(metric: &str, labels: &[(&str, &str)]) -> Vec<pb::v1::Label> {
    std::iter::once(pb::v1::Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    })
    .chain(labels.iter().map(|(name, value)| pb::v1::Label {
        name: (*name).to_string(),
        value: (*value).to_string(),
    }))
    .collect()
}

async fn post_remote_write(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: &str,
    body: &[u8],
) -> TestResult {
    let response = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Scope-OrgID", tenant)
        .body(body.to_vec())
        .send()
        .await?;
    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::NO_CONTENT) {
        let detail = response.text().await.unwrap_or_default();
        return Err(format!("remote_write to {base}{path} returned {status}: {detail}").into());
    }
    Ok(())
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, path: &str) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    while std::time::Instant::now() < deadline {
        if client
            .get(format!("{base}{path}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("{base}{path} did not become ready").into())
}

async fn wait_for_query_ready(
    client: &reqwest::Client,
    base: &str,
    query_path: &str,
    tenant: &str,
    query: &str,
) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let json = query_instant(client, base, query_path, tenant, query, 45_000).await?;
        if json["data"]["result"]
            .as_array()
            .is_some_and(|result| !result.is_empty())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("query `{query}` did not become non-empty on {base}").into())
}

async fn query_case(
    client: &reqwest::Client,
    base: &str,
    prefix: &str,
    tenant: &str,
    kind: QueryKind,
    promql: &str,
) -> TestResult<Value> {
    match kind {
        QueryKind::Instant { time } => {
            query_instant(
                client,
                base,
                &format!("{prefix}/api/v1/query"),
                tenant,
                promql,
                time,
            )
            .await
        }
        QueryKind::Range { start, end, step } => {
            query_range(
                client,
                base,
                &format!("{prefix}/api/v1/query_range"),
                tenant,
                promql,
                start,
                end,
                step,
            )
            .await
        }
    }
}

async fn query_instant(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: &str,
    promql: &str,
    time_ms: i64,
) -> TestResult<Value> {
    Ok(client
        .get(query_url(
            base,
            path,
            &[
                ("query", promql.to_string()),
                ("time", seconds_param(time_ms)),
            ],
        ))
        .header("X-Scope-OrgID", tenant)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

#[allow(
    clippy::too_many_arguments,
    reason = "range query API mirrors Prometheus' parameter set"
)]
async fn query_range(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: &str,
    promql: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> TestResult<Value> {
    Ok(client
        .get(query_url(
            base,
            path,
            &[
                ("query", promql.to_string()),
                ("start", seconds_param(start_ms)),
                ("end", seconds_param(end_ms)),
                ("step", seconds_param(step_ms)),
            ],
        ))
        .header("X-Scope-OrgID", tenant)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

fn query_url(base: &str, path: &str, params: &[(&str, String)]) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params.iter().map(|(name, value)| (*name, value.as_str())))
        .finish();
    format!("{base}{path}?{query}")
}

fn seconds_param(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let abs_ms = i128::from(ms).abs();
    format!("{sign}{}.{:03}", abs_ms / 1000, abs_ms % 1000)
}

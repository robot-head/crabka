//! Docker-backed differential probe against real Prometheus.
//!
//! Ignored by default because it pulls and runs `mirror.gcr.io/prom/prometheus`.
//! Run with:
//!
//! `cargo test -p crabka-metrics-service --test diff_prometheus -- --ignored --nocapture`

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
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::oneshot;

#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TENANT: &str = "compliance";
const PROMETHEUS_PORT: u16 = 9090;

#[tokio::test]
async fn crabka_remote_write_endpoint_feeds_query_store() -> TestResult {
    let client = reqwest::Client::new();
    let crabka = start_crabka_query_server().await?;
    let remote_write = remote_write_body();

    post_remote_write(
        &client,
        &crabka.base_url,
        "/api/v1/write",
        Some(TENANT),
        &remote_write,
    )
    .await?;
    wait_for_query_ready(&client, &crabka.base_url, Some(TENANT), "up").await?;

    crabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn prometheus_compliance_corpus_matches_crabka() -> TestResult {
    let client = reqwest::Client::new();
    let prometheus = start_prometheus().await?;
    let prometheus_base = mapped_base_url(&prometheus, PROMETHEUS_PORT).await?;
    wait_for_http_ok(&client, &prometheus_base, "/-/ready").await?;

    let crabka = start_crabka_query_server().await?;
    let remote_write = remote_write_body();
    post_remote_write(
        &client,
        &crabka.base_url,
        "/api/v1/write",
        Some(TENANT),
        &remote_write,
    )
    .await?;
    post_remote_write(
        &client,
        &prometheus_base,
        "/api/v1/write",
        None,
        &remote_write,
    )
    .await?;
    wait_for_query_ready(&client, &crabka.base_url, Some(TENANT), "up").await?;
    wait_for_query_ready(&client, &prometheus_base, None, "up").await?;

    for case in query_corpus() {
        let crabka_json = query_case(
            &client,
            &crabka.base_url,
            Some(TENANT),
            case.kind,
            case.promql,
        )
        .await?;
        let prometheus_json =
            query_case(&client, &prometheus_base, None, case.kind, case.promql).await?;
        assert_query_equal(case.name, &crabka_json, &prometheus_json);
    }

    crabka.shutdown();
    Ok(())
}

async fn start_prometheus() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_PROMETHEUS_IMAGE_TAG").unwrap_or_else(|_| "v3.8.0".to_string());
    Ok(
        GenericImage::new("mirror.gcr.io/prom/prometheus".to_string(), tag)
            .with_exposed_port(PROMETHEUS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "Server is ready to receive web requests",
            ))
            .with_cmd([
                "--config.file=/etc/prometheus/prometheus.yml",
                "--storage.tsdb.path=/prometheus",
                "--web.enable-remote-write-receiver",
                "--enable-feature=native-histograms",
            ])
            .start()
            .await?,
    )
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
    tenant: Option<&str>,
    body: &[u8],
) -> TestResult {
    let mut request = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body.to_vec());
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    let status = request.send().await?.status();
    if !(status == StatusCode::OK || status == StatusCode::NO_CONTENT) {
        return Err(format!("remote_write to {base}{path} returned {status}").into());
    }
    Ok(())
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, path: &str) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
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
    tenant: Option<&str>,
    query: &str,
) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let json = query_instant(client, base, tenant, query, 45_000).await?;
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
    tenant: Option<&str>,
    kind: QueryKind,
    promql: &str,
) -> TestResult<Value> {
    match kind {
        QueryKind::Instant { time } => query_instant(client, base, tenant, promql, time).await,
        QueryKind::Range { start, end, step } => {
            query_range(client, base, tenant, promql, start, end, step).await
        }
    }
}

async fn query_instant(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    promql: &str,
    time_ms: i64,
) -> TestResult<Value> {
    let mut request = client.get(query_url(
        base,
        "/api/v1/query",
        &[
            ("query", promql.to_string()),
            ("time", seconds_param(time_ms)),
        ],
    ));
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    Ok(request.send().await?.error_for_status()?.json().await?)
}

async fn query_range(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    promql: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> TestResult<Value> {
    let mut request = client.get(query_url(
        base,
        "/api/v1/query_range",
        &[
            ("query", promql.to_string()),
            ("start", seconds_param(start_ms)),
            ("end", seconds_param(end_ms)),
            ("step", seconds_param(step_ms)),
        ],
    ));
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    Ok(request.send().await?.error_for_status()?.json().await?)
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

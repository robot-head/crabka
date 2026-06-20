//! Docker-backed compatibility probes for real Pyroscope/Grafana surfaces.
//!
//! These tests are ignored by default because they pull and run upstream Docker
//! images. Run them explicitly with:
//!
//! `cargo test -p crabka-profiles --test pyroscope_differential -- --ignored`

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use crabka_profiles::distributor::{self, DistributorState, WalSink};
use crabka_profiles::hot_store::WalTailProfileStore;
use crabka_profiles::ingest::TenantLimitConfig;
use crabka_profiles::query::{self, QuerierState};
use crabka_profiles::{ProfileRecord, ProfilesError};
use reqwest::StatusCode;
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::oneshot;

const TENANT: &str = "tenant-a";
const PROFILE_ENV: &str = "pprofdiff";
const PROFILE_TYPE: &str = "goroutines:goroutine:count:goroutine:count";
const SELECTOR: &str = r#"{env="pprofdiff"}"#;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Default)]
struct CapturingSink {
    records: Arc<Mutex<Vec<ProfileRecord>>>,
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
        self.records
            .lock()
            .map_err(|_| ProfilesError::Wal("capturing sink lock poisoned".to_string()))?
            .push(rec);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires Docker and the grafana/pyroscope image"]
async fn real_pyroscope_render_matches_crabka_after_identical_ingest() -> TestResult {
    let client = reqwest::Client::new();
    let pyroscope = start_pyroscope().await?;
    let pyroscope_base = mapped_base_url(&pyroscope, 4040).await?;
    wait_for_http_ok(&client, &pyroscope_base, &["/ready"]).await?;
    let gzipped_pprof = fetch_goroutine_pprof(&client, &pyroscope_base).await?;

    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let crabka = start_crabka_pair(sink.clone(), store.clone()).await?;

    post_push_profile(&client, &pyroscope_base, None, &gzipped_pprof).await?;
    post_push_profile(
        &client,
        &crabka.distributor_base,
        Some(TENANT),
        &gzipped_pprof,
    )
    .await?;
    for record in sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone()
    {
        store.append_record(record)?;
    }

    let pyroscope_render = render_until_non_empty(
        &client,
        &pyroscope_base,
        &[format!("{PROFILE_TYPE}{SELECTOR}")],
        "now-1h",
        "now",
        None,
    )
    .await?;
    let crabka_render = render_any(
        &client,
        &crabka.querier_base,
        &[format!("{PROFILE_TYPE}{SELECTOR}")],
        "0",
        "9223372036854775807",
        Some(TENANT),
        false,
    )
    .await?;

    assert!(flame_ticks(&pyroscope_render).is_some_and(|ticks| ticks > 0));
    assert!(flame_ticks(&crabka_render).is_some_and(|ticks| ticks > 0));
    assert!(flame_names(&pyroscope_render).contains("runtime/pprof.profileWriter"));
    assert!(flame_names(&crabka_render).contains("runtime/pprof.profileWriter"));
    assert_flamebearer_equal(&pyroscope_render, &crabka_render)?;

    assert_profile_types_match(&client, &pyroscope_base, &crabka.querier_base).await?;
    assert_label_values_match(&client, &pyroscope_base, &crabka.querier_base, "env").await?;

    assert_profile_types_contain(&client, &crabka.querier_base, Some(TENANT)).await?;
    assert_label_names_contain(&client, &crabka.querier_base, Some(TENANT)).await?;
    assert_label_values_contain(
        &client,
        &crabka.querier_base,
        Some(TENANT),
        "env",
        PROFILE_ENV,
    )
    .await?;
    assert_select_merge_stacktraces_has_symbol(&client, &crabka.querier_base, Some(TENANT)).await?;
    assert_select_series_has_points(&client, &crabka.querier_base, Some(TENANT)).await?;
    assert_select_heatmap_has_slots(&client, &crabka.querier_base, Some(TENANT)).await?;
    assert_diff_has_ticks(&client, &crabka.querier_base, Some(TENANT)).await?;

    crabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the grafana/grafana image"]
async fn grafana_accepts_pyroscope_datasource_pointing_at_crabka() -> TestResult {
    let client = reqwest::Client::new();
    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let crabka = start_crabka_pair(sink, store).await?;

    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, 3000).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;

    let payload = json!({
        "name": "Crabka Profiles",
        "type": "grafana-pyroscope-datasource",
        "access": "proxy",
        "url": crabka.querier_base,
        "isDefault": true,
        "jsonData": {}
    });
    let created: Value = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let uid = created
        .get("datasource")
        .and_then(|datasource| datasource.get("uid"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let id = created
        .get("datasource")
        .and_then(|datasource| datasource.get("id"))
        .and_then(Value::as_i64)
        .unwrap_or_default();

    let fetched: Value = if let Some(uid) = uid {
        client
            .get(format!("{grafana_base}/api/datasources/uid/{uid}"))
            .basic_auth("admin", Some("admin"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    } else if id != 0 {
        client
            .get(format!("{grafana_base}/api/datasources/id/{id}"))
            .basic_auth("admin", Some("admin"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    } else {
        let encoded = url::form_urlencoded::byte_serialize(b"Crabka Profiles").collect::<String>();
        client
            .get(format!("{grafana_base}/api/datasources/name/{encoded}"))
            .basic_auth("admin", Some("admin"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    };

    assert_eq!(
        fetched.get("type").and_then(Value::as_str),
        Some("grafana-pyroscope-datasource")
    );
    assert_eq!(
        fetched.get("url").and_then(Value::as_str),
        Some(crabka.querier_base.as_str())
    );

    crabka.shutdown();
    Ok(())
}

async fn start_pyroscope() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_PYROSCOPE_IMAGE_TAG").unwrap_or_else(|_| "latest".to_string());
    Ok(GenericImage::new("grafana/pyroscope".to_string(), tag)
        .with_exposed_port(4040.tcp())
        .with_wait_for(WaitFor::seconds(3))
        .start()
        .await?)
}

async fn start_grafana() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_GRAFANA_IMAGE_TAG").unwrap_or_else(|_| "latest".to_string());
    Ok(GenericImage::new("grafana/grafana".to_string(), tag)
        .with_exposed_port(3000.tcp())
        .with_wait_for(WaitFor::seconds(5))
        .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
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

struct CrabkaPair {
    distributor_base: String,
    querier_base: String,
    distributor_shutdown: Option<oneshot::Sender<()>>,
    querier_shutdown: Option<oneshot::Sender<()>>,
}

impl CrabkaPair {
    fn shutdown(mut self) {
        if let Some(tx) = self.distributor_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.querier_shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_crabka_pair(
    sink: CapturingSink,
    store: WalTailProfileStore,
) -> TestResult<CrabkaPair> {
    let (distributor_shutdown, distributor_rx) = oneshot::channel();
    let distributor_state = Arc::new(DistributorState {
        sink: Arc::new(sink),
        limits: TenantLimitConfig::default(),
        relabel: Vec::new(),
        max_decompressed: 16 * 1024 * 1024,
    });
    let distributor_addr =
        distributor::serve("127.0.0.1:0".parse()?, distributor_state, async move {
            let _ = distributor_rx.await;
        })
        .await?;

    let (querier_shutdown, querier_rx) = oneshot::channel();
    let querier_state = Arc::new(QuerierState::new(Arc::new(store)));
    let querier_addr = query::serve("127.0.0.1:0".parse()?, querier_state, async move {
        let _ = querier_rx.await;
    })
    .await?;

    Ok(CrabkaPair {
        distributor_base: format!("http://{distributor_addr}"),
        querier_base: format!("http://{querier_addr}"),
        distributor_shutdown: Some(distributor_shutdown),
        querier_shutdown: Some(querier_shutdown),
    })
}

async fn fetch_goroutine_pprof(client: &reqwest::Client, base: &str) -> TestResult<Vec<u8>> {
    Ok(client
        .get(format!("{base}/debug/pprof/goroutine?debug=0"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec())
}

async fn post_push_profile(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    gzipped_pprof: &[u8],
) -> TestResult {
    let body = json!({
        "series": [{
            "labels": [
                { "name": "__name__", "value": "goroutines" },
                { "name": "service_name", "value": "api" },
                { "name": "env", "value": PROFILE_ENV }
            ],
            "samples": [{
                "rawProfile": BASE64.encode(gzipped_pprof),
                "ID": "crabka-differential-goroutine"
            }]
        }]
    });
    let mut request = client
        .post(format!("{base}/push.v1.PusherService/Push"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&body);
    if let Some(tenant) = tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("push.v1 profile push to {base} returned {status}: {body}").into());
    }
    Ok(())
}

async fn render_any(
    client: &reqwest::Client,
    base: &str,
    queries: &[String],
    from: &str,
    until: &str,
    tenant: Option<&str>,
    require_non_empty: bool,
) -> TestResult<Value> {
    let mut attempts = Vec::new();
    for path in ["/pyroscope/render", "/render"] {
        for query in queries {
            let encoded = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("query", query)
                .append_pair("from", from)
                .append_pair("until", until)
                .finish();
            let mut request = client.get(format!("{base}{path}?{encoded}"));
            if let Some(tenant) = tenant {
                request = request.header("x-scope-orgid", tenant);
            }
            let response = request.send().await?;
            let status = response.status();
            if status.is_success() {
                let value = response.json().await?;
                if !require_non_empty || flame_names(&value).len() > 1 {
                    return Ok(value);
                }
                attempts.push(format!("{path} query={query}: {status}: empty flamegraph"));
                continue;
            }
            let body = response.text().await.unwrap_or_default();
            attempts.push(format!("{path} query={query}: {status}: {body}"));
        }
    }
    Err(format!(
        "no render endpoint succeeded for {base}: {}",
        attempts.join(" | ")
    )
    .into())
}

async fn render_until_non_empty(
    client: &reqwest::Client,
    base: &str,
    queries: &[String],
    from: &str,
    until: &str,
    tenant: Option<&str>,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..90 {
        let value = render_any(client, base, queries, from, until, tenant, true).await;
        match value {
            Ok(value) => return Ok(value),
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| "render did not become non-empty".to_string())
        .into())
}

async fn assert_profile_types_contain(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json_until(
        client,
        base,
        tenant,
        "ProfileTypes",
        json_time_range(),
        |value| {
            value
                .get("profileTypes")
                .or_else(|| value.get("profile_types"))
                .and_then(Value::as_array)
                .is_some_and(|types| !types.is_empty())
        },
    )
    .await?;
    let ids = response
        .get("profileTypes")
        .or_else(|| response.get("profile_types"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("ProfileTypes response missing profileTypes: {response}"))?
        .iter()
        .inspect(|profile_type| {
            if profile_type
                .get("ID")
                .or_else(|| profile_type.get("id"))
                .and_then(Value::as_str)
                == Some(PROFILE_TYPE)
            {
                assert_eq!(
                    profile_type.get("name").and_then(Value::as_str),
                    Some("goroutines")
                );
                assert_eq!(
                    profile_type.get("sampleType").and_then(Value::as_str),
                    Some("goroutine")
                );
                assert_eq!(
                    profile_type.get("sampleUnit").and_then(Value::as_str),
                    Some("count")
                );
                assert_eq!(
                    profile_type.get("periodType").and_then(Value::as_str),
                    Some("goroutine")
                );
                assert_eq!(
                    profile_type.get("periodUnit").and_then(Value::as_str),
                    Some("count")
                );
            }
        })
        .filter_map(|value| {
            value
                .get("ID")
                .or_else(|| value.get("id"))
                .and_then(Value::as_str)
        })
        .collect::<BTreeSet<_>>();
    if !ids.contains(PROFILE_TYPE) {
        return Err(format!("ProfileTypes did not include {PROFILE_TYPE}: {response}").into());
    }
    Ok(())
}

async fn assert_label_names_contain(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(client, base, tenant, "LabelNames", json_time_range()).await?;
    let names = string_array(&response, "names")?;
    for expected in ["__name__", "__profile_type__", "env"] {
        if !names.contains(expected) {
            return Err(format!("LabelNames did not include {expected}: {response}").into());
        }
    }
    Ok(())
}

async fn assert_label_values_contain(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    name: &str,
    expected: &str,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "LabelValues",
        json!({
            "name": name,
            "start": query_start_ms(),
            "end": query_end_ms(),
        }),
    )
    .await?;
    let values = string_array(&response, "names")?;
    if !values.contains(expected) {
        return Err(format!("LabelValues({name}) did not include {expected}: {response}").into());
    }
    Ok(())
}

async fn assert_profile_types_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    crabka_base: &str,
) -> TestResult {
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "ProfileTypes",
        json_time_range(),
        |value| canonical_profile_type(value, PROFILE_TYPE).is_ok(),
    )
    .await?;
    let crabka = connect_json_until(
        client,
        crabka_base,
        Some(TENANT),
        "ProfileTypes",
        json_time_range(),
        |value| canonical_profile_type(value, PROFILE_TYPE).is_ok(),
    )
    .await?;

    assert_canonical_json_equal(
        "ProfileTypes",
        canonical_profile_type(&pyroscope, PROFILE_TYPE)?,
        canonical_profile_type(&crabka, PROFILE_TYPE)?,
    )
}

async fn assert_label_values_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    crabka_base: &str,
    name: &str,
) -> TestResult {
    let body = json!({
        "name": name,
        "start": query_start_ms(),
        "end": query_end_ms(),
    });
    let pyroscope = connect_json(client, pyroscope_base, None, "LabelValues", body.clone()).await?;
    let crabka = connect_json(
        client,
        crabka_base,
        Some(TENANT),
        "LabelValues",
        body.clone(),
    )
    .await?;

    assert_canonical_json_equal(
        &format!("LabelValues({name})"),
        canonical_string_list(&pyroscope, "names")?,
        canonical_string_list(&crabka, "names")?,
    )
}

async fn assert_select_series_has_points(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "SelectSeries",
        json!({
            "profileTypeID": PROFILE_TYPE,
            "labelSelector": SELECTOR,
            "start": query_start_ms(),
            "end": query_end_ms(),
            "groupBy": ["env"],
            "step": 10.0,
            "aggregation": "TIME_SERIES_AGGREGATION_TYPE_SUM",
            "limit": 10,
        }),
    )
    .await?;
    let series = response
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("SelectSeries response missing series: {response}"))?;
    let has_point = series.iter().any(|series| {
        series
            .get("points")
            .and_then(Value::as_array)
            .is_some_and(|points| points.iter().any(|point| point_value(point) > 0.0))
    });
    if !has_point {
        return Err(format!("SelectSeries had no positive points: {response}").into());
    }
    Ok(())
}

async fn assert_select_heatmap_has_slots(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "SelectHeatmap",
        json!({
            "profileTypeID": PROFILE_TYPE,
            "labelSelector": SELECTOR,
            "start": query_start_ms(),
            "end": query_end_ms(),
            "step": 10.0,
            "groupBy": ["env"],
            "queryType": "HEATMAP_QUERY_TYPE_INDIVIDUAL",
            "exemplarType": "EXEMPLAR_TYPE_NONE",
            "limit": 10,
        }),
    )
    .await?;
    let series = response
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("SelectHeatmap response missing series: {response}"))?;
    let has_slot = series.iter().any(|series| {
        series
            .get("slots")
            .and_then(Value::as_array)
            .is_some_and(|slots| !slots.is_empty())
    });
    if !has_slot {
        return Err(format!("SelectHeatmap had no slots: {response}").into());
    }
    Ok(())
}

async fn assert_select_merge_stacktraces_has_symbol(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "SelectMergeStacktraces",
        json!({
            "profileTypeID": PROFILE_TYPE,
            "labelSelector": SELECTOR,
            "start": query_start_ms(),
            "end": query_end_ms(),
            "maxNodes": 1024,
            "format": "PROFILE_FORMAT_FLAMEGRAPH",
        }),
    )
    .await?;
    let flamegraph = response
        .get("flamegraph")
        .ok_or_else(|| format!("SelectMergeStacktraces response missing flamegraph: {response}"))?;
    if flamegraph_ticks(flamegraph) <= 0 {
        return Err(format!("SelectMergeStacktraces had no positive ticks: {response}").into());
    }
    let names = flamegraph_names(flamegraph);
    if !names.contains("runtime/pprof.profileWriter") {
        return Err(format!(
            "SelectMergeStacktraces missed runtime/pprof.profileWriter: {response}"
        )
        .into());
    }
    Ok(())
}

async fn assert_diff_has_ticks(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let query = json!({
        "profileTypeID": PROFILE_TYPE,
        "labelSelector": SELECTOR,
        "start": query_start_ms(),
        "end": query_end_ms(),
        "maxNodes": 1024,
        "format": "PROFILE_FORMAT_FLAMEGRAPH",
    });
    let response = connect_json(
        client,
        base,
        tenant,
        "Diff",
        json!({
            "left": query,
            "right": query,
        }),
    )
    .await?;
    let flamegraph = response
        .get("flamegraph")
        .ok_or_else(|| format!("Diff response missing flamegraph: {response}"))?;
    let left_ticks = flamegraph
        .get("leftTicks")
        .or_else(|| flamegraph.get("left_ticks"))
        .and_then(json_i64)
        .unwrap_or_default();
    let right_ticks = flamegraph
        .get("rightTicks")
        .or_else(|| flamegraph.get("right_ticks"))
        .and_then(json_i64)
        .unwrap_or_default();
    if left_ticks <= 0 || right_ticks <= 0 {
        return Err(format!("Diff had no positive side ticks: {response}").into());
    }
    Ok(())
}

async fn connect_json_until(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    method: &str,
    body: Value,
    ready: impl Fn(&Value) -> bool,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..90 {
        let value = connect_json(client, base, tenant, method, body.clone()).await;
        match value {
            Ok(value) if ready(&value) => return Ok(value),
            Ok(value) => last = Some(format!("{method} response not ready for {body}: {value}")),
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| format!("{method} response did not become ready"))
        .into())
}

async fn connect_json(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    method: &str,
    body: Value,
) -> TestResult<Value> {
    let mut request = client
        .post(format!("{base}/querier.v1.QuerierService/{method}"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&body);
    if let Some(tenant) = tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(format!("{method} returned {status}: {text}").into());
    }
    serde_json::from_str(&text)
        .map_err(|err| format!("{method} returned non-JSON body `{text}`: {err}").into())
}

fn string_array<'a>(value: &'a Value, key: &str) -> TestResult<BTreeSet<&'a str>> {
    Ok(value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("response missing {key} array: {value}"))?
        .iter()
        .filter_map(Value::as_str)
        .collect())
}

fn canonical_profile_type(value: &Value, expected_id: &str) -> TestResult<Value> {
    let profile_types = value
        .get("profileTypes")
        .or_else(|| value.get("profile_types"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("ProfileTypes response missing profileTypes: {value}"))?;
    let profile_type = profile_types
        .iter()
        .find(|profile_type| {
            profile_type
                .get("ID")
                .or_else(|| profile_type.get("id"))
                .and_then(Value::as_str)
                == Some(expected_id)
        })
        .ok_or_else(|| format!("ProfileTypes response missing {expected_id}: {value}"))?;

    Ok(json!({
        "id": profile_type
            .get("ID")
            .or_else(|| profile_type.get("id"))
            .and_then(Value::as_str),
        "name": profile_type.get("name").and_then(Value::as_str),
        "sampleType": profile_type.get("sampleType").and_then(Value::as_str),
        "sampleUnit": profile_type.get("sampleUnit").and_then(Value::as_str),
        "periodType": profile_type.get("periodType").and_then(Value::as_str),
        "periodUnit": profile_type.get("periodUnit").and_then(Value::as_str),
    }))
}

fn canonical_string_list(value: &Value, key: &str) -> TestResult<Value> {
    Ok(json!({
        key: string_array(value, key)?.into_iter().collect::<Vec<_>>()
    }))
}

fn point_value(point: &Value) -> f64 {
    point
        .get("value")
        .and_then(Value::as_f64)
        .or_else(|| point.get("value").and_then(Value::as_str)?.parse().ok())
        .unwrap_or_default()
}

fn flamegraph_names(value: &Value) -> BTreeSet<String> {
    value
        .get("names")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|names| names.iter())
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn flamegraph_ticks(value: &Value) -> i64 {
    value
        .get("total")
        .or_else(|| value.get("leftTicks"))
        .or_else(|| value.get("left_ticks"))
        .and_then(json_i64)
        .unwrap_or_default()
}

fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, paths: &[&str]) -> TestResult {
    for _ in 0..300 {
        for path in paths {
            if let Ok(response) = client.get(format!("{base}{path}")).send().await
                && response.status().is_success()
            {
                return Ok(());
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Err(format!("{base} did not become ready").into())
}

fn flame_names(value: &Value) -> BTreeSet<String> {
    value
        .pointer("/flamebearer/names")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|names| names.iter())
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn flame_ticks(value: &Value) -> Option<i64> {
    value
        .pointer("/flamebearer/numTicks")
        .or_else(|| value.pointer("/flamebearer/total"))
        .and_then(Value::as_i64)
}

fn assert_flamebearer_equal(expected: &Value, actual: &Value) -> TestResult {
    let expected = canonical_flamebearer(expected)?;
    let actual = canonical_flamebearer(actual)?;
    if expected != actual {
        return Err(format!("flamebearer mismatch: expected {expected}, got {actual}").into());
    }
    Ok(())
}

fn assert_canonical_json_equal(method: &str, expected: Value, actual: Value) -> TestResult {
    let expected = canonical_json(expected);
    let actual = canonical_json(actual);
    if expected != actual {
        return Err(format!("{method} mismatch: expected {expected}, got {actual}").into());
    }
    Ok(())
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(values)
            if values
                .iter()
                .all(|value| value.as_str().is_some() || value.as_i64().is_some()) =>
        {
            let mut values = values.into_iter().map(canonical_json).collect::<Vec<_>>();
            values.sort_by_key(ToString::to_string);
            Value::Array(values)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        other => other,
    }
}

fn canonical_flamebearer(value: &Value) -> TestResult<Value> {
    let flamebearer = value
        .get("flamebearer")
        .ok_or_else(|| format!("response missing flamebearer object: {value}"))?;
    let names = flamebearer
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamebearer missing names array: {value}"))?;
    let levels = flamebearer
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamebearer missing levels array: {value}"))?;
    let ticks = flamebearer
        .get("numTicks")
        .or_else(|| flamebearer.get("total"))
        .and_then(json_i64)
        .ok_or_else(|| format!("flamebearer missing numTicks/total: {value}"))?;
    let max_self = flamebearer
        .get("maxSelf")
        .or_else(|| flamebearer.get("max_self"))
        .and_then(json_i64)
        .ok_or_else(|| format!("flamebearer missing maxSelf: {value}"))?;

    Ok(json!({
        "names": names,
        "levels": levels,
        "numTicks": ticks,
        "maxSelf": max_self,
    }))
}

fn query_end_ms() -> i64 {
    i64::MAX
}

fn query_start_ms() -> i64 {
    0
}

fn json_time_range() -> Value {
    json!({ "start": query_start_ms(), "end": query_end_ms() })
}

#[test]
fn flamebearer_differential_rejects_shape_drift() {
    let expected = json!({
        "flamebearer": {
            "names": ["total", "main"],
            "levels": [[0, 7, 0, 0], [0, 7, 7, 1]],
            "numTicks": 7,
            "maxSelf": 7
        }
    });
    let actual = json!({
        "flamebearer": {
            "names": ["total", "main"],
            "levels": [[0, 8, 0, 0], [0, 8, 8, 1]],
            "numTicks": 8,
            "maxSelf": 8
        }
    });

    let err = assert_flamebearer_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("flamebearer mismatch"));
}

#[test]
fn connect_differential_rejects_canonical_response_drift() {
    let expected = json!({ "names": ["__name__", "env"] });
    let actual = json!({ "names": ["__name__", "service_name"] });

    let err = assert_canonical_json_equal("LabelNames", expected, actual).unwrap_err();
    assert!(err.to_string().contains("LabelNames mismatch"));
}

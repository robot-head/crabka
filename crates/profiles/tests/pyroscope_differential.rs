//! Docker-backed compatibility probes for real Pyroscope/Grafana surfaces.
//!
//! These tests are ignored by default because they pull and run upstream Docker
//! images. Run them explicitly with:
//!
//! `cargo test -p crabka-profiles --test pyroscope_differential -- --ignored`

use std::{
    collections::BTreeSet,
    io::Write,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use crabka_pprof::{PprofProfile, proto};
use crabka_profiles::{
    ProfileRecord, ProfilesError,
    distributor::{self, DistributorState, WalSink},
    hot_store::WalTailProfileStore,
    ingest::TenantLimitConfig,
    limits::{Limits, OverridesProvider},
    query::{self, QuerierState},
};
use flate2::{Compression, write::GzEncoder};
use reqwest::StatusCode;
use serde_json::{Value, json};
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::oneshot;

const TENANT: &str = "tenant-a";
/// Pyroscope HTTP port inside the container.
const PYROSCOPE_HTTP_PORT: u16 = 4040;
const PROFILE_ENV: &str = "pprofdiff";
const PROFILE_TYPE: &str = "goroutines:goroutine:count:goroutine:count";
const SELECTOR: &str = r#"{env="pprofdiff"}"#;

const TENANT_B: &str = "tenant-b";
const CPU_PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const CPU_NAME: &str = "process_cpu";
const E2E_SERVICE: &str = "checkout";
const E2E_SELECTOR: &str = r#"{service_name="checkout"}"#;
const FUNC_WORK: &str = "main.work";
const FUNC_HOT: &str = "main.hotloop";

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
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_render_matches_crabka_after_identical_ingest() -> TestResult {
    let client = reqwest::Client::new();
    let pyroscope = start_pyroscope().await?;
    let pyroscope_base = mapped_base_url(&pyroscope, PYROSCOPE_HTTP_PORT).await?;
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

    let cases = [("pyroscope", &pyroscope_render), ("crabka", &crabka_render)];
    for (backend, render) in cases {
        assert!(
            flame_ticks(render).is_some_and(|ticks| ticks > 0),
            "{backend} render must report positive ticks"
        );
        assert!(
            flame_names(render).contains("runtime/pprof.profileWriter"),
            "{backend} render must contain runtime/pprof.profileWriter"
        );
    }
    assert_flamebearer_equal(&pyroscope_render, &crabka_render)?;

    assert_profile_types_match(&client, &pyroscope_base, &crabka.querier_base).await?;
    assert_label_names_match(&client, &pyroscope_base, &crabka.querier_base).await?;
    assert_label_values_match(&client, &pyroscope_base, &crabka.querier_base, "env").await?;
    assert_select_merge_stacktraces_match(&client, &pyroscope_base, &crabka.querier_base).await?;
    assert_select_series_match(&client, &pyroscope_base, &crabka.querier_base).await?;
    assert_diff_match(&client, &pyroscope_base, &crabka.querier_base).await?;

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

/// Differential coverage for the two RPCs the grafana-pyroscope-app v2.0.7 "all
/// services" drilldown grid issues *before* it decides whether to fan out the
/// per-panel time-series queries:
///
///   * `querier.v1.QuerierService/GetProfileStats` (empty request body), and
///   * `querier.v1.QuerierService/Series` with `matchers:[]` and
///     `labelNames:["service_name","__profile_type__"]` (and, as a control, the
///     full-label-set form `labelNames:[]`).
///
/// If the drilldown shows "No data" on every panel without a JS error, the most
/// likely cause is a response *shape* deviation in one of these two RPCs that
/// makes the app's response parser silently bail (rather than throw). This test
/// ingests one identical goroutine profile into both real Pyroscope and crabka,
/// issues the exact same calls to both, and compares the responses field by
/// field: JSON key casing, presence/absence of a spurious empty labelset, label
/// key NAMES + ORDER within each set, the SET of `(service_name,__profile_type__)`
/// tuples, `int64`-as-string-vs-number for the `GetProfileStats` times, and any
/// extra/missing top-level fields. It always prints both raw bodies under
/// `--nocapture` so the deviation (if any) is quotable.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_series_and_stats_match_crabka_after_identical_ingest() -> TestResult {
    let client = reqwest::Client::new();
    let pyroscope = start_pyroscope().await?;
    let pyroscope_base = mapped_base_url(&pyroscope, PYROSCOPE_HTTP_PORT).await?;
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

    // (a) GetProfileStats — empty (all-default) request body. Pyroscope ingests
    // asynchronously, so poll until it reports data, then compare.
    let stats_body = json!({});
    let pyroscope_stats = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "GetProfileStats",
        stats_body.clone(),
        profile_stats_has_data,
    )
    .await?;
    let crabka_stats = connect_json_until(
        &client,
        &crabka.querier_base,
        Some(TENANT),
        "GetProfileStats",
        stats_body.clone(),
        profile_stats_has_data,
    )
    .await?;
    eprintln!("[GetProfileStats] pyroscope = {pyroscope_stats}");
    eprintln!("[GetProfileStats] crabka    = {crabka_stats}");
    assert_get_profile_stats_compatible(&pyroscope_stats, &crabka_stats)?;

    // The two backends do NOT ingest identical label sets: real Pyroscope also
    // self-instruments (a `service_name="pyroscope"` series tree), while crabka
    // holds only the one `service_name="api"` goroutine profile we pushed. So the
    // FULL set of (service_name,__profile_type__) tuples legitimately differs.
    // The comparison below is therefore scoped to the tuple BOTH backends share —
    // the ingested `(api, goroutines:...)` — plus the shape invariants that gate
    // the drilldown: wire-key casing, absence of a spurious empty label set, and
    // (the core regression) that crabka returns data for the drilldown's exact
    // call shape, which carries NO time range.
    let shared_tuple = vec![
        ("service_name".to_string(), "api".to_string()),
        ("__profile_type__".to_string(), PROFILE_TYPE.to_string()),
    ];

    // (b) Series with the EXACT body the grafana-pyroscope-app drilldown sends:
    // `matchers:[]`, `labelNames:[service_name,__profile_type__]`, and crucially
    // NO `start`/`end` (they default to 0). Real Pyroscope's Series is range-
    // agnostic and returns the full enumeration regardless; crabka must do the
    // same, or the drilldown sees zero services and never fans out panel queries.
    let drilldown_body = json!({
        "matchers": [],
        "labelNames": ["service_name", "__profile_type__"],
    });
    let pyroscope_drilldown = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "Series",
        drilldown_body.clone(),
        |value| series_contains_tuple(value, &shared_tuple),
    )
    .await?;
    // crabka is fed synchronously above, so a single call suffices; do not poll on
    // readiness here — that would mask the very "returns nothing for a no-range
    // request" regression this asserts.
    let crabka_drilldown = connect_json(
        &client,
        &crabka.querier_base,
        Some(TENANT),
        "Series",
        drilldown_body,
    )
    .await?;
    eprintln!("[Series drilldown no-range] pyroscope = {pyroscope_drilldown}");
    eprintln!("[Series drilldown no-range] crabka    = {crabka_drilldown}");
    assert_series_drilldown_compatible(&pyroscope_drilldown, &crabka_drilldown, &shared_tuple)?;

    // (c) Series with the same projection but an explicit wide range. Both should
    // still surface the shared tuple, and crabka must not emit a spurious empty
    // label set.
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
        .unwrap_or(i64::MAX);
    let ranged_body = json!({
        "matchers": [],
        "labelNames": ["service_name", "__profile_type__"],
        "start": now_ms - 3_600_000,
        "end": now_ms + 3_600_000,
    });
    let pyroscope_ranged = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "Series",
        ranged_body.clone(),
        |value| series_contains_tuple(value, &shared_tuple),
    )
    .await?;
    let crabka_ranged = connect_json(
        &client,
        &crabka.querier_base,
        Some(TENANT),
        "Series",
        ranged_body,
    )
    .await?;
    eprintln!("[Series projected ranged] pyroscope = {pyroscope_ranged}");
    eprintln!("[Series projected ranged] crabka    = {crabka_ranged}");
    assert_series_drilldown_compatible(&pyroscope_ranged, &crabka_ranged, &shared_tuple)?;

    // (d) Series with empty labelNames (full label sets) over the wide range. The
    // spurious-empty-labelset bug (`{"labelsSet":[{}]}`) reproduces here: crabka
    // inserts an empty projection when `labelNames` is empty. Assert no empty set
    // and that crabka returns the full label set for the shared `api` series, the
    // way Pyroscope does (autocomplete + the drilldown both rely on this).
    let full_body = json!({
        "matchers": [],
        "labelNames": [],
        "start": now_ms - 3_600_000,
        "end": now_ms + 3_600_000,
    });
    let pyroscope_full = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "Series",
        full_body.clone(),
        series_has_labelsets,
    )
    .await?;
    let crabka_full = connect_json(
        &client,
        &crabka.querier_base,
        Some(TENANT),
        "Series",
        full_body,
    )
    .await?;
    eprintln!("[Series full labelNames=[]] pyroscope = {pyroscope_full}");
    eprintln!("[Series full labelNames=[]] crabka    = {crabka_full}");
    assert_series_full_compatible(&pyroscope_full, &crabka_full)?;

    crabka.shutdown();
    Ok(())
}

/// `GetProfileStats` is "ready" once the backend reports any ingested data; in
/// canonical proto-JSON `dataIngested:false` is omitted, so treat a present-and-
/// true `dataIngested` as the readiness signal.
fn profile_stats_has_data(value: &Value) -> bool {
    value
        .get("dataIngested")
        .or_else(|| value.get("data_ingested"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// A `Series` response is "ready" once it carries at least one non-empty label
/// set (the `labelsSet` key; the `labels_set` spelling is accepted as a
/// fallback).
fn series_has_labelsets(value: &Value) -> bool {
    series_label_sets(value).is_some_and(|sets| sets.iter().any(|set| !set.is_empty()))
}

/// Extract the `Series` label sets as ordered `(name, value)` vectors, preserving
/// the on-the-wire key order within each set. Accepts either the `labelsSet` key
/// (canonical proto-JSON / connect-go) or the `labels_set` spelling.
/// Returns `None` only when neither key is present at all (which is itself a
/// shape signal distinct from "present but empty").
fn series_label_sets(value: &Value) -> Option<Vec<Vec<(String, String)>>> {
    let sets = value
        .get("labelsSet")
        .or_else(|| value.get("labels_set"))?
        .as_array()?;
    Some(
        sets.iter()
            .map(|entry| {
                entry
                    .get("labels")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(|labels| labels.iter())
                    .filter_map(|pair| {
                        let name = pair.get("name").and_then(Value::as_str)?;
                        // proto-JSON omits empty-string values; treat a missing
                        // value as the empty string so projection still records
                        // the key (an empty-VALUE label is itself a shape signal).
                        let val = pair.get("value").and_then(Value::as_str).unwrap_or("");
                        Some((name.to_string(), val.to_string()))
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
    )
}

/// True if the `Series` response contains a label set that, as an unordered
/// `(name,value)` collection, equals `tuple`. Used both as a Pyroscope readiness
/// predicate and as the crabka assertion target. Order-insensitive within the
/// set because key order is checked separately by [`assert_series_key_order`].
fn series_contains_tuple(value: &Value, tuple: &[(String, String)]) -> bool {
    let want = tuple.iter().cloned().collect::<BTreeSet<_>>();
    series_label_sets(value).is_some_and(|sets| {
        sets.iter()
            .any(|set| set.iter().cloned().collect::<BTreeSet<_>>() == want)
    })
}

/// Compare a *projected* `Series` response (the drilldown's
/// `labelNames=[service_name,__profile_type__]` call) between Pyroscope and
/// crabka along the axes that gate the drilldown:
///   1. the wire key (`labelsSet` vs `labels_set` vs absent),
///   2. absence of a spurious empty `{}` label set on the crabka side,
///   3. the shared `(api, goroutines:...)` tuple is present on BOTH (this is the
///      core regression: crabka must return it even with no time range), and
///   4. the per-set key ORDER agrees for that shared tuple.
///
/// The FULL set of tuples is intentionally not compared: real Pyroscope also
/// self-instruments, so its enumeration is a strict superset of crabka's.
fn assert_series_drilldown_compatible(
    pyroscope: &Value,
    crabka: &Value,
    shared_tuple: &[(String, String)],
) -> TestResult {
    let py_key = series_wire_key(pyroscope);
    let cr_key = series_wire_key(crabka);
    if py_key != cr_key {
        return Err(format!(
            "Series(projected): label-set wire key differs: pyroscope={py_key:?} crabka={cr_key:?}\n  pyroscope={pyroscope}\n  crabka={crabka}"
        )
        .into());
    }

    let cr_sets = series_label_sets(crabka).ok_or_else(|| {
        format!("Series(projected): crabka response missing label-set array: {crabka}")
    })?;
    let cr_empty = cr_sets.iter().filter(|set| set.is_empty()).count();
    if cr_empty != 0 {
        return Err(format!(
            "Series(projected): crabka emitted {cr_empty} spurious empty label set(s): {crabka}"
        )
        .into());
    }

    if !series_contains_tuple(pyroscope, shared_tuple) {
        return Err(format!(
            "Series(projected): pyroscope missing shared tuple {shared_tuple:?}: {pyroscope}"
        )
        .into());
    }
    if !series_contains_tuple(crabka, shared_tuple) {
        return Err(format!(
            "Series(projected): crabka missing shared tuple {shared_tuple:?} (drilldown sees no service → \"No data\"): {crabka}"
        )
        .into());
    }

    assert_series_key_order("Series(projected)", pyroscope, crabka, shared_tuple)
}

/// Compare a *full-label-set* `Series` response (`labelNames=[]`) between the two
/// backends: no spurious empty set on either side, and crabka returns the full
/// label set for the shared `api` series (not a projection or an empty set).
fn assert_series_full_compatible(pyroscope: &Value, crabka: &Value) -> TestResult {
    let py_key = series_wire_key(pyroscope);
    let cr_key = series_wire_key(crabka);
    if py_key != cr_key {
        return Err(format!(
            "Series(full): label-set wire key differs: pyroscope={py_key:?} crabka={cr_key:?}\n  pyroscope={pyroscope}\n  crabka={crabka}"
        )
        .into());
    }

    let py_sets = series_label_sets(pyroscope).ok_or_else(|| {
        format!("Series(full): pyroscope response missing label-set array: {pyroscope}")
    })?;
    let cr_sets = series_label_sets(crabka).ok_or_else(|| {
        format!("Series(full): crabka response missing label-set array: {crabka}")
    })?;

    let py_empty = py_sets.iter().filter(|set| set.is_empty()).count();
    let cr_empty = cr_sets.iter().filter(|set| set.is_empty()).count();
    if py_empty != 0 {
        return Err(format!(
            "Series(full): pyroscope unexpectedly emitted {py_empty} empty label set(s): {pyroscope}"
        )
        .into());
    }
    if cr_empty != 0 {
        return Err(format!(
            "Series(full): crabka emitted {cr_empty} spurious empty label set(s): {crabka}"
        )
        .into());
    }

    // crabka must surface the ingested `api` series with its full label set,
    // including `service_name`, `__name__`, `env`, and `__profile_type__`.
    let crabka_api = cr_sets
        .iter()
        .find(|set| {
            set.iter()
                .any(|(name, value)| name == "service_name" && value == "api")
        })
        .ok_or_else(|| {
            format!("Series(full): crabka missing api series in full label sets: {crabka}")
        })?;
    for required in ["service_name", "__name__", "env", "__profile_type__"] {
        if !crabka_api.iter().any(|(name, _)| name == required) {
            return Err(format!(
                "Series(full): crabka api label set missing `{required}`: {crabka_api:?}"
            )
            .into());
        }
    }
    Ok(())
}

/// For the shared tuple, assert the per-set key ORDER matches between the two
/// backends. Both sides project onto the requested `labelNames`, so the on-the-
/// wire key order should be identical.
fn assert_series_key_order(
    label: &str,
    pyroscope: &Value,
    crabka: &Value,
    shared_tuple: &[(String, String)],
) -> TestResult {
    let want = shared_tuple.iter().cloned().collect::<BTreeSet<_>>();
    let find_keys = |value: &Value| -> Option<Vec<String>> {
        series_label_sets(value)?.into_iter().find_map(|set| {
            (set.iter().cloned().collect::<BTreeSet<_>>() == want)
                .then(|| set.iter().map(|(name, _)| name.clone()).collect())
        })
    };
    let py_order = find_keys(pyroscope);
    let cr_order = find_keys(crabka);
    if py_order != cr_order {
        return Err(format!(
            "{label}: key order for shared tuple differs: pyroscope={py_order:?} crabka={cr_order:?}\n  pyroscope={pyroscope}\n  crabka={crabka}"
        )
        .into());
    }
    Ok(())
}

/// Which top-level key carries the label sets, for a casing-sensitive diff.
fn series_wire_key(value: &Value) -> Option<&'static str> {
    if value.get("labelsSet").is_some() {
        Some("labelsSet")
    } else if value.get("labels_set").is_some() {
        Some("labels_set")
    } else {
        None
    }
}

/// Compare `GetProfileStats` between Pyroscope and crabka. The Drilldown only
/// gates on `dataIngested` being truthy; the time window is used to seed a
/// default range, not to decide whether to fan out panel queries. So this checks
/// the axes that could actually break the app's parser or its gate:
///   1. the `dataIngested` wire key (casing) + truthiness, and
///   2. for any time field *present on both sides*, the int64-as-string-vs-
///      number JSON representation.
///
/// What is intentionally NOT a failure: a time field present on one side and
/// omitted on the other, or differing timestamp magnitudes. Canonical proto-JSON
/// omits fields equal to their zero default, and the goroutine pprof carries no
/// `time_nanos`, so the two backends legitimately disagree on whether
/// `oldestProfileTime` is 0 (omitted) or the ingest instant. A `0`-vs-now
/// `oldestProfileTime` does not stop the drilldown from issuing panel queries;
/// treating it as a hard failure would be a false positive. The deviation is
/// still surfaced via the always-on `eprintln!` of both raw bodies.
fn assert_get_profile_stats_compatible(pyroscope: &Value, crabka: &Value) -> TestResult {
    let py_obj = pyroscope
        .as_object()
        .ok_or_else(|| format!("GetProfileStats: pyroscope response not an object: {pyroscope}"))?;
    let cr_obj = crabka
        .as_object()
        .ok_or_else(|| format!("GetProfileStats: crabka response not an object: {crabka}"))?;

    // 1. dataIngested: same wire key (casing), both truthy.
    let py_ingested_key = stats_field_key(py_obj, "dataIngested", "data_ingested");
    let cr_ingested_key = stats_field_key(cr_obj, "dataIngested", "data_ingested");
    if py_ingested_key != cr_ingested_key {
        return Err(format!(
            "GetProfileStats: dataIngested wire key differs: pyroscope={py_ingested_key:?} crabka={cr_ingested_key:?}\n  pyroscope={pyroscope}\n  crabka={crabka}"
        )
        .into());
    }
    let py_ingested = stats_truthy(py_obj, "dataIngested", "data_ingested");
    let cr_ingested = stats_truthy(cr_obj, "dataIngested", "data_ingested");
    if py_ingested != cr_ingested {
        return Err(format!(
            "GetProfileStats: dataIngested truthiness differs: pyroscope={py_ingested} crabka={cr_ingested}\n  pyroscope={pyroscope}\n  crabka={crabka}"
        )
        .into());
    }

    // 2. oldest/newest time fields: where present on BOTH sides, the JSON
    // representation (string vs number) must agree. Presence itself is not
    // required to match (proto3 zero-omission is canonical on both backends).
    for (camel, snake) in [
        ("oldestProfileTime", "oldest_profile_time"),
        ("newestProfileTime", "newest_profile_time"),
    ] {
        let py_repr =
            stats_field_key(py_obj, camel, snake).and_then(|key| json_number_repr(&py_obj[key]));
        let cr_repr =
            stats_field_key(cr_obj, camel, snake).and_then(|key| json_number_repr(&cr_obj[key]));
        if let (Some(py_repr), Some(cr_repr)) = (py_repr, cr_repr)
            && py_repr != cr_repr
        {
            return Err(format!(
                "GetProfileStats: {camel} JSON representation differs: pyroscope={py_repr:?} crabka={cr_repr:?}\n  pyroscope={pyroscope}\n  crabka={crabka}"
            )
            .into());
        }
    }

    Ok(())
}

/// Which of the `camelCase` / `snake_case` spellings of a stats field is present.
fn stats_field_key(
    obj: &serde_json::Map<String, Value>,
    camel: &'static str,
    snake: &'static str,
) -> Option<&'static str> {
    if obj.contains_key(camel) {
        Some(camel)
    } else if obj.contains_key(snake) {
        Some(snake)
    } else {
        None
    }
}

/// A stats boolean is truthy if present and `true`, or present as a non-zero
/// number (Pyroscope's proto types `data_ingested` as bool, but tolerate a
/// numeric encoding so an int-vs-bool deviation surfaces as a *representation*
/// difference, not a crash).
fn stats_truthy(
    obj: &serde_json::Map<String, Value>,
    camel: &'static str,
    snake: &'static str,
) -> bool {
    let Some(key) = stats_field_key(obj, camel, snake) else {
        return false;
    };
    let value = &obj[key];
    value.as_bool().unwrap_or(false)
        || value.as_i64().is_some_and(|n| n != 0)
        || value.as_str().is_some_and(|s| s == "true" || s == "1")
}

/// Classify a JSON number-ish value as `"string"` or `"number"` so int64-as-
/// string (canonical proto-JSON) vs int64-as-number deviations are caught.
fn json_number_repr(value: &Value) -> Option<&'static str> {
    if value.is_string() {
        Some("string")
    } else if value.is_number() {
        Some("number")
    } else {
        None
    }
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
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
    Ok(
        GenericImage::new("mirror.gcr.io/grafana/pyroscope".to_string(), tag)
            .with_exposed_port(PYROSCOPE_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::seconds(3))
            .start()
            .await?,
    )
}

async fn start_grafana() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_GRAFANA_IMAGE_TAG").unwrap_or_else(|_| "latest".to_string());
    Ok(
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(3000.tcp())
            .with_wait_for(WaitFor::seconds(5))
            .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
            // Let the container reach the in-process Crabka querier on the host via
            // host.docker.internal (host-gateway mapping; works on Docker Desktop + Linux).
            .with_host("host.docker.internal", Host::HostGateway)
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
        profile_overrides: OverridesProvider::new(Limits::default()),
        active_series: Mutex::default(),
        ingestion_buckets: Mutex::default(),
        relabel: Vec::new(),
        max_decompressed: 16 * 1024 * 1024,
        metrics: crabka_profiles::metrics::ServiceMetrics::new(),
    });
    let distributor_addr =
        distributor::serve("127.0.0.1:0".parse()?, distributor_state, async move {
            let _ = distributor_rx.await;
        })
        .await?;

    let (querier_shutdown, querier_rx) = oneshot::channel();
    // The differential / e2e corpus intentionally queries the full `[0, i64::MAX]`
    // range to compare against real Pyroscope, so disable the per-query range cap.
    let querier_state = Arc::new(QuerierState::new_with_limits(
        Arc::new(store),
        crabka_profiles::limits::Limits {
            max_query_length_secs: 0,
            ..Default::default()
        },
    ));
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
                let field_cases = [
                    ("name", "goroutines"),
                    ("sampleType", "goroutine"),
                    ("sampleUnit", "count"),
                    ("periodType", "goroutine"),
                    ("periodUnit", "count"),
                ];
                for (field, expected) in field_cases {
                    assert_eq!(
                        profile_type.get(field).and_then(Value::as_str),
                        Some(expected),
                        "profile type field `{field}`"
                    );
                }
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

async fn assert_label_names_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    crabka_base: &str,
) -> TestResult {
    let body = json!({
        "matchers": [SELECTOR],
        "start": query_start_ms(),
        "end": query_end_ms(),
    });
    let pyroscope = connect_json(client, pyroscope_base, None, "LabelNames", body.clone()).await?;
    let crabka = connect_json(
        client,
        crabka_base,
        Some(TENANT),
        "LabelNames",
        body.clone(),
    )
    .await?;

    assert_label_names_equal(&pyroscope, &crabka)
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

async fn assert_select_series_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    crabka_base: &str,
) -> TestResult {
    let body = select_series_body();
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "SelectSeries",
        body.clone(),
        select_series_has_positive_point,
    )
    .await?;
    let crabka = connect_json_until(
        client,
        crabka_base,
        Some(TENANT),
        "SelectSeries",
        body,
        select_series_has_positive_point,
    )
    .await?;

    assert_select_series_equal(&pyroscope, &crabka)
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

async fn assert_select_merge_stacktraces_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    crabka_base: &str,
) -> TestResult {
    let body = select_merge_stacktraces_body();
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "SelectMergeStacktraces",
        body.clone(),
        |value| {
            value
                .get("flamegraph")
                .is_some_and(|flamegraph| flamegraph_ticks(flamegraph) > 0)
        },
    )
    .await?;
    let crabka = connect_json_until(
        client,
        crabka_base,
        Some(TENANT),
        "SelectMergeStacktraces",
        body,
        |value| {
            value
                .get("flamegraph")
                .is_some_and(|flamegraph| flamegraph_ticks(flamegraph) > 0)
        },
    )
    .await?;

    assert_connect_flamegraph_equal("SelectMergeStacktraces", &pyroscope, &crabka)
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

async fn assert_diff_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    crabka_base: &str,
) -> TestResult {
    let body = diff_body();
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "Diff",
        body.clone(),
        diff_has_positive_ticks,
    )
    .await?;
    let crabka = connect_json_until(
        client,
        crabka_base,
        Some(TENANT),
        "Diff",
        body,
        diff_has_positive_ticks,
    )
    .await?;

    assert_diff_equal(&pyroscope, &crabka)
}

fn select_merge_stacktraces_body() -> Value {
    json!({
        "profileTypeID": PROFILE_TYPE,
        "labelSelector": SELECTOR,
        "start": query_start_ms(),
        "end": query_end_ms(),
        "maxNodes": 1024,
        "format": "PROFILE_FORMAT_FLAMEGRAPH",
    })
}

fn diff_body() -> Value {
    let query = select_merge_stacktraces_body();
    json!({
        "left": query,
        "right": query,
    })
}

fn select_series_body() -> Value {
    json!({
        "profileTypeID": PROFILE_TYPE,
        "labelSelector": SELECTOR,
        "start": query_start_ms(),
        "end": query_end_ms(),
        "groupBy": ["env"],
        "step": 10.0,
        "aggregation": "TIME_SERIES_AGGREGATION_TYPE_SUM",
        "limit": 10,
    })
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

fn select_series_has_positive_point(value: &Value) -> bool {
    value
        .get("series")
        .and_then(Value::as_array)
        .is_some_and(|series| {
            series.iter().any(|series| {
                series
                    .get("points")
                    .and_then(Value::as_array)
                    .is_some_and(|points| points.iter().any(|point| point_value(point) > 0.0))
            })
        })
}

fn diff_has_positive_ticks(value: &Value) -> bool {
    value.get("flamegraph").is_some_and(|flamegraph| {
        flamegraph
            .get("leftTicks")
            .or_else(|| flamegraph.get("left_ticks"))
            .and_then(json_i64)
            .unwrap_or_default()
            > 0
            && flamegraph
                .get("rightTicks")
                .or_else(|| flamegraph.get("right_ticks"))
                .and_then(json_i64)
                .unwrap_or_default()
                > 0
    })
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
        return Err(format!(
            "flamebearer mismatch:\nexpected summary:\n{}\nactual summary:\n{}\nexpected {expected}\ngot {actual}",
            flamebearer_summary(&expected),
            flamebearer_summary(&actual),
        )
        .into());
    }
    Ok(())
}

fn flamebearer_summary(value: &Value) -> String {
    let names = value
        .get("names")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let levels = value
        .get("levels")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for (level_idx, level) in levels.iter().take(5).enumerate() {
        let Some(values) = level.as_array() else {
            continue;
        };
        let mut x = 0_i64;
        let mut bars = Vec::new();
        for chunk in values.chunks(4).take(8) {
            let [delta, total, self_, name_idx] = chunk else {
                continue;
            };
            x += json_i64(delta).unwrap_or_default();
            let total = json_i64(total).unwrap_or_default();
            let self_ = json_i64(self_).unwrap_or_default();
            let name_idx = json_i64(name_idx).unwrap_or_default();
            let name = usize::try_from(name_idx)
                .ok()
                .and_then(|idx| names.get(idx))
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            bars.push(format!("{name}@{x}+{total}/self={self_}"));
            x += total;
        }
        out.push(format!("L{level_idx}: {}", bars.join(" | ")));
    }
    out.join("\n")
}

fn assert_canonical_json_equal(method: &str, expected: Value, actual: Value) -> TestResult {
    let expected = canonical_json(expected);
    let actual = canonical_json(actual);
    if expected != actual {
        return Err(format!("{method} mismatch: expected {expected}, got {actual}").into());
    }
    Ok(())
}

fn assert_label_names_equal(expected: &Value, actual: &Value) -> TestResult {
    assert_canonical_json_equal(
        "LabelNames",
        canonical_string_list(expected, "names")?,
        canonical_string_list(actual, "names")?,
    )
}

fn assert_connect_flamegraph_equal(method: &str, expected: &Value, actual: &Value) -> TestResult {
    assert_canonical_json_equal(
        method,
        canonical_connect_flamegraph(expected)?,
        canonical_connect_flamegraph(actual)?,
    )
}

fn assert_select_series_equal(expected: &Value, actual: &Value) -> TestResult {
    assert_canonical_json_equal(
        "SelectSeries",
        canonical_select_series(expected)?,
        canonical_select_series(actual)?,
    )
}

fn assert_diff_equal(expected: &Value, actual: &Value) -> TestResult {
    assert_canonical_json_equal("Diff", canonical_diff(expected)?, canonical_diff(actual)?)
}

fn canonical_diff(value: &Value) -> TestResult<Value> {
    let flamegraph = value
        .get("flamegraph")
        .ok_or_else(|| format!("Diff response missing flamegraph object: {value}"))?;
    flamegraph
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Diff flamegraph missing names array: {value}"))?;
    flamegraph
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Diff flamegraph missing levels array: {value}"))?;
    let total = flamegraph
        .get("total")
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing total: {value}"))?;
    let max_self = flamegraph
        .get("maxSelf")
        .or_else(|| flamegraph.get("max_self"))
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing maxSelf: {value}"))?;
    let left_ticks = flamegraph
        .get("leftTicks")
        .or_else(|| flamegraph.get("left_ticks"))
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing leftTicks: {value}"))?;
    let right_ticks = flamegraph
        .get("rightTicks")
        .or_else(|| flamegraph.get("right_ticks"))
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing rightTicks: {value}"))?;

    Ok(json!({
        "total": total,
        "maxSelf": max_self,
        "leftTicks": left_ticks,
        "rightTicks": right_ticks,
    }))
}

fn canonical_select_series(value: &Value) -> TestResult<Value> {
    let series = value
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("SelectSeries response missing series array: {value}"))?;
    let canonical = series
        .iter()
        .map(|series| {
            let labels = series
                .get("labels")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("SelectSeries series missing labels array: {value}"))?
                .iter()
                .map(|label| {
                    Ok(json!({
                        "name": label.get("name").and_then(Value::as_str),
                        "value": label.get("value").and_then(Value::as_str),
                    }))
                })
                .collect::<TestResult<Vec<_>>>()?;
            let points = series
                .get("points")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("SelectSeries series missing points array: {value}"))?
                .iter()
                .map(|point| {
                    point
                        .get("timestamp")
                        .and_then(json_i64)
                        .ok_or_else(|| format!("SelectSeries point missing timestamp: {value}"))?;
                    Ok(json!({
                        "value": point_value(point),
                    }))
                })
                .collect::<TestResult<Vec<_>>>()?;
            Ok(json!({
                "labels": labels,
                "points": points,
            }))
        })
        .collect::<TestResult<Vec<_>>>()?;

    Ok(json!({ "series": canonical }))
}

fn canonical_connect_flamegraph(value: &Value) -> TestResult<Value> {
    let flamegraph = value
        .get("flamegraph")
        .ok_or_else(|| format!("response missing flamegraph object: {value}"))?;
    let names = flamegraph
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamegraph missing names array: {value}"))?;
    let level_values = flamegraph
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamegraph missing levels array: {value}"))?;
    let mut levels = Vec::with_capacity(level_values.len());
    for level in level_values {
        levels.push(
            level
                .get("values")
                .and_then(Value::as_array)
                .cloned()
                .ok_or_else(|| format!("flamegraph level missing values array: {value}"))?,
        );
    }
    let total = flamegraph
        .get("total")
        .and_then(json_i64)
        .ok_or_else(|| format!("flamegraph missing total: {value}"))?;
    let max_self = flamegraph
        .get("maxSelf")
        .or_else(|| flamegraph.get("max_self"))
        .and_then(json_i64)
        .ok_or_else(|| format!("flamegraph missing maxSelf: {value}"))?;

    Ok(json!({
        "names": names,
        "levels": levels,
        "total": total,
        "maxSelf": max_self,
    }))
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

#[test]
fn label_names_differential_rejects_name_drift() {
    let expected = json!({ "names": ["__name__", "env"] });
    let actual = json!({ "names": ["__name__", "service_name"] });

    let err = assert_label_names_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("LabelNames mismatch"));
}

#[test]
fn connect_flamegraph_differential_rejects_tick_drift() {
    let expected = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 7, 0, 0] }, { "values": [0, 7, 7, 1] }],
            "total": 7,
            "maxSelf": 7
        }
    });
    let actual = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 8, 0, 0] }, { "values": [0, 8, 8, 1] }],
            "total": 8,
            "maxSelf": 8
        }
    });

    let err =
        assert_connect_flamegraph_equal("SelectMergeStacktraces", &expected, &actual).unwrap_err();
    assert!(err.to_string().contains("SelectMergeStacktraces mismatch"));
}

#[test]
fn connect_series_differential_rejects_point_drift() {
    let expected = json!({
        "series": [{
            "labels": [{ "name": "env", "value": PROFILE_ENV }],
            "points": [{ "timestamp": 10, "value": 7.0 }]
        }]
    });
    let actual = json!({
        "series": [{
            "labels": [{ "name": "env", "value": PROFILE_ENV }],
            "points": [{ "timestamp": 10, "value": 8.0 }]
        }]
    });

    let err = assert_select_series_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("SelectSeries mismatch"));
}

fn shared_api_tuple() -> Vec<(String, String)> {
    vec![
        ("service_name".to_string(), "api".to_string()),
        ("__profile_type__".to_string(), PROFILE_TYPE.to_string()),
    ]
}

fn projected_set(value: &str, profile_type: &str) -> Value {
    json!({ "labels": [
        { "name": "service_name", "value": value },
        { "name": "__profile_type__", "value": profile_type }
    ] })
}

#[test]
fn series_differential_rejects_wire_key_casing_drift() {
    let tuple = shared_api_tuple();
    let camel = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let snake = json!({ "labels_set": [projected_set("api", PROFILE_TYPE)] });

    let err = assert_series_drilldown_compatible(&camel, &snake, &tuple).unwrap_err();
    assert!(err.to_string().contains("wire key differs"), "{err}");
}

#[test]
fn series_differential_rejects_spurious_empty_label_set() {
    // The defining symptom of the in-memory `series()` bug: a `{}` entry inserted
    // when `labelNames` is empty. The crabka (second) argument carries it.
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let crabka = json!({
        "labelsSet": [
            { "labels": [] },
            projected_set("api", PROFILE_TYPE)
        ]
    });

    let err = assert_series_drilldown_compatible(&pyroscope, &crabka, &tuple).unwrap_err();
    assert!(
        err.to_string().contains("spurious empty label set"),
        "{err}"
    );
}

#[test]
fn series_differential_rejects_missing_shared_tuple_on_crabka() {
    // The core drilldown regression: crabka returns label sets, but NOT the
    // ingested `api` series (e.g. it time-scoped a no-range request to [0,0] and
    // dropped everything but some unrelated series), so the shared tuple is
    // absent and the grid shows "No data".
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let crabka = json!({ "labelsSet": [projected_set("other", PROFILE_TYPE)] });

    let err = assert_series_drilldown_compatible(&pyroscope, &crabka, &tuple).unwrap_err();
    assert!(
        err.to_string().contains("crabka missing shared tuple"),
        "{err}"
    );
}

#[test]
fn series_differential_rejects_empty_crabka_response() {
    // A literal `{}` from crabka (no `labelsSet` at all) is the exact shape the
    // no-range drilldown call elicits today; it fails the wire-key check.
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let crabka = json!({});

    let err = assert_series_drilldown_compatible(&pyroscope, &crabka, &tuple).unwrap_err();
    assert!(err.to_string().contains("wire key differs"), "{err}");
}

#[test]
fn series_differential_rejects_intra_set_key_reordering() {
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [{ "labels": [
        { "name": "service_name", "value": "api" },
        { "name": "__profile_type__", "value": PROFILE_TYPE }
    ] }] });
    let crabka = json!({ "labelsSet": [{ "labels": [
        { "name": "__profile_type__", "value": PROFILE_TYPE },
        { "name": "service_name", "value": "api" }
    ] }] });

    let err = assert_series_drilldown_compatible(&pyroscope, &crabka, &tuple).unwrap_err();
    assert!(
        err.to_string()
            .contains("key order for shared tuple differs"),
        "{err}"
    );
}

#[test]
fn series_differential_accepts_pyroscope_superset() {
    // Real Pyroscope's enumeration is a strict superset (it self-instruments);
    // extra `pyroscope` series on the Pyroscope side must NOT be a difference, as
    // long as the shared `api` tuple is present on both with the same key order.
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [
        projected_set("pyroscope", "process_cpu:cpu:nanoseconds:cpu:nanoseconds"),
        projected_set("api", PROFILE_TYPE)
    ] });
    let crabka = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });

    assert_series_drilldown_compatible(&pyroscope, &crabka, &tuple).unwrap();
}

#[test]
fn series_full_differential_rejects_spurious_empty_label_set() {
    let pyroscope = json!({ "labelsSet": [{ "labels": [
        { "name": "__name__", "value": "goroutines" },
        { "name": "__profile_type__", "value": PROFILE_TYPE },
        { "name": "env", "value": PROFILE_ENV },
        { "name": "service_name", "value": "api" }
    ] }] });
    let crabka = json!({ "labelsSet": [{ "labels": [] }] });

    let err = assert_series_full_compatible(&pyroscope, &crabka).unwrap_err();
    assert!(
        err.to_string().contains("spurious empty label set"),
        "{err}"
    );
}

#[test]
fn profile_stats_differential_rejects_int_representation_drift() {
    // Canonical proto-JSON encodes int64 as a string; a number-typed time is a
    // representation deviation the drilldown parser is sensitive to.
    let as_string =
        json!({ "dataIngested": true, "oldestProfileTime": "1000", "newestProfileTime": "2000" });
    let as_number =
        json!({ "dataIngested": true, "oldestProfileTime": 1000, "newestProfileTime": 2000 });

    let err = assert_get_profile_stats_compatible(&as_string, &as_number).unwrap_err();
    assert!(
        err.to_string().contains("JSON representation differs"),
        "{err}"
    );
}

#[test]
fn profile_stats_differential_accepts_divergent_timestamps() {
    // The two backends ingest at independent wall-clock instants, so differing
    // timestamp magnitudes (same string representation) must NOT be a difference.
    let pyroscope =
        json!({ "dataIngested": true, "oldestProfileTime": "111", "newestProfileTime": "222" });
    let crabka =
        json!({ "dataIngested": true, "oldestProfileTime": "999", "newestProfileTime": "1234" });

    assert_get_profile_stats_compatible(&pyroscope, &crabka).unwrap();
}

#[test]
fn profile_stats_differential_rejects_data_ingested_key_casing_drift() {
    let camel = json!({ "dataIngested": true });
    let snake = json!({ "data_ingested": true });

    let err = assert_get_profile_stats_compatible(&camel, &snake).unwrap_err();
    assert!(
        err.to_string().contains("dataIngested wire key differs"),
        "{err}"
    );
}

#[test]
fn connect_diff_differential_rejects_tick_drift() {
    let expected = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 7, 0, 0, 7, 0, 0] }],
            "total": 14,
            "maxSelf": 0,
            "leftTicks": 7,
            "rightTicks": 7
        }
    });
    let actual = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 7, 0, 0, 8, 0, 0] }],
            "total": 15,
            "maxSelf": 0,
            "leftTicks": 7,
            "rightTicks": 8
        }
    });

    let err = assert_diff_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("Diff mismatch"));
}

// ---------------------------------------------------------------------------
// Comprehensive Grafana end-to-end test
//
// Unlike `grafana_accepts_pyroscope_datasource_pointing_at_crabka` (which only
// registers a datasource and reads it back), this test drives the *full* path:
// ingest a known profile through the real distributor push door, then stand up
// real Grafana with its built-in Pyroscope datasource pointed at Crabka and
// prove that Grafana → grafana-pyroscope-datasource → Crabka works for
//   (1) the config-test / health probe (ProfileTypes through the plugin),
//   (2) a flamegraph query driven *through* Grafana (the real Explore path),
//   (3) multi-tenant isolation enforced through Grafana's per-datasource
//       X-Scope-OrgID header injection.
// ---------------------------------------------------------------------------

/// Regression for the Grafana-compat bug surfaced by `grafana_renders_crabka_profiles_end_to_end`:
/// Grafana's built-in Pyroscope datasource is a connect-go client that issues unary requests
/// with `Content-Type: application/proto` and rejects any 200 response whose content-type does
/// not echo `application/proto`. The Docker-free reproduction sends a real `application/proto`
/// `ProfileTypes` request and asserts the response content-type echoes it. (Docker-free, runs in CI.)
#[tokio::test]
async fn querier_echoes_proto_content_type_for_proto_requests() -> TestResult {
    let store = WalTailProfileStore::new();
    let crabka = start_crabka_public(CapturingSink::default(), store).await?;
    let client = reqwest::Client::new();

    // An all-default ProfileTypesRequest (start=end=0) encodes to zero proto bytes, so an
    // empty body with Content-Type application/proto is a valid Connect unary proto request.
    let response = client
        .post(format!(
            "http://127.0.0.1:{}/querier.v1.QuerierService/ProfileTypes",
            crabka.querier_port
        ))
        .header(reqwest::header::CONTENT_TYPE, "application/proto")
        .header("x-scope-orgid", TENANT)
        .body(Vec::<u8>::new())
        .send()
        .await?;

    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response.text().await.unwrap_or_default();

    crabka.shutdown();

    assert!(
        status.is_success(),
        "ProfileTypes (application/proto) returned {status}: ct=`{content_type}` body=`{body}`"
    );
    assert!(
        content_type.starts_with("application/proto"),
        "ProfileTypes (application/proto) response must echo application/proto, got `{content_type}` (status {status})"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
async fn grafana_renders_crabka_profiles_end_to_end() -> TestResult {
    let client = reqwest::Client::new();

    let sample_time_ns = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
        .map_err(|_| "current time does not fit i64 nanoseconds")?;
    let now_ms = sample_time_ns / 1_000_000;
    let from_ms = now_ms - 3_600_000;
    let to_ms = now_ms + 3_600_000;

    // 1. Ingest a known CPU profile for tenant-a through the real distributor push door,
    //    then replay the captured WAL records into the querier's hot store.
    let gzipped = synthetic_cpu_pprof(sample_time_ns)?;
    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let crabka = start_crabka_public(sink.clone(), store.clone()).await?;
    post_cpu_profile(&client, &crabka.distributor_base, Some(TENANT), &gzipped).await?;
    for record in sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone()
    {
        store.append_record(record)?;
    }

    // 2. Real Grafana + its built-in Pyroscope datasource, one per tenant. Each datasource
    //    injects its own X-Scope-OrgID via the standard custom-HTTP-header mechanism, so the
    //    backend plugin tags every outgoing request to Crabka with the tenant.
    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, 3000).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;
    let crabka_url = format!("http://host.docker.internal:{}", crabka.querier_port);
    let uid_a = create_pyroscope_datasource(
        &client,
        &grafana_base,
        "Crabka Profiles A",
        &crabka_url,
        TENANT,
    )
    .await?;
    let uid_b = create_pyroscope_datasource(
        &client,
        &grafana_base,
        "Crabka Profiles B",
        &crabka_url,
        TENANT_B,
    )
    .await?;

    // 3. Config-test / health probe: Grafana's datasource health check drives ProfileTypes
    //    through the plugin to Crabka (the spec's health surface; there is no /ready).
    let health = datasource_health_until_ok(&client, &grafana_base, &uid_a).await?;
    assert!(
        datasource_health_is_ok(&health),
        "tenant-a datasource health not OK: {health}"
    );

    // 4. Drive a flamegraph query THROUGH Grafana and assert Crabka's symbolized data returns.
    let query_a = GrafanaQuery {
        grafana_base: &grafana_base,
        uid: &uid_a,
        profile_type: CPU_PROFILE_TYPE,
        selector: E2E_SELECTOR,
        from_ms,
        to_ms,
    };
    let (names_a, positive_a) =
        grafana_profile_evidence_until(&client, &query_a, |names, positive| {
            positive && names.contains(FUNC_WORK)
        })
        .await?;
    for func in [FUNC_WORK, FUNC_HOT] {
        assert!(
            names_a.contains(func),
            "Grafana query must return {func}: {names_a:?}"
        );
    }
    assert!(
        positive_a,
        "Grafana query must return a positive sample value: {names_a:?}"
    );

    // 5. Multi-tenant isolation THROUGH Grafana: tenant-b's datasource must not see any of
    //    tenant-a's profiles, labels, or frames.
    let query_b = GrafanaQuery {
        grafana_base: &grafana_base,
        uid: &uid_b,
        profile_type: CPU_PROFILE_TYPE,
        selector: E2E_SELECTOR,
        from_ms,
        to_ms,
    };
    let (names_b, positive_b) = grafana_profile_evidence(&client, &query_b).await?;
    assert!(
        !names_b.contains(FUNC_WORK) && !names_b.contains(FUNC_HOT),
        "tenant-b leaked tenant-a frames through Grafana: {names_b:?}"
    );
    assert!(
        !positive_b,
        "tenant-b saw tenant-a sample values through Grafana"
    );

    crabka.shutdown();
    Ok(())
}

/// Build a tiny, deterministic single-sample-type CPU pprof (gzipped) with two known
/// functions (`main.work`, `main.hotloop`) so the flamegraph names are assertable.
fn synthetic_cpu_pprof(time_nanos: i64) -> TestResult<Vec<u8>> {
    // string_table: 0="" 1="cpu" 2="nanoseconds" 3=main.work 4=main.hotloop 5="app.go"
    let profile = proto::Profile {
        sample_type: vec![proto::ValueType { r#type: 1, unit: 2 }],
        sample: vec![
            proto::Sample {
                location_id: vec![2, 1], // leaf-first: main.hotloop -> main.work
                value: vec![100],
                label: Vec::new(),
            },
            proto::Sample {
                location_id: vec![1], // main.work
                value: vec![40],
                label: Vec::new(),
            },
        ],
        mapping: vec![proto::Mapping {
            id: 1,
            symbolization: proto::MappingSymbolization::from_parts((true, false, false, false)),
            ..Default::default()
        }],
        location: vec![
            proto::Location {
                id: 1,
                mapping_id: 1,
                address: 0x1000,
                line: vec![proto::Line {
                    function_id: 1,
                    line: 10,
                    column: 0,
                }],
                is_folded: false,
            },
            proto::Location {
                id: 2,
                mapping_id: 1,
                address: 0x2000,
                line: vec![proto::Line {
                    function_id: 2,
                    line: 20,
                    column: 0,
                }],
                is_folded: false,
            },
        ],
        function: vec![
            proto::Function {
                id: 1,
                name: 3,
                system_name: 3,
                filename: 5,
                start_line: 1,
            },
            proto::Function {
                id: 2,
                name: 4,
                system_name: 4,
                filename: 5,
                start_line: 2,
            },
        ],
        string_table: vec![
            String::new(),
            "cpu".to_string(),
            "nanoseconds".to_string(),
            FUNC_WORK.to_string(),
            FUNC_HOT.to_string(),
            "app.go".to_string(),
        ],
        time_nanos,
        duration_nanos: 1_000_000_000,
        period_type: Some(proto::ValueType { r#type: 1, unit: 2 }),
        period: 10_000_000,
        ..Default::default()
    };
    gzip_bytes(&PprofProfile::from(profile).encode())
}

fn gzip_bytes(bytes: &[u8]) -> TestResult<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

async fn post_cpu_profile(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    gzipped_pprof: &[u8],
) -> TestResult {
    let body = json!({
        "series": [{
            "labels": [
                { "name": "__name__", "value": CPU_NAME },
                { "name": "service_name", "value": E2E_SERVICE },
                { "name": "env", "value": "e2e" }
            ],
            "samples": [{
                "rawProfile": BASE64.encode(gzipped_pprof),
                "ID": "crabka-grafana-e2e"
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
        return Err(format!("push.v1 cpu profile to {base} returned {status}: {body}").into());
    }
    Ok(())
}

struct CrabkaPublic {
    distributor_base: String,
    querier_port: u16,
    distributor_shutdown: Option<oneshot::Sender<()>>,
    querier_shutdown: Option<oneshot::Sender<()>>,
}

impl CrabkaPublic {
    fn shutdown(mut self) {
        if let Some(tx) = self.distributor_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.querier_shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Like `start_crabka_pair`, but binds the querier on all interfaces so the Grafana
/// container can reach it via `host.docker.internal:<port>`. The distributor stays
/// host-local (the test pushes to it directly).
async fn start_crabka_public(
    sink: CapturingSink,
    store: WalTailProfileStore,
) -> TestResult<CrabkaPublic> {
    let (distributor_shutdown, distributor_rx) = oneshot::channel();
    let distributor_state = Arc::new(DistributorState {
        sink: Arc::new(sink),
        limits: TenantLimitConfig::default(),
        profile_overrides: OverridesProvider::new(Limits::default()),
        active_series: Mutex::default(),
        ingestion_buckets: Mutex::default(),
        relabel: Vec::new(),
        max_decompressed: 16 * 1024 * 1024,
        metrics: crabka_profiles::metrics::ServiceMetrics::new(),
    });
    let distributor_addr =
        distributor::serve("127.0.0.1:0".parse()?, distributor_state, async move {
            let _ = distributor_rx.await;
        })
        .await?;

    let (querier_shutdown, querier_rx) = oneshot::channel();
    // The differential / e2e corpus intentionally queries the full `[0, i64::MAX]`
    // range to compare against real Pyroscope, so disable the per-query range cap.
    let querier_state = Arc::new(QuerierState::new_with_limits(
        Arc::new(store),
        crabka_profiles::limits::Limits {
            max_query_length_secs: 0,
            ..Default::default()
        },
    ));
    let querier_addr = query::serve("0.0.0.0:0".parse()?, querier_state, async move {
        let _ = querier_rx.await;
    })
    .await?;

    Ok(CrabkaPublic {
        distributor_base: format!("http://{distributor_addr}"),
        querier_port: querier_addr.port(),
        distributor_shutdown: Some(distributor_shutdown),
        querier_shutdown: Some(querier_shutdown),
    })
}

async fn create_pyroscope_datasource(
    client: &reqwest::Client,
    grafana_base: &str,
    name: &str,
    crabka_url: &str,
    tenant: &str,
) -> TestResult<String> {
    let payload = json!({
        "name": name,
        "type": "grafana-pyroscope-datasource",
        "access": "proxy",
        "url": crabka_url,
        "jsonData": { "httpHeaderName1": "X-Scope-OrgID" },
        "secureJsonData": { "httpHeaderValue1": tenant }
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
    created
        .get("datasource")
        .and_then(|datasource| datasource.get("uid"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("datasource create response missing uid: {created}").into())
}

async fn datasource_health_until_ok(
    client: &reqwest::Client,
    grafana_base: &str,
    uid: &str,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..120 {
        match datasource_health(client, grafana_base, uid).await {
            Ok(value) if datasource_health_is_ok(&value) => return Ok(value),
            Ok(value) => last = Some(format!("datasource health not OK: {value}")),
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| "datasource health never became OK".to_string())
        .into())
}

async fn datasource_health(
    client: &reqwest::Client,
    grafana_base: &str,
    uid: &str,
) -> TestResult<Value> {
    let response = client
        .get(format!("{grafana_base}/api/datasources/uid/{uid}/health"))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(format!("datasource health returned {status}: {text}").into());
    }
    serde_json::from_str(&text)
        .map_err(|err| format!("datasource health returned non-JSON `{text}`: {err}").into())
}

fn datasource_health_is_ok(value: &Value) -> bool {
    value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("ok"))
}

struct GrafanaQuery<'a> {
    grafana_base: &'a str,
    uid: &'a str,
    profile_type: &'a str,
    selector: &'a str,
    from_ms: i64,
    to_ms: i64,
}

/// Collect the function names and a positive-value flag returned for a profile query,
/// driven through Grafana. Tries the real `/api/ds/query` Explore path first (the backend
/// plugin applies the datasource's X-Scope-OrgID header), then the data-source proxy →
/// Crabka flamebearer as a best-effort second source. The union is returned.
async fn grafana_profile_evidence(
    client: &reqwest::Client,
    query: &GrafanaQuery<'_>,
) -> TestResult<(BTreeSet<String>, bool)> {
    let mut names = BTreeSet::new();
    let mut positive = false;

    if let Ok(value) = ds_query_profile(client, query).await {
        let (frame_names, frame_positive) = evidence_from_ds_query(&value);
        names.extend(frame_names);
        positive = positive || frame_positive;
    }

    if let Some(value) = proxy_render(client, query).await {
        names.extend(flame_names(&value));
        positive = positive || flame_ticks(&value).is_some_and(|ticks| ticks > 0);
    }

    Ok((names, positive))
}

async fn grafana_profile_evidence_until(
    client: &reqwest::Client,
    query: &GrafanaQuery<'_>,
    ready: impl Fn(&BTreeSet<String>, bool) -> bool,
) -> TestResult<(BTreeSet<String>, bool)> {
    let mut last = None;
    for _ in 0..120 {
        match grafana_profile_evidence(client, query).await {
            Ok((names, positive)) if ready(&names, positive) => return Ok((names, positive)),
            Ok((names, positive)) => {
                last = Some(format!(
                    "evidence not ready: names={names:?} positive={positive}"
                ));
            }
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| "Grafana profile evidence never became ready".to_string())
        .into())
}

async fn ds_query_profile(client: &reqwest::Client, query: &GrafanaQuery<'_>) -> TestResult<Value> {
    let body = json!({
        "from": query.from_ms.to_string(),
        "to": query.to_ms.to_string(),
        "queries": [{
            "refId": "A",
            "datasource": { "type": "grafana-pyroscope-datasource", "uid": query.uid },
            "queryType": "profile",
            "profileTypeId": query.profile_type,
            "labelSelector": query.selector,
            "groupBy": [],
            "maxNodes": 8192,
            "intervalMs": 60000,
            "maxDataPoints": 1000
        }]
    });
    let response = client
        .post(format!("{}/api/ds/query", query.grafana_base))
        .basic_auth("admin", Some("admin"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(format!("/api/ds/query returned {status}: {text}").into());
    }
    serde_json::from_str(&text)
        .map_err(|err| format!("/api/ds/query returned non-JSON `{text}`: {err}").into())
}

/// Walk the Grafana dataframe response column-major: collect every string cell as a
/// candidate frame name and flag any strictly-positive numeric cell. Schema-agnostic so it
/// tolerates Grafana version drift in field names.
fn evidence_from_ds_query(value: &Value) -> (BTreeSet<String>, bool) {
    let mut names = BTreeSet::new();
    let mut positive = false;
    let Some(results) = value.get("results").and_then(Value::as_object) else {
        return (names, positive);
    };
    for result in results.values() {
        let Some(frames) = result.get("frames").and_then(Value::as_array) else {
            continue;
        };
        for frame in frames {
            let Some(columns) = frame.pointer("/data/values").and_then(Value::as_array) else {
                continue;
            };
            for column in columns {
                let Some(cells) = column.as_array() else {
                    continue;
                };
                for cell in cells {
                    if let Some(text) = cell.as_str() {
                        names.insert(text.to_string());
                    } else if cell.as_f64().is_some_and(|number| number > 0.0) {
                        positive = true;
                    }
                }
            }
        }
    }
    (names, positive)
}

/// Best-effort: query Crabka's legacy flamebearer render through Grafana's data-source
/// proxy. Returns `None` if the proxy route is unavailable (then `/api/ds/query` carries
/// the test).
async fn proxy_render(client: &reqwest::Client, query: &GrafanaQuery<'_>) -> Option<Value> {
    let render_query = format!("{}{}", query.profile_type, query.selector);
    let encoded = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", &render_query)
        .append_pair("from", &query.from_ms.to_string())
        .append_pair("until", &query.to_ms.to_string())
        .append_pair("format", "json")
        .finish();
    let response = client
        .get(format!(
            "{}/api/datasources/proxy/uid/{}/pyroscope/render?{encoded}",
            query.grafana_base, query.uid
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

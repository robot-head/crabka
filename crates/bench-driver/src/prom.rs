//! Prometheus query client. The driver issues a small set of instant
//! queries at scenario end to capture resource usage on the broker pods
//! and (Strimzi only) JVM heap / non-heap from the JMX exporter.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

use crate::scenario::{BrokerSample, Resource, Stack};

pub struct PromClient {
    base_url: String,
    http: reqwest::Client,
}

impl PromClient {
    /// `base_url` is the Prometheus root, e.g. `http://prom.monitoring.svc:9090`.
    /// No trailing slash required.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        })
    }

    /// Execute a `PromQL` instant query. Returns the first scalar value
    /// across all returned series, summed. Returns `None` when the result
    /// vector is empty.
    pub async fn query_scalar_sum(&self, query: &str) -> Result<Option<f64>> {
        let url = format!("{}/api/v1/query", self.base_url);
        let body: PromResp = self
            .http
            .get(&url)
            .query(&[("query", query)])
            .send()
            .await
            .with_context(|| format!("GET {url} query={query}"))?
            .error_for_status()
            .with_context(|| "prometheus non-2xx")?
            .json()
            .await
            .context("decode prometheus json")?;

        if body.status != "success" {
            return Err(anyhow!(
                "prometheus query failed: status={} err={:?}",
                body.status,
                body.error
            ));
        }
        let Some(data) = body.data else {
            return Ok(None);
        };
        if data.result.is_empty() {
            return Ok(None);
        }
        let mut sum = 0.0_f64;
        let mut had = false;
        for r in &data.result {
            if let Some((_, v)) = r.value.as_ref()
                && let Ok(parsed) = v.parse::<f64>()
            {
                sum += parsed;
                had = true;
            }
        }
        Ok(had.then_some(sum))
    }

    /// Execute a `PromQL` range query, summing across all returned series per
    /// timestamp. Returns `(unix_seconds, value)` points on the step grid.
    pub async fn query_range_sum(
        &self,
        query: &str,
        start_s: f64,
        end_s: f64,
        step_s: u64,
    ) -> Result<Vec<(f64, f64)>> {
        let url = format!("{}/api/v1/query_range", self.base_url);
        let start = format!("{start_s}");
        let end = format!("{end_s}");
        let step = format!("{step_s}s");
        let body: PromResp = self
            .http
            .get(&url)
            .query(&[
                ("query", query),
                ("start", start.as_str()),
                ("end", end.as_str()),
                ("step", step.as_str()),
            ])
            .send()
            .await
            .with_context(|| format!("GET {url} (range) query={query}"))?
            .error_for_status()
            .with_context(|| "prometheus non-2xx (range)")?
            .json()
            .await
            .context("decode prometheus range json")?;

        if body.status != "success" {
            return Err(anyhow!(
                "prometheus range query failed: status={} err={:?}",
                body.status,
                body.error
            ));
        }
        let Some(data) = body.data else {
            return Ok(Vec::new());
        };
        // Sum across series per timestamp (keyed by ms to dedupe float ts).
        let mut by_ts: BTreeMap<u64, f64> = BTreeMap::new();
        for r in &data.result {
            if let Some(vals) = &r.values {
                for (ts, v) in vals {
                    if let Ok(parsed) = v.parse::<f64>() {
                        *by_ts.entry((ts * 1000.0).round() as u64).or_insert(0.0) += parsed;
                    }
                }
            }
        }
        Ok(by_ts
            .into_iter()
            .map(|(ts_ms, v)| (ts_ms as f64 / 1000.0, v))
            .collect())
    }

    /// Capture a broker CPU/memory **time series** over `[start_s, end_s]` for
    /// graphing values over the test. CPU is a 1-minute `rate()` (in cores),
    /// memory is the summed working set. Aligned onto a single `step_s` grid;
    /// `t_offset_ms` is relative to `start_s`.
    pub async fn capture_resource_series(
        &self,
        stack: Stack,
        namespace: &str,
        start_s: f64,
        end_s: f64,
        step_s: u64,
    ) -> Result<Vec<BrokerSample>> {
        let pod_re = format!("{}.*", stack.broker_pod_regex().trim_start_matches('^'));
        let cpu_q = format!(
            "sum(rate(container_cpu_usage_seconds_total{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}}[1m]))"
        );
        let mem_q = format!(
            "sum(container_memory_working_set_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}})"
        );
        let cpu = self
            .query_range_sum(&cpu_q, start_s, end_s, step_s)
            .await
            .unwrap_or_default();
        let mem = self
            .query_range_sum(&mem_q, start_s, end_s, step_s)
            .await
            .unwrap_or_default();

        let start_ms = (start_s * 1000.0).round() as u64;
        let mut by_ts: BTreeMap<u64, (f64, u64)> = BTreeMap::new();
        for (ts, v) in cpu {
            by_ts.entry((ts * 1000.0).round() as u64).or_default().0 = v;
        }
        for (ts, v) in mem {
            by_ts.entry((ts * 1000.0).round() as u64).or_default().1 = v as u64;
        }
        Ok(by_ts
            .into_iter()
            .map(|(ts_ms, (cpu_cores, mem))| BrokerSample {
                t_offset_ms: ts_ms.saturating_sub(start_ms),
                cpu_cores,
                mem_working_set_bytes: mem,
            })
            .collect())
    }

    /// Capture broker resource usage for the given stack over a window
    /// ending now. `window_s` should be slightly larger than the measured
    /// scenario duration so the `rate()` window doesn't tail off.
    pub async fn capture_resource(
        &self,
        stack: Stack,
        namespace: &str,
        window_s: u64,
        msgs_produced: u64,
    ) -> Result<Resource> {
        // `broker_pod_regex()` returns a `^`-anchored *prefix* (e.g.
        // `^demo-broker`) shared with `failover.rs`, which strips the `^` and
        // uses it via `starts_with`. PromQL `=~` matchers are *fully* anchored
        // (`^demo-broker` is compiled as `^(?:^demo-broker)$`), so the bare
        // prefix matches a pod named *exactly* `demo-broker` and never the real
        // `demo-broker-0-0` — every series came back empty → all resource
        // numbers 0. Rebuild it as an unanchored prefix-glob: drop the `^` and
        // append `.*` so `demo-broker.*` matches the ordinal-suffixed pods.
        let pod_re = format!("{}.*", stack.broker_pod_regex().trim_start_matches('^'));
        let pod_re = pod_re.as_str();
        let win = window_s.max(15); // PromQL needs at least one full scrape

        // GKE's kubelet-cadvisor series carry `pod`/`namespace` but NO
        // `container` label — so the old `container!=""` filter matched nothing
        // and every resource number came back 0. Each pod instead has one
        // pod-level cgroup series (cgroup `id` ends in `.slice`) plus a
        // per-container series (`id` ends in `.scope`). Match only the pod-level
        // rollup to get the pod's true total (all containers + pause) without
        // double-counting. The matcher is fully anchored, so `.*slice` keeps the
        // `…pod<uid>.slice` rollup and drops the `…/cri-…scope` children
        // (verified: pod-level ≈ Σ containers, vs ~2× when summing everything).

        let cpu_query = format!(
            "sum(rate(container_cpu_usage_seconds_total{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}}[{win}s]) * {win})"
        );
        let rss_query = format!(
            "max_over_time(sum(container_memory_working_set_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",id=~\".*slice\"}})[{win}s:15s])"
        );

        let broker_cpu_seconds = self.query_scalar_sum(&cpu_query).await?.unwrap_or(0.0);
        let mem_working = self.query_scalar_sum(&rss_query).await?.unwrap_or(0.0) as u64;

        let mut res = Resource {
            broker_cpu_seconds,
            mem_cgroup_working_set_bytes: mem_working,
            jvm_heap_used_bytes: None,
            jvm_nonheap_used_bytes: None,
            kafka_page_cache_approx_bytes: None,
            msgs_per_cpu_core: if broker_cpu_seconds > 0.0 {
                msgs_produced as f64 / broker_cpu_seconds
            } else {
                0.0
            },
        };

        if matches!(stack, Stack::Kafka) {
            // Strimzi 0.46 ships the new Prometheus Java client (1.x), whose
            // built-in JVM collector publishes `jvm_memory_used_bytes{area=...}`
            // — note the `used_bytes` suffix order, NOT the legacy simpleclient
            // `jvm_memory_bytes_used`. (Querying the old name returned empty, so
            // the JVM heap/non-heap split was silently 0.) The `area` label is
            // lowercase `heap`/`nonheap`, matching the selectors below.
            let heap_q = format!(
                "max_over_time(sum(jvm_memory_used_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",area=\"heap\"}})[{win}s:15s])"
            );
            let nonheap_q = format!(
                "max_over_time(sum(jvm_memory_used_bytes{{namespace=\"{namespace}\",pod=~\"{pod_re}\",area=\"nonheap\"}})[{win}s:15s])"
            );
            let heap = self.query_scalar_sum(&heap_q).await?.unwrap_or(0.0) as u64;
            let nonheap = self.query_scalar_sum(&nonheap_q).await?.unwrap_or(0.0) as u64;
            res.jvm_heap_used_bytes = Some(heap);
            res.jvm_nonheap_used_bytes = Some(nonheap);
            res.kafka_page_cache_approx_bytes =
                Some(mem_working as i64 - heap as i64 - nonheap as i64);
        }

        Ok(res)
    }
}

// ── Prometheus HTTP API types (minimal) ─────────────────────────────────────

#[derive(Debug, Deserialize)]
struct PromResp {
    status: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    data: Option<PromData>,
}

#[derive(Debug, Deserialize)]
struct PromData {
    #[serde(default)]
    result: Vec<PromResult>,
}

#[derive(Debug, Deserialize)]
struct PromResult {
    /// `[<unix-ts>, "<string-value>"]` — Prometheus encodes the scalar
    /// portion as a JSON string. The timestamp is f64 in seconds.
    #[serde(default)]
    value: Option<(f64, String)>,
    /// Matrix (range-query) payload: `[[ts, "val"], ...]`.
    #[serde(default)]
    values: Option<Vec<(f64, String)>>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parses_success_with_one_result() {
        let json = r#"{
          "status": "success",
          "data": {
            "resultType": "vector",
            "result": [
              { "metric": {}, "value": [1234.0, "12.5"] }
            ]
          }
        }"#;
        let p: PromResp = serde_json::from_str(json).unwrap();
        assert!(p.status == "success");
        assert!(p.data.unwrap().result.len() == 1);
    }

    #[test]
    fn parses_empty_result_set() {
        let json = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
        let p: PromResp = serde_json::from_str(json).unwrap();
        assert!(p.data.unwrap().result.is_empty());
    }

    #[test]
    fn parses_error_response() {
        let json = r#"{"status":"error","error":"bad query"}"#;
        let p: PromResp = serde_json::from_str(json).unwrap();
        assert!(p.status == "error");
        assert!(p.error.as_deref() == Some("bad query"));
    }
}

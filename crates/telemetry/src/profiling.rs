//! In-process profiling admin server.
//!
//! Always serves a CPU pprof profile at `GET /debug/pprof/profile?seconds=N`
//! on Unix targets. When the `heap-profiling` feature is enabled (jemalloc),
//! also serves a heap pprof profile at `GET /debug/pprof/heap`. Grafana Alloy
//! `pyroscope.scrape` pulls both. The same admin server can carry extra routes
//! (e.g. `/metrics`).
//!
//! Bodies are gzipped `Profile` protobufs — the standard pprof file format
//! (what Go's net/http/pprof serves). Alloy's `pyroscope.scrape` forwards the
//! scraped bytes verbatim as the push API's `raw_profile`, and the ingester
//! gunzips them; returning a bare (uncompressed) protobuf makes that gunzip
//! fail with "invalid gzip header".
//!
//! CPU profiling uses POSIX signals and is therefore gated to Unix; a 503 stub
//! is returned on non-Unix targets so the crate compiles on all platforms.

use std::{net::SocketAddr, str::FromStr};

use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use clap::Args;
#[cfg(unix)]
use crabka_units::convert::TimeExt as _;
use crabka_units::{Frequency, Time, convert::FrequencyExt as _, parse, per_sec, secs};
use refined_type::rule::GreaterI32;
use serde::Deserialize;
use thiserror::Error;

type RefinedPositiveFrequency = GreaterI32<0>;

/// Profiling configuration or admin-server failure.
#[derive(Debug, Error)]
pub enum ProfilingError {
    #[error("invalid profiling configuration: {0}")]
    Config(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A positive, finite, whole-Hz sampling frequency accepted by `pprof`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProfilingSampleFrequency {
    frequency: Frequency,
    hertz: i32,
}

impl ProfilingSampleFrequency {
    /// Validate a profiling sampling frequency.
    ///
    /// # Errors
    /// Returns an error unless the frequency is positive, finite, whole Hz,
    /// and representable by `pprof`'s signed frequency input.
    pub fn new(frequency: Frequency) -> Result<Self, String> {
        let hertz = frequency.per_sec_f64();
        if !hertz.is_finite() || hertz.fract() != 0.0 || hertz > f64::from(i32::MAX) {
            return Err("profiling sample frequency must be finite whole Hz".to_string());
        }
        let hertz = i32::try_from(frequency.per_sec_u64())
            .map_err(|_| "profiling sample frequency exceeds i32".to_string())?;
        RefinedPositiveFrequency::new(hertz)
            .map_err(|error| format!("profiling sample frequency: {error}"))?;
        Ok(Self { frequency, hertz })
    }

    #[cfg(unix)]
    fn hertz(self) -> i32 {
        self.hertz
    }

    /// Return the dimensioned sampling frequency.
    #[must_use]
    pub fn frequency(self) -> Frequency {
        self.frequency
    }
}

impl FromStr for ProfilingSampleFrequency {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(parse::frequency(value).map_err(|error| error.to_string())?)
    }
}

/// Process-local CPU and heap profiling policy.
#[derive(Args, Clone, Debug, PartialEq)]
pub struct ProfilingConfig {
    #[arg(long, env = "CRABKA_PROFILING_CPU_DEFAULT_DURATION", default_value = "30s", value_parser = parse::positive_time)]
    pub profiling_cpu_default_duration: Time,
    #[arg(long, env = "CRABKA_PROFILING_CPU_MAX_DURATION", default_value = "60s", value_parser = parse::positive_time)]
    pub profiling_cpu_max_duration: Time,
    #[arg(
        long,
        env = "CRABKA_PROFILING_CPU_SAMPLE_FREQUENCY",
        default_value = "99Hz"
    )]
    pub profiling_cpu_sample_frequency: ProfilingSampleFrequency,
    #[arg(long, env = "CRABKA_PROFILING_HEAP_DEFAULT_DURATION", default_value = "5s", value_parser = parse::positive_time)]
    pub profiling_heap_default_duration: Time,
    #[arg(long, env = "CRABKA_PROFILING_HEAP_MAX_DURATION", default_value = "30s", value_parser = parse::positive_time)]
    pub profiling_heap_max_duration: Time,
    #[arg(
        long,
        env = "CRABKA_PROFILING_NATIVE_FRAME_BLOCKLIST",
        default_value = "libc,libgcc,pthread,vdso",
        value_delimiter = ','
    )]
    pub profiling_native_frame_blocklist: Vec<String>,
}

impl ProfilingConfig {
    /// Validate related profiling bounds.
    ///
    /// # Errors
    /// Returns an error when a default exceeds its maximum or a maximum is
    /// below the compatible one-second request floor.
    pub fn validate(&self) -> Result<(), String> {
        if self.profiling_cpu_default_duration > self.profiling_cpu_max_duration {
            return Err("profiling CPU default duration exceeds maximum".to_string());
        }
        if self.profiling_heap_default_duration > self.profiling_heap_max_duration {
            return Err("profiling heap default duration exceeds maximum".to_string());
        }
        if self.profiling_cpu_max_duration < secs(1) || self.profiling_heap_max_duration < secs(1) {
            return Err("profiling maximum duration must be at least 1s".to_string());
        }
        Ok(())
    }
}

impl Default for ProfilingConfig {
    fn default() -> Self {
        Self {
            profiling_cpu_default_duration: secs(30),
            profiling_cpu_max_duration: secs(60),
            profiling_cpu_sample_frequency: ProfilingSampleFrequency {
                frequency: per_sec(99),
                hertz: 99,
            },
            profiling_heap_default_duration: secs(5),
            profiling_heap_max_duration: secs(30),
            profiling_native_frame_blocklist: vec![
                "libc".to_string(),
                "libgcc".to_string(),
                "pthread".to_string(),
                "vdso".to_string(),
            ],
        }
    }
}

#[derive(Debug, Deserialize)]
struct CpuQuery {
    #[cfg_attr(not(unix), allow(dead_code))]
    seconds: Option<u64>,
}

#[cfg(all(unix, feature = "heap-profiling"))]
#[derive(Debug, Deserialize)]
struct HeapQuery {
    seconds: Option<u64>,
}

/// CPU profile in pprof protobuf, sampled for `?seconds=N` (default 30, clamped 1..=60).
#[cfg(unix)]
async fn cpu_profile(
    State(config): State<ProfilingConfig>,
    Query(q): Query<CpuQuery>,
) -> axum::response::Response {
    // pprof::protos::Message re-exports the prost 0.12 Message trait bundled
    // inside the pprof crate, which is the version Profile was generated with.
    use pprof::protos::Message as _;

    let duration = requested_duration(
        q.seconds,
        config.profiling_cpu_default_duration,
        config.profiling_cpu_max_duration,
    );
    let blocklist = config
        .profiling_native_frame_blocklist
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(config.profiling_cpu_sample_frequency.hertz())
        .blocklist(&blocklist)
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("profiler: {e}")).into_response();
        }
    };
    tokio::time::sleep(duration.to_std()).await;
    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("report: {e}")).into_response();
        }
    };
    let profile = match report.pprof() {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("pprof: {e}")).into_response();
        }
    };
    let body = gzip(&profile.encode_to_vec());
    (
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        body,
    )
        .into_response()
}

/// Gzip a buffer — the pprof file format is a gzipped `Profile` protobuf.
#[cfg(unix)]
fn gzip(raw: &[u8]) -> Vec<u8> {
    use std::io::Write as _;

    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(raw.len() / 2),
        flate2::Compression::fast(),
    );
    encoder
        .write_all(raw)
        .expect("gzip of in-memory buffer is infallible");
    encoder
        .finish()
        .expect("gzip finish of in-memory buffer is infallible")
}

/// Stub for non-Unix targets: CPU profiling is unavailable.
// cargo-mutants: non-Unix stub is not built or exercised on the default Linux mutation run.
#[cfg(not(unix))]
#[cfg_attr(test, mutants::skip)]
async fn cpu_profile(
    _config: State<ProfilingConfig>,
    _q: Query<CpuQuery>,
) -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "CPU profiling requires a Unix target",
    )
        .into_response()
}

// cargo-mutants: optional heap-profiling route is feature-gated out of the default mutation run.
#[cfg(all(unix, feature = "heap-profiling"))]
#[cfg_attr(test, mutants::skip)]
async fn heap_profile(
    State(config): State<ProfilingConfig>,
    Query(q): Query<HeapQuery>,
) -> axum::response::Response {
    let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "jemalloc profiling not enabled (build with --features heap-profiling and set MALLOC_CONF)",
        )
            .into_response();
    };
    let mut ctl = ctl.lock().await;
    let activated_here = !ctl.activated();
    if activated_here {
        if let Err(e) = ctl.activate() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("jemalloc prof activate: {e}"),
            )
                .into_response();
        }
        let duration = requested_duration(
            q.seconds,
            config.profiling_heap_default_duration,
            config.profiling_heap_max_duration,
        );
        tokio::time::sleep(duration.to_std()).await;
    }
    let dump = ctl.dump_pprof();
    if activated_here && let Err(e) = ctl.deactivate() {
        tracing::warn!(error = %e, "could not deactivate jemalloc profiling after heap dump");
    }
    match dump {
        Ok(pprof) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            pprof,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("heap dump: {e}")).into_response(),
    }
}

/// The pprof routes with explicit policy: CPU always (returns 503 on
/// non-Unix); heap under the `heap-profiling` feature (Unix only).
///
/// # Errors
/// Returns an error when related profiling duration bounds are invalid.
pub fn pprof_router_with_config(config: ProfilingConfig) -> Result<Router, ProfilingError> {
    config.validate().map_err(ProfilingError::Config)?;
    Ok(pprof_router_unchecked(config))
}

fn pprof_router_unchecked(config: ProfilingConfig) -> Router {
    let router = Router::new().route("/debug/pprof/profile", get(cpu_profile));
    #[cfg(all(unix, feature = "heap-profiling"))]
    let router = router.route("/debug/pprof/heap", get(heap_profile));
    router.with_state(config)
}

/// The pprof routes with the compatible default policy.
pub fn pprof_router() -> Router {
    pprof_router_unchecked(ProfilingConfig::default())
}

#[cfg(unix)]
fn requested_duration(seconds: Option<u64>, default: Time, maximum: Time) -> Time {
    seconds
        .map_or(default, |seconds| {
            secs(u32::try_from(seconds.max(1)).unwrap_or(u32::MAX))
        })
        .min(maximum)
}

/// Bind an admin HTTP server on `addr` serving `pprof_router()` merged with
/// `extra` (e.g. a `/metrics` route). Spawns the server and returns once bound.
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn serve_admin(addr: SocketAddr, extra: Router) -> std::io::Result<()> {
    serve_router(addr, pprof_router().merge(extra)).await
}

async fn serve_router(addr: SocketAddr, app: Router) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "profiling admin server listening");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::warn!(error = %e, "admin server error");
        }
    });
    Ok(())
}

/// Bind a profiling admin server with explicit policy.
///
/// # Errors
/// Returns an error for invalid profiling configuration or listener failure.
pub async fn serve_admin_with_config(
    addr: SocketAddr,
    extra: Router,
    config: ProfilingConfig,
) -> Result<(), ProfilingError> {
    let app = pprof_router_with_config(config)?.merge(extra);
    serve_router(addr, app).await?;
    Ok(())
}

/// Like [`serve_admin`] but resolves the bind address from
/// `CRABKA_ADMIN_LISTEN_ADDR`, falling back to `default_addr`.
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn serve_admin_from_env(default_addr: &str) -> std::io::Result<()> {
    serve_admin_from_env_with(default_addr, Router::new()).await
}

/// Like [`serve_admin_from_env`] but merges `extra` (e.g. a `GET /metrics`
/// route) alongside the pprof routes. Services that expose Prometheus metrics
/// call this with their `/metrics` router so the exporter shares the admin port.
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
/// # Panics
/// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
pub async fn serve_admin_from_env_with(default_addr: &str, extra: Router) -> std::io::Result<()> {
    let raw =
        std::env::var("CRABKA_ADMIN_LISTEN_ADDR").unwrap_or_else(|_| default_addr.to_string());
    let addr: SocketAddr = raw
        .parse()
        .unwrap_or_else(|e| panic!("invalid CRABKA_ADMIN_LISTEN_ADDR `{raw}`: {e}"));
    serve_admin(addr, extra).await
}

/// Like [`serve_admin_from_env_with`] with explicit profiling policy.
///
/// # Errors
/// Returns an error for invalid profiling configuration or listener failure.
///
/// # Panics
/// Panics when `CRABKA_ADMIN_LISTEN_ADDR` is not a socket address, preserving
/// the default-compatible wrapper's behavior.
pub async fn serve_admin_from_env_with_config(
    default_addr: &str,
    extra: Router,
    config: ProfilingConfig,
) -> Result<(), ProfilingError> {
    let raw =
        std::env::var("CRABKA_ADMIN_LISTEN_ADDR").unwrap_or_else(|_| default_addr.to_string());
    let addr: SocketAddr = raw
        .parse()
        .unwrap_or_else(|e| panic!("invalid CRABKA_ADMIN_LISTEN_ADDR `{raw}`: {e}"));
    serve_admin_with_config(addr, extra, config).await
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use crabka_units::{convert::FrequencyExt as _, millis, minutes};

    use super::*;

    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(flatten)]
        profiling: ProfilingConfig,
    }

    #[test]
    fn profiling_config_defaults_and_overrides() {
        let defaults = TestCli::parse_from(["test"]).profiling;
        assert_eq!(defaults, ProfilingConfig::default());

        let configured = TestCli::try_parse_from([
            "test",
            "--profiling-cpu-default-duration=2s",
            "--profiling-cpu-max-duration=3s",
            "--profiling-cpu-sample-frequency=101Hz",
            "--profiling-heap-default-duration=4s",
            "--profiling-heap-max-duration=5s",
            "--profiling-native-frame-blocklist=libc,custom",
        ])
        .expect("valid profiling policy")
        .profiling;
        assert_eq!(configured.profiling_cpu_default_duration, secs(2));
        assert_eq!(configured.profiling_cpu_max_duration, secs(3));
        assert_eq!(
            configured.profiling_cpu_sample_frequency.frequency(),
            Frequency::from_per_sec(101.0)
        );
        assert_eq!(configured.profiling_heap_default_duration, secs(4));
        assert_eq!(configured.profiling_heap_max_duration, secs(5));
        assert_eq!(
            configured.profiling_native_frame_blocklist,
            ["libc", "custom"]
        );
        assert!(configured.validate().is_ok());
    }

    #[test]
    fn profiling_config_rejects_invalid_values_and_bounds() {
        for argument in [
            "--profiling-cpu-default-duration=0s",
            "--profiling-cpu-max-duration=-1s",
            "--profiling-cpu-sample-frequency=0Hz",
            "--profiling-cpu-sample-frequency=1.5Hz",
            "--profiling-heap-default-duration=0s",
            "--profiling-heap-max-duration=-1s",
        ] {
            assert!(TestCli::try_parse_from(["test", argument]).is_err());
        }

        let cpu_bounds = TestCli::parse_from([
            "test",
            "--profiling-cpu-default-duration=2s",
            "--profiling-cpu-max-duration=1s",
        ]);
        assert!(cpu_bounds.profiling.validate().is_err());

        let heap_bounds = TestCli::parse_from([
            "test",
            "--profiling-heap-default-duration=2s",
            "--profiling-heap-max-duration=1s",
        ]);
        assert!(heap_bounds.profiling.validate().is_err());
    }

    #[test]
    fn requested_profile_duration_uses_configured_default_floor_and_cap() {
        assert_eq!(requested_duration(None, secs(2), secs(5)), secs(2));
        assert_eq!(requested_duration(Some(0), secs(2), secs(5)), secs(1));
        assert_eq!(requested_duration(Some(3), secs(2), secs(5)), secs(3));
        assert_eq!(requested_duration(Some(9), secs(2), secs(5)), secs(5));
        assert!(
            ProfilingConfig {
                profiling_cpu_max_duration: millis(500),
                ..ProfilingConfig::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            ProfilingConfig {
                profiling_heap_default_duration: minutes(1),
                ..ProfilingConfig::default()
            }
            .validate()
            .is_err()
        );
    }
}

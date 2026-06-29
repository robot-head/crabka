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

use std::net::SocketAddr;
#[cfg(unix)]
use std::time::Duration;

use axum::Router;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use serde::Deserialize;

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
async fn cpu_profile(Query(q): Query<CpuQuery>) -> axum::response::Response {
    // pprof::protos::Message re-exports the prost 0.12 Message trait bundled
    // inside the pprof crate, which is the version Profile was generated with.
    use pprof::protos::Message as _;

    let seconds = q.seconds.unwrap_or(30).clamp(1, 60);
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("profiler: {e}")).into_response();
        }
    };
    tokio::time::sleep(Duration::from_secs(seconds)).await;
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
async fn cpu_profile(_q: Query<CpuQuery>) -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "CPU profiling requires a Unix target",
    )
        .into_response()
}

// cargo-mutants: optional heap-profiling route is feature-gated out of the default mutation run.
#[cfg(all(unix, feature = "heap-profiling"))]
#[cfg_attr(test, mutants::skip)]
async fn heap_profile(Query(q): Query<HeapQuery>) -> axum::response::Response {
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
        let seconds = q.seconds.unwrap_or(5).clamp(1, 30);
        tokio::time::sleep(Duration::from_secs(seconds)).await;
    }
    let dump = ctl.dump_pprof();
    if activated_here {
        if let Err(e) = ctl.deactivate() {
            tracing::warn!(error = %e, "could not deactivate jemalloc profiling after heap dump");
        }
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

/// The pprof routes: CPU always (returns 503 on non-Unix); heap under the
/// `heap-profiling` feature (Unix only).
pub fn pprof_router() -> Router {
    let router = Router::new().route("/debug/pprof/profile", get(cpu_profile));
    #[cfg(all(unix, feature = "heap-profiling"))]
    let router = router.route("/debug/pprof/heap", get(heap_profile));
    router
}

/// Bind an admin HTTP server on `addr` serving `pprof_router()` merged with
/// `extra` (e.g. a `/metrics` route). Spawns the server and returns once bound.
pub async fn serve_admin(addr: SocketAddr, extra: Router) -> std::io::Result<()> {
    let app = pprof_router().merge(extra);
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

/// Like [`serve_admin`] but resolves the bind address from
/// `CRABKA_ADMIN_LISTEN_ADDR`, falling back to `default_addr`.
pub async fn serve_admin_from_env(default_addr: &str) -> std::io::Result<()> {
    serve_admin_from_env_with(default_addr, Router::new()).await
}

/// Like [`serve_admin_from_env`] but merges `extra` (e.g. a `GET /metrics`
/// route) alongside the pprof routes. Services that expose Prometheus metrics
/// call this with their `/metrics` router so the exporter shares the admin port.
pub async fn serve_admin_from_env_with(default_addr: &str, extra: Router) -> std::io::Result<()> {
    let raw =
        std::env::var("CRABKA_ADMIN_LISTEN_ADDR").unwrap_or_else(|_| default_addr.to_string());
    let addr: SocketAddr = raw
        .parse()
        .unwrap_or_else(|e| panic!("invalid CRABKA_ADMIN_LISTEN_ADDR `{raw}`: {e}"));
    serve_admin(addr, extra).await
}

//! Integration tests for the `/metrics` endpoint and metric-increment helpers.
//!
//! These tests drive `crabka_grpc_gateway::metrics::router()` in-process via
//! `tower::ServiceExt::oneshot` (the same pattern used in `tests/forward_unit.rs`)
//! and assert against the global [`crabka_grpc_gateway::metrics::metrics()`]
//! singleton that is rendered by the `/metrics` route.
//!
//! ## Shared-global determinism
//! The `static OnceLock` is shared across all tests in the binary, which run
//! in parallel threads.  To keep increment assertions non-flaky:
//! - Presence-only assertions simply call `.contains("crabka_gateway_<name>")`.
//! - Increment assertions use a **unique label value** never recorded by any
//!   other code path (`"p8_unique_*"`).  The helper `count_for_label` scans
//!   the rendered text for that exact labeled series and parses its value, so
//!   even under parallel execution only THIS test can touch that delta.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_grpc_gateway::metrics::metrics;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Render the `/metrics` endpoint via an in-process oneshot request.
///
/// Returns the response status and the decoded body text.
async fn render_metrics() -> (StatusCode, String) {
    let app = crabka_grpc_gateway::metrics::router();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    (status, text)
}

/// Parse the integer value of a labeled counter series line in `OpenMetrics`
/// text output.  Returns `0` if the label is not yet present in the output
/// (the counter was never incremented and therefore never serialised).
///
/// Expected line shape (OpenMetrics):
/// ```text
/// crabka_gateway_sends_total{result="p8_unique_send"} 3
/// ```
fn count_for_label(text: &str, series: &str) -> u64 {
    for line in text.lines() {
        if line.starts_with(series) {
            // The value is the last whitespace-separated token on the line.
            if let Some(tok) = line.split_whitespace().last()
                && let Ok(n) = tok.parse::<u64>()
            {
                return n;
            }
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// GET `/metrics` returns 200 and a body that contains the expected metric
/// family names, proving the registry, encoder, and route are all wired up.
///
/// `prometheus-client` omits `Family` counters from the output until at least
/// one label series has been recorded.  We prime every family here with unique
/// `p8_router_*` labels before rendering so the assertion is exhaustive.
#[tokio::test]
async fn metrics_router_renders() {
    // Prime every family so the encoder emits all families in this binary run.
    metrics().record_send("p8_router_sends_probe");
    metrics().record_forward("p8_router_forward_probe");
    metrics().record_txn("p8_router_txn_probe");
    metrics().record_webhook_in("p8_router_wh_in_probe");
    metrics().record_webhook_out("p8_router_wh_out_probe");
    // Non-Family metrics (histograms, gauges, plain counters) appear
    // unconditionally once the OnceLock is initialised — no extra priming
    // needed.

    let (status, body) = render_metrics().await;

    let expected = [
        "crabka_gateway_sends",
        "crabka_gateway_produce_latency_seconds",
        "crabka_gateway_webhook_out",
        "crabka_gateway_webhook_in",
        "crabka_gateway_dead_letter",
    ];
    let missing: Vec<_> = expected
        .into_iter()
        .filter(|needle| !body.contains(needle))
        .collect();
    assert_eq!(status, StatusCode::OK, "metrics body:\n{body}");
    assert_eq!(missing, Vec::<&str>::new(), "metrics body:\n{body}");
}

/// `record_send` increments `crabka_gateway_sends_total` for the provided
/// result label.  Uses a unique label so parallel test runs cannot affect the
/// delta.
#[tokio::test]
async fn send_increments_sends_total() {
    const UNIQUE_LABEL: &str = r#"crabka_gateway_sends_total{result="p8_unique_send_xyz"}"#;

    // Snapshot the count BEFORE — may be 0 if the label was never recorded.
    let (_, before_text) = render_metrics().await;
    let before = count_for_label(&before_text, UNIQUE_LABEL);

    // Record exactly 3 times.
    metrics().record_send("p8_unique_send_xyz");
    metrics().record_send("p8_unique_send_xyz");
    metrics().record_send("p8_unique_send_xyz");

    // Re-render and confirm the delta.
    let (_, after_text) = render_metrics().await;
    let after = count_for_label(&after_text, UNIQUE_LABEL);

    assert_eq!(
        after,
        before + 3,
        "expected {UNIQUE_LABEL} to increase by 3 (before={before}, after={after})"
    );
}

/// Recording a webhook-out result and a dead-letter event causes those labeled
/// series to appear in the rendered output.  Presence assertions — no broker
/// needed.
#[tokio::test]
async fn webhook_out_and_dead_letter_present() {
    metrics().record_webhook_out("p8_unique_wh");
    metrics().record_dead_letter();

    let (_, body) = render_metrics().await;

    assert!(
        body.contains(r#"crabka_gateway_webhook_out_total{result="p8_unique_wh"}"#),
        "expected webhook and dead-letter metrics in body:\n{body}"
    );
    assert!(
        body.contains("crabka_gateway_dead_letter_total"),
        "expected webhook and dead-letter metrics in body:\n{body}"
    );
}

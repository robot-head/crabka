//! Standalone Crabka traces querier preloaded with a rich, multi-service demo
//! fixture, for pointing a real Grafana Tempo datasource at Crabka and running
//! complex `TraceQL` queries by hand (or via a headless browser).
//!
//! Serves the Tempo query API on `0.0.0.0:3201` (override with `CRABKA_DEMO_ADDR`).
//!
//! Run: `cargo run -p crabka-traces --example grafana_demo`
//!
//! The fixture is timestamped at "a few minutes ago" so it falls inside
//! Grafana's default "last 1 hour" window. Showcased queries (all valid against
//! the engine):
//!   * `{ resource.service.name = "checkout-frontend" }`   (root-service selector)
//!   * `{ span:status = error }`                            (error spans)
//!   * `{ span:duration > 100ms }`                          (slow spans)
//!   * `{ .http.method = "GET" } >> { .db.system = "postgresql" }`  (structural)
//!   * `{ .http.method = "POST" && span:status = error }`  (boolean span filter)
//!   * `{ resource.service.name = "checkout-frontend" } | rate()`  (metrics)

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::Request,
    middleware::{self, Next},
    response::Response as AxumResponse,
};
use crabka_traceql::{AttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine};
use crabka_traces::querier::http::router;
use crabka_units::{Time, convert::TimeExt as _};

const TENANT: &str = "anonymous";
// OTLP span kinds.
const SERVER: i32 = 2;
const CLIENT: i32 = 3;
// OTLP status codes.
const OK: i32 = 1;
const ERROR: i32 = 2;

fn base_ns() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    i64::try_from(now).unwrap_or(i64::MAX) - 180_000_000_000 // 3 minutes ago
}

fn span(
    base: i64,
    identity: (u8, u8, Option<u8>),
    name: &str,
    kind: i32,
    timing_ms: (i64, i64),
    status: (i32, &str),
    attrs: Vec<(&str, AttrValue)>,
) -> InputSpan {
    let (trace_id, id, parent) = identity;
    let (start_offset_ms, duration_ms) = timing_ms;
    let (status, status_message) = status;
    InputSpan {
        trace_id: [trace_id; 16],
        span_id: [id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.to_string(),
        kind,
        start_unix_nano: base + start_offset_ms * 1_000_000,
        duration: Time::from_millis(duration_ms),
        status_code: status,
        status_message: status_message.to_string(),
        instrumentation_name: "crabka-demo".to_string(),
        instrumentation_version: "1.0.0".to_string(),
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

fn str_attr(value: &str) -> AttrValue {
    AttrValue::Str(value.to_string())
}

async fn log_req(req: Request, next: Next) -> AxumResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let accept = req
        .headers()
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let resp = next.run(req).await;
    println!("{method} {uri} accept=[{accept}] -> {}", resp.status());
    resp
}

fn demo_store() -> InMemorySpanStore {
    let base = base_ns();
    let mut store = InMemorySpanStore::new();

    // Trace 1 — checkout flow with a failed payment leg (5 spans, 3 services).
    store.push_trace(
        TENANT,
        "checkout-frontend",
        "GET /checkout",
        vec![
            span(
                base,
                (0x11, 0x02, None),
                "GET /checkout",
                SERVER,
                (0, 120),
                (OK, ""),
                vec![
                    ("http.method", str_attr("GET")),
                    ("http.target", str_attr("/checkout")),
                    ("http.status_code", AttrValue::Int(200)),
                ],
            ),
            span(
                base,
                (0x11, 0x03, Some(0x02)),
                "cart.rpc",
                CLIENT,
                (5, 45),
                (OK, ""),
                vec![
                    ("rpc.system", str_attr("grpc")),
                    ("peer.service", str_attr("cart-backend")),
                ],
            ),
            span(
                base,
                (0x11, 0x04, Some(0x03)),
                "cart.lookup",
                SERVER,
                (10, 30),
                (OK, ""),
                vec![
                    ("db.system", str_attr("postgresql")),
                    ("db.statement", str_attr("SELECT * FROM cart WHERE user=$1")),
                ],
            ),
            span(
                base,
                (0x11, 0x05, Some(0x02)),
                "POST /charge",
                CLIENT,
                (55, 70),
                (ERROR, "payment declined"),
                vec![
                    ("http.method", str_attr("POST")),
                    ("http.status_code", AttrValue::Int(402)),
                    ("peer.service", str_attr("payment-svc")),
                ],
            ),
            span(
                base,
                (0x11, 0x06, Some(0x05)),
                "payment.charge",
                SERVER,
                (60, 55),
                (ERROR, "card_declined"),
                vec![
                    ("db.system", str_attr("mysql")),
                    ("payment.provider", str_attr("stripe")),
                ],
            ),
        ],
    );

    // Trace 2 — product browse (2 spans).
    store.push_trace(
        TENANT,
        "web-frontend",
        "GET /products",
        vec![
            span(
                base,
                (0x22, 0x02, None),
                "GET /products",
                SERVER,
                (0, 80),
                (OK, ""),
                vec![
                    ("http.method", str_attr("GET")),
                    ("http.status_code", AttrValue::Int(200)),
                ],
            ),
            span(
                base,
                (0x22, 0x03, Some(0x02)),
                "inventory.list",
                SERVER,
                (8, 25),
                (OK, ""),
                vec![("db.system", str_attr("postgresql"))],
            ),
        ],
    );

    // Trace 3 — fast checkout (2 spans, also checkout-frontend rooted).
    store.push_trace(
        TENANT,
        "checkout-frontend",
        "GET /checkout",
        vec![
            span(
                base,
                (0x33, 0x02, None),
                "GET /checkout",
                SERVER,
                (0, 35),
                (OK, ""),
                vec![
                    ("http.method", str_attr("GET")),
                    ("http.status_code", AttrValue::Int(200)),
                ],
            ),
            span(
                base,
                (0x33, 0x03, Some(0x02)),
                "cart.lookup",
                SERVER,
                (4, 15),
                (OK, ""),
                vec![("db.system", str_attr("postgresql"))],
            ),
        ],
    );

    // Trace 4 — slow analytics report (2 spans, both > 100ms).
    store.push_trace(
        TENANT,
        "reporting-svc",
        "GET /report",
        vec![
            span(
                base,
                (0x44, 0x02, None),
                "GET /report",
                SERVER,
                (0, 450),
                (OK, ""),
                vec![
                    ("http.method", str_attr("GET")),
                    ("http.status_code", AttrValue::Int(200)),
                ],
            ),
            span(
                base,
                (0x44, 0x03, Some(0x02)),
                "analytics.aggregate",
                SERVER,
                (20, 400),
                (OK, ""),
                vec![("db.system", str_attr("clickhouse"))],
            ),
        ],
    );

    // Trace 5 — failed login (1 span, POST + error).
    store.push_trace(
        TENANT,
        "auth-svc",
        "POST /login",
        vec![span(
            base,
            (0x55, 0x02, None),
            "POST /login",
            SERVER,
            (0, 60),
            (ERROR, "invalid credentials"),
            vec![
                ("http.method", str_attr("POST")),
                ("http.status_code", AttrValue::Int(401)),
            ],
        )],
    );

    store
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("CRABKA_DEMO_ADDR").unwrap_or_else(|_| "0.0.0.0:3201".to_string());
    let engine = Arc::new(TraceqlEngine::new(
        Arc::new(demo_store()),
        EngineOpts::default(),
    ));
    let app = router(engine).layer(middleware::from_fn(log_req));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("crabka traces demo querier listening on http://{addr}");
    println!("tenant: {TENANT} (no X-Scope-OrgID header required)");
    println!(
        "trace ids: 11..(checkout+error)  22..(browse)  33..(fast checkout)  44..(slow report)  55..(failed login)"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

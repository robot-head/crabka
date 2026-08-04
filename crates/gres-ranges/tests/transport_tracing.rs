//! Trace propagation across the range RPC hop, over a real mTLS connection.
//!
//! This is the headline behaviour of gres tracing: a statement on one node
//! produces spans on the node that owns the range, and both belong to one
//! trace. Everything here is asserted from exported [`SpanData`], because
//! `tracing-opentelemetry` resolves a span's parent and trace id when the span
//! *closes* — a live `tracing::Span` handle reports a tree that does not match
//! what an operator will see.
//!
//! Three details of the harness are load-bearing:
//!
//! - **Install the propagator.** Without
//!   `set_text_map_propagator(TraceContextPropagator::new())`,
//!   `TraceCarrier::apply_to` silently does nothing and every assertion below
//!   passes vacuously with two unrelated traces. `crabka_telemetry::init`
//!   installs it in production; a test has to do it itself.
//! - **Install with `set_global_default`, not `with_default`.** The server half
//!   runs on a task spawned by the TLS accept loop, where a thread-local
//!   subscriber is invisible.
//! - **Wait for the server span to close.** It closes on the serving task, not
//!   on the caller's, so reading the exporter straight after the RPC returns
//!   races it.
//!
//! Each test installs its own global subscriber, which relies on the repository
//! convention of running tests under `cargo nextest` (one process per test).

use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use assert2::{assert, check};
use async_trait::async_trait;
use crabka_gres_ranges::{
    FramedTcpClient, RangeId, RangeRequest, RangeResponse, RangeService, RangeTlsClientConfig,
    RangeTlsServerConfig, TxnReq, TxnResp, serve_tls, telemetry::ROUTE_TARGET,
};
use opentelemetry::trace::{SpanKind, Status, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tracing_subscriber::{EnvFilter, Layer as _, layer::SubscriberExt as _};

/// The principal in the client certificate fixture, and the only one the
/// server config below authorizes.
const CLIENT_PRINCIPAL: &str = "CN=test-client,OU=integration,O=crabka";

/// Rendered in place of an attribute the span never recorded, so a missing
/// attribute fails a whole-map comparison with a readable diff instead of
/// silently matching nothing.
const UNSET: &str = "<unset>";

struct Traces {
    provider: SdkTracerProvider,
    exporter: InMemorySpanExporter,
}

impl Traces {
    fn install() -> Self {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("transport-tracing"))
            .with_filter(EnvFilter::new("crabka_gres_ranges::route=trace"));
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
            .expect("install global subscriber; run these tests under cargo nextest");
        Self { provider, exporter }
    }

    fn finished(&self) -> Vec<SpanData> {
        self.provider.force_flush().expect("flush exporter");
        self.exporter.get_finished_spans().expect("finished spans")
    }

    /// Poll until both halves of the hop have closed.
    ///
    /// The server span closes on the serving task; without this the exporter is
    /// read while that task is still unwinding and the test flakes.
    async fn hop(&self) -> Hop {
        for _ in 0..500 {
            let spans = self.finished();
            let client = of_kind(&spans, &SpanKind::Client).len();
            let server = of_kind(&spans, &SpanKind::Server).len();
            if client >= 1 && server >= 1 {
                return Hop { spans };
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("client and server RPC spans never both closed");
    }
}

/// One closed client span and one closed server span, and nothing else claimed
/// about the rest of the trace.
struct Hop {
    spans: Vec<SpanData>,
}

impl Hop {
    fn client(&self) -> &SpanData {
        only(&of_kind(&self.spans, &SpanKind::Client), "client")
    }

    fn server(&self) -> &SpanData {
        only(&of_kind(&self.spans, &SpanKind::Server), "server")
    }
}

/// Range RPC spans of one `otel.kind`.
///
/// Looked up by kind rather than by name because `otel.name` renames the
/// exported span to the request's variant (`"Txn"`), which is what an operator
/// sees in the waterfall — nothing downstream may search for a span literally
/// named `gres.range_rpc`.
fn of_kind<'a>(spans: &'a [SpanData], kind: &SpanKind) -> Vec<&'a SpanData> {
    spans
        .iter()
        .filter(|span| {
            span.span_kind == *kind
                && attribute(span, "rpc.system").is_some_and(|value| value == "crabka.range")
        })
        .collect()
}

fn only<'a>(spans: &[&'a SpanData], what: &str) -> &'a SpanData {
    assert!(spans.len() == 1, "expected exactly one {what} RPC span");
    spans[0]
}

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| attribute.value.to_string())
}

/// Compare exactly the attributes `pairs` names, as one map.
fn check_attributes(span: &SpanData, pairs: &[(&str, &str)]) {
    let actual = pairs
        .iter()
        .map(|(key, _)| {
            (
                (*key).to_owned(),
                attribute(span, key).unwrap_or_else(|| UNSET.to_owned()),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let expected = pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    check!(actual == expected);
}

/// The value of a numeric attribute, insisting it exported as an OTLP integer.
///
/// OTLP has no unsigned integer type, so a `u64`/`usize` span field is
/// stringified — and Tempo cannot compare, sort or range-filter a string, so
/// `pg.request_bytes > 100` silently matches nothing. Asserting the attribute
/// exists does not catch that; asserting its variant does.
fn integer_attribute(span: &SpanData, key: &str) -> i64 {
    match span
        .attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
    {
        Some(opentelemetry::Value::I64(value)) => *value,
        other => panic!("{key} must export as an OTLP integer, got {other:?}"),
    }
}

struct MtlsFixture {
    _dir: tempfile::TempDir,
    server: RangeTlsServerConfig,
    client: RangeTlsClientConfig,
}

impl MtlsFixture {
    /// `authorized` is the principal set the server accepts for range RPC. It
    /// must be non-empty — a tenant with no authorized principal fails to build
    /// an acceptor at all — so rejecting the fixture client means naming
    /// somebody else.
    fn new(tenant: &str, authorized: BTreeSet<String>) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = tempfile::tempdir().expect("temporary certificate directory");
        let server_cert = write_fixture(&dir, "server-cert.pem", "dev_cert.pem");
        let server_key = write_fixture(&dir, "server-key.pem", "dev_key.pem");
        let client_ca = write_fixture(&dir, "client-ca.pem", "dev_client_ca.pem");
        let client_cert = write_fixture(&dir, "client-cert.pem", "dev_client_cert.pem");
        let client_key = write_fixture(&dir, "client-key.pem", "dev_client_key.pem");
        Self {
            _dir: dir,
            server: RangeTlsServerConfig {
                tenant: tenant.to_owned(),
                tls: crabka_security::TlsConfig {
                    cert_chain_path: server_cert.clone(),
                    private_key_path: server_key,
                    trust_roots_path: Some(server_cert.clone()),
                    client_ca_path: Some(client_ca),
                    client_auth: crabka_security::ClientAuthMode::Required,
                },
                range_rpc_principals: authorized,
                operator_control_principals: BTreeSet::from([CLIENT_PRINCIPAL.to_owned()]),
            },
            client: RangeTlsClientConfig {
                tls: crabka_security::TlsConfig {
                    cert_chain_path: client_cert,
                    private_key_path: client_key,
                    trust_roots_path: Some(server_cert),
                    client_ca_path: None,
                    client_auth: crabka_security::ClientAuthMode::Disabled,
                },
                server_name: "crabka-dev".to_owned(),
            },
        }
    }

    fn authorized(tenant: &str) -> Self {
        Self::new(tenant, BTreeSet::from([CLIENT_PRINCIPAL.to_owned()]))
    }
}

fn write_fixture(dir: &tempfile::TempDir, name: &str, fixture: &str) -> PathBuf {
    let path = dir.path().join(name);
    let contents: &[u8] = match fixture {
        "dev_cert.pem" => include_bytes!("../../security/tests/fixtures/dev_cert.pem"),
        "dev_key.pem" => include_bytes!("../../security/tests/fixtures/dev_key.pem"),
        "dev_client_ca.pem" => include_bytes!("../../security/tests/fixtures/dev_client_ca.pem"),
        "dev_client_cert.pem" => {
            include_bytes!("../../security/tests/fixtures/dev_client_cert.pem")
        }
        "dev_client_key.pem" => include_bytes!("../../security/tests/fixtures/dev_client_key.pem"),
        _ => unreachable!("fixture name is fixed by this test"),
    };
    std::fs::write(&path, contents).expect("write certificate fixture");
    path
}

async fn spawn_tls(service: Arc<dyn RangeService>, config: RangeTlsServerConfig) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let address = listener.local_addr().expect("TLS listener address");
    tokio::spawn(async move {
        let _ = serve_tls(listener, service, config).await;
    });
    address.to_string()
}

/// Answers every request with a barrier response, so the hop under test is the
/// transport and nothing else.
struct BarrierService;

#[async_trait]
impl RangeService for BarrierService {
    async fn handle(&self, _request: RangeRequest) -> RangeResponse {
        RangeResponse::Txn(TxnResp::Barrier {
            substrate_offset: 42,
        })
    }
}

fn barrier_request() -> RangeRequest {
    RangeRequest::Txn(TxnReq::Barrier {
        range_id: RangeId::new(3),
    })
}

/// The whole point of the feature: the server's span is a child of the client's
/// across a real TLS connection, in one trace, with the parent marked remote.
#[tokio::test]
async fn range_serve_span_is_a_remote_child_of_the_calling_range_rpc_span() {
    let traces = Traces::install();
    let fixture = MtlsFixture::authorized("tenant_trace_hop");
    let endpoint = spawn_tls(Arc::new(BarrierService), fixture.server).await;
    let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS range client");

    let response = {
        let statement = tracing::debug_span!(target: ROUTE_TARGET, "test.statement");
        let _guard = statement.enter();
        client
            .call(&endpoint, &barrier_request())
            .await
            .expect("traced range RPC")
    };
    assert!(
        response
            == RangeResponse::Txn(TxnResp::Barrier {
                substrate_offset: 42
            })
    );

    let hop = traces.hop().await;
    let (rpc, serve) = (hop.client(), hop.server());

    assert!(serve.parent_span_id == rpc.span_context.span_id());
    assert!(serve.span_context.trace_id() == rpc.span_context.trace_id());
    // Without this the span is a child by id only: the exporter would not flag
    // it as a process boundary, and a backend would render one flat service.
    assert!(serve.parent_span_is_remote);
    // The client span itself hangs off the caller's statement, so the whole
    // chain is one tree rather than two roots that happen to share an id.
    let statement = hop
        .spans
        .iter()
        .find(|span| span.name == "test.statement")
        .expect("caller statement span");
    assert!(rpc.parent_span_id == statement.span_context.span_id());
}

/// Both halves carry the attributes an operator filters and sorts on. Asserted
/// as whole maps so a renamed or dropped attribute fails with a diff.
#[tokio::test]
async fn both_halves_of_the_hop_record_their_rpc_attributes() {
    let traces = Traces::install();
    let fixture = MtlsFixture::authorized("tenant_trace_attributes");
    let endpoint = spawn_tls(Arc::new(BarrierService), fixture.server).await;
    let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS range client");

    client
        .call(&endpoint, &barrier_request())
        .await
        .expect("range RPC");

    let hop = traces.hop().await;
    let (rpc, serve) = (hop.client(), hop.server());

    // `otel.name` renames the exported span to the request variant, which is
    // what a waterfall shows.
    check!(rpc.name == "Txn");
    check!(serve.name == "Txn");
    check_attributes(
        rpc,
        &[
            ("rpc.system", "crabka.range"),
            ("rpc.method", "Txn"),
            ("server.address", endpoint.as_str()),
            ("pg.range_id", "3"),
            // The first call to a fresh endpoint dials and handshakes; there is
            // nothing in the pool yet.
            ("pg.pooled_connection", "false"),
        ],
    );
    check_attributes(
        serve,
        &[
            ("rpc.system", "crabka.range"),
            ("rpc.method", "Txn"),
            ("pg.principal", CLIENT_PRINCIPAL),
            ("pg.tenant", "tenant_trace_attributes"),
            ("pg.range_id", "3"),
        ],
    );

    // Byte counters must export as OTLP integers, or `pg.request_bytes > 4096`
    // matches nothing in Tempo. Both halves saw a non-empty frame.
    check!(integer_attribute(rpc, "pg.range_id") == 3);
    check!(integer_attribute(rpc, "pg.request_bytes") > 0);
    check!(integer_attribute(rpc, "pg.response_bytes") > 0);
    check!(integer_attribute(serve, "pg.response_bytes") > 0);

    // A successful RPC leaves the OTel status Unset on both sides: "OK" is a
    // claim the transport deliberately never makes.
    check!(rpc.status == Status::Unset);
    check!(serve.status == Status::Unset);
}

/// The second call to the same endpoint reuses the parked connection, and says
/// so — the usual explanation for an outlier RPC is the handshake the first one
/// paid.
#[tokio::test]
async fn a_reused_connection_is_recorded_as_pooled() {
    let traces = Traces::install();
    let fixture = MtlsFixture::authorized("tenant_trace_pool");
    let endpoint = spawn_tls(Arc::new(BarrierService), fixture.server).await;
    let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS range client");

    client
        .call(&endpoint, &barrier_request())
        .await
        .expect("first range RPC");
    client
        .call(&endpoint, &barrier_request())
        .await
        .expect("second range RPC");

    // Both client spans have closed by the time the second call returns.
    let spans = traces.finished();
    let mut pooled = of_kind(&spans, &SpanKind::Client)
        .iter()
        .map(|span| attribute(span, "pg.pooled_connection").unwrap_or_else(|| UNSET.to_owned()))
        .collect::<Vec<_>>();
    pooled.sort();
    check!(pooled == vec!["false".to_owned(), "true".to_owned()]);
}

/// With no caller span, the trace starts at the client span rather than at a
/// fabricated parent — and the hop still stitches, because `gres.range_rpc` is
/// created unconditionally and is what the carrier picks up.
#[tokio::test]
async fn an_rpc_with_no_caller_span_roots_the_trace_at_the_client_span() {
    let traces = Traces::install();
    let fixture = MtlsFixture::authorized("tenant_trace_untraced");
    let endpoint = spawn_tls(Arc::new(BarrierService), fixture.server).await;
    let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS range client");

    // Deliberately no enclosing span, so `TraceCarrier::capture_current` has
    // nothing to capture.
    let response = client
        .call(&endpoint, &barrier_request())
        .await
        .expect("untraced range RPC");
    assert!(
        response
            == RangeResponse::Txn(TxnResp::Barrier {
                substrate_offset: 42
            })
    );

    let hop = traces.hop().await;
    let (rpc, serve) = (hop.client(), hop.server());

    check!(rpc.parent_span_id == opentelemetry::trace::SpanId::INVALID);
    check!(serve.parent_span_id == rpc.span_context.span_id());
    check!(serve.span_context.trace_id() == rpc.span_context.trace_id());
    check!(serve.parent_span_is_remote);
}

/// A peer the tenant does not authorize is rejected before the service sees the
/// request, and the rejection lands on the server span — which is the only
/// place an operator can see it, since the client only learns the connection
/// closed.
#[tokio::test]
async fn an_unauthorized_peer_records_the_rejection_on_the_server_span() {
    let traces = Traces::install();
    let fixture = MtlsFixture::new(
        "tenant_trace_denied",
        BTreeSet::from(["CN=someone-else,OU=integration,O=crabka".to_owned()]),
    );
    let endpoint = spawn_tls(Arc::new(BarrierService), fixture.server).await;
    let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS range client");

    let error = client
        .call(&endpoint, &barrier_request())
        .await
        .expect_err("unauthorized peer is rejected");
    assert!(matches!(
        error,
        crabka_gres_ranges::TransportError::Io(_) | crabka_gres_ranges::TransportError::Json(_)
    ));

    let hop = traces.hop().await;
    let serve = hop.server();
    check_attributes(serve, &[("error.type", "unauthorized_peer")]);
    // Pinning the description is what catches the `otel.status_message`
    // misspelling, which exports an empty one.
    assert!(let Status::Error { description } = &serve.status);
    check!(description.contains("tenant_trace_denied"));
}

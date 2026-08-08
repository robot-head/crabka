//! Layer 5 of the gres distributed-tracing verification: a client's W3C trace
//! context survives across real operating-system processes.
//!
//! Every other tracing test in the workspace runs in one process. There a span
//! handle can move from producer to consumer as a clone, and a propagation bug
//! stays invisible. `TraceCarrier::apply_to` quietly does nothing without an
//! installed propagator, and an in-process test would still see the right
//! context, because the span never really left. Here the context must survive
//! a longer path. The client writes it into a sqlcommenter tag. One
//! `crabka-gres` process parses it and serialises it into a `RangeEnvelope` on
//! the mTLS range RPC. A *different* `crabka-gres` process reconstitutes it.
//! Both processes then export it over OTLP to the collector that this test
//! runs.
//!
//! The topology is two nodes over two ranges. Range 1, table id `1000000`,
//! lives on node 1. A statement issued at node 0's gateway against `t1000000`
//! must therefore cross the process boundary to get an answer, and that is the
//! whole point. The setup exercises the write path. The traced statement is a
//! read, because a read gives the smallest span tree that still spans two
//! processes, and therefore the sharpest assertion.
//!
//! The test skips when the binaries it launches are not built. It does not
//! fail there. This matches the rest of the harness.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use assert2::{assert, check};
use crabka_gres_control::RegistryPolicy;
use crabka_gres_loadtest::{
    cluster::{Binaries, Cluster, ClusterOptions, SqlEndpoint},
    config::LoadtestRuntimePolicy,
    scenario::{ModeSpec, TopologySpec},
};
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use support::collector::{FlatSpan, OtlpCollector, RANGE_RPC_SYSTEM};

/// The trace that the client claims its statement belongs to. The value is
/// fixed, so the assertions pin *which* trace came back, and not only that
/// some trace did. A test that checked only "the ids all match each other"
/// would pass against a gres that ignored the client and rooted its own
/// trace.
const CLIENT_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
/// The client's own span id, which the statement span must name as its parent.
const CLIENT_SPAN_ID: &str = "00f067aa0ba902b7";

/// The harness's label for the gateway node the client connects to, which is
/// also its `service.instance.id`. It hosts range 0.
const GATEWAY_NODE: &str = "node0";
/// The harness's label for the node hosting range 1, where [`REMOTE_TABLE`]
/// lives. The traced statement cannot be answered without it.
const OWNER_NODE: &str = "node1";

/// The `tracing` target attribute every exported span carries.
const TARGET: &str = "target";
/// The target the pgwire session and statement spans are emitted on.
const SESSION_TARGET: &str = "crabka_pgwire::session";

/// The sharded table whose range is *not* hosted by the gateway node.
const REMOTE_TABLE: &str = "t1000000";
/// The table on the gateway node's own range, created so the schema exercises
/// both ranges and the traced read has a local counterpart that is not used.
const LOCAL_TABLE: &str = "t0";

/// How long to wait for the export of the traced statement's spans. The value
/// is generous, because two batch exporters must fire and the host may be
/// loaded.
const COLLECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
/// Polling interval while waiting for spans.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);
/// A window of silence that must pass before the traced statement runs, so a
/// late batch from the setup traffic cannot be mistaken for its spans.
const QUIET_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);
/// Upper bound on how long to wait for that silence.
const QUIET_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_trace_context_survives_across_gres_processes() {
    let binaries = match Binaries::resolve() {
        Ok(binaries) => binaries,
        Err(error) => {
            eprintln!(
                "skipping cross-process tracing test: {error:#}\n\
                 build them with `cargo build -p crabka-gres -p crabka-broker -p crabka-cli`"
            );
            return;
        }
    };

    let collector = OtlpCollector::start()
        .await
        .expect("start in-test OTLP collector");
    let work_dir = tempfile::tempdir().expect("work dir");
    let cluster = Cluster::launch(ClusterOptions {
        topology: TopologySpec {
            nodes: 2,
            ranges: 2,
            clock_skew: BTreeMap::new(),
            cpus_per_node: None,
            broker_cpus: None,
        },
        mode: ModeSpec::LogicalTso,
        work_dir: work_dir.path().to_path_buf(),
        binaries,
        registry_policy: RegistryPolicy::default(),
        runtime_policy: LoadtestRuntimePolicy::default(),
        node_env: tracing_env(collector.endpoint()),
    })
    .await
    .expect("launch cluster");

    let received = run_traced_statement(&cluster, &collector).await;
    cluster.shutdown().await.expect("shutdown cluster");

    assert_ingress_context_reached_the_statement(&received);
    assert_trace_crossed_processes(&received);
}

/// Environment every spawned node gets.
///
/// `CRABKA_OTLP_SAMPLE_RATIO=1.0` covers both samplers that could otherwise
/// drop the trace. Those are the SDK's `ParentBased(TraceIdRatioBased)` head
/// sampler, and the ingress `Resample` policy that recomputes the client's
/// sampled flag locally. At 1.0 both keep everything, so a missing span means
/// a missing span and not an unlucky trace id.
fn tracing_env(endpoint: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("CRABKA_OTLP_ENDPOINT".to_owned(), endpoint.to_owned()),
        ("CRABKA_OTLP_PROTOCOL".to_owned(), "grpc".to_owned()),
        ("CRABKA_OTLP_SAMPLE_RATIO".to_owned(), "1.0".to_owned()),
        // Shorten the batch exporter's flush cadence from its 5s default so the
        // test waits seconds rather than tens of seconds.
        ("OTEL_BSP_SCHEDULE_DELAY".to_owned(), "500".to_owned()),
    ])
}

/// Creates the schema, quiesces the collector, then runs exactly one
/// sqlcommenter-tagged statement and returns every span exported afterwards.
///
/// The connection that carries the traced statement is deliberately still open
/// when the collector gathers the spans. `gres.session` closes only when the
/// client disconnects, and it exports at that point, as the root of a trace of
/// its own.
async fn run_traced_statement(cluster: &Cluster, collector: &OtlpCollector) -> Vec<FlatSpan> {
    let gateway = cluster.sql_endpoint(0);
    let (setup, setup_driver) = connect(&gateway).await;
    for statement in [
        &format!("CREATE TABLE {LOCAL_TABLE} (id int4)"),
        &format!("CREATE TABLE {REMOTE_TABLE} (id int4)"),
        &format!("INSERT INTO {REMOTE_TABLE} VALUES (7)"),
    ] {
        setup
            .simple_query(statement)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    drop(setup);
    let _ = setup_driver.await;
    wait_for_quiet(collector).await;

    let (client, driver) = connect(&gateway).await;
    let tagged = format!(
        "SELECT id FROM {REMOTE_TABLE} /*traceparent='00-{CLIENT_TRACE_ID}-{CLIENT_SPAN_ID}-01'*/"
    );
    let rows = client
        .simple_query(&tagged)
        .await
        .unwrap_or_else(|error| panic!("{tagged}: {error}"));
    // The tag must not have disturbed the statement: it sits in a comment the
    // lexer skips, so the row it selects still comes back.
    check!(
        rows.iter()
            .filter(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()
            == 1
    );

    let received = collect_until_complete(collector).await;
    drop(client);
    let _ = driver.await;
    received
}

/// Drains the collector until nothing new arrives for [`QUIET_WINDOW`], so the
/// spans gathered afterwards belong to the traced statement alone.
async fn wait_for_quiet(collector: &OtlpCollector) {
    let deadline = tokio::time::Instant::now() + QUIET_TIMEOUT;
    loop {
        let _ = collector.drain();
        tokio::time::sleep(QUIET_WINDOW).await;
        if collector.drain().is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster never went quiet: spans kept arriving with no client traffic"
        );
    }
}

/// Accumulates exported spans until the traced statement's tree has arrived
/// from both processes, or [`COLLECT_TIMEOUT`] elapses.
///
/// A return on a timeout, rather than a panic, is deliberate. The assertions
/// that follow describe exactly what was missing, which is much more useful
/// than "timed out".
async fn collect_until_complete(collector: &OtlpCollector) -> Vec<FlatSpan> {
    let deadline = tokio::time::Instant::now() + COLLECT_TIMEOUT;
    let mut received = Vec::new();
    loop {
        received.extend(collector.drain());
        let traced = traced_spans(&received);
        let instances: BTreeSet<&str> = traced.iter().map(|span| span.instance.as_str()).collect();
        let served = traced
            .iter()
            .any(|span| span.is_range_rpc(SpanKind::Server));
        if served && instances.len() >= 2 {
            return received;
        }
        if tokio::time::Instant::now() >= deadline {
            return received;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The spans belonging to the trace the client named.
fn traced_spans(received: &[FlatSpan]) -> Vec<&FlatSpan> {
    received
        .iter()
        .filter(|span| span.trace_id == CLIENT_TRACE_ID)
        .collect()
}

/// The ingress claim. The statement that gres ran is a child of the span the
/// client named in its sqlcommenter tag, inside the client's trace. It is not
/// the root of a trace that gres invented for itself.
fn assert_ingress_context_reached_the_statement(received: &[FlatSpan]) {
    // Every session-tier span exported in this window belongs to the client's
    // trace. The setup traffic was drained and the traced connection is still
    // open, so a second trace id on this target would mean a statement escaped
    // the ingress context and rooted a trace of its own.
    //
    // The check is scoped to the target rather than applied to everything
    // received because gres also emits root traces that are genuinely not part
    // of any statement — the substrate's background WAL append and journal
    // commit, and the TSO client's batched grant, all of which run on their own
    // tasks and would be wrong to attribute to whichever statement happened to
    // be in flight.
    let escaped: BTreeSet<&str> = received
        .iter()
        .filter(|span| span.has_attribute(TARGET, SESSION_TARGET))
        .map(|span| span.trace_id.as_str())
        .filter(|trace_id| *trace_id != CLIENT_TRACE_ID)
        .collect();
    assert!(
        escaped.is_empty(),
        "statement spans outside the client's trace: {escaped:?}"
    );

    let traced = traced_spans(received);
    // `pg.protocol` is unique to the pgwire statement span, which is the one
    // the ingress context is attached to.
    let statement = traced
        .iter()
        .find(|span| span.has_attribute("pg.protocol", "simple"))
        .expect("a simple-protocol gres.statement span in the client's trace");
    assert!(statement.parent_span_id == CLIENT_SPAN_ID);
    assert!(statement.has_attribute("db.system.name", "postgresql"));
    assert!(statement.kind == SpanKind::Server);
}

/// The cross-process claim. A served range RPC on one node is the child of the
/// client-side range RPC span that a *different* node emitted.
fn assert_trace_crossed_processes(received: &[FlatSpan]) {
    let traced = traced_spans(received);
    assert!(!traced.is_empty(), "the client's trace was never exported");
    // Both processes, named — not merely "more than one", which a duplicated
    // resource attribute would also satisfy.
    let instances: BTreeSet<&str> = traced.iter().map(|span| span.instance.as_str()).collect();
    assert!(instances == BTreeSet::from([GATEWAY_NODE, OWNER_NODE]));

    let serve = traced
        .iter()
        .find(|span| span.is_range_rpc(SpanKind::Server))
        .expect("a gres.range_serve span in the client's trace");
    let rpc = traced
        .iter()
        .find(|span| span.span_id == serve.parent_span_id)
        .expect("the gres.range_rpc span that issued the served RPC");

    assert!(rpc.is_range_rpc(SpanKind::Client));
    // The direction is pinned: the gateway issued it, the range owner served
    // it. `!=` alone would also pass if the two were swapped.
    assert!(rpc.instance == GATEWAY_NODE);
    assert!(serve.instance == OWNER_NODE);
    assert!(rpc.attribute("rpc.method") == serve.attribute("rpc.method"));
    assert!(rpc.has_attribute("rpc.system", RANGE_RPC_SYSTEM));

    // `otel.name` renames the exported span, so neither end arrives under the
    // name it is declared with; both carry their RPC method instead. Pinned
    // here because it is the trap that breaks every downstream query written
    // against the declared name.
    assert!(serve.name != "gres.range_serve");
    assert!(rpc.name != "gres.range_rpc");
    assert!(Some(serve.name.as_str()) == serve.attribute("rpc.method"));
    assert!(Some(rpc.name.as_str()) == rpc.attribute("rpc.method"));
    // A routed SELECT travels as the `Sql` range request, so that — not
    // `gres.range_serve` — is the name a trace query must search for.
    assert!(serve.name == "Sql");
}

/// Connects to a node's SQL front door. It returns the client and the driver
/// task, which the caller must keep alive for the lifetime of the
/// connection.
async fn connect(endpoint: &SqlEndpoint) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, connection) = tokio_postgres::Config::new()
        .host(endpoint.addr.ip().to_string())
        .port(endpoint.addr.port())
        .user(&endpoint.user)
        .password(&endpoint.password)
        .dbname(&endpoint.database)
        .connect(tokio_postgres::NoTls)
        .await
        .expect("connect to sql endpoint");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, driver)
}

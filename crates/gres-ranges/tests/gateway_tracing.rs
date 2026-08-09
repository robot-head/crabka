//! Trace assertions for the multi-range gateway.
//!
//! Two things about the harness are load-bearing, and each one costs hours to
//! find again:
//!
//! - **Assert on exported [`SpanData`], never on a live `tracing::Span`.**
//!   `tracing-opentelemetry` resolves a span's parent and trace id when the
//!   span *closes*, so a live handle reports a tree that does not match the
//!   exported tree.
//! - **Install with `set_global_default`, not `with_default`.** The gateway
//!   moves work onto `spawn_blocking` threads and `std::thread::scope` threads,
//!   where a thread-local subscriber is invisible. A test that uses
//!   `with_default` passes with zero spans collected.
//!
//! Each test installs its own global subscriber, which relies on the repository
//! convention of running tests under `cargo nextest` (one process per test).

use std::collections::BTreeMap;

use assert2::{assert, check};
use crabka_gres_ranges::{
    GatewayCommitFault, HashShardSpec, MultiRangeTenant, MultiRangeTenantConfig, RangeId, TableId,
    TenantName,
};
use crabka_pgwire::engine::{Engine, Session};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tracing_subscriber::{EnvFilter, Layer as _, layer::SubscriberExt as _};

/// The test renders this in place of an attribute the span never recorded. A
/// missing attribute then fails the whole-map comparison with a readable diff,
/// and does not silently match nothing.
const UNSET: &str = "<unset>";

struct Traces {
    provider: SdkTracerProvider,
    exporter: InMemorySpanExporter,
}

impl Traces {
    fn install() -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        // `=trace` so the `pg.route` spans, which sit a level below the rest of
        // the gateway tier, are collected too.
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("gateway-tracing"))
            .with_filter(EnvFilter::new("crabka_gres_ranges::route=trace"));
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
            .expect("install global subscriber; run these tests under cargo nextest");
        Self { provider, exporter }
    }

    fn finished(&self) -> Vec<SpanData> {
        self.provider.force_flush().expect("flush exporter");
        self.exporter.get_finished_spans().expect("finished spans")
    }
}

fn named<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
    spans.iter().filter(|span| span.name == name).collect()
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a opentelemetry::Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
}

/// The gateway's statement span for `operation`.
///
/// The lookup uses an attribute rather than the name on purpose. `otel.name`
/// renames the span to its `db.query.summary`, such as `"SELECT t150"`. That is
/// the `OTel` convention for database spans, and it is what an operator sees in
/// the waterfall.
fn statement_span<'a>(spans: &'a [SpanData], operation: &str) -> &'a SpanData {
    let matching = spans
        .iter()
        .filter(|span| {
            attribute(span, "db.operation.name").is_some_and(|value| value.as_str() == operation)
        })
        .collect::<Vec<_>>();
    assert!(
        matching.len() == 1,
        "expected exactly one {operation} statement span"
    );
    matching[0]
}

/// The value of a numeric attribute. This function requires that the attribute
/// exported as an OTLP integer.
///
/// OTLP has no unsigned integer type, so `tracing-opentelemetry` turns a `u64`
/// or `usize` span field into a string. Tempo cannot compare, sort, or
/// range-filter a string, so `pg.participants > 2` silently matches nothing. An
/// assertion that the attribute only exists does not catch that. An assertion on
/// its variant does.
fn integer_attribute(span: &SpanData, key: &str) -> i64 {
    match attribute(span, key) {
        Some(opentelemetry::Value::I64(value)) => *value,
        other => panic!("{key} must export as an OTLP integer, got {other:?}"),
    }
}

fn only<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let matching = named(spans, name);
    assert!(matching.len() == 1, "expected exactly one {name} span");
    matching[0]
}

/// Render the named attributes of `span` as strings, so a test compares one
/// whole map rather than a chain of per-field assertions.
fn attributes(span: &SpanData, keys: &[&str]) -> BTreeMap<String, String> {
    keys.iter()
        .map(|key| {
            let value = span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == *key)
                .map_or_else(|| UNSET.to_owned(), |attribute| attribute.value.to_string());
            ((*key).to_owned(), value)
        })
        .collect()
}

fn expected(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

fn keys<'a>(pairs: &'a [(&'a str, &'a str)]) -> Vec<&'a str> {
    pairs.iter().map(|(key, _)| *key).collect()
}

/// Compare exactly the attributes `pairs` names, as one map.
fn check_attributes(span: &SpanData, pairs: &[(&str, &str)]) {
    check!(attributes(span, &keys(pairs)) == expected(pairs));
}

fn hash_split_config(tenant: &str) -> MultiRangeTenantConfig {
    MultiRangeTenantConfig::from_boundaries(
        TenantName::parse(tenant).expect("tenant"),
        "0:0:0,150:0:0,150:8:0,151:0:0",
    )
    .expect("config")
}

/// Two `id` values that hash into different ranges. An `INSERT` that carries
/// both is therefore a true multi-participant scatter, and not a one-range write
/// that only looks like one.
fn cross_range_ids(range_map: &crabka_gres_ranges::RangeMap, spec: &HashShardSpec) -> (i32, i32) {
    let first = 0_i32;
    let first_range = hash_range(range_map, spec, first);
    for second in 1_i32..100 {
        if hash_range(range_map, spec, second) != first_range {
            return (first, second);
        }
    }
    panic!("expected ids in different hash ranges")
}

fn hash_range(range_map: &crabka_gres_ranges::RangeMap, spec: &HashShardSpec, id: i32) -> RangeId {
    range_map
        .route_hash_equality(spec, id.to_be_bytes())
        .expect("route")
        .range_id
}

fn hash_spec() -> HashShardSpec {
    HashShardSpec::new(TableId::new(150), vec!["id".into()], 16, None).expect("hash spec")
}

#[tokio::test]
async fn routed_select_records_route_and_statement_spans() {
    let traces = Traces::install();
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_trace_route").expect("tenant"),
        "0,100,200",
    )
    .expect("config");
    let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t150 VALUES (7)")
        .await
        .expect("insert");
    session
        .simple_query("SELECT id FROM t150")
        .await
        .expect("select");
    drop(session);

    let spans = traces.finished();
    let select = statement_span(&spans, "SELECT");
    // `otel.kind` is consumed by the layer rather than exported as an
    // attribute, so it is checked on the span's kind.
    check!(select.span_kind == opentelemetry::trace::SpanKind::Internal);
    check!(select.name == "SELECT t150");
    check_attributes(
        select,
        &[
            ("db.system.name", "postgresql"),
            ("db.operation.name", "SELECT"),
            ("db.collection.name", "t150"),
            ("db.query.summary", "SELECT t150"),
            ("db.response.returned_rows", "1"),
            // Off unless CRABKA_OTLP_SQL_TEXT says otherwise: the only
            // attribute here that can carry a literal off the node.
            ("db.query.text", UNSET),
            ("pg.tenant", "tenant_trace_route"),
            ("pg.statement_kind", "query"),
            ("pg.table_id", "150"),
            // A SELECT changed nothing: the rows it returned are
            // `db.response.returned_rows`, a different question.
            ("pg.rows_affected", UNSET),
            ("pg.fast_path", "false"),
        ],
    );
    // A successful statement leaves the OTel status Unset — "OK" is a claim
    // the gateway deliberately never makes.
    check!(select.status == opentelemetry::trace::Status::Unset);

    let route = named(&spans, "pg.route")
        .into_iter()
        .find(|span| {
            attribute(span, "pg.statement_kind").is_some_and(|value| value.as_str() == "query")
        })
        .expect("query route span");
    check_attributes(
        route,
        &[
            ("pg.tenant", "tenant_trace_route"),
            ("pg.statement_kind", "query"),
            ("pg.range_id", "1"),
            ("pg.table_id", "150"),
            ("pg.scatter", "false"),
            ("pg.scatter_ranges", "0"),
        ],
    );

    // The routed dispatch hangs off the statement span, so a waterfall shows
    // which range the statement's time was spent on.
    let routed = named(&spans, "pg.routed_statement")
        .into_iter()
        .find(|span| {
            attribute(span, "pg.statement_kind").is_some_and(|value| value.as_str() == "query")
        })
        .expect("query dispatch span");
    check!(integer_attribute(routed, "pg.range_id") == 1);
    check!(integer_attribute(route, "pg.table_id") == 150);
    check!(routed.parent_span_id == select.span_context.span_id());
    check!(routed.span_context.trace_id() == select.span_context.trace_id());
    check_attributes(
        routed,
        &[
            ("pg.range_id", "1"),
            ("pg.statement_kind", "query"),
            ("pg.local", "true"),
        ],
    );
}

#[tokio::test]
async fn ddl_span_records_the_local_catalog_owner() {
    let traces = Traces::install();
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_trace_ddl").expect("tenant"),
        "0,100,200",
    )
    .expect("config");
    let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    drop(session);

    let spans = traces.finished();
    let ddl = only(&spans, "pg.ddl");
    check_attributes(
        ddl,
        &[
            ("pg.tenant", "tenant_trace_ddl"),
            ("pg.range_id", "0"),
            ("pg.local", "true"),
        ],
    );
    let statement = statement_span(&spans, "CREATE");
    check!(ddl.parent_span_id == statement.span_context.span_id());
    check_attributes(
        statement,
        &[
            ("db.operation.name", "CREATE"),
            ("db.query.summary", "CREATE"),
            ("db.collection.name", ""),
            ("pg.statement_kind", "ddl"),
        ],
    );
}

#[tokio::test]
async fn scatter_write_records_participants_and_commits() {
    let traces = Traces::install();
    let (gateway, handles) =
        MultiRangeTenant::start(hash_split_config("tenant_trace_scatter")).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    let spec = hash_spec();
    let range_map = handles.range_map();
    let (first_id, second_id) = cross_range_ids(&range_map, &spec);
    let mut participants = [
        hash_range(&range_map, &spec, first_id),
        hash_range(&range_map, &spec, second_id),
    ];
    participants.sort_unstable();
    let participant_ranges = format!("{},{}", participants[0].as_u32(), participants[1].as_u32());

    session
        .simple_query(&format!(
            "INSERT INTO t150 VALUES ({first_id}, 10), ({second_id}, 20)"
        ))
        .await
        .expect("cross-range scatter insert");
    drop(session);

    let spans = traces.finished();
    let scatter = only(&spans, "pg.timestamp_scatter");
    check_attributes(
        scatter,
        &[
            ("pg.tenant", "tenant_trace_scatter"),
            ("pg.table_id", "150"),
            ("pg.participants", "2"),
            ("pg.participant_ranges", &participant_ranges),
            ("pg.primary_range", &participants[0].as_u32().to_string()),
            ("pg.writes", "2"),
            ("pg.autocommit", "true"),
            ("pg.single_shard_bypass", "false"),
            ("pg.outcome", "committed"),
        ],
    );
    check!(scatter.status == opentelemetry::trace::Status::Unset);

    // Numeric attributes must be integers, not stringified `u64`s, or an
    // operator cannot filter on `pg.participants > 1` at all.
    check!(integer_attribute(scatter, "pg.participants") == 2);
    check!(integer_attribute(scatter, "pg.writes") == 2);
    check!(integer_attribute(scatter, "pg.table_id") == 150);
    check!(integer_attribute(scatter, "pg.primary_range") == i64::from(participants[0].as_u32()));
    // The timestamps are minted at run time, so pin their ordering rather than
    // their values — the point of the assertion is that they are comparable.
    let start_ts = integer_attribute(scatter, "pg.start_ts");
    check!(integer_attribute(scatter, "pg.global_xid") == start_ts);
    check!(integer_attribute(scatter, "pg.commit_ts") > start_ts);
    check!(integer_attribute(statement_span(&spans, "INSERT"), "pg.rows_affected") == 2);

    // One prewrite per participant, exactly one of them the primary, all of
    // them children of the scatter round.
    let prewrites = named(&spans, "pg.prewrite");
    check!(prewrites.len() == 2);
    let mut roles = prewrites
        .iter()
        .map(|span| attributes(span, &["pg.role"])["pg.role"].clone())
        .collect::<Vec<_>>();
    roles.sort();
    check!(roles == vec!["primary".to_owned(), "secondary".to_owned()]);
    for prewrite in &prewrites {
        check!(prewrite.parent_span_id == scatter.span_context.span_id());
    }

    // Both participants are resolved against the same committed decision.
    let resolves = named(&spans, "pg.resolve");
    check!(resolves.len() == 2);
    for resolve in &resolves {
        check_attributes(
            resolve,
            &[("pg.decision", "committed"), ("pg.local", "true")],
        );
    }
}

#[tokio::test]
async fn rolled_back_scatter_records_prepared_then_aborted() {
    let traces = Traces::install();
    let (gateway, handles) =
        MultiRangeTenant::start(hash_split_config("tenant_trace_rollback")).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    let spec = hash_spec();
    let range_map = handles.range_map();
    let (first_id, second_id) = cross_range_ids(&range_map, &spec);
    let mut participants = [
        hash_range(&range_map, &spec, first_id),
        hash_range(&range_map, &spec, second_id),
    ];
    participants.sort_unstable();

    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query(&format!(
            "INSERT INTO t150 VALUES ({first_id}, 10), ({second_id}, 20)"
        ))
        .await
        .expect("scatter insert inside a transaction");
    session.simple_query("ROLLBACK").await.expect("rollback");
    drop(session);

    let spans = traces.finished();
    // The statement's prewrites are durable but undecided when it returns:
    // the decision belongs to the COMMIT that never came.
    check_attributes(
        only(&spans, "pg.timestamp_scatter"),
        &[("pg.autocommit", "false"), ("pg.outcome", "prepared")],
    );
    check_attributes(
        only(&spans, "pg.abort_scatter"),
        &[
            ("pg.participants", "2"),
            (
                "pg.participant_ranges",
                &format!("{},{}", participants[0].as_u32(), participants[1].as_u32()),
            ),
            ("pg.primary_range", &participants[0].as_u32().to_string()),
            ("pg.outcome", "aborted"),
        ],
    );
}

#[tokio::test]
async fn scatter_failing_before_its_decision_records_indeterminate_and_error() {
    let traces = Traces::install();
    let config = hash_split_config("tenant_trace_indeterminate")
        .with_commit_fault_for_testing(GatewayCommitFault::AfterTimestampPrewriteBeforeDecision);
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    let spec = hash_spec();
    let range_map = handles.range_map();
    let (first_id, second_id) = cross_range_ids(&range_map, &spec);

    let error = session
        .simple_query(&format!(
            "INSERT INTO t150 VALUES ({first_id}, 10), ({second_id}, 20)"
        ))
        .await
        .expect_err("injected failure between prewrite and decision");
    check!(error.code == "XX000");
    drop(session);

    let spans = traces.finished();
    let scatter = only(&spans, "pg.timestamp_scatter");
    // Prewrites are durable and no decision exists: participants may still
    // hold locks, which is the one outcome an operator must be able to find.
    check_attributes(
        scatter,
        &[("pg.outcome", "indeterminate"), ("error.type", "XX000")],
    );
    // `otel.status_code` / `otel.status_description` become the OTel status
    // rather than attributes. Pinning the description is what catches the
    // `otel.status_message` misspelling, which exports an empty one.
    assert!(let opentelemetry::trace::Status::Error { description } = &scatter.status);
    check!(description.as_ref() == error.message);

    // The statement span folds the same SQLSTATE, under both the OTel and the
    // database semantic-convention names.
    let statement = statement_span(&spans, "INSERT");
    check_attributes(
        statement,
        &[
            ("error.type", "XX000"),
            ("db.response.status_code", "XX000"),
        ],
    );
    assert!(let opentelemetry::trace::Status::Error { description } = &statement.status);
    check!(description.as_ref() == error.message);
}

/// A `BEGIN`, write, write, `COMMIT` sequence that touches two ranges must
/// reach commit as an escalated transaction and settle through the
/// **global-xid** two-phase commit. That is `pg.commit_global` with one
/// `pg.prepare` per participant.
///
/// This is an atomicity assertion in the clothes of a tracing test. Those two
/// spans are the only external evidence that `touch_write_range` escalated.
/// Without the escalation, the writes would land on their ranges independently,
/// and a crash between them would leave the transaction half-applied.
///
/// The test also pins which of the two commit protocols this shape uses. Plain
/// per-range tables settle through the global-xid path asserted here. Only
/// hash-sharded tables use the timestamp-scatter protocol, which emits
/// `pg.timestamp_scatter`, `pg.prewrite`, and `pg.resolve`, and which
/// `scatter_write_records_participants_and_commits` covers. To expect scatter
/// spans from a transaction of this shape is a misreading, not a bug. See
/// `GatewayTransaction` in `tenant.rs`.
#[tokio::test]
async fn cross_range_transaction_commits_through_global_two_phase_commit() {
    let traces = Traces::install();
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_trace_xrange").expect("tenant"),
        "0,100,200",
    )
    .expect("config");
    let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    // `t50` and `t150` straddle the boundary at 100, so the two writes enlist
    // different ranges and the second one is what forces escalation.
    for ddl in ["CREATE TABLE t50 (id int4)", "CREATE TABLE t150 (id int4)"] {
        session.simple_query(ddl).await.expect("create");
    }
    for statement in [
        "BEGIN",
        "INSERT INTO t50 VALUES (1)",
        "INSERT INTO t150 VALUES (2)",
        "COMMIT",
    ] {
        session
            .simple_query(statement)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
    }

    // Both writes survived the commit. A half-applied transaction shows up here
    // as one of these reading zero rows.
    for table in ["t50", "t150"] {
        let result = session
            .simple_query(&format!("SELECT id FROM {table}"))
            .await
            .unwrap_or_else(|error| panic!("read {table}: {error:?}"));
        let rows = match result.as_slice() {
            [crabka_pgwire::engine::QueryResult::Rows { rows, .. }] => rows.len(),
            other => panic!("read {table}: unexpected result {other:?}"),
        };
        check!(rows == 1, "expected one row in {table}");
    }
    drop(session);

    let spans = traces.finished();
    let commit = only(&spans, "pg.commit_global");
    check!(commit.status == opentelemetry::trace::Status::Unset);
    // One prepare per enlisted range: the first phase actually ran, rather than
    // the gateway committing each participant outright.
    check!(named(&spans, "pg.prepare").len() == 2);
    // The scatter protocol is the *other* commit path and must stay absent, or
    // this test would pass against a gateway that silently changed protocols.
    for absent in ["pg.timestamp_scatter", "pg.prewrite", "pg.resolve"] {
        check!(
            named(&spans, absent).is_empty(),
            "{absent} belongs to the sharded scatter path, not this one"
        );
    }
}

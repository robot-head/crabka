use assert2::{assert, check};
use crabka_trace_context::{TRACEPARENT, TRACESTATE, TraceCarrier, TraceContextError};
use opentelemetry::trace::{SpanId, TraceId, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tracing_subscriber::layer::SubscriberExt as _;

/// A named case plus the record headers it feeds to `TraceCarrier::from_headers`.
type HeaderCase<'a> = (&'a str, Vec<(&'a str, &'a [u8])>);

const REMOTE: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const REMOTE_TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
const REMOTE_SPAN_ID: &str = "b7ad6b7169203331";

fn remote_trace_id() -> TraceId {
    TraceId::from_hex(REMOTE_TRACE_ID).unwrap()
}

fn remote_span_id() -> SpanId {
    SpanId::from_hex(REMOTE_SPAN_ID).unwrap()
}

/// Run `f` under a subscriber wired to a real `OTel` tracer with `AlwaysOn`
/// sampling, and collect the spans closed inside it.
///
/// A check on the exported [`SpanData`], and not on a live `Span` handle, is
/// what makes the parent and link difference observable.
/// `tracing-opentelemetry` resolves a re-parented span's trace id when the span
/// closes, not when `set_parent` runs.
fn exported_spans(f: impl FnOnce()) -> Vec<SpanData> {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("carrier-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::with_default(subscriber, f);
    provider.force_flush().unwrap();
    exporter.get_finished_spans().unwrap()
}

fn find_span<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|span| span.name == name)
        .unwrap_or_else(|| panic!("no exported span named {name}"))
}

/// A `tracestate` of exactly `total` bytes, spread over three list members.
///
/// Three members keep every single value inside the 256-byte W3C per-value cap.
fn tracestate_of_len(total: usize) -> String {
    let keys = ["ka", "kb", "kc"];
    // Each member contributes `k=` plus its padding, and the members are joined
    // by `keys.len() - 1` commas.
    let overhead = keys.len() * 3 + keys.len() - 1;
    let padding = total - overhead;
    let base = padding / keys.len();
    let extra = padding % keys.len();
    keys.iter()
        .enumerate()
        .map(|(index, key)| {
            let width = base + usize::from(index < extra);
            format!("{key}={}", "v".repeat(width))
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn tracestate_with_members(count: usize) -> String {
    (0..count)
        .map(|index| format!("k{index}=v"))
        .collect::<Vec<_>>()
        .join(",")
}

#[test]
fn from_w3c_accepts_only_well_formed_version_00_traceparents() {
    let cases: [(&str, &str, Option<TraceContextError>); 13] = [
        ("canonical", REMOTE, None),
        (
            "unsampled flags are still valid",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
            None,
        ),
        (
            "future version",
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            Some(TraceContextError::UnsupportedVersion),
        ),
        (
            "invalid version ff",
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            Some(TraceContextError::UnsupportedVersion),
        ),
        (
            "one byte short",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0",
            Some(TraceContextError::Length(54)),
        ),
        (
            "one byte long",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-010",
            Some(TraceContextError::Length(56)),
        ),
        ("empty", "", Some(TraceContextError::Length(0))),
        // W3C mandates lower-case hex in all three fields, and each needs its
        // own case: the permissive parsers underneath disagree about which
        // ones they would otherwise wave through. `TraceId`/`SpanId::from_hex`
        // accept upper case, and `u8::from_str_radix(_, 16)` accepts `0A`, so
        // dropping any one of these guards silently admits a non-conforming
        // traceparent rather than failing later.
        (
            "upper-case hex trace id",
            "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
            Some(TraceContextError::Malformed),
        ),
        (
            "upper-case hex span id",
            "00-0af7651916cd43dd8448eb211c80319c-B7AD6B7169203331-01",
            Some(TraceContextError::Malformed),
        ),
        (
            "upper-case hex flags",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0A",
            Some(TraceContextError::Malformed),
        ),
        (
            "wrong separator",
            "00-0af7651916cd43dd8448eb211c80319c_b7ad6b7169203331-01",
            Some(TraceContextError::Malformed),
        ),
        (
            "all-zero trace id",
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            Some(TraceContextError::ZeroTraceId),
        ),
        (
            "all-zero span id",
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            Some(TraceContextError::ZeroSpanId),
        ),
    ];

    for (name, traceparent, expected) in cases {
        let actual = TraceCarrier::from_w3c(traceparent, None);
        match expected {
            None => {
                assert!(let Ok(carrier) = actual, "{name}");
                check!(
                    carrier.traceparent.as_deref() == Some(traceparent),
                    "{name}"
                );
                check!(carrier.tracestate.is_none(), "{name}");
            }
            Some(error) => {
                check!(actual.unwrap_err() == error, "{name}");
            }
        }
    }
}

#[test]
fn from_w3c_keeps_or_drops_tracestate_by_size_and_shape() {
    let at_limit = tracestate_of_len(512);
    let over_limit = tracestate_of_len(513);
    let max_members = tracestate_with_members(32);
    let too_many_members = tracestate_with_members(33);

    let cases: [(&str, &str, Option<&str>); 7] = [
        (
            "simple vendor state",
            "congo=t61rcWkgMzE",
            Some("congo=t61rcWkgMzE"),
        ),
        ("512 bytes is kept", &at_limit, Some(&at_limit)),
        ("513 bytes is dropped", &over_limit, None),
        ("32 members are kept", &max_members, Some(&max_members)),
        ("33 members are dropped", &too_many_members, None),
        ("a member without `=` is dropped", "congo", None),
        // Re-rendered from the parsed `TraceState`, so the trailing separator
        // the client sent does not survive into the stored value.
        (
            "trailing separator is re-rendered away",
            "congo=t61,",
            Some("congo=t61"),
        ),
    ];

    for (name, tracestate, expected) in cases {
        assert!(
            let Ok(carrier) = TraceCarrier::from_w3c(REMOTE, Some(tracestate)),
            "{name}"
        );
        check!(carrier.traceparent.as_deref() == Some(REMOTE), "{name}");
        check!(carrier.tracestate.as_deref() == expected, "{name}");
    }
}

#[test]
fn tracestate_length_helper_produces_the_requested_size() {
    // The 512 / 513 boundary cases above are only meaningful if the fixture
    // really is that many bytes.
    check!(tracestate_of_len(512).len() == 512);
    check!(tracestate_of_len(513).len() == 513);
    check!(tracestate_with_members(32).split(',').count() == 32);
    check!(tracestate_with_members(33).split(',').count() == 33);
}

#[test]
fn capture_current_round_trips_through_serde_into_a_child_span() {
    let mut encoded = String::new();
    let spans = exported_spans(|| {
        let carrier = {
            let producer = tracing::info_span!("producer");
            let _guard = producer.enter();
            TraceCarrier::capture_current()
        };

        encoded = serde_json::to_string(&carrier).unwrap();
        let decoded: TraceCarrier = serde_json::from_str(&encoded).unwrap();

        // Created outside the producer's scope, so this span is a root until
        // the carrier re-parents it into the producer's trace.
        let consumer = tracing::info_span!("consumer");
        decoded.apply_to(&consumer);
        let _guard = consumer.enter();
    });

    let producer = find_span(&spans, "producer");
    let consumer = find_span(&spans, "consumer");
    let expected = format!(
        "00-{}-{}-01",
        producer.span_context.trace_id(),
        producer.span_context.span_id()
    );

    check!(encoded == format!(r#"{{"traceparent":"{expected}"}}"#));
    check!(consumer.span_context.trace_id() == producer.span_context.trace_id());
    check!(consumer.parent_span_id == producer.span_context.span_id());
    check!(consumer.parent_span_is_remote);
}

#[test]
fn capture_current_is_empty_without_an_active_span() {
    let carrier = TraceCarrier::capture_current();
    check!(carrier.is_empty());
    check!(carrier.traceparent.is_none());
    check!(carrier.tracestate.is_none());
    check!(carrier.headers().count() == 0);
}

#[test]
fn an_empty_carrier_serialises_to_nothing() {
    check!(serde_json::to_string(&TraceCarrier::default()).unwrap() == "{}");
    let decoded: TraceCarrier = serde_json::from_str("{}").unwrap();
    check!(decoded.is_empty());
}

#[test]
fn apply_to_makes_the_span_a_child_of_the_remote_parent() {
    let spans = exported_spans(|| {
        let carrier = TraceCarrier::from_w3c(REMOTE, None).unwrap();
        let span = tracing::info_span!("consumer");
        carrier.apply_to(&span);
        let _guard = span.enter();
    });

    let consumer = find_span(&spans, "consumer");
    check!(consumer.span_context.trace_id() == remote_trace_id());
    check!(consumer.parent_span_id == remote_span_id());
    check!(consumer.parent_span_is_remote);
    check!(consumer.links.is_empty());
}

#[test]
fn link_into_records_a_link_and_leaves_the_span_a_root() {
    let spans = exported_spans(|| {
        let carrier = TraceCarrier::from_w3c(REMOTE, None).unwrap();
        let span = tracing::info_span!("apply");
        carrier.link_into(&span);
        let _guard = span.enter();
    });

    let applied = find_span(&spans, "apply");
    let linked: Vec<_> = applied
        .links
        .iter()
        .map(|link| (link.span_context.trace_id(), link.span_context.span_id()))
        .collect();
    check!(linked == vec![(remote_trace_id(), remote_span_id())]);

    // The link must not double as a parent: the applying span keeps its own
    // trace and stays a root.
    check!(applied.span_context.trace_id() != remote_trace_id());
    check!(applied.parent_span_id == SpanId::INVALID);
}

#[test]
fn an_empty_carrier_neither_parents_nor_links() {
    let spans = exported_spans(|| {
        let carrier = TraceCarrier::default();
        let span = tracing::info_span!("solo");
        carrier.apply_to(&span);
        carrier.link_into(&span);
        let _guard = span.enter();
    });

    let solo = find_span(&spans, "solo");
    check!(solo.parent_span_id == SpanId::INVALID);
    check!(solo.links.is_empty());
}

#[test]
fn from_headers_reads_a_record_header_pair() {
    let state = "congo=t61rcWkgMzE";
    let carrier = TraceCarrier::from_headers([
        ("content-type", b"application/json".as_slice()),
        (TRACEPARENT, REMOTE.as_bytes()),
        (TRACESTATE, state.as_bytes()),
    ]);

    check!(carrier.traceparent.as_deref() == Some(REMOTE));
    check!(carrier.tracestate.as_deref() == Some(state));

    let headers: Vec<_> = carrier
        .headers()
        .map(|(key, value)| (key.to_owned(), String::from_utf8(value.to_vec()).unwrap()))
        .collect();
    check!(
        headers
            == vec![
                (TRACEPARENT.to_owned(), REMOTE.to_owned()),
                (TRACESTATE.to_owned(), state.to_owned()),
            ]
    );
}

#[test]
fn from_headers_yields_an_empty_carrier_for_unusable_values() {
    let cases: [HeaderCase<'_>; 4] = [
        (
            "no trace headers at all",
            vec![("key", b"value".as_slice())],
        ),
        (
            "non-utf8 traceparent",
            vec![(TRACEPARENT, [0xff, 0xfe].as_slice())],
        ),
        (
            "invalid traceparent",
            vec![(TRACEPARENT, b"not-a-traceparent".as_slice())],
        ),
        (
            "tracestate without a traceparent",
            vec![(TRACESTATE, b"congo=t61".as_slice())],
        ),
    ];

    for (name, headers) in cases {
        let carrier = TraceCarrier::from_headers(headers);
        check!(carrier.is_empty(), "{name}");
        check!(carrier.tracestate.is_none(), "{name}");
        check!(carrier.headers().count() == 0, "{name}");
    }
}

#[test]
fn span_context_reports_the_parsed_remote_context() {
    let carrier = TraceCarrier::from_w3c(REMOTE, Some("congo=t61")).unwrap();
    assert!(let Some(span_context) = carrier.span_context());

    check!(span_context.trace_id() == remote_trace_id());
    check!(span_context.span_id() == remote_span_id());
    check!(span_context.is_sampled());
    check!(span_context.is_remote());
    check!(span_context.trace_state().header() == "congo=t61");
    check!(TraceCarrier::default().span_context().is_none());
}

//! Session- and statement-scoped tracing for the `PostgreSQL` wire protocol.
//!
//! pgwire is the single ingress choke point for both gres engine
//! implementations, so this module owns two things: the spans that describe one
//! connection and the statements executed on it, and the policy that decides how
//! much of a *client-supplied* trace context those spans are allowed to inherit.
//!
//! # Zero cost when off
//!
//! Every span here is emitted at `DEBUG` under the dedicated [`SESSION_TARGET`]
//! target, which only the OTLP layer enables (see `crabka_gres::telemetry`). A
//! disabled callsite costs a load and a branch — but its *field expressions
//! still evaluate*, so anything more expensive than a field read is built behind
//! an explicit `tracing::enabled!` check with a [`tracing::Span::none`]
//! fallback. The two costs worth avoiding are the `getpeername` call behind
//! [`session_span`]'s peer address (guarded by its caller) and the sqlcommenter
//! scan in [`ingress_from_sql`] (guarded by the span it would attach to).
//!
//! Spans are built by hand rather than with `#[instrument]`: the attribute
//! cannot express that guard, and every span here records outcome fields after
//! the fact.
//!
//! # Attributes
//!
//! Attribute names follow the `OpenTelemetry` semantic conventions where one
//! exists (`db.*`, `network.*`, `error.type`), and are prefixed `pg.` where the
//! value is `PostgreSQL`-specific. `otel.kind`, `otel.name`, `otel.status_code`
//! and `otel.status_description` are the fields `tracing-opentelemetry` lifts
//! onto the `OpenTelemetry` span itself rather than exporting as attributes —
//! note `status_description`, not the `status_message` the semantic conventions
//! name: the field the layer looks for is the one that has to be spelled here.
//!
//! `db.query.text` is deliberately **not** recorded here. Verbatim SQL is off by
//! default behind `CRABKA_OTLP_SQL_TEXT` and is attached by the engine, which is
//! also where the parsed statement needed for `db.query.summary` exists.

use std::net::SocketAddr;

use crabka_trace_context::{TraceCarrier, extract_sqlcommenter};
use num_traits::ToPrimitive as _;

use crate::error::{PgError, sqlstate};

/// `tracing` target carrying the pgwire session and statement spans.
///
/// Spelled out rather than imported: `crabka-gres` names the same string in its
/// default OTLP `EnvFilter`, but `crabka-pgwire` cannot depend on it without a
/// cycle. The two must stay in step — `crabka_gres::telemetry` has a test that
/// its filter enables every target.
pub const SESSION_TARGET: &str = "crabka_pgwire::session";

/// Sampling ratio assumed when nothing configures one, matching
/// `CRABKA_OTLP_SAMPLE_RATIO`'s own default.
pub const DEFAULT_SAMPLE_RATIO: f64 = 1.0;

/// A span status is a human-readable description, not a payload; a runaway
/// engine message must not become the largest field on the span.
const MAX_STATUS_MESSAGE_BYTES: usize = 512;

/// The `sampled` bit of the W3C `traceparent` trace-flags byte.
const TRACE_FLAG_SAMPLED: u8 = 0x01;

/// `2^63`, exactly. The size of the space `TraceIdRatioBased` samples over: it
/// compares the low 63 bits of the trace-id against `ratio * 2^63`.
const SAMPLING_SPACE: f64 = 9_223_372_036_854_775_808.0;

/// How much of a client-supplied W3C trace context this server honours.
///
/// The client controls the `traceparent` it appends to its SQL, and the OTLP
/// pipeline samples with `Sampler::ParentBased(TraceIdRatioBased(ratio))`. A
/// *sampled* remote parent makes `ParentBased` return `RecordAndSample`
/// unconditionally, so a client that stamps `-01` on every statement forces
/// 100% export of every gres span it touches — on every range owner in the
/// cluster. This is the knob that decides whether that is allowed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IngressTracePolicy {
    /// Ignore ingress context entirely. Gres traces are always its own.
    Off,

    /// Record the client's context as an `OpenTelemetry` **link** rather than as
    /// a parent, and let gres head-sample independently. Correlation without
    /// ceding the sampling decision.
    Link,

    /// Accept the client's context as the parent, but **recompute** the sampled
    /// flag locally from the incoming trace-id at `ratio`.
    ///
    /// Recompute, not clear: `ParentBased` returns `Drop` for a *non-sampled*
    /// parent — it does not fall through to the root sampler — so clearing the
    /// bit would drop exactly the statements the client took the trouble to
    /// instrument. Because `TraceIdRatioBased` is a pure function of the
    /// trace-id, a client and gres running at the same ratio agree by
    /// construction and traces stay whole across the boundary.
    ///
    /// `ratio` must be the same value the OTLP pipeline was built with
    /// (`CRABKA_OTLP_SAMPLE_RATIO`); it is carried here because pgwire has no
    /// access to the tracer provider. Use [`IngressTracePolicy::resample`] to
    /// build one from an unvalidated number.
    Resample {
        /// Head-sampling ratio in `[0.0, 1.0]`.
        ratio: f64,
    },

    /// Honour the client's sampled flag verbatim. Trusted clients only.
    Trust,
}

impl Default for IngressTracePolicy {
    /// [`IngressTracePolicy::Resample`] at [`DEFAULT_SAMPLE_RATIO`] — the client
    /// gets to say *which* trace a statement belongs to, gres keeps the say over
    /// how much of it is exported.
    fn default() -> Self {
        Self::Resample {
            ratio: DEFAULT_SAMPLE_RATIO,
        }
    }
}

impl IngressTracePolicy {
    /// Build a [`IngressTracePolicy::Resample`] policy, clamping `ratio` into
    /// `[0.0, 1.0]` exactly as the OTLP pipeline clamps its own.
    #[must_use]
    pub fn resample(ratio: f64) -> Self {
        Self::Resample {
            ratio: if ratio.is_nan() {
                DEFAULT_SAMPLE_RATIO
            } else {
                ratio.clamp(0.0, 1.0)
            },
        }
    }

    /// Relate `span` to the client context in `carrier` according to this
    /// policy. A no-op for an empty carrier or a disabled span.
    pub fn attach(self, carrier: &TraceCarrier, span: &tracing::Span) {
        if carrier.is_empty() || span.is_disabled() {
            return;
        }
        match self {
            Self::Off => {}
            Self::Link => carrier.link_into(span),
            Self::Trust => carrier.apply_to(span),
            Self::Resample { ratio } => resampled(carrier, ratio).apply_to(span),
        }
    }
}

/// Re-render `carrier` with the sampled flag recomputed locally at `ratio`.
///
/// The carrier's `traceparent` has already been validated and re-rendered by
/// [`TraceCarrier::from_w3c`], so the round trip through [`TraceCarrier::span_context`]
/// cannot lose anything. Trace-flag bits other than `sampled` are preserved: the
/// W3C specification requires unknown flags to be forwarded untouched.
fn resampled(carrier: &TraceCarrier, ratio: f64) -> TraceCarrier {
    let Some(context) = carrier.span_context() else {
        return TraceCarrier::default();
    };
    let flags = if sampled_by_ratio(context.trace_id().to_bytes(), ratio) {
        context.trace_flags().to_u8() | TRACE_FLAG_SAMPLED
    } else {
        context.trace_flags().to_u8() & !TRACE_FLAG_SAMPLED
    };
    let traceparent = format!(
        "00-{}-{}-{flags:02x}",
        context.trace_id(),
        context.span_id()
    );
    TraceCarrier::from_w3c(&traceparent, carrier.tracestate.as_deref()).unwrap_or_default()
}

/// The `TraceIdRatioBased` head-sampling decision for `trace_id`.
///
/// A byte-for-byte restatement of `opentelemetry_sdk`'s
/// `sample_based_on_probability`, which is what makes a client and gres running
/// at the same ratio reach the same answer for the same trace. Reimplemented
/// rather than called because `opentelemetry_sdk` is an exporter-side dependency
/// that has no business inside a wire-protocol crate.
fn sampled_by_ratio(trace_id: [u8; 16], ratio: f64) -> bool {
    if ratio >= 1.0 {
        return true;
    }
    if ratio.is_nan() || ratio <= 0.0 {
        return false;
    }
    // `ratio` is in `(0.0, 1.0)`, so the product is in `(0.0, 2^63)` and the
    // conversion is always in range; a NaN ratio was already rejected above.
    let Some(upper_bound) = (ratio * SAMPLING_SPACE).to_u64() else {
        return false;
    };
    let mut low = [0u8; 8];
    low.copy_from_slice(&trace_id[8..]);
    (u64::from_be_bytes(low) >> 1) < upper_bound
}

/// Which query protocol raised a `gres.statement` span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementProtocol {
    /// A simple-protocol `Query` message, which carries its own SQL.
    Simple,
    /// An extended-protocol `Execute` message against a bound portal.
    Extended,
}

impl StatementProtocol {
    /// The value recorded as `pg.protocol`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Extended => "extended",
        }
    }
}

/// Build the per-connection `gres.session` span.
///
/// `peer` is the only attribute known at connection time; the startup
/// parameters, backend pid and TLS decision arrive later and are filled in by
/// [`record_session_startup`] and [`record_session_tls`].
///
/// Callers should build this behind a `tracing::enabled!` check — resolving the
/// peer address is a syscall, and the argument evaluates whether or not the
/// callsite is enabled.
#[must_use]
pub fn session_span(peer: Option<SocketAddr>) -> tracing::Span {
    let span = tracing::debug_span!(
        target: SESSION_TARGET,
        "gres.session",
        otel.kind = "server",
        db.system.name = "postgresql",
        network.peer.address = tracing::field::Empty,
        network.peer.port = tracing::field::Empty,
        db.namespace = tracing::field::Empty,
        db.user = tracing::field::Empty,
        db.client.application_name = tracing::field::Empty,
        pg.backend_pid = tracing::field::Empty,
        pg.tls = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    );
    if let Some(peer) = peer {
        span.record("network.peer.address", tracing::field::display(peer.ip()));
        span.record("network.peer.port", peer.port());
    }
    span
}

/// Record the attributes a connection only learns once its `StartupMessage` has
/// been read and its backend id allocated.
///
/// Takes the raw startup parameters so the three lookups happen only when the
/// span is live.
pub fn record_session_startup(
    span: &tracing::Span,
    startup_params: &[(String, String)],
    backend_pid: i32,
) {
    if span.is_disabled() {
        return;
    }
    let value = |key: &str| {
        startup_params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    if let Some(database) = value("database") {
        span.record("db.namespace", database);
    }
    if let Some(user) = value("user") {
        span.record("db.user", user);
    }
    if let Some(application_name) = value("application_name") {
        span.record("db.client.application_name", application_name);
    }
    span.record("pg.backend_pid", backend_pid);
}

/// Record whether the connection negotiated TLS.
pub fn record_session_tls(span: &tracing::Span, tls: bool) {
    span.record("pg.tls", tls);
}

/// Build the `gres.statement` span covering one executed statement message.
///
/// `otel.name` stays [`tracing::field::Empty`]: the engine records
/// `db.query.summary` onto it once the statement has been parsed, and until then
/// the span keeps its generic name. Every outcome field is declared here so the
/// caller can fold the statement's result onto it with
/// [`record_statement_rows`] and [`record_statement_error`].
#[must_use]
pub fn statement_span(protocol: StatementProtocol) -> tracing::Span {
    tracing::debug_span!(
        target: SESSION_TARGET,
        "gres.statement",
        otel.kind = "server",
        otel.name = tracing::field::Empty,
        db.system.name = "postgresql",
        pg.protocol = protocol.as_str(),
        db.response.returned_rows = tracing::field::Empty,
        pg.result_pages = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        pg.canceled = tracing::field::Empty,
    )
}

/// Build the `gres.parse` span.
///
/// `Parse` earns a span of its own because it is not always local work: for a
/// sharded table the gateway forwards the prepare to the range owner, a real
/// network hop that would otherwise be invisible between `Query` boundaries.
#[must_use]
pub fn parse_span(statement: &str) -> tracing::Span {
    tracing::debug_span!(
        target: SESSION_TARGET,
        "gres.parse",
        otel.kind = "internal",
        pg.statement_name = statement,
        db.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

/// Build the `gres.bind` span.
#[must_use]
pub fn bind_span(portal: &str, statement: &str) -> tracing::Span {
    tracing::debug_span!(
        target: SESSION_TARGET,
        "gres.bind",
        otel.kind = "internal",
        pg.portal_name = portal,
        pg.statement_name = statement,
        db.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

/// Build the `gres.describe` span. `kind` is the wire byte: `S` for a prepared
/// statement, `P` for a portal.
#[must_use]
pub fn describe_span(kind: u8, name: &str) -> tracing::Span {
    tracing::debug_span!(
        target: SESSION_TARGET,
        "gres.describe",
        otel.kind = "internal",
        pg.describe_kind = describe_kind(kind),
        pg.object_name = name,
        db.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

fn describe_kind(kind: u8) -> &'static str {
    match kind {
        b'S' => "statement",
        b'P' => "portal",
        _ => "unknown",
    }
}

/// Record the result size of one statement, once, after the caller's page loop.
///
/// Streaming sinks get no spans of their own: a 100k-row result would emit a
/// hundred page spans that the exporter drops, for one number an operator can
/// read off the statement.
pub fn record_statement_rows(span: &tracing::Span, rows: usize, pages: usize) {
    if span.is_disabled() {
        return;
    }
    span.record("db.response.returned_rows", rows);
    span.record("pg.result_pages", pages);
}

/// Fold a failed statement's outcome onto its `gres.statement` span, including
/// the cancellation flag when the failure was a `CancelRequest`.
pub fn record_statement_error(span: &tracing::Span, error: &PgError) {
    record_error(span, error);
    if error.code == sqlstate::QUERY_CANCELED {
        // The client saw an error, so the span's status is `ERROR` like any
        // other failure; `pg.canceled` is what tells the two apart.
        span.record("pg.canceled", true);
    }
}

/// Mark `span` failed.
///
/// Only the error case is ever recorded: `tracing-opentelemetry` leaves a span
/// `Unset` without an `otel.status_code`, and `Unset` — not `OK` — is what the
/// `OpenTelemetry` specification says a *server* span means by success.
///
/// `db.response.status_code` and `error.type` both carry the five-character
/// SQLSTATE, which is the correctly low-cardinality discriminator for a
/// `PostgreSQL` failure; the message goes in `otel.status_description`,
/// truncated.
pub fn record_error(span: &tracing::Span, error: &PgError) {
    if span.is_disabled() {
        return;
    }
    // Order matters: the layer treats `otel.status_description` as setting the
    // whole status, and `otel.status_code = "ERROR"` as setting it with an empty
    // description, so recording the code second would erase the message.
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", truncate_message(&error.message));
    span.record("db.response.status_code", error.code.as_str());
    span.record("error.type", error.code.as_str());
}

/// Truncate a diagnostic to [`MAX_STATUS_MESSAGE_BYTES`], on a character
/// boundary so the result is still valid UTF-8.
fn truncate_message(message: &str) -> &str {
    if message.len() <= MAX_STATUS_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_STATUS_MESSAGE_BYTES;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    &message[..end]
}

/// Read a client trace context out of `sql`'s [sqlcommenter] tag, relate `span`
/// to it under `policy`, and return the carrier so the extended protocol can
/// reuse it at `Execute` time.
///
/// The SQL text is never rewritten. `crabka_pgparser`'s lexer discards comments
/// without emitting a token, so the tag changes no AST — and the parser keeps
/// the original string, so a `ParseError`'s byte offset still points at the
/// right character in the SQLSTATE 42601 the client receives.
///
/// A malformed `traceparent` yields an empty carrier and nothing else: a bad
/// trace header must never fail the query it rode in on, and
/// [`TraceCarrier::from_w3c`] never embeds the offending input in its error.
///
/// [sqlcommenter]: https://google.github.io/sqlcommenter/
#[must_use]
pub fn ingress_from_sql(
    policy: IngressTracePolicy,
    sql: &str,
    span: &tracing::Span,
) -> TraceCarrier {
    if matches!(policy, IngressTracePolicy::Off) || span.is_disabled() {
        return TraceCarrier::default();
    }
    let Some(found) = extract_sqlcommenter(sql) else {
        return TraceCarrier::default();
    };
    let carrier = TraceCarrier::from_w3c(found.traceparent, found.tracestate).unwrap_or_default();
    policy.attach(&carrier, span);
    carrier
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// A `traceparent` whose trace-id's low 64 bits are all zero, so
    /// `TraceIdRatioBased` samples it at any ratio above zero.
    const LOW_TRACE: &str = "00-0af7651916cd43dd0000000000000000-b7ad6b7169203331-01";
    /// The mirror image: low 64 bits all ones, so it is sampled only at 1.0.
    const HIGH_TRACE: &str = "00-0af7651916cd43ddffffffffffffffff-b7ad6b7169203331-00";

    fn trace_id_of(traceparent: &str) -> [u8; 16] {
        TraceCarrier::from_w3c(traceparent, None)
            .expect("fixture traceparent is valid")
            .span_context()
            .expect("carrier holds a span context")
            .trace_id()
            .to_bytes()
    }

    /// The sampling decision is what keeps a client and gres agreeing about one
    /// trace, so it is pinned against the ratios either side of each boundary
    /// rather than merely exercised.
    #[test]
    fn ratio_sampling_is_a_pure_function_of_the_trace_id() {
        let low = trace_id_of(LOW_TRACE);
        let high = trace_id_of(HIGH_TRACE);

        let cases: [(&str, [u8; 16], f64, bool); 10] = [
            ("everything at 1.0", high, 1.0, true),
            ("above 1.0 saturates", low, 2.0, true),
            ("nothing at 0.0", low, 0.0, false),
            ("below 0.0 saturates", low, -1.0, false),
            ("a NaN ratio samples nothing", low, f64::NAN, false),
            ("the lowest trace id at 1%", low, 0.01, true),
            ("the highest trace id at 99%", high, 0.99, false),
            ("the lowest trace id at 50%", low, 0.5, true),
            ("the highest trace id at 50%", high, 0.5, false),
            (
                "the lowest trace id just above 0",
                low,
                f64::MIN_POSITIVE,
                false,
            ),
        ];

        for (name, trace_id, ratio, expected) in cases {
            check!(sampled_by_ratio(trace_id, ratio) == expected, "{name}");
        }
    }

    /// `Resample` must *recompute* the flag: clearing it unconditionally would
    /// make `ParentBased` drop every statement a client had instrumented.
    #[test]
    fn resample_rewrites_only_the_sampled_bit() {
        let cases: [(&str, &str, f64, &str); 4] = [
            (
                "a sampled trace survives a full ratio",
                LOW_TRACE,
                1.0,
                LOW_TRACE,
            ),
            (
                "a sampled trace is demoted at a zero ratio",
                LOW_TRACE,
                0.0,
                "00-0af7651916cd43dd0000000000000000-b7ad6b7169203331-00",
            ),
            (
                "an unsampled trace is promoted at a full ratio",
                HIGH_TRACE,
                1.0,
                "00-0af7651916cd43ddffffffffffffffff-b7ad6b7169203331-01",
            ),
            (
                "an unsampled trace stays down at a zero ratio",
                HIGH_TRACE,
                0.0,
                HIGH_TRACE,
            ),
        ];

        for (name, traceparent, ratio, expected) in cases {
            let carrier = TraceCarrier::from_w3c(traceparent, Some("congo=t61")).expect(name);
            let out = resampled(&carrier, ratio);
            check!(out.traceparent.as_deref() == Some(expected), "{name}");
            // Vendor state is orthogonal to the sampling decision and must ride
            // through untouched.
            check!(out.tracestate.as_deref() == Some("congo=t61"), "{name}");
        }
    }

    #[test]
    fn resampling_an_empty_carrier_yields_an_empty_carrier() {
        check!(resampled(&TraceCarrier::default(), 1.0).is_empty());
    }

    #[test]
    fn the_default_policy_resamples_at_the_pipeline_default_ratio() {
        check!(
            IngressTracePolicy::default()
                == IngressTracePolicy::Resample {
                    ratio: DEFAULT_SAMPLE_RATIO
                }
        );
    }

    #[test]
    fn resample_clamps_an_out_of_range_ratio() {
        let cases: [(&str, f64, f64); 5] = [
            ("negative", -0.5, 0.0),
            ("zero", 0.0, 0.0),
            ("in range", 0.25, 0.25),
            ("above one", 7.0, 1.0),
            ("not a number", f64::NAN, DEFAULT_SAMPLE_RATIO),
        ];
        for (name, input, expected) in cases {
            check!(
                IngressTracePolicy::resample(input)
                    == IngressTracePolicy::Resample { ratio: expected },
                "{name}"
            );
        }
    }

    #[test]
    fn a_status_message_is_truncated_on_a_character_boundary() {
        let short = "canceling statement due to user request";
        check!(truncate_message(short) == short);

        // A 3-byte character straddling the limit must not be split.
        let long = format!("{}€€", "a".repeat(MAX_STATUS_MESSAGE_BYTES - 1));
        let truncated = truncate_message(&long);
        check!(truncated.len() == MAX_STATUS_MESSAGE_BYTES - 1);
        check!(truncated == "a".repeat(MAX_STATUS_MESSAGE_BYTES - 1));
    }

    #[test]
    fn describe_kind_names_both_wire_bytes() {
        check!(describe_kind(b'S') == "statement");
        check!(describe_kind(b'P') == "portal");
        check!(describe_kind(b'X') == "unknown");
    }

    #[test]
    fn protocol_names_are_the_recorded_attribute_values() {
        check!(StatementProtocol::Simple.as_str() == "simple");
        check!(StatementProtocol::Extended.as_str() == "extended");
    }

    /// The tagged statement every ingress case below starts from.
    const TAGGED: &str =
        "SELECT 1 /*traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/";
    const TAGGED_TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    /// Run `f` under a bare `Registry`, which enables every callsite, so the
    /// spans built here are live without an exporter behind them.
    fn with_live_spans<T>(f: impl FnOnce() -> T) -> T {
        tracing::subscriber::with_default(tracing_subscriber::registry(), f)
    }

    /// Which statements yield a client context, and which are discarded — a
    /// hostile or absent `traceparent` must never fail the statement it rode in
    /// on, so every rejection is silent.
    #[test]
    fn ingress_extraction_follows_the_policy_and_never_fails_a_statement() {
        let cases: [(&str, IngressTracePolicy, &str, Option<&str>); 7] = [
            (
                "trust reads the tag",
                IngressTracePolicy::Trust,
                TAGGED,
                Some(TAGGED_TRACEPARENT),
            ),
            (
                "resample reads the tag",
                IngressTracePolicy::resample(1.0),
                TAGGED,
                Some(TAGGED_TRACEPARENT),
            ),
            (
                "link still captures the carrier",
                IngressTracePolicy::Link,
                TAGGED,
                Some(TAGGED_TRACEPARENT),
            ),
            (
                "policy off reads nothing",
                IngressTracePolicy::Off,
                TAGGED,
                None,
            ),
            (
                "an untagged statement",
                IngressTracePolicy::Trust,
                "SELECT 1",
                None,
            ),
            (
                "a malformed traceparent is discarded",
                IngressTracePolicy::Trust,
                "SELECT 1 /*traceparent='nonsense'*/",
                None,
            ),
            (
                "a string literal that merely looks like a tag",
                IngressTracePolicy::Trust,
                "SELECT '/*traceparent=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01*/'",
                None,
            ),
        ];

        with_live_spans(|| {
            for (name, policy, sql, expected) in cases {
                let span = statement_span(StatementProtocol::Simple);
                assert!(!span.is_disabled(), "{name}");
                let carrier = ingress_from_sql(policy, sql, &span);
                check!(carrier.traceparent.as_deref() == expected, "{name}");
            }
        });
    }

    /// The sqlcommenter scan is the one ingress cost that is not a field read,
    /// so it must not run when the span it would feed is disabled.
    #[test]
    fn a_disabled_span_skips_ingress_extraction_entirely() {
        let carrier = ingress_from_sql(IngressTracePolicy::Trust, TAGGED, &tracing::Span::none());
        check!(carrier.is_empty());
    }
}

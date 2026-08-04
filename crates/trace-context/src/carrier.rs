//! A serialisable W3C trace context that can ride inside an existing payload.

use std::str::FromStr as _;

use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::propagation::{TRACEPARENT, TRACESTATE, current_trace_headers, set_remote_parent};

/// Byte length of a version-`00` `traceparent`: `00-` + 32 + `-` + 16 + `-` + 2.
const TRACEPARENT_LEN: usize = 55;
/// Byte offsets of the three `-` separators in a version-`00` `traceparent`.
const TRACE_ID_RANGE: std::ops::Range<usize> = 3..35;
const SPAN_ID_RANGE: std::ops::Range<usize> = 36..52;
const FLAGS_RANGE: std::ops::Range<usize> = 53..55;

/// W3C caps `tracestate` at 512 bytes and 32 list members; anything larger is
/// dropped rather than forwarded, so a client cannot use it as free storage on
/// every span Crabka emits.
const MAX_TRACESTATE_BYTES: usize = 512;
const MAX_TRACESTATE_MEMBERS: usize = 32;

/// Why a client-supplied W3C trace context was rejected.
///
/// No variant embeds the offending input: a rejected `traceparent` is attacker
/// controlled, and a message carrying it verbatim would put that string into
/// whatever log field the error is rendered into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TraceContextError {
    /// The `traceparent` is not exactly 55 bytes long.
    #[error("traceparent must be {TRACEPARENT_LEN} bytes, got {0}")]
    Length(usize),

    /// Only version `00` of the W3C traceparent format is understood.
    #[error("unsupported traceparent version")]
    UnsupportedVersion,

    /// A separator was missing or a field held a non-lower-hex byte.
    #[error("malformed traceparent")]
    Malformed,

    /// An all-zero trace-id is defined as invalid by the W3C specification.
    #[error("traceparent carries an all-zero trace-id")]
    ZeroTraceId,

    /// An all-zero parent span-id is defined as invalid by the W3C specification.
    #[error("traceparent carries an all-zero span-id")]
    ZeroSpanId,
}

/// A W3C trace context in transit, sized to be embedded in a request payload.
///
/// Both fields are `Option` because most calls happen with no active span — no
/// sampling, or OTLP switched off entirely — and a carrier is then two `None`s
/// that serialise to nothing. `#[serde(default, skip_serializing_if = …)]` is
/// what makes that free on the wire; it is a payload-size optimisation for the
/// common empty case, **not** a compatibility shim for older encodings (Crabka
/// keeps none — see `CLAUDE.md`).
///
/// `PartialEq` is deliberately not derived. A carrier is embedded alongside a
/// request in RPC envelopes whose equality must stay a pure function of the
/// request payload; deriving it here would silently make two identical requests
/// compare unequal because they were traced differently.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TraceCarrier {
    /// The validated, re-rendered W3C `traceparent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,

    /// The validated, re-rendered W3C `tracestate`, when the peer sent one
    /// small enough to forward.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}

impl TraceCarrier {
    /// Capture the currently active span's trace context.
    ///
    /// Empty when there is no active span, the span is not sampled, or OTLP is
    /// disabled — so it is always safe to call on a hot path.
    #[must_use]
    pub fn capture_current() -> Self {
        let headers = current_trace_headers();
        let find = |name: &str| {
            headers
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.as_str())
        };
        let Some(traceparent) = find(TRACEPARENT) else {
            return Self::default();
        };
        Self::from_w3c(traceparent, find(TRACESTATE)).unwrap_or_default()
    }

    /// Build a carrier from a peer-supplied `traceparent` (and optional
    /// `tracestate`), validating both.
    ///
    /// `traceparent` must match `^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`
    /// with a non-zero trace-id and a non-zero span-id. `tracestate` is
    /// *dropped* — not an error — when it exceeds 512 bytes or 32 list
    /// members, or when it is not a well-formed list; the `traceparent` is
    /// still honoured, because losing vendor state is better than losing the
    /// trace.
    ///
    /// Neither input is stored verbatim. Both are parsed and re-rendered from
    /// the resulting [`SpanContext`] / [`TraceState`], so a hostile value
    /// cannot survive into a span attribute or a log line.
    ///
    /// # Errors
    ///
    /// Returns [`TraceContextError`] when `traceparent` fails validation.
    /// Callers on an ingress path should discard the error rather than surface
    /// it: a bad trace header must never fail the request it rode in on.
    pub fn from_w3c(
        traceparent: &str,
        tracestate: Option<&str>,
    ) -> Result<Self, TraceContextError> {
        let span_context = parse_traceparent(traceparent)?;
        Ok(Self {
            traceparent: Some(render_traceparent(&span_context)),
            tracestate: tracestate.and_then(sanitize_tracestate),
        })
    }

    /// Build a carrier from record headers (each a key plus a raw byte value),
    /// as carried on a Kafka record.
    ///
    /// Runs the same validation as [`TraceCarrier::from_w3c`], but a
    /// non-UTF-8, absent, or invalid value yields an empty carrier instead of
    /// an error — a consumer that cannot read the producer's trace context
    /// still has to apply the record.
    #[must_use]
    pub fn from_headers<'a, I, V>(headers: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, V)>,
        V: AsRef<[u8]>,
    {
        let mut traceparent = None;
        let mut tracestate = None;
        for (key, value) in headers {
            let target = if key.eq_ignore_ascii_case(TRACEPARENT) {
                &mut traceparent
            } else if key.eq_ignore_ascii_case(TRACESTATE) {
                &mut tracestate
            } else {
                continue;
            };
            if let Ok(text) = std::str::from_utf8(value.as_ref()) {
                *target = Some(text.to_owned());
            }
        }
        let Some(traceparent) = traceparent else {
            return Self::default();
        };
        Self::from_w3c(&traceparent, tracestate.as_deref()).unwrap_or_default()
    }

    /// `true` when there is no trace context to propagate.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none()
    }

    /// Make `span` a child of the carried context, so work on this side of the
    /// boundary joins the caller's trace. A no-op when the carrier is empty.
    pub fn apply_to(&self, span: &tracing::Span) {
        if self.is_empty() {
            return;
        }
        set_remote_parent(span, self.headers());
    }

    /// Attach the carried context to `span` as an OpenTelemetry **link** rather
    /// than as its parent. A no-op when the carrier is empty.
    ///
    /// This is the right relationship whenever the linked work fans out or runs
    /// far later than the originating operation — a WAL record applied by every
    /// follower, or replayed at recovery hours after the commit. Parenting
    /// those would stretch one trace across the retention period and force
    /// export of every apply of every sampled write.
    pub fn link_into(&self, span: &tracing::Span) {
        let Some(span_context) = self.span_context() else {
            return;
        };
        span.add_link(span_context);
    }

    /// The remote [`SpanContext`] this carrier describes, or `None` when the
    /// carrier is empty or holds a value that does not parse.
    #[must_use]
    pub fn span_context(&self) -> Option<SpanContext> {
        let parsed = parse_traceparent(self.traceparent.as_deref()?).ok()?;
        let state = self
            .tracestate
            .as_deref()
            .and_then(|value| TraceState::from_str(value).ok())
            .unwrap_or(TraceState::NONE);
        Some(SpanContext::new(
            parsed.trace_id(),
            parsed.span_id(),
            parsed.trace_flags(),
            true,
            state,
        ))
    }

    /// The carrier as `(key, value)` header pairs, ready to attach to a Kafka
    /// record. Yields nothing when the carrier is empty.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &[u8])> + '_ {
        let traceparent = self.traceparent.as_deref();
        // `tracestate` alone carries no trace, so it is never emitted without
        // the `traceparent` it qualifies.
        let tracestate = traceparent.and(self.tracestate.as_deref());
        traceparent
            .map(|value| (TRACEPARENT, value.as_bytes()))
            .into_iter()
            .chain(tracestate.map(|value| (TRACESTATE, value.as_bytes())))
    }
}

/// Validate a version-`00` W3C `traceparent` and return it as a remote
/// [`SpanContext`] with no vendor state.
pub fn parse_traceparent(value: &str) -> Result<SpanContext, TraceContextError> {
    let bytes = value.as_bytes();
    if bytes.len() != TRACEPARENT_LEN {
        return Err(TraceContextError::Length(bytes.len()));
    }
    if &bytes[0..2] != b"00" {
        return Err(TraceContextError::UnsupportedVersion);
    }
    if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
        return Err(TraceContextError::Malformed);
    }

    let trace_id = &bytes[TRACE_ID_RANGE];
    let span_id = &bytes[SPAN_ID_RANGE];
    let flags = &bytes[FLAGS_RANGE];
    if !is_lower_hex(trace_id) || !is_lower_hex(span_id) || !is_lower_hex(flags) {
        return Err(TraceContextError::Malformed);
    }
    if trace_id.iter().all(|byte| *byte == b'0') {
        return Err(TraceContextError::ZeroTraceId);
    }
    if span_id.iter().all(|byte| *byte == b'0') {
        return Err(TraceContextError::ZeroSpanId);
    }

    // Every byte is validated lower-hex of the exact expected width, so the
    // three conversions below cannot fail.
    let trace_id =
        TraceId::from_hex(&value[TRACE_ID_RANGE]).map_err(|_| TraceContextError::Malformed)?;
    let span_id =
        SpanId::from_hex(&value[SPAN_ID_RANGE]).map_err(|_| TraceContextError::Malformed)?;
    let flags =
        u8::from_str_radix(&value[FLAGS_RANGE], 16).map_err(|_| TraceContextError::Malformed)?;

    Ok(SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::new(flags),
        true,
        TraceState::NONE,
    ))
}

fn is_lower_hex(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn render_traceparent(span_context: &SpanContext) -> String {
    format!(
        "00-{}-{}-{:02x}",
        span_context.trace_id(),
        span_context.span_id(),
        span_context.trace_flags().to_u8()
    )
}

/// Re-render a peer's `tracestate`, or drop it when it is oversized or not a
/// well-formed W3C list.
fn sanitize_tracestate(value: &str) -> Option<String> {
    if value.is_empty() || value.len() > MAX_TRACESTATE_BYTES {
        return None;
    }
    if value.split(',').count() > MAX_TRACESTATE_MEMBERS {
        return None;
    }
    let state = TraceState::from_str(value).ok()?;
    let header = state.header();
    (!header.is_empty()).then_some(header)
}

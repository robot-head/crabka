//! A serialisable W3C trace context that fits inside an existing payload.

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

/// W3C caps `tracestate` at 512 bytes and 32 list members. Crabka drops a
/// larger value and does not forward it, so a client cannot use `tracestate`
/// as free storage on every span Crabka emits.
const MAX_TRACESTATE_BYTES: usize = 512;
const MAX_TRACESTATE_MEMBERS: usize = 32;

/// Why Crabka rejected a client-supplied W3C trace context.
///
/// No variant holds the input that failed. An attacker controls a rejected
/// `traceparent`, and a message that keeps it verbatim would put that string
/// into the log field that shows the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TraceContextError {
    /// The `traceparent` is not exactly 55 bytes long.
    #[error("traceparent must be {TRACEPARENT_LEN} bytes, got {0}")]
    Length(usize),

    /// Crabka reads only version `00` of the W3C traceparent format.
    #[error("unsupported traceparent version")]
    UnsupportedVersion,

    /// A separator was missing or a field held a non-lower-hex byte.
    #[error("malformed traceparent")]
    Malformed,

    /// The W3C specification defines an all-zero trace-id as invalid.
    #[error("traceparent carries an all-zero trace-id")]
    ZeroTraceId,

    /// The W3C specification defines an all-zero parent span-id as invalid.
    #[error("traceparent carries an all-zero span-id")]
    ZeroSpanId,
}

/// A W3C trace context in transit, sized to fit in a request payload.
///
/// Both fields are `Option` because most calls happen with no active span: no
/// sampling, or OTLP switched off. The carrier is then two `None`s that
/// serialise to nothing. `#[serde(default, skip_serializing_if = …)]` makes
/// that free on the wire. It is a payload-size optimisation for the common
/// empty case, **not** a compatibility shim for older encodings. Crabka keeps
/// no such shims: see `CLAUDE.md`.
///
/// This type does not derive `PartialEq`, and that is deliberate. RPC envelopes
/// hold a carrier next to a request, and their equality must stay a pure
/// function of the request payload. A derived `PartialEq` would silently make
/// two identical requests compare unequal because they were traced differently.
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
    /// The carrier is empty when there is no active span, when the span is not
    /// sampled, or when OTLP is disabled. It is always safe to call on a hot
    /// path.
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

    /// Build a carrier from a peer-supplied `traceparent` and `tracestate`.
    ///
    /// This function validates both inputs. `traceparent` must match
    /// `^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$` with a non-zero trace-id
    /// and a non-zero span-id. The function *drops* the `tracestate`, and does
    /// not return an error, when it is more than 512 bytes, when it has more
    /// than 32 list members, or when it is not a well-formed list. The
    /// `traceparent` still applies, because a lost vendor state is better than
    /// a lost trace.
    ///
    /// This function stores neither input verbatim. It parses both, then
    /// re-renders them from the [`SpanContext`] and the [`TraceState`], so a
    /// hostile value cannot get into a span attribute or a log line.
    ///
    /// # Errors
    ///
    /// Returns [`TraceContextError`] when `traceparent` fails validation. A
    /// caller on an ingress path should discard the error and not show it: a
    /// bad trace header must never fail the request that carried it.
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

    /// Build a carrier from the headers on a Kafka record.
    ///
    /// Each header is a key plus a raw byte value. This function does the same
    /// validation as [`TraceCarrier::from_w3c`]. But a non-UTF-8, absent, or
    /// invalid value gives an empty carrier and not an error, because a
    /// consumer that cannot read the producer's trace context must still apply
    /// the record.
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

    /// Make `span` a child of the carried context.
    ///
    /// Work on this side of the boundary then joins the caller's trace. This
    /// method does nothing when the carrier is empty.
    pub fn apply_to(&self, span: &tracing::Span) {
        if self.is_empty() {
            return;
        }
        set_remote_parent(span, self.headers());
    }

    /// Attach the carried context to `span` as an OpenTelemetry **link**.
    ///
    /// The carried context becomes a link, not the parent of `span`. This
    /// method does nothing when the carrier is empty.
    ///
    /// A link is the correct relation when many nodes do the linked work, or
    /// when the work runs long after the operation that started it. Every
    /// follower applies a WAL record, and recovery replays that record hours
    /// after the commit. A parent relation would stretch one trace across the
    /// retention period, and would force export of every apply of every
    /// sampled write.
    pub fn link_into(&self, span: &tracing::Span) {
        let Some(span_context) = self.span_context() else {
            return;
        };
        span.add_link(span_context);
    }

    /// The remote [`SpanContext`] this carrier describes.
    ///
    /// `None` when the carrier is empty, or when it holds a value that does
    /// not parse.
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

    /// The carrier as `(key, value)` header pairs for a Kafka record.
    ///
    /// The iterator yields nothing when the carrier is empty.
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

/// Validate a version-`00` W3C `traceparent`.
///
/// Returns the `traceparent` as a remote [`SpanContext`] with no vendor state.
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

/// Re-render a peer's `tracestate`.
///
/// This function drops the `tracestate` when it is too large, or when it is
/// not a well-formed W3C list.
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

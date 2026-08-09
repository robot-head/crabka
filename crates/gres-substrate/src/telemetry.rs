//! Tracing target and span builders for the substrate WAL path.
//!
//! Every span this crate emits carries the dedicated [`WAL_TARGET`] target, so
//! a compute with no OTLP pipeline pays one disabled-callsite level check per
//! commit and the stdout `fmt` layer never prints them. Only the OTLP filter in
//! `crates/gres/src/telemetry.rs` enables the target. The constant is
//! duplicated there and not shared, because `crabka-gres` depends on this
//! crate and the dependency cannot run the other way.
//!
//! # Zero cost when off
//!
//! A disabled callsite is a load and a branch, but the *field expressions still
//! evaluate*. Builders here therefore take only already-available scalars and
//! borrowed strings. Any value that must be computed, such as the encoded byte
//! total on `pg.commit` or the elapsed gate wait, is guarded by
//! [`tracing::Span::is_disabled`] at the call site and set afterwards through
//! [`tracing::Span::record`].
//!
//! # Status
//!
//! `tracing-opentelemetry` maps the `otel.status_code` /
//! `otel.status_description` fields onto the OpenTelemetry span status. Both
//! are declared [`tracing::field::Empty`] at creation and set by
//! [`record_error`], which is the only writer. A span that succeeds is left
//! `Unset` and not recorded as `"OK"`. This matches the OpenTelemetry
//! convention that `Ok` is reserved for an explicit application-level
//! assertion.
//!
//! The description field is `otel.status_description`, **not**
//! `otel.status_message`. `tracing-opentelemetry` recognises only
//! `otel.status_description`. It stores `otel.status_message` silently as an
//! ordinary span attribute and leaves the status without a message.
//!
//! # Integer attributes
//!
//! OTLP has no unsigned integer attribute type, and `tracing-opentelemetry`
//! renders a `u64`/`usize` field as a *string*. A trace backend cannot compare
//! or aggregate a string numerically. Every count here goes through
//! [`integer`] so it arrives as a real integer.

use std::fmt::Display;

use tracing::{Span, field::Empty};

use crate::writer::WriterGeneration;

/// `tracing` target that carries the substrate WAL spans.
///
/// This target stays off the `fmt` layer's default filter, so WAL spans
/// materialise only for OTLP.
pub const WAL_TARGET: &str = "crabka_gres_substrate::wal";

/// Upper bound on the recorded `otel.status_description`, in bytes. A WAL error
/// message is server-authored, but a broker string can still be long enough to
/// dominate a span's payload.
const MAX_STATUS_MESSAGE_BYTES: usize = 512;

/// Build the `pg.commit` span covering one engine commit: the group-commit
/// gate wait, the WAL append, and the local apply.
///
/// `pg.gate_wait_ms` is the wait for the commit permit alone, not the whole
/// commit. That field separates "the WAL is slow" from "this compute is
/// serialising behind another writer". `pg.commit.frames`, `pg.commit.bytes`,
/// `pg.journal_seq.first`, `pg.journal_seq.next`, and `pg.commit_ts` are only
/// known once the batch has been chunked, so they are recorded later.
#[must_use]
pub fn commit_span(ops: usize) -> Span {
    tracing::debug_span!(
        target: WAL_TARGET,
        "pg.commit",
        otel.kind = "internal",
        otel.status_code = Empty,
        otel.status_description = Empty,
        error.type = Empty,
        pg.commit.ops = integer(ops),
        pg.commit.frames = Empty,
        pg.commit.bytes = Empty,
        pg.journal_seq.first = Empty,
        pg.journal_seq.next = Empty,
        pg.commit_ts = Empty,
        pg.gate_wait_ms = Empty,
    )
}

/// Build the `gres.wal_append` producer span for one transactional group
/// commit against the range's WAL topic.
///
/// `pg.wal.paused` and `pg.wal.fenced` are recorded only when they are the
/// reason the append was refused, so their presence alone identifies the two
/// non-broker rejections.
#[must_use]
pub fn wal_append_span(topic: &str, generation: WriterGeneration, frames: usize) -> Span {
    tracing::debug_span!(
        target: WAL_TARGET,
        "gres.wal_append",
        otel.kind = "producer",
        otel.status_code = Empty,
        otel.status_description = Empty,
        error.type = Empty,
        messaging.system = "kafka",
        messaging.destination.name = topic,
        pg.wal.generation = integer(generation.0),
        pg.wal.frames = integer(frames),
        pg.wal.bytes = Empty,
        pg.wal.first_offset = Empty,
        pg.wal.last_offset = Empty,
        pg.wal.paused = Empty,
        pg.wal.fenced = Empty,
    )
}

/// Build the `wal.chunk` span covering the split of one logical operation
/// batch into monotone `GRW1` frames.
///
/// This span is `TRACE`, not `DEBUG`. Chunking is pure CPU inside the
/// enclosing `pg.commit`, and it matters only when the frame-size limit splits
/// a batch.
#[must_use]
pub fn chunk_span(ops: usize, first_journal_seq: u64) -> Span {
    tracing::trace_span!(
        target: WAL_TARGET,
        "wal.chunk",
        wal.chunk.ops = integer(ops),
        wal.chunk.first_journal_seq = integer(first_journal_seq),
        wal.chunk.frames = Empty,
    )
}

/// Build the `kv.apply` span covering one journaled frame's application to the
/// local read model.
#[must_use]
pub fn apply_span(ops: usize) -> Span {
    tracing::debug_span!(
        target: WAL_TARGET,
        "kv.apply",
        otel.kind = "internal",
        pg.frame.ops = integer(ops),
    )
}

/// Mark `span` failed with OpenTelemetry status `Error`.
///
/// `error.type` carries the low-cardinality discriminator, and the rendered
/// error becomes the status message.
///
/// There is deliberately no success counterpart. See the module docs.
pub fn record_error(span: &Span, error_type: &str, message: &dyn Display) {
    if span.is_disabled() {
        return;
    }
    let message = message.to_string();
    span.record("otel.status_code", "ERROR");
    span.record("error.type", error_type);
    span.record(
        "otel.status_description",
        truncate_on_char_boundary(&message),
    );
}

/// Coerce a count into the widest integer type that OTLP has.
///
/// The conversion saturates and cannot fail. A telemetry attribute must never
/// fail a commit.
#[must_use]
pub fn integer<T: TryInto<i64>>(value: T) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

/// Clamp `message` to [`MAX_STATUS_MESSAGE_BYTES`] without splitting a UTF-8
/// scalar value.
fn truncate_on_char_boundary(message: &str) -> &str {
    if message.len() <= MAX_STATUS_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_STATUS_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    &message[..end]
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn status_messages_are_clamped_on_a_char_boundary() {
        // The leading ASCII byte shifts every two-byte `é` onto an odd offset,
        // so the budget lands *inside* a character and the helper has to step
        // back. Without it — `"é".repeat(n)` alone — the budget is already a
        // boundary and the step-back loop never runs, which is how it went
        // untested despite a test that appeared to cover exactly this.
        let message = format!("a{}", "é".repeat(MAX_STATUS_MESSAGE_BYTES));
        let truncated = truncate_on_char_boundary(&message);

        check!(truncated.len() == MAX_STATUS_MESSAGE_BYTES - 1);
        check!(message.is_char_boundary(truncated.len()));
        check!(truncated.starts_with('a'));
        check!(truncated[1..].chars().all(|c| c == 'é'));
    }

    #[test]
    fn a_budget_already_on_a_boundary_keeps_the_whole_budget() {
        // The complement: two-byte characters with no offset put a boundary
        // exactly on the budget, so nothing is stepped back.
        let message = "é".repeat(MAX_STATUS_MESSAGE_BYTES);
        let truncated = truncate_on_char_boundary(&message);

        check!(truncated.len() == MAX_STATUS_MESSAGE_BYTES);
        check!(truncated.chars().all(|c| c == 'é'));
    }

    #[test]
    fn short_status_messages_are_untouched() {
        check!(truncate_on_char_boundary("fenced") == "fenced");
    }
}

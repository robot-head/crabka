//! Write execution telemetry.

use super::*;

pub(super) fn record_write_outcome(
    span: &tracing::Span,
    outcome: &WriteOutcome,
    ops: &[crabka_pgkv::WriteOp],
    triggers_fired: u64,
) {
    if span.is_disabled() {
        return;
    }
    if let Some(rows) = crate::telemetry::command_tag_row_count(&outcome.tag) {
        span.record("pg.rows_affected", crate::telemetry::integer(rows));
    }
    span.record("pg.write_ops", crate::telemetry::integer(ops.len()));
    span.record("pg.index_ops", crate::telemetry::integer(index_ops(ops)));
    span.record(
        "pg.triggers_fired",
        crate::telemetry::integer(triggers_fired),
    );
    span.record("pg.returning", outcome.returning.is_some());
}

fn index_ops(ops: &[crabka_pgkv::WriteOp]) -> usize {
    ops.iter()
        .filter(|op| {
            let key = match op {
                crabka_pgkv::WriteOp::Put { key, .. }
                | crabka_pgkv::WriteOp::ConditionalPut { key, .. }
                | crabka_pgkv::WriteOp::Delete { key } => key,
            };
            matches!(
                crabka_pgkv::key::classify_key(key),
                crabka_pgkv::key::KeyClass::SecondaryIndex { .. }
            )
        })
        .count()
}

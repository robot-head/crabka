//! The linearizable-read seam. Mirrors the durable-write `Committer` seam: a read
//! confirms it may observe local state before taking its MVCC snapshot. The local
//! impl is a no-op (single-node applied state is authoritative); the replicated
//! impl (`cluster::RaftLinearizer`) performs an openraft ReadIndex check.

use crate::{error::ExecError, telemetry::EXEC_TARGET};

#[async_trait::async_trait]
pub trait Linearizer: Send + Sync {
    /// Confirm this node may serve a linearizable read now. Replicated: confirm
    /// leadership via a quorum heartbeat and block until the local state machine
    /// has applied through the read log id. `Err(NotLeader)` (or `Unavailable`)
    /// if leadership can't be confirmed (deposed/partitioned), so the caller
    /// rejects the read rather than serving stale state.
    ///
    /// # Tracing
    ///
    /// An implementation that can block opens its own span describing *why* it
    /// blocked — the range-0 barrier's `range.barrier` names the sampled offset
    /// it waited for, which a generic wrapper here could not. This module only
    /// traces the gate that cannot block, so that a waterfall distinguishes "the
    /// gate was a no-op" from "the gate was never consulted".
    async fn ensure_readable(&self) -> Result<(), ExecError>;
}

/// Single-node / non-replicated: local applied state is authoritative, so a read
/// is always immediately serveable.
pub struct LocalLinearizer;

#[async_trait::async_trait]
impl Linearizer for LocalLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        let _gate = read_gate_span().entered();
        Ok(())
    }
}

/// Build the `pg.read_gate` span covering a linearizability gate that resolves
/// locally.
///
/// `TRACE`, and deliberately field-free beyond `pg.gate.local`: this gate does
/// no work, so the only thing it can tell an operator is that a read reached the
/// gate and passed it without leaving the node. The spans worth reading are the
/// blocking implementations' own.
#[must_use]
fn read_gate_span() -> tracing::Span {
    tracing::trace_span!(
        target: EXEC_TARGET,
        "pg.read_gate",
        otel.kind = "internal",
        pg.gate.local = true,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::check;
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    use super::*;

    /// Names of every span opened while the closure ran.
    #[derive(Clone, Default)]
    struct Opened(Arc<Mutex<Vec<&'static str>>>);

    impl<S: tracing::Subscriber> Layer<S> for Opened {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::Id,
            _ctx: Context<'_, S>,
        ) {
            self.0
                .lock()
                .expect("opened spans")
                .push(attrs.metadata().name());
        }
    }

    /// The no-op gate must still be visible: a read that reached the gate and a
    /// read that skipped it are indistinguishable otherwise.
    #[tokio::test]
    async fn the_local_gate_opens_a_read_gate_span() {
        let opened = Opened::default();
        let subscriber = tracing_subscriber::registry().with(
            opened
                .clone()
                .with_filter(tracing_subscriber::filter::LevelFilter::TRACE),
        );
        let guard = tracing::subscriber::set_default(subscriber);
        LocalLinearizer
            .ensure_readable()
            .await
            .expect("the local gate always admits");
        drop(guard);

        check!(opened.0.lock().expect("opened spans").as_slice() == ["pg.read_gate"]);
    }
}

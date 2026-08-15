//! The linearizable-read seam. It follows the durable-write `Committer` seam: a
//! read confirms it may observe local state before it takes its MVCC snapshot.
//! The local impl is a no-op, because single-node applied state is
//! authoritative. The replicated impl, `cluster::RaftLinearizer`, does an
//! openraft ReadIndex check.

use crate::{error::ExecError, telemetry::EXEC_TARGET};

#[async_trait::async_trait]
pub trait Linearizer: Send + Sync {
    /// Confirm this node may serve a linearizable read now. A replicated
    /// implementation confirms leadership with a quorum heartbeat, and blocks
    /// until the local state machine has applied through the read log id. It
    /// returns `Err(NotLeader)`, or `Unavailable`, when it cannot confirm
    /// leadership, because the node is deposed or partitioned. The caller then
    /// rejects the read rather than serving stale state.
    ///
    /// # Tracing
    ///
    /// An implementation that can block opens its own span that describes *why*
    /// it blocked. The range-0 barrier's `range.barrier` names the sampled
    /// offset it waited for, which a generic wrapper here could not. This module
    /// only traces the gate that cannot block, so a waterfall tells "the gate
    /// was a no-op" apart from "the gate was never consulted".
    async fn ensure_readable(&self) -> Result<(), ExecError>;
}

/// The single-node, non-replicated gate. Local applied state is authoritative,
/// so this gate can always serve a read immediately.
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
/// The span is `TRACE`, and deliberately field-free beyond `pg.gate.local`. This
/// gate does no work, so the only thing it can tell an operator is that a read
/// reached the gate and passed it without leaving the node. The spans worth
/// reading are the blocking implementations' own.
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

    /// The no-op gate must still be visible. Otherwise nothing tells a read that
    /// reached the gate apart from a read that skipped it.
    #[tokio::test]
    async fn the_local_gate_opens_a_read_gate_span() {
        // Every other test in this binary drives reads through this same gate,
        // and with no subscriber of its own. Whichever thread reaches the
        // callsite first decides its cached interest for the whole process.
        crate::telemetry::install_interest_floor();
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

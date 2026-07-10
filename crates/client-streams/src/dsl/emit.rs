//! `EmitStrategy` — when a windowed aggregation forwards results. Mirrors the JVM
//! `org.apache.kafka.streams.kstream.EmitStrategy`: `on_window_update()` (the
//! default, emit on every update) vs `on_window_close()` (emit each window's final
//! result once stream-time passes its close).
//!
//! Carried as a `Copy` field on the windowed handles and threaded into the
//! aggregate processors at lowering. It changes ONLY runtime forwarding behavior —
//! the lowered topology (node kind, store registration, names) is identical for
//! both strategies, matching the JVM (one `KStreamWindowAggregate` class
//! parameterized by `EmitStrategy`).

/// When a windowed aggregation forwards its results downstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmitStrategy {
    kind: EmitKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EmitKind {
    OnWindowUpdate,
    OnWindowClose,
}

impl EmitStrategy {
    /// Emit on every update (the default).
    #[must_use]
    pub fn on_window_update() -> Self {
        Self {
            kind: EmitKind::OnWindowUpdate,
        }
    }

    /// Emit each window's final result once stream-time passes its close.
    #[must_use]
    pub fn on_window_close() -> Self {
        Self {
            kind: EmitKind::OnWindowClose,
        }
    }

    /// True for the emit-on-update (default) strategy. Aggregate processors guard
    /// their per-update `ctx.forward` with this.
    pub(crate) fn is_on_update(self) -> bool {
        matches!(self.kind, EmitKind::OnWindowUpdate)
    }

    /// True for the emit-on-close strategy.
    pub(crate) fn is_on_close(self) -> bool {
        matches!(self.kind, EmitKind::OnWindowClose)
    }
}

impl Default for EmitStrategy {
    fn default() -> Self {
        Self::on_window_update()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_on_update() {
        let strategy = EmitStrategy::default();
        assert_eq!(
            (strategy.is_on_update(), strategy.is_on_close()),
            (true, false)
        );
    }

    #[test]
    fn on_window_close_is_close() {
        let e = EmitStrategy::on_window_close();
        assert_eq!((e.is_on_update(), e.is_on_close()), (false, true));
    }
}

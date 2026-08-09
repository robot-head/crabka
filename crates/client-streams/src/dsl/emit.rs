//! `EmitStrategy` sets when a windowed aggregation forwards its results. It
//! mirrors the JVM `org.apache.kafka.streams.kstream.EmitStrategy`.
//! `on_window_update()` is the default and emits on every update.
//! `on_window_close()` emits each window's final result once stream-time passes
//! that window's close.
//!
//! The windowed handles carry it as a `Copy` field, and the lowering threads it
//! into the aggregate processors. It changes ONLY the runtime forwarding
//! behavior. The lowered topology, that is the node kind, the store
//! registration, and the names, is identical for both strategies. This matches
//! the JVM, which has one `KStreamWindowAggregate` class parameterized by
//! `EmitStrategy`.

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
    /// Emit on every update. This is the default.
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

    /// True for the emit-on-update strategy, which is the default. The aggregate
    /// processors guard their per-update `ctx.forward` with this method.
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
        assert!(EmitStrategy::default().is_on_update());
        assert!(!EmitStrategy::default().is_on_close());
    }

    #[test]
    fn on_window_close_is_close() {
        let e = EmitStrategy::on_window_close();
        assert!(e.is_on_close());
        assert!(!e.is_on_update());
    }
}

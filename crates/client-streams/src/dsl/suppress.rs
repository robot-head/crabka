//! `Suppressed` + `BufferConfig` — the suppress configuration surface.
//!
//! Slice A implements `until_window_closes(unbounded())` (final results for
//! windowed tables). Slice B adds `with_max_records` (bounded buffer +
//! `shutDownWhenFull`). Slice C adds `until_time_limit` + `emit_early_when_full`
//! overflow toggle + eager `max_records(n)` constructor + `record_cap()`/`is_emit_early()`.
//! Slice D adds the logging toggle.

/// How the suppress buffer is bounded + what happens when it's full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    max_records: Option<usize>,
    /// `false` = shutDownWhenFull (strict, panic); `true` = emitEarlyWhenFull (eager).
    emit_early: bool,
}

impl BufferConfig {
    /// Unbounded, strict (shutDownWhenFull).
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            max_records: None,
            emit_early: false,
        }
    }

    /// Cap at `n` records, EAGER (emit-early-when-full) — the JVM static
    /// `BufferConfig.maxRecords(n)` (the rate-limiter default overflow).
    #[must_use]
    pub fn max_records(n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self {
            max_records: Some(n),
            emit_early: true,
        }
    }

    /// Cap at `n` records, keeping the current overflow mode (strict on the
    /// `unbounded()` path) — the JVM `unbounded().withMaxRecords(n)`.
    #[must_use]
    pub fn with_max_records(self, n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self {
            max_records: Some(n),
            ..self
        }
    }

    /// Evict + emit the oldest buffered record when full (eager).
    #[must_use]
    pub fn emit_early_when_full(self) -> Self {
        Self {
            emit_early: true,
            ..self
        }
    }

    /// Shut the task down when full (strict).
    #[must_use]
    pub fn shut_down_when_full(self) -> Self {
        Self {
            emit_early: false,
            ..self
        }
    }

    pub(crate) fn record_cap(&self) -> Option<usize> {
        self.max_records
    }
    #[allow(dead_code)] // wired in T2 (KTableSuppressProcessor::process)
    pub(crate) fn is_emit_early(&self) -> bool {
        self.emit_early
    }
}

/// A suppression configuration. Slice A: `until_window_closes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Suppressed {
    #[allow(dead_code)] // read by the lowering once Slice B/C branch on the buffer
    pub(crate) buffer: BufferConfig,
}

impl Suppressed {
    /// Emit each window's final result once the window closes
    /// (`stream_time >= window.end + grace`). Requires a windowed `KTable`.
    #[must_use]
    pub fn until_window_closes(buffer: BufferConfig) -> Self {
        Self { buffer }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_config_caps_and_overflow() {
        assert_eq!(BufferConfig::unbounded().record_cap(), None);
        assert!(!BufferConfig::unbounded().is_emit_early()); // strict
        let strict = BufferConfig::unbounded().with_max_records(3);
        assert_eq!(strict.record_cap(), Some(3));
        assert!(!strict.is_emit_early());
        let eager = BufferConfig::max_records(5); // eager
        assert_eq!(eager.record_cap(), Some(5));
        assert!(eager.is_emit_early());
        assert!(!eager.shut_down_when_full().is_emit_early());
        assert!(
            BufferConfig::unbounded()
                .emit_early_when_full()
                .is_emit_early()
        );
    }
}

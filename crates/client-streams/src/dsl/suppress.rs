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
    /// Cap on the total serialized size of the buffer (`key_bytes + value_bytes`
    /// summed). Enforced by the processor against the registered store's
    /// `byte_size()`. The JVM `BufferConfig.maxBytes`.
    max_bytes: Option<usize>,
    /// `false` = shutDownWhenFull (strict, panic); `true` = emitEarlyWhenFull (eager).
    emit_early: bool,
}

impl BufferConfig {
    /// Unbounded, strict (shutDownWhenFull).
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            max_records: None,
            max_bytes: None,
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
            max_bytes: None,
            emit_early: true,
        }
    }

    /// Cap at `n` bytes, EAGER (emit-early-when-full) — the JVM static
    /// `BufferConfig.maxBytes(n)`. The byte unit is the serialized
    /// `key_bytes + value_bytes` summed across buffered entries.
    #[must_use]
    pub fn max_bytes(n: usize) -> Self {
        assert!(n >= 1, "max_bytes must be >= 1");
        Self {
            max_records: None,
            max_bytes: Some(n),
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

    /// Cap at `n` bytes, keeping the current overflow mode — the JVM
    /// `unbounded().withMaxBytes(n)`.
    #[must_use]
    pub fn with_max_bytes(self, n: usize) -> Self {
        assert!(n >= 1, "max_bytes must be >= 1");
        Self {
            max_bytes: Some(n),
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
    pub(crate) fn byte_cap(&self) -> Option<usize> {
        self.max_bytes
    }
    pub(crate) fn is_emit_early(&self) -> bool {
        self.emit_early
    }
}

/// A suppression configuration, parameterized by the table key `K`. Carries a
/// `fn(&K, i64) -> i64` (record key + timestamp → buffer time): window-close reads
/// `window.end`, time-limit reads the record timestamp. Fn pointers are `Copy`, so
/// `Suppressed<K>` is `Copy` without requiring `K: Copy`.
#[derive(Debug)]
pub struct Suppressed<K> {
    pub(crate) buffer: BufferConfig,
    pub(crate) buffer_time: fn(&K, i64) -> i64,
    pub(crate) wait: WaitKind,
    /// Whether the suppress buffer's changelog topic is emitted (default `true`).
    /// `false` (the JVM `withLoggingDisabled()`) keeps the buffer in memory only —
    /// no changelog topic in the wire topology, no restore.
    pub(crate) logging: bool,
}

// All fields are Copy independently of K (fn pointer + plain enums/bool).
impl<K> Clone for Suppressed<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Copy for Suppressed<K> {}

impl<K> Suppressed<K> {
    /// Disable the suppress buffer's changelog (the JVM `withLoggingDisabled()`):
    /// the buffer stays in memory only — no changelog topic, no fault-tolerance.
    #[must_use]
    pub fn with_logging_disabled(self) -> Self {
        Self {
            logging: false,
            ..self
        }
    }

    /// Re-enable the suppress buffer's changelog (the default).
    #[must_use]
    pub fn with_logging_enabled(self) -> Self {
        Self {
            logging: true,
            ..self
        }
    }
}

/// How long to wait before emitting a buffered record.
#[derive(Clone, Copy, Debug)]
pub(crate) enum WaitKind {
    /// Window-close: wait = the upstream window's grace (from the `KTable` handle).
    UpstreamGrace,
    /// Time-limit: wait = the configured duration (ms).
    Fixed(i64),
}

impl<KInner> Suppressed<crate::dsl::windows::Windowed<KInner>> {
    /// Emit each window's final result once it closes (`stream_time >= window.end +
    /// grace`). Requires a windowed `KTable` + a STRICT buffer (shutDownWhenFull).
    #[must_use]
    pub fn until_window_closes(buffer: BufferConfig) -> Self {
        assert!(
            !buffer.is_emit_early(),
            "untilWindowCloses requires a strict (shutDownWhenFull) buffer config"
        );
        Self {
            buffer,
            buffer_time: |k, _ts| k.window.end,
            wait: WaitKind::UpstreamGrace,
            logging: true,
        }
    }
}

impl<K> Suppressed<K> {
    /// Rate-limiter: emit at most one update per key per `wait_ms` (stream-time); a
    /// newer record for a key replaces the buffered one and resets the timer.
    #[must_use]
    pub fn until_time_limit(wait_ms: i64, buffer: BufferConfig) -> Self {
        assert!(wait_ms >= 0, "time limit must be >= 0");
        Self {
            buffer,
            buffer_time: |_k, ts| ts,
            wait: WaitKind::Fixed(wait_ms),
            logging: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn buffer_config_caps_and_overflow() {
        assert_eq!(BufferConfig::unbounded().record_cap(), None);
        assert!(!BufferConfig::unbounded().is_emit_early()); // strict
        let strict = BufferConfig::unbounded().with_max_records(3);
        assert_eq!(strict.record_cap(), Some(3));
        assert!(!strict.is_emit_early());
        let eager = BufferConfig::max_records(5); // eager
        assert_eq!(eager.record_cap(), Some(5));
        check!(eager.is_emit_early());
        check!(!eager.shut_down_when_full().is_emit_early());
        check!(
            BufferConfig::unbounded()
                .emit_early_when_full()
                .is_emit_early()
        );
    }

    #[test]
    fn buffer_config_byte_caps() {
        assert_eq!(BufferConfig::unbounded().byte_cap(), None);
        let eager = BufferConfig::max_bytes(1024); // eager static
        assert_eq!(eager.byte_cap(), Some(1024));
        assert_eq!(eager.record_cap(), None);
        assert!(eager.is_emit_early());
        // strict path keeps shutDownWhenFull
        let strict = BufferConfig::unbounded().with_max_bytes(512);
        assert_eq!(strict.byte_cap(), Some(512));
        assert!(!strict.is_emit_early());
        // records + bytes coexist
        let both = BufferConfig::unbounded()
            .with_max_records(3)
            .with_max_bytes(99);
        assert_eq!(both.record_cap(), Some(3));
        assert_eq!(both.byte_cap(), Some(99));
    }

    #[test]
    fn logging_toggles() {
        use crate::dsl::windows::{Window, Windowed};
        let on = Suppressed::until_window_closes(BufferConfig::unbounded());
        assert!(on.logging); // default on
        let off = on.with_logging_disabled();
        assert!(!off.logging);
        assert!(off.with_logging_enabled().logging);
        // window-close buffer_time still reads window.end after the toggle
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 0, end: 7 },
        };
        assert_eq!((off.buffer_time)(&wk, 1), 7);
    }

    #[test]
    fn suppressed_constructors() {
        use crate::dsl::windows::{Window, Windowed};
        let wc = Suppressed::until_window_closes(BufferConfig::unbounded());
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 0, end: 99 },
        };
        assert_eq!((wc.buffer_time)(&wk, 5), 99); // window.end
        let tl = Suppressed::<String>::until_time_limit(50, BufferConfig::max_records(2));
        assert_eq!((tl.buffer_time)(&"k".to_string(), 5), 5); // record ts
    }
}

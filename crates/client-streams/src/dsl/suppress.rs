//! `Suppressed` and `BufferConfig`: the suppress configuration surface.
//!
//! Slice A implements `until_window_closes(unbounded())`, which gives final
//! results for windowed tables. Slice B adds `with_max_records`, a bounded
//! buffer with `shutDownWhenFull`. Slice C adds `until_time_limit`, the
//! `emit_early_when_full` overflow toggle, the eager `max_records(n)`
//! constructor, `record_cap()`, and `is_emit_early()`. Slice D adds the logging
//! toggle.

use crabka_units::prelude::*;

/// The bound on the suppress buffer and the behaviour when it is full.
///
/// A [`ByteSize`] stores `f64`, so this type is `PartialEq` but not `Eq`. Nothing
/// keys a map or a set on a buffer config, so the weaker bound costs nothing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BufferConfig {
    max_records: Option<usize>,
    /// Cap on the total serialized size of the buffer, the sum of `key_bytes`
    /// and `value_bytes`. The processor enforces it against the registered
    /// store's `byte_size()`. This is the JVM `BufferConfig.maxBytes`.
    max_bytes: Option<ByteSize>,
    /// `false` is shutDownWhenFull, which is strict and panics. `true` is
    /// emitEarlyWhenFull, which is eager.
    emit_early: bool,
}

impl BufferConfig {
    /// Unbounded and strict, that is, shutDownWhenFull.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            max_records: None,
            max_bytes: None,
            emit_early: false,
        }
    }

    /// Cap at `n` records, EAGER, that is, emit-early-when-full.
    ///
    /// This matches the JVM static `BufferConfig.maxRecords(n)`, the
    /// rate-limiter default overflow.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn max_records(n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self {
            max_records: Some(n),
            max_bytes: None,
            emit_early: true,
        }
    }

    /// Cap at `n`, EAGER, that is, emit-early-when-full.
    ///
    /// This matches the JVM static `BufferConfig.maxBytes(n)`. The processor
    /// measures the cap against the serialized `key_bytes` and `value_bytes`
    /// summed across the buffered entries.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn max_bytes(n: ByteSize) -> Self {
        assert!(n >= bytes(1), "max_bytes must be >= 1");
        Self {
            max_records: None,
            max_bytes: Some(n),
            emit_early: true,
        }
    }

    /// Cap at `n` records and keep the current overflow mode.
    ///
    /// The mode is strict on the `unbounded()` path. This matches the JVM
    /// `unbounded().withMaxRecords(n)`.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn with_max_records(self, n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self {
            max_records: Some(n),
            ..self
        }
    }

    /// Cap at `n` and keep the current overflow mode. This matches the JVM
    /// `unbounded().withMaxBytes(n)`.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn with_max_bytes(self, n: ByteSize) -> Self {
        assert!(n >= bytes(1), "max_bytes must be >= 1");
        Self {
            max_bytes: Some(n),
            ..self
        }
    }

    /// Evict and emit the oldest buffered record when the buffer is full. This
    /// is the eager mode.
    #[must_use]
    pub fn emit_early_when_full(self) -> Self {
        Self {
            emit_early: true,
            ..self
        }
    }

    /// Shut the task down when the buffer is full. This is the strict mode.
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
    pub(crate) fn byte_cap(&self) -> Option<ByteSize> {
        self.max_bytes
    }
    pub(crate) fn is_emit_early(&self) -> bool {
        self.emit_early
    }
}

/// A suppression configuration, parameterized by the table key `K`.
///
/// It carries a `fn(&K, i64) -> i64` that maps a record key and a timestamp to a
/// buffer time. Window-close reads `window.end`, and time-limit reads the record
/// timestamp. Fn pointers are `Copy`, so `Suppressed<K>` is `Copy` and needs no
/// `K: Copy` bound.
#[derive(Debug)]
pub struct Suppressed<K> {
    pub(crate) buffer: BufferConfig,
    pub(crate) buffer_time: fn(&K, i64) -> i64,
    pub(crate) wait: WaitKind,
    /// Whether the client emits the suppress buffer's changelog topic. Default:
    /// `true`. `false` matches the JVM `withLoggingDisabled()` and keeps the
    /// buffer in memory only. There is then no changelog topic in the wire
    /// topology and no restore.
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
    /// Disable the suppress buffer's changelog, as the JVM
    /// `withLoggingDisabled()` does. The buffer then stays in memory only, with
    /// no changelog topic and no fault-tolerance.
    #[must_use]
    pub fn with_logging_disabled(self) -> Self {
        Self {
            logging: false,
            ..self
        }
    }

    /// Re-enable the suppress buffer's changelog. This is the default.
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
    /// Window-close. The wait is the upstream window's grace, taken from the
    /// `KTable` handle.
    UpstreamGrace,
    /// Time-limit. The wait is the configured duration.
    Fixed(Time),
}

impl<KInner> Suppressed<crate::dsl::windows::Windowed<KInner>> {
    /// Emit each window's final result once the window closes, that is, once
    /// `stream_time >= window.end + grace`. This needs a windowed `KTable` and a
    /// STRICT buffer, that is, shutDownWhenFull.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
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
    /// Rate-limiter: emit at most one update per key per `wait` in stream time.
    /// A newer record for a key replaces the buffered one and resets the timer.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn until_time_limit(wait: Time, buffer: BufferConfig) -> Self {
        assert!(wait >= Time::ZERO, "time limit must be >= 0");
        Self {
            buffer,
            buffer_time: |_k, ts| ts,
            wait: WaitKind::Fixed(wait),
            logging: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn buffer_config_caps_and_overflow() {
        check!(BufferConfig::unbounded().record_cap() == None);
        check!(!BufferConfig::unbounded().is_emit_early()); // strict
        let strict = BufferConfig::unbounded().with_max_records(3);
        check!(strict.record_cap() == Some(3));
        check!(!strict.is_emit_early());
        let eager = BufferConfig::max_records(5); // eager
        check!(eager.record_cap() == Some(5));
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
        check!(BufferConfig::unbounded().byte_cap() == None);
        let eager = BufferConfig::max_bytes(kibibytes(1)); // eager static
        check!(eager.byte_cap() == Some(kibibytes(1)));
        check!(eager.record_cap() == None);
        check!(eager.is_emit_early());
        // strict path keeps shutDownWhenFull
        let strict = BufferConfig::unbounded().with_max_bytes(bytes(512));
        check!(strict.byte_cap() == Some(bytes(512)));
        check!(!strict.is_emit_early());
        // records + bytes coexist
        let both = BufferConfig::unbounded()
            .with_max_records(3)
            .with_max_bytes(bytes(99));
        check!(both.record_cap() == Some(3));
        check!(both.byte_cap() == Some(bytes(99)));
    }

    #[test]
    fn logging_toggles() {
        use crate::dsl::windows::{Window, Windowed};
        let on = Suppressed::until_window_closes(BufferConfig::unbounded());
        check!(on.logging); // default on
        let off = on.with_logging_disabled();
        check!(!off.logging);
        check!(off.with_logging_enabled().logging);
        // window-close buffer_time still reads window.end after the toggle
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 0, end: 7 },
        };
        check!((off.buffer_time)(&wk, 1) == 7);
    }

    #[test]
    fn suppressed_constructors() {
        use crate::dsl::windows::{Window, Windowed};
        let wc = Suppressed::until_window_closes(BufferConfig::unbounded());
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 0, end: 99 },
        };
        check!((wc.buffer_time)(&wk, 5) == 99); // window.end
        let tl = Suppressed::<String>::until_time_limit(millis(50), BufferConfig::max_records(2));
        check!((tl.buffer_time)(&"k".to_string(), 5) == 5); // record ts
    }
}

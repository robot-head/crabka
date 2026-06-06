//! `Suppressed` + `BufferConfig` — the suppress configuration surface.
//!
//! Slice A implements `until_window_closes(unbounded())` (final results for
//! windowed tables). Slice B adds `with_max_records` (bounded buffer +
//! `shutDownWhenFull`). Slice C adds `until_time_limit`, Slice D adds the
//! logging toggle.

/// How the suppress buffer is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    max_records: Option<usize>,
}

impl BufferConfig {
    /// An unbounded in-memory buffer (no record cap).
    #[must_use]
    pub fn unbounded() -> Self {
        Self { max_records: None }
    }

    /// Cap the buffer at `n` records. Exceeding the cap shuts the task down
    /// (`shutDownWhenFull`). JVM strict path: `unbounded().withMaxRecords(n)`.
    #[must_use]
    pub fn with_max_records(self, n: usize) -> Self {
        assert!(n >= 1, "max_records must be >= 1");
        Self {
            max_records: Some(n),
        }
    }

    /// The record cap, if set (read by the suppress lowering).
    #[allow(dead_code)] // wired in T2 (KTable::suppress)
    pub(crate) fn max_records(&self) -> Option<usize> {
        self.max_records
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
    fn buffer_config_record_cap() {
        assert_eq!(BufferConfig::unbounded().max_records(), None);
        assert_eq!(
            BufferConfig::unbounded().with_max_records(3).max_records(),
            Some(3)
        );
        let s = Suppressed::until_window_closes(BufferConfig::unbounded().with_max_records(5));
        assert_eq!(s.buffer.max_records(), Some(5));
    }
}

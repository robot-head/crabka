//! `Suppressed` + `BufferConfig` — the suppress configuration surface.
//!
//! Slice A implements `until_window_closes(unbounded())` (final results for
//! windowed tables). `BufferConfig` is a marker for the unbounded buffer here;
//! Slice B grows it (`max_records`/`max_bytes` + overflow), Slice C adds
//! `until_time_limit`, Slice D adds the logging toggle.

/// How the suppress buffer is bounded. Slice A: unbounded only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferConfig {
    _private: (),
}

impl BufferConfig {
    /// An unbounded in-memory buffer (no record/byte cap). The only Slice-A config.
    #[must_use]
    pub fn unbounded() -> Self {
        Self { _private: () }
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
    fn constructors() {
        let s = Suppressed::until_window_closes(BufferConfig::unbounded());
        assert_eq!(s.buffer, BufferConfig::unbounded());
    }
}

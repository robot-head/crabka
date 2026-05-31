//! `KraftLog`: the real replicated metadata log behind the 3a `LogView` seam.
//! A thin facade over `crabka_log::Log` that adds high-watermark tracking,
//! committed-read filtering for KIP-595 `Fetch`, and divergence lookup. Wired
//! into the controller (replacing openraft's `log_store`) in slice 3c.

use std::path::Path;

use crabka_log::{Log, LogConfig, RawRead};
use crabka_protocol::records::RecordBatch;

use crate::error::RaftError;
use crate::kraft::types::{LeaderEpoch, LogView};

pub struct KraftLog {
    log: Log,
    /// Highest committed offset (consensus state; crabka-log does not track it).
    hwm: i64,
}

impl KraftLog {
    /// Open or create the metadata log under `dir/@metadata-0`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the log directory cannot be created or the
    /// underlying `crabka_log::Log` fails to open.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, RaftError> {
        let log_dir = dir.as_ref().join("@metadata-0");
        std::fs::create_dir_all(&log_dir).map_err(crabka_log::LogError::Io)?;
        let log = Log::open(&log_dir, LogConfig::default())?;
        let hwm = log.log_start_offset();
        Ok(Self { log, hwm })
    }

    #[must_use]
    pub fn log_start_offset(&self) -> i64 {
        self.log.log_start_offset()
    }
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        self.log.log_end_offset()
    }
    #[must_use]
    pub fn hwm(&self) -> i64 {
        self.hwm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn open_tmp() -> (KraftLog, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open");
        (log, dir)
    }

    #[test]
    fn opens_empty_at_offset_zero() {
        let (log, _dir) = open_tmp();
        assert!(log.log_start_offset() == 0);
        assert!(log.log_end_offset() == 0);
        assert!(log.hwm() == 0);
    }
}

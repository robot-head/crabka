//! On-disk active-execution marker. `{data_dir}/in_flight.json` exists
//! when an execution is in flight; its absence is the "idle" signal on
//! startup. Written atomically; deleted on terminal.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::model::proposal::ProposalStatus;

const FILE_VERSION: u32 = 1;
const FILENAME: &str = "in_flight.json";

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema version {found} not supported (expected {expected})")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("state backend: {0}")]
    Backend(#[from] crate::state_topic::StateTopicError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    ApplyThrottle,
    Submit,
    Wait,
    ClearThrottle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightFile {
    pub version: u32,
    pub proposal_id: String,
    pub phase: Phase,
    pub started_at_ms: i64,
    pub throttle_bytes_per_sec: i64,
    /// Set when transitioning into `ClearThrottle` so a resume-during-clear
    /// knows which terminal status to commit.
    #[serde(default)]
    pub target_terminal_status: Option<ProposalStatus>,
    /// Stamped at the same time as `target_terminal_status` when
    /// `target = Failed`.
    #[serde(default)]
    pub failure_reason: Option<String>,
}

impl InFlightFile {
    #[must_use]
    pub fn new(
        proposal_id: String,
        phase: Phase,
        started_at_ms: i64,
        throttle_bytes_per_sec: i64,
    ) -> Self {
        Self {
            version: FILE_VERSION,
            proposal_id,
            phase,
            started_at_ms,
            throttle_bytes_per_sec,
            target_terminal_status: None,
            failure_reason: None,
        }
    }

    pub fn write(&self, data_dir: &Path) -> Result<(), StateError> {
        let path = path_of(data_dir);
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    pub fn load(data_dir: &Path) -> Result<Option<Self>, StateError> {
        let path = path_of(data_dir);
        match fs::read(&path) {
            Ok(bytes) => {
                let parsed: Self = serde_json::from_slice(&bytes)?;
                if parsed.version != FILE_VERSION {
                    return Err(StateError::UnsupportedVersion {
                        found: parsed.version,
                        expected: FILE_VERSION,
                    });
                }
                Ok(Some(parsed))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn delete(data_dir: &Path) -> Result<(), StateError> {
        let path = path_of(data_dir);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

fn path_of(data_dir: &Path) -> PathBuf {
    data_dir.join(FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn round_trip_write_load() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = InFlightFile::new("p".into(), Phase::Submit, 42, 50_000_000);
        f.write(dir.path()).unwrap();
        let loaded = InFlightFile::load(dir.path()).unwrap().unwrap();
        assert!(
            loaded
                == InFlightFile {
                    version: 1,
                    proposal_id: "p".to_string(),
                    phase: Phase::Submit,
                    started_at_ms: 42,
                    throttle_bytes_per_sec: 50_000_000,
                    target_terminal_status: None,
                    failure_reason: None,
                }
        );

        f.phase = Phase::ClearThrottle;
        f.target_terminal_status = Some(ProposalStatus::Completed);
        f.write(dir.path()).unwrap();
        let loaded2 = InFlightFile::load(dir.path()).unwrap().unwrap();
        assert!(loaded2.phase == Phase::ClearThrottle);
        assert!(loaded2.target_terminal_status == Some(ProposalStatus::Completed));
    }

    #[test]
    fn load_returns_none_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn delete_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        InFlightFile::new("p".into(), Phase::Submit, 0, 0)
            .write(dir.path())
            .unwrap();
        InFlightFile::delete(dir.path()).unwrap();
        InFlightFile::delete(dir.path()).unwrap();
        assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn load_rejects_unsupported_version() {
        let dir = tempfile::tempdir().unwrap();
        let bogus = r#"{"version":999,"proposal_id":"x","phase":"Submit","started_at_ms":0,"throttle_bytes_per_sec":0}"#;
        std::fs::write(dir.path().join(FILENAME), bogus).unwrap();
        let err = InFlightFile::load(dir.path()).unwrap_err();
        assert!(matches!(
            err,
            StateError::UnsupportedVersion {
                found: 999,
                expected: 1
            }
        ));
    }

    #[test]
    fn load_propagates_non_not_found_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(FILENAME)).unwrap();
        let err = InFlightFile::load(dir.path()).unwrap_err();
        assert!(matches!(err, StateError::Io(_)), "got {err:?}");
    }

    #[test]
    fn delete_propagates_non_not_found_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(FILENAME)).unwrap();
        let err = InFlightFile::delete(dir.path()).unwrap_err();
        assert!(matches!(err, StateError::Io(_)), "got {err:?}");
    }
}

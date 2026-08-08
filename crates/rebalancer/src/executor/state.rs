//! On-disk active-execution marker. `{data_dir}/in_flight.json` exists when an
//! execution is in flight, and its absence is the "idle" signal on startup.
//! The executor writes it atomically and deletes it on a terminal status.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use crabka_units::ByteRate;
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

/// `PartialEq` but not `Eq`, because [`InFlightFile::throttle`] is an
/// `f64`-backed quantity, so equality is not reflexive over the whole
/// domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InFlightFile {
    pub version: u32,
    pub proposal_id: String,
    pub phase: Phase,
    pub started_at_ms: i64,
    #[serde(with = "crabka_units::serde_units::numeric::bytes_per_sec_i64")]
    pub throttle: ByteRate,
    /// Set on the transition into `ClearThrottle`, so a resume during the
    /// clear knows which terminal status to commit.
    #[serde(default)]
    pub target_terminal_status: Option<ProposalStatus>,
    /// Stamped at the same time as `target_terminal_status` when
    /// `target = Failed`.
    #[serde(default)]
    pub failure_reason: Option<String>,
}

impl InFlightFile {
    #[must_use]
    pub fn new(proposal_id: String, phase: Phase, started_at_ms: i64, throttle: ByteRate) -> Self {
        Self {
            version: FILE_VERSION,
            proposal_id,
            phase,
            started_at_ms,
            throttle,
            target_terminal_status: None,
            failure_reason: None,
        }
    }

    /// # Errors
    /// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
    pub fn write(&self, data_dir: &Path) -> Result<(), StateError> {
        let path = path_of(data_dir);
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// # Errors
    /// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

    /// # Errors
    /// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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
    use crabka_units::{convert::ByteRateExt as _, mebibytes_per_sec};

    use super::*;

    #[test]
    fn round_trip_write_load() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = InFlightFile::new(
            "p".into(),
            Phase::Submit,
            42,
            ByteRate::from_bytes_per_sec(50_000_000),
        );
        f.write(dir.path()).unwrap();
        let loaded = InFlightFile::load(dir.path()).unwrap().unwrap();
        assert2::assert!(
            loaded
                == InFlightFile {
                    version: 1,
                    proposal_id: "p".to_string(),
                    phase: Phase::Submit,
                    started_at_ms: 42,
                    throttle: ByteRate::from_bytes_per_sec(50_000_000),
                    target_terminal_status: None,
                    failure_reason: None,
                }
        );

        f.phase = Phase::ClearThrottle;
        f.target_terminal_status = Some(ProposalStatus::Completed);
        f.write(dir.path()).unwrap();
        let loaded2 = InFlightFile::load(dir.path()).unwrap().unwrap();
        assert2::assert!(
            (loaded2.phase, loaded2.target_terminal_status)
                == (Phase::ClearThrottle, Some(ProposalStatus::Completed))
        );
    }

    #[test]
    fn throttle_persists_as_a_bytes_per_sec_integer() {
        let dir = tempfile::tempdir().unwrap();
        InFlightFile::new("p".into(), Phase::Wait, 1, mebibytes_per_sec(8))
            .write(dir.path())
            .unwrap();

        let raw: serde_json::Value =
            serde_json::from_slice(&fs::read(path_of(dir.path())).unwrap()).unwrap();

        assert2::assert!(raw["throttle"] == serde_json::json!(8 * 1024 * 1024));
        assert2::assert!(
            InFlightFile::load(dir.path()).unwrap().unwrap().throttle == mebibytes_per_sec(8)
        );
    }

    #[test]
    fn load_returns_none_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert2::assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn delete_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        InFlightFile::new("p".into(), Phase::Submit, 0, ByteRate::ZERO)
            .write(dir.path())
            .unwrap();
        InFlightFile::delete(dir.path()).unwrap();
        InFlightFile::delete(dir.path()).unwrap();
        assert2::assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn load_rejects_unsupported_version() {
        let dir = tempfile::tempdir().unwrap();
        let bogus =
            r#"{"version":999,"proposal_id":"x","phase":"Submit","started_at_ms":0,"throttle":0}"#;
        std::fs::write(dir.path().join(FILENAME), bogus).unwrap();
        let err = InFlightFile::load(dir.path()).unwrap_err();
        assert2::assert!(matches!(
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
        assert2::assert!(matches!(err, StateError::Io(_)));
    }

    #[test]
    fn delete_propagates_non_not_found_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(FILENAME)).unwrap();
        let err = InFlightFile::delete(dir.path()).unwrap_err();
        assert2::assert!(matches!(err, StateError::Io(_)));
    }
}

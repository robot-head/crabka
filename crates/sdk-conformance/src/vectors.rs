//! File-backed conformance vector loader.

use std::{path::Path, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::protocol::{Command, Response};

/// Contract major/minor pair used for vector selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ContractVersion {
    /// Contract major.
    pub major: u16,
    /// Contract minor.
    pub minor: u16,
}

impl ContractVersion {
    /// Create a contract version.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Return true when this version is inside the vector's supported range.
    #[must_use]
    pub fn satisfies(self, vector: &Vector) -> bool {
        if self < vector.since {
            return false;
        }
        if let Some(until) = vector.until {
            return self <= until;
        }
        true
    }
}

/// A contract vector composed of ordered adapter commands and expectations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vector {
    /// Stable vector id.
    pub id: String,
    /// First contract version that must run this vector.
    pub since: ContractVersion,
    /// Last contract version that must run this vector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<ContractVersion>,
    /// Ordered steps for this vector.
    pub steps: Vec<VectorStep>,
}

/// One command/expectation pair inside a vector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorStep {
    /// Human-readable step name used in diagnostics.
    pub name: String,
    /// Command sent to the adapter.
    pub command: Command,
    /// Exact response expected from the adapter.
    pub expect: Response,
}

/// Load all JSON vectors from a directory in deterministic filename order.
pub fn load_vectors(dir: impl AsRef<Path>) -> Result<Vec<Vector>, VectorError> {
    let dir = dir.as_ref();
    let mut entries = std::fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    let mut vectors = Vec::with_capacity(entries.len());
    for path in entries {
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let vector =
            serde_json::from_str::<Vector>(&text).map_err(|source| VectorError::JsonFile {
                path: path.clone(),
                source,
            })?;
        vectors.push(vector);
    }
    Ok(vectors)
}

/// Parse an embedded vector JSON document.
pub fn parse_vector(text: &str) -> Result<Vector, serde_json::Error> {
    serde_json::from_str(text)
}

/// Errors returned while loading vectors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VectorError {
    /// Directory or file read failed.
    #[error("vector io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON in a named vector file failed to decode.
    #[error("vector json in {}: {source}", path.display())]
    JsonFile {
        /// File path.
        path: std::path::PathBuf,
        /// JSON error.
        source: serde_json::Error,
    },
}

impl FromStr for Vector {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

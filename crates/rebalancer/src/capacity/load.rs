//! YAML loader for the broker-capacity file. Schema versioned at 1.

use std::{fs, path::Path};

use serde::Deserialize;

use super::BrokerCapacities;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum CapacityError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("schema version {found} not supported (expected {expected})")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("negative cpu_cores ({0}) for broker {1}")]
    NegativeCpu(f64, i32),
    #[error("cpu_cores ({0}) for broker {1} is not finite")]
    NonFiniteCpu(f64, i32),
}

#[derive(Debug, Deserialize)]
struct OnDisk {
    version: u32,
    #[serde(default)]
    brokers: std::collections::HashMap<i32, super::BrokerCapacity>,
}

pub fn load_from_path(path: &Path) -> Result<BrokerCapacities, CapacityError> {
    let bytes = fs::read(path)?;
    let parsed: OnDisk = serde_yaml::from_slice(&bytes)?;
    if parsed.version != SCHEMA_VERSION {
        return Err(CapacityError::UnsupportedVersion {
            found: parsed.version,
            expected: SCHEMA_VERSION,
        });
    }
    // Reject obvious operator typos: non-finite or negative cpu_cores.
    for (broker, cap) in &parsed.brokers {
        if let Some(cpu) = cap.cpu_cores {
            if cpu.is_nan() || cpu.is_infinite() {
                return Err(CapacityError::NonFiniteCpu(cpu, *broker));
            }
            if cpu < 0.0 {
                return Err(CapacityError::NegativeCpu(cpu, *broker));
            }
        }
    }
    Ok(BrokerCapacities {
        by_broker: parsed.brokers,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use assert2::assert;

    use super::*;
    use crate::capacity::BrokerCapacity;

    fn write_yaml(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        f.write_all(content.as_bytes()).expect("write");
        f
    }

    #[test]
    fn load_round_trips_full_file() {
        let f = write_yaml(
            r"
version: 1
brokers:
  1:
    max_replicas: 4096
    disk_bytes: 1099511627776
    network_in_bytes_per_sec: 125000000
    network_out_bytes_per_sec: 125000000
    cpu_cores: 8.0
  2:
    max_replicas: 2048
",
        );
        let c = load_from_path(f.path()).expect("load");
        let b1 = c.for_broker(1).expect("broker 1");
        assert!(
            *b1 == BrokerCapacity {
                max_replicas: Some(4096),
                disk_bytes: Some(1_099_511_627_776),
                network_in_bytes_per_sec: Some(125_000_000),
                network_out_bytes_per_sec: Some(125_000_000),
                cpu_cores: Some(8.0),
            }
        );
        let b2 = c.for_broker(2).expect("broker 2");
        assert!(
            *b2 == BrokerCapacity {
                max_replicas: Some(2048),
                disk_bytes: None,
                network_in_bytes_per_sec: None,
                network_out_bytes_per_sec: None,
                cpu_cores: None,
            }
        );
        assert!(c.for_broker(3).is_none(), "broker 3 unconstrained");
    }

    #[test]
    fn load_errors_on_missing_file() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let p = dir.path().join("nonexistent");
        let err = load_from_path(&p).expect_err("missing file");
        assert!(matches!(err, CapacityError::Io(_)), "got {err:?}");
    }

    #[test]
    fn load_omits_missing_fields_as_none() {
        let f = write_yaml(
            r"
version: 1
brokers:
  1:
    max_replicas: 100
",
        );
        let c = load_from_path(f.path()).expect("load");
        let b1 = c.for_broker(1).expect("broker 1");
        assert!(
            *b1 == BrokerCapacity {
                max_replicas: Some(100),
                disk_bytes: None,
                network_in_bytes_per_sec: None,
                network_out_bytes_per_sec: None,
                cpu_cores: None,
            }
        );
    }

    #[test]
    fn load_rejects_unsupported_version() {
        let f = write_yaml(
            r"
version: 999
brokers: {}
",
        );
        let err = load_from_path(f.path()).expect_err("bad version");
        assert!(matches!(
            err,
            CapacityError::UnsupportedVersion {
                found: 999,
                expected: 1
            }
        ));
    }

    #[test]
    fn load_rejects_negative_cpu_cores() {
        let f = write_yaml(
            r"
version: 1
brokers:
  5:
    cpu_cores: -1.0
",
        );
        let err = load_from_path(f.path()).expect_err("negative cpu");
        assert!(matches!(err, CapacityError::NegativeCpu(_, 5)));
    }

    #[test]
    fn load_accepts_zero_cpu_cores() {
        let f = write_yaml(
            r"
version: 1
brokers:
  5:
    cpu_cores: 0.0
",
        );
        let c = load_from_path(f.path()).expect("zero cpu is finite and non-negative");
        let b = c.for_broker(5).expect("broker 5");
        assert!(b.cpu_cores == Some(0.0));
    }

    #[test]
    fn load_rejects_nan_cpu_cores() {
        let f = write_yaml(
            r"
version: 1
brokers:
  5:
    cpu_cores: .nan
",
        );
        let err = load_from_path(f.path()).expect_err("nan cpu");
        assert!(matches!(err, CapacityError::NonFiniteCpu(c, 5) if c.is_nan()));
    }

    #[test]
    fn load_rejects_infinity_cpu_cores() {
        let f = write_yaml(
            r"
version: 1
brokers:
  5:
    cpu_cores: .inf
",
        );
        let err = load_from_path(f.path()).expect_err("infinity cpu");
        assert!(matches!(err, CapacityError::NonFiniteCpu(c, 5) if c.is_infinite()));
    }
}

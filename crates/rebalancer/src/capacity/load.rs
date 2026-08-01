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

/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

    use crabka_units::prelude::*;

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
        assert2::assert!(
            *b1 == BrokerCapacity {
                max_replicas: Some(4096),
                disk_bytes: Some(gibibytes(1024)),
                network_in_bytes_per_sec: Some(bytes_per_sec(125_000_000)),
                network_out_bytes_per_sec: Some(bytes_per_sec(125_000_000)),
                cpu_cores: Some(8.0),
            }
        );
        let b2 = c.for_broker(2).expect("broker 2");
        assert2::assert!(
            *b2 == BrokerCapacity {
                max_replicas: Some(2048),
                disk_bytes: None,
                network_in_bytes_per_sec: None,
                network_out_bytes_per_sec: None,
                cpu_cores: None,
            }
        );
        assert2::assert!(c.for_broker(3).is_none());
    }

    /// The YAML keys carry plain byte counts and the loader is the seam that
    /// turns them into quantities, so a value that is not a round power of two
    /// has to survive exactly.
    #[test]
    fn plain_byte_counts_parse_into_quantities_exactly() {
        let f = write_yaml(
            r"
version: 1
brokers:
  7:
    disk_bytes: 1500
    network_in_bytes_per_sec: 1
    network_out_bytes_per_sec: 999999
",
        );
        let c = load_from_path(f.path()).expect("load");
        let b = c.for_broker(7).expect("broker 7");
        assert2::check!(b.disk_bytes == Some(bytes(1500)));
        assert2::check!(b.network_in_bytes_per_sec == Some(bytes_per_sec(1)));
        assert2::check!(b.network_out_bytes_per_sec == Some(bytes_per_sec(999_999)));
    }

    #[test]
    fn load_errors_on_missing_file() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let p = dir.path().join("nonexistent");
        let err = load_from_path(&p).expect_err("missing file");
        assert2::assert!(matches!(err, CapacityError::Io(_)));
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
        assert2::assert!(
            *b1 == BrokerCapacity {
                max_replicas: Some(100),
                disk_bytes: None,
                network_in_bytes_per_sec: None,
                network_out_bytes_per_sec: None,
                cpu_cores: None,
            }
        );
    }

    /// The YAML keys stay bare integers counting bytes and bytes/sec, so the
    /// loader is the conversion seam: a round value must land on the
    /// equivalent binary-unit quantity, an unround one must survive exactly,
    /// and unwrapping must give the same integers back.
    #[test]
    fn load_parses_raw_integers_into_quantities() {
        let f = write_yaml(
            r"
version: 1
brokers:
  7:
    disk_bytes: 1073741824
    network_in_bytes_per_sec: 10485760
    network_out_bytes_per_sec: 1500
",
        );
        let c = load_from_path(f.path()).expect("load");
        let b = c.for_broker(7).expect("broker 7");
        assert2::check!(b.disk_bytes == Some(gibibytes(1)));
        assert2::check!(b.network_in_bytes_per_sec == Some(mebibytes_per_sec(10)));
        assert2::check!(b.network_out_bytes_per_sec == Some(bytes_per_sec(1500)));
        assert2::check!(b.disk_bytes.map(ByteSizeExt::bytes_u64) == Some(1_073_741_824));
        assert2::check!(
            b.network_in_bytes_per_sec
                .map(ByteRateExt::bytes_per_sec_i64)
                == Some(10_485_760)
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
        assert2::assert!(matches!(
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
        assert2::assert!(matches!(err, CapacityError::NegativeCpu(_, 5)));
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
        assert2::assert!(b.cpu_cores == Some(0.0));
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
        assert2::assert!(matches!(err, CapacityError::NonFiniteCpu(c, 5) if c.is_nan()));
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
        assert2::assert!(matches!(err, CapacityError::NonFiniteCpu(c, 5) if c.is_infinite()));
    }
}

//! Metrics WAL topic record shared by ingest, compaction, and query.

use bytes::Bytes;
use crabka_blockstore::Labels;
use serde::{Deserialize, Serialize};

use crate::NativeHistogram;

/// The metrics WAL topic name.
pub const WAL_TOPIC: &str = "__crabka_metrics_wal";

/// WAL codec errors.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal encode failed: {0}")]
    Encode(String),

    #[error("wal decode failed: {0}")]
    Decode(String),
}

/// One sample's WAL payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SamplePayload {
    Float {
        timestamp_ms: i64,
        value: f64,
        start_timestamp_ms: Option<i64>,
    },
    Hist {
        timestamp_ms: i64,
        hist: NativeHistogram,
    },
    Metadata {
        metric_family_name: String,
        metric_type: String,
        help: String,
        unit: String,
    },
    Exemplars,
}

impl SamplePayload {
    #[must_use]
    pub fn timestamp_ms(&self) -> Option<i64> {
        match self {
            Self::Float { timestamp_ms, .. } | Self::Hist { timestamp_ms, .. } => {
                Some(*timestamp_ms)
            }
            Self::Metadata { .. } | Self::Exemplars => None,
        }
    }
}

/// An exemplar carried alongside a sample.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalExemplar {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ms: i64,
}

/// A single metrics WAL record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    pub tenant: String,
    pub labels: Vec<(String, String)>,
    pub payload: SamplePayload,
    pub exemplars: Vec<WalExemplar>,
}

impl WalRecord {
    /// Encodes with `serde-wincode`, which matches the codebase
    /// metadata-record codec.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn encode(&self) -> Result<Vec<u8>, WalError> {
        <serde_wincode::SerdeCompat<WalRecord> as wincode::Serialize>::serialize(self)
            .map_err(|error| WalError::Encode(error.to_string()))
    }

    /// Decodes a [`WalRecord`] from its `serde-wincode` bytes.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, WalError> {
        <serde_wincode::SerdeCompat<WalRecord> as wincode::Deserialize>::deserialize(bytes)
            .map_err(|error| WalError::Decode(error.to_string()))
    }

    /// Series fingerprint from the blockstore's order-independent [`Labels`]
    /// hash.
    #[must_use]
    pub fn series_fingerprint(&self) -> u64 {
        self.labels().fingerprint()
    }

    /// Builds the blockstore label set for this record.
    #[must_use]
    pub fn labels(&self) -> Labels {
        self.labels.iter().cloned().collect()
    }
}

/// Producer key for a tenant and fingerprint pair. The Kafka producer hashes
/// this byte key to choose a partition, which keeps the per-series order.
#[must_use]
pub fn partition_key(tenant: &str, fp: u64) -> Bytes {
    let mut bytes = Vec::with_capacity(tenant.len() + 1 + std::mem::size_of::<u64>());
    bytes.extend_from_slice(tenant.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&fp.to_be_bytes());
    Bytes::from(bytes)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{BucketSpan, ResetHint};

    fn hist() -> NativeHistogram {
        NativeHistogram {
            schema: 2,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-128,
            zero_count: 0.0,
            count: 7.0,
            sum: 3.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![4.0, 3.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    #[test]
    fn float_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![
                ("__name__".into(), "up".into()),
                ("job".into(), "api".into()),
            ],
            payload: SamplePayload::Float {
                timestamp_ms: 100,
                value: 1.5,
                start_timestamp_ms: Some(50),
            },
            exemplars: Vec::new(),
        };

        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();

        assert!(back == rec);
    }

    #[test]
    fn hist_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![("__name__".into(), "latency".into())],
            payload: SamplePayload::Hist {
                timestamp_ms: 200,
                hist: hist(),
            },
            exemplars: vec![WalExemplar {
                labels: vec![("trace_id".into(), "abc".into())],
                value: 0.9,
                timestamp_ms: 200,
            }],
        };

        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();

        assert!(back == rec);
    }

    #[test]
    fn exemplar_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![("__name__".into(), "requests_total".into())],
            payload: SamplePayload::Exemplars,
            exemplars: vec![WalExemplar {
                labels: vec![("trace_id".into(), "abc".into())],
                value: 0.9,
                timestamp_ms: 200,
            }],
        };

        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();

        assert!(back == rec);
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = WalRecord {
            tenant: "t".into(),
            labels: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            payload: SamplePayload::Float {
                timestamp_ms: 0,
                value: 0.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let mut b = a.clone();
        b.labels = vec![("b".into(), "2".into()), ("a".into(), "1".into())];

        assert!(a.series_fingerprint() == b.series_fingerprint());
    }

    #[test]
    fn partition_key_is_stable() {
        let k1 = partition_key("t", 42);
        let k2 = partition_key("t", 42);
        let k3 = partition_key("t", 43);

        assert!(k1 == k2);
        assert!(k1 != k3);
    }
}

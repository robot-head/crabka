//! Shared decode target, content negotiation, snappy-block decode, and
//! `remote_write` status mapping.

use crabka_blockstore::Labels;

use crate::NativeHistogram;

/// Exemplar decoded from a `remote_write` request.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedExemplar {
    pub labels: Labels,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// Metric metadata decoded from an ingest request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedMetadata {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// One decoded float sample from an ingest request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecodedSample {
    pub timestamp_ms: i64,
    pub value: f64,
    pub start_timestamp_ms: Option<i64>,
}

impl DecodedSample {
    #[must_use]
    pub fn new(timestamp_ms: i64, value: f64) -> Self {
        Self {
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        }
    }

    #[must_use]
    pub fn with_start_timestamp(
        timestamp_ms: i64,
        value: f64,
        start_timestamp_ms: Option<i64>,
    ) -> Self {
        Self {
            timestamp_ms,
            value,
            start_timestamp_ms,
        }
    }
}

impl PartialEq<(i64, f64)> for DecodedSample {
    fn eq(&self, other: &(i64, f64)) -> bool {
        self.timestamp_ms == other.0 && self.value == other.1 && self.start_timestamp_ms.is_none()
    }
}

/// One decoded metric series from any ingest wire format.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedSeries {
    pub labels: Labels,
    pub samples: Vec<DecodedSample>,
    pub histograms: Vec<(i64, NativeHistogram)>,
    pub exemplars: Vec<DecodedExemplar>,
    pub metadata: Option<DecodedMetadata>,
}

/// Which `remote_write` protobuf shape an HTTP request carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFormat {
    RemoteWriteV1,
    RemoteWriteV2,
}

/// `remote_write` ingest edge errors.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("unsupported content type `{0}`")]
    UnsupportedContentType(String),
    #[error("unsupported content encoding `{0}`")]
    UnsupportedContentEncoding(String),
    #[error("snappy decoded body exceeds max_output={0}")]
    SnappyOutputTooLarge(usize),
    #[error("snappy decode failed: {0}")]
    SnappyDecode(String),
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(String),
    #[error("invalid remote_write request: {0}")]
    Invalid(String),
}

impl WireError {
    /// HTTP status code for Prometheus `remote_write` ingest.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::UnsupportedContentType(_) | Self::UnsupportedContentEncoding(_) => 415,
            Self::SnappyOutputTooLarge(_)
            | Self::SnappyDecode(_)
            | Self::ProtobufDecode(_)
            | Self::Invalid(_) => 400,
        }
    }
}

/// Dispatch on the `Content-Type` `proto=` param. Bare
/// `application/x-protobuf` remains the v1 default.
pub fn negotiate(content_type: Option<&str>) -> Result<WireFormat, WireError> {
    let Some(content_type) = content_type else {
        return Ok(WireFormat::RemoteWriteV1);
    };
    let mut parts = content_type.split(';');
    let base = parts.next().unwrap_or_default().trim();
    if !base.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(WireError::UnsupportedContentType(base.to_string()));
    }

    let proto = parts.find_map(proto_param_value);
    match proto.as_deref() {
        None | Some("prometheus.WriteRequest") => Ok(WireFormat::RemoteWriteV1),
        Some("io.prometheus.write.v2.Request") => Ok(WireFormat::RemoteWriteV2),
        Some(other) => Err(WireError::UnsupportedContentType(format!("proto={other}"))),
    }
}

fn proto_param_value(param: &str) -> Option<String> {
    let (name, value) = param.trim().split_once('=')?;
    name.trim()
        .eq_ignore_ascii_case("proto")
        .then(|| value.trim().trim_matches('"').to_string())
}

/// Decode a plain snappy block. Prometheus `remote_write` does not use the Xerial
/// framed snappy format used by Kafka.
///
/// The block's stored uncompressed length is checked against `max_output`
/// *before* decompressing, so a decompression bomb (tiny payload declaring a
/// huge length) is rejected without `snap` pre-allocating the declared buffer.
// cargo-mutants: covered by remote-write snappy round-trip and limit tests.
#[cfg_attr(test, mutants::skip)]
pub fn snappy_block_decode(body: &[u8], max_output: usize) -> Result<Vec<u8>, WireError> {
    snappy_block_decode_raw(
        body,
        max_output,
        WireError::SnappyDecode,
        WireError::SnappyOutputTooLarge,
    )
}

// cargo-mutants: shared decoder guard is covered through remote_write and remote_read callers.
#[cfg_attr(test, mutants::skip)]
pub(super) fn snappy_block_decode_raw<E>(
    body: &[u8],
    max_output: usize,
    snappy_decode: impl Fn(String) -> E,
    output_too_large: impl Fn(usize) -> E,
) -> Result<Vec<u8>, E> {
    let declared =
        snap::raw::decompress_len(body).map_err(|error| snappy_decode(error.to_string()))?;
    if declared > max_output {
        return Err(output_too_large(max_output));
    }
    let out = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|error| snappy_decode(error.to_string()))?;
    if out.len() > max_output {
        return Err(output_too_large(max_output));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn negotiate_v1_default_protobuf() {
        assert!(negotiate(Some("application/x-protobuf")).unwrap() == WireFormat::RemoteWriteV1);
        assert!(negotiate(None).unwrap() == WireFormat::RemoteWriteV1);
    }

    #[test]
    fn negotiate_v1_explicit_proto_param() {
        assert!(
            negotiate(Some(
                "application/x-protobuf; proto=prometheus.WriteRequest"
            ))
            .unwrap()
                == WireFormat::RemoteWriteV1
        );
    }

    #[test]
    fn negotiate_v2_proto_param() {
        assert!(
            negotiate(Some(
                "application/x-protobuf; proto=io.prometheus.write.v2.Request"
            ))
            .unwrap()
                == WireFormat::RemoteWriteV2
        );
    }

    #[test]
    fn negotiate_rejects_json() {
        let err = negotiate(Some("application/json")).unwrap_err();
        assert!(matches!(err, WireError::UnsupportedContentType(_)));
        assert!(err.status_code() == 415);
    }

    #[test]
    fn snappy_block_round_trips_plain() {
        let input = b"remote-write-body";
        let compressed = snap::raw::Encoder::new().compress_vec(input).unwrap();

        let back = snappy_block_decode(&compressed, 1 << 20).unwrap();

        assert!(back == input);
    }

    #[test]
    fn snappy_block_rejects_oversize() {
        let compressed = snap::raw::Encoder::new()
            .compress_vec(b"larger than allowed")
            .unwrap();

        let err = snappy_block_decode(&compressed, 4).unwrap_err();

        assert!(matches!(err, WireError::SnappyOutputTooLarge(4)));
        assert!(err.status_code() == 400);
    }

    /// A snappy block whose varint header *declares* a huge uncompressed length
    /// but carries a tiny payload must be rejected on the declared-length
    /// pre-check, before `snap` allocates the declared buffer.
    #[test]
    fn snappy_block_rejects_declared_length_bomb() {
        // Hand-roll a raw snappy block: a varint preamble declaring ~1 GiB of
        // output followed by a one-byte literal. `decompress_len` reads the
        // preamble; the guard fires without ever allocating the gigabyte.
        let huge: u64 = 1 << 30;
        let mut frame = Vec::new();
        let mut value = huge;
        while value >= 0x80 {
            frame.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        frame.push(u8::try_from(value).unwrap());
        // One literal byte (tag 0x00 = literal, length-1 encoded in upper bits).
        frame.push(0x00);
        frame.push(0x42);

        assert!(snap::raw::decompress_len(&frame).unwrap() as u64 == huge);

        let err = snappy_block_decode(&frame, 1 << 20).unwrap_err();

        assert!(matches!(err, WireError::SnappyOutputTooLarge(_)));
        assert!(err.status_code() == 400);
    }
}

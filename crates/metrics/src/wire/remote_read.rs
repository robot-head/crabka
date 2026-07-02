//! Prometheus `remote_read` protobuf helpers.
//!
//! This module implements the SAMPLES response path for the v1 read format.
//! `STREAMED_XOR_CHUNKS` is intentionally not advertised or encoded here.

use crabka_blockstore::{LabelMatcher, Labels, MatchOp};
use prost::Message;
use thiserror::Error;

use crate::wire::pb::v1;

/// Default decompressed-body cap for `remote_read` requests when a caller does
/// not supply its own. Mirrors the distributor's ingest default so a single
/// `read` request cannot decompress to an unbounded allocation.
pub const DEFAULT_MAX_READ_DECOMPRESSED: usize = 32 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum RemoteReadError {
    #[error("snappy decode failed: {0}")]
    SnappyDecode(String),
    #[error("snappy decoded body exceeds max_output={0}")]
    SnappyOutputTooLarge(usize),
    #[error("snappy encode failed: {0}")]
    SnappyEncode(String),
    #[error("protobuf decode failed: {0}")]
    Decode(String),
    #[error("protobuf encode failed: {0}")]
    Encode(String),
    #[error("unsupported remote_read matcher type {0}")]
    UnsupportedMatcher(i32),
}

pub fn decode_read_request(
    snappy_body: &[u8],
    max_output: usize,
) -> Result<v1::ReadRequest, RemoteReadError> {
    // Reject a decompression bomb on the block's *declared* uncompressed length
    // before `snap` pre-allocates the declared buffer.
    let declared = snap::raw::decompress_len(snappy_body)
        .map_err(|error| RemoteReadError::SnappyDecode(error.to_string()))?;
    if declared > max_output {
        return Err(RemoteReadError::SnappyOutputTooLarge(max_output));
    }
    let raw = snap::raw::Decoder::new()
        .decompress_vec(snappy_body)
        .map_err(|error| RemoteReadError::SnappyDecode(error.to_string()))?;
    if raw.len() > max_output {
        return Err(RemoteReadError::SnappyOutputTooLarge(max_output));
    }
    v1::ReadRequest::decode(raw.as_slice())
        .map_err(|error| RemoteReadError::Decode(error.to_string()))
}

pub fn encode_read_response(response: &v1::ReadResponse) -> Result<Vec<u8>, RemoteReadError> {
    let mut raw = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut raw)
        .map_err(|error| RemoteReadError::Encode(error.to_string()))?;
    snap::raw::Encoder::new()
        .compress_vec(&raw)
        .map_err(|error| RemoteReadError::SnappyEncode(error.to_string()))
}

pub fn matchers_to_selectors(
    query: &v1::Query,
) -> Result<(Vec<LabelMatcher>, i64, i64), RemoteReadError> {
    let selectors = query
        .matchers
        .iter()
        .map(|matcher| {
            let op = match matcher.r#type {
                0 => MatchOp::Eq,
                1 => MatchOp::Neq,
                2 => MatchOp::Re,
                3 => MatchOp::Nre,
                other => return Err(RemoteReadError::UnsupportedMatcher(other)),
            };
            Ok(LabelMatcher::new(&matcher.name, op, &matcher.value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((selectors, query.start_timestamp_ms, query.end_timestamp_ms))
}

#[must_use]
pub fn series_to_timeseries(series: Vec<(Labels, Vec<(i64, f64)>)>) -> v1::QueryResult {
    let mut timeseries = series
        .into_iter()
        .map(|(labels, samples)| {
            let mut labels = labels
                .iter()
                .map(|(name, value)| v1::Label {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            labels.sort_by(|left, right| left.name.cmp(&right.name));

            let mut samples = samples
                .into_iter()
                .map(|(timestamp, value)| v1::Sample { value, timestamp })
                .collect::<Vec<_>>();
            samples.sort_by_key(|sample| sample.timestamp);

            v1::TimeSeries {
                labels,
                samples,
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    timeseries.sort_by(|left, right| {
        left.labels
            .iter()
            .map(|label| (&label.name, &label.value))
            .cmp(right.labels.iter().map(|label| (&label.name, &label.value)))
    });
    v1::QueryResult { timeseries }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_blockstore::{Labels, MatchOp};
    use prost::Message;

    use super::*;
    use crate::wire::pb::v1::{self, label_matcher};

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn read_request_snappy_round_trips() {
        let req = v1::ReadRequest {
            queries: vec![v1::Query {
                start_timestamp_ms: 1000,
                end_timestamp_ms: 2000,
                matchers: vec![v1::LabelMatcher {
                    r#type: label_matcher::Type::Eq as i32,
                    name: "__name__".into(),
                    value: "http_requests_total".into(),
                }],
                hints: None,
            }],
            accepted_response_types: Vec::new(),
        };

        let back =
            decode_read_request(&snappy(&req.encode_to_vec()), DEFAULT_MAX_READ_DECOMPRESSED)
                .unwrap();

        assert!(back.queries.len() == 1);
        let (selectors, start, end) = matchers_to_selectors(&back.queries[0]).unwrap();
        check!(start == 1000);
        check!(end == 2000);
        check!(selectors[0].name == "__name__");
        check!(selectors[0].op == MatchOp::Eq);
        check!(selectors[0].value == "http_requests_total");
    }

    /// A `remote_read` snappy block declaring a huge uncompressed length but
    /// carrying a tiny payload must be rejected on the declared-length
    /// pre-check, before `snap` allocates the declared buffer.
    #[test]
    fn read_request_rejects_declared_length_bomb() {
        // Hand-roll a raw snappy block: a varint preamble declaring ~1 GiB of
        // output followed by a one-byte literal.
        let huge: u64 = 1 << 30;
        let mut frame = Vec::new();
        let mut value = huge;
        while value >= 0x80 {
            frame.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        frame.push(u8::try_from(value).unwrap());
        frame.push(0x00);
        frame.push(0x42);

        assert!(snap::raw::decompress_len(&frame).unwrap() as u64 == huge);

        let err = decode_read_request(&frame, 1 << 20).unwrap_err();

        assert!(matches!(err, RemoteReadError::SnappyOutputTooLarge(_)));
    }

    #[test]
    fn samples_response_is_sorted() {
        let mut labels = Labels::new();
        labels.insert("job", "api");
        labels.insert("__name__", "x");
        let result = series_to_timeseries(vec![(labels, vec![(2_i64, 2.0_f64), (1, 1.0)])]);

        let ts = &result.timeseries[0];
        check!(ts.labels[0].name == "__name__");
        check!(ts.labels[1].name == "job");
        check!(ts.samples[0].timestamp == 1);
        check!(ts.samples[1].timestamp == 2);
    }

    #[test]
    fn response_encodes_as_snappy_protobuf() {
        let response = v1::ReadResponse {
            results: vec![v1::QueryResult {
                timeseries: vec![v1::TimeSeries {
                    samples: vec![v1::Sample {
                        timestamp: 42,
                        value: 7.0,
                    }],
                    ..Default::default()
                }],
            }],
        };

        let encoded = encode_read_response(&response).unwrap();
        let raw = snap::raw::Decoder::new().decompress_vec(&encoded).unwrap();
        let decoded = v1::ReadResponse::decode(raw.as_slice()).unwrap();

        assert!(decoded.results[0].timeseries[0].samples[0].timestamp == 42);
    }
}

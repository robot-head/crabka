//! Connect `push.v1.PusherService/Push` decode: each `RawSample.raw_profile` is
//! a gzipped pprof; gunzip -> `PprofProfile::decode` -> one `RawProfile` per sample.

use std::io::Read;

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::{error::ProfilesError, ingest::RawProfile, wire::pb};

/// Gunzip a gzipped body with an output-size cap.
pub fn gunzip(body: &[u8], max_output: usize) -> Result<Vec<u8>, ProfilesError> {
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut out = Vec::new();
    let mut buf = [0_u8; 8192];

    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| ProfilesError::Gunzip(e.to_string()))?;
        if n == 0 {
            break;
        }
        if out.len() + n > max_output {
            return Err(ProfilesError::TooLarge { limit: max_output });
        }
        out.extend_from_slice(&buf[..n]);
    }

    Ok(out)
}

/// Decode a `push.v1` `PushRequest` into per-(series, sample) `RawProfile`s.
pub fn decode_push(
    req: &pb::push::v1::PushRequest,
    max_decompressed: usize,
) -> Result<Vec<RawProfile>, ProfilesError> {
    let mut out = Vec::new();
    for series in &req.series {
        let mut labels = Labels::new();
        for label in &series.labels {
            labels.insert(label.name.clone(), label.value.clone());
        }

        for sample in &series.samples {
            let raw = gunzip(&sample.raw_profile, max_decompressed)?;
            let profile = PprofProfile::decode(&raw)?;
            let mut labels = labels.clone();
            if !sample.id.is_empty() {
                labels.insert("__profile_id__", sample.id.clone());
            }
            out.push(RawProfile {
                labels,
                profile,
                delta: false,
                sample_timestamps_ns: Vec::new(),
                sample_span_ids: Vec::new(),
                sample_trace_ids: Vec::new(),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use assert2::assert;

    use super::*;
    use crate::wire::pb;

    fn gzip(raw: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(raw).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn gunzip_round_trips_and_caps() {
        let raw = b"the quick brown fox";
        let gz = gzip(raw);
        assert!(gunzip(&gz, 1 << 20).unwrap() == raw);
        assert!(gunzip(&gz, 4).is_err());
    }

    #[test]
    fn decode_push_gunzips_and_parses_pprof() {
        let pprof_bytes = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let req = pb::push::v1::PushRequest {
            series: vec![pb::push::v1::RawProfileSeries {
                labels: vec![
                    pb::types::v1::LabelPair {
                        name: "__name__".into(),
                        value: "process_cpu".into(),
                    },
                    pb::types::v1::LabelPair {
                        name: "service_name".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![pb::push::v1::RawSample {
                    raw_profile: gzip(&pprof_bytes),
                    id: "s1".into(),
                }],
                annotations: Vec::new(),
            }],
        };

        let out = decode_push(&req, 1 << 20).unwrap();

        assert!(out.len() == 1);
        assert!(out[0].labels.get("__name__") == Some("process_cpu"));
    }

    #[test]
    fn decode_push_promotes_sample_id_to_profile_id_label() {
        let pprof_bytes = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let req = pb::push::v1::PushRequest {
            series: vec![pb::push::v1::RawProfileSeries {
                labels: vec![
                    pb::types::v1::LabelPair {
                        name: "__name__".into(),
                        value: "process_cpu".into(),
                    },
                    pb::types::v1::LabelPair {
                        name: "service_name".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![pb::push::v1::RawSample {
                    raw_profile: gzip(&pprof_bytes),
                    id: "profile-a".into(),
                }],
                annotations: Vec::new(),
            }],
        };

        let out = decode_push(&req, 1 << 20).unwrap();

        assert!(out.len() == 1);
        assert!(out[0].labels.get("__profile_id__") == Some("profile-a"));
    }
}

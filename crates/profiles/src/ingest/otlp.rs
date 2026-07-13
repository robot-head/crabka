//! OTLP `v1development` profiles -> `Vec<RawProfile>`.
//!
//! The generated OTLP types live in this crate, so the edge converts them into
//! the pprof wire model owned by `crabka-pprof`.

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::{error::ProfilesError, ingest::RawProfile, wire::pb};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn decode_otlp(
    req: &pb::otlp_profiles::ExportProfilesServiceRequest,
) -> Result<Vec<RawProfile>, ProfilesError> {
    let dict = req
        .dictionary
        .as_ref()
        .ok_or_else(|| ProfilesError::Invalid("OTLP profiles missing dictionary".to_string()))?;
    let mut out = Vec::new();

    for resource_profiles in &req.resource_profiles {
        let service_name = resolve_service_name(resource_profiles);
        for scope_profiles in &resource_profiles.scope_profiles {
            for profile in &scope_profiles.profiles {
                let sample_timestamps_ns = otlp_sample_timestamps(profile)?;
                let (sample_span_ids, sample_trace_ids) = otlp_sample_links(profile, dict)?;
                let profile_labels = profile_labels(profile, dict)?;
                let profile_id =
                    (!profile.profile_id.is_empty()).then(|| hex_lower(&profile.profile_id));
                let profile = otlp_profile_to_pprof(profile, dict)?;
                let mut labels = Labels::new();
                labels.insert("service_name", service_name.clone());
                if let Some(profile_id) = profile_id {
                    labels.insert("__profile_id__", profile_id);
                }
                for (name, value) in profile_labels {
                    labels.insert(name, value);
                }
                if let Some((name, _)) = profile.sample_types().first() {
                    labels.insert("__name__", name.clone());
                }
                out.push(RawProfile {
                    labels,
                    profile,
                    delta: false,
                    sample_timestamps_ns,
                    sample_span_ids,
                    sample_trace_ids,
                });
            }
        }
    }

    Ok(out)
}

fn otlp_profile_to_pprof(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<PprofProfile, ProfilesError> {
    let mut pprof = crabka_pprof::proto::Profile {
        string_table: string_table(dict),
        mapping: dict
            .mapping_table
            .iter()
            .enumerate()
            .map(|(idx, mapping)| crabka_pprof::proto::Mapping {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                memory_start: mapping.memory_start,
                memory_limit: mapping.memory_limit,
                file_offset: mapping.file_offset,
                filename: i64::from(mapping.filename_strindex),
                ..Default::default()
            })
            .collect(),
        function: dict
            .function_table
            .iter()
            .enumerate()
            .map(|(idx, function)| crabka_pprof::proto::Function {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                name: i64::from(function.name_strindex),
                system_name: i64::from(function.system_name_strindex),
                filename: i64::from(function.filename_strindex),
                start_line: function.start_line,
            })
            .collect(),
        location: dict
            .location_table
            .iter()
            .enumerate()
            .map(|(idx, location)| crabka_pprof::proto::Location {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                mapping_id: table_ref(location.mapping_index, dict.mapping_table.len()),
                address: location.address,
                line: location
                    .lines
                    .iter()
                    .map(|line| crabka_pprof::proto::Line {
                        function_id: table_ref(line.function_index, dict.function_table.len()),
                        line: line.line,
                        column: line.column,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        time_nanos: i64::try_from(profile.time_unix_nano)
            .map_err(|_| ProfilesError::Invalid("OTLP profile time overflows i64".to_string()))?,
        duration_nanos: i64::try_from(profile.duration_nano).map_err(|_| {
            ProfilesError::Invalid("OTLP profile duration overflows i64".to_string())
        })?,
        period: profile.period,
        ..Default::default()
    };

    if let Some(sample_type) = &profile.sample_type {
        pprof.sample_type.push(value_type(*sample_type));
    }
    if let Some(period_type) = &profile.period_type {
        pprof.period_type = Some(value_type(*period_type));
    }

    for sample in &profile.samples {
        let stack = usize::try_from(sample.stack_index)
            .ok()
            .and_then(|idx| dict.stack_table.get(idx))
            .ok_or_else(|| ProfilesError::Invalid("OTLP sample references missing stack".into()))?;
        let location_id = stack
            .location_indices
            .iter()
            .map(|idx| {
                table_ref_checked(
                    *idx,
                    dict.location_table.len(),
                    "OTLP stack references missing location",
                )
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?;
        pprof.sample.push(crabka_pprof::proto::Sample {
            location_id,
            value: sample.values.clone(),
            label: sample_labels(sample, dict, &mut pprof.string_table)?,
        });
    }

    Ok(PprofProfile::from(pprof))
}

fn profile_labels(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<Vec<(String, String)>, ProfilesError> {
    profile
        .attribute_indices
        .iter()
        .map(|idx| attribute_label(*idx, dict))
        .collect()
}

fn sample_labels(
    sample: &pb::otlp_profiles::Sample,
    dict: &pb::otlp_profiles::ProfilesDictionary,
    strings: &mut Vec<String>,
) -> Result<Vec<crabka_pprof::proto::Label>, ProfilesError> {
    let mut labels = Vec::new();
    for attr_idx in &sample.attribute_indices {
        let (name, value) = attribute_label(*attr_idx, dict)?;
        let key = intern_string(strings, &name);
        let value_idx = intern_string(strings, &value);
        labels.push(crabka_pprof::proto::Label {
            key,
            str: value_idx,
            num: 0,
            num_unit: 0,
        });
    }
    Ok(labels)
}

fn attribute_label(
    index: i32,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<(String, String), ProfilesError> {
    use pb::opentelemetry::proto::common::v1::any_value::Value;

    let attr = usize::try_from(index)
        .ok()
        .and_then(|idx| dict.attribute_table.get(idx))
        .ok_or_else(|| ProfilesError::Invalid("OTLP references missing attribute".into()))?;
    let key_idx = usize::try_from(attr.key_strindex).map_err(|_| {
        ProfilesError::Invalid("OTLP attribute key references missing string".to_string())
    })?;
    let key = dict
        .string_table
        .get(key_idx)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ProfilesError::Invalid("OTLP attribute key references missing string".to_string())
        })?
        .clone();
    let value = match attr.value.as_ref().and_then(|value| value.value.as_ref()) {
        Some(Value::StringValue(value)) => value.clone(),
        Some(Value::IntValue(value)) => value.to_string(),
        None => String::new(),
    };
    Ok((key, value))
}

fn intern_string(strings: &mut Vec<String>, value: &str) -> i64 {
    if let Some(idx) = strings.iter().position(|existing| existing == value) {
        return i64::try_from(idx).expect("string index fits i64");
    }
    let idx = i64::try_from(strings.len()).expect("string index fits i64");
    strings.push(value.to_string());
    idx
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn otlp_sample_timestamps(
    profile: &pb::otlp_profiles::Profile,
) -> Result<Vec<Vec<i64>>, ProfilesError> {
    profile
        .samples
        .iter()
        .map(|sample| {
            sample
                .timestamps_unix_nano
                .iter()
                .map(|timestamp| {
                    i64::try_from(*timestamp).map_err(|_| {
                        ProfilesError::Invalid("OTLP sample timestamp overflows i64".to_string())
                    })
                })
                .collect()
        })
        .collect()
}

type OtlpSampleLinks = (Vec<Option<u64>>, Vec<Option<Vec<u8>>>);

fn otlp_sample_links(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<OtlpSampleLinks, ProfilesError> {
    let mut span_ids = Vec::with_capacity(profile.samples.len());
    let mut trace_ids = Vec::with_capacity(profile.samples.len());
    for sample in &profile.samples {
        if dict.link_table.is_empty() {
            span_ids.push(None);
            trace_ids.push(None);
            continue;
        }
        let link = usize::try_from(sample.link_index)
            .ok()
            .and_then(|idx| dict.link_table.get(idx))
            .ok_or_else(|| ProfilesError::Invalid("OTLP sample references missing link".into()))?;
        let span_id = if link.span_id.is_empty() {
            None
        } else {
            let bytes: [u8; 8] = link.span_id.as_slice().try_into().map_err(|_| {
                ProfilesError::Invalid("OTLP link span_id must be 8 bytes".to_string())
            })?;
            Some(u64::from_be_bytes(bytes))
        };
        let trace_id = (!link.trace_id.is_empty()).then(|| link.trace_id.clone());
        span_ids.push(span_id);
        trace_ids.push(trace_id);
    }
    Ok((span_ids, trace_ids))
}

fn value_type(value: pb::otlp_profiles::ValueType) -> crabka_pprof::proto::ValueType {
    crabka_pprof::proto::ValueType {
        r#type: i64::from(value.type_strindex),
        unit: i64::from(value.unit_strindex),
    }
}

fn string_table(dict: &pb::otlp_profiles::ProfilesDictionary) -> Vec<String> {
    if dict.string_table.is_empty() {
        vec![String::new()]
    } else {
        dict.string_table.clone()
    }
}

fn table_ref(index: i32, len: usize) -> u64 {
    table_ref_checked(index, len, "").unwrap_or(0)
}

fn table_ref_checked(index: i32, len: usize, message: &str) -> Result<u64, ProfilesError> {
    let idx = usize::try_from(index).map_err(|_| ProfilesError::Invalid(message.to_string()))?;
    if idx >= len {
        return Err(ProfilesError::Invalid(message.to_string()));
    }
    Ok(u64::try_from(idx + 1).unwrap_or(u64::MAX))
}

fn resolve_service_name(rp: &pb::otlp_profiles::ResourceProfiles) -> String {
    use pb::opentelemetry::proto::common::v1::any_value::Value;

    let Some(resource) = &rp.resource else {
        return "unknown_service".to_string();
    };
    for attr in &resource.attributes {
        if attr.key == "service.name"
            && let Some(value) = &attr.value
            && let Some(Value::StringValue(service)) = &value.value
            && !service.is_empty()
        {
            return service.clone();
        }
    }
    "unknown_service".to_string()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::wire::pb;

    #[test]
    fn otlp_resolves_dictionary_into_rawprofile() {
        use pb::{
            opentelemetry::proto::common::v1::{AnyValue, any_value::Value},
            otlp_profiles::{
                Function, KeyValueAndUnit, Line, Link, Location, Profile, ProfilesDictionary,
                ResourceProfiles, Sample, ScopeProfiles, Stack, ValueType,
            },
        };

        let dict = ProfilesDictionary {
            string_table: vec![
                String::new(),
                "samples".into(),
                "count".into(),
                "main".into(),
                "target".into(),
                "all".into(),
                "env".into(),
            ],
            attribute_table: vec![
                KeyValueAndUnit {
                    key_strindex: 4,
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("all".to_string())),
                    }),
                    unit_strindex: 0,
                },
                KeyValueAndUnit {
                    key_strindex: 6,
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("prod".to_string())),
                    }),
                    unit_strindex: 0,
                },
            ],
            function_table: vec![Function {
                name_strindex: 3,
                ..Default::default()
            }],
            link_table: vec![Link {
                trace_id: vec![0xaa; 16],
                span_id: 42_u64.to_be_bytes().to_vec(),
            }],
            location_table: vec![Location {
                address: 0x40,
                lines: vec![Line {
                    function_index: 0,
                    line: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            stack_table: vec![Stack {
                location_indices: vec![0],
            }],
            ..Default::default()
        };
        let profile = Profile {
            sample_type: Some(ValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            period_type: Some(ValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            samples: vec![Sample {
                stack_index: 0,
                link_index: 0,
                attribute_indices: vec![0],
                values: vec![7],
                timestamps_unix_nano: vec![1_700_000_000_000_000_123],
            }],
            time_unix_nano: 1_700_000_000_000_000_000,
            attribute_indices: vec![1],
            profile_id: vec![0xab, 0xcd],
            ..Default::default()
        };
        let req = pb::otlp_profiles::ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                scope_profiles: vec![ScopeProfiles {
                    profiles: vec![profile],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            dictionary: Some(dict),
        };

        let out = decode_otlp(&req).unwrap();

        assert!(out.len() == 1);
        for (name, want) in [
            ("__name__", "samples"),
            ("env", "prod"),
            ("__profile_id__", "abcd"),
        ] {
            check!(out[0].labels.get(name) == Some(want));
        }
        check!(!out[0].profile.sample_types().is_empty());
        let split = crate::ingest::split_sample_types(&out[0]).unwrap();
        check!(split[0].samples[0].timestamp_ns == 1_700_000_000_000_000_123);
        check!(split[0].samples[0].span_id == Some(42));
        check!(split[0].samples[0].trace_id == Some(vec![0xaa; 16]));
        check!(split[0].labels.get("target") == Some("all"));
    }
}

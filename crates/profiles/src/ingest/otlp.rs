//! OTLP `v1development` profiles -> `Vec<RawProfile>`.
//!
//! The generated OTLP types live in this crate, so the edge converts them into
//! the pprof wire model owned by `crabka-pprof`.

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::error::ProfilesError;
use crate::ingest::RawProfile;
use crate::wire::pb;

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
                let profile = otlp_profile_to_pprof(profile, dict)?;
                let mut labels = Labels::new();
                labels.insert("service_name", service_name.clone());
                if let Some((name, _)) = profile.sample_types().first() {
                    labels.insert("__name__", name.clone());
                }
                out.push(RawProfile { labels, profile });
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
        pprof.sample_type.push(value_type(sample_type));
    }
    if let Some(period_type) = &profile.period_type {
        pprof.period_type = Some(value_type(period_type));
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
                Ok(table_ref_checked(
                    *idx,
                    dict.location_table.len(),
                    "OTLP stack references missing location",
                )?)
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?;
        pprof.sample.push(crabka_pprof::proto::Sample {
            location_id,
            value: sample.values.clone(),
            label: Vec::new(),
        });
    }

    Ok(PprofProfile::from(pprof))
}

fn value_type(value: &pb::otlp_profiles::ValueType) -> crabka_pprof::proto::ValueType {
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
    use assert2::assert;

    use super::*;
    use crate::wire::pb;

    #[test]
    fn otlp_resolves_dictionary_into_rawprofile() {
        use pb::otlp_profiles::{
            Function, Line, Location, Profile, ProfilesDictionary, ResourceProfiles, Sample,
            ScopeProfiles, Stack, ValueType,
        };

        let dict = ProfilesDictionary {
            string_table: vec![
                String::new(),
                "samples".into(),
                "count".into(),
                "main".into(),
            ],
            function_table: vec![Function {
                name_strindex: 3,
                ..Default::default()
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
            samples: vec![Sample {
                stack_index: 0,
                values: vec![7],
                timestamps_unix_nano: vec![1_700_000_000_000_000_000],
                ..Default::default()
            }],
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
        assert!(out[0].labels.get("__name__") == Some("samples"));
        assert!(!out[0].profile.sample_types().is_empty());
    }
}

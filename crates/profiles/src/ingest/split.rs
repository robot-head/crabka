//! Multi-value split: one pprof with N `sample_type[]` becomes N profile series.

use crabka_pprof::ProfileType;

use crate::error::ProfilesError;
use crate::ingest::{DecodedProfile, DecodedSample, RawProfile};

/// Split one multi-value pprof into one `DecodedProfile` per `sample_type[]`.
pub fn split_sample_types(raw: &RawProfile) -> Result<Vec<DecodedProfile>, ProfilesError> {
    let name = raw
        .labels
        .get("__name__")
        .filter(|name| !name.is_empty())
        .ok_or_else(|| ProfilesError::Invalid("missing __name__".to_string()))?
        .to_string();
    let (period_type, period_unit) = raw.profile.period_type_strings();
    if period_type.is_empty() || period_unit.is_empty() {
        return Err(ProfilesError::Decode(
            "profile period_type is missing or invalid".to_string(),
        ));
    }

    let sample_types = raw.profile.sample_types();
    let mut out = Vec::with_capacity(sample_types.len());
    for (idx, (sample_type, sample_unit)) in sample_types.into_iter().enumerate() {
        if sample_type.is_empty() || sample_unit.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "sample_type[{idx}] is missing or invalid"
            )));
        }

        let profile_type = ProfileType {
            name: name.clone(),
            sample_type,
            sample_unit,
            period_type: period_type.clone(),
            period_unit: period_unit.clone(),
        }
        .to_string();

        let mut labels = raw.labels.clone();
        labels.insert("__profile_type__", profile_type.clone());
        labels.insert("__period_type__", period_type.clone());
        labels.insert("__period_unit__", period_unit.clone());

        let mut samples = Vec::with_capacity(raw.profile.samples().len());
        for sample in raw.profile.samples() {
            let value = sample
                .value
                .get(idx)
                .copied()
                .ok_or_else(|| ProfilesError::Decode(format!("sample value[{idx}] missing")))?;
            let stacktrace_location_refs = sample
                .location_id
                .iter()
                .map(|location| {
                    u32::try_from(*location).map_err(|err| {
                        ProfilesError::Decode(format!(
                            "location id {location} does not fit u32: {err}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            samples.push(DecodedSample {
                stacktrace_location_refs,
                value,
                timestamp_ns: 0,
                span_id: None,
                trace_id: None,
            });
        }

        out.push(DecodedProfile {
            labels,
            profile_type,
            samples,
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_blockstore::Labels;
    use crabka_pprof::PprofProfile;

    use crate::ingest::RawProfile;

    use super::*;

    fn two_type_profile() -> PprofProfile {
        let profile = crabka_pprof::proto::Profile {
            sample_type: vec![
                crabka_pprof::proto::ValueType { r#type: 1, unit: 2 },
                crabka_pprof::proto::ValueType { r#type: 3, unit: 4 },
            ],
            sample: vec![crabka_pprof::proto::Sample {
                location_id: vec![7],
                value: vec![3, 4096],
                label: Vec::new(),
            }],
            string_table: vec![
                String::new(),
                "alloc_objects".to_string(),
                "count".to_string(),
                "alloc_space".to_string(),
                "bytes".to_string(),
                "space".to_string(),
            ],
            period_type: Some(crabka_pprof::proto::ValueType { r#type: 5, unit: 4 }),
            ..Default::default()
        };
        PprofProfile::from(profile)
    }

    #[test]
    fn split_yields_one_series_per_sample_type() {
        let mut labels = Labels::new();
        labels.insert("__name__", "memory");
        labels.insert("service_name", "api");
        let raw = RawProfile {
            labels,
            profile: two_type_profile(),
        };

        let out = split_sample_types(&raw).unwrap();
        assert!(out.len() == 2);

        let types: Vec<&str> = out
            .iter()
            .map(|profile| profile.profile_type.as_str())
            .collect();
        assert!(
            types
                .iter()
                .any(|profile_type| profile_type == &"memory:alloc_objects:count:space:bytes")
        );
        assert!(
            types
                .iter()
                .any(|profile_type| profile_type == &"memory:alloc_space:bytes:space:bytes")
        );

        let objects = out
            .iter()
            .find(|profile| profile.profile_type.contains("alloc_objects"))
            .unwrap();
        let space = out
            .iter()
            .find(|profile| profile.profile_type.contains("alloc_space"))
            .unwrap();

        assert!(objects.samples[0].value == 3);
        assert!(space.samples[0].value == 4096);
        assert!(objects.samples[0].stacktrace_location_refs == vec![7]);
        assert!(objects.labels.get("__profile_type__") == Some(objects.profile_type.as_str()));
        assert!(objects.labels.get("__period_type__") == Some("space"));
        assert!(objects.labels.get("__period_unit__") == Some("bytes"));
    }
}

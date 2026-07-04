//! Multi-value split: one pprof with N `sample_type[]` becomes N profile series.

use std::collections::{BTreeMap, HashMap};

use crabka_blockstore::Labels;
use crabka_pprof::ProfileType;

use crate::{
    error::ProfilesError,
    ingest::{DecodedProfile, DecodedSample, RawProfile},
};

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
    let timestamp_ns = raw.profile.inner().time_nanos;
    let location_refs = raw
        .profile
        .inner()
        .location
        .iter()
        .enumerate()
        .map(|(idx, location)| {
            let idx = u32::try_from(idx).map_err(|err| {
                ProfilesError::Decode(format!("location index does not fit u32: {err}"))
            })?;
            Ok((location.id, idx))
        })
        .collect::<Result<HashMap<_, _>, ProfilesError>>()?;
    let mut out = Vec::with_capacity(sample_types.len());
    for (idx, (sample_type, sample_unit)) in sample_types.into_iter().enumerate() {
        if sample_type.is_empty() || sample_unit.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "sample_type[{idx}] is missing or invalid"
            )));
        }

        let profile_type = ProfileType {
            name: name.clone(),
            sample_type: sample_type.clone(),
            sample_unit: sample_unit.clone(),
            period_type: period_type.clone(),
            period_unit: period_unit.clone(),
            delta: raw.delta,
        }
        .to_string();

        let mut labels = raw.labels.clone();
        labels.insert("__profile_type__", profile_type.clone());
        labels.insert("__period_type__", period_type.clone());
        labels.insert("__period_unit__", period_unit.clone());
        labels.insert("__type__", sample_type.clone());
        labels.insert("__unit__", sample_unit.clone());
        if let Some(service_name) = raw.labels.get("service_name") {
            labels.insert("__service_name__", service_name.to_string());
        }

        let mut groups = BTreeMap::<Vec<(String, String)>, (Labels, Vec<DecodedSample>)>::new();
        for (sample_idx, sample) in raw.profile.samples().iter().enumerate() {
            let value = sample
                .value
                .get(idx)
                .copied()
                .ok_or_else(|| ProfilesError::Decode(format!("sample value[{idx}] missing")))?;
            let timestamp_ns = raw
                .sample_timestamps_ns
                .get(sample_idx)
                .and_then(|timestamps| timestamps.get(idx))
                .copied()
                .unwrap_or(timestamp_ns);
            let stacktrace_location_refs = sample
                .location_id
                .iter()
                .map(|location| {
                    location_refs.get(location).copied().ok_or_else(|| {
                        ProfilesError::Decode(format!(
                            "sample references missing location id {location}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let sample_labels = labels_with_sample_labels(&labels, &raw.profile, sample);
            let key = labels_key(&sample_labels);
            groups
                .entry(key)
                .or_insert((sample_labels, Vec::new()))
                .1
                .push(DecodedSample {
                    stacktrace_location_refs,
                    value,
                    timestamp_ns,
                    span_id: raw.sample_span_ids.get(sample_idx).copied().flatten(),
                    trace_id: raw.sample_trace_ids.get(sample_idx).cloned().flatten(),
                });
        }

        out.extend(
            groups
                .into_values()
                .map(|(labels, samples)| DecodedProfile {
                    labels,
                    profile_type: profile_type.clone(),
                    samples,
                }),
        );
    }

    Ok(out)
}

fn labels_with_sample_labels(
    base: &Labels,
    profile: &crabka_pprof::PprofProfile,
    sample: &crabka_pprof::proto::Sample,
) -> Labels {
    let mut labels = base.clone();
    for label in &sample.label {
        if label.str <= 0 {
            continue;
        }
        let Some(name) = profile.string(label.key) else {
            continue;
        };
        if labels.get(name).is_some() {
            continue;
        }
        if let Some(value) = profile.string(label.str) {
            labels.insert(name.to_string(), value.to_string());
        }
    }
    labels
}

fn labels_key(labels: &Labels) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_blockstore::Labels;
    use crabka_pprof::PprofProfile;

    use super::*;
    use crate::ingest::RawProfile;

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
            location: (1..=7)
                .map(|id| crabka_pprof::proto::Location {
                    id,
                    ..Default::default()
                })
                .collect(),
            string_table: vec![
                String::new(),
                "alloc_objects".to_string(),
                "count".to_string(),
                "alloc_space".to_string(),
                "bytes".to_string(),
                "space".to_string(),
            ],
            period_type: Some(crabka_pprof::proto::ValueType { r#type: 5, unit: 4 }),
            time_nanos: 123_000_000,
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
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
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

        check!(objects.samples[0].value == 3);
        check!(space.samples[0].value == 4096);
        check!(objects.samples[0].timestamp_ns == 123_000_000);
        check!(objects.samples[0].stacktrace_location_refs == vec![6]);
        for (name, want) in [
            ("__profile_type__", objects.profile_type.as_str()),
            ("__period_type__", "space"),
            ("__period_unit__", "bytes"),
            ("__type__", "alloc_objects"),
            ("__unit__", "count"),
            ("__service_name__", "api"),
        ] {
            check!(objects.labels.get(name) == Some(want));
        }
    }

    #[test]
    fn split_normalizes_pprof_location_ids_to_symbol_indices() {
        let profile = crabka_pprof::proto::Profile {
            sample_type: vec![crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![crabka_pprof::proto::Sample {
                location_id: vec![2],
                value: vec![5],
                label: Vec::new(),
            }],
            location: vec![
                crabka_pprof::proto::Location {
                    id: 1,
                    ..Default::default()
                },
                crabka_pprof::proto::Location {
                    id: 2,
                    ..Default::default()
                },
            ],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "sample".to_string(),
            ],
            period_type: Some(crabka_pprof::proto::ValueType { r#type: 3, unit: 2 }),
            ..Default::default()
        };
        let mut labels = Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");

        let out = split_sample_types(&RawProfile {
            labels,
            profile: PprofProfile::from(profile),
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
        })
        .unwrap();

        assert!(out[0].samples[0].stacktrace_location_refs == vec![1]);
    }

    #[test]
    fn split_promotes_pprof_string_sample_labels_to_series_labels() {
        let profile = crabka_pprof::proto::Profile {
            sample_type: vec![crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![
                crabka_pprof::proto::Sample {
                    location_id: vec![1],
                    value: vec![5],
                    label: vec![crabka_pprof::proto::Label {
                        key: 4,
                        str: 5,
                        num: 0,
                        num_unit: 0,
                    }],
                },
                crabka_pprof::proto::Sample {
                    location_id: vec![1],
                    value: vec![7],
                    label: vec![crabka_pprof::proto::Label {
                        key: 4,
                        str: 6,
                        num: 0,
                        num_unit: 0,
                    }],
                },
            ],
            location: vec![crabka_pprof::proto::Location {
                id: 1,
                ..Default::default()
            }],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "sample".to_string(),
                "target".to_string(),
                "all".to_string(),
                "self".to_string(),
            ],
            period_type: Some(crabka_pprof::proto::ValueType { r#type: 3, unit: 2 }),
            ..Default::default()
        };
        let mut labels = Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");

        let out = split_sample_types(&RawProfile {
            labels,
            profile: PprofProfile::from(profile),
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
        })
        .unwrap();

        check!(out.len() == 2);
        for target in ["all", "self"] {
            check!(
                out.iter()
                    .any(|profile| profile.labels.get("target") == Some(target))
            );
        }
    }
}

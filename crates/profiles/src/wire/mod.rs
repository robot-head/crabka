//! Generated message + Connect-server types from vendored protos.

/// Generated protobuf + Connect server stubs.
#[allow(clippy::pedantic, clippy::style)]
pub mod pb {
    /// Pyroscope `push.v1.PusherService`.
    pub mod push {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/push.v1.rs"));
        }
    }

    /// Shared Pyroscope `types.v1` messages.
    pub mod types {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/types.v1.rs"));
        }
    }

    /// Pyroscope `querier.v1.QuerierService`.
    pub mod querier {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/querier.v1.rs"));
        }
    }

    /// OpenTelemetry packages, nested to match generated cross-package paths.
    pub mod opentelemetry {
        pub mod proto {
            pub mod common {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.common.v1.rs"
                    ));
                }
            }

            pub mod resource {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.resource.v1.rs"
                    ));
                }
            }

            pub mod profiles {
                pub mod v1development {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.profiles.v1development.rs"
                    ));
                }
            }

            pub mod collector {
                pub mod profiles {
                    pub mod v1development {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/opentelemetry.proto.collector.profiles.v1development.rs"
                        ));
                    }
                }
            }
        }
    }

    /// Ergonomic alias for OTLP collector profile service + message types.
    pub mod otlp_profiles {
        pub use super::opentelemetry::proto::collector::profiles::v1development::*;
        pub use super::opentelemetry::proto::profiles::v1development::*;
    }
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use crabka_blockstore::Labels;
    use crabka_pprof::PprofProfile;
    pub(crate) fn cpu_profile_pprof_bytes() -> Vec<u8> {
        cpu_profile().encode()
    }

    pub(crate) fn raw_profile_cpu() -> crate::ingest::RawProfile {
        let mut labels = Labels::new();
        labels.insert("__name__", "process_cpu");
        labels.insert("service_name", "api");
        crate::ingest::RawProfile {
            labels,
            profile: cpu_profile(),
        }
    }

    pub(crate) fn raw_profile_2types() -> crate::ingest::RawProfile {
        let mut labels = Labels::new();
        labels.insert("__name__", "memory");
        labels.insert("service_name", "api");
        crate::ingest::RawProfile {
            labels,
            profile: PprofProfile::from(crabka_pprof::proto::Profile {
                sample_type: vec![
                    crabka_pprof::proto::ValueType { r#type: 1, unit: 2 },
                    crabka_pprof::proto::ValueType { r#type: 3, unit: 4 },
                ],
                sample: vec![crabka_pprof::proto::Sample {
                    location_id: vec![1],
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
                    "main".to_string(),
                ],
                location: vec![crabka_pprof::proto::Location {
                    id: 1,
                    address: 0x40,
                    line: vec![crabka_pprof::proto::Line {
                        function_id: 1,
                        line: 10,
                        column: 0,
                    }],
                    ..Default::default()
                }],
                function: vec![crabka_pprof::proto::Function {
                    id: 1,
                    name: 6,
                    system_name: 6,
                    ..Default::default()
                }],
                period_type: Some(crabka_pprof::proto::ValueType { r#type: 5, unit: 4 }),
                ..Default::default()
            }),
        }
    }

    fn cpu_profile() -> PprofProfile {
        PprofProfile::from(crabka_pprof::proto::Profile {
            sample_type: vec![crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![crabka_pprof::proto::Sample {
                location_id: vec![1],
                value: vec![42],
                label: Vec::new(),
            }],
            string_table: vec![
                String::new(),
                "cpu".to_string(),
                "nanoseconds".to_string(),
                "main".to_string(),
            ],
            location: vec![crabka_pprof::proto::Location {
                id: 1,
                address: 0x40,
                line: vec![crabka_pprof::proto::Line {
                    function_id: 1,
                    line: 10,
                    column: 0,
                }],
                ..Default::default()
            }],
            function: vec![crabka_pprof::proto::Function {
                id: 1,
                name: 3,
                system_name: 3,
                ..Default::default()
            }],
            period_type: Some(crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::pb;

    #[test]
    fn push_request_round_trips_via_prost() {
        let request = pb::push::v1::PushRequest {
            series: vec![pb::push::v1::RawProfileSeries {
                labels: vec![pb::types::v1::LabelPair {
                    name: "__name__".to_string(),
                    value: "process_cpu".to_string(),
                }],
                samples: vec![pb::push::v1::RawSample {
                    raw_profile: vec![1, 2, 3],
                    id: "abc".to_string(),
                }],
                annotations: vec![pb::types::v1::ProfileAnnotation {
                    key: "source".to_string(),
                    value: "test".to_string(),
                }],
            }],
        };

        let bytes = request.encode_to_vec();
        let decoded = pb::push::v1::PushRequest::decode(bytes.as_slice()).unwrap();

        assert_eq!(decoded.series.len(), 1);
        assert_eq!(decoded.series[0].samples[0].raw_profile, vec![1, 2, 3]);
        assert_eq!(decoded.series[0].samples[0].id, "abc");
        assert_eq!(decoded.series[0].annotations[0].key, "source");
    }

    #[test]
    fn otlp_profiles_dictionary_round_trips() {
        let dictionary = pb::otlp_profiles::ProfilesDictionary {
            string_table: vec![String::new(), "samples".to_string(), "count".to_string()],
            stack_table: vec![pb::otlp_profiles::Stack {
                location_indices: vec![0, 1],
            }],
            ..Default::default()
        };

        let bytes = dictionary.encode_to_vec();
        let decoded = pb::otlp_profiles::ProfilesDictionary::decode(bytes.as_slice()).unwrap();

        assert!(decoded.string_table[0].is_empty());
        assert_eq!(decoded.stack_table[0].location_indices, vec![0, 1]);

        let sample = pb::otlp_profiles::Sample {
            stack_index: 0,
            values: vec![5],
            ..Default::default()
        };
        let sample_bytes = sample.encode_to_vec();
        assert_eq!(
            pb::otlp_profiles::Sample::decode(sample_bytes.as_slice())
                .unwrap()
                .values,
            vec![5]
        );
    }
}

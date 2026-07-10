//! Generated message + Connect-server types from vendored protos.

/// Generated protobuf + Connect server stubs.
#[allow(clippy::pedantic, clippy::style, clippy::useless_borrows_in_formatting)]
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

    /// Pprof-compatible `google.v1.Profile` messages used by Pyroscope.
    pub mod google {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/google.v1.rs"));
        }
    }

    /// Pyroscope `querier.v1.QuerierService`.
    pub mod querier {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/querier.v1.rs"));
        }
    }

    /// Pyroscope `settings.v1.SettingsService` (UI settings the Grafana
    /// Profiles Drilldown app loads on init).
    pub mod settings {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/settings.v1.rs"));
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
        pub use super::opentelemetry::proto::{
            collector::profiles::v1development::*, profiles::v1development::*,
        };
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
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
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
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
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

        assert_eq!(decoded, request);
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

        assert_eq!(decoded, dictionary);

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

    #[test]
    fn querier_point_uses_upstream_field_numbers() {
        let point = pb::querier::v1::Point {
            timestamp: 42,
            value: 1.5,
            annotations: Vec::new(),
            exemplars: Vec::new(),
        };

        let bytes = point.encode_to_vec();

        assert_eq!(
            bytes,
            vec![
                0x09, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f, // value = 1.5, field 1
                0x10, 42, // timestamp = 42, field 2
            ]
        );
    }

    #[test]
    fn querier_point_carries_upstream_annotations_and_exemplars() {
        let point = pb::querier::v1::Point {
            value: 1.5,
            timestamp: 42,
            annotations: vec![pb::types::v1::ProfileAnnotation {
                key: "source".to_string(),
                value: "agent".to_string(),
            }],
            exemplars: vec![pb::types::v1::Exemplar {
                timestamp: 42,
                profile_id: "profile-1".to_string(),
                span_id: "span-1".to_string(),
                value: 7,
                labels: vec![pb::types::v1::LabelPair {
                    name: "pod".to_string(),
                    value: "api-0".to_string(),
                }],
            }],
        };

        let bytes = point.encode_to_vec();

        assert!(bytes.contains(&0x1a)); // annotations, field 3
        assert!(bytes.contains(&0x22)); // exemplars, field 4
    }

    #[test]
    fn querier_diff_uses_upstream_field_numbers() {
        let diff = pb::querier::v1::FlameGraphDiff {
            names: Vec::new(),
            levels: Vec::new(),
            total: 100,
            max_self: 40,
            left_ticks: 60,
            right_ticks: 40,
        };

        let bytes = diff.encode_to_vec();

        assert_eq!(
            bytes,
            vec![
                0x18, 100, // total, field 3
                0x20, 40, // max_self, field 4
                0x28, 60, // leftTicks, field 5
                0x30, 40, // rightTicks, field 6
            ]
        );
    }

    #[test]
    fn querier_enums_use_upstream_names_and_values() {
        assert_eq!(pb::querier::v1::ProfileFormat::Flamegraph as i32, 1);
        assert_eq!(
            pb::querier::v1::ProfileFormat::Flamegraph.as_str_name(),
            "PROFILE_FORMAT_FLAMEGRAPH"
        );
        assert_eq!(
            pb::querier::v1::SeriesAggregationType::TimeSeriesAggregationTypeAverage as i32,
            1
        );
        assert_eq!(pb::querier::v1::ExemplarType::None as i32, 1);
        assert_eq!(
            pb::querier::v1::ExemplarType::Span.as_str_name(),
            "EXEMPLAR_TYPE_SPAN"
        );
    }

    #[test]
    fn select_series_request_uses_upstream_optional_fields() {
        let request = pb::querier::v1::SelectSeriesRequest {
            profile_type_id: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            label_selector: "{}".to_string(),
            start: 10,
            end: 20,
            group_by: vec!["env".to_string()],
            step: 5.0,
            aggregation: pb::querier::v1::SeriesAggregationType::TimeSeriesAggregationTypeSum
                as i32,
            stack_trace_selector: Some(pb::types::v1::StackTraceSelector {
                call_site: vec![pb::types::v1::Location {
                    name: "main".to_string(),
                }],
                go_pgo: None,
            }),
            limit: 10,
            exemplar_type: pb::querier::v1::ExemplarType::Individual as i32,
        };

        let bytes = request.encode_to_vec();

        assert!(bytes.contains(&0x42)); // stack_trace_selector, field 8
        assert!(bytes.contains(&0x48)); // limit, field 9
        assert!(bytes.contains(&0x50)); // exemplar_type, field 10
    }

    #[test]
    fn select_merge_profile_request_uses_upstream_selector_fields() {
        let request = pb::querier::v1::SelectMergeProfileRequest {
            profile_type_id: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            label_selector: "{}".to_string(),
            start: 10,
            end: 20,
            max_nodes: 30,
            stack_trace_selector: Some(pb::types::v1::StackTraceSelector {
                call_site: vec![pb::types::v1::Location {
                    name: "main".to_string(),
                }],
                go_pgo: None,
            }),
            profile_id_selector: vec!["profile-a".to_string()],
        };

        let bytes = request.encode_to_vec();

        assert!(bytes.contains(&0x28)); // max_nodes, field 5
        assert!(bytes.contains(&0x32)); // stack_trace_selector, field 6
        assert!(bytes.contains(&0x3a)); // profile_id_selector, field 7
    }

    #[test]
    fn span_profile_request_uses_upstream_span_selector_field() {
        let request = pb::querier::v1::SelectMergeSpanProfileRequest {
            profile_type_id: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            label_selector: "{}".to_string(),
            span_selector: vec!["9a517183f26a089d".to_string()],
            start: 10,
            end: 20,
            max_nodes: 30,
            format: pb::querier::v1::ProfileFormat::Flamegraph as i32,
        };

        let bytes = request.encode_to_vec();

        assert_eq!(
            bytes,
            vec![
                0x0a, 0x2b, b'p', b'r', b'o', b'c', b'e', b's', b's', b'_', b'c', b'p', b'u', b':',
                b'c', b'p', b'u', b':', b'n', b'a', b'n', b'o', b's', b'e', b'c', b'o', b'n', b'd',
                b's', b':', b'c', b'p', b'u', b':', b'n', b'a', b'n', b'o', b's', b'e', b'c', b'o',
                b'n', b'd', b's', // profile_typeID, field 1
                0x12, 0x02, b'{', b'}', // label_selector, field 2
                0x1a, 0x10, b'9', b'a', b'5', b'1', b'7', b'1', b'8', b'3', b'f', b'2', b'6', b'a',
                b'0', b'8', b'9', b'd', // span_selector, field 3
                0x20, 10, // start, field 4
                0x28, 20, // end, field 5
                0x30, 30, // max_nodes, field 6
                0x38, 1, // format, field 7
            ]
        );
    }

    #[test]
    fn heatmap_messages_use_upstream_shape() {
        assert_eq!(
            pb::querier::v1::HeatmapQueryType::Individual.as_str_name(),
            "HEATMAP_QUERY_TYPE_INDIVIDUAL"
        );
        let request = pb::querier::v1::SelectHeatmapRequest {
            profile_type_id: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            label_selector: "{}".to_string(),
            start: 10,
            end: 20,
            step: 5.0,
            group_by: vec!["env".to_string()],
            query_type: pb::querier::v1::HeatmapQueryType::Individual as i32,
            exemplar_type: pb::querier::v1::ExemplarType::None as i32,
            limit: 10,
        };
        let response = pb::querier::v1::SelectHeatmapResponse {
            series: vec![pb::querier::v1::HeatmapSeries {
                labels: vec![pb::querier::v1::LabelPair {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                }],
                slots: vec![pb::querier::v1::HeatmapSlot {
                    timestamp: 15,
                    y_min: vec![0.0, 10.0],
                    counts: vec![1, 2],
                    exemplars: Vec::new(),
                }],
            }],
        };

        let request_bytes = request.encode_to_vec();
        let response_bytes = response.encode_to_vec();

        assert!(request_bytes.contains(&0x29)); // step, field 5, fixed64 wire type
        assert!(!response_bytes.is_empty());
        assert_eq!(response.series[0].slots[0].counts, vec![1, 2]);
    }

    #[test]
    fn analyze_query_messages_use_upstream_shape() {
        let request = pb::querier::v1::AnalyzeQueryRequest {
            start: 10,
            end: 20,
            query: "process_cpu:cpu:nanoseconds:cpu:nanoseconds{}".to_string(),
        };
        let response = pb::querier::v1::AnalyzeQueryResponse {
            query_scopes: vec![pb::querier::v1::QueryScope {
                component_type: "Long term storage".to_string(),
                component_count: 1,
                block_count: 2,
                series_count: 3,
                profile_count: 4,
                sample_count: 5,
                index_bytes: 6,
                profile_bytes: 7,
                symbol_bytes: 8,
            }],
            query_impact: Some(pb::querier::v1::QueryImpact {
                total_bytes_in_time_range: 9,
                total_queried_series: 10,
                deduplication_needed: false,
            }),
        };

        let request_bytes = request.encode_to_vec();
        let response_bytes = response.encode_to_vec();

        assert_eq!(request_bytes[0], 0x10); // start, field 2
        assert_eq!(request_bytes[2], 0x18); // end, field 3
        assert!(request_bytes.contains(&0x22)); // query, field 4
        assert!(response_bytes.contains(&0x0a)); // query_scopes, field 1
        assert!(response_bytes.contains(&0x12)); // query_impact, field 2
    }
}

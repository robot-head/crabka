use clap::Parser;
use crabka_observability::{QuerierIndexSource, Role, ServiceConfig};
use crabka_units::{bytes, kibibytes, millis, nanos};

#[test]
fn service_config_reads_environment() {
    temp_env::with_vars(
        [
            ("CRABKA_OBSERVABILITY_TARGET", Some("querier")),
            ("CRABKA_OBSERVABILITY_LISTEN_ADDR", Some("127.0.0.1:3200")),
            (
                "CRABKA_OBSERVABILITY_OBJECT_STORE_URL",
                Some("s3://crabka-observability"),
            ),
            (
                "CRABKA_OBSERVABILITY_WAL_BOOTSTRAP_SERVER",
                Some("127.0.0.1:9092"),
            ),
            ("CRABKA_OBSERVABILITY_WAL_TOPIC", Some("logs-wal")),
            ("CRABKA_OBSERVABILITY_WAL_GROUP_ID", Some("logs-querier")),
            (
                "CRABKA_OBSERVABILITY_DATA_ROOT",
                Some("/var/lib/crabka-observability"),
            ),
            (
                "CRABKA_OBSERVABILITY_QUERIER_INDEX_SOURCE",
                Some("tenant-object-store-shards"),
            ),
            ("CRABKA_OBSERVABILITY_TENANT", Some("tenant-a")),
            (
                "CRABKA_OBSERVABILITY_INDEX_PREFIX",
                Some("observability/logs"),
            ),
            ("CRABKA_OBSERVABILITY_QUERY_START_NS", Some("10")),
            ("CRABKA_OBSERVABILITY_QUERY_END_NS", Some("30")),
            ("CRABKA_OBSERVABILITY_MAX_QUERY_RANGE", Some("20ns")),
            ("CRABKA_OBSERVABILITY_MAX_QUERY_SERIES", Some("10")),
            ("CRABKA_OBSERVABILITY_MAX_QUERY_READ", Some("1KiB")),
            ("CRABKA_OBSERVABILITY_MAX_QUERY_LENGTH", Some("64B")),
            ("CRABKA_OBSERVABILITY_MAX_INGEST_BODY", Some("2KiB")),
            ("CRABKA_OBSERVABILITY_WAL_APPEND_TIMEOUT", Some("250ms")),
        ],
        || {
            let config =
                ServiceConfig::try_parse_from(["crabka-observability"]).expect("parse environment");

            assert_eq!(
                config,
                ServiceConfig {
                    target: Role::Querier,
                    listen_addr: "127.0.0.1:3200".parse().unwrap(),
                    object_store_url: Some("s3://crabka-observability".to_string()),
                    wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                    wal_topic: "logs-wal".to_string(),
                    wal_group_id: "logs-querier".to_string(),
                    data_root: "/var/lib/crabka-observability".into(),
                    querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
                    tenant: Some("tenant-a".to_string()),
                    index_prefix: Some("observability/logs".to_string()),
                    query_start_ns: Some(10),
                    query_end_ns: Some(30),
                    max_query_range: Some(nanos(20)),
                    max_query_series: Some(10),
                    max_query_read: Some(kibibytes(1)),
                    max_query_length: Some(bytes(64)),
                    max_ingest_body: Some(kibibytes(2)),
                    wal_append_timeout: Some(millis(250)),
                }
            );
        },
    );
}

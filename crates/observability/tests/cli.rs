use assert2::assert;
use clap::Parser;
use crabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, build_service_dependencies, run,
};
use crabka_units::{kibibytes, millis, nanos};

#[test]
fn parses_explicit_service_targets() {
    for (target, expected) in [
        ("distributor", Role::Distributor),
        ("compactor", Role::Compactor),
        ("querier", Role::Querier),
    ] {
        let config =
            ServiceConfig::try_parse_from(["crabka-observability", "--target", target]).unwrap();

        assert!(config.target == expected);
        assert!(run(config).unwrap().role == expected);
    }
}

#[test]
fn parses_querier_object_store_shard_catalog_config() {
    let config = ServiceConfig::try_parse_from([
        "crabka-observability",
        "--target",
        "querier",
        "--listen-addr",
        "127.0.0.1:3200",
        "--object-store-url",
        "s3://crabka-observability",
        "--data-root",
        "/var/lib/crabka-observability",
        "--querier-index-source",
        "tenant-object-store-shards",
        "--tenant",
        "tenant-a",
        "--index-prefix",
        "observability/logs",
        "--query-start-ns",
        "10",
        "--query-end-ns",
        "30",
        "--max-query-range",
        "20ns",
        "--max-query-series",
        "10",
        "--max-query-read",
        "1KiB",
        "--max-query-length",
        "64",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::Querier,
                listen_addr: "127.0.0.1:3200".parse().unwrap(),
                object_store_url: Some("s3://crabka-observability".to_string()),
                wal_bootstrap_server: None,
                wal_topic: "__crabka_observability_logs_wal".to_string(),
                wal_group_id: "crabka-observability-compactor".to_string(),
                data_root: "/var/lib/crabka-observability".into(),
                querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
                tenant: Some("tenant-a".to_string()),
                index_prefix: Some("observability/logs".to_string()),
                query_start_ns: Some(10),
                query_end_ns: Some(30),
                max_query_range: Some(nanos(20)),
                max_query_series: Some(10),
                max_query_read: Some(kibibytes(1)),
                max_query_length: Some(64),
                max_ingest_body: None,
                wal_append_timeout: None,
            }
    );
}

#[test]
fn parses_distributor_wal_config() {
    let config = ServiceConfig::try_parse_from([
        "crabka-observability",
        "--target",
        "distributor",
        "--wal-bootstrap-server",
        "127.0.0.1:9092",
        "--wal-topic",
        "__crabka_observability_logs_wal",
        "--max-ingest-body",
        "2KiB",
        "--wal-append-timeout",
        "250ms",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::Distributor,
                listen_addr: "127.0.0.1:3100".parse().unwrap(),
                object_store_url: None,
                wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                wal_topic: "__crabka_observability_logs_wal".to_string(),
                wal_group_id: "crabka-observability-compactor".to_string(),
                data_root: ".".into(),
                querier_index_source: QuerierIndexSource::LocalManifest,
                tenant: None,
                index_prefix: None,
                query_start_ns: None,
                query_end_ns: None,
                max_query_range: None,
                max_query_series: None,
                max_query_read: None,
                max_query_length: None,
                max_ingest_body: Some(kibibytes(2)),
                wal_append_timeout: Some(millis(250)),
            }
    );
}

#[test]
fn parses_compactor_wal_consumer_config() {
    let config = ServiceConfig::try_parse_from([
        "crabka-observability",
        "--target",
        "compactor",
        "--wal-bootstrap-server",
        "127.0.0.1:9092",
        "--wal-topic",
        "__crabka_observability_logs_wal",
        "--wal-group-id",
        "crabka-observability-compactor",
        "--object-store-url",
        "file:///tmp/crabka-observability",
        "--index-prefix",
        "observability/logs",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::Compactor,
                listen_addr: "127.0.0.1:3100".parse().unwrap(),
                object_store_url: Some("file:///tmp/crabka-observability".to_string()),
                wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                wal_topic: "__crabka_observability_logs_wal".to_string(),
                wal_group_id: "crabka-observability-compactor".to_string(),
                data_root: ".".into(),
                querier_index_source: QuerierIndexSource::LocalManifest,
                tenant: None,
                index_prefix: Some("observability/logs".to_string()),
                query_start_ns: None,
                query_end_ns: None,
                max_query_range: None,
                max_query_series: None,
                max_query_read: None,
                max_query_length: None,
                max_ingest_body: None,
                wal_append_timeout: None,
            }
    );
}

#[test]
fn parses_querier_wal_tail_config() {
    let config = ServiceConfig::try_parse_from([
        "crabka-observability",
        "--target",
        "querier",
        "--wal-bootstrap-server",
        "127.0.0.1:9092",
        "--wal-topic",
        "__crabka_observability_logs_wal",
        "--wal-group-id",
        "crabka-observability-querier-tail",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::Querier,
                listen_addr: "127.0.0.1:3100".parse().unwrap(),
                object_store_url: None,
                wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                wal_topic: "__crabka_observability_logs_wal".to_string(),
                wal_group_id: "crabka-observability-querier-tail".to_string(),
                data_root: ".".into(),
                querier_index_source: QuerierIndexSource::LocalManifest,
                tenant: None,
                index_prefix: None,
                query_start_ns: None,
                query_end_ns: None,
                max_query_range: None,
                max_query_series: None,
                max_query_read: None,
                max_query_length: None,
                max_ingest_body: None,
                wal_append_timeout: None,
            }
    );
}

#[tokio::test]
async fn querier_dependencies_require_wal_bootstrap_server() {
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_length: None,
        max_ingest_body: None,
        wal_append_timeout: None,
    };

    match build_service_dependencies(&config).await {
        Ok(_) => panic!("querier dependencies should require WAL bootstrap config"),
        Err(error) => {
            assert!(error.to_string().contains("missing --wal-bootstrap-server"));
        }
    }
}

#[test]
fn rejects_missing_target() {
    let error = ServiceConfig::try_parse_from(["crabka-observability"]).unwrap_err();

    assert!(error.to_string().contains("--target"));
}

#[test]
fn rejects_unknown_target() {
    let error = ServiceConfig::try_parse_from(["crabka-observability", "--target", "ingester"])
        .unwrap_err();

    assert!(error.to_string().contains("invalid value"));
}

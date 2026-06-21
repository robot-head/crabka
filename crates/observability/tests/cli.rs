use assert2::assert;
use clap::Parser;
use crabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, build_service_dependencies, run,
};

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
        "--max-query-range-ns",
        "20",
        "--max-query-series",
        "10",
        "--max-query-bytes",
        "1024",
        "--max-query-length",
        "64",
    ])
    .unwrap();

    assert!(config.target == Role::Querier);
    assert!(config.listen_addr.to_string() == "127.0.0.1:3200");
    assert!(config.object_store_url.as_deref() == Some("s3://crabka-observability"));
    assert!(config.data_root == std::path::Path::new("/var/lib/crabka-observability"));
    assert!(config.querier_index_source == QuerierIndexSource::TenantObjectStoreShards);
    assert!(config.tenant.as_deref() == Some("tenant-a"));
    assert!(config.index_prefix.as_deref() == Some("observability/logs"));
    assert!(config.query_start_ns == Some(10));
    assert!(config.query_end_ns == Some(30));
    assert!(config.max_query_range_ns == Some(20));
    assert!(config.max_query_series == Some(10));
    assert!(config.max_query_bytes == Some(1024));
    assert!(config.max_query_length == Some(64));
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
        "--max-ingest-body-bytes",
        "2048",
        "--wal-append-timeout-ms",
        "250",
    ])
    .unwrap();

    assert!(config.target == Role::Distributor);
    assert!(config.wal_bootstrap_server.as_deref() == Some("127.0.0.1:9092"));
    assert!(config.wal_topic == "__crabka_observability_logs_wal");
    assert!(config.max_ingest_body_bytes == Some(2048));
    assert!(config.wal_append_timeout_ms == Some(250));
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

    assert!(config.target == Role::Compactor);
    assert!(config.wal_bootstrap_server.as_deref() == Some("127.0.0.1:9092"));
    assert!(config.wal_topic == "__crabka_observability_logs_wal");
    assert!(config.wal_group_id == "crabka-observability-compactor");
    assert!(config.object_store_url.as_deref() == Some("file:///tmp/crabka-observability"));
    assert!(config.index_prefix.as_deref() == Some("observability/logs"));
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

    assert!(config.target == Role::Querier);
    assert!(config.wal_bootstrap_server.as_deref() == Some("127.0.0.1:9092"));
    assert!(config.wal_topic == "__crabka_observability_logs_wal");
    assert!(config.wal_group_id == "crabka-observability-querier-tail");
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
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
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

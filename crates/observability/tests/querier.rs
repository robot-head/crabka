#![allow(clippy::unreadable_literal)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use assert2::assert;
use async_trait::async_trait;
use crabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels,
    write_log_block, write_log_block_to_object_store,
};
use crabka_logql::{parse_metric_query, parse_query, plan_stream_query};
use crabka_observability::{
    BufferedLogHotTail, CompactionFrontier, KafkaWalHeader, KafkaWalRecord, LogWalConsumer, Offset,
    PartitionIndex, WalConsumerError, WalLogRecord, WalPosition, build_kafka_wal_record,
    execute_metric_query, execute_metric_query_from_object_store, execute_metric_query_range,
    execute_metric_query_range_with_hot_tail, execute_stream_query,
    execute_stream_query_from_object_store, execute_stream_query_with_hot_tail,
    execute_stream_query_with_hot_tail_frontier, execute_tail_query,
    execute_tail_query_with_frontier, metric_plan_scan_sql, poll_log_hot_tail_once,
    stream_plan_scan_sql,
};
use object_store::{ObjectStore, local::LocalFileSystem, path::Path as ObjectPath};
use serde_json::json;

#[tokio::test]
async fn executes_stream_query_over_planned_cold_blocks_as_loki_json() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn literal_line_filter_pushdown_treats_like_wildcards_as_plain_text() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api cpu 100% ok", BTreeMap::new()),
            LogRow::new(api, 11, "api cpu 1000 ok", BTreeMap::new()),
            LogRow::new(api, 12, "api shard a_b ok", BTreeMap::new()),
            LogRow::new(api, 13, "api shard acb ok", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let percent_plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 20).unwrap(),
        parse_query(r#"{app="api"} |= "100%""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let percent_response = execute_stream_query(dir.path(), &percent_plan, &label_index)
        .await
        .unwrap();
    assert!(percent_response["data"]["result"][0]["values"] == json!([["10", "api cpu 100% ok"]]));

    let underscore_plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 20).unwrap(),
        parse_query(r#"{app="api"} |= "a_b""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let underscore_response = execute_stream_query(dir.path(), &underscore_plan, &label_index)
        .await
        .unwrap();
    assert!(
        underscore_response["data"]["result"][0]["values"] == json!([["12", "api shard a_b ok"]])
    );
}

#[tokio::test]
async fn executes_stream_query_merging_cold_blocks_with_hot_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 19,
            line: "api error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-b".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 21,
            line: "other tenant error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
    ];

    let response =
        execute_stream_query_with_hot_tail(dir.path(), &plan, &label_index, &hot_tail, 19)
            .await
            .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"],
                                ["20", "api hot error"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[test]
fn stream_plan_scan_sql_pushes_down_time_fingerprints_and_literal_line_filters() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app=~".+"} |= "error" != "debug""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let fingerprints = plan
        .fingerprints
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        stream_plan_scan_sql(&plan)
            == format!(
                "select series_fingerprint, timestamp_ns, line, structured_metadata \
                 from logs \
                 where timestamp_ns >= 0 and timestamp_ns <= 30 \
                 and series_fingerprint in ({fingerprints}) \
                 and line like '%error%' \
                 and line not like '%debug%' \
                 order by series_fingerprint, timestamp_ns"
            )
    );
}

#[test]
fn metric_plan_scan_sql_uses_eval_range_selector_and_stream_pushdowns() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let query = parse_metric_query(r#"count_over_time({app=~".+"} |= "error" [20ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 40).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let fingerprints = plan
        .fingerprints
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    assert!(
        metric_plan_scan_sql(&plan, &query, TimeRange::new(30, 40).unwrap()).unwrap()
            == format!(
                "select series_fingerprint, timestamp_ns, line, structured_metadata \
                 from logs \
                 where timestamp_ns >= 10 and timestamp_ns <= 40 \
                 and series_fingerprint in ({fingerprints}) \
                 and line like '%error%' \
                 order by series_fingerprint, timestamp_ns"
            )
    );
}

#[tokio::test]
async fn executes_stream_query_filters_hot_tail_by_partition_offset_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let label_index = LabelIndex::default();
    let block_index = BlockIndex::default();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "already compacted error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(42),
            }),
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 21,
            line: "new hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(43),
            }),
        },
    ];
    let frontier = CompactionFrontier::new(19).with_partition_offset(PartitionIndex(0), Offset(42));

    let response = execute_stream_query_with_hot_tail_frontier(
        dir.path(),
        &plan,
        &label_index,
        &hot_tail,
        &frontier,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["21", "new hot error"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn hot_tail_buffer_polls_and_decodes_kafka_wal_records() {
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns: 1_900_000,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        position: None,
    };
    let produced = build_kafka_wal_record("__crabka_observability_logs_wal", &record).unwrap();
    let mut consumer = RecordingWalConsumer::new(vec![vec![
        KafkaWalRecord {
            value: produced.value.unwrap().to_vec(),
            partition: PartitionIndex(2),
            offset: Offset(42),
            timestamp_ms: produced.timestamp_ms,
            headers: produced
                .headers
                .into_iter()
                .map(|header| KafkaWalHeader {
                    key: header.key,
                    value: header.value.map(|value| value.to_vec()),
                })
                .collect(),
        },
        KafkaWalRecord {
            value: b"worker error".to_vec(),
            partition: PartitionIndex(3),
            offset: Offset(7),
            timestamp_ms: Some(2),
            headers: vec![
                kafka_header("crabka-wal-record-type", "log-line"),
                kafka_header("crabka-tenant", "tenant-a"),
                kafka_header("crabka-log-label-app", "worker"),
            ],
        },
    ]]);
    let hot_tail = BufferedLogHotTail::default();

    let decoded = poll_log_hot_tail_once(&mut consumer, &hot_tail, Duration::from_millis(1))
        .await
        .unwrap();

    assert!(decoded == 2);
    assert!(
        hot_tail.records()
            == vec![
                WalLogRecord {
                    position: Some(WalPosition {
                        partition: PartitionIndex(2),
                        offset: Offset(42),
                    }),
                    ..record
                },
                WalLogRecord {
                    tenant: "tenant-a".to_string(),
                    labels: labels([("app", "worker")]),
                    timestamp_ns: 2_000_000,
                    line: "worker error".to_string(),
                    structured_metadata: BTreeMap::new(),
                    position: Some(WalPosition {
                        partition: PartitionIndex(3),
                        offset: Offset(7),
                    }),
                },
            ]
    );
}

#[test]
fn executes_tail_query_over_hot_wal_tail_as_loki_streams_json_frame() {
    let label_index = LabelIndex::default();
    let block_index = BlockIndex::default();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 19,
            line: "api cold error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-b".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 21,
            line: "other tenant error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "worker"), ("env", "prod")]),
            timestamp_ns: 22,
            line: "worker error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
    ];

    let response = execute_tail_query(&plan, &hot_tail, 19);

    assert!(
        response
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["20", "api hot error"]
                        ]
                    }
                ]
            })
    );
}

#[test]
fn executes_tail_query_filters_hot_tail_by_partition_offset_frontier() {
    let label_index = LabelIndex::default();
    let block_index = BlockIndex::default();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "already compacted error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(42),
            }),
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 21,
            line: "new hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(43),
            }),
        },
    ];
    let frontier = CompactionFrontier::new(19).with_partition_offset(PartitionIndex(0), Offset(42));

    let response = execute_tail_query_with_frontier(&plan, &hot_tail, &frontier);

    assert!(
        response
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["21", "new hot error"]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_json_field_filter_over_structured_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                "api ok",
                BTreeMap::from([("status".to_string(), "200".to_string())]),
            ),
            LogRow::new(
                api,
                19,
                "api error",
                BTreeMap::from([("status".to_string(), "500".to_string())]),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | status >= 500"#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "status": "500"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_field_filter_over_original_labels() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_dev = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "dev")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api prod error", BTreeMap::new()),
            LogRow::new(api_dev, 19, "api dev error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | env = "prod""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["10", "api prod error"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_extracted_label_collision_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"env":"stage","status":200}"#, BTreeMap::new()),
            LogRow::new(api, 19, r#"{"env":"dev","status":500}"#, BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | json | env = "prod" | env_extracted = "dev""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "env_extracted": "dev",
                                "status": "500"
                            },
                            "values": [
                                ["19", r#"{"env":"dev","status":500}"#]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_nested_json_field_filter_over_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"{"request":{"method":"GET"},"response":{"status":200}}"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                19,
                r#"{"request":{"method":"GET"},"response":{"status":500}}"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | json | request_method = "GET" | response_status >= 500"#)
            .unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "request_method": "GET",
                                "response_status": "500"
                            },
                            "values": [
                                ["19", r#"{"request":{"method":"GET"},"response":{"status":500}}"#]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_selected_json_field_filter_over_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"{"servers":["10.0.0.1"],"request":{"headers":{"User-Agent":"Agent/1"},"method":"GET"},"status":200}"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                19,
                r#"{"servers":["10.0.0.2"],"request":{"headers":{"User-Agent":"Agent/2"},"method":"POST"},"status":500}"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(
            r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua = "Agent/2""#,
        )
        .unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "first_server": "10.0.0.2",
                                "ua": "Agent/2"
                            },
                            "values": [
                                ["19", r#"{"servers":["10.0.0.2"],"request":{"headers":{"User-Agent":"Agent/2"},"method":"POST"},"status":500}"#]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_logfmt_field_filter_over_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, r#"status=200 msg="api ok""#, BTreeMap::new()),
            LogRow::new(api, 19, r#"status=500 msg="api error""#, BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | logfmt | status >= 500"#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "msg": "api error",
                                "status": "500"
                            },
                            "values": [
                                ["19", r#"status=500 msg="api error""#]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_parameterized_logfmt_field_filter_over_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"status=200 msg="api ok" path=/ready"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                19,
                r#"status=500 msg="api error" path=/api"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | logfmt status, message="msg" | status >= 500"#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "message": "api error",
                                "status": "500"
                            },
                            "values": [
                                ["19", r#"status=500 msg="api error" path=/api"#]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_pattern_parser_over_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "GET /ready (200) 1ms", BTreeMap::new()),
            LogRow::new(
                api,
                19,
                "POST /api/prom/query_range (500) 1.5s",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(
            r#"{app="api"} | pattern `<method> <path> (<status>) <duration>` | status >= 500"#,
        )
        .unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "duration": "1.5s",
                                "env": "prod",
                                "method": "POST",
                                "path": "/api/prom/query_range",
                                "status": "500"
                            },
                            "values": [
                                ["19", "POST /api/prom/query_range (500) 1.5s"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_regexp_parser_over_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "GET /ready (200) 1ms", BTreeMap::new()),
            LogRow::new(
                api,
                19,
                "POST /api/prom/query_range (500) 1.5s",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(
            r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)` | status >= 500"#,
        )
        .unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "duration": "1.5s",
                                "env": "prod",
                                "method": "POST",
                                "path": "/api/prom/query_range",
                                "status": "500"
                            },
                            "values": [
                                ["19", "POST /api/prom/query_range (500) 1.5s"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_unpack_parser_replacing_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"{"container":"myapp","pod":"pod-3223f","_entry":"original log message"}"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                19,
                r#"{"container":"myapp","pod":"pod-3223f","_entry":"container original log message"}"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | unpack != "container" | pod = "pod-3223f""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "container": "myapp",
                                "detected_level": "unknown",
                                "env": "prod",
                                "pod": "pod-3223f"
                            },
                            "values": [
                                ["10", "original log message"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_line_format_replacing_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, r#"status=200 msg="api ok""#, BTreeMap::new()),
            LogRow::new(api, 19, r#"status=500 msg="api error""#, BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(
            r#"{app="api"} | logfmt | line_format `{{.msg}} {{.status}}` |= "api error 500""#,
        )
        .unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "msg": "api error",
                                "status": "500"
                            },
                            "values": [
                                ["19", "api error 500"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_label_format_rewriting_stream_labels() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"method=GET status=200 path=/ready"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                19,
                r#"method=GET status=500 path=/api"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(
            r#"{app="api",env="prod"} | logfmt | label_format namespace=env, summary="{{.method}} {{.status}}" | namespace = "prod" | summary = "GET 500""#,
        )
        .unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "method": "GET",
                                "namespace": "prod",
                                "path": "/api",
                                "status": "500",
                                "summary": "GET 500"
                            },
                            "values": [
                                ["19", "method=GET status=500 path=/api"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_drop_and_keep_rewriting_stream_labels() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"method=GET status=200 level=info path=/ready"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                19,
                r#"method=GET status=500 level=debug path=/api"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(
            r#"{app="api",env="prod"} | logfmt | drop env, level="debug" | keep app, method, status="500" | status = "500""#,
        )
        .unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "method": "GET",
                                "status": "500"
                            },
                            "values": [
                                ["19", "method=GET status=500 level=debug path=/api"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_decolorize_rewriting_line_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "\u{1b}[32mapi ok\u{1b}[0m", BTreeMap::new()),
            LogRow::new(api, 19, "\u{1b}[31mapi error\u{1b}[0m", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | decolorize |= "error" !~ `\x1b\[`"#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_duration_and_bytes_logfmt_field_filters() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                "duration=10ms bytes_consumed=21MB msg=too-fast",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                15,
                "duration=25ms bytes_consumed=19MB msg=too-small",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                19,
                "duration=25ms bytes_consumed=21MB msg=matched",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | logfmt | duration >= 20ms | bytes_consumed > 20MB"#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "bytes_consumed": "21MB",
                                "detected_level": "unknown",
                                "duration": "25ms",
                                "env": "prod",
                                "msg": "matched"
                            },
                            "values": [
                                ["19", "duration=25ms bytes_consumed=21MB msg=matched"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_with_or_logfmt_field_filter_chain() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "status=200 level=info", BTreeMap::new()),
            LogRow::new(api, 15, "status=200 level=warn", BTreeMap::new()),
            LogRow::new(api, 19, "status=500 level=info", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} | logfmt | status >= 500 or level = "warn""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "env": "prod",
                                "level": "info",
                                "status": "500"
                            },
                            "values": [
                                ["19", "status=500 level=info"]
                            ]
                        },
                        {
                            "stream": {
                                "app": "api",
                                "env": "prod",
                                "level": "warn",
                                "status": "200"
                            },
                            "values": [
                                ["15", "status=200 level=warn"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_count_over_time_query_as_loki_matrix_json() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
            LogRow::new(api, 29, "api error again", BTreeMap::new()),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);

    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [30s])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_absent_over_time_query_for_empty_plan() {
    let dir = tempfile::tempdir().unwrap();
    let label_index = LabelIndex::default();
    let block_index = BlockIndex::default();
    let query = parse_metric_query(r#"absent_over_time({app="api",env="prod"} [30s])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            // Real Loki 3.4.2's `absent_over_time` synthesizes a series from the
                            // selector's equality matchers only — no `detected_level` (there is no
                            // log line to detect a level from).
                            "metric": {
                                "app": "api",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn count_over_time_honors_json_parser_error_filters() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("format", "json")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"msg":"api ok"}"#, BTreeMap::new()),
            LogRow::new(api, 20, "not json", BTreeMap::new()),
            LogRow::new(api, 29, r#"{"msg":"api later"}"#, BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"count_over_time({app="api",format="json"} | json | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "format": "json",
                                "msg": "api later"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        },
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "format": "json",
                                "msg": "api ok"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_count_over_time_merging_cold_blocks_with_hot_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [30ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 19,
            line: "api error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
    ];

    let response = execute_metric_query_range_with_hot_tail(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
        &hot_tail,
        19,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_rate_merging_cold_blocks_with_hot_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 10, "api error", BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"rate({app="api"} |= "error" [20s])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-20_000_000_000, 30_000_000_000).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 10,
            line: "api error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
    ];

    let response = execute_metric_query_range_with_hot_tail(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
        &hot_tail,
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.1"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_bytes_rate_merging_cold_blocks_with_hot_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 10, "aa", BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"bytes_rate({app="api"} [20s])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-20_000_000_000, 30_000_000_000).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 10,
            line: "aa".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "bbbb".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
    ];

    let response = execute_metric_query_range_with_hot_tail(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
        &hot_tail,
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.3"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_bytes_over_time_merging_cold_blocks_with_hot_wal_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 10, "aa", BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"bytes_over_time({app="api"} [30ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 10,
            line: "aa".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
        WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "bbbb".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        },
    ];

    let response = execute_metric_query_range_with_hot_tail(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
        &hot_tail,
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "6"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn metric_query_rejects_unfiltered_cold_block_pipeline_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("format", "json")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 20, 20, TimeRange::new(20, 20).unwrap()),
        vec![LogRow::new(api, 20, "not json", BTreeMap::new())],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query =
        parse_metric_query(r#"count_over_time({app="api",format="json"} | json [30ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let error = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("JSONParserErr"));
}

#[tokio::test]
async fn metric_query_rejects_unfiltered_hot_tail_pipeline_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("format", "json")]));
    let block_index = BlockIndex::default();

    let query =
        parse_metric_query(r#"count_over_time({app="api",format="json"} | json [30ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("format", "json")]),
        timestamp_ns: 20,
        line: "not json".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }];

    let error = execute_metric_query_range_with_hot_tail(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
        &hot_tail,
        10,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("JSONParserErr"));
}

#[tokio::test]
async fn executes_parser_metric_query_with_loki_pipeline_labels() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 10, TimeRange::new(10, 10).unwrap()),
        vec![LogRow::new(
            api,
            10,
            r#"{"request":{"method":"GET"},"response":{"status":500}}"#,
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"count_over_time({app="api"} | json | response_status >= 500 [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    let hot_tail = vec![WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns: 20,
        line: r#"{"request":{"method":"GET"},"response":{"status":500}}"#.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }];

    let response = execute_metric_query_range_with_hot_tail(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
        &hot_tail,
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "request_method": "GET",
                                "response_status": "500"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sum_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=7", BTreeMap::new()),
            LogRow::new(api, 20, "cost=5", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "12"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sum_over_time_decimal_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=1.5", BTreeMap::new()),
            LogRow::new(api, 20, "cost=2.25", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "3.75"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sum_over_time_signed_decimal_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=-1.5", BTreeMap::new()),
            LogRow::new(api, 20, "cost=2.25", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.75"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sum_over_time_scientific_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=1.5e2", BTreeMap::new()),
            LogRow::new(api, 20, "cost=-2.5e1", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "125"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sum_over_time_unwrap_bytes_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "size=1KiB", BTreeMap::new()),
            LogRow::new(api, 20, "size=2KiB", BTreeMap::new()),
            LogRow::new(api, 25, "size=wat", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap bytes(size) | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "3072"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sum_over_time_unwrap_duration_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "latency=250ms", BTreeMap::new()),
            LogRow::new(api, 20, "latency=500ms", BTreeMap::new()),
            LogRow::new(api, 25, "latency=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap duration(latency) | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.75"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_rate_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=7", BTreeMap::new()),
            LogRow::new(api, 20, "cost=5", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query =
        parse_metric_query(r#"rate({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#)
            .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.4"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_rate_counter_unwrap_metric_query_with_reset() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "requests=3", BTreeMap::new()),
            LogRow::new(api, 15, "requests=9", BTreeMap::new()),
            LogRow::new(api, 20, "requests=2", BTreeMap::new()),
            LogRow::new(api, 25, "requests=5", BTreeMap::new()),
            LogRow::new(api, 29, "requests=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"rate_counter({app="api"} | logfmt | unwrap requests | __error__ = "" [30s])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.366666666"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_avg_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=7", BTreeMap::new()),
            LogRow::new(api, 20, "cost=5", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "6"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_metric_query_with_range_offset() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 5, 30, TimeRange::new(5, 30).unwrap()),
        vec![
            LogRow::new(api, 5, "api error old one", BTreeMap::new()),
            LogRow::new(api, 10, "api error old two", BTreeMap::new()),
            LogRow::new(api, 25, "api error current", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [10ns] offset 20ns)"#)
        .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_avg_over_time_unwrap_metric_query_with_range_grouping() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let pod_a = label_index.insert_series("tenant-a", labels([("app", "api"), ("pod", "a")]));
    let pod_b = label_index.insert_series("tenant-a", labels([("app", "api"), ("pod", "b")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(pod_a, 10, "cost=2", BTreeMap::new()),
            LogRow::new(pod_a, 12, "cost=4", BTreeMap::new()),
            LogRow::new(pod_b, 20, "cost=100", BTreeMap::new()),
            LogRow::new(pod_b, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns]) by (app)"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api"
                            },
                            "values": [
                                [0.00000003, "35.333333333"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stdvar_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=2", BTreeMap::new()),
            LogRow::new(api, 20, "cost=4", BTreeMap::new()),
            LogRow::new(api, 25, "cost=4", BTreeMap::new()),
            LogRow::new(api, 29, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"stdvar_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.888888888"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stddev_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=2", BTreeMap::new()),
            LogRow::new(api, 20, "cost=4", BTreeMap::new()),
            LogRow::new(api, 25, "cost=4", BTreeMap::new()),
            LogRow::new(api, 29, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"stddev_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0.942809041"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_quantile_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=1", BTreeMap::new()),
            LogRow::new(api, 15, "cost=2", BTreeMap::new()),
            LogRow::new(api, 20, "cost=10", BTreeMap::new()),
            LogRow::new(api, 25, "cost=100", BTreeMap::new()),
            LogRow::new(api, 29, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"quantile_over_time(0.75, {app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "32.5"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_min_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=7", BTreeMap::new()),
            LogRow::new(api, 20, "cost=5", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"min_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "5"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_max_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=7", BTreeMap::new()),
            LogRow::new(api, 20, "cost=5", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"max_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "7"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_first_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=7", BTreeMap::new()),
            LogRow::new(api, 20, "cost=5", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"first_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "7"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_last_over_time_unwrap_metric_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "cost=7", BTreeMap::new()),
            LogRow::new(api, 20, "cost=5", BTreeMap::new()),
            LogRow::new(api, 25, "cost=bad", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"last_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30ns])"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query(dir.path(), &plan, &query, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "5"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_count_over_time_query_with_stepped_matrix_samples() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "api error one", BTreeMap::new()),
            LogRow::new(api, 19, "api error two", BTreeMap::new()),
            LogRow::new(api, 29, "api error three", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [20ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-10, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(10, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "1"],
                                [0.00000002, "2"],
                                [0.00000003, "2"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_present_over_time_query_with_stepped_matrix_samples() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "api error one", BTreeMap::new()),
            LogRow::new(api, 19, "api error two", BTreeMap::new()),
            LogRow::new(api, 29, "api error three", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"present_over_time({app="api"} |= "error" [20ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-10, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(10, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "1"],
                                [0.00000002, "1"],
                                [0.00000003, "1"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_rate_query_with_stepped_matrix_samples() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "api error one", BTreeMap::new()),
            LogRow::new(api, 19, "api error two", BTreeMap::new()),
            LogRow::new(api, 29, "api error three", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"rate({app="api"} |= "error" [20s])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-20_000_000_000, 30_000_000_000).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(10, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "0.05"],
                                [0.00000002, "0.1"],
                                [0.00000003, "0.15"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_bytes_over_time_query_with_stepped_matrix_samples() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "aa", BTreeMap::new()),
            LogRow::new(api, 19, "bbb", BTreeMap::new()),
            LogRow::new(api, 29, "cccc", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"bytes_over_time({app="api"} [20ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-10, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(10, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "2"],
                                [0.00000002, "5"],
                                [0.00000003, "7"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_bytes_rate_query_with_stepped_matrix_samples() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api, 10, "aa", BTreeMap::new()),
            LogRow::new(api, 19, "bbb", BTreeMap::new()),
            LogRow::new(api, 29, "cccc", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(r#"bytes_rate({app="api"} [20s])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-20_000_000_000, 30_000_000_000).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(10, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "0.1"],
                                [0.00000002, "0.25"],
                                [0.00000003, "0.45"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sum_by_vector_aggregation_with_stepped_matrix_samples() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let pod_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "a")]),
    );
    let pod_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "b")]),
    );

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(pod_a, 10, "api error one", BTreeMap::new()),
            LogRow::new(pod_b, 19, "api error two", BTreeMap::new()),
            LogRow::new(pod_a, 29, "api error three", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query =
        parse_metric_query(r#"sum by (env) (count_over_time({app="api"} |= "error" [20ns]))"#)
            .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-10, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(10, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "1"],
                                [0.00000002, "2"],
                                [0.00000003, "2"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_avg_without_vector_aggregation_with_stepped_matrix_samples() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let pod_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "a")]),
    );
    let pod_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "b")]),
    );

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(pod_a, 10, "aa", BTreeMap::new()),
            LogRow::new(pod_b, 19, "bbbb", BTreeMap::new()),
            LogRow::new(pod_a, 29, "cccccc", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let query = parse_metric_query(
        r#"avg without (pod) (bytes_over_time({app="api", env="prod"} [20ns]))"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-10, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(10, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "2"],
                                [0.00000002, "3"],
                                [0.00000003, "5"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_count_min_and_max_vector_aggregations() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let pod_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "a")]),
    );
    let pod_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "b")]),
    );

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(pod_a, 10, "api error one", BTreeMap::new()),
            LogRow::new(pod_a, 19, "api error two", BTreeMap::new()),
            LogRow::new(pod_b, 19, "api error three", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let cases = [
        (
            r#"count by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            json!([[0.00000001, "1"], [0.00000002, "2"], [0.00000003, "2"]]),
        ),
        (
            r#"min by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            json!([[0.00000001, "1"], [0.00000002, "1"], [0.00000003, "1"]]),
        ),
        (
            r#"max by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            json!([[0.00000001, "1"], [0.00000002, "2"], [0.00000003, "1"]]),
        ),
    ];

    for (query, expected_values) in cases {
        let query = parse_metric_query(query).unwrap();
        let plan = plan_stream_query(
            "tenant-a",
            TimeRange::new(-10, 30).unwrap(),
            query.stream.clone(),
            &label_index,
            &block_index,
        )
        .unwrap();

        let response = execute_metric_query_range(
            dir.path(),
            &plan,
            &query,
            &label_index,
            TimeRange::new(10, 30).unwrap(),
            10,
        )
        .await
        .unwrap();

        assert!(
            response
                == json!({
                    "status": "success",
                    "data": {
                        "resultType": "matrix",
                        "result": [
                            {
                                "metric": {
                                    "env": "prod"
                                },
                                "values": expected_values
                            }
                        ]
                    }
                })
        );
    }
}

#[tokio::test]
async fn executes_count_values_vector_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let pod_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "a")]),
    );
    let pod_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "b")]),
    );
    let pod_c = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "c")]),
    );

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(pod_a, 11, "api error one", BTreeMap::new()),
            LogRow::new(pod_b, 12, "api error two", BTreeMap::new()),
            LogRow::new(pod_b, 13, "api error three", BTreeMap::new()),
            LogRow::new(pod_c, 14, "api error four", BTreeMap::new()),
            LogRow::new(pod_c, 15, "api error five", BTreeMap::new()),
            LogRow::new(pod_c, 16, "api error six", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let query = parse_metric_query(
        r#"count_values by (env) ("events", count_over_time({app="api"} |= "error" [20ns]))"#,
    )
    .unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(-10, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_metric_query_range(
        dir.path(),
        &plan,
        &query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        10,
    )
    .await
    .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "env": "prod",
                                "events": "1"
                            },
                            "values": [[0.00000003, "1"]]
                        },
                        {
                            "metric": {
                                "env": "prod",
                                "events": "2"
                            },
                            "values": [[0.00000003, "1"]]
                        },
                        {
                            "metric": {
                                "env": "prod",
                                "events": "3"
                            },
                            "values": [[0.00000003, "1"]]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_stdvar_and_stddev_vector_aggregations() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let pod_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "a")]),
    );
    let pod_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "b")]),
    );
    let pod_c = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "c")]),
    );

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(pod_a, 11, "api error one", BTreeMap::new()),
            LogRow::new(pod_b, 12, "api error two", BTreeMap::new()),
            LogRow::new(pod_b, 13, "api error three", BTreeMap::new()),
            LogRow::new(pod_c, 14, "api error four", BTreeMap::new()),
            LogRow::new(pod_c, 15, "api error five", BTreeMap::new()),
            LogRow::new(pod_c, 16, "api error six", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let cases = [
        (
            r#"stdvar by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            "0.666666666",
        ),
        (
            r#"stddev by (env) (count_over_time({app="api"} |= "error" [20ns]))"#,
            "0.81649658",
        ),
    ];

    for (query, expected_value) in cases {
        let query = parse_metric_query(query).unwrap();
        let plan = plan_stream_query(
            "tenant-a",
            TimeRange::new(-10, 30).unwrap(),
            query.stream.clone(),
            &label_index,
            &block_index,
        )
        .unwrap();

        let response = execute_metric_query_range(
            dir.path(),
            &plan,
            &query,
            &label_index,
            TimeRange::new(30, 30).unwrap(),
            10,
        )
        .await
        .unwrap();

        assert!(
            response
                == json!({
                    "status": "success",
                    "data": {
                        "resultType": "matrix",
                        "result": [
                            {
                                "metric": {
                                    "env": "prod"
                                },
                                "values": [
                                    [0.00000003, expected_value]
                                ]
                            }
                        ]
                    }
                })
        );
    }
}

#[tokio::test]
async fn executes_topk_and_bottomk_vector_aggregations() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let api_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "a")]),
    );
    let api_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "b")]),
    );
    let worker_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "stage"), ("pod", "a")]),
    );
    let worker_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "stage"), ("pod", "b")]),
    );

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(api_a, 10, "api error a1", BTreeMap::new()),
            LogRow::new(api_a, 11, "api error a2", BTreeMap::new()),
            LogRow::new(api_b, 12, "api error b1", BTreeMap::new()),
            LogRow::new(api_b, 13, "api error b2", BTreeMap::new()),
            LogRow::new(api_b, 14, "api error b3", BTreeMap::new()),
            LogRow::new(worker_a, 15, "api error c1", BTreeMap::new()),
            LogRow::new(worker_b, 16, "api error d1", BTreeMap::new()),
            LogRow::new(worker_b, 17, "api error d2", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    let topk_query =
        parse_metric_query(r#"topk by (env) (1, count_over_time({app="api"} |= "error" [30ns]))"#)
            .unwrap();
    let topk_plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        topk_query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let topk_response = execute_metric_query_range(
        dir.path(),
        &topk_plan,
        &topk_query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
    )
    .await
    .unwrap();

    assert!(
        topk_response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "pod": "b"
                            },
                            "values": [
                                [0.00000003, "3"]
                            ]
                        },
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage",
                                "pod": "b"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ]
                }
            })
    );

    let approx_topk_query =
        parse_metric_query(r#"approx_topk(2, count_over_time({app="api"} |= "error" [30ns]))"#)
            .unwrap();
    let approx_topk_plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        approx_topk_query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let approx_topk_response = execute_metric_query_range(
        dir.path(),
        &approx_topk_plan,
        &approx_topk_query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
    )
    .await
    .unwrap();

    assert!(
        approx_topk_response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "pod": "a"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        },
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "pod": "b"
                            },
                            "values": [
                                [0.00000003, "3"]
                            ]
                        }
                    ]
                }
            })
    );

    let bottomk_query =
        parse_metric_query(r#"bottomk(2, count_over_time({app="api"} |= "error" [30ns]))"#)
            .unwrap();
    let bottomk_plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        bottomk_query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let bottomk_response = execute_metric_query_range(
        dir.path(),
        &bottomk_plan,
        &bottomk_query,
        &label_index,
        TimeRange::new(30, 30).unwrap(),
        1,
    )
    .await
    .unwrap();

    assert!(
        bottomk_response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "pod": "a"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        },
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage",
                                "pod": "a"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn executes_sort_and_sort_desc_vector_aggregations() {
    let dir = tempfile::tempdir().unwrap();
    let mut label_index = LabelIndex::default();
    let pod_a = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "a")]),
    );
    let pod_b = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "b")]),
    );
    let pod_c = label_index.insert_series(
        "tenant-a",
        labels([("app", "api"), ("env", "prod"), ("pod", "c")]),
    );

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 29, TimeRange::new(10, 29).unwrap()),
        vec![
            LogRow::new(pod_a, 10, "api error a1", BTreeMap::new()),
            LogRow::new(pod_a, 11, "api error a2", BTreeMap::new()),
            LogRow::new(pod_b, 12, "api error b1", BTreeMap::new()),
            LogRow::new(pod_c, 13, "api error c1", BTreeMap::new()),
            LogRow::new(pod_c, 14, "api error c2", BTreeMap::new()),
            LogRow::new(pod_c, 15, "api error c3", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);

    for (query, expected_pods) in [
        (
            r#"sort(count_over_time({app="api"} |= "error" [30ns]))"#,
            ["b", "a", "c"],
        ),
        (
            r#"sort_desc(count_over_time({app="api"} |= "error" [30ns]))"#,
            ["c", "a", "b"],
        ),
    ] {
        let query = parse_metric_query(query).unwrap();
        let plan = plan_stream_query(
            "tenant-a",
            TimeRange::new(0, 30).unwrap(),
            query.stream.clone(),
            &label_index,
            &block_index,
        )
        .unwrap();

        let response = execute_metric_query_range(
            dir.path(),
            &plan,
            &query,
            &label_index,
            TimeRange::new(30, 30).unwrap(),
            1,
        )
        .await
        .unwrap();

        let pods = response["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|series| series["metric"]["pod"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(pods == expected_pods);
    }
}

#[tokio::test]
async fn empty_stream_plan_returns_empty_loki_streams_result() {
    let dir = tempfile::tempdir().unwrap();
    let label_index = LabelIndex::default();
    let block_index = BlockIndex::default();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"}"#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query(dir.path(), &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": []
                }
            })
    );
}

#[tokio::test]
async fn executes_stream_query_over_object_store_blocks_as_loki_json() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let worker_block = write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .await
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query_from_object_store(store, &prefix, &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test]
async fn object_store_stream_query_returns_partial_result_with_warning_for_unreadable_block() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let readable_block = write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .await
    .unwrap();
    let missing_block = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([api]),
    );

    let mut block_index = BlockIndex::default();
    block_index.insert(readable_block);
    block_index.insert(missing_block);

    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        parse_query(r#"{app="api"} |= "error""#).unwrap(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response = execute_stream_query_from_object_store(store, &prefix, &plan, &label_index)
        .await
        .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ]
                },
                "warnings": [
                    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet"
                ]
            })
    );
}

#[tokio::test]
async fn object_store_metric_query_returns_partial_result_with_warning_for_unreadable_block() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let readable_block = write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let missing_block = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([api]),
    );

    let mut block_index = BlockIndex::default();
    block_index.insert(readable_block);
    block_index.insert(missing_block);
    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [30ns])"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();

    let response =
        execute_metric_query_from_object_store(store, &prefix, &plan, &query, &label_index)
            .await
            .unwrap();

    assert!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ]
                },
                "warnings": [
                    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet"
                ]
            })
    );
}

struct RecordingWalConsumer {
    batches: Vec<Vec<KafkaWalRecord>>,
}

impl RecordingWalConsumer {
    fn new(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl LogWalConsumer for RecordingWalConsumer {
    async fn poll(&mut self, _timeout: Duration) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if self.batches.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self.batches.remove(0))
        }
    }

    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        Ok(())
    }
}

fn kafka_header(key: &str, value: &str) -> KafkaWalHeader {
    KafkaWalHeader {
        key: key.to_string(),
        value: Some(value.as_bytes().to_vec()),
    }
}

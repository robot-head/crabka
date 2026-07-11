use std::collections::BTreeSet;

use crabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, TimeRange, labels,
};
use crabka_logql::{LineFilterOp, PipelineStage, parse_query, plan_stream_query};

#[test]
fn stream_planner_prunes_series_and_blocks_before_line_filters() {
    let mut labels_index = LabelIndex::default();
    let api_prod =
        labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_dev = labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "dev")]));
    let worker_prod =
        labels_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    labels_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api_prod]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([api_dev]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 1, 30, 39, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([worker_prod]),
    ));

    let query = parse_query(r#"{app="api", env!="dev"} |= "error""#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(150, 250).unwrap(),
        query,
        &labels_index,
        &blocks,
    )
    .unwrap();

    assert2::assert!(plan.fingerprints == BTreeSet::from([api_prod]));
    assert2::assert!(
        plan.blocks
            .iter()
            .map(|block| block.key.object_key())
            .collect::<Vec<_>>()
            == vec!["tenant=tenant-a/partition=0/offsets=10-19/time=100-199.parquet".to_string(),]
    );
    assert2::assert!(matches!(
        &plan.query.pipeline[..],
        [PipelineStage::LineFilter(filter)] if filter.op == LineFilterOp::Contains && filter.pattern == "error"
    ));
}

#[test]
fn stream_planner_keeps_regex_and_negative_matchers_in_index_filter() {
    let mut labels_index = LabelIndex::default();
    let api_prod =
        labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let web_prod =
        labels_index.insert_series("tenant-a", labels([("app", "web"), ("env", "prod")]));
    let admin_prod =
        labels_index.insert_series("tenant-a", labels([("app", "admin"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 1, 10, TimeRange::new(10, 20).unwrap()),
        BTreeSet::from([api_prod, web_prod, admin_prod]),
    ));

    let query = parse_query(r#"{app=~"api|web|admin", app!~"admin"} !~ "debug""#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(1, 30).unwrap(),
        query,
        &labels_index,
        &blocks,
    )
    .unwrap();

    assert2::assert!(plan.fingerprints == BTreeSet::from([api_prod, web_prod]));
    assert2::assert!(plan.blocks.len() == 1);
}

#[test]
fn stream_planner_treats_empty_compatible_regex_matcher_as_matching_absent_label() {
    let mut labels_index = LabelIndex::default();
    let api_without_env = labels_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_prod =
        labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker_prod =
        labels_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 1, 10, TimeRange::new(10, 20).unwrap()),
        BTreeSet::from([api_without_env, api_prod, worker_prod]),
    ));

    let query = parse_query(r#"{app="api", env=~".*"}"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(1, 30).unwrap(),
        query,
        &labels_index,
        &blocks,
    )
    .unwrap();

    assert2::assert!(plan.fingerprints == BTreeSet::from([api_without_env, api_prod]));
    assert2::assert!(plan.blocks.len() == 1);
}

#[test]
fn stream_planner_anchors_regex_label_matchers() {
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series("tenant-a", labels([("app", "api")]));
    let worker = labels_index.insert_series("tenant-a", labels([("app", "worker")]));
    let prefixed_api = labels_index.insert_series("tenant-a", labels([("app", "myapi")]));
    let suffixed_api = labels_index.insert_series("tenant-a", labels([("app", "api-v2")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 1, 10, TimeRange::new(10, 20).unwrap()),
        BTreeSet::from([api, worker, prefixed_api, suffixed_api]),
    ));

    let query = parse_query(r#"{app=~"api|worker"}"#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(1, 30).unwrap(),
        query,
        &labels_index,
        &blocks,
    )
    .unwrap();

    assert2::assert!(plan.fingerprints == BTreeSet::from([api, worker]));
    assert2::assert!(plan.blocks.len() == 1);
}

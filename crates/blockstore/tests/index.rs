use std::collections::BTreeSet;

use assert2::{assert, check};
use crabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LabelPredicate, LogBlockIndex as BlockIndex,
    LogMatchOp as MatchOp, TimeRange, labels, series_fingerprint,
};

#[test]
fn series_fingerprint_is_stable_across_label_ordering() {
    let left = labels([("env", "prod"), ("app", "api")]);
    let right = labels([("app", "api"), ("env", "prod")]);

    assert!(series_fingerprint(&left) == series_fingerprint(&right));
}

#[test]
fn label_index_is_tenant_scoped_and_applies_all_matcher_ops() {
    let mut index = LabelIndex::default();
    let api_prod = index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_dev = index.insert_series("tenant-a", labels([("app", "api"), ("env", "dev")]));
    let worker_prod = index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let matched = index.match_series(
        "tenant-a",
        &[
            LabelPredicate::new("app", MatchOp::RegexEqual, "api|worker").unwrap(),
            LabelPredicate::new("env", MatchOp::NotEqual, "dev").unwrap(),
            LabelPredicate::new("app", MatchOp::RegexNotEqual, "admin").unwrap(),
        ],
    );

    assert!(matched == BTreeSet::from([api_prod, worker_prod]));
    assert!(!matched.contains(&api_dev));
}

#[test]
fn label_predicate_equal_matches_only_identical_label_value() {
    let predicate = LabelPredicate::new("app", MatchOp::Equal, "api").unwrap();
    let matching = labels([("app", "api"), ("env", "prod")]);
    let different = labels([("app", "worker"), ("env", "prod")]);

    for (candidate, want) in [
        (matching, true),
        (different, false),
        (labels([("env", "prod")]), false),
    ] {
        assert!(predicate.matches(&candidate) == want, "for {candidate:?}");
    }
}

#[test]
fn label_index_exact_match_requires_posting_candidate_set() {
    let mut index = LabelIndex::default();
    let api = index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker = index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let matched = index.match_series(
        "tenant-a",
        &[LabelPredicate::new("app", MatchOp::Equal, "api").unwrap()],
    );

    assert!(matched == BTreeSet::from([api]));
    assert!(!matched.contains(&worker));
}

#[test]
fn label_metadata_is_tenant_scoped() {
    let mut index = LabelIndex::default();
    index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    index.insert_series(
        "tenant-b",
        labels([("service", "billing"), ("env", "prod")]),
    );

    check!(index.label_names("tenant-a") == BTreeSet::from(["app".into(), "env".into()]));
    check!(index.label_values("tenant-a", "app") == BTreeSet::from(["api".into()]));
    check!(index.label_names("tenant-b") == BTreeSet::from(["env".into(), "service".into()]));
    check!(index.label_values("tenant-b", "app").is_empty());
}

#[test]
fn label_index_returns_labels_and_tenant_series() {
    let mut index = LabelIndex::default();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let worker_labels = labels([("app", "worker"), ("env", "prod")]);
    let api = index.insert_series("tenant-a", api_labels.clone());
    let worker = index.insert_series("tenant-a", worker_labels.clone());
    index.insert_series("tenant-b", labels([("app", "api")]));

    for (tenant, series_id, want) in [
        ("tenant-a", api, Some(&api_labels)),
        ("tenant-a", worker, Some(&worker_labels)),
        ("tenant-a", 0, None),
        ("tenant-b", api, None),
    ] {
        assert!(
            index.labels_for(tenant, series_id) == want,
            "labels_for({tenant}, {series_id})"
        );
    }

    let series = index.tenant_series("tenant-a");
    check!(series.len() == 2);
    check!(series.contains(&(api, api_labels)));
    check!(series.contains(&(worker, worker_labels)));
    check!(index.tenant_series("missing").is_empty());
}

#[test]
fn block_index_prunes_by_tenant_time_and_fingerprint() {
    let mut series = LabelIndex::default();
    let api = series.insert_series("tenant-a", labels([("app", "api")]));
    let worker = series.insert_series("tenant-a", labels([("app", "worker")]));
    let other_tenant_api = series.insert_series("tenant-b", labels([("app", "api")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 21, 30, TimeRange::new(300, 399).unwrap()),
        BTreeSet::from([worker]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-b", 1, 10, 20, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([other_tenant_api]),
    ));

    let matched = blocks.match_blocks("tenant-a", TimeRange::new(150, 350).unwrap(), &[api]);

    assert!(matched.len() == 1);
    assert!(
        matched[0].key.object_key()
            == "tenant=tenant-a/partition=0/offsets=10-20/time=100-199.parquet"
    );
}

#[test]
fn block_index_blocks_exposes_inserted_descriptors_in_key_order() {
    let mut blocks = BlockIndex::default();
    let first = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([1]),
    );
    let second = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 21, 30, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([2]),
    );

    blocks.insert(second.clone());
    blocks.insert(first.clone());

    assert!(blocks.blocks() == &[first, second]);
}

#[test]
fn deterministic_block_keys_encode_compactor_idempotency_fields() {
    let key = BlockKey::new("tenant-a", 2, 42, 99, TimeRange::new(1_000, 2_000).unwrap());

    assert!(key.object_key() == "tenant=tenant-a/partition=2/offsets=42-99/time=1000-2000.parquet");
}

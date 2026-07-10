use std::collections::BTreeSet;

use assert2::assert;
use crabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, TimeRange, labels,
    log_tenant_index_manifest_object_path, log_tenant_index_shard_catalog_object_path,
    log_tenant_index_shard_manifest_object_path, read_log_index_manifest,
    read_log_index_manifest_from_object_store, read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store, read_tenant_log_index_shards_from_object_store,
    write_log_index_manifest, write_log_index_manifest_to_object_store,
    write_tenant_log_index_manifest_to_object_store, write_tenant_log_index_shard_to_object_store,
    write_tenant_log_index_shards_to_object_store,
};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};

#[test]
fn log_index_manifest_round_trips_label_and_block_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        labels_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    labels_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([worker]),
    ));

    write_log_index_manifest(dir.path(), &labels_index, &blocks).unwrap();
    let (loaded_labels, loaded_blocks) = read_log_index_manifest(dir.path()).unwrap();

    assert_eq!(loaded_labels, labels_index);
    assert_eq!(loaded_blocks, blocks);
}

#[tokio::test]
async fn log_index_manifest_round_trips_through_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        labels_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([worker]),
    ));

    write_log_index_manifest_to_object_store(&store, &prefix, &labels_index, &blocks)
        .await
        .unwrap();
    let (loaded_labels, loaded_blocks) = read_log_index_manifest_from_object_store(&store, &prefix)
        .await
        .unwrap();

    assert_eq!(loaded_labels, labels_index);
    assert_eq!(loaded_blocks, blocks);
}

#[test]
fn tenant_log_index_manifest_object_path_is_tenant_prefixed() {
    let prefix = ObjectPath::from("observability/logs");

    assert!(
        log_tenant_index_manifest_object_path(&prefix, "tenant-a").to_string()
            == "observability/logs/tenant=tenant-a/index/logs/manifest.json"
    );
}

#[test]
fn tenant_log_index_shard_manifest_object_path_is_tenant_and_time_prefixed() {
    let prefix = ObjectPath::from("observability/logs");

    assert!(
        log_tenant_index_shard_manifest_object_path(
            &prefix,
            "tenant-a",
            TimeRange::new(100, 199).unwrap(),
        )
        .to_string()
            == "observability/logs/tenant=tenant-a/index/logs/shards/time=100-199/manifest.json"
    );
}

#[test]
fn tenant_log_index_shard_catalog_object_path_is_tenant_prefixed() {
    let prefix = ObjectPath::from("observability/logs");

    assert!(
        log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a").to_string()
            == "observability/logs/tenant=tenant-a/index/logs/shards/manifest.json"
    );
}

#[tokio::test]
async fn tenant_log_index_manifest_round_trips_only_one_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let selected_api =
        labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let other_tenant_api =
        labels_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([selected_api]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-b", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([other_tenant_api]),
    ));

    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &labels_index,
        &blocks,
    )
    .await
    .unwrap();
    let (loaded_labels, loaded_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    let expected_selected = blocks.match_blocks(
        "tenant-a",
        TimeRange::new(0, 1_000).unwrap(),
        &[selected_api],
    );

    assert_eq!(
        loaded_labels.label_values("tenant-a", "app"),
        BTreeSet::from(["api".into()])
    );
    assert_eq!(
        loaded_labels.label_values("tenant-b", "app"),
        BTreeSet::new()
    );
    assert_eq!(
        loaded_blocks.match_blocks(
            "tenant-a",
            TimeRange::new(0, 1_000).unwrap(),
            &[selected_api],
        ),
        expected_selected
    );
    assert_eq!(
        loaded_blocks.match_blocks(
            "tenant-b",
            TimeRange::new(0, 1_000).unwrap(),
            &[other_tenant_api],
        ),
        Vec::new()
    );
}

#[tokio::test]
async fn tenant_log_index_shard_round_trips_only_matching_time_and_series() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        labels_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let admin = labels_index.insert_series("tenant-a", labels([("app", "admin"), ("env", "prod")]));
    let other_tenant_api =
        labels_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([worker]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 30, 39, TimeRange::new(400, 499).unwrap()),
        BTreeSet::from([admin]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-b", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([other_tenant_api]),
    ));

    let shard_range = TimeRange::new(150, 250).unwrap();
    write_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        shard_range,
        &labels_index,
        &blocks,
    )
    .await
    .unwrap();
    let (loaded_labels, loaded_blocks) =
        read_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", shard_range)
            .await
            .unwrap();
    let expected_blocks = blocks.match_blocks("tenant-a", TimeRange::new(150, 250).unwrap(), &[]);

    assert_eq!(
        loaded_labels.label_values("tenant-a", "app"),
        BTreeSet::from(["api".into(), "worker".into()])
    );
    assert_eq!(
        loaded_labels.label_values("tenant-b", "app"),
        BTreeSet::new()
    );
    assert_eq!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 1_000).unwrap(), &[]),
        expected_blocks
    );
    assert_eq!(loaded_labels.labels_for("tenant-a", admin), None);
}

#[tokio::test]
async fn tenant_log_index_shard_catalog_selects_overlapping_shards_and_merges_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("tenant-indexes");
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        labels_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let admin = labels_index.insert_series("tenant-a", labels([("app", "admin"), ("env", "prod")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([worker]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 30, 39, TimeRange::new(400, 499).unwrap()),
        BTreeSet::from([admin]),
    ));

    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[
            TimeRange::new(100, 199).unwrap(),
            TimeRange::new(200, 299).unwrap(),
            TimeRange::new(400, 499).unwrap(),
        ],
        &labels_index,
        &blocks,
    )
    .await
    .unwrap();
    let (loaded_labels, loaded_blocks) = read_tenant_log_index_shards_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(150, 250).unwrap(),
    )
    .await
    .unwrap();
    let expected_blocks = blocks.match_blocks("tenant-a", TimeRange::new(150, 250).unwrap(), &[]);

    assert_eq!(
        loaded_labels.label_values("tenant-a", "app"),
        BTreeSet::from(["api".into(), "worker".into()])
    );
    assert_eq!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 1_000).unwrap(), &[]),
        expected_blocks
    );
    assert_eq!(loaded_labels.labels_for("tenant-a", admin), None);
}

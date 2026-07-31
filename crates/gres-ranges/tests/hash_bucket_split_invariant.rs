//! Every row of a hash bucket must live on exactly one range.
//!
//! Hash-equality routing probes a bucket at rowid 0 and answers for the whole
//! bucket, so a boundary that falls between two rows of one bucket sends a
//! value to a range that does not store all of its rows.

use assert2::assert;
use crabka_gres_ranges::{
    CoLocationGroup, HashShardSpec, MapEpoch, MapValidationError, RangeId, RangeKey, RangeMap,
    RangeSpec, TableId, TenantName,
};
use proptest::prelude::{ProptestConfig, any, prop_assert_eq, proptest};

const TABLE: TableId = TableId::new(70);
const SIBLING: TableId = TableId::new(71);
const ROW_SHARDED: TableId = TableId::new(60);
const BUCKET_COUNT: u32 = 16;
const VALUE: &[u8] = b"alice";

fn tenant() -> TenantName {
    TenantName::parse("tenant_a").unwrap()
}

fn spec() -> HashShardSpec {
    HashShardSpec::new(TABLE, vec!["id".into()], BUCKET_COUNT, None).unwrap()
}

fn map_with_boundaries(boundaries: &[RangeKey]) -> Result<RangeMap, MapValidationError> {
    map_with_boundaries_and_groups(boundaries, Vec::new())
}

fn map_with_boundaries_and_groups(
    boundaries: &[RangeKey],
    groups: Vec<CoLocationGroup>,
) -> Result<RangeMap, MapValidationError> {
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut start = RangeKey::MIN;
    for (index, boundary) in boundaries.iter().copied().enumerate() {
        let range_id = u32::try_from(index).unwrap();
        ranges.push(RangeSpec::for_interval(
            RangeId::new(range_id),
            start,
            Some(boundary),
        ));
        start = boundary;
    }
    let last = u32::try_from(boundaries.len()).unwrap();
    ranges.push(RangeSpec::for_interval(RangeId::new(last), start, None));

    RangeMap::new_with_co_location(tenant(), MapEpoch::new(1), ranges, groups)
}

fn co_location_group() -> CoLocationGroup {
    CoLocationGroup {
        name: "orders".into(),
        tables: vec![TABLE, SIBLING],
        bucket_count: BUCKET_COUNT,
    }
}

// Co-location demands both group tables put bucket `b` on one range, and range
// keys are table major, so a group map may only be cut outside the group.
fn co_located_map() -> RangeMap {
    map_with_boundaries_and_groups(
        &[RangeKey::table_start(TableId::new(90))],
        vec![co_location_group()],
    )
    .unwrap()
}

#[test]
fn mid_bucket_boundary_makes_hash_routing_disagree_with_row_ownership() {
    let spec = spec();
    let bucket = spec.bucket_for_value(VALUE);
    let boundary = RangeKey::hash(TABLE, bucket, 9);
    let range_map = map_with_boundaries(&[boundary]).unwrap();

    let routed = range_map.route_hash_equality(&spec, VALUE).unwrap();
    let low_row = range_map.range_for_hash_bucket(TABLE, bucket, 0).unwrap();
    let high_row = range_map.range_for_hash_bucket(TABLE, bucket, 100).unwrap();

    // The harm: one bucket, two owners, and equality routing only sees the first.
    assert!(routed.range_id == low_row.range_id);
    assert!(routed.range_id != high_row.range_id);

    assert!(
        range_map.validate_hash_shard_boundaries(&spec)
            == Err(MapValidationError::HashBucketSplitBoundary { boundary })
    );
}

#[test]
fn bucket_aligned_map_passes_hash_shard_validation() {
    let range_map = map_with_boundaries(&[
        RangeKey::hash_bucket_start(TABLE, 4),
        RangeKey::hash_bucket_start(TABLE, 9),
    ])
    .unwrap();

    assert!(range_map.validate_hash_shard_boundaries(&spec()) == Ok(()));
}

#[test]
fn hash_shard_validation_ignores_row_boundaries_of_other_tables() {
    let range_map = map_with_boundaries(&[
        RangeKey::new(ROW_SHARDED, 500),
        RangeKey::hash_bucket_start(TABLE, 4),
        RangeKey::hash(SIBLING, 2, 700),
    ])
    .unwrap();

    assert!(range_map.validate_hash_shard_boundaries(&spec()) == Ok(()));
}

#[test]
fn plan_hash_split_at_key_rejects_a_mid_bucket_split_point() {
    let range_map = map_with_boundaries(&[RangeKey::hash_bucket_start(TABLE, 4)]).unwrap();
    let split_at = RangeKey::hash(TABLE, 9, 4096);

    let error = range_map
        .plan_hash_split_at_key(&spec(), RangeId::new(1), split_at, RangeId::new(7))
        .unwrap_err();

    assert!(error == MapValidationError::HashBucketSplitBoundary { boundary: split_at });
}

#[test]
fn plan_hash_split_at_key_rejects_a_bucket_the_table_does_not_have() {
    let range_map = map_with_boundaries(&[RangeKey::hash_bucket_start(TABLE, 4)]).unwrap();
    let split_at = RangeKey::hash_bucket_start(TABLE, BUCKET_COUNT);

    let error = range_map
        .plan_hash_split_at_key(&spec(), RangeId::new(1), split_at, RangeId::new(7))
        .unwrap_err();

    // The registry stores boundaries below the bucket count only, so this split
    // would be refused there, and the successor it opens would own no rows.
    assert!(
        error
            == MapValidationError::HashBucketOutOfRange {
                boundary: split_at,
                bucket_count: BUCKET_COUNT,
            }
    );
}

#[test]
fn plan_hash_split_at_key_accepts_the_last_bucket_of_the_table() {
    let range_map = map_with_boundaries(&[RangeKey::hash_bucket_start(TABLE, 4)]).unwrap();
    let split_at = RangeKey::hash_bucket_start(TABLE, BUCKET_COUNT - 1);

    let plan = range_map
        .plan_hash_split_at_key(&spec(), RangeId::new(1), split_at, RangeId::new(7))
        .unwrap();

    assert!(plan.split_at == split_at);
}

#[test]
fn plan_split_at_key_rejects_an_undeclared_bucket_for_a_co_located_table() {
    let split_at = RangeKey::hash_bucket_start(TABLE, BUCKET_COUNT + 3);

    let error = co_located_map()
        .plan_split_at_key(RangeId::COORDINATOR, split_at, RangeId::new(7))
        .unwrap_err();

    assert!(
        error
            == MapValidationError::HashBucketOutOfRange {
                boundary: split_at,
                bucket_count: BUCKET_COUNT,
            }
    );
}

#[test]
fn hash_shard_validation_rejects_a_boundary_past_the_last_bucket() {
    let boundary = RangeKey::hash_bucket_start(TABLE, BUCKET_COUNT);
    let range_map = map_with_boundaries(&[boundary]).unwrap();

    assert!(
        range_map.validate_hash_shard_boundaries(&spec())
            == Err(MapValidationError::HashBucketOutOfRange {
                boundary,
                bucket_count: BUCKET_COUNT,
            })
    );
}

#[test]
fn co_located_map_rejects_a_boundary_past_the_last_bucket() {
    let boundary = RangeKey::hash_bucket_start(TABLE, BUCKET_COUNT);

    let error = map_with_boundaries_and_groups(&[boundary], vec![co_location_group()]).unwrap_err();

    assert!(
        error
            == MapValidationError::HashBucketOutOfRange {
                boundary,
                bucket_count: BUCKET_COUNT,
            }
    );
}

#[test]
fn plan_hash_split_at_key_accepts_a_bucket_start() {
    let range_map = map_with_boundaries(&[RangeKey::hash_bucket_start(TABLE, 4)]).unwrap();
    let split_at = RangeKey::hash_bucket_start(TABLE, 9);

    let plan = range_map
        .plan_hash_split_at_key(&spec(), RangeId::new(1), split_at, RangeId::new(7))
        .unwrap();

    assert!(plan.split_at == split_at);
    assert!(plan.left.end == Some(split_at));
    assert!(plan.right.start == split_at);
}

#[test]
fn plan_hash_split_at_key_leaves_other_tables_row_splittable() {
    let range_map = map_with_boundaries(&[RangeKey::hash_bucket_start(TABLE, 4)]).unwrap();
    let split_at = RangeKey::new(TableId::new(90), 500);

    let plan = range_map
        .plan_hash_split_at_key(&spec(), RangeId::new(1), split_at, RangeId::new(7))
        .unwrap();

    assert!(plan.split_at == split_at);
}

#[test]
fn plan_split_at_key_rejects_a_mid_bucket_point_for_a_co_located_table() {
    let split_at = RangeKey::hash(TABLE, 9, 4096);

    let error = co_located_map()
        .plan_split_at_key(RangeId::COORDINATOR, split_at, RangeId::new(7))
        .unwrap_err();

    assert!(error == MapValidationError::HashBucketSplitBoundary { boundary: split_at });
}

#[test]
fn plan_split_at_key_still_splits_a_row_sharded_table_at_a_rowid() {
    let split_at = RangeKey::new(ROW_SHARDED, 500);

    let plan = co_located_map()
        .plan_split_at_key(RangeId::COORDINATOR, split_at, RangeId::new(7))
        .unwrap();

    assert!(plan.split_at == split_at);
}

#[test]
fn co_located_map_rejects_a_mid_bucket_boundary() {
    let boundary = RangeKey::hash(TABLE, 4, 12);

    let error = map_with_boundaries_and_groups(&[boundary], vec![co_location_group()]).unwrap_err();

    assert!(error == MapValidationError::HashBucketSplitBoundary { boundary });
}

#[test]
fn co_located_map_rejects_a_mid_bucket_boundary_in_a_sibling_table() {
    let boundary = RangeKey::hash(SIBLING, 4, 12);

    let error = map_with_boundaries_and_groups(
        &[RangeKey::hash_bucket_start(TABLE, 4), boundary],
        vec![co_location_group()],
    )
    .unwrap_err();

    assert!(error == MapValidationError::HashBucketSplitBoundary { boundary });
}

#[test]
fn co_located_map_accepts_bucket_aligned_boundaries() {
    let range_map =
        map_with_boundaries_and_groups(&[RangeKey::table_start(TABLE)], vec![co_location_group()]);

    assert!(range_map.is_ok());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn bucket_aligned_map_routes_every_row_of_a_bucket_to_the_routed_range(
        first in 1_u32..BUCKET_COUNT,
        second in 1_u32..BUCKET_COUNT,
        rowid in any::<u64>(),
        value in proptest::collection::vec(any::<u8>(), 0..24),
    ) {
        let low = first.min(second);
        let high = first.max(second);
        let mut boundaries = vec![RangeKey::hash_bucket_start(TABLE, low)];
        if high > low {
            boundaries.push(RangeKey::hash_bucket_start(TABLE, high));
        }
        let range_map = map_with_boundaries(&boundaries).unwrap();
        let spec = spec();

        prop_assert_eq!(range_map.validate_hash_shard_boundaries(&spec), Ok(()));

        let routed = range_map.route_hash_equality(&spec, &value).unwrap();
        let owner = range_map
            .range_for_hash_bucket(TABLE, spec.bucket_for_value(&value), rowid)
            .unwrap();

        prop_assert_eq!(routed.range_id, owner.range_id);
    }

    // The registry checks the bucket before the rowid, so an undeclared bucket
    // is reported as such however far inside its bucket the point falls.
    #[test]
    fn a_bucket_past_the_last_is_rejected_for_every_split_point(
        bucket in BUCKET_COUNT..BUCKET_COUNT * 8,
        rowid in any::<u64>(),
    ) {
        let range_map = map_with_boundaries(&[RangeKey::hash_bucket_start(TABLE, 4)]).unwrap();
        let split_at = RangeKey::hash(TABLE, bucket, rowid);

        prop_assert_eq!(
            range_map
                .plan_hash_split_at_key(&spec(), RangeId::new(1), split_at, RangeId::new(7))
                .unwrap_err(),
            MapValidationError::HashBucketOutOfRange {
                boundary: split_at,
                bucket_count: BUCKET_COUNT,
            }
        );
    }

    #[test]
    fn mid_bucket_boundary_is_rejected_for_every_bucket_and_rowid(
        bucket in 0_u32..BUCKET_COUNT,
        rowid in 1_u64..u64::MAX,
    ) {
        let boundary = RangeKey::hash(TABLE, bucket, rowid);
        let range_map = map_with_boundaries(&[boundary]).unwrap();

        prop_assert_eq!(
            range_map.validate_hash_shard_boundaries(&spec()),
            Err(MapValidationError::HashBucketSplitBoundary { boundary })
        );
    }
}

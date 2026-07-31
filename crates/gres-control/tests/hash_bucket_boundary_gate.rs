//! A hash-sharded table's range boundary must sit on a bucket start.
//!
//! Hash equality routing probes a bucket at rowid 0 and answers for the whole
//! bucket, so a boundary between two rows of one bucket leaves the rest of that
//! bucket on a range the router never consults. Every layout that reaches the
//! registry passes through [`TenantRecord::ensure_valid`], so that is where the
//! alignment is enforced.

use assert2::assert;
use crabka_gres_control::{
    ControlError, HashPlacement, RangeBoundary, RangeLayoutEntry, RangeLifecycle, SqlUser,
    TenantId, TenantName, TenantRecord, TenantState,
};

const HASH_TABLE: u64 = 7;
const ROW_TABLE: u64 = 9;
const BUCKET_COUNT: u32 = 8;

fn hash_placement() -> HashPlacement {
    HashPlacement {
        table_id: HASH_TABLE,
        hash_columns: vec!["id".to_string()],
        bucket_count: BUCKET_COUNT,
        co_location_group: None,
    }
}

fn record_with_boundary(boundary: RangeBoundary, placements: Vec<HashPlacement>) -> TenantRecord {
    let mut record = TenantRecord::new(
        1,
        TenantId::try_from("tenant-a").unwrap(),
        TenantName::try_from("tenant-a").unwrap(),
        TenantState::Active,
        SqlUser::try_from("alice").unwrap(),
        "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
        3,
    )
    .unwrap();
    record.hash_placements = placements;
    record.ranges = vec![
        RangeLayoutEntry {
            range_id: 0,
            end_key: Some(boundary),
            endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
            wal_generation: 1,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "tenant-a-r1.gres.svc:7432".to_string(),
            wal_generation: 1,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        },
    ];
    record
}

fn rejected_field(boundary: RangeBoundary, placements: Vec<HashPlacement>) -> String {
    match record_with_boundary(boundary, placements).ensure_valid() {
        Err(ControlError::InvalidField { field, .. }) => field.to_string(),
        other => panic!("expected an invalid-field rejection, got {other:?}"),
    }
}

#[test]
fn mid_bucket_hash_boundary_is_rejected() {
    assert!(
        rejected_field(
            RangeBoundary::hash(HASH_TABLE, 3, 9),
            vec![hash_placement()]
        ) == "ranges.end_key.rowid"
    );
}

#[test]
fn every_bucket_of_a_hash_table_rejects_a_nonzero_rowid_boundary() {
    for bucket in 0..BUCKET_COUNT {
        assert!(
            rejected_field(
                RangeBoundary::hash(HASH_TABLE, bucket, 1),
                vec![hash_placement()]
            ) == "ranges.end_key.rowid",
            "bucket={bucket}"
        );
    }
}

#[test]
fn bucket_aligned_hash_boundary_is_accepted() {
    for bucket in 0..BUCKET_COUNT {
        let record = record_with_boundary(
            RangeBoundary::hash(HASH_TABLE, bucket, 0),
            vec![hash_placement()],
        );

        assert!(record.ensure_valid().is_ok(), "bucket={bucket}");
    }
}

#[test]
fn row_sharded_table_still_splits_at_any_rowid() {
    let record = record_with_boundary(RangeBoundary::new(ROW_TABLE, 4_096), vec![hash_placement()]);

    assert!(record.ensure_valid().is_ok());
}

#[test]
fn hash_boundary_still_needs_a_bucket_below_the_bucket_count() {
    assert!(
        rejected_field(
            RangeBoundary::hash(HASH_TABLE, BUCKET_COUNT, 0),
            vec![hash_placement()]
        ) == "ranges.end_key.bucket"
    );
}

#[test]
fn hash_table_still_needs_a_bucket_on_its_boundary() {
    assert!(
        rejected_field(RangeBoundary::new(HASH_TABLE, 0), vec![hash_placement()])
            == "ranges.end_key.bucket"
    );
}

#[test]
fn bucket_on_a_row_sharded_table_boundary_is_still_rejected() {
    assert!(
        rejected_field(RangeBoundary::hash(ROW_TABLE, 0, 0), vec![hash_placement()])
            == "ranges.end_key.bucket"
    );
}

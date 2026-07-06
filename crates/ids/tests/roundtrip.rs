use assert2::check;
use crabka_ids::{Offset, PartitionIndex};

#[test]
fn offset_serialises_as_bare_integer() {
    check!(serde_json::to_string(&Offset(42)).unwrap() == "42");
    check!(serde_json::from_str::<Offset>("42").unwrap() == 42);
}

#[test]
fn partition_index_serialises_as_bare_integer() {
    check!(serde_json::to_string(&PartitionIndex(3)).unwrap() == "3");
    check!(serde_json::from_str::<PartitionIndex>("3").unwrap() == 3);
}

#[test]
fn offset_advances_and_rewinds_by_a_count() {
    let base = Offset(100);
    check!(base + 5 == 105);
    check!(base - 1 == 99);

    let mut cursor = base;
    cursor += 3;
    check!(cursor == 103);

    // Delta between two offsets is a plain count.
    check!((Offset(110).get() - base.get()) == 10);
}

#[test]
fn conversions_at_the_wire_boundary() {
    let raw: i64 = 7;
    let offset = Offset::from(raw);
    check!(offset.get() == 7);
    let back: i64 = offset.into();
    check!(back == 7);

    check!(PartitionIndex::from(2).get() == 2);
}

use crabka_ids::{Offset, PartitionIndex};

#[test]
fn offset_serialises_as_bare_integer() {
    assert_eq!(serde_json::to_string(&Offset(42)).unwrap(), "42");
    assert_eq!(serde_json::from_str::<Offset>("42").unwrap(), Offset(42));
}

#[test]
fn partition_index_serialises_as_bare_integer() {
    assert_eq!(serde_json::to_string(&PartitionIndex(3)).unwrap(), "3");
    assert_eq!(
        serde_json::from_str::<PartitionIndex>("3").unwrap(),
        PartitionIndex(3)
    );
}

#[test]
fn offset_advances_and_rewinds_by_a_count() {
    let base = Offset(100);
    assert_eq!(base + 5, Offset(105));
    assert_eq!(base - 1, Offset(99));

    let mut cursor = base;
    cursor += 3;
    assert_eq!(cursor, Offset(103));

    // Delta between two offsets is a plain count.
    assert_eq!(Offset(110).get() - base.get(), 10);
}

#[test]
fn conversions_at_the_wire_boundary() {
    let raw: i64 = 7;
    let offset = Offset::from(raw);
    assert_eq!(offset, Offset(7));
    let back: i64 = offset.into();
    assert_eq!(back, 7);

    assert_eq!(PartitionIndex::from(2), PartitionIndex(2));
}

use std::path::PathBuf;

use assert2::check;
use crabka_protocol_codegen::{emit, emit::EmittedMessage, fmt, ir, resolve};

const CURATED: &[&str] = &[
    "ApiVersionsRequest",
    "ApiVersionsResponse",
    "MetadataRequest",
    "MetadataResponse",
    "ProduceRequest",
    "ProduceResponse",
    "OffsetCommitRequest",
    "OffsetCommitResponse",
    "RequestHeader",
    "ResponseHeader",
    "DescribeGroupsRequest",
    "DescribeGroupsResponse",
    // Exercises a field nullable only above its valid range (Records: versions
    // "0+", nullableVersions "0") — the per-version nullable/non-nullable split.
    "ShareFetchResponse",
    // Exercises a field nullable only within a sub-range (Topics: versions "0-7",
    // nullableVersions "2-7") — the lower-bound side of the split.
    "OffsetFetchRequest",
];

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas")
}

fn snap_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots")
}

fn check(snap_path: &std::path::Path, generated: &str) {
    // The quote-based emitters return single-line token text; rustfmt is the
    // canonical secondary pass (same as the regeneration binary applies), so
    // snapshots stay readable and mirror the committed generated form.
    let generated = fmt::rustfmt(generated)
        .unwrap_or_else(|e| panic!("rustfmt failed for {}: {e}", snap_path.display()));
    if std::env::var("UPDATE_SNAPSHOTS").is_ok() {
        if let Some(parent) = snap_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(snap_path, &generated).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(snap_path).unwrap_or_else(|_| {
        panic!(
            "snapshot file not found: {}; run with UPDATE_SNAPSHOTS=1 to create it",
            snap_path.display()
        )
    });
    assert2::assert!(generated == expected);
}

fn check_emitted(flavor: &str, em: &EmittedMessage, name: &str) {
    let base = snap_dir();
    check(&base.join(format!("{name}.{flavor}.rs")), &em.primary);
    for (cs_name, body) in &em.commons {
        check(&base.join(format!("common/{cs_name}.{flavor}.rs")), body);
    }
}

/// `commonStructs` are MESSAGE-LOCAL in Kafka schemas: two different messages
/// may each declare a struct named `Assignment` with different fields. The
/// resolver must scope them per owning message so the paths differ and each
/// carries its message segment — otherwise one shape silently wins globally and
/// corrupts the other message's type.
#[test]
fn common_structs_are_message_scoped() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    let share = specs
        .iter()
        .find(|s| s.name == "ShareGroupDescribeResponse")
        .unwrap();
    let streams = specs
        .iter()
        .find(|s| s.name == "StreamsGroupDescribeResponse")
        .unwrap();

    let share_map = resolve::resolve_message(share).unwrap();
    let streams_map = resolve::resolve_message(streams).unwrap();

    let share_path = &share_map.get("Assignment").unwrap().rust_path;
    let streams_path = &streams_map.get("Assignment").unwrap().rust_path;

    check!(
        share_path != streams_path,
        "ShareGroupDescribeResponse and StreamsGroupDescribeResponse both \
         resolve Assignment to the same path `{share_path}` — common structs \
         are not message-scoped"
    );
    check!(
        share_path.contains("share_group_describe_response"),
        "ShareGroupDescribeResponse Assignment path `{share_path}` lacks its \
         message segment"
    );
    check!(
        streams_path.contains("streams_group_describe_response"),
        "StreamsGroupDescribeResponse Assignment path `{streams_path}` lacks \
         its message segment"
    );
}

#[test]
fn curated_owned_snapshots() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    for name in CURATED {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        let em = emit::owned_quote::emit(spec, "test").unwrap();
        check_emitted("owned", &em, name);
    }
}

#[test]
fn curated_borrowed_snapshots() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    for name in CURATED {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        let em = emit::borrowed_quote::emit(spec, "test", None).unwrap();
        check_emitted("borrowed", &em, name);
    }
}

const CURATED_KAFKA_3_6_2: &[&str] = &[
    "ProduceRequest",
    "ProduceResponse",
    "FetchRequest",
    "FetchResponse",
];

fn ns_schemas_dir(ns: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas")
        .join("versions")
        .join(ns)
}

fn ns_snap_dir(ns: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(ns)
}

#[test]
fn curated_owned_snapshots_kafka_3_6_2() {
    let specs = ir::load_dir(&ns_schemas_dir("kafka_3_6_2")).unwrap();
    for name in CURATED_KAFKA_3_6_2 {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        let em = emit::owned_quote::emit(spec, "test").unwrap();
        let base = ns_snap_dir("kafka_3_6_2");
        check(&base.join(format!("{name}.owned.rs")), &em.primary);
        for (cs_name, body) in &em.commons {
            check(&base.join(format!("common/{cs_name}.owned.rs")), body);
        }
    }
}

#[test]
fn curated_borrowed_snapshots_kafka_3_6_2() {
    let specs = ir::load_dir(&ns_schemas_dir("kafka_3_6_2")).unwrap();
    for name in CURATED_KAFKA_3_6_2 {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        let em = emit::borrowed_quote::emit(spec, "test", None).unwrap();
        let base = ns_snap_dir("kafka_3_6_2");
        check(&base.join(format!("{name}.borrowed.rs")), &em.primary);
        for (cs_name, body) in &em.commons {
            check(&base.join(format!("common/{cs_name}.borrowed.rs")), body);
        }
    }
}

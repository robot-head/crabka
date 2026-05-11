use std::path::PathBuf;

use crabka_protocol_codegen::{emit, ir};

const CURATED: &[&str] = &[
    "ApiVersionsRequest",
    "ApiVersionsResponse",
    "MetadataRequest",
    "MetadataResponse",
];

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas")
}

fn check(snap_name: &str, generated: &str) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(snap_name);
    if std::env::var("UPDATE_SNAPSHOTS").is_ok() {
        std::fs::write(&path, generated).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        generated, expected,
        "snapshot mismatch in {snap_name}; UPDATE_SNAPSHOTS=1 to refresh"
    );
}

#[test]
fn curated_owned_snapshots() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    for name in CURATED {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        check(
            &format!("{name}.owned.rs"),
            &emit::owned::emit(spec, "test").unwrap(),
        );
    }
}

#[test]
fn curated_borrowed_snapshots() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    for name in CURATED {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        check(
            &format!("{name}.borrowed.rs"),
            &emit::borrowed::emit(spec, "test").unwrap(),
        );
    }
}

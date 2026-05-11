use std::path::PathBuf;

use crabka_protocol_codegen::{emit_borrowed, emit_owned, ir};

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
fn api_versions_request_owned_snapshot() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    let spec = specs
        .iter()
        .find(|s| s.name == "ApiVersionsRequest")
        .unwrap();
    check(
        "ApiVersionsRequest.owned.rs",
        &emit_owned::emit(spec, "test").unwrap(),
    );
}

#[test]
fn api_versions_request_borrowed_snapshot() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    let spec = specs
        .iter()
        .find(|s| s.name == "ApiVersionsRequest")
        .unwrap();
    check(
        "ApiVersionsRequest.borrowed.rs",
        &emit_borrowed::emit(spec, "test").unwrap(),
    );
}

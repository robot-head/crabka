use std::path::PathBuf;

use crabka_protocol_codegen::{emit_owned, ir};

#[test]
fn api_versions_request_snapshot() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas");
    let specs = ir::load_dir(&dir).unwrap();
    let spec = specs.iter().find(|s| s.name == "ApiVersionsRequest").unwrap();

    let generated = emit_owned::emit(spec).unwrap();
    let snap_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots/ApiVersionsRequest.owned.rs");

    if std::env::var("UPDATE_SNAPSHOTS").is_ok() {
        std::fs::write(&snap_path, &generated).unwrap();
        return;
    }

    let expected = std::fs::read_to_string(&snap_path).unwrap();
    assert_eq!(generated, expected, "snapshot mismatch; run with UPDATE_SNAPSHOTS=1 to update");
}

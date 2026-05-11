// Smoke test: confirm the snapshotted generated source compiles when wired into
// the crabka-protocol crate. We don't include it directly here (lifetime of the
// snapshot file is asymmetric with the test); a separate compile check happens
// via crates/protocol/build.rs in a later task.

#[test]
fn snapshot_smoke() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots/ApiVersionsRequest.owned.rs");
    assert!(path.exists(), "snapshot file missing");
    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.contains("pub struct ApiVersionsRequest"));
    assert!(contents.contains("impl Encode for ApiVersionsRequest"));
    assert!(contents.contains("impl<'de> Decode<'de> for ApiVersionsRequest"));
}

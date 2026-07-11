// Smoke test: confirm the snapshotted generated source compiles when wired into
// the crabka-protocol crate. We don't include it directly here (lifetime of the
// snapshot file is asymmetric with the test); a separate compile check happens
// via crates/protocol/build.rs in a later task.

use assert2::check;
#[test]
fn snapshot_smoke() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots/ApiVersionsRequest.owned.rs");
    assert2::assert!(path.exists());
    let contents = std::fs::read_to_string(&path).unwrap();
    check!(contents.contains("pub struct ApiVersionsRequest"));
    check!(contents.contains("impl Encode for ApiVersionsRequest"));
    check!(contents.contains("impl<'de> Decode<'de> for ApiVersionsRequest"));
}

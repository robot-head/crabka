//! Build script — generates Connect-RPC server stubs + prost message
//! types from the `.proto` file. Outputs are written to `OUT_DIR` and
//! pulled in via the `pb::` module declared in `src/lib.rs`.
//!
//! Prefers a system-installed `protoc`. Falls back to fetching a
//! vendored release tarball at build time only when none is found —
//! that keeps `cargo build --offline` (with system protoc) working
//! and avoids reaching the network in sandboxed CI environments.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/crabka/rebalancer/v1/rebalancer.proto";
    let mut builder =
        connectrpc_axum_build::compile_protos(&[proto], &["proto"]);
    if !system_protoc_available() {
        builder = builder.fetch_protoc(None, None)?;
    }
    builder.compile()?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

/// Returns true when `protoc --version` succeeds (i.e. the binary is
/// reachable on `$PATH` or via the `PROTOC` env var that prost-build
/// honors).
fn system_protoc_available() -> bool {
    if std::env::var_os("PROTOC").is_some() {
        return true;
    }
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

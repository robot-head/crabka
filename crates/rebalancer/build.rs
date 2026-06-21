//! Build script — generates Connect-RPC server stubs + prost message types from
//! the `.proto` file. Outputs are written to `OUT_DIR` and pulled in via the
//! `pb::` module declared in `src/lib.rs`.
//!
//! Drives codegen through a vendored `protoc` binary (`protoc-bin-vendored`) so
//! the build is hermetic — no system `protoc` and no network fetch. The Connect
//! generator (connectrpc-axum-build) always invokes a `protoc` binary, so the
//! vendored one is supplied via `prost-build`'s `protoc_executable`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/crabka/rebalancer/v1/rebalancer.proto";
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    connectrpc_axum_build::compile_protos(&[proto], &["proto"])
        .with_prost_config(move |config| {
            config.protoc_executable(protoc.clone());
        })
        .compile()?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

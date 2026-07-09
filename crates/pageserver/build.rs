//! Generates Connect-RPC server stubs and prost message types for pageserver.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/crabka/pageserver/v1/pageserver.proto";
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    connectrpc_axum_build::compile_protos(&[proto], &["proto"])
        .with_prost_config(move |config| {
            config.protoc_executable(protoc_path.clone());
        })
        .compile()?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

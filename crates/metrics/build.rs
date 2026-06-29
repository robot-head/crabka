//! Generates prost message types from the vendored `remote_write` v1/v2 protos.
//!
//! Drives codegen through a vendored `protoc` binary (`protoc-bin-vendored`) so
//! the build is hermetic: no system `protoc`, no network fetch, and no
//! platform-specific protobuf release archive naming.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/prometheus/remote.proto",
        "proto/io/prometheus/write/v2/types.proto",
    ];
    let includes = ["proto"];

    let mut config = prost_build::Config::new();
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    config.protoc_executable(protoc_path);

    config.compile_protos(&protos, &includes)?;
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}

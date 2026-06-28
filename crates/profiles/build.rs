//! Generates Connect-RPC server stubs + prost message types from the vendored
//! `push.v1`, `querier.v1`, and OTLP `profiles/v1development` protos.
//!
//! Drives codegen through a vendored `protoc` binary (`protoc-bin-vendored`) so
//! the build is hermetic — no system `protoc` and no network fetch. The Connect
//! generator (connectrpc-axum-build) always invokes a `protoc` binary, so the
//! vendored one is supplied via `prost-build`'s `protoc_executable`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/google/v1/profile.proto",
        "proto/push/v1/push.proto",
        "proto/querier/v1/querier.proto",
        "proto/settings/v1/settings.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ];
    let includes = ["proto"];
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    connectrpc_axum_build::compile_protos(&protos, &includes)
        .with_prost_config(move |config| {
            config.protoc_executable(protoc_path.clone());
        })
        .compile()?;
    for path in [
        "proto/types/v1/types.proto",
        "proto/google/v1/profile.proto",
        "proto/push/v1/push.proto",
        "proto/querier/v1/querier.proto",
        "proto/settings/v1/settings.proto",
        "proto/opentelemetry/proto/common/v1/common.proto",
        "proto/opentelemetry/proto/resource/v1/resource.proto",
        "proto/opentelemetry/proto/profiles/v1development/profiles.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    Ok(())
}

//! Generates Connect-RPC server stubs + prost message types from the vendored
//! `push.v1` + OTLP `profiles/v1development` protos. Prefers a system `protoc`;
//! falls back to a vendored fetch only when none is found.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/push/v1/push.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ];
    let includes = ["proto"];
    let mut builder = connectrpc_axum_build::compile_protos(&protos, &includes);
    if !system_protoc_available() {
        builder = builder.fetch_protoc(None, None)?;
    }
    builder.compile()?;
    for path in [
        "proto/types/v1/types.proto",
        "proto/push/v1/push.proto",
        "proto/opentelemetry/proto/common/v1/common.proto",
        "proto/opentelemetry/proto/resource/v1/resource.proto",
        "proto/opentelemetry/proto/profiles/v1development/profiles.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    Ok(())
}

fn system_protoc_available() -> bool {
    if std::env::var_os("PROTOC").is_some() {
        return true;
    }
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

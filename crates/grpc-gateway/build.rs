//! Generates Connect-RPC server stubs + prost message types from the
//! `.proto`. Prefers a system `protoc`; falls back to a vendored fetch
//! only when none is found (keeps `--offline` working with system protoc).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/crabka/gateway/v1/gateway.proto";
    let mut builder = connectrpc_axum_build::compile_protos(&[proto], &["proto"]);
    if !system_protoc_available() {
        builder = builder.fetch_protoc(None, None)?;
    }
    builder.compile()?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

fn system_protoc_available() -> bool {
    if std::env::var_os("PROTOC").is_some() {
        return true;
    }
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

//! Compile the vendored perftools.profiles `Profile` proto.
//!
//! Prefers a system-installed `protoc`. Falls back to a vendored binary only
//! when none is found, matching the repository's Connect build scripts.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/profile.proto";
    let mut config = prost_build::Config::new();
    if !system_protoc_available() {
        config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    }
    config.compile_protos(&[proto], &["proto"])?;
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

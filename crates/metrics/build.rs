//! Generates prost message types from the vendored `remote_write` v1/v2 protos.

use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/prometheus/remote.proto",
        "proto/io/prometheus/write/v2/types.proto",
    ];
    let includes = ["proto"];

    let mut config = prost_build::Config::new();
    if !system_protoc_available() {
        let out_dir = std::env::var_os("OUT_DIR")
            .map(PathBuf::from)
            .ok_or("OUT_DIR is not set for build script")?;
        let protoc_path = protoc_fetcher::protoc("31.1", Path::new(&out_dir))?;
        config.protoc_executable(protoc_path);
    }

    config.compile_protos(&protos, &includes)?;
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
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

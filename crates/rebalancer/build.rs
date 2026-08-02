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
    let protoc_path = if let Some(path) = std::env::var_os("PROTOC") {
        path.into()
    } else {
        protoc_bin_vendored::protoc_bin_path()?
    };
    connectrpc_axum_build::compile_protos(&[proto], &["proto"])
        .with_prost_config(move |config| {
            config.protoc_executable(protoc_path.clone());
        })
        .compile()?;
    normalize_generated_docs_and_builder()?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

fn normalize_generated_docs_and_builder() -> Result<(), Box<dyn std::error::Error>> {
    let generated =
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("crabka.rebalancer.v1.rs");
    let source = std::fs::read_to_string(&generated)?;
    let source = source
        .replace("ProtoBuf", "`Protobuf`")
        .replace(
            "pub struct RebalancerServiceBuilder",
            "#[must_use]\npub struct RebalancerServiceBuilder",
        )
        .replace(
            "    pub fn as_str_name",
            "    #[must_use]\n    pub fn as_str_name",
        )
        .replace(
            "    pub fn from_str_name",
            "    #[must_use]\n    pub fn from_str_name",
        )
        .replace("&FIELDS)", "FIELDS)")
        .replace(
            "write!(formatter, \"expected one of: {:?}\", FIELDS)",
            "write!(formatter, \"expected one of: {FIELDS:?}\")",
        )
        .replace("[`build()`]", "`build()`");
    std::fs::write(generated, source)?;
    Ok(())
}

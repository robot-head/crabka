//! Generates Connect-RPC server stubs + prost message types from the `.proto`.
//!
//! The Connect generator requires a protoc-compatible executable. Bazel
//! supplies the pure-Rust `protox` CLI; Cargo-only builds fall back to
//! `protoc-bin-vendored`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/crabka/gateway/v1/gateway.proto";
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
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("crabka.gateway.v1.rs");
    let source = std::fs::read_to_string(&generated)?;
    let source = source
        .replace("ProtoBuf", "`Protobuf`")
        .replace("a JSONPath expression", "a `JSONPath` expression")
        .replace("via TopicNameStrategy", "via `TopicNameStrategy`")
        .replace("under\n    /// RawCodec", "under\n    /// `RawCodec`")
        .replace(
            "pub struct GatewayServiceBuilder",
            "#[must_use]\npub struct GatewayServiceBuilder",
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

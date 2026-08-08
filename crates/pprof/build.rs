//! Compile the vendored perftools.profiles `Profile` proto.
//!
//! This build script uses the pure-Rust `protox` compiler to produce a
//! `FileDescriptorSet`. It then passes that set to `prost-build` with
//! `compile_fds`. The build does not need a `protoc` binary.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/profile.proto";
    let fds = protox::compile([proto], ["proto"])?;
    let mut config = prost_build::Config::new();
    config.disable_comments(["."]);
    config.compile_fds(fds)?;
    rewrite_mapping_flags()?;
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=src/proto_mapping.rsfrag");
    Ok(())
}

fn rewrite_mapping_flags() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    let path = std::path::Path::new(&out_dir).join("perftools.profiles.rs");
    let generated = std::fs::read_to_string(&path)?;
    let updated = generated.replace(
        generated_mapping(),
        include_str!("src/proto_mapping.rsfrag"),
    );
    if updated == generated {
        return Err("generated Mapping shape changed".into());
    }
    std::fs::write(path, updated)?;
    Ok(())
}

fn generated_mapping() -> &'static str {
    r#"#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Mapping {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub memory_start: u64,
    #[prost(uint64, tag = "3")]
    pub memory_limit: u64,
    #[prost(uint64, tag = "4")]
    pub file_offset: u64,
    #[prost(int64, tag = "5")]
    pub filename: i64,
    #[prost(int64, tag = "6")]
    pub build_id: i64,
    #[prost(bool, tag = "7")]
    pub has_functions: bool,
    #[prost(bool, tag = "8")]
    pub has_filenames: bool,
    #[prost(bool, tag = "9")]
    pub has_line_numbers: bool,
    #[prost(bool, tag = "10")]
    pub has_inline_frames: bool,
}
"#
}

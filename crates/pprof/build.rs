//! Compile the vendored perftools.profiles `Profile` proto.
//!
//! Uses the pure-Rust `protox` compiler to produce a `FileDescriptorSet`, then
//! hands it to `prost-build` via `compile_fds`. No `protoc` binary is required.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/profile.proto";
    let fds = protox::compile([proto], ["proto"])?;
    prost_build::Config::new().compile_fds(fds)?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

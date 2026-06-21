fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/jaeger/api_v2/collector.proto";
    let fds = protox::compile([proto], ["proto/jaeger/api_v2"])?;
    tonic_prost_build::compile_fds(fds)?;
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=proto/jaeger/api_v2/model.proto");
    Ok(())
}

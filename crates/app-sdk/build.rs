fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../grpc-gateway/proto/crabka/gateway/v1/gateway.proto";
    let include = "../grpc-gateway/proto";
    let fds = protox::compile([proto], [include])?;
    prost_build::Config::new().compile_fds(fds)?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

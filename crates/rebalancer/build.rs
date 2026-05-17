//! Build script — generates Connect-RPC server stubs + prost message
//! types from the `.proto` file. Outputs are written to `OUT_DIR` and
//! pulled in via the `pb::` module declared in `src/lib.rs`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    connectrpc_axum_build::compile_protos(
        &["proto/crabka/rebalancer/v1/rebalancer.proto"],
        &["proto"],
    )
    .out_dir(&out_dir)
    .fetch_protoc(None, None)?
    .compile()?;
    println!("cargo:rerun-if-changed=proto/crabka/rebalancer/v1/rebalancer.proto");
    Ok(())
}

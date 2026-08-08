//! Push a compiled protobuf `FileDescriptorSet` into Crabka Schema Registry.

use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "crabka-schema-push",
    version,
    about = "Import a compiled protobuf FileDescriptorSet into Crabka Schema Registry"
)]
struct Args {
    /// Schema Registry base URL.
    #[arg(
        long,
        env = "CRABKA_SCHEMA_REGISTRY_URL",
        default_value = "http://localhost:8081"
    )]
    registry_url: String,
    /// Path to a binary `FileDescriptorSet`. `protoc --descriptor_set_out`
    /// usually produces it.
    descriptor_set: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let bytes = std::fs::read(&args.descriptor_set).map_err(|e| {
        anyhow::anyhow!("read descriptor set {}: {e}", args.descriptor_set.display())
    })?;
    let url = format!("{}/schemas/import", args.registry_url.trim_end_matches('/'));
    let resp = reqwest::Client::new()
        .post(url)
        .header("content-type", "application/octet-stream")
        .body(bytes)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("schema import failed ({status}): {body}");
    }
    println!("{body}");
    Ok(())
}

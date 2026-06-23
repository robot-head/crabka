use clap::Parser;
use crabka_observability::{ServiceConfig, build_service_dependencies, serve_service};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServiceConfig::parse();
    let dependencies = build_service_dependencies(&config).await?;
    serve_service(config, dependencies, None).await?;
    Ok(())
}

// Required for compiler analysis of Tokio's generated async binary future, not runtime recursion.
#![recursion_limit = "256"]

use clap::Parser;

fn main() -> std::io::Result<()> {
    crabka_gres_fdw::provider::install_default_provider();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = crabka_gres::Cli::parse();
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()?
        .block_on(crabka_gres::run_serve(cli.serve))
}

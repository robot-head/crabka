// Required for compiler analysis of Tokio's generated async binary future, not runtime recursion.
#![recursion_limit = "256"]

use clap::Parser;
use crabka_gres::telemetry::{
    FMT_DEFAULT_FILTER, OTEL_DEFAULT_FILTER, OtlpConfig, init, service_instance_id,
};

/// Service name reported to the trace backend as `service.name`, unless
/// `OTEL_SERVICE_NAME` overrides it.
const SERVICE_NAME: &str = "crabka-gres";

fn main() -> std::io::Result<()> {
    crabka_gres_fdw::provider::install_default_provider();

    let mut cli = crabka_gres::Cli::parse();

    let cli = crabka_gres::Cli::parse();
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()?
        .block_on(crabka_gres::run_serve(cli.serve))
}

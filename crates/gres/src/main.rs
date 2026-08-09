// Required for compiler analysis of Tokio's generated async binary future, not runtime recursion.
#![recursion_limit = "256"]

use clap::Parser;
use crabka_gres::telemetry::{
    FMT_DEFAULT_FILTER, OTEL_DEFAULT_FILTER, OtlpConfig, init, service_instance_id,
};

/// Service name reported to the trace backend as `service.name`, unless
/// `OTEL_SERVICE_NAME` overrides it.
const SERVICE_NAME: &str = "crabka-gres";

/// The runtime is built by hand rather than through `#[tokio::main]` so the
/// worker stack can be raised: the executor recurses through nested plan nodes,
/// and the default 2 MiB overflows on deep queries.
fn main() -> std::io::Result<()> {
    crabka_gres_fdw::provider::install_default_provider();

    let mut cli = crabka_gres::Cli::parse();

    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()?
        .block_on(async move {
            // Install the tracing subscriber — stdout JSON `fmt` plus an optional
            // OTLP export layer. OTLP stays off unless the environment opts in
            // (see `crabka_gres::telemetry`). Built inside the runtime so the
            // gRPC exporter captures the runtime handle.
            let otlp = OtlpConfig::from_env(
                |key| std::env::var(key).ok(),
                &service_instance_id(&cli.serve, |key| std::env::var(key).ok()),
                env!("CARGO_PKG_VERSION"),
                SERVICE_NAME,
            )
            .map_err(std::io::Error::other)?;
            // `--gres-trace-ingress resample` recomputes a client's sampled bit,
            // and only agrees with that client when it uses the pipeline's own
            // ratio: take it from the resolved config rather than re-reading the
            // environment.
            cli.serve.adopt_otlp_sample_ratio(otlp.as_ref());
            let telemetry = init(otlp, FMT_DEFAULT_FILTER, OTEL_DEFAULT_FILTER, SERVICE_NAME)
                .map_err(std::io::Error::other)?;

            // Shut down on both exit paths: the guard's final flush carries the
            // batch that describes whatever made gres stop, which is the batch an
            // operator came for.
            let result = crabka_gres::run_serve(cli.serve).await;
            telemetry.shutdown();
            result
        })
}

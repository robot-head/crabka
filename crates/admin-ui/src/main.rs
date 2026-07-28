use anyhow::Context;
use clap::Parser;
use crabka_admin_ui::config::{AdminUiConfig, AdminUiRuntimeArgs};

#[cfg_attr(test, mutants::skip)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let runtime_args = AdminUiRuntimeArgs::parse();
    let mut cfg = AdminUiConfig::from_env().context("load admin UI config")?;
    cfg.mutation_json_body_limit_bytes = runtime_args.mutation_json_body_limit_bytes;
    cfg.session_ttl = runtime_args.session_ttl;
    let listen_addr = cfg.listen_addr;
    let cluster_name = cfg.cluster_name.clone();
    let state = crabka_admin_ui::server::AppState::new(cfg);

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .context("bind admin UI listener")?;
    let bound = listener
        .local_addr()
        .context("read admin UI listener addr")?;
    tracing::info!(%bound, cluster = %cluster_name, "crabka admin UI listening");

    axum::serve(listener, crabka_admin_ui::server::router(state))
        .await
        .context("serve admin UI")
}

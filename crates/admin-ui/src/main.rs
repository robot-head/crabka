use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = crabka_admin_ui::config::AdminUiConfig::from_env().context("load admin UI config")?;

    let listener = tokio::net::TcpListener::bind(cfg.listen_addr)
        .await
        .context("bind admin UI listener")?;
    let bound = listener
        .local_addr()
        .context("read admin UI listener addr")?;
    tracing::info!(%bound, cluster = %cfg.cluster_name, "crabka admin UI listening");

    axum::serve(listener, crabka_admin_ui::server::health_router())
        .await
        .context("serve admin UI")
}

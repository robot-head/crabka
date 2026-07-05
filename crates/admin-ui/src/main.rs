use anyhow::Context;

#[cfg_attr(test, mutants::skip)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = crabka_admin_ui::config::AdminUiConfig::from_env().context("load admin UI config")?;
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

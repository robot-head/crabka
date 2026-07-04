use std::net::SocketAddr;

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = std::env::var("CRABKA_ADMIN_UI_LISTEN_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8088".to_string())
        .parse()
        .context("parse CRABKA_ADMIN_UI_LISTEN_ADDR")?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind admin UI listener")?;
    let bound = listener
        .local_addr()
        .context("read admin UI listener addr")?;
    tracing::info!(%bound, "crabka admin UI listening");

    axum::serve(listener, crabka_admin_ui::server::health_router())
        .await
        .context("serve admin UI")
}

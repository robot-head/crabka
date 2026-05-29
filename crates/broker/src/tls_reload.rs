//! Background TLS hot-reload watcher.
//!
//! Polls the cert / key / client-CA paths configured on the broker's
//! [`crabka_security::TlsConfig`]. On any mtime change, rebuilds the
//! `ServerConfig` and swaps it into the shared
//! [`crabka_security::DynamicServerConfig`]. New TLS handshakes pick
//! up the swap on the next `accept`; in-flight handshakes are not
//! affected.
//!
//! Errors during rebuild are logged at `warn` and the previous config
//! stays in place — better to keep serving with the old cert than to
//! drop connections.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crabka_security::{DynamicServerConfig, TlsConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PathMtimes {
    cert: Option<SystemTime>,
    key: Option<SystemTime>,
    client_ca: Option<SystemTime>,
}

fn read_mtime(p: &Path) -> Option<SystemTime> {
    std::fs::metadata(p).ok()?.modified().ok()
}

fn snapshot_mtimes(cfg: &TlsConfig) -> PathMtimes {
    PathMtimes {
        cert: read_mtime(&cfg.cert_chain_path),
        key: read_mtime(&cfg.private_key_path),
        client_ca: cfg.client_ca_path.as_deref().and_then(read_mtime),
    }
}

/// Spawned task entry point. Polls every `interval`. Cancels on the
/// `shutdown` token.
pub(crate) async fn run(
    dynamic: Arc<DynamicServerConfig>,
    cfg: TlsConfig,
    interval: Duration,
    shutdown: CancellationToken,
) {
    if interval.is_zero() {
        info!("tls hot-reload watcher disabled (interval == 0)");
        return;
    }
    let mut last = snapshot_mtimes(&cfg);
    let mut ticker = tokio::time::interval(interval);
    // First tick fires immediately; skip it so we don't double-load on
    // startup (the broker already built the initial ServerConfig).
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                info!("tls hot-reload watcher shutting down");
                return;
            }
        }
        let now = snapshot_mtimes(&cfg);
        if now == last {
            debug!("tls hot-reload watcher: no change");
            continue;
        }
        match dynamic.reload_from(&cfg) {
            Ok(()) => {
                info!("tls hot-reload watcher: server config swapped");
                last = now;
            }
            Err(e) => {
                warn!(error = %e, "tls hot-reload watcher: reload failed; keeping prior config");
                // Don't update `last` — if the next tick succeeds, we
                // want it to retry against the same input.
            }
        }
    }
}

//! Parse a Kafka-style bootstrap string ("host:port,host:port") into
//! a list of resolved [`SocketAddr`](std::net::SocketAddr)s.

use std::net::SocketAddr;

use crate::error::ClientError;

/// Parse a comma-separated `host:port` list and resolve each entry via
/// [`tokio::net::lookup_host`]. Silently skips entries that fail to resolve;
/// returns [`ClientError::Disconnected`] if *none* resolve.
pub async fn resolve(bootstrap: &str) -> Result<Vec<SocketAddr>, ClientError> {
    let mut out = Vec::new();
    for part in bootstrap.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match tokio::net::lookup_host(part).await {
            Ok(iter) => out.extend(iter),
            Err(e) => {
                tracing::warn!(part, error = %e, "bootstrap resolve failed");
            }
        }
    }
    if out.is_empty() {
        return Err(ClientError::Disconnected);
    }
    Ok(out)
}

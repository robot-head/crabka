//! Parse a Kafka-style bootstrap string ("host:port,host:port") into
//! a list of resolved [`SocketAddr`](std::net::SocketAddr)s.

use std::net::SocketAddr;

use crate::error::ClientError;

/// Parse a comma-separated `host:port` list and resolve each entry via
/// [`tokio::net::lookup_host`]. Silently skips entries that fail to resolve;
/// returns [`ClientError::Disconnected`] if *none* resolve.
#[tracing::instrument(level = "debug", skip_all, fields(bootstrap = %bootstrap), err)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_returns_addresses_for_valid_entries() {
        let addrs = resolve("127.0.0.1:9092, 127.0.0.1:9093").await.unwrap();

        assert_eq!(addrs.len(), 2);
        assert!(addrs.iter().any(|addr| addr.port() == 9092));
        assert!(addrs.iter().any(|addr| addr.port() == 9093));
    }

    #[tokio::test]
    async fn resolve_errors_when_no_entries_resolve() {
        let err = resolve(" , ").await.unwrap_err();

        assert!(matches!(err, ClientError::Disconnected));
    }
}

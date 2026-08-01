//! Parse a Kafka-style bootstrap string ("host:port,host:port") into
//! a list of resolved [`SocketAddr`](std::net::SocketAddr)s.

use std::{future::Future, net::SocketAddr};

use crabka_units::convert::TimeExt as _;

use crate::{connection::ClientDnsTimeout, error::ClientError};

pub(crate) async fn bounded_lookup<F>(
    timeout: ClientDnsTimeout,
    lookup: F,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future,
{
    tokio::time::timeout(timeout.time().to_std(), lookup).await
}

/// Parse a comma-separated `host:port` list and resolve each entry via
/// [`tokio::net::lookup_host`]. Silently skips entries that fail to resolve;
/// returns [`ClientError::Disconnected`] if *none* resolve.
#[tracing::instrument(level = "debug", skip_all, fields(bootstrap = %bootstrap), err)]
pub async fn resolve(
    bootstrap: &str,
    dns_timeout: ClientDnsTimeout,
) -> Result<Vec<SocketAddr>, ClientError> {
    let mut out = Vec::new();
    for part in bootstrap.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match bounded_lookup(dns_timeout, tokio::net::lookup_host(part)).await {
            Ok(Ok(iter)) => out.extend(iter),
            Ok(Err(error)) => {
                tracing::warn!(part, error = %error, "bootstrap resolve failed");
            }
            Err(error) => {
                tracing::warn!(part, error = %error, "bootstrap resolve timed out");
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
    use std::time::Duration;

    use assert2::assert;
    use crabka_units::millis;

    use super::*;
    use crate::connection::ClientDnsTimeout;

    #[tokio::test]
    async fn resolve_returns_addresses_for_valid_entries() {
        let addrs = resolve(
            "127.0.0.1:9092, 127.0.0.1:9093",
            ClientDnsTimeout::default(),
        )
        .await
        .expect("literal addresses resolve");

        assert!(addrs.len() == 2);
        assert!(addrs.iter().any(|addr| addr.port() == 9092));
        assert!(addrs.iter().any(|addr| addr.port() == 9093));
    }

    #[tokio::test]
    async fn resolve_errors_when_no_entries_resolve() {
        let err = resolve(" , ", ClientDnsTimeout::default())
            .await
            .expect_err("empty entries do not resolve");

        assert!(matches!(err, ClientError::Disconnected));
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_lookup_stops_at_the_configured_deadline() {
        let timeout = ClientDnsTimeout::new(millis(37)).expect("positive timeout");
        let started = tokio::time::Instant::now();
        let result = bounded_lookup(timeout, std::future::pending::<()>()).await;
        assert!(result.is_err());
        assert!(started.elapsed() == Duration::from_millis(37));
    }

    #[tokio::test]
    async fn resolve_skips_a_failed_entry_and_keeps_later_addresses() {
        let addrs = resolve(":,127.0.0.1:9093", ClientDnsTimeout::default())
            .await
            .expect("later address resolves");
        assert!(addrs.iter().any(|addr| addr.port() == 9093));
    }
}

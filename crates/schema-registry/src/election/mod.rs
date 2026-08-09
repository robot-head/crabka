//! Schema Registry primary election over the cp-exact `"sr"` Kafka group. A
//! node joins the group. The leader selects the primary and broadcasts it.
//! Every node publishes its `PrimaryState` for the forwarding middleware.

pub mod client;
pub mod protocol;

use client::ElectionClient;
use protocol::SchemaRegistryIdentity;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::RegistryConfig;

/// Who the primary is, from this node's point of view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryState {
    pub is_primary: bool,
    pub primary_url: Option<String>,
    /// Classic-group generation that elected this primary.
    pub generation_id: Option<i32>,
    /// This node's member id in `generation_id` when it is the primary.
    pub member_id: Option<String>,
}

/// The election handle: spawns the group-membership task and exposes the
/// `PrimaryState` watch.
pub struct Election;

impl Election {
    /// Parse `advertised_url` into a `SchemaRegistryIdentity`, spawn the `"sr"`
    /// group loop, and return a watch receiver of `PrimaryState`. The task runs
    /// until `cancel` fires, and it then sends `LeaveGroup`.
    pub fn start(
        cfg: &RegistryConfig,
        cancel: CancellationToken,
    ) -> std::future::Ready<anyhow::Result<watch::Receiver<PrimaryState>>> {
        std::future::ready((|| {
            let identity = parse_identity(&cfg.advertised_url, cfg.leader_eligibility)?;
            let (tx, rx) = watch::channel(PrimaryState::default());
            let client = ElectionClient {
                bootstrap: cfg.bootstrap.clone(),
                client_id: format!("{}-election", cfg.client_id),
                group_id: cfg.group_id.clone(),
                identity,
                tx,
                security: cfg.security.client.clone(),
                runtime: cfg.runtime.clone(),
            };
            tokio::spawn(client.run(cancel));
            Ok(rx)
        })())
    }
}

/// Parse `http://host:port` into a version 1 `SchemaRegistryIdentity`.
fn parse_identity(url: &str, eligible: bool) -> anyhow::Result<SchemaRegistryIdentity> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("advertised_url missing scheme: {url}"))?;
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("advertised_url missing port: {url}"))?;
    Ok(SchemaRegistryIdentity {
        version: 1,
        host: host.to_string(),
        port: port.parse()?,
        master_eligibility: eligible,
        scheme: scheme.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_identity;

    #[test]
    fn advertised_url_cases() {
        for (_name, input, expected) in [
            (
                "valid",
                "http://10.0.0.5:8081",
                Some(("10.0.0.5", 8081, "http", true)),
            ),
            ("missing_host", "nohost", None),
        ] {
            let actual = parse_identity(input, true).ok().map(|identity| {
                (
                    identity.host,
                    identity.port,
                    identity.scheme,
                    identity.master_eligibility,
                )
            });
            let expected = expected.map(|(host, port, scheme, eligible)| {
                (host.to_owned(), port, scheme.to_owned(), eligible)
            });
            assert2::assert!(actual == expected);
        }
    }
}

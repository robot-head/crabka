//! Gateway configuration, parsed from CLI flags / env in `bin/gateway.rs`.

use std::net::SocketAddr;
use std::path::PathBuf;

pub use crabka_security::ClientAuthMode;

/// TLS / mTLS settings for the gateway listener and the forward channel.
/// Present ⇒ the gateway serves over rustls; absent ⇒ plaintext.
#[derive(Debug, Clone)]
pub struct TlsSettings {
    /// Server cert chain (PEM). Doubles as the gateway's client identity when
    /// forwarding (the cert is issued with server+client EKU).
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    /// CA(s) the forwarder trusts when verifying a peer gateway's server cert.
    pub trust_roots_path: Option<PathBuf>,
    /// CA(s) used to verify incoming client certs (mTLS). Required if
    /// `client_auth != Disabled`.
    pub client_ca_path: Option<PathBuf>,
    pub client_auth: ClientAuthMode,
    /// Cert hot-reload poll interval (seconds).
    pub reload_interval_secs: u64,
}

impl TlsSettings {
    /// Map to the `crabka-security` config used to build server/client configs.
    #[must_use]
    pub fn to_security(&self) -> crabka_security::TlsConfig {
        crabka_security::TlsConfig {
            cert_chain_path: self.cert_chain_path.clone(),
            private_key_path: self.private_key_path.clone(),
            trust_roots_path: self.trust_roots_path.clone(),
            client_ca_path: self.client_ca_path.clone(),
            client_auth: self.client_auth,
        }
    }
}

/// Runtime configuration for the gateway process.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// `host:port,host:port,...` of brokers for bootstrap.
    pub bootstrap: String,
    /// Connect-RPC + HTTP listen address.
    pub listen_addr: SocketAddr,
    /// Base `client.id` for the native clients this gateway opens.
    pub client_id: String,
    /// Internal compacted topic that stores dedup claims.
    pub dedup_topic: String,
    /// Partition count of the dedup topic (also the ownership shard count in P3).
    pub dedup_partitions: u32,
    /// Dedup window: claim-topic `retention.ms` and the dedup guarantee horizon.
    pub dedup_window_ms: i64,
    /// `transactional.id` prefix; the per-partition id is `{prefix}-{p}`.
    pub dedup_txn_id_prefix: String,
    /// Address other replicas reach THIS gateway at (host:port of `listen_addr`,
    /// externally routable). Published to membership; used to forward.
    pub advertised_addr: String,
    /// Internal compacted topic carrying replica membership / owner routing.
    pub membership_topic: String,
    /// TLS/mTLS settings; `None` ⇒ plaintext (all current tests).
    pub tls: Option<TlsSettings>,
}

impl GatewayConfig {
    /// Replication factor requested for the dedup topic at create time.
    /// Kept here so `bin` and tests agree; broker may downgrade.
    pub const DEDUP_TOPIC_REPLICATION: i16 = 3;
    /// Replication factor requested for the membership topic at create time.
    pub const MEMBERSHIP_TOPIC_REPLICATION: i16 = 3;
}

//! Query-frontend configuration.

use std::{net::SocketAddr, time::Duration};

/// Static configuration for the `query-frontend` role. Ports the fields the
/// legacy `QueryFrontendConfig` carried (querier addrs, live frontier, queue
/// depth, target bytes per job), plus the typed-merge knobs (default
/// limit/spss, max trace bytes, timeouts).
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    /// Querier backend addresses (`host:port`, no scheme) the HTTP pool fans
    /// over.
    pub querier_addrs: Vec<String>,
    /// Target bytes per search job; a block larger than this fans into
    /// per-row-group-range jobs. `0` disables row-group splitting.
    pub target_bytes_per_job: u64,
    /// Max jobs in flight at once across all queriers.
    pub max_concurrency: usize,
    /// Default trace limit when the request omits `limit` (Tempo default 20).
    pub default_limit: usize,
    /// Default spans-per-spanSet when the request omits `spss` (Tempo
    /// default 3).
    pub default_spss: usize,
    /// The cold-edge timestamp: data at/after it is in the live (hot) tier; the
    /// live shard is probed when a query window's `end_ns` reaches it.
    pub hot_frontier_ns: i64,
    /// Max assembled-trace size before the v2 by-id path returns `PARTIAL`.
    pub max_trace_bytes: u64,
    /// Per-backend-job timeout.
    pub request_timeout: Duration,
    /// The frontend's own listen address.
    pub listen_addr: SocketAddr,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            querier_addrs: vec!["127.0.0.1:3200".to_string()],
            // 0 => whole-block jobs (no row-group splitting), matching the
            // legacy default; the binary sets a real budget.
            target_bytes_per_job: 0,
            max_concurrency: 128,
            default_limit: 20,
            default_spss: 3,
            // 0 => the live tier is always probed (every window's end >= 0); the
            // binary wires the real per-partition frontier (hardening slice).
            hot_frontier_ns: 0,
            max_trace_bytes: 50 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            listen_addr: "0.0.0.0:3200".parse().expect("valid default addr"),
        }
    }
}

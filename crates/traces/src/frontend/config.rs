//! Query-frontend configuration.

use std::net::SocketAddr;

use crabka_units::{ByteSize, Time, bytes, mebibytes, secs};

/// Static configuration for the `query-frontend` role.
///
/// It ports the fields the legacy `QueryFrontendConfig` carried: querier
/// addresses, the live frontier, the queue depth, and the target bytes per job.
/// It adds the typed-merge knobs: the default limit and spss, the max trace
/// bytes, and the timeouts.
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    /// Querier backend addresses the HTTP pool fans over, as `host:port` with
    /// no scheme.
    pub querier_addrs: Vec<String>,
    /// Target size per search job; a block larger than this fans into
    /// per-row-group-range jobs. Zero disables row-group splitting.
    pub target_per_job: ByteSize,
    /// Max jobs in flight at once across all queriers.
    pub max_concurrency: usize,
    /// Default trace limit when the request omits `limit`. The Tempo default
    /// is 20.
    pub default_limit: usize,
    /// Default spans-per-spanSet when the request omits `spss`. The Tempo
    /// default is 3.
    pub default_spss: usize,
    /// The cold-edge timestamp. Data at or after it is in the live hot tier.
    /// The planner probes the live shard when a query window's `end_ns` reaches
    /// this timestamp.
    pub hot_frontier_ns: i64,
    /// Max assembled-trace size before the v2 by-id path returns `PARTIAL`.
    pub max_trace: ByteSize,
    /// Per-backend-job timeout.
    pub request_timeout: Time,
    /// The frontend's own listen address.
    pub listen_addr: SocketAddr,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            querier_addrs: vec!["127.0.0.1:3200".to_string()],
            // 0 => whole-block jobs (no row-group splitting), matching the
            // legacy default; the binary sets a real budget.
            target_per_job: bytes(0),
            max_concurrency: 128,
            default_limit: 20,
            default_spss: 3,
            // 0 => the live tier is always probed (every window's end >= 0); the
            // binary wires the real per-partition frontier (hardening slice).
            hot_frontier_ns: 0,
            max_trace: mebibytes(50),
            request_timeout: secs(30),
            listen_addr: "0.0.0.0:3200".parse().expect("valid default addr"),
        }
    }
}

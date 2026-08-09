//! CPU-profiling harness for the streams processing hot path.
//!
//! This harness builds a stateful `group_by_key` -> `count` topology with a
//! single subtopology and no repartition. It feeds N records through the
//! broker-free `TopologyTestDriver`, so the per-record engine path dominates the
//! profile: source deserialize, then processor graph, then state-store get and
//! put, then changelog drain. A bounded key cardinality keeps the store small and
//! exercises the read-modify-write path on existing keys.
//!
//! The test driver uses the in-memory (`BTreeMap`) store backend. This harness
//! therefore measures the engine and the in-memory store, and not the `turso`
//! state backend that production uses.
//!
//! ```text
//! CARGO_PROFILE_RELEASE_DEBUG=true cargo run --release \
//!   -p crabka-client-streams --example profile_streams
//! ```
//!
//! Env: `STREAMS_N` (records, default 2M), `STREAMS_KEYS` (default 1000),
//! `STREAMS_VALUE_BYTES` (default 100).

use crabka_client_streams::{Consumed, StringSerde, TopologyTestDriver, dsl::StreamsBuilder};

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

fn main() {
    let n: usize = env("STREAMS_N", 2_000_000);
    let keys: usize = env("STREAMS_KEYS", 1000);
    let value_bytes: usize = env("STREAMS_VALUE_BYTES", 100);

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count("counts");
    let built = b.build("app").expect("build topology");
    let mut d = TopologyTestDriver::new(&built).expect("driver");

    // Precompute the key pool so the measured loop doesn't pay for key
    // construction (only an unavoidable owned-String clone per pipe_input).
    let key_pool: Vec<String> = (0..keys).map(|i| format!("key-{i:08}")).collect();
    let value = "x".repeat(value_bytes);

    let start = std::time::Instant::now();
    for i in 0..n {
        let k = key_pool[i % keys].clone();
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k),
            value.clone(),
            i64::try_from(i).unwrap_or(i64::MAX),
        );
    }
    let elapsed = start.elapsed().as_secs_f64();
    let rate = n
        .to_string()
        .parse::<f64>()
        .expect("usize is representable as finite f64")
        / elapsed;
    eprintln!(
        "profile_streams: n={n} keys={keys} value={value_bytes}B | {rate:.0} rec/s in {elapsed:.2}s"
    );
}

//! Throughput of the range-0 grant path under high aggregate grant rate.
//!
//! Ignored by default (timing-sensitive, seconds long): run explicitly with
//! `cargo test -p crabka-gres-ranges --test tso_grant_pressure -- --ignored
//! --nocapture`. It drives the real serialized grant conveyor over a
//! [`TsoOracle`] whose durable committer models a range-0 WAL commit latency,
//! and compares `logical-tso` against `hlc` so the cost of the logical mode's
//! grant-volume-driven persist cadence is visible against HLC's wall-bounded
//! one.

use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use crabka_gres_ranges::{
    BatchedTsoClient, EpochHeartbeat, GrantLease, MemoryTsoHorizon, TsoError, TsoHorizonCommitter,
    TsoOracle, TsoOracleStats, TsoOracleStatsSnapshot, TsoRpc, TsoTimestamp, hlc_wall_clock,
};
use crabka_pgkv::MemKv;

/// Concurrent grant loops hammering the single serialized oracle.
const CONCURRENCY: u64 = 64;
/// Grants issued per loop.
const GRANTS_PER_TASK: u64 = 4_000;
/// Modeled range-0 durable-write latency for one horizon persist.
const PERSIST_LATENCY: Duration = Duration::from_micros(800);

/// Committer decorator that sleeps to model the range-0 WAL commit a horizon
/// persist pays for, then delegates to an in-memory horizon.
struct LatencyCommitter {
    inner: MemoryTsoHorizon,
    latency: Duration,
}

#[async_trait::async_trait]
impl TsoHorizonCommitter for LatencyCommitter {
    async fn persist_max_ts_for_epoch(
        &self,
        epoch: i16,
        max_ts: TsoTimestamp,
    ) -> Result<(), TsoError> {
        tokio::time::sleep(self.latency).await;
        self.inner.persist_max_ts_for_epoch(epoch, max_ts).await
    }
}

/// Adapts an oracle into the `TsoRpc` seam the conveyor drives.
struct OracleRpc<C, H>(Arc<TsoOracle<C, H>>);

#[async_trait::async_trait]
impl<C, H> TsoRpc for OracleRpc<C, H>
where
    C: TsoHorizonCommitter + 'static,
    H: EpochHeartbeat + 'static,
{
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        self.0.grant(count).await
    }
}

fn one() -> NonZeroU64 {
    NonZeroU64::new(1).expect("one is non-zero")
}

async fn hammer(client: BatchedTsoClient<impl TsoRpc>) {
    let mut tasks = Vec::new();
    for _ in 0..CONCURRENCY {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..GRANTS_PER_TASK {
                client.grant(one()).await.expect("grant");
            }
        }));
    }
    for task in tasks {
        task.await.expect("join");
    }
}

fn report(mode: &str, elapsed: Duration, stats: TsoOracleStatsSnapshot) {
    let total = CONCURRENCY * GRANTS_PER_TASK;
    // Integer grants-per-second: avoids a lossy u64 -> f64 cast, and the inputs
    // are exact anyway.
    let tps = u128::from(total) * 1_000 / elapsed.as_millis().max(1);
    println!(
        "{mode:>11}: {total} grants in {elapsed:>8.2?} = {tps:>10} grants/s | \
         persists={} waits={} timestamps={}",
        stats.horizon_persists, stats.horizon_waits, stats.timestamps_granted,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timing benchmark; run explicitly with --ignored --nocapture"]
async fn logical_vs_hlc_grant_throughput_under_pressure() {
    // Logical: dense counter, base stride 1024 timestamps.
    let logical_stats = Arc::new(TsoOracleStats::default());
    let logical_elapsed = {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 1);
        let committer = LatencyCommitter {
            inner: horizon.clone(),
            latency: PERSIST_LATENCY,
        };
        let oracle = Arc::new(
            TsoOracle::recover(committer, horizon, 1, NonZeroU64::new(1024).unwrap(), 0)
                .expect("recover logical")
                .with_stats(Arc::clone(&logical_stats)),
        );
        let client = BatchedTsoClient::new(Arc::new(OracleRpc(oracle)));
        let start = Instant::now();
        hammer(client).await;
        start.elapsed()
    };

    // HLC: wall-anchored, stride = pack(128ms, 0) = 128 << 22 in the packed
    // domain (whole milliseconds of wall headroom).
    let hlc_stats = Arc::new(TsoOracleStats::default());
    let hlc_elapsed = {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 1);
        let committer = LatencyCommitter {
            inner: horizon.clone(),
            latency: PERSIST_LATENCY,
        };
        let stride = NonZeroU64::new(128_u64 << 22).expect("packed stride is non-zero");
        let oracle = Arc::new(
            TsoOracle::recover_hlc(committer, horizon, 1, stride, 0, hlc_wall_clock(0))
                .expect("recover hlc")
                .with_stats(Arc::clone(&hlc_stats)),
        );
        let client = BatchedTsoClient::new(Arc::new(OracleRpc(oracle)));
        let start = Instant::now();
        hammer(client).await;
        start.elapsed()
    };

    report("logical-tso", logical_elapsed, logical_stats.snapshot());
    report("hlc", hlc_elapsed, hlc_stats.snapshot());
}

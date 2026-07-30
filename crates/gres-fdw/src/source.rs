//! Bounded per-partition snapshot scan over `crabka-client-core`.
//!
//! Phase-1 contract:
//! * [`scan_topic`] materialises all records between a per-partition start
//!   offset (inclusive) and the high-water mark that was snapshotted at the
//!   beginning of the scan (exclusive), using `READ_COMMITTED` isolation.
//! * [`plan_fetch`] is a pure helper that maps `(earliest, hwm, partition,
//!   bounds)` → [`FetchPlan`]; it is unit-tested independently of any broker.

use crabka_client_admin::AdminClient;
use crabka_client_core::{
    Connection, DEFAULT_FETCH_RESPONSE_MAX, FetchedHeader, IsolatedFetch,
    fetch_partition_with_isolation,
};
use crabka_pgexec::foreign::ScanBounds;
use crabka_protocol::{
    owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    primitives::uuid::Uuid as WireUuid,
};
use crabka_units::{ByteSize, Time, convert::TimeExt as _, mebibytes, secs};

use crate::{config::ConnProfile, error::KafkaFdwError};

// ── Public types ─────────────────────────────────────────────────────────────

/// A single Kafka record decoded from a raw fetch, before schema-aware decoding.
#[derive(Debug, Clone)]
pub struct RawRecord {
    /// Partition this record came from.
    pub partition: i32,
    /// Absolute offset within the partition.
    pub offset: i64,
    /// Record timestamp (epoch millis).
    pub timestamp_ms: i64,
    /// Record key, if present.
    pub key: Option<Vec<u8>>,
    /// Record value, if present.
    pub value: Option<Vec<u8>>,
    /// Record headers as (key, optional-value) pairs.
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

fn raw_headers_from_fetched(headers: Vec<FetchedHeader>) -> Vec<(String, Option<Vec<u8>>)> {
    headers
        .into_iter()
        .map(|header| (header.key, header.value.map(|value| value.to_vec())))
        .collect()
}

/// Per-partition fetch boundaries and record count limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPlan {
    /// First offset to fetch (inclusive).
    pub start: i64,
    /// First offset to stop at (exclusive).  When `start >= stop` the
    /// partition is empty and the fetch loop can be skipped.
    pub stop: i64,
    /// Optional record count cap.  When `None`, fetch until `stop`.
    pub max_records: Option<usize>,
}

// ── Pure boundary math ────────────────────────────────────────────────────────

/// Compute the fetch plan for one partition.
///
/// * `start = max(earliest, start_offset_for_partition)` — clamp to what the
///   caller asked for, but never below the partition's earliest retained offset.
/// * `stop  = min(hwm,      end_offset_for_partition)`   — clamp to the HWM
///   snapshotted at scan-start; never read past the mark in effect when the
///   scan started.
/// * `max_records` — taken from the `ScanBounds::end_offsets` map when the
///   stop offset is a tight bound (i.e. `end_offsets` is non-empty for this
///   partition).  `None` otherwise.
///
/// The function is **pure** (no I/O) and is the TDD gate for offset clamping.
#[must_use]
pub fn plan_fetch(earliest: i64, hwm: i64, partition: i32, bounds: &ScanBounds) -> FetchPlan {
    // Resolve per-partition start offset from `bounds.start_offsets`.
    let start_bound = bounds
        .start_offsets
        .iter()
        .find(|(p, _)| *p == partition)
        .map(|(_, off)| *off);

    // Resolve per-partition end offset from `bounds.end_offsets`.
    let end_bound = bounds
        .end_offsets
        .iter()
        .find(|(p, _)| *p == partition)
        .map(|(_, off)| *off);

    let start = match start_bound {
        Some(lo) => lo.max(earliest),
        None => earliest,
    };

    let stop = match end_bound {
        Some(hi) => hi.min(hwm),
        None => hwm,
    };

    // `max_records` is surfaced as the number of records between start and
    // stop when there is an explicit end bound, so that callers can allocate
    // exactly that much.  When the bounds cover all the way to HWM it stays
    // `None` (unlimited until HWM).
    let max_records = end_bound.map(|hi| {
        let count = hi.min(hwm) - start;
        if count > 0 {
            usize::try_from(count).unwrap_or(usize::MAX)
        } else {
            0
        }
    });

    FetchPlan {
        start,
        stop,
        max_records,
    }
}

// ── Broker-backed scan ────────────────────────────────────────────────────────

/// Timestamp sentinel meaning "earliest available offset".
const EARLIEST: i64 = -2;
/// Timestamp sentinel meaning "latest offset (high-water mark)".
const LATEST: i64 = -1;
/// Consumer replica ID (`-1` = non-replica consumer).
const CONSUMER_REPLICA_ID: i32 = -1;
/// `READ_COMMITTED` isolation level for the Fetch API.
const READ_COMMITTED: i8 = 1;
/// Maximum wait time per Fetch RPC.
const MAX_WAIT: Time = secs(5);
/// Maximum bytes per partition per Fetch RPC.
const PARTITION_MAX: ByteSize = mebibytes(10);
/// Deadline for one TCP connection attempt to a bootstrap broker.
const CONNECT_TIMEOUT: Time = secs(10);
/// Deadline for one request issued over the scan's connection.
const REQUEST_TIMEOUT: Time = secs(30);

/// The Fetch RPC's per-call budgets: how long the broker may hold the request
/// open, and how much of one partition it may return.
fn fetch_budgets() -> (Time, ByteSize) {
    (MAX_WAIT, PARTITION_MAX)
}

/// Materialise a bounded snapshot of `topic` into a flat `Vec<RawRecord>`.
///
/// # Behaviour
/// 1. Installs the rustcrypto TLS provider (idempotent).
/// 2. Resolves partition metadata via `AdminClient`.
/// 3. Opens a **single** [`Connection`] to the first bootstrap address and
///    reuses it for both the `ListOffsets` RPCs and the per-partition fetch
///    loop — no second connect, no `Client`/`ApiVersionsRequest` probe.
/// 4. For each partition (filtered by `bounds.start_offsets`/`end_offsets` when
///    non-empty), the batched `ListOffsets` RPCs supply the earliest retained
///    offset and the current high-water mark.
/// 5. Computes a [`FetchPlan`] per partition and loops
///    `fetch_partition_with_isolation` until the plan is exhausted.
/// 6. Returns all records in (partition, offset) order.
///
/// The broker-backed path is exercised end-to-end in Task 16 (in-process
/// broker).  The pure [`plan_fetch`] tests are the gate here.
///
/// # Errors
/// Returns [`KafkaFdwError`] on transport failures, unknown topics, or broker
/// errors.
pub async fn scan_topic(
    profile: &ConnProfile,
    topic: &str,
    bounds: &ScanBounds,
) -> Result<Vec<RawRecord>, KafkaFdwError> {
    scan_topic_with_dns_timeout(
        profile,
        topic,
        bounds,
        crabka_client_core::ClientDnsTimeout::default(),
    )
    .await
}

/// Materialise a bounded snapshot with an explicit broker DNS deadline.
///
/// # Errors
/// Returns [`KafkaFdwError`] on transport failures, unknown topics, or broker
/// errors.
pub async fn scan_topic_with_dns_timeout(
    profile: &ConnProfile,
    topic: &str,
    bounds: &ScanBounds,
    dns_timeout: crabka_client_core::ClientDnsTimeout,
) -> Result<Vec<RawRecord>, KafkaFdwError> {
    // Step 1: ensure the rustcrypto TLS provider is installed.
    crate::provider::install_default_provider();

    // Step 2: resolve partition metadata.
    let mut admin = AdminClient::connect_secured_with_dns_timeout(
        &profile.bootstrap,
        profile.security.clone(),
        dns_timeout,
    )
    .await
    .map_err(|e| KafkaFdwError::Other(format!("admin connect: {e}")))?;

    let meta = admin
        .metadata(&[topic])
        .await
        .map_err(|e| KafkaFdwError::Other(format!("metadata: {e}")))?;

    let topic_meta = meta
        .topics
        .into_iter()
        .find(|t| t.name == topic)
        .ok_or_else(|| {
            KafkaFdwError::Other(format!("topic {topic:?} not found in metadata response"))
        })?;

    if let Some(ref err) = topic_meta.error {
        return Err(KafkaFdwError::Other(format!(
            "metadata error for topic {topic:?}: {} ({})",
            err.name, err.code
        )));
    }

    // The topic UUID (may be None for pre-v2.8 clusters; zero UUID is fine).
    let topic_uuid: WireUuid = topic_meta
        .topic_id
        .map_or(WireUuid::ZERO, |u| WireUuid(u.into_bytes()));

    // Enumerate partitions 0..partition_count; filter when bounds specify a
    // subset (non-empty start_offsets acts as the partition allowlist).
    let all_partitions: Vec<i32> = (0..topic_meta.partition_count).collect();
    let partitions: Vec<i32> = if bounds.start_offsets.is_empty() && bounds.end_offsets.is_empty() {
        all_partitions
    } else {
        // Union of partition ids mentioned in either vector.
        let mut ids: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
        for (p, _) in &bounds.start_offsets {
            ids.insert(*p);
        }
        for (p, _) in &bounds.end_offsets {
            ids.insert(*p);
        }
        if ids.is_empty() {
            all_partitions
        } else {
            ids.into_iter()
                .filter(|p| *p < topic_meta.partition_count)
                .collect()
        }
    };

    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    // Step 3: open ONE connection and reuse it for ListOffsets + Fetch.
    let conn = open_connection(profile, dns_timeout).await?;

    // Step 4: ListOffsets — batch earliest + HWM for all partitions in one RPC.
    let list_offsets_req_earliest = ListOffsetsRequest {
        replica_id: CONSUMER_REPLICA_ID,
        isolation_level: READ_COMMITTED,
        topics: vec![ListOffsetsTopic {
            name: topic.to_string(),
            partitions: partitions
                .iter()
                .map(|&p| ListOffsetsPartition {
                    partition_index: p,
                    timestamp: EARLIEST,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let list_offsets_req_latest = ListOffsetsRequest {
        replica_id: CONSUMER_REPLICA_ID,
        isolation_level: READ_COMMITTED,
        topics: vec![ListOffsetsTopic {
            name: topic.to_string(),
            partitions: partitions
                .iter()
                .map(|&p| ListOffsetsPartition {
                    partition_index: p,
                    timestamp: LATEST,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let earliest_resp = conn
        .send(list_offsets_req_earliest)
        .await
        .map_err(|e| KafkaFdwError::Other(format!("ListOffsets(earliest): {e}")))?;

    let latest_resp = conn
        .send(list_offsets_req_latest)
        .await
        .map_err(|e| KafkaFdwError::Other(format!("ListOffsets(latest): {e}")))?;

    // Build lookup maps: partition → earliest offset / HWM.
    let mut earliest_map: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
    for t in &earliest_resp.topics {
        if t.name == topic {
            for p in &t.partitions {
                if p.error_code == 0 {
                    earliest_map.insert(p.partition_index, p.offset);
                }
            }
        }
    }

    let mut hwm_map: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
    for t in &latest_resp.topics {
        if t.name == topic {
            for p in &t.partitions {
                if p.error_code == 0 {
                    hwm_map.insert(p.partition_index, p.offset);
                }
            }
        }
    }

    // Step 5: fetch loop per partition, over the same `conn` used above.
    let mut records: Vec<RawRecord> = Vec::new();

    for partition in &partitions {
        let partition = *partition;
        let earliest = *earliest_map.get(&partition).unwrap_or(&0);
        let hwm = *hwm_map.get(&partition).unwrap_or(&0);

        let plan = plan_fetch(earliest, hwm, partition, bounds);

        if plan.start >= plan.stop {
            // Nothing to fetch for this partition.
            continue;
        }

        let mut next_offset = plan.start;

        loop {
            if next_offset >= plan.stop {
                break;
            }
            // LATENT CROSS-PARTITION TRUNCATION: `plan.max_records` is compared
            // against the CUMULATIVE `records.len()` across ALL partitions, not
            // just the current one.  Today this is masked because pushdown emits
            // at most one `_partition=N` anchor per query (so ≤1 partition has
            // an end bound and `plan.stop` is the real guard), but if multi-
            // partition pushdown is added, the first partition can consume the
            // entire `max_records` budget and silently truncate later partitions.
            // Fix before enabling multi-partition end_offsets pushdown.
            if let Some(max) = plan.max_records
                && records.len() >= max
            {
                break;
            }

            let (max_wait, partition_max) = fetch_budgets();
            let fetched = fetch_partition_with_isolation(
                &conn,
                IsolatedFetch {
                    topic,
                    topic_id: topic_uuid,
                    partition,
                    fetch_offset: next_offset,
                    max_wait,
                    max: DEFAULT_FETCH_RESPONSE_MAX,
                    partition_max,
                    fetch_min: crabka_client_core::FetchMinBytes::default(),
                    isolation_level: READ_COMMITTED,
                },
            )
            .await
            .map_err(|e| {
                KafkaFdwError::Other(format!(
                    "fetch partition {partition} offset {next_offset}: {e}"
                ))
            })?;

            if fetched.is_empty() {
                // No records at or after `next_offset` within `MAX_WAIT`.
                break;
            }

            let mut advanced = false;
            for fr in fetched {
                if fr.offset >= plan.stop {
                    break;
                }
                if fr.offset >= next_offset {
                    next_offset = fr.offset + 1;
                    advanced = true;
                }
                records.push(RawRecord {
                    partition,
                    offset: fr.offset,
                    timestamp_ms: fr.timestamp,
                    key: fr.key.map(|b| b.to_vec()),
                    value: fr.value.map(|b| b.to_vec()),
                    headers: raw_headers_from_fetched(fr.headers),
                });

                if let Some(max) = plan.max_records
                    && records.len() >= max
                {
                    break;
                }
            }

            if !advanced {
                // Guard against an infinite loop if the broker returns
                // records all below `next_offset` (shouldn't happen but
                // is defensive).
                break;
            }
        }
    }

    Ok(records)
}

/// Resolve the first address within the configured DNS deadline.
async fn lookup_first<F, I>(
    host_port: &str,
    dns_timeout: crabka_client_core::ClientDnsTimeout,
    lookup: F,
) -> Result<std::net::SocketAddr, KafkaFdwError>
where
    F: std::future::Future<Output = std::io::Result<I>>,
    I: Iterator<Item = std::net::SocketAddr>,
{
    let mut addrs = tokio::time::timeout(dns_timeout.time().to_std(), lookup)
        .await
        .map_err(|_| {
            KafkaFdwError::Other(format!(
                "DNS lookup {host_port} timed out after {} ms",
                dns_timeout.milliseconds(),
            ))
        })?
        .map_err(|error| KafkaFdwError::Other(format!("DNS lookup {host_port}: {error}")))?;
    addrs
        .next()
        .ok_or_else(|| KafkaFdwError::Other(format!("no addresses for {host_port}")))
}

/// Open a single raw [`Connection`] to the first bootstrap address.
///
/// `fetch_partition_with_isolation` requires a `&Connection`, and `Connection`
/// also serves the `ListOffsets` RPCs via [`Connection::send`], so one
/// connection covers the whole scan. (`Client` exposes neither a fetch method
/// nor its underlying `Connection`, so there is nothing to be gained by also
/// building a `Client`.)
async fn open_connection(
    profile: &ConnProfile,
    dns_timeout: crabka_client_core::ClientDnsTimeout,
) -> Result<Connection, KafkaFdwError> {
    let host_port = profile.bootstrap.first().ok_or_else(|| {
        KafkaFdwError::Config("no bootstrap address in connection profile".to_string())
    })?;

    let addr = lookup_first(host_port, dns_timeout, tokio::net::lookup_host(host_port)).await?;

    crabka_client_core::Connection::connect_with_options(addr, connection_options(profile))
        .await
        .map_err(|e| KafkaFdwError::Other(format!("connect to {host_port}: {e}")))
}

/// The scan connection's knobs.
fn connection_options(profile: &ConnProfile) -> crabka_client_core::ConnectionOptions {
    crabka_client_core::ConnectionOptions {
        client_id: "crabka-fdw".to_string(),
        connect_timeout: CONNECT_TIMEOUT,
        request_timeout: REQUEST_TIMEOUT,
        security: profile.security.clone().map(Box::new),
        ..crabka_client_core::ConnectionOptions::default()
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    // Helper: `ScanBounds` with per-partition start/end vectors.
    fn bounds_with(start_offsets: Vec<(i32, i64)>, end_offsets: Vec<(i32, i64)>) -> ScanBounds {
        ScanBounds {
            start_offsets,
            end_offsets,
        }
    }

    // ── Verbatim test from task brief (adapted to actual ScanBounds) ──────

    /// Adapted from the task-8 brief: partition hwm=100, earliest=0;
    /// bounds request start at offset 10 for partition 0; no end bound.
    /// Expected: start=10, stop=100 (hwm), `max_records=None`.
    #[test]
    fn offset_bounds_clamp_to_hwm() {
        let plan = plan_fetch(0, 100, 0, &bounds_with(vec![(0, 10)], vec![]));
        assert_eq!(plan.start, 10, "start should clamp to offset_lo=10");
        assert_eq!(plan.stop, 100, "stop should clamp to hwm=100");
        assert_eq!(
            plan.max_records, None,
            "no end_offset → max_records is None"
        );
    }

    // ── Additional coverage ───────────────────────────────────────────────

    /// No bounds at all → scan from earliest to hwm.
    #[test]
    fn no_bounds_scans_full_range() {
        let plan = plan_fetch(5, 200, 0, &ScanBounds::default());
        assert_eq!(plan.start, 5, "start = earliest when no start_offsets");
        assert_eq!(plan.stop, 200, "stop = hwm when no end_offsets");
        assert_eq!(plan.max_records, None);
    }

    /// `offset_lo` above hwm → empty range (start >= stop).
    #[test]
    fn start_offset_above_hwm_gives_empty_range() {
        let plan = plan_fetch(0, 50, 0, &bounds_with(vec![(0, 99)], vec![]));
        // start = max(0, 99) = 99; stop = min(50, hwm=50) = 50; 99 >= 50 → empty
        assert!(
            plan.start >= plan.stop,
            "start ({}) should be >= stop ({}) when lo > hwm",
            plan.start,
            plan.stop
        );
    }

    /// `offset_lo` below earliest → clamped up to earliest.
    #[test]
    fn start_offset_below_earliest_clamps_up() {
        let plan = plan_fetch(10, 100, 0, &bounds_with(vec![(0, 2)], vec![]));
        // start = max(10, 2) = 10
        assert_eq!(plan.start, 10, "start must not go below earliest");
        assert_eq!(plan.stop, 100);
    }

    /// End bound below hwm clips the stop.
    #[test]
    fn end_offset_below_hwm_clips_stop() {
        let plan = plan_fetch(0, 100, 0, &bounds_with(vec![], vec![(0, 40)]));
        assert_eq!(plan.stop, 40, "stop should be min(end_offset=40, hwm=100)");
        assert_eq!(plan.start, 0);
        // max_records = 40 - 0 = 40
        assert_eq!(plan.max_records, Some(40));
    }

    /// End bound above hwm → clamped to hwm.
    #[test]
    fn end_offset_above_hwm_clamps_to_hwm() {
        let plan = plan_fetch(0, 50, 0, &bounds_with(vec![], vec![(0, 200)]));
        // stop = min(200, 50) = 50
        assert_eq!(plan.stop, 50, "stop must not exceed hwm");
        // max_records = min(200, 50) - 0 = 50
        assert_eq!(plan.max_records, Some(50));
    }

    /// Bounds for a different partition are ignored.
    #[test]
    fn bounds_for_other_partition_are_ignored() {
        // Partition 1 has start_offset=30, but we're planning partition 0.
        let plan = plan_fetch(0, 100, 0, &bounds_with(vec![(1, 30)], vec![]));
        // No bounds apply to partition 0 → full range.
        assert_eq!(plan.start, 0);
        assert_eq!(plan.stop, 100);
        assert_eq!(plan.max_records, None);
    }

    /// Both start and end offsets set → narrow range.
    #[test]
    fn both_start_and_end_offset_set() {
        let plan = plan_fetch(0, 100, 0, &bounds_with(vec![(0, 20)], vec![(0, 60)]));
        assert_eq!(plan.start, 20);
        assert_eq!(plan.stop, 60);
        // max_records = 60 - 20 = 40
        assert_eq!(plan.max_records, Some(40));
    }

    /// Empty partition (earliest == hwm) always gives an empty plan.
    #[test]
    fn empty_partition_earliest_eq_hwm() {
        let plan = plan_fetch(42, 42, 0, &ScanBounds::default());
        assert!(
            plan.start >= plan.stop,
            "start ({}) >= stop ({}) for empty partition",
            plan.start,
            plan.stop
        );
    }

    /// The Fetch budgets carry the magnitudes the constants name: 5 s and
    /// 10 MiB.
    #[test]
    fn fetch_budgets_carry_the_configured_magnitudes() {
        assert!(fetch_budgets() == (secs(5), mebibytes(10)));
    }

    /// The connection deadlines reach `crabka-client-core` as the durations the
    /// quantities name.
    #[test]
    fn connection_options_carry_the_configured_deadlines() {
        let profile = ConnProfile {
            bootstrap: vec!["b:9092".into()],
            registry_url: String::new(),
            security: None,
            topic: "events".into(),
            value_format: crate::decode::Wire::Raw,
            key_format: crate::decode::Wire::Raw,
        };

        let options = connection_options(&profile);

        assert!((options.connect_timeout, options.request_timeout) == (secs(10), secs(30)));
    }

    #[tokio::test(start_paused = true)]
    async fn raw_dns_lookup_stops_at_configured_deadline() {
        let timeout = crabka_client_core::ClientDnsTimeout::new(Time::from_millis(37))
            .expect("positive timeout");
        let started = tokio::time::Instant::now();
        let pending =
            std::future::pending::<std::io::Result<std::vec::IntoIter<std::net::SocketAddr>>>();

        let error = lookup_first("broker.example:9092", timeout, pending)
            .await
            .expect_err("lookup times out");

        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_millis(37)
        );
        assert!(
            error
                .to_string()
                .contains("DNS lookup broker.example:9092 timed out after 37 ms")
        );
    }
}

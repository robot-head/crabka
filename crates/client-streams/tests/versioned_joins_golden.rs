//! Behavioral golden-replay tests for KIP-914 versioned-table joins.
//!
//! Each test builds the same topology the JVM capture programs build, replays
//! the captured input records through the broker-free [`TopologyTestDriver`],
//! and asserts the produced output matches a committed golden JSON captured
//! from JVM Kafka Streams 4.1.0.
//!
//! Golden files live under `tests/testdata/versioned_joins/`. The `out` array
//! in each golden is the JVM-produced output sequence (`key`, `value`, `ts`).
//! The driver's `read_output` exposes key+value (not timestamp), so we assert
//! key+value+order; the as-of *values* already encode timestamp-correctness
//! (an as-of(150) read of the table yields 10 → 11, an as-of(250) read yields
//! 20 → 21), so a wrong as-of timestamp would surface as a wrong value.

use crabka_client_streams::dsl::StreamsBuilder;
use crabka_client_streams::{Consumed, I64Serde, Materialized, Produced, StringSerde};
use serde::Deserialize;

/// One output record in a golden `out` array.
#[derive(Debug, Deserialize)]
struct GoldenOut {
    key: String,
    value: i64,
    #[allow(dead_code)] // `read_output` does not expose ts; kept for fidelity
    ts: i64,
}

/// The golden envelope shared by the versioned-join scenarios.
#[derive(Debug, Deserialize)]
struct Golden {
    #[allow(dead_code)]
    scenario: String,
    #[allow(dead_code)]
    history_retention_ms: i64,
    out: Vec<GoldenOut>,
    #[allow(dead_code)]
    describe: String,
}

/// Load + parse a golden JSON from the versioned-joins testdata dir.
fn load_golden(name: &str) -> Golden {
    let path = format!("tests/testdata/versioned_joins/{name}");
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read golden {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse golden {path}: {e}"))
}

/// As-of stream-table inner join (KIP-914).
///
/// A versioned table holds `(a,10)@100` then `(a,20)@200` (`history_retention` =
/// `600_000` ms). A `KStream` inner-joins it with `|s, t| s + t`:
///   - stream `(a,1)@150` → as-of(150) = 10 → emits 11
///   - stream `(a,1)@250` → as-of(250) = 20 → emits 21
///   - stream `(a,1)@50`  → predates the first version → inner join → NO output
#[test]
fn asof_stream_table_join_matches_golden() {
    let golden = load_golden("asof.json");

    let b = StreamsBuilder::new();
    let table = b.table_explicit::<StringSerde, I64Serde>(
        "table",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", 600_000),
    );
    b.stream_explicit::<StringSerde, I64Serde>(["stream"], Consumed::with(StringSerde, I64Serde))
        .join_table(&table, |s: &i64, t: &i64| s + t)
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    drop(table);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Pipe the TABLE versions first so they exist when the stream side joins.
    for (k, v, ts) in [("a", 10_i64, 100_i64), ("a", 20, 200)] {
        d.pipe_input(
            "table",
            Consumed::with(StringSerde, I64Serde),
            Some(k.to_string()),
            v,
            ts,
        );
    }
    // Then the STREAM records. The @50 record predates the first version.
    for (k, v, ts) in [("a", 1_i64, 150_i64), ("a", 1, 250), ("a", 1, 50)] {
        d.pipe_input(
            "stream",
            Consumed::with(StringSerde, I64Serde),
            Some(k.to_string()),
            v,
            ts,
        );
    }

    // Collect every output record (key + value) in order.
    let mut got: Vec<(Option<String>, i64)> = Vec::new();
    while let Some(rec) = d.read_output("out", Produced::with(StringSerde, I64Serde)) {
        got.push(rec);
    }

    let expected: Vec<(Option<String>, i64)> = golden
        .out
        .iter()
        .map(|o| (Some(o.key.clone()), o.value))
        .collect();

    assert_eq!(
        expected,
        vec![(Some("a".to_string()), 11), (Some("a".to_string()), 21)],
        "golden sanity: as-of join expected outputs"
    );
    assert_eq!(
        got, expected,
        "as-of join output must match JVM golden (the @50 record must produce nothing)"
    );
}

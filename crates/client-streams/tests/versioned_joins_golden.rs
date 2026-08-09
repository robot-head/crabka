//! Behavioral golden-replay tests for KIP-914 versioned-table joins.
//!
//! Each test builds the same topology the JVM capture programs build. The test
//! replays the captured input records through the broker-free
//! [`TopologyTestDriver`] and asserts that the output matches a committed golden
//! JSON captured from JVM Kafka Streams 4.1.0.
//!
//! Golden files live under `tests/testdata/versioned_joins/`. The `out` array in
//! each golden holds the JVM-produced output sequence of `key`, `value`, and
//! `ts`. The driver's `read_output` exposes key and value but not the timestamp,
//! so the tests assert key, value, and order.
//!
//! The as-of *values* already encode timestamp correctness. An as-of(150) read of
//! the table yields 10, so the join emits 11. An as-of(250) read yields 20, so
//! the join emits 21. A wrong as-of timestamp gives a wrong value.

use assert2::check;
use crabka_client_streams::{
    Consumed, I64Serde, Materialized, Produced, StringSerde, dsl::StreamsBuilder,
};
use crabka_units::prelude::*;
use serde::Deserialize;

/// One String-valued output record in the table-table golden `out` array.
#[derive(Debug, Deserialize)]
struct GoldenOutStr {
    key: String,
    value: String,
    #[allow(dead_code)] // `read_output` does not expose ts; kept for fidelity
    ts: i64,
}

/// The table-table golden envelope with a String-valued `out` array.
#[derive(Debug, Deserialize)]
struct TableTableGolden {
    #[allow(dead_code)]
    scenario: String,
    #[allow(dead_code)]
    history_retention_ms: i64,
    out: Vec<GoldenOutStr>,
    #[allow(dead_code)]
    describe: String,
}

fn load_table_table_golden() -> TableTableGolden {
    let path = "tests/testdata/versioned_joins/tabletable.json";
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read golden {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse golden {path}: {e}"))
}

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

/// Loads and parses a golden JSON file from the versioned-joins testdata directory.
fn load_golden(name: &str) -> Golden {
    let path = format!("tests/testdata/versioned_joins/{name}");
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read golden {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse golden {path}: {e}"))
}

/// As-of stream-table inner join (KIP-914).
///
/// A versioned table with `history_retention` = `600_000` ms holds `(a,10)@100`
/// then `(a,20)@200`. A `KStream` inner-joins it with `|s, t| s + t`:
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
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
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

/// As-of stream-table LEFT join (KIP-914), no grace.
///
/// The test routes a left join with `emit_on_miss = true` to the as-of processor.
/// The versioned table holds `(a,10)@100`. The joiner is
/// `|s, t| s + t.copied().unwrap_or(-1)`, so a hit adds the table value and a
/// miss adds the `-1` sentinel:
///   - stream `(a,1)@150` → as-of(150) = 10 → hit → emits `1 + 10 = 11`
///   - stream `(b,1)@150` → no version for `b` → as-of miss → `None` → emits
///     `1 + (-1) = 0`, because the left branch forwards on a miss
///
/// This test exercises the `left_join_table` routing into the as-of processor and
/// the as-of-miss → `None` path with `emit_on_miss = true`.
#[test]
fn asof_stream_table_left_join_emits_on_miss() {
    let b = StreamsBuilder::new();
    let table = b.table_explicit::<StringSerde, I64Serde>(
        "table",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
    );
    b.stream_explicit::<StringSerde, I64Serde>(["stream"], Consumed::with(StringSerde, I64Serde))
        .left_join_table(&table, |s: &i64, t: Option<&i64>| {
            s + t.copied().unwrap_or(-1)
        })
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    drop(table);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // TABLE version: (a,10)@100. There is no version for key `b`.
    d.pipe_input(
        "table",
        Consumed::with(StringSerde, I64Serde),
        Some("a".to_string()),
        10_i64,
        100,
    );

    // STREAM (a,1)@150 → hit → 1 + 10 = 11.
    d.pipe_input(
        "stream",
        Consumed::with(StringSerde, I64Serde),
        Some("a".to_string()),
        1_i64,
        150,
    );
    // STREAM (b,1)@150 → as-of miss (no `b` version) → joiner gets None → 1 + -1 = 0.
    d.pipe_input(
        "stream",
        Consumed::with(StringSerde, I64Serde),
        Some("b".to_string()),
        1_i64,
        150,
    );

    let mut got: Vec<(Option<String>, i64)> = Vec::new();
    while let Some(rec) = d.read_output("out", Produced::with(StringSerde, I64Serde)) {
        got.push(rec);
    }

    assert_eq!(
        got,
        vec![(Some("a".to_string()), 11), (Some("b".to_string()), 0)],
        "as-of LEFT join: hit forwards 1+10=11; the as-of MISS for `b` must still \
         forward (emit_on_miss) with the joiner receiving None → 1+(-1)=0"
    );
}

/// Grace stream-table LEFT join (KIP-923) with a table miss.
///
/// The grace wiring is the same as [`grace_stream_table_join_matches_golden`],
/// but this test lowers a left join with `emit_on_miss = true` through the *left*
/// branch of `build_grace_lowering`. The versioned table holds `(a,10)@100`. The
/// joiner is `|s, t| s + t.copied().unwrap_or(-1)`. Buffered stream records drain
/// in ascending-ts order once stream-time crosses `bufTs + 60_000`:
///   - `(a,1)@150` → drained as-of(150) = 10 → hit → `1 + 10 = 11`
///   - `(b,1)@150` → drained as-of(150) for `b` = MISS → `None` → `1 + (-1) = 0`
///
/// A flush record `(x,9)@1_000_000` advances stream-time past the
/// `150 + 60_000 = 60_150` horizon and drains both buffered records. The flush
/// record itself stays buffered, because stream-time never reaches its horizon
/// `1_060_000`.
///
/// This is the only test that exercises the grace processor's
/// `emit_on_miss = true` drain path and the left branch of `build_grace_lowering`.
#[test]
fn grace_stream_table_left_join_emits_on_miss() {
    use crabka_client_streams::Joined;

    let b = StreamsBuilder::new();
    let table = b.table_explicit::<StringSerde, I64Serde>(
        "table",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
    );
    b.stream_explicit::<StringSerde, I64Serde>(["stream"], Consumed::with(StringSerde, I64Serde))
        .left_join_table_with(
            &table,
            |s: &i64, t: Option<&i64>| s + t.copied().unwrap_or(-1),
            Joined::with_grace_period(millis(60_000)),
        )
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    drop(table);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // TABLE version: (a,10)@100. No version for key `b`.
    d.pipe_input(
        "table",
        Consumed::with(StringSerde, I64Serde),
        Some("a".to_string()),
        10_i64,
        100,
    );

    // STREAM records buffer (their grace horizon ts+60_000 is far past stream-time).
    for k in ["a", "b"] {
        d.pipe_input(
            "stream",
            Consumed::with(StringSerde, I64Serde),
            Some(k.to_string()),
            1_i64,
            150,
        );
    }
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        None,
        "buffered records must not emit before the grace horizon is crossed"
    );

    // FLUSH record @1_000_000 advances stream-time → threshold 940_000, draining
    // both @150 records. The flush record (key `x`) itself stays buffered.
    d.pipe_input(
        "stream",
        Consumed::with(StringSerde, I64Serde),
        Some("x".to_string()),
        9_i64,
        1_000_000,
    );

    let mut got: Vec<(Option<String>, i64)> = Vec::new();
    while let Some(rec) = d.read_output("out", Produced::with(StringSerde, I64Serde)) {
        got.push(rec);
    }

    assert_eq!(
        got,
        vec![(Some("a".to_string()), 11), (Some("b".to_string()), 0)],
        "grace LEFT join drain: (a,1)@150 hits as-of(150)=10 → 11; (b,1)@150 is an \
         as-of MISS but the LEFT grace path still emits with None → 1+(-1)=0; the \
         flush record (x) must NOT emit"
    );
}

/// The grace-scenario golden envelope.
///
/// On top of the shared `out` sequence, it adds the buffer-store changelog name
/// and the config that the wire assertion pins.
#[derive(Debug, Deserialize)]
struct GraceGolden {
    #[allow(dead_code)]
    scenario: String,
    #[allow(dead_code)]
    grace_ms: i64,
    #[allow(dead_code)]
    history_retention_ms: i64,
    #[allow(dead_code)]
    buffer_store_name: String,
    /// The fully-qualified buffer-store changelog topic name, of the form `app-…-changelog`.
    buffer_changelog_topic: String,
    /// The buffer-store changelog topic config map, from key to value.
    buffer_changelog_configs: std::collections::BTreeMap<String, String>,
    out: Vec<GoldenOut>,
    #[allow(dead_code)]
    describe: String,
}

fn load_grace_golden() -> GraceGolden {
    let path = "tests/testdata/versioned_joins/grace.json";
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read golden {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse golden {path}: {e}"))
}

/// Builds the grace stream-table join topology the JVM capture program builds.
///
/// The topology joins a versioned table "vt" with `history_retention` =
/// `600_000` to a `KStream` with a `60_000` ms grace period. The unoptimized
/// `build("app")` reproduces the JVM's `KSTREAM-JOIN-0000000003` node and store
/// names. The buffer store and its changelog hang off that node.
fn build_grace_app() -> crabka_client_streams::topology::BuiltTopology {
    use crabka_client_streams::Joined;
    let b = StreamsBuilder::new();
    let table = b.table_explicit::<StringSerde, I64Serde>(
        "table",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
    );
    b.stream_explicit::<StringSerde, I64Serde>(["stream"], Consumed::with(StringSerde, I64Serde))
        .join_table_with(
            &table,
            |s: &i64, t: &i64| s + t,
            Joined::with_grace_period(millis(60_000)),
        )
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    drop(table);
    b.build("app").unwrap()
}

/// Grace stream-table join behavioral replay (KIP-923).
///
/// Grace is `60_000` ms, and records carry small timestamps from 100 to 300. A
/// buffered stream record drains only once `stream_time - grace >= bufTs`, that
/// is, once stream-time reaches `bufTs + 60_000`. The JVM advances stream-time
/// with a final flush record `(a,9)@1_000_000`. That record drains the three
/// buffered records `@150/@250/@300` in ascending-timestamp order. The flush
/// record itself stays buffered, because stream-time never reaches its own grace
/// horizon `1_060_000`.
///
/// The out-of-order pipe order `(@300,@250,@150)` and the as-of-correct values
/// together prove both the reorder-on-drain and as-of semantics. The `@150`
/// record reads table 10 and emits 11. The `@250` and `@300` records read table
/// 20 and emit 21. `read_output` exposes key and value, so the value sequence
/// already encodes the timestamp correctness.
#[test]
fn grace_stream_table_join_matches_golden() {
    let golden = load_grace_golden();

    let built = build_grace_app();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // 1. TABLE versions: (a,10)@100 then (a,20)@200.
    for (k, v, ts) in [("a", 10_i64, 100_i64), ("a", 20, 200)] {
        d.pipe_input(
            "table",
            Consumed::with(StringSerde, I64Serde),
            Some(k.to_string()),
            v,
            ts,
        );
    }
    // 2. STREAM records out-of-order: @300, @250, @150 — all buffer, no output.
    for (k, v, ts) in [("a", 1_i64, 300_i64), ("a", 1, 250), ("a", 1, 150)] {
        d.pipe_input(
            "stream",
            Consumed::with(StringSerde, I64Serde),
            Some(k.to_string()),
            v,
            ts,
        );
    }
    // No output yet: every buffered record's grace horizon (ts + 60_000) is far
    // beyond the current stream-time (300).
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        None,
        "buffered records must not emit before the grace horizon is crossed"
    );
    // 3. FLUSH record @1_000_000 advances stream-time → threshold = 940_000,
    //    draining @150/@250/@300 (ascending ts). The flush record itself stays
    //    buffered and must NOT emit.
    d.pipe_input(
        "stream",
        Consumed::with(StringSerde, I64Serde),
        Some("a".to_string()),
        9_i64,
        1_000_000,
    );

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

    // Golden sanity: the three drained records in ascending-ts order.
    assert_eq!(
        expected,
        vec![
            (Some("a".to_string()), 11),
            (Some("a".to_string()), 21),
            (Some("a".to_string()), 21),
        ],
        "golden sanity: grace drain expected outputs"
    );
    assert_eq!(
        got, expected,
        "grace join output must match JVM golden: drain in ascending ts with as-of \
         table reads, and the flush record (@1_000_000) must NOT emit a 4th record"
    );
}

/// Grace buffer-store changelog wire assertion (KIP-923).
///
/// The grace buffer store hangs a changelog off the join node. This test pins its
/// fully-qualified name and config against `grace.json`. The name is
/// `app-KSTREAM-JOIN-0000000003-Buffer-changelog`. The config is a compacted KV
/// changelog with `cleanup.policy=compact` and
/// `message.timestamp.type=CreateTime`, and with NO `retention.ms`, because the
/// buffer is a plain KV store and not a window store.
#[test]
fn grace_buffer_changelog_matches_golden_wire() {
    let golden = load_grace_golden();

    let wire = build_grace_app().to_wire();

    // Locate the buffer changelog topic (the only changelog whose name carries
    // "Buffer") across all subtopologies.
    let buffer: Vec<_> = wire
        .subtopologies
        .iter()
        .flat_map(|st| st.state_changelog_topics.iter())
        .filter(|t| t.name.contains("Buffer"))
        .collect();
    assert_eq!(
        buffer.len(),
        1,
        "expected exactly one buffer changelog; got {:?}",
        buffer.iter().map(|t| &t.name).collect::<Vec<_>>()
    );
    let buffer = buffer[0];

    // Name must match grace.json `buffer_changelog_topic`.
    assert_eq!(
        buffer.name, golden.buffer_changelog_topic,
        "buffer changelog name must match JVM golden"
    );

    // Config must match grace.json `buffer_changelog_configs` exactly (and carry
    // no `retention.ms` — a KV, not window, changelog).
    let got_configs: std::collections::BTreeMap<String, String> = buffer
        .topic_configs
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();
    assert_eq!(
        got_configs, golden.buffer_changelog_configs,
        "buffer changelog configs must match JVM golden"
    );
    assert!(
        !got_configs.contains_key("retention.ms"),
        "buffer changelog (a KV store) must not carry retention.ms; got {got_configs:?}"
    );
}

/// Table-table versioned inner-join out-of-order suppression (KIP-914 / KIP-889).
///
/// The test inner-joins two versioned tables `va` and `vb`, each with
/// `history_retention` = `600_000` ms, with `|va, vb| "{va}|{vb}"`, then calls
/// `.to_stream().to("out")`. Versioned stores SUPPRESS an update whose record
/// timestamp predates the latest version already stored for that key. The stale
/// update never becomes the "current" value, so it yields NO new join result.
///
/// This test replicates the JVM drive sequence from
/// `TableTableVersionedBehavior.java` exactly:
///   1. `a:(k,"1")@100` — a's current = "1"; b absent → inner join, NO output.
///   2. `b:(k,"2")@100` — in-order, both current → emit `(k,"1|2")@100`.
///   3. `a:(k,"3")@200` — in-order update of a → emit `(k,"3|2")@200`.
///   4. `a:(k,"9")@150` — OUT-OF-ORDER (150 < latest validFrom 200) → suppressed,
///      NO new join result.
///
/// Expected `out` = `[(k,"1|2"), (k,"3|2")]`, because the @150 record emits
/// nothing.
///
/// NOTE on the `describe()` divergence: it is intentional and is not a bug, so do
/// NOT assert the JVM store-connection list here. Crabka detects out-of-order
/// records because the versioned join processor reads its OWN store's latest
/// `valid_from`. Its `describe()` therefore lists the join connected to its own
/// store in addition to the other side's. The JVM instead carries an internal
/// `Change.isLatest` flag and connects the join only to the OTHER store. The two
/// implementations have IDENTICAL observable behavior and NO wire impact, and add
/// no extra changelog, because the versioned stores' changelogs already exist
/// from the table sources. We assert only the observable `out` sequence, plus the
/// assertion below that no extra changelog appeared.
///
/// Both join processors use the versioned-store API for the other side's latest
/// value and use their own versioned store to suppress stale updates. This test
/// keeps that two-sided behavior pinned to the JVM result.
#[test]
fn table_table_versioned_join_matches_golden() {
    let golden = load_table_table_golden();

    let b = StreamsBuilder::new();
    let a = b.table_explicit::<StringSerde, StringSerde>(
        "a",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_versioned("va", millis(600_000)),
    );
    let b_table = b.table_explicit::<StringSerde, StringSerde>(
        "b",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_versioned("vb", millis(600_000)),
    );
    a.join(&b_table, |va: &String, vb: &String| format!("{va}|{vb}"))
        .to_stream()
        .to("out");
    drop(a);
    drop(b_table);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Exact JVM drive sequence (topic, key, value, ts).
    let drive: [(&str, &str, &str, i64); 4] = [
        ("a", "k", "1", 100), // a current = "1"; b absent → no output
        ("b", "k", "2", 100), // in-order → emit "1|2"
        ("a", "k", "3", 200), // in-order update → emit "3|2"
        ("a", "k", "9", 150), // out-of-order (150 < 200) → suppressed, no output
    ];
    for (topic, k, v, ts) in drive {
        d.pipe_input(
            topic,
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            v.to_string(),
            ts,
        );
    }

    // Collect every output record (key + value) in order.
    let mut got: Vec<(Option<String>, String)> = Vec::new();
    while let Some(rec) = d.read_output("out", Produced::with(StringSerde, StringSerde)) {
        got.push(rec);
    }

    let expected: Vec<(Option<String>, String)> = golden
        .out
        .iter()
        .map(|o| (Some(o.key.clone()), o.value.clone()))
        .collect();

    // Golden sanity: the JVM output is exactly the two in-order joins.
    assert_eq!(
        expected,
        vec![
            (Some("k".to_string()), "1|2".to_string()),
            (Some("k".to_string()), "3|2".to_string()),
        ],
        "golden sanity: table-table versioned expected outputs"
    );
    assert_eq!(
        got, expected,
        "table-table versioned join output must match JVM golden: the out-of-order \
         record (a:@150) must produce NO additional output (no 3rd record)"
    );
    assert_eq!(
        got.len(),
        2,
        "exactly two outputs; the out-of-order @150 record must be suppressed"
    );
}

/// Table-table versioned wire assertion for the out-of-order gate.
///
/// The gate must NOT introduce any extra changelog topic. The only state
/// changelogs are the two versioned table-source stores `va` and `vb`. There is
/// no `-Buffer` changelog and no join-specific changelog, because the gate reuses
/// the existing store. See the `describe()` note on
/// `table_table_versioned_join_matches_golden`.
#[test]
fn table_table_versioned_no_extra_changelog_wire() {
    let b = StreamsBuilder::new();
    let a = b.table_explicit::<StringSerde, StringSerde>(
        "a",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_versioned("va", millis(600_000)),
    );
    let b_table = b.table_explicit::<StringSerde, StringSerde>(
        "b",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_versioned("vb", millis(600_000)),
    );
    a.join(&b_table, |va: &String, vb: &String| format!("{va}|{vb}"))
        .to_stream()
        .to("out");
    drop(a);
    drop(b_table);
    let wire = b.build("app").unwrap().to_wire();

    let changelogs: Vec<&str> = wire
        .subtopologies
        .iter()
        .flat_map(|st| st.state_changelog_topics.iter())
        .map(|t| t.name.as_str())
        .collect();

    // Exactly the two versioned table-source changelogs (va/vb), nothing else.
    assert_eq!(
        changelogs.len(),
        2,
        "expected exactly the two table-source changelogs; got {changelogs:?}"
    );
    check!(
        changelogs.iter().any(|n| n.contains("va")),
        "expected a 'va' changelog; got {changelogs:?}"
    );
    check!(
        changelogs.iter().any(|n| n.contains("vb")),
        "expected a 'vb' changelog; got {changelogs:?}"
    );
    check!(
        !changelogs.iter().any(|n| n.contains("Buffer")),
        "the out-of-order gate must not introduce a Buffer changelog; got {changelogs:?}"
    );
}

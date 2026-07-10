//! `IQv2` (KIP-796/806) behavioral parity, replayed through `TopologyTestDriver`.
//!
//! Ground truth is `tests/testdata/iqv2/behavior.json`, whose values match Docker
//! Streams 4.1 for the same inputs. Each test rebuilds the equivalent Rust DSL
//! topology, replays the same records the JVM fed, then reads the materialized
//! store back through the driver's `IQv2` `query` surface (`KeyQuery` /
//! `RangeQuery` / `WindowKeyQuery` / `WindowRangeQuery`) and asserts parity.

use crabka_client_streams::{
    Consumed, FailureReason, KeyQuery, RangeQuery, StateQueryRequest, StreamsBuilder, StringSerde,
    TimeWindows, TopologyTestDriver, WindowKeyQuery, WindowRangeQuery,
};
use serde_json::Value;

/// The committed golden, parsed once per test.
fn golden() -> Value {
    let raw = include_str!("testdata/iqv2/behavior.json");
    serde_json::from_str(raw).expect("parse iqv2 behavior.json golden")
}

/// `[value, valid_from, valid_to]` JSON (or `null`) → `Option<VersionedRecord>`.
fn ver(triple: &Value) -> Option<crabka_client_streams::VersionedRecord<i64>> {
    if triple.is_null() {
        return None;
    }
    Some(crabka_client_streams::VersionedRecord {
        value: triple[0].as_i64().unwrap(),
        valid_from: triple[1].as_i64().unwrap(),
        valid_to: triple[2].as_i64(), // null → None
    })
}

/// `[[value, valid_from, valid_to], ...]` JSON → `Vec<VersionedRecord>`.
fn vers(arr: &Value) -> Vec<crabka_client_streams::VersionedRecord<i64>> {
    arr.as_array()
        .unwrap()
        .iter()
        .map(|t| ver(t).unwrap())
        .collect()
}

/// `[[key, value], ...]` JSON → `Vec<(String, i64)>`.
fn pairs(v: &Value) -> Vec<(String, i64)> {
    v.as_array()
        .expect("pairs array")
        .iter()
        .map(|p| {
            (
                p[0].as_str().expect("pair key").to_string(),
                p[1].as_i64().expect("pair value"),
            )
        })
        .collect()
}

/// `KeyQuery` + `RangeQuery` over a keyed count store.
#[tokio::test]
async fn iqv2_kv_key_and_range_parity() {
    let g = golden();
    let kv = &g["kv"];

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count("counts");
    let built = b.build("app").unwrap();
    let mut d = TopologyTestDriver::new(&built).unwrap();
    for v in kv["records"].as_array().unwrap() {
        let v = v.as_str().unwrap().to_string();
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(v.clone()),
            v,
            0,
        );
    }

    // KeyQuery("a") → Some(2).
    let got_a = d
        .query(
            StateQueryRequest::in_store("counts")
                .with_query(KeyQuery::<String, i64>::with_key("a".into())),
        )
        .await;
    assert_eq!(
        got_a.only_partition_result().unwrap().result(),
        Some(&Some(kv["key_a"].as_i64().unwrap())),
    );

    // KeyQuery("z") → Some(None) (present query, absent key).
    let got_z = d
        .query(
            StateQueryRequest::in_store("counts")
                .with_query(KeyQuery::<String, i64>::with_key("z".into())),
        )
        .await;
    assert!(kv["key_z"].is_null());
    assert_eq!(got_z.only_partition_result().unwrap().result(), Some(&None),);

    // RangeQuery [a, b] ascending.
    let r = d
        .query(StateQueryRequest::in_store("counts").with_query(
            RangeQuery::<String, i64>::with_range("a".into(), "b".into()),
        ))
        .await;
    assert_eq!(
        r.only_partition_result().unwrap().result(),
        Some(&pairs(&kv["range_a_b_asc"])),
        "range [a,b] ascending parity",
    );

    // RangeQuery all keys, descending.
    let rd = d
        .query(
            StateQueryRequest::in_store("counts")
                .with_query(RangeQuery::<String, i64>::with_no_bounds().with_descending_keys()),
        )
        .await;
    assert_eq!(
        rd.only_partition_result().unwrap().result(),
        Some(&pairs(&kv["range_all_desc"])),
        "range all descending parity",
    );

    // RangeQuery lower-bound b.
    let rl = d
        .query(
            StateQueryRequest::in_store("counts")
                .with_query(RangeQuery::<String, i64>::with_lower_bound("b".into())),
        )
        .await;
    assert_eq!(
        rl.only_partition_result().unwrap().result(),
        Some(&pairs(&kv["range_lower_b"])),
        "range lower-bound b parity",
    );
}

/// Driver failure paths: an unknown store name and a wrong-kind query against
/// an existing `KeyValue` store both surface as per-partition `Failure`s.
#[tokio::test]
async fn iqv2_failure_paths() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count("counts");
    let built = b.build("app").unwrap();
    let mut d = TopologyTestDriver::new(&built).unwrap();
    // Seed one record so the store's partition has a live task.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "a".to_string(),
        0,
    );

    // Unknown store name → DoesNotExist.
    let bogus = d
        .query(
            StateQueryRequest::in_store("nope")
                .with_query(KeyQuery::<String, i64>::with_key("a".into())),
        )
        .await;
    let r = bogus
        .only_partition_result()
        .expect("single partition result");
    assert_eq!(r.is_success(), false);
    assert_eq!(r.failure_reason(), Some(FailureReason::DoesNotExist));

    // Existing KeyValue store queried with a Window query (wrong kind) → NotPresent.
    let wrong_kind = d
        .query(
            StateQueryRequest::in_store("counts")
                .with_query(WindowKeyQuery::<String, i64>::with_key("a".into())),
        )
        .await;
    let r = wrong_kind
        .only_partition_result()
        .expect("single partition result");
    assert_eq!(r.is_success(), false);
    assert_eq!(r.failure_reason(), Some(FailureReason::NotPresent));
}

/// `WindowKeyQuery` + `WindowRangeQuery` over a 1000ms tumbling count store.
#[tokio::test]
async fn iqv2_window_key_and_range_parity() {
    let g = golden();
    let w = &g["window"];
    let size = w["size_ms"].as_i64().unwrap();

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(size))
        .count("wcounts");
    let built = b.build("app").unwrap();
    let mut d = TopologyTestDriver::new(&built).unwrap();
    for rec in w["records"].as_array().unwrap() {
        let key = rec[0].as_str().unwrap().to_string();
        let ts = rec[1].as_i64().unwrap();
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(key.clone()),
            key,
            ts,
        );
    }

    // WindowKeyQuery: key "a", window starts in [0, 2000] → ascending (start, count).
    let wk = d
        .query(
            StateQueryRequest::in_store("wcounts").with_query(
                WindowKeyQuery::<String, i64>::with_key("a".into())
                    .from_time(0)
                    .to_time(2000),
            ),
        )
        .await;
    let want_by_key: Vec<(i64, i64)> = w["wkey_a_0_2000"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p[0].as_i64().unwrap(), p[1].as_i64().unwrap()))
        .collect();
    assert_eq!(
        wk.only_partition_result().unwrap().result(),
        Some(&want_by_key),
        "window-key fetch parity",
    );

    // WindowRangeQuery: all keys, window starts in [0, 0].
    let wr = d
        .query(
            StateQueryRequest::in_store("wcounts").with_query(
                WindowRangeQuery::<String, i64>::with_all_keys()
                    .from_time(0)
                    .to_time(0),
            ),
        )
        .await;
    let want_by_range: Vec<((String, i64), i64)> = w["wrange_all_0_0"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                (
                    p[0][0].as_str().unwrap().to_string(),
                    p[0][1].as_i64().unwrap(),
                ),
                p[1].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        wr.only_partition_result().unwrap().result(),
        Some(&want_by_range),
        "window-range fetch parity",
    );
}

/// `VersionedKeyQuery` (KIP-960, latest + as-of) and `MultiVersionedKeyQuery`
/// (KIP-968, all + range × asc/desc) over a versioned `KTable`. Three in-order
/// versions of key `k` (`10@100, 20@200, 30@300`) chain into history; the
/// versioned source uses each record's timestamp as the version `valid_from`.
#[tokio::test]
async fn iqv2_versioned_key_and_multi_parity() {
    use crabka_client_streams::{
        I64Serde, Materialized, MultiVersionedKeyQuery, VersionedKeyQuery,
    };

    let g = golden();
    let v = &g["versioned"];
    let retention = v["retention_ms"].as_i64().unwrap();

    let b = StreamsBuilder::new();
    b.table_explicit::<StringSerde, I64Serde>(
        "vt",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vstore", retention),
    );
    let built = b.build("app").unwrap();
    let mut d = TopologyTestDriver::new(&built).unwrap();
    for rec in v["records"].as_array().unwrap() {
        let key = rec[0].as_str().unwrap().to_string();
        let val = rec[1].as_i64().unwrap();
        let ts = rec[2].as_i64().unwrap();
        d.pipe_input(
            "vt",
            Consumed::with(StringSerde, I64Serde),
            Some(key),
            val,
            ts,
        );
    }

    let key = || VersionedKeyQuery::<String, i64>::with_key("k".into());

    // VersionedKeyQuery: latest, as-of 250 (sees 200-version, superseded at 300),
    // and as-of 50 (before any version → null).
    let latest = d
        .query(StateQueryRequest::in_store("vstore").with_query(key()))
        .await;
    assert_eq!(
        latest.only_partition_result().unwrap().result(),
        Some(&ver(&v["latest"])),
        "versioned latest parity",
    );
    let asof = d
        .query(StateQueryRequest::in_store("vstore").with_query(key().as_of(250)))
        .await;
    assert_eq!(
        asof.only_partition_result().unwrap().result(),
        Some(&ver(&v["as_of_250"])),
        "versioned as-of 250 parity",
    );
    let asof_miss = d
        .query(StateQueryRequest::in_store("vstore").with_query(key().as_of(50)))
        .await;
    assert_eq!(
        asof_miss.only_partition_result().unwrap().result(),
        Some(&ver(&v["as_of_50"])),
        "versioned as-of 50 (pre-history) parity",
    );

    // MultiVersionedKeyQuery: all ascending, then [150,250] descending (the
    // 300-version's [300,∞) doesn't overlap the window).
    let all = d
        .query(
            StateQueryRequest::in_store("vstore")
                .with_query(MultiVersionedKeyQuery::<String, i64>::with_key("k".into())),
        )
        .await;
    assert_eq!(
        all.only_partition_result().unwrap().result(),
        Some(&vers(&v["all_asc"])),
        "versioned multi all-ascending parity",
    );
    let win = d
        .query(
            StateQueryRequest::in_store("vstore").with_query(
                MultiVersionedKeyQuery::<String, i64>::with_key("k".into())
                    .from_time(150)
                    .to_time(250)
                    .with_descending_timestamps(),
            ),
        )
        .await;
    assert_eq!(
        win.only_partition_result().unwrap().result(),
        Some(&vers(&v["range_150_250_desc"])),
        "versioned multi range [150,250] descending parity",
    );
}

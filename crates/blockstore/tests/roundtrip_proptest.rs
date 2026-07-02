//! Property tests for the block store.
//!
//! `write_then_read_preserves_all_rows` (logs, blockstore plan Task 8) generates
//! arbitrary log rows, writes them to a Parquet block, reads them back, and
//! asserts the round-trip preserves every row (as a multiset) and the
//! descriptor's fingerprint set. The deterministic example-based round-trips
//! live in `tests/parquet.rs`; this is the generative complement.
//!
//! `equality_matcher_returns_only_matching_series` writes a block and asserts an
//! equality matcher returns exactly the rows whose fingerprint matches within
//! the time window.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{Int64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use crabka_blockstore::{
    BlockKey, BlockStore, COL_FINGERPRINT, COL_TIMESTAMP, LabelMatcher, Labels, LogRow, MatchOp,
    TimeRange, read_log_block, write_log_block,
};
use object_store::{ObjectStore, memory::InMemory};
use proptest::prelude::*;

/// A total ordering over a row's full contents, so the read-back (sorted by
/// `(fingerprint, timestamp)`) and the input compare as multisets even when two
/// rows share a `(fingerprint, timestamp)` pair.
fn row_sort_key(row: &LogRow) -> (u64, i64, String, BTreeMap<String, String>) {
    (
        row.series_fingerprint,
        row.timestamp_ns,
        row.line.clone(),
        row.structured_metadata.clone(),
    )
}

fn arb_row() -> impl Strategy<Value = (u64, i64, String, BTreeMap<String, String>)> {
    (
        any::<u64>(),
        0_i64..1_000_000_000_000_i64,
        "[a-zA-Z0-9 ,.:|=_/-]{0,40}",
        proptest::collection::btree_map("[a-zA-Z0-9_]{1,12}", "[a-zA-Z0-9_ ]{0,20}", 0..4_usize),
    )
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new("line", DataType::Utf8, true),
    ]))
}

fn arb_rows() -> impl Strategy<Value = Vec<(bool, i64, String)>> {
    proptest::collection::vec((any::<bool>(), 0_i64..1_000_i64, "[a-z]{1,8}"), 1..40)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn write_then_read_preserves_all_rows(raw in proptest::collection::vec(arb_row(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();

        let min_ts = raw.iter().map(|(_, ts, _, _)| *ts).min().unwrap();
        let max_ts = raw.iter().map(|(_, ts, _, _)| *ts).max().unwrap();
        let key = BlockKey::new(
            "tenant-prop",
            0,
            0,
            i64::try_from(raw.len()).unwrap(),
            TimeRange::new(min_ts, max_ts).unwrap(),
        );

        let rows: Vec<LogRow> = raw
            .iter()
            .map(|(fp, ts, line, metadata)| LogRow::new(*fp, *ts, line.clone(), metadata.clone()))
            .collect();

        let descriptor = write_log_block(dir.path(), &key, rows.clone()).unwrap();

        let expected_fingerprints: BTreeSet<u64> =
            rows.iter().map(|row| row.series_fingerprint).collect();
        prop_assert_eq!(&descriptor.fingerprints, &expected_fingerprints);

        let mut got = read_log_block(dir.path(), &key).unwrap();
        let mut want = rows;
        got.sort_by_key(row_sort_key);
        want.sort_by_key(row_sort_key);

        prop_assert_eq!(got, want);
    }

    #[test]
    fn equality_matcher_returns_only_matching_series(rows in arb_rows()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let mut api = Labels::new();
            api.insert("app", "api");
            let api_fp = api.fingerprint();
            let mut web = Labels::new();
            web.insert("app", "web");
            let web_fp = web.fingerprint();

            let mut sorted: Vec<(u64, i64, String)> = rows
                .iter()
                .map(|(is_api, ts, line)| {
                    (if *is_api { api_fp } else { web_fp }, *ts, line.clone())
                })
                .collect();
            sorted.sort_by_key(|(fp, ts, _)| (*fp, *ts));

            let expected_api_count = sorted
                .iter()
                .filter(|(fp, _, _)| *fp == api_fp)
                .count();

            let fps = UInt64Array::from(sorted.iter().map(|(fp, _, _)| *fp).collect::<Vec<_>>());
            let timestamps = Int64Array::from(sorted.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>());
            let lines = StringArray::from(
                sorted.iter().map(|(_, _, line)| line.as_str()).collect::<Vec<_>>(),
            );
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(fps), Arc::new(timestamps), Arc::new(lines)],
            )
            .unwrap();

            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = url::Url::parse("memory:///").unwrap();
            let mut block_store = BlockStore::new(store, base);
            let meta = block_store
                .writer()
                .write_block("t", "b.parquet", schema(), &[batch])
                .await
                .unwrap();
            block_store.index_mut().add_series("t", api_fp, &api);
            block_store.index_mut().add_series("t", web_fp, &web);
            block_store.index_mut().add_block(&meta);

            let (ctx, table) = block_store
                .scan_context(
                    "t",
                    &[LabelMatcher::new("app", MatchOp::Eq, "api")],
                    i64::MIN,
                    i64::MAX,
                    schema(),
                )
                .await
                .unwrap();
            let df = ctx
                .sql(&format!(
                    "SELECT count(*) AS c FROM {table} WHERE series_fingerprint = {api_fp}"
                ))
                .await
                .unwrap();
            let out = df.collect().await.unwrap();
            let count = out[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);

            prop_assert_eq!(usize::try_from(count).unwrap(), expected_api_count);
            Ok(())
        })?;
    }
}

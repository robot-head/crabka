//! Property: rows written as a block and queried by an equality matcher return
//! exactly the rows whose fingerprint matches within the time window.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockStore, COL_FINGERPRINT, COL_TIMESTAMP, LabelMatcher, Labels, MatchOp,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use proptest::prelude::*;

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

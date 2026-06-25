//! Query facade over object storage, index pruning, and `DataFusion` scans.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::ObjectStore;
use url::Url;

use crate::error::{BlockStoreError, Result};
use crate::index::Index;
use crate::matcher::LabelMatcher;
use crate::reader::read_block_row_groups;
use crate::writer::BlockWriter;

const TABLE_NAME: &str = "logs";

/// One named `DataFusion` table registration request over indexed blocks.
pub struct ScanTableRequest<'a> {
    pub table_name: &'a str,
    pub tenant: &'a str,
    pub matchers: &'a [LabelMatcher],
    pub min_ts: i64,
    pub max_ts: i64,
    pub schema: SchemaRef,
}

/// Owns the object store, its `DataFusion` URL prefix, and the in-memory index.
#[derive(Clone)]
pub struct BlockStore {
    store: Arc<dyn ObjectStore>,
    base: Url,
    index: Index,
}

impl BlockStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, base: Url) -> Self {
        Self {
            store,
            base,
            index: Index::new(),
        }
    }

    #[must_use]
    pub fn writer(&self) -> BlockWriter {
        BlockWriter::new(self.store.clone())
    }

    #[must_use]
    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut Index {
        &mut self.index
    }

    #[must_use]
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    #[must_use]
    pub fn empty_like(&self) -> Self {
        Self::new(self.store.clone(), self.base.clone())
    }

    pub async fn scan_context(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        let ctx = SessionContext::new();
        self.register_scan_table(
            &ctx,
            ScanTableRequest {
                table_name: TABLE_NAME,
                tenant,
                matchers,
                min_ts,
                max_ts,
                schema,
            },
        )
        .await?;
        Ok((ctx, TABLE_NAME.to_string()))
    }

    pub async fn register_scan_table(
        &self,
        ctx: &SessionContext,
        request: ScanTableRequest<'_>,
    ) -> Result<bool> {
        let fingerprints = self.index.resolve(request.tenant, request.matchers)?;
        let candidates = self.index.candidate_blocks(
            request.tenant,
            &fingerprints,
            request.min_ts,
            request.max_ts,
        );
        ctx.register_object_store(&self.base, self.store.clone());
        if candidates.is_empty() {
            let table = MemTable::try_new(request.schema, vec![Vec::new()])?;
            ctx.register_table(request.table_name, Arc::new(table))?;
            return Ok(false);
        }

        let paths = candidates
            .iter()
            .map(|object_key| {
                self.base
                    .join(object_key)
                    .map(|url| url.to_string())
                    .map_err(|error| {
                        BlockStoreError::InvalidBlock(format!(
                            "invalid block object key `{object_key}`: {error}"
                        ))
                    })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let options = ParquetReadOptions::default().schema(request.schema.as_ref());
        let dataframe = ctx.read_parquet(paths, options).await?;
        ctx.register_table(request.table_name, dataframe.into_view())?;

        Ok(true)
    }

    pub async fn scan_block_keys(
        &self,
        keys: &[String],
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        let ctx = SessionContext::new();
        ctx.register_object_store(&self.base, self.store.clone());

        if keys.is_empty() {
            let empty = MemTable::try_new(schema, vec![vec![]])?;
            ctx.register_table(TABLE_NAME, Arc::new(empty))?;
            return Ok((ctx, TABLE_NAME.to_string()));
        }

        // Compose each block's location with `Url::join` (the same way
        // `register_scan_table` does) — a raw `format!("{base}{key}")` concat
        // omits the path separator, so a base like `s3://crabka-traces` + key
        // `traces/…` becomes `s3://crabka-tracestraces/…` (the prefix merges
        // into the bucket authority) and DataFusion can't resolve the store.
        let paths = keys
            .iter()
            .map(|key| {
                self.base
                    .join(key.trim_start_matches('/'))
                    .map(|url| url.to_string())
                    .map_err(|error| {
                        BlockStoreError::InvalidBlock(format!(
                            "invalid block object key `{key}`: {error}"
                        ))
                    })
            })
            .collect::<std::result::Result<Vec<String>, _>>()?;
        let df = ctx
            .read_parquet(paths, ParquetReadOptions::default())
            .await?;
        ctx.register_table(TABLE_NAME, df.into_view())?;
        Ok((ctx, TABLE_NAME.to_string()))
    }

    pub async fn scan_block_row_groups(
        &self,
        object_key: &str,
        row_groups: &[usize],
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        let ctx = SessionContext::new();
        ctx.register_object_store(&self.base, self.store.clone());

        if row_groups.is_empty() {
            let empty = MemTable::try_new(schema, vec![vec![]])?;
            ctx.register_table(TABLE_NAME, Arc::new(empty))?;
            return Ok((ctx, TABLE_NAME.to_string()));
        }

        let batches = read_block_row_groups(self.store.clone(), object_key, row_groups).await?;
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions)?;
        ctx.register_table(TABLE_NAME, Arc::new(table))?;
        Ok((ctx, TABLE_NAME.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, LargeStringArray, StringArray, StringViewArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;
    use crate::labels::Labels;
    use crate::matcher::{LabelMatcher, MatchOp};

    fn log_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    async fn seeded_store() -> (BlockStore, SchemaRef) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = BlockStore::new(object_store, base);
        let schema = log_schema();

        let mut api = Labels::new();
        api.insert("app", "api");
        let fp = api.fingerprint();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![fp, fp])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let meta = bs
            .writer()
            .write_block("t", "blocks/b1.parquet", schema.clone(), &[batch])
            .await
            .unwrap();
        bs.index_mut().add_series("t", fp, &api);
        bs.index_mut().add_block(&meta);
        (bs, schema)
    }

    #[tokio::test]
    async fn scan_returns_rows_for_matching_series() {
        let (bs, schema) = seeded_store().await;
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "api")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();

        let df = ctx
            .sql(&format!("SELECT line FROM {table} ORDER BY timestamp"))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 2);

        let first = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(0))
            .or_else(|| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .map(|a| a.value(0))
            })
            .or_else(|| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .map(|a| a.value(0))
            })
            .expect("line column is utf8");
        assert!(first == "hello");
    }

    #[tokio::test]
    async fn index_returns_the_stores_own_populated_index() {
        // The accessor must hand back the store's real index, not a fresh
        // default one: the seeded `app=api` series must resolve.
        let (bs, _schema) = seeded_store().await;
        let mut api = Labels::new();
        api.insert("app", "api");
        let got = bs
            .index()
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert!(got == std::collections::BTreeSet::from([api.fingerprint()]));
    }

    #[tokio::test]
    async fn scan_block_keys_reads_named_blocks() {
        let (bs, schema) = seeded_store().await;
        let (ctx, table) = bs
            .scan_block_keys(&["blocks/b1.parquet".to_string()], schema)
            .await
            .unwrap();
        // Table name is the fixed logical name, not a stub string.
        assert!(table == "logs");
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 2);
    }

    #[tokio::test]
    async fn scan_block_row_groups_reads_selected_groups() {
        let (bs, schema) = seeded_store().await;
        let (ctx, table) = bs
            .scan_block_row_groups("blocks/b1.parquet", &[0], schema)
            .await
            .unwrap();
        assert!(table == "logs");
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 2);
    }

    #[tokio::test]
    async fn scan_with_no_matching_blocks_returns_empty_shape() {
        let (bs, schema) = seeded_store().await;
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "absent")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 0);
    }
}

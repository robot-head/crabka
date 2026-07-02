//! Search-space sharding: turn the candidate block set + the hot/cold frontier
//! into a list of bounded jobs (time -> shard -> block -> row-group), and the
//! by-id candidate enumeration.
//!
//! The shard grain matches what the querier (`querier/http`) honors: a search
//! job restricts to one block + a row-group range via `block` /
//! `rowGroupStart` / `rowGroupEnd` (the querier's [`crabka_traceql::ScanJob`]);
//! the live hot tier is the unrestricted scan. A block larger than
//! `target_bytes_per_job` fans into multiple row-group-range jobs.

use std::collections::BTreeMap;

use async_trait::async_trait;
use crabka_blockstore::{
    BlockStore, Result as BlockStoreResult, TraceIndex, read_row_group_metadata,
};

/// One candidate row-group of a backend block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowGroupInfo {
    pub index: u32,
    pub compressed_bytes: u64,
}

/// Block metadata the planner needs (from the querier's block catalog).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMetaInfo {
    pub block_id: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub size_bytes: u64,
    pub row_groups: Vec<RowGroupInfo>,
}

impl BlockMetaInfo {
    /// Total compressed bytes across this block's row-groups (falls back to
    /// `size_bytes` when row-group sizes are unavailable).
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        let rg_total: u64 = self.row_groups.iter().map(|rg| rg.compressed_bytes).sum();
        if rg_total == 0 {
            self.size_bytes
        } else {
            rg_total
        }
    }
}

/// The shard a single search job scans: the live hot tier, or one cold block
/// narrowed to a half-open row-group range `[start, end)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobShard {
    Live,
    Block {
        block_id: String,
        row_group_start: u32,
        row_group_end: u32,
    },
}

/// The output of planning: the jobs to dispatch + how many blocks they cover
/// (seeds `metrics.totalBlocks`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobPlan {
    pub jobs: Vec<JobShard>,
    pub total_blocks: u64,
}

/// Errors enumerating blocks.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("block catalog error: {0}")]
    Backend(String),
}

/// The block-catalog door: which blocks overlap `[start_ns, end_ns]` for a
/// tenant. Tests use [`MockCatalog`]; production uses [`TraceIndexCatalog`].
#[async_trait]
pub trait BlockCatalog: Send + Sync {
    async fn blocks(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError>;
}

/// A canned block catalog for tests.
pub struct MockCatalog {
    blocks: Vec<BlockMetaInfo>,
}

impl MockCatalog {
    #[must_use]
    pub fn new(blocks: Vec<BlockMetaInfo>) -> Self {
        Self { blocks }
    }
}

#[async_trait]
impl BlockCatalog for MockCatalog {
    async fn blocks(
        &self,
        _tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        Ok(self
            .blocks
            .iter()
            .filter(|b| b.end_ns >= start_ns && b.start_ns <= end_ns)
            .cloned()
            .collect())
    }
}

/// The production block catalog: backed by a pre-resolved
/// [`crabka_blockstore::TraceIndex`] (per tenant). Built once at startup from
/// the index; ports `backend_blocks_from_trace_index` from the legacy
/// query-frontend.
pub struct TraceIndexCatalog {
    by_tenant: BTreeMap<String, Vec<BlockMetaInfo>>,
}

impl TraceIndexCatalog {
    #[must_use]
    pub fn new(by_tenant: BTreeMap<String, Vec<BlockMetaInfo>>) -> Self {
        Self { by_tenant }
    }

    /// Build the catalog from a `BlockStore` + `TraceIndex`, reading each
    /// block's parquet row-group metadata (the per-tenant block list).
    ///
    /// # Errors
    /// Propagates object-store / parquet read errors.
    pub async fn from_trace_index(
        blocks: &BlockStore,
        index: &TraceIndex,
    ) -> BlockStoreResult<Self> {
        let mut by_tenant = BTreeMap::new();
        for tenant in index.tenants() {
            let metas = blocks_for_tenant(blocks, index, &tenant).await?;
            by_tenant.insert(tenant, metas);
        }
        Ok(Self::new(by_tenant))
    }
}

/// Read the block metadata for one tenant out of a `TraceIndex` (+ parquet
/// row-group metadata). Ported from the legacy `backend_blocks_from_trace_index`.
///
/// # Errors
/// Propagates object-store / parquet read errors.
pub async fn blocks_for_tenant(
    blocks: &BlockStore,
    index: &TraceIndex,
    tenant: &str,
) -> BlockStoreResult<Vec<BlockMetaInfo>> {
    let mut out = Vec::new();
    for block in index.trace_blocks(tenant) {
        let row_groups: Vec<RowGroupInfo> =
            read_row_group_metadata(blocks.object_store(), &block.object_key)
                .await?
                .into_iter()
                .filter_map(|rg| {
                    let index = u32::try_from(rg.index).ok()?;
                    Some(RowGroupInfo {
                        index,
                        compressed_bytes: rg.compressed_bytes,
                    })
                })
                .collect();
        let size_bytes = row_groups.iter().map(|rg| rg.compressed_bytes).sum();
        out.push(BlockMetaInfo {
            block_id: block.object_key.clone(),
            start_ns: block.min_ts,
            end_ns: block.max_ts,
            size_bytes,
            row_groups,
        });
    }
    Ok(out)
}

#[async_trait]
impl BlockCatalog for TraceIndexCatalog {
    async fn blocks(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        Ok(self
            .by_tenant
            .get(tenant)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|b| b.end_ns >= start_ns && b.start_ns <= end_ns)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
}

/// Fan one block into row-group-range jobs sized ~`target_bytes_per_job`.
fn plan_block_jobs(block: &BlockMetaInfo, target_bytes_per_job: u64) -> Vec<JobShard> {
    // A whole-block job when sizing is disabled, the block has <=1 row-group, or
    // it fits under the budget.
    if target_bytes_per_job == 0
        || block.row_groups.len() <= 1
        || block.total_bytes() <= target_bytes_per_job
    {
        let end = block
            .row_groups
            .last()
            .map_or(1, |rg| rg.index.saturating_add(1));
        let start = block.row_groups.first().map_or(0, |rg| rg.index);
        return vec![JobShard::Block {
            block_id: block.block_id.clone(),
            row_group_start: start,
            row_group_end: end,
        }];
    }

    let mut jobs = Vec::new();
    let mut range_start: Option<u32> = None;
    let mut range_end = 0u32;
    let mut bytes = 0u64;
    for rg in &block.row_groups {
        range_start.get_or_insert(rg.index);
        range_end = rg.index.saturating_add(1);
        bytes = bytes.saturating_add(rg.compressed_bytes);
        if bytes >= target_bytes_per_job {
            jobs.push(JobShard::Block {
                block_id: block.block_id.clone(),
                row_group_start: range_start.take().unwrap_or(rg.index),
                row_group_end: range_end,
            });
            bytes = 0;
        }
    }
    if let Some(start) = range_start {
        jobs.push(JobShard::Block {
            block_id: block.block_id.clone(),
            row_group_start: start,
            row_group_end: range_end,
        });
    }
    jobs
}

/// Plan search jobs for a query window ending at `query_end_ns` over the
/// candidate blocks + the hot/cold frontier.
///
/// - One `Live` job iff the query window reaches the hot tier
///   (`query_end_ns >= hot_frontier_ns`).
/// - For each block: one whole-block job if it fits the budget, else one
///   row-group-range job per ~`target_bytes_per_job` chunk.
#[must_use]
pub fn plan_search_jobs(
    blocks: &[BlockMetaInfo],
    query_end_ns: i64,
    hot_frontier_ns: i64,
    target_bytes_per_job: u64,
) -> JobPlan {
    let mut jobs = Vec::new();
    if query_end_ns >= hot_frontier_ns {
        jobs.push(JobShard::Live);
    }
    for b in blocks {
        jobs.extend(plan_block_jobs(b, target_bytes_per_job));
    }
    JobPlan {
        jobs,
        total_blocks: blocks.len() as u64,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn block(id: &str, start: i64, end: i64, rgs: &[u64]) -> BlockMetaInfo {
        let row_groups = rgs
            .iter()
            .enumerate()
            .map(|(i, &b)| RowGroupInfo {
                index: u32::try_from(i).unwrap(),
                compressed_bytes: b,
            })
            .collect();
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ns: start,
            end_ns: end,
            size_bytes: rgs.iter().sum(),
            row_groups,
        }
    }

    #[test]
    fn small_block_is_one_job_plus_live() {
        // Query window ends at 300, frontier 200 => window reaches hot.
        let blocks = vec![block("b1", 0, 100, &[500])];
        let plan = plan_search_jobs(&blocks, 300, 200, 10_000);
        assert_eq!(
            plan,
            JobPlan {
                jobs: vec![
                    JobShard::Live,
                    JobShard::Block {
                        block_id: "b1".to_string(),
                        row_group_start: 0,
                        row_group_end: 1,
                    },
                ],
                total_blocks: 1,
            }
        );
    }

    #[test]
    fn large_block_splits_into_row_group_jobs() {
        // size 30k > budget 10k, 3 row-groups => 3 row-group-range jobs, no Live
        // (query window ends at -10, before the frontier 0).
        let blocks = vec![block("b2", -1000, -10, &[10_000, 10_000, 10_000])];
        let plan = plan_search_jobs(&blocks, -10, 0, 10_000);
        // Each job is a single-row-group range; no Live job.
        assert_eq!(
            plan,
            JobPlan {
                jobs: vec![
                    JobShard::Block {
                        block_id: "b2".to_string(),
                        row_group_start: 0,
                        row_group_end: 1,
                    },
                    JobShard::Block {
                        block_id: "b2".to_string(),
                        row_group_start: 1,
                        row_group_end: 2,
                    },
                    JobShard::Block {
                        block_id: "b2".to_string(),
                        row_group_start: 2,
                        row_group_end: 3,
                    },
                ],
                total_blocks: 1,
            }
        );
    }

    #[test]
    fn empty_blocks_with_hot_window_is_just_live() {
        let blocks: Vec<BlockMetaInfo> = vec![];
        let plan = plan_search_jobs(&blocks, i64::MAX, 0, 10_000);
        assert!(plan.jobs.len() == 1);
        assert!(matches!(plan.jobs[0], JobShard::Live));
        assert!(plan.total_blocks == 0);
    }

    #[test]
    fn target_bytes_zero_never_splits() {
        let blocks = vec![block("b", 0, 10, &[10_000, 10_000, 10_000])];
        let plan = plan_search_jobs(&blocks, i64::MAX, 0, 0);
        let rg_jobs: Vec<_> = plan
            .jobs
            .iter()
            .filter(|j| matches!(j, JobShard::Block { .. }))
            .collect();
        assert!(rg_jobs.len() == 1);
        assert!(matches!(
            rg_jobs[0],
            JobShard::Block {
                row_group_start: 0,
                row_group_end: 3,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn mock_catalog_returns_overlapping_blocks() {
        let cat = MockCatalog::new(vec![
            block("b1", 0, 100, &[500]),
            block("b2", 500, 600, &[500]),
        ]);
        let got = cat.blocks("t1", 0, 200).await.unwrap();
        assert!(got.len() == 1);
        assert!(got[0].block_id == "b1");
    }
}

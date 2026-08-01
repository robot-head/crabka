//! Query-frontend range splitting, sharding, and merge helpers.

pub use crabka_blockstore::QUERY_SHARD_LABEL;
use crabka_blockstore::{LabelMatcher, MatchOp};
use crabka_units::prelude::*;
use promql_parser::parser::LabelModifier;

mod cache;
mod execution;
mod merge;
mod plan;

#[cfg(test)]
use cache::Clock;
pub use cache::{ObjectStoreQueryFrontendCache, QueryFrontendCache, RangeQueryCache};
#[cfg(test)]
use execution::execute_planned_range_queries;
pub use execution::{RangeQueryExecutor, execute_range_query_frontend};
pub use merge::merge_range_query_results;
#[cfg(test)]
use merge::merge_range_query_results_with_reducer;
pub use plan::plan_range_query;
#[cfg(test)]
use plan::query_with_shard_selector;

#[cfg(test)]
use crate::PromqlError;

/// Query-frontend range splitting and sharding options.
///
/// Not `Eq`: `split_interval` is a [`Time`], which stores `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueryFrontendOptions {
    /// Width of the absolute window each sub-range is split on.
    pub split_interval: Time,
    pub shard_count: usize,
}

/// One user range query entering the query-frontend.
#[derive(Clone, Debug, PartialEq)]
pub struct FrontendRangeRequest {
    pub tenant: String,
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step: Time,
    pub opts: QueryFrontendOptions,
}

/// One Mimir-compatible query shard. Shards are one-based on the wire:
/// `1_of_3`, `2_of_3`, ...
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryShard {
    pub index: usize,
    pub total: usize,
}

impl QueryShard {
    #[must_use]
    pub fn selector_value(self) -> String {
        format!("{}_of_{}", self.index, self.total)
    }

    #[must_use]
    pub fn matcher(self) -> LabelMatcher {
        LabelMatcher {
            name: QUERY_SHARD_LABEL.to_string(),
            op: MatchOp::Eq,
            value: self.selector_value(),
        }
    }
}

/// One subquery the query-frontend can fan out to a querier.
///
/// Not `Eq`: `step` is a [`Time`], which stores `f64`.
#[derive(Clone, Debug, PartialEq)]
pub struct FrontendRangeQuery {
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step: Time,
    pub shard: Option<QueryShard>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryShardReducer {
    First,
    Sum,
    Min,
    Max,
}

enum QueryShardExecution {
    Merge(QueryShardReducer),
    Avg {
        sum_query: String,
        count_query: String,
    },
    Moments {
        sum_query: String,
        count_query: String,
        sum_squares_query: String,
        kind: MomentReduction,
    },
    Rank {
        k: usize,
        kind: RankReduction,
        modifier: Option<LabelModifier>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MomentReduction {
    Stddev,
    Stdvar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankReduction {
    Bottom,
    Top,
}

impl FrontendRangeQuery {
    #[must_use]
    pub fn shard_matcher(&self) -> Option<LabelMatcher> {
        self.shard.map(QueryShard::matcher)
    }
}

#[cfg(test)]
mod tests;

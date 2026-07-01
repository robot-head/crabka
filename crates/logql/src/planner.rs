use std::collections::BTreeSet;

use tracing::field::Empty;

use crabka_blockstore::{
    BlockDescriptor, LabelIndex, LabelPredicate, LogBlockIndex as BlockIndex,
    LogBlockStoreError as BlockStoreError, LogMatchOp as BlockMatchOp,
    LogSeriesFingerprint as SeriesFingerprint, TimeRange,
};
use thiserror::Error;

use crate::{LabelMatcher, MatchOp, StreamQuery};

#[derive(Clone, Debug, PartialEq)]
pub struct StreamPlan {
    pub tenant: String,
    pub time_range: TimeRange,
    pub query: StreamQuery,
    pub fingerprints: BTreeSet<SeriesFingerprint>,
    pub blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
}

#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        tenant = Empty,
        matchers = query.matchers.len(),
        pipeline_stages = query.pipeline.len(),
        fingerprints = Empty,
        blocks = Empty,
    ),
    err
)]
pub fn plan_stream_query(
    tenant: impl Into<String>,
    time_range: TimeRange,
    query: StreamQuery,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<StreamPlan, PlanError> {
    let tenant = tenant.into();
    tracing::Span::current().record("tenant", tenant.as_str());
    let predicates = query
        .matchers
        .iter()
        .map(label_predicate)
        .collect::<Result<Vec<_>, _>>()?;
    let fingerprints = label_index.match_series(&tenant, &predicates);
    let fingerprint_list = fingerprints.iter().copied().collect::<Vec<_>>();
    let blocks = if fingerprint_list.is_empty() {
        Vec::new()
    } else {
        block_index.match_blocks(&tenant, time_range, &fingerprint_list)
    };
    let span = tracing::Span::current();
    span.record("fingerprints", fingerprint_list.len());
    span.record("blocks", blocks.len());

    Ok(StreamPlan {
        tenant,
        time_range,
        query,
        fingerprints,
        blocks,
    })
}

fn label_predicate(matcher: &LabelMatcher) -> Result<LabelPredicate, BlockStoreError> {
    LabelPredicate::new(
        matcher.name.clone(),
        match matcher.op {
            MatchOp::Equal => BlockMatchOp::Equal,
            MatchOp::NotEqual => BlockMatchOp::NotEqual,
            MatchOp::RegexEqual => BlockMatchOp::RegexEqual,
            MatchOp::RegexNotEqual => BlockMatchOp::RegexNotEqual,
        },
        matcher.value.clone(),
    )
}

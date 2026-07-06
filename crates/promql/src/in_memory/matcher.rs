use crabka_blockstore::{
    LabelMatcher, Labels, MatchOp, QUERY_SHARD_LABEL, QueryShardSelector, SeriesFingerprint,
    parse_query_shard_selector,
};

use crate::{PromqlError, error::Result};

pub(super) enum PreparedMatcher {
    LabelEq { name: String, value: String },
    LabelNeq { name: String, value: String },
    LabelRe { name: String, regex: regex::Regex },
    LabelNre { name: String, regex: regex::Regex },
    QueryShardEq(QueryShardSelector),
    QueryShardNeq(QueryShardSelector),
}

impl PreparedMatcher {
    fn new(matcher: &LabelMatcher) -> Result<Self> {
        if matcher.name == QUERY_SHARD_LABEL {
            let selector = parse_query_shard_selector(&matcher.value).map_err(|error| {
                PromqlError::Plan(format!("invalid query shard matcher: {error}"))
            })?;
            return match matcher.op {
                MatchOp::Eq => Ok(Self::QueryShardEq(selector)),
                MatchOp::Neq => Ok(Self::QueryShardNeq(selector)),
                MatchOp::Re | MatchOp::Nre => Err(PromqlError::Plan(
                    "query shard matcher must use equality or inequality".into(),
                )),
            };
        }

        match matcher.op {
            MatchOp::Eq => Ok(Self::LabelEq {
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            }),
            MatchOp::Neq => Ok(Self::LabelNeq {
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            }),
            MatchOp::Re => Ok(Self::LabelRe {
                name: matcher.name.clone(),
                regex: regex_anchored(&matcher.value)?,
            }),
            MatchOp::Nre => Ok(Self::LabelNre {
                name: matcher.name.clone(),
                regex: regex_anchored(&matcher.value)?,
            }),
        }
    }

    fn matches(&self, fp: SeriesFingerprint, labels: &Labels) -> bool {
        match self {
            Self::LabelEq { name, value } => labels.get(name).unwrap_or("") == value.as_str(),
            Self::LabelNeq { name, value } => labels.get(name).unwrap_or("") != value.as_str(),
            Self::LabelRe { name, regex } => regex.is_match(labels.get(name).unwrap_or("")),
            Self::LabelNre { name, regex } => !regex.is_match(labels.get(name).unwrap_or("")),
            Self::QueryShardEq(selector) => selector.matches(fp),
            Self::QueryShardNeq(selector) => !selector.matches(fp),
        }
    }
}

fn regex_anchored(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|error| PromqlError::Plan(format!("bad regex `{pattern}`: {error}")))
}

pub(super) fn prepare_matchers(matchers: &[LabelMatcher]) -> Result<Vec<PreparedMatcher>> {
    matchers.iter().map(PreparedMatcher::new).collect()
}

pub(super) fn all_match(
    fp: SeriesFingerprint,
    labels: &Labels,
    matchers: &[PreparedMatcher],
) -> bool {
    for matcher in matchers {
        if !matcher.matches(fp, labels) {
            return false;
        }
    }
    true
}

pub(super) fn row_matches(
    fp: SeriesFingerprint,
    labels: &Labels,
    ts_ms: i64,
    matchers: &[PreparedMatcher],
    start_ms: i64,
    end_ms: i64,
) -> bool {
    if ts_ms.cmp(&start_ms).is_lt() {
        return false;
    }
    if ts_ms.cmp(&end_ms).is_gt() {
        return false;
    }
    all_match(fp, labels, matchers)
}

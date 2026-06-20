//! Label matchers.

use serde::{Deserialize, Serialize};

/// Matcher operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchOp {
    /// `name="value"`
    Eq,
    /// `name!="value"`
    Neq,
    /// `name=~"regex"`
    Re,
    /// `name!~"regex"`
    Nre,
}

/// A single label matcher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

impl LabelMatcher {
    #[must_use]
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }
}

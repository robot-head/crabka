//! Label matcher types used by signal indexes.

/// Label matching operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchOp {
    Eq,
    Neq,
    Re,
    Nre,
}

/// A matcher against one label name.
#[derive(Clone, Debug, PartialEq, Eq)]
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

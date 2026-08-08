//! Remote-topic naming and loop-prevention primitives.

use crate::config::NamingPolicy;

/// Header key that every produced record carries with the origin cluster alias.
/// The identity-naming loop-guard uses it, and it is also useful for provenance.
pub const PROVENANCE_HEADER: &str = "__crabka_origin";

/// Maps source topic names to target topic names and enforces loop-prevention
/// rules for active/active replication.
#[derive(Debug, Clone)]
pub struct Renamer {
    policy: NamingPolicy,
    source_alias: String,
}

impl Renamer {
    /// Create a new [`Renamer`] for the given policy and source cluster alias.
    #[must_use]
    pub fn new(policy: NamingPolicy, source_alias: &str) -> Self {
        Self {
            policy,
            source_alias: source_alias.to_owned(),
        }
    }

    /// Return the name this topic should have on the target cluster.
    ///
    /// - [`NamingPolicy::Default`][]: `<source_alias>.<source_topic>`
    /// - [`NamingPolicy::Identity`][]: `<source_topic>` (unchanged)
    #[must_use]
    pub fn target_name(&self, source_topic: &str) -> String {
        match self.policy {
            NamingPolicy::Default => format!("{}.{}", self.source_alias, source_topic),
            NamingPolicy::Identity => source_topic.to_owned(),
        }
    }

    /// Return `true` if this topic looks like a topic that replication already
    /// produced. This flow should exclude it from its subscription, for loop
    /// prevention.
    ///
    /// Under [`NamingPolicy::Default`] a topic that contains `.`, but does not
    /// *start* with `.`, counts as remote. This mirrors the "topic contains the
    /// replication separator" heuristic of `MirrorMaker` 2. The rule is
    /// deliberately simple for Slice 1, and the two-cluster integration test
    /// pins the exact behaviour.
    ///
    /// Under [`NamingPolicy::Identity`] the loop guard uses the header. See
    /// [`PROVENANCE_HEADER`]. This method then always returns `false`.
    #[must_use]
    pub fn is_remote(&self, topic: &str) -> bool {
        match self.policy {
            NamingPolicy::Default => topic.contains('.') && !topic.starts_with('.'),
            NamingPolicy::Identity => false,
        }
    }

    /// The source cluster alias stamped in [`PROVENANCE_HEADER`] on produced
    /// records, used for identity-policy loop prevention.
    #[must_use]
    pub fn provenance_alias(&self) -> &str {
        &self.source_alias
    }

    /// The naming policy in effect.
    #[must_use]
    pub fn policy(&self) -> NamingPolicy {
        self.policy
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::config::NamingPolicy;

    #[test]
    fn default_policy_prefixes_with_source_alias() {
        let n = Renamer::new(NamingPolicy::Default, "us-east");
        assert2::assert!(n.target_name("orders") == "us-east.orders");
        check!(n.is_remote("eu-west.billing"));
        check!(!n.is_remote("orders"));
    }

    #[test]
    fn identity_policy_keeps_name_and_uses_provenance() {
        let n = Renamer::new(NamingPolicy::Identity, "us-east");
        assert2::assert!(n.target_name("orders") == "orders");
        check!(!n.is_remote("orders"));
        assert2::assert!(n.provenance_alias() == "us-east");
        // `policy()` returns the configured policy verbatim, not the Default.
        assert2::assert!(n.policy() == NamingPolicy::Identity);
    }
}

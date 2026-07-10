//! Remote-topic naming and loop-prevention primitives.

use crate::config::NamingPolicy;

/// Header key stamped on every produced record carrying the origin cluster alias.
/// Used by the identity-naming loop-guard (and useful for provenance generally).
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

    /// Return `true` if this topic looks like it was already produced by
    /// replication and should therefore be excluded from this flow's
    /// subscription (loop prevention).
    ///
    /// Under [`NamingPolicy::Default`] a topic that contains `.` (but does not
    /// *start* with `.`) is treated as remote — this mirrors `MirrorMaker` 2's
    /// "topic contains the replication separator" heuristic.  Intentionally
    /// simple for Slice 1; the two-cluster integration test pins exact
    /// behaviour.
    ///
    /// Under [`NamingPolicy::Identity`] the loop guard is header-based (see
    /// [`PROVENANCE_HEADER`]), so this method always returns `false`.
    #[must_use]
    pub fn is_remote(&self, topic: &str) -> bool {
        match self.policy {
            NamingPolicy::Default => topic.contains('.') && !topic.starts_with('.'),
            NamingPolicy::Identity => false,
        }
    }

    /// The source cluster alias stamped in [`PROVENANCE_HEADER`] on produced
    /// records (used for identity-policy loop prevention).
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
        assert_eq!(n.target_name("orders"), "us-east.orders");
        check!(n.is_remote("eu-west.billing"));
        check!(!n.is_remote("orders"));
    }

    #[test]
    fn identity_policy_keeps_name_and_uses_provenance() {
        let n = Renamer::new(NamingPolicy::Identity, "us-east");
        assert_eq!(n.target_name("orders"), "orders");
        check!(!n.is_remote("orders"));
        assert_eq!(n.provenance_alias(), "us-east");
        // `policy()` returns the configured policy verbatim, not the Default.
        assert_eq!(n.policy(), NamingPolicy::Identity);
    }
}

//! Static broker config for the KIP-848 next-gen consumer group protocol.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use super::assignor::{Assignor, RangeAssignor, UniformAssignor};

/// `group.consumer.migration.policy` — governs classic ↔ next-gen consumer
/// group conversion. Default `Bidirectional`, matching Apache Kafka 4.0
/// (verified empirically against `mirror.gcr.io/apache/kafka:4.0.0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsumerGroupMigrationPolicy {
    /// No conversion in either direction.
    Disabled,
    /// Classic → consumer only.
    Upgrade,
    /// Consumer → classic only.
    Downgrade,
    /// Both upgrade and downgrade are enabled.
    #[default]
    Bidirectional,
}

impl ConsumerGroupMigrationPolicy {
    /// `true` if a classic group may be upgraded to a consumer group.
    #[must_use]
    pub fn allows_upgrade(self) -> bool {
        matches!(self, Self::Upgrade | Self::Bidirectional)
    }

    /// `true` if a consumer group may be downgraded to a classic group.
    #[must_use]
    pub fn allows_downgrade(self) -> bool {
        matches!(self, Self::Downgrade | Self::Bidirectional)
    }

    /// The Kafka config string for this policy.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Upgrade => "upgrade",
            Self::Downgrade => "downgrade",
            Self::Bidirectional => "bidirectional",
        }
    }
}

impl FromStr for ConsumerGroupMigrationPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "upgrade" => Ok(Self::Upgrade),
            "downgrade" => Ok(Self::Downgrade),
            "bidirectional" => Ok(Self::Bidirectional),
            other => Err(format!("invalid group.consumer.migration.policy: {other}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NextGenConfig {
    /// Comma-separated list; "consumer" enables KIP-848. Default "classic,consumer".
    pub rebalance_protocols: Vec<RebalanceProtocol>,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    /// Registered server-side assignors. The list IS the registry; the
    /// client's `server_assignor` field is matched against `Assignor::name()`
    /// by string equality. `Default` seeds the two built-ins
    /// (`uniform`, `range`); operators add their own via
    /// `register_assignor` before `Broker::start`.
    pub assignors: Vec<Arc<dyn Assignor>>,
    pub max_size: usize,
    /// `group.consumer.migration.policy` — governs classic ↔ next-gen
    /// conversion. Consulted by the conversion triggers.
    pub migration_policy: ConsumerGroupMigrationPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceProtocol {
    Classic,
    Consumer,
}

/// Returned by [`NextGenConfig::register_assignor`] when the supplied
/// assignor's `name()` collides with one that is already registered
/// (either a built-in or a previously-registered custom).
#[derive(Debug, thiserror::Error)]
pub enum AssignorRegistrationError {
    #[error("an assignor named {0} is already registered")]
    DuplicateName(String),
}

impl Default for NextGenConfig {
    fn default() -> Self {
        Self {
            rebalance_protocols: vec![RebalanceProtocol::Classic, RebalanceProtocol::Consumer],
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_mins(1),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            assignors: vec![Arc::new(UniformAssignor), Arc::new(RangeAssignor)],
            max_size: 200,
            migration_policy: ConsumerGroupMigrationPolicy::default(),
        }
    }
}

impl NextGenConfig {
    #[must_use]
    pub fn next_gen_enabled(&self) -> bool {
        self.rebalance_protocols
            .contains(&RebalanceProtocol::Consumer)
    }

    /// Register an additional assignor. Returns an error if the name is
    /// already taken. Built-ins (`uniform`, `range`) are registered by
    /// [`Default::default`]; calling `register_assignor` with either
    /// name surfaces as a duplicate-name error.
    pub fn register_assignor(
        &mut self,
        assignor: Arc<dyn Assignor>,
    ) -> Result<(), AssignorRegistrationError> {
        let name = assignor.name();
        if self.assignors.iter().any(|a| a.name() == name) {
            return Err(AssignorRegistrationError::DuplicateName(name.into()));
        }
        self.assignors.push(assignor);
        Ok(())
    }

    /// Resolve a registered assignor by name. Cloning an `Arc` is cheap.
    #[must_use]
    pub fn find_assignor(&self, name: &str) -> Option<Arc<dyn Assignor>> {
        self.assignors.iter().find(|a| a.name() == name).cloned()
    }

    /// `true` when a client may legally request this name via
    /// `ConsumerGroupHeartbeatRequest::server_assignor`.
    #[must_use]
    pub fn assignor_enabled(&self, name: &str) -> bool {
        self.find_assignor(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use std::collections::HashMap;

    use super::*;
    use crate::coordinator::unified::assignor::{Assignment, MemberSubscription, TopicMetadata};

    #[derive(Debug)]
    struct TestAssignor(&'static str);
    impl Assignor for TestAssignor {
        fn name(&self) -> &'static str {
            self.0
        }
        fn assign(&self, _members: &[MemberSubscription], _topics: &TopicMetadata) -> Assignment {
            HashMap::new()
        }
    }

    #[test]
    fn default_registers_uniform_and_range() {
        let cfg = NextGenConfig::default();
        assert!(cfg.assignors.len() == 2);
        let names: Vec<&str> = cfg.assignors.iter().map(|a| a.name()).collect();
        assert!(names.contains(&"uniform"));
        assert!(names.contains(&"range"));
    }

    #[test]
    fn register_assignor_succeeds_for_new_name() {
        let mut cfg = NextGenConfig::default();
        cfg.register_assignor(Arc::new(TestAssignor("custom")))
            .unwrap();
        assert!(cfg.find_assignor("custom").is_some());
    }

    #[test]
    fn register_assignor_rejects_duplicate_name() {
        let mut cfg = NextGenConfig::default();
        let err = cfg
            .register_assignor(Arc::new(TestAssignor("uniform")))
            .unwrap_err();
        match err {
            AssignorRegistrationError::DuplicateName(name) => assert!(name == "uniform"),
        }
    }

    #[test]
    fn find_assignor_returns_registered_impl() {
        let mut cfg = NextGenConfig::default();
        cfg.register_assignor(Arc::new(TestAssignor("x"))).unwrap();
        let resolved = cfg.find_assignor("x").expect("registered");
        assert!(resolved.name() == "x");
    }

    #[test]
    fn assignor_enabled_matches_find_assignor() {
        let mut cfg = NextGenConfig::default();
        cfg.register_assignor(Arc::new(TestAssignor("y"))).unwrap();
        for name in ["uniform", "range", "y", "ghost"] {
            assert!(cfg.assignor_enabled(name) == cfg.find_assignor(name).is_some());
        }
    }

    #[test]
    fn migration_policy_default_is_bidirectional() {
        // Matches Apache Kafka 4.0 (verified empirically).
        assert!(
            NextGenConfig::default().migration_policy
                == ConsumerGroupMigrationPolicy::Bidirectional
        );
    }

    #[test]
    fn migration_policy_from_str_round_trips_all_names() {
        use ConsumerGroupMigrationPolicy as P;
        for p in [P::Disabled, P::Upgrade, P::Downgrade, P::Bidirectional] {
            assert!(p.as_str().parse::<P>().unwrap() == p);
        }
        // Case-insensitive.
        assert!("BiDirectional".parse::<P>().unwrap() == P::Bidirectional);
        assert!("UPGRADE".parse::<P>().unwrap() == P::Upgrade);
    }

    #[test]
    fn migration_policy_from_str_rejects_junk() {
        assert!("sideways".parse::<ConsumerGroupMigrationPolicy>().is_err());
        assert!("".parse::<ConsumerGroupMigrationPolicy>().is_err());
    }

    #[test]
    fn migration_policy_direction_truth_table() {
        use ConsumerGroupMigrationPolicy as P;
        let truth_table = [P::Disabled, P::Upgrade, P::Downgrade, P::Bidirectional]
            .map(|p| (p.allows_upgrade(), p.allows_downgrade()));
        assert!(
            truth_table
                == [
                    (false, false), // Disabled
                    (true, false),  // Upgrade
                    (false, true),  // Downgrade
                    (true, true),   // Bidirectional
                ]
        );
    }
}

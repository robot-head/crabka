//! Static broker config for the KIP-848 next-gen consumer group protocol.

use std::{str::FromStr, sync::Arc, time::Duration};

use qubit_clock::sleep::{AsyncSleeper, SystemSleeper};

use super::assignor::{Assignor, RangeAssignor, UniformAssignor};

/// `group.consumer.migration.policy` governs classic ↔ next-gen consumer
/// group conversion. The default is `Bidirectional`, which matches Apache
/// Kafka 4.0, verified empirically against
/// `mirror.gcr.io/apache/kafka:4.0.0`.
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

#[derive(Clone)]
pub struct NextGenConfig {
    /// Comma-separated list. "consumer" enables KIP-848. Default
    /// "classic,consumer".
    pub rebalance_protocols: Vec<RebalanceProtocol>,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    pub session_expiry_tick: Duration,
    pub actor_mailbox_capacity: usize,
    pub shutdown_ack_timeout: Duration,
    pub classic_initial_rebalance_delay: Duration,
    /// Registered server-side assignors. The list IS the registry. The
    /// broker matches the client's `server_assignor` field against
    /// `Assignor::name()` by string equality. `Default` seeds the two
    /// built-ins, `uniform` and `range`. Operators add their own with
    /// `register_assignor` before `Broker::start`.
    pub assignors: Vec<Arc<dyn Assignor>>,
    pub max_size: usize,
    /// `group.consumer.migration.policy` governs classic ↔ next-gen
    /// conversion. The conversion triggers consult it.
    pub migration_policy: ConsumerGroupMigrationPolicy,
    /// Relative sleeper that drives the per-group actor's session-expiry tick
    /// cadence. Production uses [`qubit_clock::sleep::SystemSleeper`], which is
    /// real time. Tests inject a [`qubit_clock::sleep::MockSleeper`] so the
    /// tick fires on a controlled mock timeline instead of wall-clock time.
    pub sleeper: Arc<dyn AsyncSleeper>,
}

// Manual `Debug` (the `AsyncSleeper` trait object is not `Debug`): print every
// operator-relevant field and elide the sleeper. Kept so the enclosing
// `#[derive(Debug)]` `GroupCoordinator` still derives.
impl std::fmt::Debug for NextGenConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextGenConfig")
            .field("rebalance_protocols", &self.rebalance_protocols)
            .field("session_timeout", &self.session_timeout)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("min_session_timeout", &self.min_session_timeout)
            .field("max_session_timeout", &self.max_session_timeout)
            .field("min_heartbeat_interval", &self.min_heartbeat_interval)
            .field("max_heartbeat_interval", &self.max_heartbeat_interval)
            .field("session_expiry_tick", &self.session_expiry_tick)
            .field("actor_mailbox_capacity", &self.actor_mailbox_capacity)
            .field("shutdown_ack_timeout", &self.shutdown_ack_timeout)
            .field(
                "classic_initial_rebalance_delay",
                &self.classic_initial_rebalance_delay,
            )
            .field("assignors", &self.assignors)
            .field("max_size", &self.max_size)
            .field("migration_policy", &self.migration_policy)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceProtocol {
    Classic,
    Consumer,
}

/// Returned by [`NextGenConfig::register_assignor`] when the supplied
/// assignor's `name()` collides with one that is already registered. The
/// existing entry can be a built-in or a previously-registered custom
/// assignor.
#[derive(Debug, thiserror::Error)]
pub enum AssignorRegistrationError {
    #[error("an assignor named {0} is already registered")]
    DuplicateName(String),
}

/// Default consumer session timeout: 45 s, matching Kafka's
/// `group.consumer.session.timeout.ms`.
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(45);

/// Default consumer heartbeat interval: 5 s, matching Kafka's
/// `group.consumer.heartbeat.interval.ms`.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Lower bound on the negotiated session timeout: 45 s, matching Kafka's
/// `group.consumer.min.session.timeout.ms`.
pub const DEFAULT_MIN_SESSION_TIMEOUT: Duration = Duration::from_secs(45);

/// Upper bound on the negotiated session timeout: 60 s, matching Kafka's
/// `group.consumer.max.session.timeout.ms`.
pub const DEFAULT_MAX_SESSION_TIMEOUT: Duration = Duration::from_mins(1);

/// Lower bound on the negotiated heartbeat interval: 5 s, matching Kafka's
/// `group.consumer.min.heartbeat.interval.ms`.
pub const DEFAULT_MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Upper bound on the negotiated heartbeat interval: 15 s, matching Kafka's
/// `group.consumer.max.heartbeat.interval.ms`.
pub const DEFAULT_MAX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Crabka's default cap on consumer-group membership
/// (`group.consumer.max.size`).
pub const DEFAULT_MAX_GROUP_SIZE: usize = 200;

impl Default for NextGenConfig {
    fn default() -> Self {
        Self {
            rebalance_protocols: vec![RebalanceProtocol::Classic, RebalanceProtocol::Consumer],
            session_timeout: DEFAULT_SESSION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            min_session_timeout: DEFAULT_MIN_SESSION_TIMEOUT,
            max_session_timeout: DEFAULT_MAX_SESSION_TIMEOUT,
            min_heartbeat_interval: DEFAULT_MIN_HEARTBEAT_INTERVAL,
            max_heartbeat_interval: DEFAULT_MAX_HEARTBEAT_INTERVAL,
            session_expiry_tick: Duration::from_secs(1),
            actor_mailbox_capacity: 64,
            shutdown_ack_timeout: Duration::from_secs(5),
            classic_initial_rebalance_delay: Duration::from_secs(3),
            assignors: vec![Arc::new(UniformAssignor), Arc::new(RangeAssignor)],
            max_size: DEFAULT_MAX_GROUP_SIZE,
            migration_policy: ConsumerGroupMigrationPolicy::default(),
            sleeper: Arc::new(SystemSleeper::new()),
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
    /// already taken. [`Default::default`] registers the built-ins
    /// `uniform` and `range`, so a `register_assignor` call with either
    /// name returns a duplicate-name error.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
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

    /// Resolve a registered assignor by name. An `Arc` clone is cheap.
    #[must_use]
    pub fn find_assignor(&self, name: &str) -> Option<Arc<dyn Assignor>> {
        self.assignors.iter().find(|a| a.name() == name).cloned()
    }

    /// `true` when a client may legally request this name in
    /// `ConsumerGroupHeartbeatRequest::server_assignor`.
    #[must_use]
    pub fn assignor_enabled(&self, name: &str) -> bool {
        self.find_assignor(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::assert;

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
        let cases = [
            (P::Disabled, (false, false)),
            (P::Upgrade, (true, false)),
            (P::Downgrade, (false, true)),
            (P::Bidirectional, (true, true)),
        ];
        for (policy, want) in cases {
            assert!(
                (policy.allows_upgrade(), policy.allows_downgrade()) == want,
                "policy {policy:?}"
            );
        }
    }

    #[test]
    fn debug_renders_operator_fields_and_elides_sleeper() {
        // The manual `Debug` impl exists because the `AsyncSleeper` trait object
        // is not `Debug`. Assert on the rendered content — a stubbed-out `fmt`
        // body (writing nothing) must not pass. Distinctive values on a subset
        // of fields prove the real fields are emitted, not a static placeholder.
        let cfg = NextGenConfig {
            session_timeout: Duration::from_secs(37),
            max_size: 4242,
            migration_policy: ConsumerGroupMigrationPolicy::Downgrade,
            ..Default::default()
        };

        let rendered = format!("{cfg:?}");

        assert!(rendered.starts_with("NextGenConfig"), "got {rendered}");
        for needle in [
            "rebalance_protocols",
            "session_timeout",
            "37s",
            "heartbeat_interval",
            "min_session_timeout",
            "max_session_timeout",
            "min_heartbeat_interval",
            "max_heartbeat_interval",
            "assignors",
            "max_size",
            "4242",
            "migration_policy",
            "Downgrade",
        ] {
            assert!(
                rendered.contains(needle),
                "Debug output missing {needle:?}: {rendered}"
            );
        }
        // `finish_non_exhaustive` elides the sleeper with a trailing `..`; the
        // elided field's name must not leak into the output.
        assert!(
            rendered.contains(".."),
            "expected non-exhaustive marker: {rendered}"
        );
        assert!(
            !rendered.contains("sleeper"),
            "sleeper must be elided: {rendered}"
        );
    }
}

//! Static broker config for the KIP-848 next-gen consumer group protocol.

use std::sync::Arc;
use std::time::Duration;

use super::assignor::{Assignor, RangeAssignor, UniformAssignor};

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
    use crate::coordinator::next_gen::assignor::{Assignment, MemberSubscription, TopicMetadata};

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
}

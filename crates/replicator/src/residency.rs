//! Deny-wins residency gate. A topic-flow is permitted unless a matching policy
//! denies it. Semantics mirror Crabka's ACL authorizer: DENY beats ALLOW; an
//! `allow_zones` list means "only these zones are allowed" (deny-by-default for
//! the matched topic).

use crate::{
    config::{PolicyConfig, Residency},
    error::ReplicatorError,
    selector::Selector,
};

struct Rule {
    topics: Selector,
    residency: Residency,
}

/// Deny-wins residency gate compiled from a slice of [`PolicyConfig`]s.
pub struct ResidencyGate {
    rules: Vec<Rule>,
}

impl ResidencyGate {
    /// Compile a [`ResidencyGate`] from a slice of policies.
    ///
    /// Only policies that carry a [`Residency`] constraint contribute rules;
    /// policies without one are silently skipped.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicatorError::Config`] if any topic-glob pattern is invalid.
    pub fn compile(policies: &[PolicyConfig]) -> Result<Self, ReplicatorError> {
        let mut rules = Vec::new();
        for p in policies {
            if let Some(res) = &p.residency {
                rules.push(Rule {
                    topics: Selector::compile(&p.topics, &[])?,
                    residency: res.clone(),
                });
            }
        }
        Ok(Self { rules })
    }

    /// May `topic` replicate to a target whose compliance zones are `target_zones`?
    ///
    /// Returns `true` (permitted) unless a matching rule blocks the flow:
    /// - A zone in `deny_zones` always blocks (DENY beats ALLOW).
    /// - A non-empty `allow_zones` list blocks if the target has none of those zones.
    /// - An unmatched topic is always permitted.
    #[must_use]
    pub fn permits(&self, topic: &str, target_zones: &[String]) -> bool {
        for rule in &self.rules {
            if !rule.topics.matches(topic) {
                continue;
            }
            // DENY beats ALLOW.
            if rule
                .residency
                .deny_zones
                .iter()
                .any(|z| target_zones.contains(z))
            {
                return false;
            }
            // allow_zones is a whitelist: if non-empty, the target must have at least one.
            if !rule.residency.allow_zones.is_empty()
                && !rule
                    .residency
                    .allow_zones
                    .iter()
                    .any(|z| target_zones.contains(z))
            {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::config::{PolicyConfig, Residency};

    fn policies() -> Vec<PolicyConfig> {
        vec![PolicyConfig {
            name: "keep-pii-in-eu".into(),
            topics: vec!["customers".into(), "kyc.*".into()],
            residency: Some(Residency {
                allow_zones: vec!["gdpr".into()],
                deny_zones: vec![],
            }),
        }]
    }

    #[test]
    fn allows_when_target_zone_in_allow_list() {
        let gate = ResidencyGate::compile(&policies()).unwrap();
        assert!(gate.permits("customers", &["eu".into(), "gdpr".into()]));
    }

    #[test]
    fn blocks_when_target_lacks_allowed_zone() {
        let gate = ResidencyGate::compile(&policies()).unwrap();
        assert!(!gate.permits("customers", &["us".into()]));
        assert!(!gate.permits("kyc.eu", &["us".into()]));
    }

    #[test]
    fn deny_wins_over_allow() {
        let mut p = policies();
        p[0].residency = Some(Residency {
            allow_zones: vec!["gdpr".into()],
            deny_zones: vec!["gdpr".into()],
        });
        let gate = ResidencyGate::compile(&p).unwrap();
        assert!(!gate.permits("customers", &["gdpr".into()]));
    }

    #[test]
    fn unmatched_topic_is_permitted() {
        let gate = ResidencyGate::compile(&policies()).unwrap();
        assert!(gate.permits("orders", &["us".into()]));
    }
}

//! Partition assignors: `range` (eager) and `cooperative-sticky` (KIP-429 incremental).

#![allow(dead_code)]

pub(crate) mod cooperative_sticky;
pub(crate) mod range;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebalanceProtocol {
    Eager,
    Cooperative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assignor {
    Range,
    CooperativeSticky,
}

impl std::str::FromStr for Assignor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "range" => Ok(Self::Range),
            "cooperative-sticky" => Ok(Self::CooperativeSticky),
            _ => Err(format!("invalid assignor: {value}")),
        }
    }
}

impl Assignor {
    pub(crate) fn protocol_name(self) -> &'static str {
        match self {
            Assignor::Range => "range",
            Assignor::CooperativeSticky => "cooperative-sticky",
        }
    }
    pub(crate) fn rebalance_protocol(self) -> RebalanceProtocol {
        match self {
            Assignor::Range => RebalanceProtocol::Eager,
            Assignor::CooperativeSticky => RebalanceProtocol::Cooperative,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn assignor_values_parse_exact_spellings() {
        assert2::assert!("range".parse::<Assignor>().unwrap() == Assignor::Range);
        assert2::assert!(
            "cooperative-sticky".parse::<Assignor>().unwrap() == Assignor::CooperativeSticky
        );
        assert2::assert!("cooperative_sticky".parse::<Assignor>().is_err());
        assert2::assert!("unknown".parse::<Assignor>().is_err());
    }

    #[test]
    fn assignor_protocol_names_match_kafka_protocols() {
        for (_name, assignor, expected) in [
            ("range", Assignor::Range, "range"),
            (
                "cooperative sticky",
                Assignor::CooperativeSticky,
                "cooperative-sticky",
            ),
        ] {
            assert2::assert!(assignor.protocol_name() == expected);
        }
    }

    #[test]
    fn assignor_rebalance_protocols_match_assignor_strategy() {
        for (_name, assignor, expected) in [
            ("range", Assignor::Range, RebalanceProtocol::Eager),
            (
                "cooperative sticky",
                Assignor::CooperativeSticky,
                RebalanceProtocol::Cooperative,
            ),
        ] {
            assert2::assert!(assignor.rebalance_protocol() == expected);
        }
    }
}

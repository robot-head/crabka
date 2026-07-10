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
    use assert2::assert;

    use super::*;

    #[test]
    fn assignor_protocol_names_match_kafka_protocols() {
        for (name, assignor, expected) in [
            ("range", Assignor::Range, "range"),
            (
                "cooperative sticky",
                Assignor::CooperativeSticky,
                "cooperative-sticky",
            ),
        ] {
            assert!(assignor.protocol_name() == expected, "case {name}");
        }
    }

    #[test]
    fn assignor_rebalance_protocols_match_assignor_strategy() {
        for (name, assignor, expected) in [
            ("range", Assignor::Range, RebalanceProtocol::Eager),
            (
                "cooperative sticky",
                Assignor::CooperativeSticky,
                RebalanceProtocol::Cooperative,
            ),
        ] {
            assert!(assignor.rebalance_protocol() == expected, "case {name}");
        }
    }
}

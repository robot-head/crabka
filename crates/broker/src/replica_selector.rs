//! KIP-392 replica selection. The partition leader runs `select` on every
//! consumer Fetch that carries a `client.rack` (`rack_id`) and reports the
//! chosen node id in `FetchResponse.preferred_read_replica`. Returning `-1`
//! means "no preference — read from the leader".

/// One replica's view as the leader sees it, for selection purposes.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaView {
    /// Wire replica id (broker node id as `i32`).
    pub node_id: i32,
    /// The broker's configured rack, if any.
    pub rack: Option<String>,
    /// Whether this replica is currently in the ISR.
    pub in_isr: bool,
}

/// Which built-in selector the broker uses. Maps to Kafka's
/// `replica.selector.class`, but as a native enum (Crabka does not load
/// JVM classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicaSelectorKind {
    /// Always read from the leader. Default.
    #[default]
    Leader,
    /// Prefer a same-rack in-sync replica when the client advertises a rack.
    RackAware,
}

impl ReplicaSelectorKind {
    /// Parse the `replica.selector` config value. Accepts `"leader"` and
    /// `"rack-aware"`. Returns `Err(value)` on anything else.
    pub fn from_config_str(s: &str) -> Result<Self, String> {
        match s.trim() {
            "leader" => Ok(Self::Leader),
            "rack-aware" => Ok(Self::RackAware),
            other => Err(other.to_string()),
        }
    }

    /// Choose the preferred read replica. Returns a node id, or `-1` for
    /// "no preference — use the leader".
    pub(crate) fn select(
        self,
        client_rack: Option<&str>,
        leader_id: i32,
        replicas: &[ReplicaView],
    ) -> i32 {
        match self {
            Self::Leader => -1,
            Self::RackAware => {
                let Some(rack) = client_rack.filter(|r| !r.is_empty()) else {
                    return -1;
                };
                let winner = replicas
                    .iter()
                    .filter(|r| r.in_isr && r.rack.as_deref() == Some(rack))
                    .min_by_key(|r| r.node_id);
                match winner {
                    Some(r) if r.node_id != leader_id => r.node_id,
                    _ => -1,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn view(node_id: i32, rack: &str, in_isr: bool) -> ReplicaView {
        ReplicaView {
            node_id,
            rack: Some(rack.to_string()),
            in_isr,
        }
    }

    #[test]
    fn parse_known_values() {
        for (input, want) in [
            ("leader", ReplicaSelectorKind::Leader),
            ("rack-aware", ReplicaSelectorKind::RackAware),
        ] {
            assert!(
                ReplicaSelectorKind::from_config_str(input) == Ok(want),
                "{input}"
            );
        }
        assert!(ReplicaSelectorKind::from_config_str("bogus").is_err());
    }

    #[test]
    fn leader_kind_always_returns_minus_one() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert!(ReplicaSelectorKind::Leader.select(Some("b"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_picks_same_rack_isr_member() {
        let replicas = [view(1, "a", true), view(2, "b", true), view(3, "b", true)];
        // leader is node 1 (rack a); client in rack b -> lowest-id same-rack
        // ISR member is node 2.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == 2);
    }

    #[test]
    fn rack_aware_none_when_client_rack_missing() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert!(ReplicaSelectorKind::RackAware.select(None, 1, &replicas) == -1);
        assert!(ReplicaSelectorKind::RackAware.select(Some(""), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_none_when_no_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "a", true)];
        assert!(ReplicaSelectorKind::RackAware.select(Some("z"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_ignores_non_isr_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "b", false)];
        // Node 2 is same-rack but out of ISR -> no redirect.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_none_when_only_same_rack_replica_is_leader() {
        let replicas = [view(1, "b", true), view(2, "a", true)];
        // Client rack b matches only the leader (node 1) -> stay on leader.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == -1);
    }
}

//! KIP-73 throttled replication — value types and parser.

pub use crabka_throttle::{ThrottleState, TokenBucket};

mod refresh;
pub use refresh::{ImageWatcher, run};

use crabka_metadata::{MetadataImage, NodeId};

/// Topic-level `*.throttled.replicas` config value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottledReplicas {
    /// Empty string — no replicas throttled.
    None,
    /// `"*"` wildcard — all replicas of this topic throttled.
    All,
    /// `"partition:broker,partition:broker,..."` — specific pairs.
    List(Vec<(i32, NodeId)>),
}

impl ThrottledReplicas {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Ok(Self::None);
        }
        if value == "*" {
            return Ok(Self::All);
        }
        let mut out = Vec::new();
        for pair in value.split(',') {
            let (p_str, n_str) = pair
                .split_once(':')
                .ok_or_else(|| format!("invalid pair {pair:?}"))?;
            let p: i32 = p_str
                .trim()
                .parse()
                .map_err(|e| format!("partition: {e}"))?;
            let n: NodeId = n_str.trim().parse().map_err(|e| format!("broker: {e}"))?;
            out.push((p, n));
        }
        Ok(Self::List(out))
    }

    #[must_use]
    pub fn contains(&self, partition: i32, node: NodeId) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::List(v) => v.iter().any(|&(p, n)| p == partition && n == node),
        }
    }
}

/// Both leader-side and follower-side throttled replicas for a topic.
#[derive(Debug, Clone)]
pub struct TopicThrottle {
    pub leader: ThrottledReplicas,
    pub follower: ThrottledReplicas,
}

impl TopicThrottle {
    #[must_use]
    pub fn for_topic(image: &MetadataImage, topic: &str) -> Self {
        let configs = image.topic_config(topic);
        let read = |key: &str| -> ThrottledReplicas {
            configs
                .and_then(|c| c.get(key))
                .and_then(|v| ThrottledReplicas::parse(v).ok())
                .unwrap_or(ThrottledReplicas::None)
        };
        Self {
            leader: read("leader.replication.throttled.replicas"),
            follower: read("follower.replication.throttled.replicas"),
        }
    }
}

pub const LEADER_THROTTLED_REPLICAS_KEY: &str = "leader.replication.throttled.replicas";
pub const FOLLOWER_THROTTLED_REPLICAS_KEY: &str = "follower.replication.throttled.replicas";
pub const LEADER_THROTTLED_RATE_KEY: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_THROTTLED_RATE_KEY: &str = "follower.replication.throttled.rate";

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn empty_string_parses_as_none() {
        assert!(ThrottledReplicas::parse("").unwrap() == ThrottledReplicas::None);
    }

    #[test]
    fn wildcard_parses_as_all() {
        assert!(ThrottledReplicas::parse("*").unwrap() == ThrottledReplicas::All);
    }

    #[test]
    fn single_pair_parses() {
        let r = ThrottledReplicas::parse("0:1").unwrap();
        assert!(r.contains(0, 1));
        assert!(!r.contains(0, 2));
        assert!(!r.contains(1, 1));
    }

    #[test]
    fn multiple_pairs_parse() {
        let r = ThrottledReplicas::parse("0:1,0:2,1:3").unwrap();
        assert!(r.contains(0, 1));
        assert!(r.contains(0, 2));
        assert!(r.contains(1, 3));
        assert!(!r.contains(1, 1));
    }

    #[test]
    fn malformed_pair_rejected() {
        assert!(ThrottledReplicas::parse("not-a-pair").is_err());
        assert!(ThrottledReplicas::parse("0:x").is_err());
        assert!(ThrottledReplicas::parse("x:1").is_err());
    }

    #[test]
    fn whitespace_tolerated() {
        let r = ThrottledReplicas::parse(" 0 : 1 , 2:3 ").unwrap();
        assert!(r.contains(0, 1));
        assert!(r.contains(2, 3));
    }
}

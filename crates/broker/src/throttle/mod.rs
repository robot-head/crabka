//! KIP-73 throttled replication: the value types and the parser.

pub use crabka_throttle::{ThrottleState, TokenBucket};

mod refresh;
use crabka_metadata::{MetadataImage, NodeId};
pub(crate) use refresh::apply_image;
pub use refresh::{ImageWatcher, run};

/// Topic-level `*.throttled.replicas` config value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottledReplicas {
    /// Empty string: no replicas are throttled.
    None,
    /// `"*"` wildcard: all replicas of this topic are throttled.
    All,
    /// `"partition:broker,partition:broker,..."`: specific pairs.
    List(Vec<(i32, NodeId)>),
}

impl ThrottledReplicas {
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
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
            let n: u64 = n_str.trim().parse().map_err(|e| format!("broker: {e}"))?;
            out.push((p, NodeId(n)));
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
pub const ALTER_LOG_DIRS_THROTTLED_RATE_KEY: &str =
    "replica.alter.log.dirs.io.max.bytes.per.second";

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

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
        for (partition, broker, want) in [(0, 1, true), (0, 2, false), (1, 1, false)] {
            assert!(
                r.contains(partition, NodeId(broker)) == want,
                "{partition}:{broker}"
            );
        }
    }

    #[test]
    fn multiple_pairs_parse() {
        let r = ThrottledReplicas::parse("0:1,0:2,1:3").unwrap();
        for (partition, broker, want) in [(0, 1, true), (0, 2, true), (1, 3, true), (1, 1, false)] {
            assert!(
                r.contains(partition, NodeId(broker)) == want,
                "{partition}:{broker}"
            );
        }
    }

    #[test]
    fn malformed_pair_rejected() {
        for input in ["not-a-pair", "0:x", "x:1"] {
            assert!(ThrottledReplicas::parse(input).is_err(), "{input}");
        }
    }

    #[test]
    fn whitespace_tolerated() {
        let r = ThrottledReplicas::parse(" 0 : 1 , 2:3 ").unwrap();
        assert!(r.contains(0, NodeId(1)));
        assert!(r.contains(2, NodeId(3)));
    }
}

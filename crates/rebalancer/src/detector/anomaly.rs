//! Anomaly types persisted by `AnomalyStore` and surfaced via the
//! `GetAnomalies` RPC. `(kind, key)` is the dedup unit used by the
//! detector tick loop to fold a sustained condition into one record.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::IntoStaticStr)]
pub enum AnomalyKind {
    BrokerDeath,
    UnderReplicatedPartitions,
    DiskPressure,
    SlowBroker,
}

impl AnomalyKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalySeverity {
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AnomalyKey {
    Broker(i32),
    Partition {
        topic: String,
        partition: i32,
    },
    BrokerPartition {
        broker: i32,
        topic: String,
        partition: i32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Anomaly {
    pub id: String,
    pub kind: AnomalyKind,
    pub key: AnomalyKey,
    pub severity: AnomalySeverity,
    pub detected_at_ms: i64,
    pub last_seen_at_ms: i64,
    #[serde(default)]
    pub resolved_at_ms: Option<i64>,
    #[serde(default)]
    pub triggered_proposal_id: Option<String>,
    #[serde(default)]
    pub mute_until_ms: Option<i64>,
    pub details: String,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn anomaly_kind_as_str_unique() {
        let kinds = [
            AnomalyKind::BrokerDeath,
            AnomalyKind::UnderReplicatedPartitions,
            AnomalyKind::DiskPressure,
            AnomalyKind::SlowBroker,
        ];
        let strs: Vec<&'static str> = kinds.iter().map(|k| k.as_str()).collect();
        assert!(
            strs == vec![
                "BrokerDeath",
                "UnderReplicatedPartitions",
                "DiskPressure",
                "SlowBroker",
            ]
        );
        let mut sorted = strs.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert!(sorted.len() == 4);
    }

    #[test]
    fn anomaly_key_serde_roundtrip_for_each_variant() {
        let cases = [
            AnomalyKey::Broker(7),
            AnomalyKey::Partition {
                topic: "orders".into(),
                partition: 3,
            },
            AnomalyKey::BrokerPartition {
                broker: 2,
                topic: "orders".into(),
                partition: 3,
            },
        ];
        for k in cases {
            let json = serde_json::to_string(&k).expect("serialize");
            let back: AnomalyKey = serde_json::from_str(&json).expect("deserialize");
            assert!(k == back);
        }
    }

    #[test]
    fn anomaly_default_state_unresolved_and_unmuted() {
        let a = Anomaly {
            id: "test-id".into(),
            kind: AnomalyKind::BrokerDeath,
            key: AnomalyKey::Broker(1),
            severity: AnomalySeverity::Critical,
            detected_at_ms: 100,
            last_seen_at_ms: 100,
            resolved_at_ms: None,
            triggered_proposal_id: None,
            mute_until_ms: None,
            details: "broker 1 down".into(),
        };
        assert!((a.resolved_at_ms, a.mute_until_ms) == (None, None));
    }
}

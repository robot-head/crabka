//! cp's `SchemaRegistryProtocol` wire types (the `"sr"` group), serialized
//! byte-exactly to cp-schema-registry 7.4.0 (JSON; calibrated in Task 5).

use serde::{Deserialize, Serialize};

/// Protocol type for the SR election group (cp constant).
pub const SR_PROTOCOL_TYPE: &str = "sr";
/// The single `JoinGroup` protocol name cp advertises (seed; cp-captured in Task 5).
pub const SR_PROTOCOL_NAME: &str = "v1";

/// A node's identity, serialized into the `JoinGroup` protocol `metadata`.
/// Field order is fixed to match cp's `SchemaRegistryIdentity` JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRegistryIdentity {
    pub version: i32,
    pub host: String,
    pub port: i32,
    pub master_eligibility: bool,
    pub scheme: String,
}

impl SchemaRegistryIdentity {
    /// The node's advertised REST base URL.
    #[must_use]
    pub fn url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }
}

/// The `SyncGroup` assignment cp's leader broadcasts: which identity is master.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRegistryGroupAssignment {
    pub error: i32,
    #[serde(default)]
    pub master: Option<SchemaRegistryIdentity>,
}

/// The leader's deterministic master-selection among the eligible members.
/// Seed rule: the eligible member whose identity sorts first by `(host, port)`;
/// ties broken by `member_id`. The EXACT cp comparator is pinned in Task 5.
#[must_use]
pub fn select_master(
    members: &[(String, SchemaRegistryIdentity)],
) -> Option<SchemaRegistryIdentity> {
    members
        .iter()
        .filter(|(_, id)| id.master_eligibility)
        .min_by(|(am, ai), (bm, bi)| {
            (ai.host.as_str(), ai.port, am.as_str()).cmp(&(bi.host.as_str(), bi.port, bm.as_str()))
        })
        .map(|(_, id)| id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(host: &str, port: i32, eligible: bool) -> SchemaRegistryIdentity {
        SchemaRegistryIdentity {
            version: 1,
            host: host.into(),
            port,
            scheme: "http".into(),
            master_eligibility: eligible,
        }
    }

    #[test]
    fn identity_json_round_trips_and_is_field_ordered() {
        let i = id("h", 8081, true);
        let bytes = serde_json::to_vec(&i).unwrap();
        // cp's SchemaRegistryIdentity JSON (field order pinned; calibrated in Task 5).
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            r#"{"version":1,"host":"h","port":8081,"master_eligibility":true,"scheme":"http"}"#
        );
        let back: SchemaRegistryIdentity = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, i);
    }

    #[test]
    fn assignment_round_trips_with_and_without_master() {
        let a = SchemaRegistryGroupAssignment {
            error: 0,
            master: Some(id("h", 8081, true)),
        };
        let b: SchemaRegistryGroupAssignment =
            serde_json::from_slice(&serde_json::to_vec(&a).unwrap()).unwrap();
        assert_eq!(a, b);
        let none = SchemaRegistryGroupAssignment {
            error: 1,
            master: None,
        };
        let n: SchemaRegistryGroupAssignment =
            serde_json::from_slice(&serde_json::to_vec(&none).unwrap()).unwrap();
        assert_eq!(none, n);
    }

    #[test]
    fn select_master_picks_a_deterministic_eligible_member() {
        let a = ("m2".to_string(), id("b", 8081, true));
        let b = ("m1".to_string(), id("a", 8081, true));
        let c = ("m3".to_string(), id("z", 8081, false)); // ineligible
        let pick1 = select_master(&[a.clone(), b.clone(), c.clone()]);
        let pick2 = select_master(&[c, b, a]);
        assert_eq!(pick1, pick2);
        assert!(pick1.as_ref().unwrap().master_eligibility);
    }

    #[test]
    fn select_master_none_when_no_eligible() {
        assert!(select_master(&[("m".into(), id("a", 1, false))]).is_none());
    }

    #[test]
    fn identity_url_builds_scheme_host_port() {
        assert_eq!(id("h", 8081, true).url(), "http://h:8081");
    }
}

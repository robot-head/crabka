//! cp's `SchemaRegistryProtocol` wire types (the `"sr"` group), serialized
//! byte-exactly to cp-schema-registry 7.4.0 (JSON).
//!
//! Matches Confluent Schema Registry's
//! `io.confluent.kafka.schemaregistry.{storage.SchemaRegistryIdentity,
//! leaderelector.kafka.SchemaRegistryProtocol$Assignment}` wire contracts. cp
//! wins every divergence:
//!
//!   * `SchemaRegistryIdentity` JSON key order is `host, port,
//!     master_eligibility, scheme, version` (the eligibility field is
//!     `master_eligibility` even in 7.4.0 — the master→leader rename touched
//!     REST/config, not this wire JSON; `version` is LAST, not first).
//!   * `SchemaRegistryGroupAssignment` (cp's `Assignment`) is `{error, master,
//!     master_identity, version}` where `master` is the elected master's Kafka
//!     MEMBER-ID string and `master_identity` is its `SchemaRegistryIdentity`.
//!   * the `JoinGroup` protocol NAME is `"v0"` (the identity/assignment
//!     `"version":1` is cp's internal SR-protocol *version* field, a different
//!     thing from the protocol name).

use serde::{Deserialize, Serialize};

/// Protocol type for the SR election group (cp constant `"sr"`).
pub const SR_PROTOCOL_TYPE: &str = "sr";
/// The single `JoinGroup` protocol name cp advertises (cp constant `"v0"`,
/// captured from `SchemaRegistryCoordinator`).
pub const SR_PROTOCOL_NAME: &str = "v0";
/// cp's internal SR-protocol version stamped into the identity + assignment
/// (`"version":1`). Distinct from `SR_PROTOCOL_NAME`.
pub const SR_VERSION: i32 = 1;

/// A node's identity, serialized into the `JoinGroup` protocol `metadata` and
/// echoed in the assignment's `master_identity`. JSON key order matches cp's
/// `SchemaRegistryIdentity`: `host, port, master_eligibility, scheme, version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRegistryIdentity {
    pub host: String,
    pub port: i32,
    pub master_eligibility: bool,
    pub scheme: String,
    pub version: i32,
}

impl SchemaRegistryIdentity {
    /// The node's advertised REST base URL (cp's `getUrl()`), used as the
    /// master-selection sort key.
    #[must_use]
    pub fn url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }
}

/// cp's `SchemaRegistryProtocol$Assignment`: the `SyncGroup` payload the leader
/// broadcasts to every member. `master` is the elected master's Kafka member-id
/// string; `master_identity` is that master's identity (absent only on error).
/// JSON key order matches cp: `error, master, master_identity, version`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRegistryGroupAssignment {
    pub error: i32,
    #[serde(default)]
    pub master: Option<String>,
    #[serde(default)]
    pub master_identity: Option<SchemaRegistryIdentity>,
    pub version: i32,
}

/// The leader's deterministic master selection among the eligible members, as
/// cp's `SchemaRegistryCoordinator` performs it: filter to `master_eligibility`
/// members, then pick the one whose `url()` sorts first (cp collects the
/// eligible members' `getUrl()`s and takes the minimum). Returns
/// `(member_id, identity)` of the elected master.
#[must_use]
pub fn select_master(
    members: &[(String, SchemaRegistryIdentity)],
) -> Option<(String, SchemaRegistryIdentity)> {
    members
        .iter()
        .filter(|(_, id)| id.master_eligibility)
        .min_by(|(_, ai), (_, bi)| ai.url().cmp(&bi.url()))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(host: &str, port: i32, eligible: bool) -> SchemaRegistryIdentity {
        SchemaRegistryIdentity {
            host: host.into(),
            port,
            master_eligibility: eligible,
            scheme: "http".into(),
            version: SR_VERSION,
        }
    }

    #[test]
    fn identity_json_round_trips_and_is_field_ordered() {
        let i = id("sr-node-1", 8081, true);
        let bytes = serde_json::to_vec(&i).unwrap();
        // cp's SchemaRegistryIdentity JSON (field order pinned to cp 7.4.0).
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            r#"{"host":"sr-node-1","port":8081,"master_eligibility":true,"scheme":"http","version":1}"#
        );
        let back: SchemaRegistryIdentity = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, i);
    }

    #[test]
    fn assignment_round_trips_with_and_without_master() {
        for (name, assignment) in [
            (
                "with_master",
                SchemaRegistryGroupAssignment {
                    error: 0,
                    master: Some("crabka-d7c9d4c3".into()),
                    master_identity: Some(id("sr-node-1", 8081, true)),
                    version: SR_VERSION,
                },
            ),
            (
                "without_master",
                SchemaRegistryGroupAssignment {
                    error: 1,
                    master: None,
                    master_identity: None,
                    version: SR_VERSION,
                },
            ),
        ] {
            let decoded: SchemaRegistryGroupAssignment =
                serde_json::from_slice(&serde_json::to_vec(&assignment).unwrap()).unwrap();
            assert_eq!(decoded, assignment, "case {name}");
        }
    }

    #[test]
    fn select_master_picks_url_first_eligible_member() {
        let a = ("m2".to_string(), id("b", 8081, true));
        let b = ("m1".to_string(), id("a", 8081, true));
        let c = ("m3".to_string(), id("z", 8081, false)); // ineligible
        let pick1 = select_master(&[a.clone(), b.clone(), c.clone()]);
        let pick2 = select_master(&[c, b.clone(), a]);
        // `http://a:8081` sorts before `http://b:8081`, so member `m1` wins
        // regardless of input order.
        assert_eq!(pick1, Some(b.clone()));
        assert_eq!(pick2, Some(b));
    }

    #[test]
    fn select_master_none_when_no_eligible() {
        assert!(select_master(&[("m".into(), id("a", 1, false))]).is_none());
    }

    #[test]
    fn identity_url_builds_scheme_host_port() {
        assert_eq!(id("h", 8081, true).url(), "http://h:8081");
    }

    // ── cp-byte-exact pins (captured from cp-schema-registry 7.4.0) ──────────────
    //
    // Exact bytes from `tests/fixtures/election/{members,group}.json`, captured
    // by booting two real cp nodes against a Crabka broker and reading the group
    // via DescribeGroups. These run WITHOUT Docker — the durable regression
    // proof that our encoders reproduce cp's wire JSON byte-for-byte.

    /// cp's elected master identity (the `master_identity` from the captured
    /// assignment): `sr-node-1:8081`, http, eligible, version 1.
    fn cp_master_identity() -> SchemaRegistryIdentity {
        SchemaRegistryIdentity {
            host: "sr-node-1".into(),
            port: 8081,
            master_eligibility: true,
            scheme: "http".into(),
            version: 1,
        }
    }

    #[test]
    fn identity_encodes_cp_member_metadata_byte_exactly() {
        // Captured `master_identity` bytes (tests/fixtures/election/members.json).
        let id = cp_master_identity();
        assert_eq!(
            serde_json::to_vec(&id).unwrap(),
            br#"{"host":"sr-node-1","port":8081,"master_eligibility":true,"scheme":"http","version":1}"#
                .to_vec()
        );
    }

    #[test]
    fn assignment_encodes_cp_member_assignment_byte_exactly() {
        // Captured `member_assignment` bytes (tests/fixtures/election/members.json):
        // the master member-id string + master_identity object + version.
        let a = SchemaRegistryGroupAssignment {
            error: 0,
            master: Some("crabka-d7c9d4c3-a778-465d-a069-954b68d772f9".into()),
            master_identity: Some(cp_master_identity()),
            version: 1,
        };
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            br#"{"error":0,"master":"crabka-d7c9d4c3-a778-465d-a069-954b68d772f9","master_identity":{"host":"sr-node-1","port":8081,"master_eligibility":true,"scheme":"http","version":1},"version":1}"#
                .to_vec()
        );
    }

    #[test]
    fn select_master_matches_cp() {
        // The two CAPTURED identities (both port 8081, hosts sr-node-1/2). cp
        // elected member `crabka-d7c9...` (host sr-node-1) — the URL-first
        // eligible member. Our comparator must pick the same one.
        let node1 = (
            "crabka-d7c9d4c3-a778-465d-a069-954b68d772f9".to_string(),
            SchemaRegistryIdentity {
                host: "sr-node-1".into(),
                port: 8081,
                master_eligibility: true,
                scheme: "http".into(),
                version: 1,
            },
        );
        let node2 = (
            "crabka-e0f909d7-bc38-4a07-8f00-84cf9fd71f17".to_string(),
            SchemaRegistryIdentity {
                host: "sr-node-2".into(),
                port: 8081,
                master_eligibility: true,
                scheme: "http".into(),
                version: 1,
            },
        );
        // Order-independent: cp's pick is stable regardless of member order.
        for set in [
            vec![node1.clone(), node2.clone()],
            vec![node2.clone(), node1.clone()],
        ] {
            let (mid, idn) = select_master(&set).expect("a master");
            assert_eq!(mid.as_str(), "crabka-d7c9d4c3-a778-465d-a069-954b68d772f9");
            assert_eq!(idn.host.as_str(), "sr-node-1");
        }
    }
}

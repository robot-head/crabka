//! Quota lookup with Kafka's 8-priority entity matching.

use crabka_metadata::{EntityKey, MetadataImage};

/// Return the configured value for `quota_key` under the most-specific
/// matching entity for `(principal, client_id)`. First match wins per
/// Kafka's documented precedence:
///   1. (client-id=app1, user=alice)
///   2. (client-id=app1, user=default)
///   3. (client-id=default, user=alice)
///   4. (client-id=default, user=default)
///   5. (user=alice)
///   6. (client-id=app1)
///   7. (user=default)
///   8. (client-id=default)
///
/// All candidate keys are pre-sorted by `entity_type` ("client-id" <
/// "user" alphabetically), so the lookup runs against the image map
/// without further canonicalization.
// Disjoint from `lookup_ip_quota` (which checks `("ip", *)` candidates only).
#[must_use]
pub fn lookup_quota(
    image: &MetadataImage,
    principal: &str,
    client_id: &str,
    quota_key: &str,
) -> Option<f64> {
    lookup_quota_with_key(image, principal, client_id, quota_key).map(|(_, v)| v)
}

/// Like `lookup_quota` but also returns the canonical entity key
/// that matched. Used by enforcement code to bind the lookup to a
/// bucket in `QuotaBuckets`.
#[must_use]
pub fn lookup_quota_with_key(
    image: &MetadataImage,
    principal: &str,
    client_id: &str,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let candidates: [EntityKey; 8] = [
        vec![
            ("client-id".into(), Some(client_id.into())),
            ("user".into(), Some(principal.into())),
        ],
        vec![
            ("client-id".into(), Some(client_id.into())),
            ("user".into(), None),
        ],
        vec![
            ("client-id".into(), None),
            ("user".into(), Some(principal.into())),
        ],
        vec![("client-id".into(), None), ("user".into(), None)],
        vec![("user".into(), Some(principal.into()))],
        vec![("client-id".into(), Some(client_id.into()))],
        vec![("user".into(), None)],
        vec![("client-id".into(), None)],
    ];
    for key in candidates {
        if let Some(configs) = image.client_quotas().get(&key)
            && let Some(&v) = configs.get(quota_key)
        {
            return Some((key, v));
        }
    }
    None
}

/// Lookup an `ip`-scoped quota for `peer_ip`. Priority order:
///   1. (ip = `Some(peer_ip)`) — specific
///   2. (ip = None)            — default
///
/// Disjoint from `lookup_quota` (which checks `("user", *)` and
/// `("client-id", *)` candidates only). Used by KIP-612
/// `connection_creation_rate` enforcement.
#[must_use]
pub fn lookup_ip_quota(
    image: &MetadataImage,
    peer_ip: &std::net::Ipv4Addr,
    quota_key: &str,
) -> Option<f64> {
    lookup_ip_quota_with_key(image, peer_ip, quota_key).map(|(_, v)| v)
}

#[must_use]
pub fn lookup_ip_quota_with_key(
    image: &MetadataImage,
    peer_ip: &std::net::Ipv4Addr,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let candidates: [EntityKey; 2] = [
        vec![("ip".into(), Some(peer_ip.to_string()))],
        vec![("ip".into(), None)],
    ];
    for key in candidates {
        if let Some(configs) = image.client_quotas().get(&key)
            && let Some(&v) = configs.get(quota_key)
        {
            return Some((key, v));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};

    fn img_with(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for r in records {
            img.apply(&MetadataRecord::V1ClientQuota(r));
        }
        img
    }

    fn rec(entity: Vec<(&str, Option<&str>)>, key: &str, value: f64) -> ClientQuotaRecord {
        ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|(t, n)| QuotaEntity {
                    entity_type: t.into(),
                    entity_name: n.map(Into::into),
                })
                .collect(),
            config_key: key.into(),
            config_value: Some(value),
        }
    }

    #[test]
    fn exact_user_client_pair_match() {
        let img = img_with(vec![rec(
            vec![("user", Some("alice")), ("client-id", Some("app1"))],
            "producer_byte_rate",
            1024.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(1024.0));
    }

    #[test]
    fn user_default_falls_back_to_client_specific() {
        // Only (client-id=app1) configured; user=alice should still match.
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate",
            1024.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(1024.0));
    }

    #[test]
    fn single_user_match_when_no_pair_exists() {
        let img = img_with(vec![rec(
            vec![("user", Some("alice"))],
            "producer_byte_rate",
            2048.0,
        )]);
        assert!(lookup_quota(&img, "alice", "anyclient", "producer_byte_rate") == Some(2048.0));
    }

    #[test]
    fn single_client_id_match_when_no_user_exists() {
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate",
            512.0,
        )]);
        assert!(lookup_quota(&img, "anyuser", "app1", "producer_byte_rate") == Some(512.0));
    }

    #[test]
    fn default_user_default_client_pair() {
        let img = img_with(vec![rec(
            vec![("user", None), ("client-id", None)],
            "producer_byte_rate",
            256.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(256.0));
    }

    #[test]
    fn default_user_alone() {
        let img = img_with(vec![rec(vec![("user", None)], "producer_byte_rate", 128.0)]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(128.0));
    }

    #[test]
    fn default_client_alone() {
        let img = img_with(vec![rec(
            vec![("client-id", None)],
            "producer_byte_rate",
            64.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(64.0));
    }

    #[test]
    fn no_match_returns_none() {
        let img = img_with(vec![]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == None);
    }

    #[test]
    fn pair_specific_wins_over_user_only() {
        let img = img_with(vec![
            rec(vec![("user", Some("alice"))], "producer_byte_rate", 8192.0),
            rec(
                vec![("user", Some("alice")), ("client-id", Some("app1"))],
                "producer_byte_rate",
                512.0,
            ),
        ]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(512.0));
    }

    fn rec_ip(ip: Option<&str>, key: &str, value: f64) -> ClientQuotaRecord {
        ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "ip".into(),
                entity_name: ip.map(Into::into),
            }],
            config_key: key.into(),
            config_value: Some(value),
        }
    }

    fn img_with_ip(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for r in records {
            img.apply(&MetadataRecord::V1ClientQuota(r));
        }
        img
    }

    #[test]
    fn ip_specific_match() {
        let img = img_with_ip(vec![rec_ip(
            Some("127.0.0.1"),
            "connection_creation_rate",
            1.0,
        )]);
        let ip: std::net::Ipv4Addr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, &ip, "connection_creation_rate") == Some(1.0));
    }

    #[test]
    fn ip_default_fallback() {
        let img = img_with_ip(vec![rec_ip(None, "connection_creation_rate", 2.0)]);
        let ip: std::net::Ipv4Addr = "10.0.0.7".parse().unwrap();
        assert!(lookup_ip_quota(&img, &ip, "connection_creation_rate") == Some(2.0));
    }

    #[test]
    fn ip_specific_wins_over_default() {
        let img = img_with_ip(vec![
            rec_ip(None, "connection_creation_rate", 8.0),
            rec_ip(Some("127.0.0.1"), "connection_creation_rate", 1.0),
        ]);
        let ip: std::net::Ipv4Addr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, &ip, "connection_creation_rate") == Some(1.0));
    }

    #[test]
    fn ip_no_match_returns_none() {
        let img = img_with_ip(vec![]);
        let ip: std::net::Ipv4Addr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, &ip, "connection_creation_rate").is_none());
    }
}

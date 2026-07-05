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
    first_matching_quota(image, candidates, quota_key)
}

/// Lookup an `ip`-scoped quota for `peer_ip`. Priority order:
///   1. (ip = `Some(peer_ip)`) — specific
///   2. (ip = None)            — default
///
/// Accepts both IPv4 and IPv6 peers: Kafka keys IP quotas by the IP's
/// string form for either family, so the same two-priority match applies.
///
/// Disjoint from `lookup_quota` (which checks `("user", *)` and
/// `("client-id", *)` candidates only). Used by KIP-612
/// `connection_creation_rate` enforcement.
#[must_use]
pub fn lookup_ip_quota(
    image: &MetadataImage,
    peer_ip: std::net::IpAddr,
    quota_key: &str,
) -> Option<f64> {
    lookup_ip_quota_with_key(image, peer_ip, quota_key).map(|(_, v)| v)
}

#[must_use]
pub fn lookup_ip_quota_with_key(
    image: &MetadataImage,
    peer_ip: std::net::IpAddr,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let candidates: [EntityKey; 2] = [
        vec![("ip".into(), Some(peer_ip.to_string()))],
        vec![("ip".into(), None)],
    ];
    first_matching_quota(image, candidates, quota_key)
}

fn first_matching_quota(
    image: &MetadataImage,
    candidates: impl IntoIterator<Item = EntityKey>,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
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
    use assert2::assert;
    use crabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};

    use super::*;

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
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(1.0));
    }

    #[test]
    fn ip_default_fallback() {
        let img = img_with_ip(vec![rec_ip(None, "connection_creation_rate", 2.0)]);
        let ip: std::net::IpAddr = "10.0.0.7".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(2.0));
    }

    #[test]
    fn ip_specific_wins_over_default() {
        let img = img_with_ip(vec![
            rec_ip(None, "connection_creation_rate", 8.0),
            rec_ip(Some("127.0.0.1"), "connection_creation_rate", 1.0),
        ]);
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(1.0));
    }

    #[test]
    fn ip_no_match_returns_none() {
        let img = img_with_ip(vec![]);
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate").is_none());
    }

    #[test]
    fn ipv6_specific_match() {
        // KIP-612: the connection-creation-rate quota must resolve for an
        // IPv6 peer keyed by its canonical string form, not just IPv4.
        let img = img_with_ip(vec![rec_ip(Some("::1"), "connection_creation_rate", 3.0)]);
        let ip: std::net::IpAddr = "::1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(3.0));
    }

    #[test]
    fn ipv6_default_fallback() {
        // An IPv6 peer with no specific entry falls back to the (ip=None)
        // default, proving IPv6 is no longer skipped by the quota path.
        let img = img_with_ip(vec![rec_ip(None, "connection_creation_rate", 5.0)]);
        let ip: std::net::IpAddr = "2001:db8::42".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(5.0));
    }

    // ── precedence verification: exhaustive enumeration + proptest ────────────
    //
    // The documented 8-priority (user/client) and 2-priority (IP) orders,
    // declared HERE independently of production so a reordering in
    // `lookup_quota_with_key`'s candidate array is caught. Index = priority
    // (lower wins). Parameterized by the probe `(principal, client_id)`.
    fn uc_candidates<'a>(
        principal: &'a str,
        client_id: &'a str,
    ) -> [Vec<(&'a str, Option<&'a str>)>; 8] {
        [
            vec![("client-id", Some(client_id)), ("user", Some(principal))],
            vec![("client-id", Some(client_id)), ("user", None)],
            vec![("client-id", None), ("user", Some(principal))],
            vec![("client-id", None), ("user", None)],
            vec![("user", Some(principal))],
            vec![("client-id", Some(client_id))],
            vec![("user", None)],
            vec![("client-id", None)],
        ]
    }

    /// Distinct per-candidate sentinel values (index = priority) so the matched
    /// candidate is identifiable by value, with no int→float cast.
    const CAND_VALS: [f64; 8] = [
        1000.0, 1001.0, 1002.0, 1003.0, 1004.0, 1005.0, 1006.0, 1007.0,
    ];

    /// Exhaustive: every one of the 2^8 presence configs of the 8 candidates
    /// (fixed probe) resolves to the minimum-index present candidate, with the
    /// matching value; empty config → `None`. Each candidate is configured with
    /// a distinct value (`1000 + i`) so the matched candidate is identifiable.
    #[test]
    fn quota_precedence_exhaustive() {
        let cands = uc_candidates("u", "c");
        for mask in 0u16..256 {
            let records: Vec<_> = cands
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(i, c)| rec(c.clone(), "k", CAND_VALS[i]))
                .collect();
            let img = img_with(records);
            let got = lookup_quota_with_key(&img, "u", "c", "k");
            match (0..8usize).find(|i| mask & (1 << i) != 0) {
                None => assert!(
                    got.is_none(),
                    "mask {mask:#010b}: expected None, got {got:?}"
                ),
                Some(j) => {
                    let (_key, val) = got.expect("a candidate is present");
                    // Exact sentinel values, stored and retrieved verbatim —
                    // compare bit patterns to sidestep float_cmp.
                    assert!(
                        val.to_bits() == CAND_VALS[j].to_bits(),
                        "mask {mask:#010b}: expected candidate {j} (value {}), got {val}",
                        CAND_VALS[j]
                    );
                }
            }
            // A quota_key no candidate carries never resolves.
            assert!(lookup_quota_with_key(&img, "u", "c", "absent_key").is_none());
        }
    }

    /// Exhaustive: the 2-priority IP order (specific beats default).
    #[test]
    fn ip_quota_precedence_exhaustive() {
        let ip: std::net::IpAddr = "10.1.2.3".parse().unwrap();
        let cands = [vec![("ip", Some("10.1.2.3"))], vec![("ip", None)]];
        for mask in 0u8..4 {
            let records: Vec<_> = cands
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(i, c)| rec(c.clone(), "connection_creation_rate", CAND_VALS[i]))
                .collect();
            let img = img_with(records);
            let got = lookup_ip_quota_with_key(&img, ip, "connection_creation_rate");
            match (0..2usize).find(|i| mask & (1 << i) != 0) {
                None => assert!(got.is_none(), "mask {mask:#04b}: expected None"),
                Some(j) => {
                    assert!(got.expect("present").1.to_bits() == CAND_VALS[j].to_bits());
                }
            }
        }
    }

    proptest::proptest! {
        /// Random probes + random candidate subsets + a non-matching decoy:
        /// the min-index present candidate wins, the decoy is never returned,
        /// and the user/client path never returns an IP entity.
        #[test]
        fn quota_precedence_random(
            principal in "[uv][12]",
            client_id in "[cd][12]",
            qkey_idx in 0usize..2,
            present in proptest::collection::vec(proptest::bool::ANY, 8),
            decoy in proptest::bool::ANY,
        ) {
            let qkey = ["producer_byte_rate", "consumer_byte_rate"][qkey_idx];
            let cands = uc_candidates(&principal, &client_id);
            let mut records: Vec<_> = cands
                .iter()
                .enumerate()
                .filter(|(i, _)| present[*i])
                .map(|(i, c)| rec(c.clone(), qkey, CAND_VALS[i]))
                .collect();
            if decoy {
                // Non-matching entity (never a candidate for a [uv][12]/[cd][12]
                // probe) — must never be returned.
                records.push(rec(
                    vec![("client-id", Some("ZZZ")), ("user", Some("ZZZ"))],
                    qkey,
                    9999.0,
                ));
                records.push(rec(vec![("user", Some("ZZZ"))], qkey, 9998.0));
            }
            let img = img_with(records);
            let got = lookup_quota_with_key(&img, &principal, &client_id, qkey);
            match (0..8usize).find(|i| present[*i]) {
                None => proptest::prop_assert!(got.is_none(), "no candidate present, got {got:?}"),
                Some(j) => {
                    let (_k, v) = got.expect("a candidate is present");
                    proptest::prop_assert_eq!(v.to_bits(), CAND_VALS[j].to_bits());
                }
            }
        }

        /// Random IP precedence: specific beats default; the IP path never
        /// returns a user/client entity.
        #[test]
        fn ip_precedence_random(specific in proptest::bool::ANY, default in proptest::bool::ANY) {
            let ip: std::net::IpAddr = "10.9.8.7".parse().unwrap();
            let mut records = vec![];
            if specific {
                records.push(rec(vec![("ip", Some("10.9.8.7"))], "connection_creation_rate", 1.0));
            }
            if default {
                records.push(rec(vec![("ip", None)], "connection_creation_rate", 2.0));
            }
            // A user/client entry must not leak into the IP path.
            records.push(rec(vec![("user", Some("u"))], "connection_creation_rate", 50.0));
            let img = img_with(records);
            let got = lookup_ip_quota_with_key(&img, ip, "connection_creation_rate");
            if specific {
                proptest::prop_assert_eq!(got.expect("specific present").1.to_bits(), 1.0_f64.to_bits());
            } else if default {
                proptest::prop_assert_eq!(got.expect("default present").1.to_bits(), 2.0_f64.to_bits());
            } else {
                proptest::prop_assert!(got.is_none());
            }
        }
    }
}

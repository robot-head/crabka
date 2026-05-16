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

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(
            lookup_quota(&img, "alice", "app1", "producer_byte_rate"),
            Some(1024.0)
        );
    }

    #[test]
    fn user_default_falls_back_to_client_specific() {
        // Only (client-id=app1) configured; user=alice should still match.
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate",
            1024.0,
        )]);
        assert_eq!(
            lookup_quota(&img, "alice", "app1", "producer_byte_rate"),
            Some(1024.0)
        );
    }

    #[test]
    fn single_user_match_when_no_pair_exists() {
        let img = img_with(vec![rec(
            vec![("user", Some("alice"))],
            "producer_byte_rate",
            2048.0,
        )]);
        assert_eq!(
            lookup_quota(&img, "alice", "anyclient", "producer_byte_rate"),
            Some(2048.0)
        );
    }

    #[test]
    fn single_client_id_match_when_no_user_exists() {
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate",
            512.0,
        )]);
        assert_eq!(
            lookup_quota(&img, "anyuser", "app1", "producer_byte_rate"),
            Some(512.0)
        );
    }

    #[test]
    fn default_user_default_client_pair() {
        let img = img_with(vec![rec(
            vec![("user", None), ("client-id", None)],
            "producer_byte_rate",
            256.0,
        )]);
        assert_eq!(
            lookup_quota(&img, "alice", "app1", "producer_byte_rate"),
            Some(256.0)
        );
    }

    #[test]
    fn default_user_alone() {
        let img = img_with(vec![rec(vec![("user", None)], "producer_byte_rate", 128.0)]);
        assert_eq!(
            lookup_quota(&img, "alice", "app1", "producer_byte_rate"),
            Some(128.0)
        );
    }

    #[test]
    fn default_client_alone() {
        let img = img_with(vec![rec(
            vec![("client-id", None)],
            "producer_byte_rate",
            64.0,
        )]);
        assert_eq!(
            lookup_quota(&img, "alice", "app1", "producer_byte_rate"),
            Some(64.0)
        );
    }

    #[test]
    fn no_match_returns_none() {
        let img = img_with(vec![]);
        assert_eq!(
            lookup_quota(&img, "alice", "app1", "producer_byte_rate"),
            None
        );
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
        assert_eq!(
            lookup_quota(&img, "alice", "app1", "producer_byte_rate"),
            Some(512.0)
        );
    }
}

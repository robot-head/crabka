//! KIP-48: delegation-token RPCs on `AdminClient`.
//!
//! Four operations the `KafkaUser` (`authentication.type: delegation-token`)
//! reconciler drives via the cluster's admin connection:
//! `CreateDelegationToken` (`api_key` 38) with the act-as owner field
//! populated, `RenewDelegationToken` (39), `ExpireDelegationToken` (40),
//! and `DescribeDelegationToken` (41) filtered to a single owner.
//!
//! Like `users.rs`, this module keeps wire concerns local: requests are
//! built via small `build_*` helpers, responses are mapped via
//! `parse_*` helpers, and unit tests cover each end of the pipe.

use bytes::Bytes;
use crabka_metadata::DelegationToken;
use crabka_protocol::owned::{
    create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
    create_delegation_token_response::CreateDelegationTokenResponse,
    describe_delegation_token_request::{
        DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
    },
    describe_delegation_token_response::{
        DescribeDelegationTokenResponse, DescribedDelegationToken,
    },
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    renew_delegation_token_request::RenewDelegationTokenRequest,
};
use crabka_security::KafkaPrincipal;

use crate::{AdminClient, AdminError, kafka_error_name};

impl AdminClient {
    /// KIP-48 act-as create: mint a delegation token whose owner is
    /// `owner_principal_name` (type always `"User"`).
    ///
    /// The caller MUST be a broker super-user (per broker
    /// semantics) for the act-as path to take effect; non-super callers
    /// get `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65). The full
    /// response is returned so callers can pluck out `token_id`, `hmac`,
    /// and the issue/expiry/max timestamps for downstream persistence.
    ///
    /// `renewers` items are `"User:bob"` form; entries without a `:`
    /// are interpreted with type `"User"`.
    pub async fn create_delegation_token_as_owner(
        &mut self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime_ms: i64,
    ) -> Result<CreateDelegationTokenResponse, AdminError> {
        let req = build_create_delegation_token(owner_principal_name, renewers, max_lifetime_ms);
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(broker_err("CreateDelegationToken", resp.error_code, None));
        }
        Ok(resp)
    }

    /// KIP-48 renew: bump the token's `expiry_timestamp_ms` capped by
    /// `max_timestamp_ms`. `renew_period_ms = -1` tells the broker to
    /// use its configured default. Returns the new expiry.
    pub async fn renew_delegation_token(&mut self, hmac: &[u8]) -> Result<i64, AdminError> {
        let req = build_renew_delegation_token(hmac);
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(broker_err("RenewDelegationToken", resp.error_code, None));
        }
        Ok(resp.expiry_timestamp_ms)
    }

    /// KIP-48 expire: tombstone the token immediately
    /// (`expiry_time_period_ms = -1`).
    pub async fn expire_delegation_token(&mut self, hmac: &[u8]) -> Result<(), AdminError> {
        let req = build_expire_delegation_token(hmac);
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(broker_err("ExpireDelegationToken", resp.error_code, None));
        }
        Ok(())
    }

    /// KIP-48 describe filtered to a single owner. `owner_principal` is
    /// a canonical `"Type:Name"` string (e.g. `"User:alice"`); entries
    /// without a `:` default to type `"User"`.
    pub async fn describe_delegation_tokens_owned_by(
        &mut self,
        owner_principal: &str,
    ) -> Result<Vec<DelegationToken>, AdminError> {
        let req = build_describe_owner_filter(owner_principal);
        let resp = self.conn.send(req).await?;
        parse_describe_delegation_tokens(resp)
    }
}

/// Pure: build a `CreateDelegationTokenRequest` with the act-as owner
/// fields populated and renewer strings split into the wire principal
/// pair (`User` default for items lacking a `:`).
fn build_create_delegation_token(
    owner_principal_name: &str,
    renewers: &[String],
    max_lifetime_ms: i64,
) -> CreateDelegationTokenRequest {
    CreateDelegationTokenRequest {
        owner_principal_type: Some("User".into()),
        owner_principal_name: Some(owner_principal_name.into()),
        renewers: renewers
            .iter()
            .map(|s| renewer_str_to_wire(s.as_str()))
            .collect(),
        max_lifetime_ms,
        ..Default::default()
    }
}

/// Pure: build a `RenewDelegationTokenRequest`. `renew_period_ms = -1`
/// signals "use broker default" per KIP-48.
fn build_renew_delegation_token(hmac: &[u8]) -> RenewDelegationTokenRequest {
    RenewDelegationTokenRequest {
        hmac: Bytes::copy_from_slice(hmac),
        renew_period_ms: -1,
        ..Default::default()
    }
}

/// Pure: build an `ExpireDelegationTokenRequest`.
/// `expiry_time_period_ms = -1` requests immediate tombstoning.
fn build_expire_delegation_token(hmac: &[u8]) -> ExpireDelegationTokenRequest {
    ExpireDelegationTokenRequest {
        hmac: Bytes::copy_from_slice(hmac),
        expiry_time_period_ms: -1,
        ..Default::default()
    }
}

/// Pure: split `"Type:Name"` (default type `User`) into a wire
/// `CreatableRenewers`.
fn renewer_str_to_wire(s: &str) -> CreatableRenewers {
    let (pt, pn) = s.split_once(':').unwrap_or(("User", s));
    CreatableRenewers {
        principal_type: pt.to_string(),
        principal_name: pn.to_string(),
        ..Default::default()
    }
}

/// Pure: build a `DescribeDelegationTokenRequest` whose `owners` filter
/// is a single-entry list for the given `"Type:Name"` principal.
fn build_describe_owner_filter(owner_principal: &str) -> DescribeDelegationTokenRequest {
    let (pt, pn) = owner_principal
        .split_once(':')
        .unwrap_or(("User", owner_principal));
    DescribeDelegationTokenRequest {
        owners: Some(vec![DescribeDelegationTokenOwner {
            principal_type: pt.to_string(),
            principal_name: pn.to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

/// Pure: response → `Vec<DelegationToken>` (the in-memory image type).
/// Maps an `error_code != 0` to `AdminError::Broker` so callers get
/// the same Kafka-error surface that the other admin methods use.
fn parse_describe_delegation_tokens(
    resp: DescribeDelegationTokenResponse,
) -> Result<Vec<DelegationToken>, AdminError> {
    if resp.error_code != 0 {
        return Err(broker_err("DescribeDelegationToken", resp.error_code, None));
    }
    Ok(resp.tokens.into_iter().map(described_to_image).collect())
}

/// Pure: wire `DescribedDelegationToken` → `crabka_metadata::DelegationToken`.
///
/// Field rename notes:
/// - wire `issue_timestamp` → image `issue_timestamp_ms`
/// - wire `expiry_timestamp` → image `expiry_timestamp_ms`
/// - wire `max_timestamp` → image `max_timestamp_ms`
/// - wire `hmac: bytes::Bytes` → image `hmac: Vec<u8>`
/// - wire `renewers: Vec<DescribedDelegationTokenRenewer>` →
///   image `renewers: Vec<KafkaPrincipal>`
fn described_to_image(t: DescribedDelegationToken) -> DelegationToken {
    DelegationToken {
        token_id: t.token_id,
        owner: KafkaPrincipal {
            principal_type: t.principal_type,
            name: t.principal_name,
        },
        hmac: t.hmac.to_vec(),
        issue_timestamp_ms: t.issue_timestamp,
        expiry_timestamp_ms: t.expiry_timestamp,
        max_timestamp_ms: t.max_timestamp,
        renewers: t
            .renewers
            .into_iter()
            .map(|r| KafkaPrincipal {
                principal_type: r.principal_type,
                name: r.principal_name,
            })
            .collect(),
    }
}

fn broker_err(api: &'static str, code: i16, message: Option<String>) -> AdminError {
    AdminError::Broker {
        api,
        code,
        name: kafka_error_name(code),
        message,
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::Bytes;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::describe_delegation_token_response::{
            DescribeDelegationTokenResponse, DescribedDelegationToken,
            DescribedDelegationTokenRenewer,
        },
    };

    use super::*;

    // ── build_create_delegation_token ─────────────────────────────────
    //
    // The act-as wire contract: `owner_principal_type` must be `"User"`
    // and `owner_principal_name` must carry the target user, so the
    // broker's act-as branch fires (otherwise the broker falls back to
    // self-mint and the caller's principal owns the token). Renewer
    // strings are split on the first `:`; bare names default to
    // `principal_type = "User"`.

    #[test]
    fn build_create_populates_act_as_owner_and_renewers() {
        let req = build_create_delegation_token(
            "alice",
            &["User:bob".to_string(), "carol".to_string()],
            60_000,
        );
        assert!(
            req == CreateDelegationTokenRequest {
                owner_principal_type: Some("User".to_string()),
                owner_principal_name: Some("alice".to_string()),
                renewers: vec![
                    // "User:bob" → type=User, name=bob
                    CreatableRenewers {
                        principal_type: "User".to_string(),
                        principal_name: "bob".to_string(),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    // "carol" → default type=User, name=carol
                    CreatableRenewers {
                        principal_type: "User".to_string(),
                        principal_name: "carol".to_string(),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                ],
                max_lifetime_ms: 60_000,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    // ── build_renew_delegation_token / build_expire_delegation_token ─

    /// Renew uses the broker default by passing `renew_period_ms = -1`;
    /// the hmac is copied verbatim into the wire bytes field.
    #[test]
    fn build_renew_uses_minus_one_for_broker_default() {
        let req = build_renew_delegation_token(b"\x01\x02\x03");
        assert!((req.hmac.as_ref(), req.renew_period_ms) == (&[0x01, 0x02, 0x03][..], -1));
    }

    /// Expire-immediately is signaled by `expiry_time_period_ms = -1`;
    /// the hmac is copied verbatim.
    #[test]
    fn build_expire_uses_minus_one_for_immediate_tombstone() {
        let req = build_expire_delegation_token(b"\xaa\xbb");
        assert!((req.hmac.as_ref(), req.expiry_time_period_ms) == (&[0xaa, 0xbb][..], -1));
    }

    // ── build_describe_owner_filter ──────────────────────────────────

    #[test]
    fn build_describe_owner_filter_sets_single_entry() {
        for (name, owner, principal_name) in [
            ("typed owner", "User:alice", "alice"),
            ("bare owner defaults to User", "solo", "solo"),
        ] {
            let req = build_describe_owner_filter(owner);
            assert!(
                req == DescribeDelegationTokenRequest {
                    owners: Some(vec![DescribeDelegationTokenOwner {
                        principal_type: "User".to_string(),
                        principal_name: principal_name.to_string(),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }]),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                },
                "case {name}"
            );
        }
    }

    // ── parse_describe_delegation_tokens ─────────────────────────────

    #[test]
    fn parse_describe_maps_wire_fields_to_image() {
        let resp = DescribeDelegationTokenResponse {
            error_code: 0,
            tokens: vec![DescribedDelegationToken {
                principal_type: "User".into(),
                principal_name: "alice".into(),
                // act-as: token_requester != owner — but the image type
                // tracks only the owner; requester is not surfaced.
                token_requester_principal_type: "User".into(),
                token_requester_principal_name: "operator".into(),
                issue_timestamp: 1_000,
                expiry_timestamp: 2_000,
                max_timestamp: 9_000,
                token_id: "tok-1".into(),
                hmac: Bytes::from_static(b"\xde\xad\xbe\xef"),
                renewers: vec![DescribedDelegationTokenRenewer {
                    principal_type: "User".into(),
                    principal_name: "bob".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = parse_describe_delegation_tokens(resp).expect("ok response");
        assert!(
            out == vec![DelegationToken {
                token_id: "tok-1".to_string(),
                owner: KafkaPrincipal {
                    principal_type: "User".to_string(),
                    name: "alice".to_string(),
                },
                hmac: vec![0xde, 0xad, 0xbe, 0xef],
                issue_timestamp_ms: 1_000,
                expiry_timestamp_ms: 2_000,
                max_timestamp_ms: 9_000,
                renewers: vec![KafkaPrincipal {
                    principal_type: "User".to_string(),
                    name: "bob".to_string(),
                }],
            }]
        );
    }

    // ── broker_err ─────────────────────────────────────────────────────
    //
    // The pure error-construction helper shared by all four methods.
    // A non-zero `error_code` surfaces as `AdminError::Broker` carrying
    // the Kafka-error name string the reconciler may inspect.

    /// Spec-style coverage for the non-zero-error path on
    /// `create_delegation_token_as_owner` (and by symmetry the other
    /// three methods, which use the same `broker_err` helper): a
    /// `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65) response is mapped
    /// to `AdminError::Broker { code: 65, .. }`.
    #[test]
    fn broker_err_carries_api_and_kafka_code_name() {
        let e = broker_err("CreateDelegationToken", 65, Some("not super-user".into()));
        match e {
            AdminError::Broker {
                api,
                code,
                name,
                message,
            } => {
                // 65 is not in `kafka_error_name`'s match table yet, so
                // it falls through to "UNKNOWN" — locking that behavior
                // so a later edit to the table doesn't silently change
                // the surfaced name.
                check!(
                    (api, code, name, message.as_deref())
                        == (
                            "CreateDelegationToken",
                            65,
                            "UNKNOWN",
                            Some("not super-user")
                        )
                );
            }
            other => panic!("expected AdminError::Broker, got {other:?}"),
        }
    }

    /// Non-zero `error_code` on the top-level response surfaces as
    /// `AdminError::Broker` so the reconciler can branch on the Kafka
    /// error code (e.g. `DELEGATION_TOKEN_AUTH_DISABLED = 61`).
    #[test]
    fn parse_describe_propagates_nonzero_error_code() {
        let resp = DescribeDelegationTokenResponse {
            error_code: 61, // DELEGATION_TOKEN_AUTH_DISABLED
            ..Default::default()
        };
        let err = parse_describe_delegation_tokens(resp).unwrap_err();
        match err {
            AdminError::Broker { api, code, .. } => {
                assert!((api, code) == ("DescribeDelegationToken", 61));
            }
            other => panic!("expected AdminError::Broker, got {other:?}"),
        }
    }
}

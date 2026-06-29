//! KIP-48: `CreateDelegationToken` (`api_key` 38).
//!
//! Per spec §1.2 (including act-as): caller must be
//! SASL-authenticated and NOT itself authenticated via a delegation
//! token (KIP-48 forbids token-creating-token chains). Owner resolution:
//!
//! - If both `owner_principal_type` and `owner_principal_name` are
//!   empty/absent: owner = caller (self-mint).
//! - If both are present + non-empty: caller must be a configured
//!   super-user (per `broker.config.super_users`), and the owner becomes
//!   the wire-specified `KafkaPrincipal`. The type is restricted
//!   to `"User"` (mTLS-DN owners are not supported). Non-super-users get
//!   `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65).
//! - If exactly one is set: `INVALID_REQUEST` (42) — partial act-as is
//!   never valid.
//!
//! The HMAC-SHA-256 of `(secret_key, token_id)` becomes the token's
//! "password equivalent" — clients re-authenticate with the hex
//! `token_id` as the SCRAM username and the HMAC bytes as the password.

use std::collections::HashSet;
use std::hash::BuildHasher;

use crabka_metadata::{DelegationTokenRecord, MetadataRecord};
use crabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest;
use crabka_protocol::owned::create_delegation_token_response::CreateDelegationTokenResponse;
use crabka_security::{KafkaPrincipal, SecretBytes};

use crate::network::auth::ConnectionAuth;
use crate::time_util::now_ms;

/// Wire convention: the JVM admin client serialises "not act-as" by
/// either omitting the compact-nullable string (`None`) or sending an
/// empty string. Treat both as "absent" so the act-as branch only fires
/// when the caller actually supplied a principal.
fn is_empty_owner_field(f: Option<&str>) -> bool {
    f.is_none_or(str::is_empty)
}

#[tracing::instrument(
    name = "handle_create_delegation_token",
    level = "info",
    skip_all,
    fields(api = "CreateDelegationToken")
)]
pub(crate) async fn handle<S: BuildHasher>(
    req: &CreateDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    max_lifetime_ms: i64,
    default_renew_period_ms: i64,
    controller: &dyn crate::metadata_source::MetadataSource,
    super_users: &HashSet<String, S>,
) -> CreateDelegationTokenResponse {
    let Some(secret_key) = secret_key else {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    };

    let ConnectionAuth::Authenticated {
        principal,
        authenticated_via_token,
        ..
    } = auth
    else {
        return err_response(crate::codes::INVALID_REQUEST);
    };
    if *authenticated_via_token {
        // KIP-48: a delegation-token-authed caller cannot create more
        // delegation tokens (no token-creating-token chains).
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    }

    let image = controller.current_image();
    // KIP-48/KIP-778: KRaft delegation tokens require metadata.version >= 3.6-IV2.
    if crate::features::require_feature(
        &image,
        crate::features::METADATA_VERSION,
        crabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    )
    .is_err()
    {
        return err_response(crate::codes::UNSUPPORTED_VERSION);
    }

    // KIP-48 owner resolution. The wire `owner_principal_type/name`
    // pair drives the privileged "act-as" path: super-users may mint
    // tokens owned by *other* principals so an operator can pre-mint
    // tokens for KafkaUsers without first holding their credentials.
    let owner_type_empty = is_empty_owner_field(req.owner_principal_type.as_deref());
    let owner_name_empty = is_empty_owner_field(req.owner_principal_name.as_deref());
    let owner = match (owner_type_empty, owner_name_empty) {
        (true, true) => principal.to_kafka(),
        (false, false) => {
            // Both set → act-as. Only super-users may use this path; the
            // permission is broker-wide because no token exists yet to
            // hang an ACL on.
            if !super_users.contains(&principal.name) {
                return err_response(crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
            }
            let owner_type = req.owner_principal_type.as_deref().unwrap_or_default();
            let owner_name = req.owner_principal_name.as_deref().unwrap_or_default();
            // The act-as owner type is restricted to `User`
            // (mTLS-DN owners are not supported). Match Kafka's behavior of
            // returning INVALID_REQUEST for unsupported types here
            // rather than authorization-failed — the request is
            // syntactically wrong, not unauthorized.
            if owner_type != "User" {
                return err_response(crate::codes::INVALID_REQUEST);
            }
            KafkaPrincipal {
                principal_type: owner_type.to_string(),
                name: owner_name.to_string(),
            }
        }
        // Exactly one set → caller is confused; either both or neither.
        _ => return err_response(crate::codes::INVALID_REQUEST),
    };

    // Validate + clamp `max_lifetime_ms`. `-1` defers to the broker
    // ceiling; a positive value is clamped to the ceiling; anything
    // else is invalid (zero or non-`-1` negatives).
    let chosen_lifetime = match req.max_lifetime_ms {
        -1 => max_lifetime_ms,
        n if n > 0 => n.min(max_lifetime_ms),
        _ => return err_response(crate::codes::INVALID_REQUEST),
    };

    let now = now_ms();
    let token_id = uuid::Uuid::new_v4().to_string();
    let hmac = crabka_security::compute_token_hmac(secret_key.as_bytes(), &token_id);

    let renewers: Vec<KafkaPrincipal> = req
        .renewers
        .iter()
        .map(|r| KafkaPrincipal {
            principal_type: r.principal_type.clone(),
            name: r.principal_name.clone(),
        })
        .collect();

    // KIP-48 (matches org.apache.kafka.metadata.security.DelegationTokenManager):
    // `max_timestamp_ms` is the absolute upper bound on the token's lifetime
    // — `Renew` may never push expiry past it. `expiry_timestamp_ms` is the
    // initial "next renewal due" instant, computed as `now + default_renew_period`
    // clamped down so a tiny `chosen_lifetime` never produces an `expiry >
    // max`. The two values are deliberately separate so that the typical
    // case (7-day ceiling, 24h renew window) leaves room for `Renew` to
    // actually extend `expiry_timestamp_ms` up to `max_timestamp_ms`.
    let max_timestamp_ms = now + chosen_lifetime;
    let initial_expiry_ms = now + default_renew_period_ms.min(chosen_lifetime);

    let record = DelegationTokenRecord {
        token_id: token_id.clone(),
        owner: owner.clone(),
        hmac: hmac.clone(),
        issue_timestamp_ms: now,
        expiry_timestamp_ms: initial_expiry_ms,
        max_timestamp_ms,
        renewers,
    };

    if let Err(e) = controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(record)])
        .await
    {
        tracing::warn!(error = %e, "CreateDelegationToken: submit_change failed");
        return err_response(crate::codes::INVALID_REQUEST);
    }

    // Always populate `token_requester_*` with the caller's principal.
    // On self-mint this equals the owner; on act-as it identifies the
    // super-user who minted on behalf of `owner`. Matches Kafka's
    // `DelegationTokenManager.createDelegationToken` (the JVM admin CLI
    // displays both columns unconditionally).
    let caller = principal.to_kafka();
    let (requester_type, requester_name) = (caller.principal_type, caller.name);

    CreateDelegationTokenResponse {
        principal_type: owner.principal_type.clone(),
        principal_name: owner.name.clone(),
        token_requester_principal_type: requester_type,
        token_requester_principal_name: requester_name,
        issue_timestamp_ms: now,
        expiry_timestamp_ms: initial_expiry_ms,
        max_timestamp_ms,
        token_id,
        hmac: bytes::Bytes::from(hmac),
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn err_response(code: i16) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_raft::ControllerHandle;
    use crabka_security::{AuthMethod, Principal, SaslMechanism};
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Helper: produce an empty super-users set for tests that don't
    /// exercise the act-as path.
    fn empty_super_users() -> HashSet<String> {
        HashSet::new()
    }

    /// Helper: produce a super-users set containing the given names.
    fn super_users_with(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    /// Spin up a single-voter `Controller` for tests, wait for leader.
    async fn test_controller(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
        let cfg = crabka_raft::ControllerConfig {
            election_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
            client_id: "test".into(),
            ..crabka_raft::ControllerConfig::for_tests(1, log_dir)
        };
        let handle = Arc::new(crabka_raft::Controller::start(cfg).await.unwrap());
        let mut rx = handle.watch_leader();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while rx.borrow().is_none() {
            assert!(std::time::Instant::now() < deadline, "no leader in 5s");
            let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
        }
        handle
    }

    fn authed_with_token(name: &str, via_token: bool) -> ConnectionAuth {
        ConnectionAuth::Authenticated {
            principal: Principal {
                name: name.into(),
                auth_method: AuthMethod::SaslScramSha256,
                groups: vec![],
            },
            mechanism: SaslMechanism::ScramSha256,
            expires_at_ms: None,
            authenticated_via_token: via_token,
        }
    }

    fn authed(name: &str) -> ConnectionAuth {
        authed_with_token(name, false)
    }

    /// KIP-48 24h default — matches Kafka's `delegation.token.expiry.time.ms`.
    /// Tests that don't care about renew-period clamping pass this value.
    const RENEW_24H_MS: i64 = 24 * 60 * 60 * 1_000;

    #[tokio::test]
    async fn returns_auth_disabled_when_no_secret_key() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let req = CreateDelegationTokenRequest::default();
        let auth = authed("alice");
        let resp = handle(
            &req,
            &auth,
            None,
            1_000,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn success_returns_token_id_and_hmac() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"master-key".to_vec());
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        };
        // Broker ceiling 60s; default renew period 24h. KIP-48: the renew
        // period is clamped down to chosen_lifetime when smaller, so for
        // this 60s-ceiling case expiry == max == issue + 60s.
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(resp.principal_type == "User");
        assert!(resp.principal_name == "alice");
        assert!(!resp.token_id.is_empty(), "token_id should be non-empty");
        // HMAC-SHA-256 output is 32 bytes; the response carries them raw.
        assert!(resp.hmac.len() == 32);
        // 60s ceiling < 24h default renew period → both timestamps collapse
        // to issue + 60s (the chosen_lifetime ceiling).
        assert!(resp.expiry_timestamp_ms - resp.issue_timestamp_ms == 60_000);
        assert!(resp.max_timestamp_ms == resp.expiry_timestamp_ms);
        // Persisted in image with the same hmac + owner + timestamps.
        let img = controller.current_image();
        let stored = img
            .delegation_token_by_id(&resp.token_id)
            .expect("token in image");
        assert!(stored.hmac.as_slice() == &resp.hmac[..]);
        assert!(stored.owner.principal_type == "User");
        assert!(stored.owner.name == "alice");
        assert!(stored.expiry_timestamp_ms == resp.expiry_timestamp_ms);
        assert!(stored.max_timestamp_ms == resp.max_timestamp_ms);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn token_authenticated_caller_is_rejected_with_request_not_allowed() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed_with_token("alice", true),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn max_lifetime_is_clamped_to_config_ceiling() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        // Caller requests 1 hour; broker ceiling is 5 minutes.
        let ceiling_ms = 5 * 60 * 1_000;
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: 60 * 60 * 1_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            ceiling_ms,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        // 5-minute ceiling < 24h default renew period → both timestamps
        // collapse to issue + ceiling.
        let max_offset = resp.max_timestamp_ms - resp.issue_timestamp_ms;
        let expiry_offset = resp.expiry_timestamp_ms - resp.issue_timestamp_ms;
        assert!(max_offset == ceiling_ms);
        assert!(expiry_offset == ceiling_ms);
        assert!(resp.max_timestamp_ms == resp.expiry_timestamp_ms);
        controller.cancel().await;
    }

    /// KIP-48 separates `expiry_timestamp_ms` (initially `issue + min(default_renew,
    /// chosen_lifetime)`) from `max_timestamp_ms` (`issue + chosen_lifetime`),
    /// so `Renew` can extend the former up to the latter rather than round-tripping
    /// exactly. This test pins both branches of the `min`.
    #[tokio::test]
    async fn initial_expiry_is_default_renew_period_clamped_by_max_lifetime() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());

        // Branch 1: max_lifetime_ms = 1h, default_renew_period_ms = 24h.
        // The renew period is clamped *down* to chosen_lifetime, so both
        // timestamps collapse to issue + 1h.
        let one_hour = 60 * 60 * 1_000;
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            one_hour,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(
            resp.expiry_timestamp_ms - resp.issue_timestamp_ms == one_hour,
            "1h ceiling clamps the 24h renew period; expiry must == issue + 1h"
        );
        assert!(
            resp.max_timestamp_ms - resp.issue_timestamp_ms == one_hour,
            "max must equal issue + chosen_lifetime"
        );
        assert!(
            resp.expiry_timestamp_ms == resp.max_timestamp_ms,
            "expiry must collapse to max when renew period > chosen_lifetime"
        );

        // Branch 2: max_lifetime_ms = 7d, default_renew_period_ms = 24h.
        // Now the renew period is the smaller of the two, so expiry =
        // issue + 24h and max = issue + 7d — they are SEPARATE, leaving
        // room for Renew to extend expiry up to max.
        let seven_days = 7 * 24 * 60 * 60 * 1_000;
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            seven_days,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(
            resp.expiry_timestamp_ms - resp.issue_timestamp_ms == RENEW_24H_MS,
            "24h renew period < 7d ceiling, so expiry must == issue + 24h"
        );
        assert!(
            resp.max_timestamp_ms - resp.issue_timestamp_ms == seven_days,
            "max must == issue + 7d (the ceiling, untouched)"
        );
        assert!(
            resp.expiry_timestamp_ms < resp.max_timestamp_ms,
            "expiry and max must be SEPARATE so Renew has room to extend",
        );

        controller.cancel().await;
    }

    #[tokio::test]
    async fn invalid_lifetime_returns_invalid_request() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        // Zero is invalid (only `-1` selects the default).
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: 0,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::INVALID_REQUEST);
        controller.cancel().await;
    }

    /// Spec §1.2/§1.4: a super-user caller may mint a token owned by a
    /// different principal by setting `owner_principal_type/name`. The
    /// response advertises the owner *and* records the original caller
    /// in the `token_requester_*` fields so the JVM admin CLI can show
    /// "minted by X on behalf of Y".
    #[tokio::test]
    async fn act_as_super_user_sets_specified_owner() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("alice".to_string()),
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("admin"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == 0);
        // Owner = the act-as target.
        assert!(resp.principal_type == "User");
        assert!(resp.principal_name == "alice");
        // Requester = the caller (admin), set for act-as mints.
        assert!(resp.token_requester_principal_type == "User");
        assert!(resp.token_requester_principal_name == "admin");
        // Persisted owner matches the response owner.
        let img = controller.current_image();
        let stored = img
            .delegation_token_by_id(&resp.token_id)
            .expect("token in image");
        assert!(stored.owner.principal_type == "User");
        assert!(stored.owner.name == "alice");
        controller.cancel().await;
    }

    /// Spec §1.2: act-as is privileged. A caller who is NOT in
    /// `super_users` attempting act-as gets `DELEGATION_TOKEN_AUTHORIZATION_FAILED`
    /// (65) — the broker explicitly distinguishes "you are not allowed
    /// to do this" (65) from "your request is malformed" (42).
    #[tokio::test]
    async fn act_as_non_super_user_rejected_with_authorization_failed() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("alice".to_string()),
            ..Default::default()
        };
        let resp = handle(
            &req,
            // `bob` is not in the super-users set.
            &authed("bob"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
        controller.cancel().await;
    }

    /// Spec §1.2: act-as requires BOTH `owner_principal_type` and
    /// `owner_principal_name` to be set; partial state is never valid
    /// even for a super-user. Returns `INVALID_REQUEST` (42) — a
    /// malformed request, not an authorization failure.
    #[tokio::test]
    async fn act_as_with_only_one_field_set_returns_invalid_request() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());

        // Type set but name empty.
        let req_name_missing = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: None,
            ..Default::default()
        };
        let resp = handle(
            &req_name_missing,
            &authed("admin"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::INVALID_REQUEST);

        // Name set but type empty.
        let req_type_missing = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            owner_principal_type: None,
            owner_principal_name: Some("alice".to_string()),
            ..Default::default()
        };
        let resp = handle(
            &req_type_missing,
            &authed("admin"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::INVALID_REQUEST);

        controller.cancel().await;
    }

    #[test]
    fn token_gate_uses_delegation_token_level() {
        use crabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL;
        use crabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};

        let gate = |level: Option<i16>| {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = level {
                image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: crate::features::METADATA_VERSION.to_string(),
                    level,
                }));
            }
            crate::features::require_feature(
                &image,
                crate::features::METADATA_VERSION,
                DELEGATION_TOKEN_MIN_LEVEL,
            )
            .is_err()
        };

        assert!(!gate(None));
        assert!(gate(Some(13)));
        assert!(!gate(Some(14)));
    }

    /// Spec §1.2: only `User` is supported as the act-as owner
    /// type (mTLS-DN owners are not supported). Any other type from a super-user
    /// is `INVALID_REQUEST` (42) — the request is syntactically wrong,
    /// not unauthorized.
    #[tokio::test]
    async fn act_as_with_non_user_principal_type_returns_invalid_request() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            owner_principal_type: Some("Group".to_string()),
            owner_principal_name: Some("eng".to_string()),
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("admin"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::INVALID_REQUEST);
        controller.cancel().await;
    }
}

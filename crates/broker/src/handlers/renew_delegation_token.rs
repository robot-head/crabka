//! Slice 51: `RenewDelegationToken` (`api_key` 39). Stub in T5 — full
//! body in T7.

use crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest;
use crabka_protocol::owned::renew_delegation_token_response::RenewDelegationTokenResponse;

/// `DELEGATION_TOKEN_AUTH_DISABLED` (61) — returned by every delegation-token
/// RPC when the broker is not configured with a master HMAC key.
const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;

#[allow(clippy::unused_async)]
pub(crate) async fn handle(
    _req: &RenewDelegationTokenRequest,
    secret_key: Option<&crabka_security::SecretBytes>,
) -> RenewDelegationTokenResponse {
    if secret_key.is_none() {
        return RenewDelegationTokenResponse {
            error_code: DELEGATION_TOKEN_AUTH_DISABLED,
            ..Default::default()
        };
    }
    // Body lands in T7.
    unimplemented!("filled in T7");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_auth_disabled_when_no_secret_key() {
        let req = RenewDelegationTokenRequest::default();
        let resp = handle(&req, None).await;
        assert_eq!(resp.error_code, DELEGATION_TOKEN_AUTH_DISABLED);
    }
}

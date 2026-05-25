//! Slice 51: `DescribeDelegationToken` (`api_key` 41). Stub in T5 — full
//! body in T6.

use crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest;
use crabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse;

/// `DELEGATION_TOKEN_AUTH_DISABLED` (61) — returned by every delegation-token
/// RPC when the broker is not configured with a master HMAC key.
const DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;

#[allow(clippy::unused_async)]
pub(crate) async fn handle(
    _req: &DescribeDelegationTokenRequest,
    secret_key: Option<&crabka_security::SecretBytes>,
) -> DescribeDelegationTokenResponse {
    if secret_key.is_none() {
        return DescribeDelegationTokenResponse {
            error_code: DELEGATION_TOKEN_AUTH_DISABLED,
            ..Default::default()
        };
    }
    // Body lands in T6.
    unimplemented!("filled in T6");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_auth_disabled_when_no_secret_key() {
        let req = DescribeDelegationTokenRequest::default();
        let resp = handle(&req, None).await;
        assert_eq!(resp.error_code, DELEGATION_TOKEN_AUTH_DISABLED);
    }
}

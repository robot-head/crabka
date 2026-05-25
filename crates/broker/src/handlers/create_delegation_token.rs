//! Slice 51: `CreateDelegationToken` (`api_key` 38). Stub in T5 — full
//! body in T6.

use crabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest;
use crabka_protocol::owned::create_delegation_token_response::CreateDelegationTokenResponse;

#[allow(clippy::unused_async)]
pub(crate) async fn handle(
    _req: &CreateDelegationTokenRequest,
    secret_key: Option<&crabka_security::SecretBytes>,
) -> CreateDelegationTokenResponse {
    if secret_key.is_none() {
        return CreateDelegationTokenResponse {
            error_code: crate::codes::DELEGATION_TOKEN_AUTH_DISABLED,
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
        let req = CreateDelegationTokenRequest::default();
        let resp = handle(&req, None).await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_AUTH_DISABLED
        );
    }
}

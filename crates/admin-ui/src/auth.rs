//! Broker-backed login for the admin UI.

use crabka_client_admin::AdminClient;
use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_security::SaslMechanism;
use serde::{Deserialize, Serialize};

use crate::config::AdminUiConfig;
use crate::error::UiError;
use crate::session::{SessionStore, SessionUser};

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LoginSuccess {
    pub username: String,
    pub principal: String,
    pub session_id: String,
}

#[must_use]
pub fn build_scram_sha512_security(
    cfg: &AdminUiConfig,
    username: &str,
    password: &str,
) -> ClientSecurity {
    ClientSecurity {
        protocol: cfg.security.listener_protocol(),
        tls: cfg.security.tls(),
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username: username.to_string(),
            password: password.to_string(),
        }),
        sasl_host: None,
    }
}

pub struct AuthService<'a> {
    cfg: &'a AdminUiConfig,
    sessions: &'a SessionStore,
}

impl<'a> AuthService<'a> {
    #[must_use]
    pub const fn new(cfg: &'a AdminUiConfig, sessions: &'a SessionStore) -> Self {
        Self { cfg, sessions }
    }

    pub async fn login(&self, request: LoginRequest) -> Result<LoginSuccess, UiError> {
        let security = build_scram_sha512_security(self.cfg, &request.username, &request.password);
        let mut client =
            AdminClient::connect_secured(&self.cfg.bootstrap_addrs, Some(security)).await?;
        client.metadata(&[]).await?;

        let principal = format!("User:{}", request.username);
        let session_id = self.sessions.create(SessionUser {
            username: request.username.clone(),
            principal: principal.clone(),
        });

        Ok(LoginSuccess {
            username: request.username,
            principal,
            session_id: session_id.expose_for_cookie().to_string(),
        })
    }
}
